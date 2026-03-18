"""
Integration tests for the EventBridge emitter.

Verifies that put_events reaches the bus, the SQS target receives the event,
and the detail-type mapping is applied correctly.  Also covers the
FailedEntryCount error path.
"""

import json
from unittest.mock import patch

import boto3
import pytest

from src.certification_processor.emitter import emit_downstream
from tests.integration.conftest import (
    DOWNSTREAM_BUS_ARN,
    REGION,
    drain_sqs,
    make_detail,
)


@pytest.fixture
def patched_emitter(downstream_bus_with_sqs):
    """Patch the module-level _eb client and _EVENT_BUS_ARN."""
    import src.certification_processor.emitter as emitter_mod

    fresh_eb = boto3.client("events", region_name=REGION)
    with (
        patch.object(emitter_mod, "_eb", fresh_eb),
        patch.object(emitter_mod, "_EVENT_BUS_ARN", DOWNSTREAM_BUS_ARN),
    ):
        yield {
            "sqs": downstream_bus_with_sqs["sqs"],
            "queue_url": downstream_bus_with_sqs["queue_url"],
        }


class TestEmitDownstream:
    def test_event_arrives_in_sqs_target(self, patched_emitter):
        detail = make_detail()
        emit_downstream(detail)

        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert len(messages) == 1

    def test_source_field_correct(self, patched_emitter):
        emit_downstream(make_detail())
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert messages[0]["source"] == "trade.certification.processor"

    def test_detail_type_mapped_correctly(self, patched_emitter):
        emit_downstream(make_detail(detail_type="CertificationIssued"))
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert messages[0]["detail-type"] == "CertificationActivated"

    def test_revocation_mapping(self, patched_emitter):
        emit_downstream(
            make_detail(
                cert_id="CERT-REV",
                idempotency_key="CERT-REV#v1",
                detail_type="CertificationRevoked",
            )
        )
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert messages[0]["detail-type"] == "CertificationDeactivated"

    def test_expiry_mapping(self, patched_emitter):
        emit_downstream(
            make_detail(
                cert_id="CERT-EXP",
                idempotency_key="CERT-EXP#v1",
                detail_type="CertificationExpiring",
            )
        )
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert messages[0]["detail-type"] == "CertificationExpiryWarning"

    def test_unknown_detail_type_passes_through(self, patched_emitter):
        """Unmapped types are forwarded unchanged (forward-compat)."""
        detail = make_detail()
        detail["detail-type"] = "SomeNewEventType"
        emit_downstream(detail)
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        assert messages[0]["detail-type"] == "SomeNewEventType"

    def test_eb_detail_contains_cert_fields(self, patched_emitter):
        emit_downstream(make_detail())
        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"])
        raw = messages[0]["detail"]
        eb_detail = raw if isinstance(raw, dict) else json.loads(raw)

        assert eb_detail["certId"] == "CERT-001"
        assert eb_detail["idempotencyKey"] == "CERT-001#v1"
        assert eb_detail["certType"] == "ISO_9001"
        assert eb_detail["jurisdiction"] == "US"
        assert eb_detail["schemaVersion"] == "1.0"

    def test_failed_entry_count_raises(self, patched_emitter):
        """
        Simulates EventBridge rejecting the entry (e.g. malformed detail).
        The emitter must raise so the handler marks_failed and returns the
        record to SQS.
        """
        import src.certification_processor.emitter as emitter_mod

        bad_response = {
            "FailedEntryCount": 1,
            "Entries": [{"ErrorCode": "InvalidArgument", "ErrorMessage": "bad detail"}],
        }
        with (
            patch.object(emitter_mod._eb, "put_events", return_value=bad_response),
            pytest.raises(RuntimeError, match="EventBridge PutEvents failed"),
        ):
            emit_downstream(make_detail())

    def test_multiple_events_each_arrive_independently(self, patched_emitter):
        for i in range(3):
            emit_downstream(
                make_detail(
                    cert_id=f"CERT-{i}",
                    idempotency_key=f"CERT-{i}#v1",
                )
            )

        messages = drain_sqs(patched_emitter["sqs"], patched_emitter["queue_url"], max_messages=10)
        assert len(messages) == 3

        def _detail(m):
            raw = m["detail"]
            return raw if isinstance(raw, dict) else json.loads(raw)

        cert_ids = {_detail(m)["certId"] for m in messages}
        assert cert_ids == {"CERT-0", "CERT-1", "CERT-2"}
