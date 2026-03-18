"""
Integration tests for the DynamoDB idempotency state machine.

Each test runs against a real (moto-backed) DynamoDB table and exercises
the full conditional expression logic — not mock return values.

State machine under test:
  [absent]    → PROCESSING   first delivery
  PROCESSING  → PROCESSING   crash re-entry (allowed)
  FAILED      → PROCESSING   explicit replay (allowed)
  COMPLETED   → conflict     IdempotencyConflict raised
  [new key]   → PROCESSING   new event version (allowed)
"""

import contextlib

import pytest

from src.certification_processor.idempotency import (
    IdempotencyConflict,
    mark_completed,
    mark_failed,
    write_processing,
)
from tests.integration.conftest import get_dynamo_item, make_detail


class TestFirstDelivery:
    def test_writes_item_with_processing_status(self, patched_table):
        detail = make_detail()
        write_processing(detail)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item is not None
        assert item["status"] == "PROCESSING"
        assert item["idempotencyKey"] == "CERT-001#v1"

    def test_event_payload_stored(self, patched_table):
        detail = make_detail()
        write_processing(detail)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item["eventPayload"]["certId"] == "CERT-001"

    def test_ttl_set_to_expiry_plus_90_days(self, patched_table):
        from datetime import UTC, datetime

        detail = make_detail()
        write_processing(detail)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert "ttl" in item

        # 2026-01-01 expiry + 90 days (7_776_000s) should be well after now
        expiry_epoch = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()) + 7_776_000
        assert item["ttl"] == expiry_epoch

    def test_missing_expiry_omits_ttl(self, patched_table):
        detail = make_detail()
        detail.pop("expiresAt")
        write_processing(detail)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item.get("ttl") is None


class TestCrashReentry:
    def test_processing_item_can_be_overwritten(self, patched_table):
        """Lambda crashed after DynamoDB write. Next delivery must re-enter."""
        detail = make_detail()

        # Simulate first delivery reaching DynamoDB but crashing before S3
        write_processing(detail)
        assert get_dynamo_item(patched_table, "CERT-001")["status"] == "PROCESSING"

        # Second delivery — must not raise
        write_processing(detail)
        assert get_dynamo_item(patched_table, "CERT-001")["status"] == "PROCESSING"


class TestCompletedDeduplication:
    def test_completed_item_raises_conflict(self, patched_table):
        detail = make_detail()
        write_processing(detail)
        mark_completed("CERT-001", "CERT-001#v1")

        with pytest.raises(IdempotencyConflict):
            write_processing(detail)

    def test_dynamo_state_unchanged_after_conflict(self, patched_table):
        detail = make_detail()
        write_processing(detail)
        mark_completed("CERT-001", "CERT-001#v1")

        with contextlib.suppress(IdempotencyConflict):
            write_processing(detail)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item["status"] == "COMPLETED"


class TestFailedReplay:
    def test_failed_item_allows_reprocessing(self, patched_table):
        """Ops replays a FAILED event after the root cause is fixed."""
        detail = make_detail()
        write_processing(detail)
        mark_failed("CERT-001", "CERT-001#v1", reason="S3 outage")

        assert get_dynamo_item(patched_table, "CERT-001")["status"] == "FAILED"

        # Replay — must not raise
        write_processing(detail)
        assert get_dynamo_item(patched_table, "CERT-001")["status"] == "PROCESSING"


class TestNewEventVersion:
    def test_different_idempotency_key_overwrites_completed(self, patched_table):
        """A corrected resend with a new version key must be processed."""
        v1 = make_detail(idempotency_key="CERT-001#v1")
        write_processing(v1)
        mark_completed("CERT-001", "CERT-001#v1")

        v2 = make_detail(idempotency_key="CERT-001#v2")
        write_processing(v2)  # must not raise

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item["status"] == "PROCESSING"
        assert item["idempotencyKey"] == "CERT-001#v2"


class TestMarkCompleted:
    def test_transitions_processing_to_completed(self, patched_table):
        detail = make_detail()
        write_processing(detail)
        mark_completed("CERT-001", "CERT-001#v1")

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item["status"] == "COMPLETED"
        assert "completedAt" in item

    def test_idempotent_on_already_completed(self, patched_table):
        """mark_completed is called twice (e.g. retry of a nearly-done Lambda).
        The conditional expression will raise — we verify it propagates so
        the handler does NOT silently double-complete."""
        from botocore.exceptions import ClientError

        detail = make_detail()
        write_processing(detail)
        mark_completed("CERT-001", "CERT-001#v1")

        # Second mark_completed should raise ConditionalCheckFailed
        with pytest.raises(ClientError) as exc_info:
            mark_completed("CERT-001", "CERT-001#v1")
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestMarkFailed:
    def test_transitions_processing_to_failed(self, patched_table):
        detail = make_detail()
        write_processing(detail)
        mark_failed("CERT-001", "CERT-001#v1", reason="S3 timeout")

        item = get_dynamo_item(patched_table, "CERT-001")
        assert item["status"] == "FAILED"
        assert item["failureReason"] == "S3 timeout"
        assert "failedAt" in item

    def test_reason_truncated_at_1000_chars(self, patched_table):
        detail = make_detail()
        write_processing(detail)
        mark_failed("CERT-001", "CERT-001#v1", reason="x" * 2000)

        item = get_dynamo_item(patched_table, "CERT-001")
        assert len(item["failureReason"]) == 1000

    def test_does_not_raise_on_missing_item(self, patched_table):
        """mark_failed must swallow errors — it runs inside an except block."""
        mark_failed("NONEXISTENT", "key#v1", reason="boom")  # no raise


class TestMultipleCerts:
    def test_separate_certs_do_not_interfere(self, patched_table):
        cert_a = make_detail(cert_id="CERT-A", idempotency_key="CERT-A#v1")
        cert_b = make_detail(cert_id="CERT-B", idempotency_key="CERT-B#v1")

        write_processing(cert_a)
        write_processing(cert_b)
        mark_completed("CERT-A", "CERT-A#v1")

        assert get_dynamo_item(patched_table, "CERT-A")["status"] == "COMPLETED"
        assert get_dynamo_item(patched_table, "CERT-B")["status"] == "PROCESSING"

    def test_conflict_on_cert_a_does_not_affect_cert_b(self, patched_table):
        cert_a = make_detail(cert_id="CERT-A", idempotency_key="CERT-A#v1")
        cert_b = make_detail(cert_id="CERT-B", idempotency_key="CERT-B#v1")

        write_processing(cert_a)
        mark_completed("CERT-A", "CERT-A#v1")
        write_processing(cert_b)
        mark_completed("CERT-B", "CERT-B#v1")

        with pytest.raises(IdempotencyConflict):
            write_processing(cert_a)

        # CERT-B stays COMPLETED, unaffected
        assert get_dynamo_item(patched_table, "CERT-B")["status"] == "COMPLETED"
