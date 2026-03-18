"""
End-to-end integration tests for the Lambda handler.

Every test invokes handler() with a real SQS-shaped event dict and then
asserts against:
  - DynamoDB item state (PROCESSING → COMPLETED / FAILED)
  - S3 object existence and content
  - EventBridge events received in the downstream SQS queue
  - batchItemFailures in the handler return value

All AWS calls run through moto — no real AWS account needed.
"""

import json

import pytest

from src.certification_processor.handler import handler
from tests.integration.conftest import (
    drain_sqs,
    get_dynamo_item,
    make_detail,
    make_sqs_event,
)


class TestHappyPath:
    def test_single_record_returns_no_failures(self, patched_modules):
        detail = make_detail()
        result = handler(make_sqs_event(detail), None)
        assert result == {"batchItemFailures": []}

    def test_dynamo_item_is_completed(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)

        item = get_dynamo_item(patched_modules["table"], "CERT-001")
        assert item is not None
        assert item["status"] == "COMPLETED"
        assert "completedAt" in item

    def test_s3_object_written(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)

        # List all objects in the bucket and find the one for this certId
        s3 = patched_modules["s3"]
        bucket = patched_modules["bucket"]
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        keys = [obj["Key"] for obj in objects]
        assert any("CERT-001" in k for k in keys), f"No archive key found. Keys: {keys}"

    def test_s3_archive_contains_correct_payload(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)

        s3 = patched_modules["s3"]
        bucket = patched_modules["bucket"]
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        key = next(o["Key"] for o in objects if "CERT-001" in o["Key"])

        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        payload = json.loads(body)
        assert payload["certId"] == "CERT-001"
        assert payload["idempotencyKey"] == "CERT-001#v1"

    def test_downstream_event_emitted(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)

        messages = drain_sqs(patched_modules["sqs"], patched_modules["queue_url"])
        assert len(messages) == 1
        assert messages[0]["source"] == "trade.certification.processor"
        assert messages[0]["detail-type"] == "CertificationActivated"

    def test_downstream_event_detail_content(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)

        messages = drain_sqs(patched_modules["sqs"], patched_modules["queue_url"])
        # EventBridge→SQS delivery embeds `detail` as a parsed dict in the envelope
        eb_detail = messages[0]["detail"]
        if isinstance(eb_detail, str):
            eb_detail = json.loads(eb_detail)
        assert eb_detail["certId"] == "CERT-001"
        assert eb_detail["idempotencyKey"] == "CERT-001#v1"


class TestDuplicateDelivery:
    def test_duplicate_is_silently_skipped(self, patched_modules):
        """Same record delivered twice — second must not appear in failures."""
        detail = make_detail()
        handler(make_sqs_event(detail), None)  # first delivery → COMPLETED

        result = handler(make_sqs_event(detail), None)  # duplicate
        assert result == {"batchItemFailures": []}

    def test_duplicate_does_not_overwrite_completed_item(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)
        first_item = get_dynamo_item(patched_modules["table"], "CERT-001")

        handler(make_sqs_event(detail), None)  # duplicate
        second_item = get_dynamo_item(patched_modules["table"], "CERT-001")

        assert second_item["completedAt"] == first_item["completedAt"]

    def test_duplicate_does_not_emit_second_downstream_event(self, patched_modules):
        detail = make_detail()
        handler(make_sqs_event(detail), None)
        handler(make_sqs_event(detail), None)  # duplicate

        messages = drain_sqs(patched_modules["sqs"], patched_modules["queue_url"])
        assert len(messages) == 1  # only first delivery emitted


class TestPartialBatch:
    def test_good_records_not_in_failures(self, patched_modules):
        good_1 = make_detail(cert_id="CERT-G1", idempotency_key="CERT-G1#v1")
        good_2 = make_detail(cert_id="CERT-G2", idempotency_key="CERT-G2#v1")
        # Already-completed cert triggers IdempotencyConflict (not a failure)
        dup = make_detail(cert_id="CERT-G1", idempotency_key="CERT-G1#v1")

        # Pre-complete CERT-G1 so the second delivery is a duplicate
        handler(make_sqs_event(good_1), None)
        result = handler(make_sqs_event(dup, good_2), None)

        assert result == {"batchItemFailures": []}
        assert get_dynamo_item(patched_modules["table"], "CERT-G2")["status"] == "COMPLETED"

    def test_ten_records_all_succeed(self, patched_modules):
        details = [
            make_detail(cert_id=f"CERT-{i:03d}", idempotency_key=f"CERT-{i:03d}#v1")
            for i in range(10)
        ]
        result = handler(make_sqs_event(*details), None)

        assert result == {"batchItemFailures": []}
        for i in range(10):
            item = get_dynamo_item(patched_modules["table"], f"CERT-{i:03d}")
            assert item["status"] == "COMPLETED"


class TestCrashRecovery:
    def test_processing_item_reprocessed_successfully(self, patched_modules):
        """
        Simulates a Lambda that wrote PROCESSING to DynamoDB then crashed
        before reaching S3. The next SQS delivery must complete the pipeline.
        """
        import src.certification_processor.idempotency as idempotency_mod

        detail = make_detail()

        # Plant a PROCESSING item (crash mid-flight simulation)
        idempotency_mod.write_processing(detail)
        item = get_dynamo_item(patched_modules["table"], "CERT-001")
        assert item["status"] == "PROCESSING"

        # Next delivery — must complete end-to-end
        result = handler(make_sqs_event(detail), None)

        assert result == {"batchItemFailures": []}
        assert get_dynamo_item(patched_modules["table"], "CERT-001")["status"] == "COMPLETED"

        # S3 and EventBridge should have fired on this second delivery
        objects = (
            patched_modules["s3"]
            .list_objects_v2(Bucket=patched_modules["bucket"])
            .get("Contents", [])
        )
        assert any("CERT-001" in o["Key"] for o in objects)

    def test_failed_item_reprocessed_after_ops_replay(self, patched_modules):
        """After mark_failed, the next SQS re-delivery processes successfully."""
        import src.certification_processor.idempotency as idempotency_mod

        detail = make_detail()
        idempotency_mod.write_processing(detail)
        idempotency_mod.mark_failed("CERT-001", "CERT-001#v1", reason="prior outage")

        assert get_dynamo_item(patched_modules["table"], "CERT-001")["status"] == "FAILED"

        result = handler(make_sqs_event(detail), None)
        assert result == {"batchItemFailures": []}
        assert get_dynamo_item(patched_modules["table"], "CERT-001")["status"] == "COMPLETED"


class TestDetailTypeMapping:
    @pytest.mark.parametrize(
        "inbound_type,expected_outbound",
        [
            ("CertificationIssued", "CertificationActivated"),
            ("CertificationRevoked", "CertificationDeactivated"),
            ("CertificationExpiring", "CertificationExpiryWarning"),
        ],
    )
    def test_detail_type_mapping(self, patched_modules, inbound_type, expected_outbound):
        detail = make_detail(
            cert_id=f"CERT-{inbound_type[:4]}",
            idempotency_key=f"CERT-{inbound_type[:4]}#v1",
            detail_type=inbound_type,
        )
        handler(make_sqs_event(detail), None)

        messages = drain_sqs(patched_modules["sqs"], patched_modules["queue_url"])
        assert messages[0]["detail-type"] == expected_outbound


class TestEmptyBatch:
    def test_empty_records_returns_no_failures(self, patched_modules):
        result = handler({"Records": []}, None)
        assert result == {"batchItemFailures": []}
