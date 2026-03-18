"""
Unit tests for the Lambda handler.

Mocks all AWS calls — idempotency, archiver, emitter are patched at the
module boundary so we test handler control flow in isolation.
"""

import json
from unittest.mock import MagicMock, patch

from src.certification_processor.handler import RetryableError, handler
from src.certification_processor.idempotency import IdempotencyConflict


def _make_record(cert_id="CERT-001", idempotency_key="CERT-001#v1", message_id="msg-1"):
    detail = {
        "certId": cert_id,
        "idempotencyKey": idempotency_key,
        "tradeEntityId": "TE-999",
        "certType": "ISO_9001",
        "issuedAt": "2024-01-01T00:00:00Z",
        "expiresAt": "2026-01-01T00:00:00Z",
        "jurisdiction": "US",
        "schemaVersion": "1.0",
    }
    return {
        "messageId": message_id,
        "body": json.dumps({"detail": detail}),
    }


@patch("src.certification_processor.handler.mark_completed")
@patch("src.certification_processor.handler.emit_downstream")
@patch("src.certification_processor.handler.archive_event")
@patch("src.certification_processor.handler.write_processing")
class TestHandler:
    def test_happy_path_returns_no_failures(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        event = {"Records": [_make_record()]}
        result = handler(event, MagicMock())
        assert result == {"batchItemFailures": []}
        mock_write.assert_called_once()
        mock_archive.assert_called_once()
        mock_emit.assert_called_once()
        mock_complete.assert_called_once()

    def test_idempotency_conflict_is_not_a_failure(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        mock_write.side_effect = IdempotencyConflict("already done")
        event = {"Records": [_make_record()]}
        result = handler(event, MagicMock())
        assert result == {"batchItemFailures": []}
        mock_archive.assert_not_called()

    def test_archive_failure_marks_record_for_retry(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        mock_archive.side_effect = RetryableError("S3 timeout")
        event = {"Records": [_make_record(message_id="msg-bad")]}
        result = handler(event, MagicMock())
        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-bad"}]}
        mock_complete.assert_not_called()

    def test_emit_failure_marks_record_for_retry(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        mock_emit.side_effect = RetryableError("EB throttled")
        event = {"Records": [_make_record(message_id="msg-emit")]}
        result = handler(event, MagicMock())
        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-emit"}]}

    def test_partial_batch_failure_preserves_good_records(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        """Good records must NOT appear in batchItemFailures."""
        records = [
            _make_record(cert_id="CERT-001", message_id="msg-good"),
            _make_record(cert_id="CERT-002", message_id="msg-bad"),
            _make_record(cert_id="CERT-003", message_id="msg-good-2"),
        ]

        def archive_side_effect(detail):
            if detail["certId"] == "CERT-002":
                raise RetryableError("S3 error")

        mock_archive.side_effect = archive_side_effect
        event = {"Records": records}
        result = handler(event, MagicMock())

        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-bad"}]}
        assert mock_complete.call_count == 2  # CERT-001 and CERT-003

    def test_unhandled_exception_marks_record_for_retry(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        mock_write.side_effect = RuntimeError("unexpected boom")
        event = {"Records": [_make_record(message_id="msg-boom")]}
        result = handler(event, MagicMock())
        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-boom"}]}

    def test_empty_records_returns_no_failures(
        self, mock_write, mock_archive, mock_emit, mock_complete
    ):
        result = handler({"Records": []}, MagicMock())
        assert result == {"batchItemFailures": []}
