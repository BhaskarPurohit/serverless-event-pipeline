"""
Unit tests for DynamoDB idempotency logic.

Tests the conditional expression semantics without a real DynamoDB by
patching the table at the module level.
"""

from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from src.certification_processor.idempotency import (
    IdempotencyConflict,
    mark_completed,
    mark_failed,
    write_processing,
)

_DETAIL = {
    "certId": "CERT-001",
    "idempotencyKey": "CERT-001#v1",
    "tradeEntityId": "TE-1",
    "certType": "ISO_9001",
    "expiresAt": "2026-01-01T00:00:00Z",
}


def _conditional_check_error():
    err = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "failed"}},
        "PutItem",
    )
    return err


def _throttle_error():
    return ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow"}},
        "PutItem",
    )


@patch("src.certification_processor.idempotency._table")
class TestWriteProcessing:
    def test_first_delivery_succeeds(self, mock_table):
        write_processing(_DETAIL)
        mock_table.put_item.assert_called_once()

    def test_completed_event_raises_conflict(self, mock_table):
        mock_table.put_item.side_effect = _conditional_check_error()
        with pytest.raises(IdempotencyConflict):
            write_processing(_DETAIL)

    def test_throttle_is_reraised_for_sqs_retry(self, mock_table):
        mock_table.put_item.side_effect = _throttle_error()
        with pytest.raises(ClientError):
            write_processing(_DETAIL)


@patch("src.certification_processor.idempotency._table")
class TestMarkCompleted:
    def test_transitions_processing_to_completed(self, mock_table):
        mark_completed("CERT-001", "CERT-001#v1")
        mock_table.update_item.assert_called_once()
        call_kwargs = mock_table.update_item.call_args[1]
        assert ":completed" in call_kwargs["ExpressionAttributeValues"]


@patch("src.certification_processor.idempotency._table")
class TestMarkFailed:
    def test_sets_failed_status(self, mock_table):
        mark_failed("CERT-001", "CERT-001#v1", reason="S3 timeout")
        mock_table.update_item.assert_called_once()

    def test_client_error_is_swallowed(self, mock_table):
        """mark_failed must not mask the original processing error."""
        mock_table.update_item.side_effect = _throttle_error()
        # Should not raise
        mark_failed("CERT-001", "CERT-001#v1", reason="boom")
