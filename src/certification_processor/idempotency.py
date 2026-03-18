"""
DynamoDB idempotency gate using conditional writes.

State machine per (certId, idempotencyKey):
  [absent]    → PROCESSING  (first delivery, or new version)
  PROCESSING  → PROCESSING  (re-entry after mid-flight crash — allowed)
  COMPLETED   → conflict    (already done — raise IdempotencyConflict)
  FAILED      → PROCESSING  (explicit retry of a failed event — allowed)

TTL = cert expiry + 90 days, so late-arriving replays are still deduplicated
within a reasonable window.
"""

import contextlib
import os
from datetime import UTC, datetime

import boto3
from botocore.exceptions import ClientError

_TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "test-idempotency")
_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)

_90_DAYS_SECONDS = 90 * 24 * 60 * 60


class IdempotencyConflict(Exception):
    """Raised when a record with this idempotencyKey is already COMPLETED."""


def write_processing(detail: dict) -> None:
    cert_id: str = detail["certId"]
    idempotency_key: str = detail["idempotencyKey"]
    expires_at: str = detail.get("expiresAt", "")
    ttl = _compute_ttl(expires_at)

    try:
        _table.put_item(
            Item={
                "certId": cert_id,
                "SK": "#METADATA",
                "idempotencyKey": idempotency_key,
                "status": "PROCESSING",
                "processedAt": _now(),
                "ttl": ttl,
                "eventPayload": detail,
            },
            ConditionExpression=(
                "attribute_not_exists(certId)"
                " OR (idempotencyKey = :key AND #s <> :completed)"
                " OR idempotencyKey <> :key"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":key": idempotency_key,
                ":completed": "COMPLETED",
            },
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ConditionalCheckFailedException":
            raise IdempotencyConflict(
                f"Event already processed: certId={cert_id} key={idempotency_key}"
            ) from e
        raise


def mark_completed(cert_id: str, idempotency_key: str) -> None:
    """Seal the record as COMPLETED. Only transitions from PROCESSING."""
    _table.update_item(
        Key={"certId": cert_id, "SK": "#METADATA"},
        UpdateExpression="SET #s = :completed, completedAt = :now",
        ConditionExpression="idempotencyKey = :key AND #s = :processing",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":completed": "COMPLETED",
            ":processing": "PROCESSING",
            ":key": idempotency_key,
            ":now": _now(),
        },
    )


def mark_failed(cert_id: str, idempotency_key: str, reason: str) -> None:
    with contextlib.suppress(ClientError):
        _table.update_item(
            Key={"certId": cert_id, "SK": "#METADATA"},
            UpdateExpression=("SET #s = :failed, failedAt = :now, failureReason = :reason"),
            ConditionExpression="idempotencyKey = :key",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":failed": "FAILED",
                ":now": _now(),
                ":reason": reason[:1000],
                ":key": idempotency_key,
            },
        )


def _compute_ttl(expires_at: str) -> int | None:
    if not expires_at:
        return None
    try:
        expiry_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return int(expiry_dt.timestamp()) + _90_DAYS_SECONDS
    except ValueError:
        return None


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()
