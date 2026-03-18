"""
Lambda handler for trade certification events.

SQS trigger → process each record → conditional DynamoDB write (idempotency)
→ S3 archive → downstream EventBridge emission.

Returns ReportBatchItemFailures so only failed records are retried, not the
entire batch.
"""

import json
import logging
import os
from typing import Any

from .archiver import archive_event
from .emitter import emit_downstream
from .idempotency import IdempotencyConflict, mark_completed, mark_failed, write_processing

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def handler(event: dict, context: Any) -> dict:
    """
    Entry point. Returns {"batchItemFailures": [...]} for partial batch success.
    Any record not listed is considered successfully processed and deleted from SQS.
    """
    failures: list[dict] = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            _process_record(record)
        except IdempotencyConflict as e:
            # Already processed — not an error. Log and move on.
            logger.info(
                "Skipping duplicate event",
                extra={"messageId": message_id, "reason": str(e)},
            )
        except RetryableError as e:
            logger.warning(
                "Retryable error — returning to queue",
                extra={"messageId": message_id, "error": str(e)},
                exc_info=True,
            )
            failures.append({"itemIdentifier": message_id})
        except Exception as e:
            logger.error(
                "Unretryable error — will land in DLQ after max receives",
                extra={"messageId": message_id, "error": str(e)},
                exc_info=True,
            )
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}


def _process_record(record: dict) -> None:
    """
    Full processing pipeline for a single SQS record:
      1. Parse EventBridge envelope
      2. Idempotency gate (DynamoDB conditional write → PROCESSING)
      3. Archive raw event to S3
      4. Emit downstream EventBridge event
      5. Mark COMPLETED in DynamoDB

    If step 3 or 4 fails, the item stays PROCESSING so the SQS retry will
    attempt again. write_processing allows re-entry when status == PROCESSING
    (i.e. a previous attempt crashed mid-flight).
    """
    body = json.loads(record["body"])

    # EventBridge wraps the original event when targeting SQS
    detail = body.get("detail") or body  # handle both EB envelope and raw message
    cert_id: str = detail["certId"]
    idempotency_key: str = detail["idempotencyKey"]

    logger.info(
        "Processing certification event",
        extra={"certId": cert_id, "idempotencyKey": idempotency_key},
    )

    # --- Step 1: Idempotency gate ---
    write_processing(detail)

    # --- Step 2: Archive ---
    try:
        archive_event(detail)
    except Exception as e:
        mark_failed(cert_id, idempotency_key, reason=str(e))
        raise RetryableError(f"S3 archive failed for {cert_id}: {e}") from e

    # --- Step 3: Emit downstream ---
    try:
        emit_downstream(detail)
    except Exception as e:
        mark_failed(cert_id, idempotency_key, reason=str(e))
        raise RetryableError(f"Downstream emit failed for {cert_id}: {e}") from e

    # --- Step 4: Seal as completed ---
    mark_completed(cert_id, idempotency_key)

    logger.info(
        "Certification event processed successfully",
        extra={"certId": cert_id, "idempotencyKey": idempotency_key},
    )


class RetryableError(Exception):
    """Signals the record should be retried (returned to SQS visibility)."""
