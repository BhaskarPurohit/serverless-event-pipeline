"""
Archives the raw event payload to S3 for compliance and replay.

Path pattern:
  s3://{ARCHIVE_BUCKET}/year=YYYY/month=MM/day=DD/{certId}/{idempotencyKey}.json

Partitioned by date so Athena / Glue crawlers work without a manifest.
"""

import json
import os
from datetime import UTC, datetime

import boto3
from botocore.config import Config

_BUCKET = os.environ["ARCHIVE_BUCKET"]
_s3 = boto3.client(
    "s3",
    config=Config(retries={"max_attempts": 3, "mode": "adaptive"}),
)


def archive_event(detail: dict) -> None:
    """Write event to S3. Raises on failure so handler can mark_failed."""
    key = _build_key(detail)
    _s3.put_object(
        Bucket=_BUCKET,
        Key=key,
        Body=json.dumps(detail, default=str).encode(),
        ContentType="application/json",
        # Server-side encryption — enforce even if bucket policy already does
        ServerSideEncryption="aws:kms",
    )


def _build_key(detail: dict) -> str:
    now = datetime.now(tz=UTC)
    cert_id = detail["certId"]
    idempotency_key = detail["idempotencyKey"]
    return (
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/{cert_id}/{idempotency_key}.json"
    )
