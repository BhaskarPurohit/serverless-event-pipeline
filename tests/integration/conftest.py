"""
Shared fixtures for integration tests.

Design notes
------------
Production modules (idempotency, archiver, emitter) create boto3 clients /
resources at *import time*, before any moto mock is active.  Those objects
point at real AWS endpoints.

Fix: every test fixture that touches a module patches the module-level
object (_table, _s3, _eb) with a freshly-constructed boto3 object that is
created *inside* the active mock_aws() context.  Calls through those fresh
objects are intercepted by moto.

os.environ must also be populated before the src modules are first imported
(they read env vars at module level).  We do that here at conftest *module*
level — before pytest collects test files, before any src import fires.
"""

import json
import os

# ── Must be set before any src.* import resolves os.environ ──────────────────
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("DYNAMODB_TABLE", "trade-certs-test")
os.environ.setdefault("ARCHIVE_BUCKET", "trade-cert-archive-test")
os.environ.setdefault(
    "DOWNSTREAM_EVENT_BUS_ARN",
    "arn:aws:events:us-east-1:123456789012:event-bus/trade-downstream-test",
)
# ─────────────────────────────────────────────────────────────────────────────

from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

# Constants used throughout fixtures and tests
REGION = "us-east-1"
TABLE_NAME = "trade-certs-test"
BUCKET_NAME = "trade-cert-archive-test"
DOWNSTREAM_BUS_NAME = "trade-downstream-test"
DOWNSTREAM_BUS_ARN = f"arn:aws:events:{REGION}:123456789012:event-bus/{DOWNSTREAM_BUS_NAME}"


# ── Low-level: start moto ─────────────────────────────────────────────────────


@pytest.fixture
def aws_mock():
    """Start moto mock for all AWS services for the duration of one test."""
    with mock_aws():
        yield


# ── AWS resource fixtures (all depend on aws_mock) ───────────────────────────


@pytest.fixture
def dynamodb_table(aws_mock):
    """Real DynamoDB table inside moto."""
    ddb = boto3.resource("dynamodb", region_name=REGION)
    table = ddb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "certId", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "certId", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    return table


@pytest.fixture
def s3_bucket(aws_mock):
    """Real S3 bucket inside moto."""
    s3 = boto3.client("s3", region_name=REGION)
    # us-east-1 is the default; other regions need a LocationConstraint
    s3.create_bucket(Bucket=BUCKET_NAME)
    return BUCKET_NAME


@pytest.fixture
def downstream_bus_with_sqs(aws_mock):
    """
    EventBridge bus + catch-all SQS target so emitted events can be asserted.

    Architecture:
      emitter.put_events → DOWNSTREAM_BUS → rule (match all) → SQS queue
    Callers receive from the SQS queue to verify event content.
    """
    eb = boto3.client("events", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)

    eb.create_event_bus(Name=DOWNSTREAM_BUS_NAME)

    queue_url = sqs.create_queue(QueueName="downstream-capture")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]

    eb.put_rule(
        Name="capture-all",
        EventBusName=DOWNSTREAM_BUS_NAME,
        EventPattern=json.dumps({"source": ["trade.certification.processor"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="capture-all",
        EventBusName=DOWNSTREAM_BUS_NAME,
        Targets=[{"Id": "capture-sqs", "Arn": queue_arn}],
    )
    return {"sqs": sqs, "queue_url": queue_url}


# ── Module-patching fixtures ──────────────────────────────────────────────────


@pytest.fixture
def patched_table(dynamodb_table):
    """
    Patches idempotency._table with a moto-backed Table.
    Use this for idempotency-only tests that don't need S3 or EventBridge.
    """
    import src.certification_processor.idempotency as mod

    fresh = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
    with patch.object(mod, "_table", fresh):
        yield dynamodb_table


@pytest.fixture
def patched_modules(dynamodb_table, s3_bucket, downstream_bus_with_sqs):
    """
    Patches all three module-level AWS objects so the full handler pipeline
    runs against moto instead of real AWS.

    Yields a dict with handles for post-test assertions:
      table      — boto3 DynamoDB Table
      s3         — boto3 S3 client
      bucket     — bucket name string
      sqs        — boto3 SQS client
      queue_url  — SQS queue URL for receiving emitted events
    """
    import src.certification_processor.archiver as archiver_mod
    import src.certification_processor.emitter as emitter_mod
    import src.certification_processor.idempotency as idempotency_mod

    fresh_table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
    fresh_s3 = boto3.client("s3", region_name=REGION)
    fresh_eb = boto3.client("events", region_name=REGION)

    with (
        patch.object(idempotency_mod, "_table", fresh_table),
        patch.object(archiver_mod, "_s3", fresh_s3),
        patch.object(archiver_mod, "_BUCKET", BUCKET_NAME),
        patch.object(emitter_mod, "_eb", fresh_eb),
        patch.object(emitter_mod, "_EVENT_BUS_ARN", DOWNSTREAM_BUS_ARN),
    ):
        yield {
            "table": dynamodb_table,
            "s3": fresh_s3,
            "bucket": BUCKET_NAME,
            "sqs": downstream_bus_with_sqs["sqs"],
            "queue_url": downstream_bus_with_sqs["queue_url"],
        }


# ── Helper functions (available to all test modules) ─────────────────────────


def make_detail(
    cert_id="CERT-001",
    idempotency_key="CERT-001#v1",
    detail_type="CertificationIssued",
) -> dict:
    return {
        "certId": cert_id,
        "idempotencyKey": idempotency_key,
        "detail-type": detail_type,
        "tradeEntityId": "TE-999",
        "certType": "ISO_9001",
        "issuedAt": "2024-01-01T00:00:00Z",
        "expiresAt": "2026-01-01T00:00:00Z",
        "jurisdiction": "US",
        "schemaVersion": "1.0",
    }


def make_sqs_event(*details: dict) -> dict:
    """Build the SQS Records structure the Lambda handler expects."""
    import json

    records = []
    for i, detail in enumerate(details):
        records.append(
            {
                "messageId": f"msg-{i:03d}",
                "body": json.dumps({"detail": detail}),
                "receiptHandle": f"handle-{i:03d}",
            }
        )
    return {"Records": records}


def get_dynamo_item(table, cert_id: str) -> dict | None:
    resp = table.get_item(Key={"certId": cert_id, "SK": "#METADATA"})
    return resp.get("Item")


def drain_sqs(sqs_client, queue_url: str, max_messages: int = 10) -> list[dict]:
    """Receive all available messages from a queue and parse their bodies."""
    resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=0,
    )
    messages = resp.get("Messages", [])
    return [json.loads(m["Body"]) for m in messages]
