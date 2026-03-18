# Serverless Trade Certification Event Pipeline

This project implements a production-pattern event pipeline for processing trade certification lifecycle events — issued, revoked, and expiring. It is built on Amazon EventBridge, SQS, Lambda, and DynamoDB, and is designed for strong idempotency, durable retry, and operational observability. Every architectural decision prioritizes correctness over speed: certifications are compliance artifacts, so duplicate processing and silent failures are worse outcomes than latency.

---

## Architecture

```
Event Producers
      │ PutEvents
      ▼
EventBridge (Custom Bus)
  Rules: CertificationIssued | CertificationRevoked | CertificationExpiring
      │
      ▼
SQS: trade-cert-events          SQS: trade-cert-events-dlq
  VisibilityTimeout: 90s    ──►   MessageRetention: 14d
  MaxReceiveCount: 3               │
      │                            ▼
      │                    CloudWatch Alarm
      │                    ApproximateNumberOfMessages > 0
      ▼
Lambda: CertificationProcessor
  BatchSize: 10 | BisectBatchOnFunctionError | ReportBatchItemFailures
      │
      ├── DynamoDB: trade-certs (idempotency + state)
      ├── S3: archive (date-partitioned, compliance)
      └── EventBridge: downstream bus (enriched events)
```

Event producers call `PutEvents` on a custom EventBridge bus. Rules on that bus route `CertificationIssued`, `CertificationRevoked`, and `CertificationExpiring` events to the SQS queue. Lambda polls the queue in batches of 10. After three failed delivery attempts, messages are routed to the dead-letter queue and a CloudWatch alarm fires immediately.

---

## Idempotency State Machine

The `CertificationProcessor` uses a three-state machine stored in DynamoDB to gate reprocessing. The state key is the `certId` plus the event version.

| Prior state | Condition | Outcome |
|---|---|---|
| absent | first delivery | write PROCESSING |
| PROCESSING | same key | re-entry allowed (crash recovery) |
| FAILED | same key | re-entry allowed (ops replay) |
| COMPLETED | same key | IdempotencyConflict raised, silently skipped |
| any | different key | overwrite with new version |

A two-state gate (absent / present) cannot distinguish between "currently being processed" and "successfully completed". If Lambda crashes after writing the DynamoDB record but before completing the S3 archive write and the downstream EventBridge emission, the record exists in DynamoDB with no terminal status. Every subsequent retry sees a present record and skips the event permanently. The PROCESSING state signals "in flight" and permits re-entry. Only COMPLETED blocks reprocessing.

---

## Three Production Lessons

These patterns came out of a production event pipeline built at Deloitte.

**1. The idempotency gate must have a PROCESSING state**

Without it, a Lambda that crashes after writing to DynamoDB but before completing S3 and EventBridge emissions leaves a ghost record that blocks all future retries. The PROCESSING state signals "in flight" and allows re-entry. Only COMPLETED blocks reprocessing.

**2. ReportBatchItemFailures and BisectBatchOnFunctionError are both required**

`ReportBatchItemFailures` alone means a bug in your error-reporting code can still fail the whole batch. `BisectBatchOnFunctionError` halves the batch on unhandled exceptions to isolate poison pills. Together they prevent one bad record from burning the retry budget of nine good records in the same batch.

**3. DLQ alarm threshold must be zero, not a trend**

Setting the alarm at `ApproximateNumberOfMessages > 0` with `Statistic: Maximum` means any single DLQ arrival pages the on-call engineer. Averaging over time or setting a threshold of 5 or 10 will hide sporadic failures until they accumulate into an incident. One DLQ message means a broken event — treat it as P1.

---

## Key Engineering Decisions

### DynamoDB over Redis for idempotency

**Atomicity.** DynamoDB conditional writes (`ConditionExpression`) are atomic at the item level with no TTL race condition. Redis `SET NX` is atomic too, but updating a value and its TTL in the same operation requires a Lua script or a transaction — more moving parts and more failure modes.

**Durability.** DynamoDB is durable by default: multi-AZ replication, point-in-time recovery available. A Redis cache eviction or unexpected reboot can lose idempotency records while processing is in flight, causing duplicate downstream events with no audit trail.

**Audit trail.** DynamoDB items survive the processing window and carry `status`, `failureReason`, and `completedAt` fields that are queryable for compliance audits. Redis holds keys only until TTL expiry, with no history and no introspection beyond the current value.

**Operational simplicity.** One service handles state storage and idempotency. Adding Redis introduces a second stateful dependency with its own connection pooling, eviction policy, and failure modes inside a Lambda execution environment.

**The tradeoff.** Redis is 10-20x faster for simple key lookups: approximately 0.5ms versus approximately 5ms for DynamoDB single-digit P99. For this pipeline the difference is irrelevant — certifications are not latency-sensitive and the pipeline is already asynchronous. If this were a synchronous checkout flow requiring sub-1ms deduplication, Redis would be the right choice.

### Why moto instead of localstack or a real dev account

**No infrastructure required.** moto runs in-process. No Docker daemon, no port management, no localstack container to keep alive. Any developer or CI runner can execute `pytest` with zero AWS setup.

**Deterministic isolation.** Each test gets its own `mock_aws()` context with a clean state. Shared dev accounts accumulate stale state across runs; localstack containers can drift between restarts. moto gives a fresh environment on every test.

**Speed.** The 42 integration tests complete in approximately 35 seconds. Localstack cold-start alone often exceeds that. Real AWS integration tests introduce network round-trips and IAM propagation delays that make the feedback loop unusable for local development.

**The tradeoff.** moto does not replicate every AWS behavior. EventBridge routing fidelity, IAM enforcement, KMS encryption, and service quota errors are not modeled. For those edge cases, a separate end-to-end test suite runs against a sandboxed AWS account in CI — gated behind the moto suite, not on every developer commit.

**Module-level client problem.** Production modules create boto3 clients at import time. The conftest patches module-level objects (`_table`, `_s3`, `_eb`) with fresh moto-backed instances and sets `os.environ` at conftest module level before any test imports fire. This pattern is documented in `tests/integration/conftest.py`.

### What would break at 10x load

Current assumptions: approximately 1,000 certification events per day. At 10x that is approximately 10,000 per day, or roughly 7 events per minute at peak.

**DynamoDB on-demand capacity.** No change needed up to approximately 40,000 WCU bursts. At 100x (100,000 events per day), switch to provisioned capacity with auto-scaling to avoid cold-partition throttling and reduce cost at sustained throughput.

**Lambda concurrency.** `MaximumConcurrency: 50` on the event source mapping is the first throttle point. At 10x with burst ingestion, SQS will buffer (which is correct behavior), but Lambda may hit the reserved concurrency ceiling. Raise to 200 and add a reserved concurrency alarm to detect ceiling hits before they cause visible latency.

**SQS VisibilityTimeout math.** At BatchSize=10 and MaxConcurrency=50, peak throughput is 500 records per invocation cycle. A VisibilityTimeout of 90 seconds gives 333 invocations before a message becomes visible again to another consumer. This is acceptable at 10x. Recalculate explicitly at 100x.

**S3 put throughput.** No issue at 10x or 100x. S3 scales to thousands of PUT requests per second per prefix. The date-partitioned key scheme (`year=/month=/day=/`) distributes load across prefixes naturally.

**EventBridge PutEvents.** The default limit is 10,000 events per second per bus. At 10x this pipeline never approaches that limit. At 1,000x, consider batching: `PutEvents` accepts up to 10 entries per call.

**The real bottleneck: DynamoDB hot partitions.** If a single `certId` is re-delivered at high rate — for example, a broken producer replaying the same event in a loop — all writes for that key hit the same partition. The `ConditionalCheckFailedException` path is fast, but a thundering herd on one key can exhaust partition-level write capacity. Mitigation: add exponential jitter to the SQS visibility extension on conflict, and monitor `ConditionalCheckFailedRequests` as a dedicated CloudWatch metric.

---

## Getting Started

Install dependencies:

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

Run the unit tests:

```bash
pytest tests/unit -v
```

Run the integration tests (uses moto, no AWS credentials required):

```bash
pytest tests/integration -v
```

Run the full suite with coverage:

```bash
pytest tests/ --cov=src --cov-report=term-missing
```

Required environment variables for local runs outside of moto:

```
DYNAMODB_TABLE=trade-certs
ARCHIVE_BUCKET=trade-cert-archive
DOWNSTREAM_EVENT_BUS_ARN=arn:aws:events:us-east-1:ACCOUNT_ID:event-bus/trade-downstream
```

These variables are set automatically in the moto-backed test suite via `tests/integration/conftest.py`.

---

## Project Structure

```
serverless-event-pipeline/
├── src/
│   └── certification_processor/
│       ├── handler.py          # Lambda entry point; batch loop with per-item error handling
│       ├── idempotency.py      # DynamoDB conditional write gate; three-state machine
│       ├── archiver.py         # S3 archive writes with date-partitioned keys
│       └── emitter.py          # EventBridge downstream emission of enriched events
├── tests/
│   ├── unit/                   # Mock-patched unit tests; no AWS calls
│   └── integration/
│       ├── conftest.py         # moto setup; module-level client patching; env vars
│       └── ...                 # 42 moto-backed integration tests
├── requirements.txt            # Runtime dependencies (boto3, etc.)
├── requirements-dev.txt        # Test dependencies (pytest, moto, coverage)
└── README.md                   # This file
```
