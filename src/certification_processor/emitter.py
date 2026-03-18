"""
Emits downstream EventBridge events after successful certification processing.

Maps incoming detail-type to an enriched downstream event so consumers
don't depend on the upstream schema directly.
"""

import json
import logging
import os

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

_EVENT_BUS_ARN = os.environ["DOWNSTREAM_EVENT_BUS_ARN"]
_SOURCE = "trade.certification.processor"

_eb = boto3.client(
    "events",
    config=Config(retries={"max_attempts": 3, "mode": "adaptive"}),
)

# Maps inbound detail-type → outbound detail-type
_DETAIL_TYPE_MAP = {
    "CertificationIssued": "CertificationActivated",
    "CertificationRevoked": "CertificationDeactivated",
    "CertificationExpiring": "CertificationExpiryWarning",
}


def emit_downstream(detail: dict) -> None:
    """
    Puts a single enriched event on the downstream bus.
    Raises on failure — caller will mark_failed and return the record to SQS.
    """
    inbound_type = detail.get("detail-type", "CertificationIssued")
    outbound_type = _DETAIL_TYPE_MAP.get(inbound_type, inbound_type)

    response = _eb.put_events(
        Entries=[
            {
                "EventBusName": _EVENT_BUS_ARN,
                "Source": _SOURCE,
                "DetailType": outbound_type,
                "Detail": json.dumps(
                    {
                        "certId": detail["certId"],
                        "tradeEntityId": detail["tradeEntityId"],
                        "certType": detail["certType"],
                        "jurisdiction": detail.get("jurisdiction"),
                        "issuedAt": detail.get("issuedAt"),
                        "expiresAt": detail.get("expiresAt"),
                        "idempotencyKey": detail["idempotencyKey"],
                        "schemaVersion": detail.get("schemaVersion", "1.0"),
                    }
                ),
            }
        ]
    )

    failed = response.get("FailedEntryCount", 0)
    if failed:
        error = response["Entries"][0].get("ErrorMessage", "unknown")
        raise RuntimeError(f"EventBridge PutEvents failed: {error}")

    logger.debug(
        "Downstream event emitted",
        extra={"certId": detail["certId"], "detailType": outbound_type},
    )
