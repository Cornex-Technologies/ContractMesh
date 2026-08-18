"""Cryptographic Execution Receipt Archiver & Tamper-Evident Audit Trail.

Generates SHA-256 cryptographic execution receipts and HMAC signatures recording contract mutations,
breaking diffs, test suite verification evidence, human approvals, and deployment cutovers.
Persists receipts locally and asynchronously archives them to S3 via thread offloading with non-blocking resilience.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from coordinator.config import settings

logger = logging.getLogger(__name__)

DEFAULT_RECEIPTS_DIR = Path(__file__).parent.parent / "receipts"


class ExecutionReceipt(BaseModel):
    """Immutable, tamper-evident audit receipt for an agent code repair and deployment cycle."""

    receipt_id: str = Field(default_factory=lambda: f"rcpt-{uuid.uuid4().hex[:12]}")
    task_id: str = Field(..., description="Active agent reconciliation task ID")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 UTC timestamp of receipt creation",
    )
    source_service: str = Field(..., description="Service that introduced the contract revision (e.g. billing-service)")
    target_service: str = Field(..., description="Consumer service that adapted its code (e.g. orders-service)")
    from_version: int = Field(..., description="Prior contract schema version")
    to_version: int = Field(..., description="New contract schema version")
    breaking_diff: dict[str, Any] = Field(default_factory=dict, description="Structural diff payload")
    test_results: dict[str, Any] = Field(default_factory=dict, description="Pytest test gate execution evidence")
    approved_by: str = Field(..., description="Human operator who signed off on the reconciliation")
    deployment_version: int = Field(..., description="Allocated monotonic reload version")
    source_commit: str = Field(..., description="Git commit SHA of the promoted candidate")
    receipt_sha256: str = Field(default="", description="Cryptographic SHA-256 digest over canonical payload")
    receipt_signature: str = Field(default="", description="HMAC-SHA256 signature using coordinator key")

    def canonical_payload_str(self) -> str:
        """Return deterministic, sorted canonical JSON representation of receipt fields."""
        payload = {
            "receipt_id": self.receipt_id,
            "task_id": self.task_id,
            "timestamp": self.timestamp,
            "source_service": self.source_service,
            "target_service": self.target_service,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "breaking_diff": self.breaking_diff,
            "test_results": self.test_results,
            "approved_by": self.approved_by,
            "deployment_version": self.deployment_version,
            "source_commit": self.source_commit,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def compute_sha256(self) -> str:
        """Compute deterministic SHA-256 hash over canonical JSON representation."""
        return hashlib.sha256(self.canonical_payload_str().encode("utf-8")).hexdigest()

    def compute_hmac_signature(self, secret_key: Optional[str] = None) -> str:
        """Compute HMAC-SHA256 signature using secret key."""
        key_str = secret_key or settings.coordinator_api_key or settings.changefeed_webhook_secret
        if not key_str:
            if not settings.is_demo_mode:
                raise ValueError("A secure coordinator signing key is required to generate execution receipts outside demo mode.")
            key_str = "demo-signing-secret"
        key = key_str.encode("utf-8")
        return hmac.new(key, self.canonical_payload_str().encode("utf-8"), hashlib.sha256).hexdigest()


def generate_execution_receipt(
    task_id: str,
    source_service: str,
    target_service: str,
    from_version: int,
    to_version: int,
    breaking_diff: dict[str, Any],
    test_results: dict[str, Any],
    approved_by: str,
    deployment_version: int,
    source_commit: str,
    receipt_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    signing_key: Optional[str] = None,
) -> ExecutionReceipt:
    """Construct an ExecutionReceipt with cryptographic SHA-256 digest and HMAC signature."""
    kwargs: dict[str, Any] = {
        "task_id": task_id,
        "source_service": source_service,
        "target_service": target_service,
        "from_version": from_version,
        "to_version": to_version,
        "breaking_diff": breaking_diff,
        "test_results": test_results,
        "approved_by": approved_by,
        "deployment_version": deployment_version,
        "source_commit": source_commit,
    }
    if receipt_id:
        kwargs["receipt_id"] = receipt_id
    if timestamp:
        kwargs["timestamp"] = timestamp

    receipt = ExecutionReceipt(**kwargs)
    receipt.receipt_sha256 = receipt.compute_sha256()
    receipt.receipt_signature = receipt.compute_hmac_signature(secret_key=signing_key)
    return receipt


def verify_receipt_integrity(receipt_data: dict[str, Any], signing_key: Optional[str] = None) -> bool:
    """Verify that the cryptographic SHA-256 hash and HMAC signature match the receipt contents."""
    given_hash = receipt_data.get("receipt_sha256")
    if not given_hash:
        return False

    temp_receipt = ExecutionReceipt(
        receipt_id=receipt_data.get("receipt_id", ""),
        task_id=receipt_data.get("task_id", ""),
        timestamp=receipt_data.get("timestamp", ""),
        source_service=receipt_data.get("source_service", ""),
        target_service=receipt_data.get("target_service", ""),
        from_version=receipt_data.get("from_version", 1),
        to_version=receipt_data.get("to_version", 2),
        breaking_diff=receipt_data.get("breaking_diff", {}),
        test_results=receipt_data.get("test_results", {}),
        approved_by=receipt_data.get("approved_by", ""),
        deployment_version=receipt_data.get("deployment_version", 1),
        source_commit=receipt_data.get("source_commit", ""),
        receipt_sha256="",
        receipt_signature="",
    )
    expected_hash = temp_receipt.compute_sha256()
    if not hmac.compare_digest(expected_hash, given_hash):
        return False

    # Check HMAC signature
    effective_key = signing_key or settings.coordinator_api_key or settings.changefeed_webhook_secret
    if effective_key or not settings.is_demo_mode:
        given_sig = receipt_data.get("receipt_signature")
        if not given_sig:
            return False
        try:
            expected_sig = temp_receipt.compute_hmac_signature(secret_key=effective_key)
            return hmac.compare_digest(expected_sig, given_sig)
        except Exception:
            return False

    return True


def _sync_write_local(file_path: Path, content: str) -> None:
    """Synchronous file writer helper for thread offloading."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")


def _sync_put_s3_object(bucket: str, key: str, body: bytes, metadata: dict[str, str]) -> None:
    """Synchronous S3 put_object helper for thread offloading."""
    import boto3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata=metadata,
    )


async def archive_receipt(
    receipt: ExecutionReceipt,
    local_dir: Optional[Path | str] = None,
    upload_to_s3: bool = True,
    s3_bucket: Optional[str] = None,
) -> dict[str, Any]:
    """Persist the execution receipt locally and asynchronously upload to S3 via threadpool offloading.

    Non-blocking guarantee: S3 network errors or unconfigured AWS credentials
    will be logged but will NEVER block the asyncio event loop or fail the transaction.
    """
    target_dir = Path(local_dir) if local_dir else DEFAULT_RECEIPTS_DIR
    file_path = target_dir / f"{receipt.receipt_id}.json"
    receipt_json = receipt.model_dump_json(indent=2)

    # Thread-offloaded local disk write
    await asyncio.to_thread(_sync_write_local, file_path, receipt_json)
    logger.info("Durable audit receipt persisted locally to %s (SHA-256: %s)", file_path, receipt.receipt_sha256[:12])

    bucket = s3_bucket or getattr(settings, "s3_receipt_bucket", None) or "codeclaim-audit-receipts"
    s3_uploaded = False
    s3_error = None

    if upload_to_s3:
        try:
            s3_key = f"receipts/{receipt.receipt_id}.json"
            metadata = {
                "receipt-sha256": receipt.receipt_sha256,
                "task-id": receipt.task_id,
                "source-service": receipt.source_service,
                "deployment-version": str(receipt.deployment_version),
            }
            # Thread-offloaded non-blocking S3 call
            await asyncio.to_thread(_sync_put_s3_object, bucket, s3_key, receipt_json.encode("utf-8"), metadata)
            s3_uploaded = True
            logger.info("Audit receipt %s archived to S3 bucket %s (key=%s)", receipt.receipt_id, bucket, s3_key)
        except Exception as ex:
            s3_error = str(ex)
            logger.warning(
                "Non-blocking S3 upload skipped for receipt %s: %s (Local file intact)",
                receipt.receipt_id,
                ex,
            )

    return {
        "receipt_id": receipt.receipt_id,
        "local_path": str(file_path),
        "receipt_sha256": receipt.receipt_sha256,
        "receipt_signature": receipt.receipt_signature,
        "s3_bucket": bucket if s3_uploaded else None,
        "s3_uploaded": s3_uploaded,
        "s3_error": s3_error,
    }


def list_local_receipts(local_dir: Optional[Path | str] = None, signing_key: Optional[str] = None) -> list[dict[str, Any]]:
    """Scan and list all local cryptographic audit receipts, verifying integrity."""
    target_dir = Path(local_dir) if local_dir else DEFAULT_RECEIPTS_DIR
    if not target_dir.exists():
        return []

    receipts: list[dict[str, Any]] = []
    for file in target_dir.glob("rcpt-*.json"):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            data["_is_valid"] = verify_receipt_integrity(data, signing_key=signing_key)
            receipts.append(data)
        except Exception as ex:
            logger.warning("Error reading receipt file %s: %s", file, ex)

    # Sort descending by timestamp
    receipts.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    return receipts
