"""Section 10 Verification Suite: Managed MCP Audit Role, Cryptographic Receipts & Packaging."""

import json
from pathlib import Path
import pytest

from coordinator.receipt_archiver import (
    ExecutionReceipt,
    archive_receipt,
    generate_execution_receipt,
    list_local_receipts,
    verify_receipt_integrity,
)
from infra.ccloud.inspect_cluster import inspect_ccloud_cluster, sanitize_dict, sanitize_string


def test_execution_receipt_sha256_generation_and_hmac_signature():
    """Verify ExecutionReceipt computes valid SHA-256 hash, HMAC signature, and detects tampering."""
    receipt = generate_execution_receipt(
        task_id="task-audit-101",
        source_service="billing-service",
        target_service="orders-service",
        from_version=1,
        to_version=2,
        breaking_diff={"breaking_changes": ["amount type changed"]},
        test_results={"returncode": 0, "all_passed": True},
        approved_by="lead-architect",
        deployment_version=5,
        source_commit="abcdef123456",
        signing_key="test-operator-secret",
    )

    # 1. Verify hash and signature length and format
    assert len(receipt.receipt_sha256) == 64
    assert len(receipt.receipt_signature) == 64
    receipt_dict = receipt.model_dump()

    # 2. Verify intact receipt passes verification
    assert verify_receipt_integrity(receipt_dict, signing_key="test-operator-secret") is True

    # 3. Verify tampering is detected
    tampered_dict = dict(receipt_dict)
    tampered_dict["deployment_version"] = 999  # Tamper with version
    assert verify_receipt_integrity(tampered_dict) is False

    tampered_dict_2 = dict(receipt_dict)
    tampered_dict_2["approved_by"] = "malicious-actor"
    assert verify_receipt_integrity(tampered_dict_2) is False


@pytest.mark.asyncio
async def test_archive_receipt_local_persistence_and_non_blocking_s3(tmp_path):
    """Verify archive_receipt persists JSON locally and fails gracefully on S3 error without blocking."""
    receipt = generate_execution_receipt(
        task_id="task-audit-102",
        source_service="billing-service",
        target_service="orders-service",
        from_version=1,
        to_version=2,
        breaking_diff={},
        test_results={},
        approved_by="operator-1",
        deployment_version=2,
        source_commit="11223344",
    )

    # Test local archival with mock S3 upload failure (non-blocking)
    result = await archive_receipt(
        receipt=receipt,
        local_dir=tmp_path,
        upload_to_s3=True,
        s3_bucket="non-existent-bucket-for-test",
    )

    # Assert local file exists and matches
    local_file = Path(result["local_path"])
    assert local_file.exists()
    assert local_file.name == f"{receipt.receipt_id}.json"

    saved_data = json.loads(local_file.read_text(encoding="utf-8"))
    assert saved_data["receipt_id"] == receipt.receipt_id
    assert saved_data["receipt_sha256"] == receipt.receipt_sha256
    assert saved_data["receipt_signature"] == receipt.receipt_signature

    # Assert non-blocking S3 behavior
    assert result["s3_uploaded"] is False
    assert result["s3_error"] is not None


def test_list_local_receipts_reads_and_validates_files(tmp_path):
    """Verify list_local_receipts scans directory, computes validation status, and sorts."""
    r1 = generate_execution_receipt(
        task_id="task-1",
        source_service="billing-service",
        target_service="orders-service",
        from_version=1,
        to_version=2,
        breaking_diff={},
        test_results={},
        approved_by="op",
        deployment_version=1,
        source_commit="commit1",
        timestamp="2026-08-17T10:00:00Z",
    )
    r2 = generate_execution_receipt(
        task_id="task-2",
        source_service="billing-service",
        target_service="orders-service",
        from_version=2,
        to_version=3,
        breaking_diff={},
        test_results={},
        approved_by="op",
        deployment_version=2,
        source_commit="commit2",
        timestamp="2026-08-17T12:00:00Z",
    )

    (tmp_path / f"{r1.receipt_id}.json").write_text(r1.model_dump_json(), encoding="utf-8")
    (tmp_path / f"{r2.receipt_id}.json").write_text(r2.model_dump_json(), encoding="utf-8")

    # Add a tampered file
    tampered_data = r1.model_dump()
    tampered_data["approved_by"] = "hacker"
    tampered_file = tmp_path / "rcpt-tampered.json"
    tampered_file.write_text(json.dumps(tampered_data), encoding="utf-8")

    listed = list_local_receipts(tmp_path)
    assert len(listed) == 3

    # Most recent first
    assert listed[0]["receipt_id"] == r2.receipt_id
    assert listed[0]["_is_valid"] is True

    # Find the tampered one
    tampered_item = next(i for i in listed if i["receipt_id"] == r1.receipt_id and i["approved_by"] == "hacker")
    assert tampered_item["_is_valid"] is False


def test_cluster_inspector_sanitizes_credentials_and_truthful_verification():
    """Verify cluster inspector masks connection strings and accurately reports unverified status without live CLI."""
    raw_str = "postgresql://root:MySecretPassword123!@localhost:26257/defaultdb?sslmode=disable"
    sanitized = sanitize_string(raw_str)
    assert "MySecretPassword123!" not in sanitized
    assert "[REDACTED]" in sanitized

    raw_dict = {
        "cluster_name": "codeclaim-prod",
        "api_key": "secret-api-key-999",
        "nested": {
            "password": "db-password",
            "normal_field": "active",
        },
    }
    sanitized_dict = sanitize_dict(raw_dict)
    assert sanitized_dict["api_key"] == "[REDACTED]"
    assert sanitized_dict["nested"]["password"] == "[REDACTED]"
    assert sanitized_dict["nested"]["normal_field"] == "active"

    # Test inspect_ccloud_cluster truthful reporting
    evidence = inspect_ccloud_cluster("test-cluster", require_live=False)
    assert evidence["cluster_name"] == "test-cluster"
    assert "mcp_audit_role" in evidence
    # When ccloud CLI is not installed, it must truthfully report verified: False
    if not evidence["cli_available"]:
        assert evidence["verified"] is False


def test_provision_changefeed_sql_contains_mcp_audit_role_and_exact_schema():
    """Verify SQL provisioning script contains read-only MCP user and exact table & view names."""
    sql_path = Path(__file__).parent.parent / "infra" / "ccloud" / "provision_changefeed.sql"
    assert sql_path.exists()
    content = sql_path.read_text(encoding="utf-8")

    assert "mcp_audit_agent" in content
    assert "service_contracts" in content
    assert "service_contract_revisions" in content
    assert "semantic_memory" in content
    assert "contract_drift_audit" in content
    assert "contract_publication_audit" in content
    assert "GRANT SELECT ON TABLE contract_drift_audit TO mcp_audit_agent;" in content
    assert "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, DROP, ALTER" in content
    assert "CREATE CHANGEFEED FOR TABLE coordinator_outbox" in content
    # Verify NO plaintext passwords committed
    assert "ReadonlyMcpAuditPass2026!" not in content
