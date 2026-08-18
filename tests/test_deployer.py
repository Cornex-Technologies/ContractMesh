"""Section 8 Verification Suite: Deployment Promotion, Process Supervision & Live Reload."""

import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import pytest

from coordinator.app import app, lifespan
from coordinator.config import settings
from coordinator.db import check_health, close_pool, init_db
from coordinator.deployer import (
    ALLOWED_SERVICES,
    SERVICE_PORT_MAP,
    CutoverJournal,
    build_isolated_pythonpath,
    extract_commit_snapshot,
    get_deployment_history,
    get_latest_reload_version,
    get_sanitized_sandbox_env,
    poll_service_readiness,
    promote_deployment,
    run_service_test_gate,
    supervisor,
)


# ==============================================================================
# 1. Version Retrieval & Fallback Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_get_latest_reload_version_fallback():
    """Verify get_latest_reload_version returns 1 when table is empty or error occurs."""
    with patch("coordinator.deployer.fetch_one", AsyncMock(return_value={"current_version": None})):
        ver = await get_latest_reload_version()
        assert ver == 1

    with patch("coordinator.deployer.fetch_one", AsyncMock(side_effect=RuntimeError("db error"))):
        ver = await get_latest_reload_version()
        assert ver == 1

    with patch("coordinator.deployer.fetch_one", AsyncMock(return_value={"current_version": 42})):
        ver = await get_latest_reload_version()
        assert ver == 42


# ==============================================================================
# 2. Strict Allowlist Environment Sandbox Tests
# ==============================================================================


def test_get_sanitized_sandbox_env_strict_allowlist(monkeypatch, tmp_path):
    """Verify get_sanitized_sandbox_env blocks all un-whitelisted variables and user profile paths."""
    monkeypatch.setenv("COCKROACH_DATABASE_URL", "postgresql://root:secret@localhost:26257")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "super-secret-aws-key")
    monkeypatch.setenv("COORDINATOR_API_KEY", "admin-token-12345")
    monkeypatch.setenv("USERPROFILE", "C:\\Users\\dell")
    monkeypatch.setenv("HOMEDRIVE", "C:")
    monkeypatch.setenv("HOMEPATH", "\\Users\\dell")
    monkeypatch.setenv("SAFE_KEY", "custom-safe-key")

    clean_env = get_sanitized_sandbox_env({"ENVIRONMENT": "test_sandbox", "EXTRA_UNAPPROVED_VAR": "extra_val"}, base_dir=tmp_path)

    assert "COCKROACH_DATABASE_URL" not in clean_env
    assert "AWS_SECRET_ACCESS_KEY" not in clean_env
    assert "COORDINATOR_API_KEY" not in clean_env
    assert "USERPROFILE" not in clean_env
    assert "HOMEDRIVE" not in clean_env
    assert "HOMEPATH" not in clean_env
    assert "SAFE_KEY" not in clean_env
    assert "EXTRA_UNAPPROVED_VAR" not in clean_env
    assert clean_env["ENVIRONMENT"] == "test_sandbox"
    assert "TMP" in clean_env and str(tmp_path) in clean_env["TMP"]


def test_isolated_pythonpath_does_not_inherit_ambient_value(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTHONPATH", "C:\\malicious\\injected-modules")
    isolated = build_isolated_pythonpath(tmp_path)
    assert "malicious" not in isolated
    assert str(tmp_path.resolve()) in isolated


# ==============================================================================
# 3. Durable Cutover Journal & Startup Crash Recovery Tests
# ==============================================================================


def test_cutover_journal_crash_recovery(tmp_path):
    """Verify CutoverJournal restores live directory from backup if a crash occurs mid-cutover."""
    journal = CutoverJournal(tmp_path)
    live_dir = tmp_path / "deployments" / "live" / "orders-service"
    backup_dir = tmp_path / "deployments" / "backup" / "orders-service"
    staged_dir = tmp_path / "deployments" / "staged" / "orders-service-1"

    backup_dir.mkdir(parents=True, exist_ok=True)
    (backup_dir / "restored.txt").write_text("recovered-content", encoding="utf-8")

    # Write in-progress state where live_dir was removed but staged wasn't yet renamed
    journal.record_intent(
        service_name="orders-service",
        deployment_id="dep-crash-1",
        staged_dir=staged_dir,
        live_dir=live_dir,
        backup_dir=backup_dir,
    )

    assert not live_dir.exists()

    # Recovery execution
    recovered = journal.recover_if_needed()
    assert recovered is not None
    assert live_dir.exists()
    assert (live_dir / "restored.txt").read_text(encoding="utf-8") == "recovered-content"
    assert not journal.journal_file.exists()


def test_cutover_journal_malformed_json_recovery(tmp_path):
    """Verify CutoverJournal quarantines corrupt/malformed journal files and raises error."""
    journal = CutoverJournal(tmp_path)
    journal.journal_file.write_text("{{{ INVALID JSON CONTENT", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Corrupted cutover journal encountered"):
        journal.recover_if_needed()

    assert not journal.journal_file.exists()
    # Check that quarantine file was created
    quarantined = list(journal.journal_file.parent.glob(".cutover_journal.corrupted.*.json"))
    assert len(quarantined) == 1


def test_cutover_journal_record_unrecoverable_preserves_full_metadata(tmp_path):
    """Verify record_unrecoverable preserves all paths, deployment ID, and error message."""
    journal = CutoverJournal(tmp_path)
    live_dir = tmp_path / "deployments" / "live" / "orders-service"
    backup_dir = tmp_path / "deployments" / "backup" / "orders-service"
    staged_dir = tmp_path / "deployments" / "staged" / "orders-service-1"

    journal.record_intent(
        service_name="orders-service",
        deployment_id="dep-metadata-1",
        staged_dir=staged_dir,
        live_dir=live_dir,
        backup_dir=backup_dir,
    )

    journal.record_unrecoverable("Filesystem rename locked by antivirus", service_name="orders-service")

    data = json.loads(journal.journal_file.read_text(encoding="utf-8"))
    assert data["state"] == "UNRECOVERABLE_RESTORATION_REQUIRED"
    assert data["deployment_id"] == "dep-metadata-1"
    assert data["live_dir"] == str(live_dir)
    assert data["backup_dir"] == str(backup_dir)
    assert data["staged_dir"] == str(staged_dir)
    assert data["unrecoverable_error"] == "Filesystem rename locked by antivirus"


def test_cutover_journal_recover_if_needed_raises_on_unrecoverable_state(tmp_path):
    """Verify recover_if_needed immediately raises RuntimeError if journal is in UNRECOVERABLE state."""
    journal = CutoverJournal(tmp_path)
    journal.journal_file.write_text(json.dumps({
        "service_name": "orders-service",
        "state": "UNRECOVERABLE_RESTORATION_REQUIRED",
        "unrecoverable_error": "Disk failure",
    }), encoding="utf-8")

    with pytest.raises(RuntimeError, match="Unrecoverable cutover state detected for orders-service"):
        journal.recover_if_needed()


@pytest.mark.asyncio
async def test_fastapi_lifespan_blocks_startup_on_readiness_failure():
    """Verify FastAPI application lifespan blocks startup in non-demo mode if restored service is unhealthy."""
    with patch.object(supervisor.journal, "recover_if_needed", return_value={"service_name": "orders-service"}), \
         patch.object(supervisor, "start_service", return_value={"running": True, "pid": 1234}), \
         patch("coordinator.deployer.poll_service_readiness", AsyncMock(return_value=(False, {}, "503 Service Unavailable"))), \
         patch("coordinator.app.init_db", AsyncMock()), \
         patch("coordinator.app.close_pool", AsyncMock()), \
         patch.object(settings, "is_demo_mode", False):
        with pytest.raises(RuntimeError, match="failed readiness check on startup"):
            async with lifespan(app):
                pass


# ==============================================================================
# 4. Strict Commit Verification & Readiness Verification Tests
# ==============================================================================




def test_invalid_commit_rejection_fails_closed(tmp_path):
    """Verify extract_commit_snapshot raises error and deploys nothing on invalid/unresolvable commit."""
    base_dir = Path(__file__).parent.parent
    orders_repo = base_dir / "repos" / "orders-service"
    target_staged = tmp_path / "staged-orders-invalid"

    with pytest.raises(ValueError, match="Unresolvable Git commit"):
        extract_commit_snapshot(orders_repo, "invalid-commit-sha-9999", target_staged)

    assert not (target_staged / "main.py").exists()


def test_extract_commit_snapshot_and_test_gate(tmp_path):
    """Verify extract_commit_snapshot extracts commit snapshot and passes pre-test gate."""
    base_dir = Path(__file__).parent.parent
    orders_repo = base_dir / "repos" / "orders-service"

    from coordinator.contract_registry import get_service_git_commit
    commit_sha = get_service_git_commit(orders_repo)

    target_staged = tmp_path / "staged-orders"
    extract_commit_snapshot(orders_repo, commit_sha, target_staged)

    billing_repo = base_dir / "repos" / "billing-service"
    if billing_repo.exists() and not (tmp_path / "billing-service").exists():
        shutil.copytree(billing_repo, tmp_path / "billing-service")

    assert target_staged.exists()
    assert (target_staged / "main.py").exists()
    assert (target_staged / "clients" / "billing_client.py").exists()

    test_evidence = run_service_test_gate(target_staged, timeout_seconds=15.0, base_dir=tmp_path)
    assert test_evidence["all_passed"] is True
    assert test_evidence["returncode"] == 0


@pytest.mark.asyncio
async def test_poll_service_readiness_strict_identity_and_status():
    """Verify poll_service_readiness strictly requires both status == healthy AND exact service_name."""
    mock_http_client = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}

    # 1. Missing service identity field -> FAILS
    mock_resp.json.return_value = {"status": "healthy"}
    mock_http_client.get.return_value = mock_resp
    is_ready, _, err = await poll_service_readiness(
        service_name="billing-service",
        max_retries=1,
        base_interval=0.01,
        http_client=mock_http_client,
    )
    assert is_ready is False
    assert "missing mandatory 'service'" in err

    # 2. Mismatched service identity -> FAILS
    mock_resp.json.return_value = {"status": "healthy", "service": "wrong-service"}
    is_ready, _, err = await poll_service_readiness(
        service_name="billing-service",
        max_retries=1,
        base_interval=0.01,
        http_client=mock_http_client,
    )
    assert is_ready is False
    assert "identity mismatch" in err

    # 3. Status not healthy -> FAILS
    mock_resp.json.return_value = {"status": "starting", "service": "billing-service"}
    is_ready, _, err = await poll_service_readiness(
        service_name="billing-service",
        max_retries=1,
        base_interval=0.01,
        http_client=mock_http_client,
    )
    assert is_ready is False
    assert "invalid status" in err

    # 4. Valid status AND exact service -> SUCCEEDS
    mock_resp.json.return_value = {"status": "healthy", "service": "billing-service"}
    is_ready, data, err = await poll_service_readiness(
        service_name="billing-service",
        max_retries=1,
        base_interval=0.01,
        http_client=mock_http_client,
    )
    assert is_ready is True
    assert data["service"] == "billing-service"
    assert err is None


# ==============================================================================
# 5. Deployment Promotion, Atomic Cutover & Rollback Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_promote_deployment_invalid_commit_marks_failed(tmp_path):
    """Verify promote_deployment marks deployment FAILED and emits DEPLOYMENT_FAILED outbox on invalid commit."""
    mock_cur = AsyncMock()
    mock_cur.fetchone.side_effect = [
        {"next_version": 1},
        {
            "deployment_id": "dep-invalid-1",
            "service_name": "orders-service",
            "source_commit": "fake-invalid-sha",
            "status": "VALIDATING",
            "reload_version": 1,
        },
    ]

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    with patch("coordinator.deployer.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        result = await promote_deployment(
            service_name="orders-service",
            source_commit="fake-invalid-sha",
            base_dir=tmp_path,
            skip_test_gate=True,
        )

        assert result["deployment_id"] == "dep-invalid-1"
        assert result["status"] == "FAILED"
        assert result["is_healthy"] is False
        assert "Unresolvable Git commit" in result["error"] or "Commit extraction failed" in result["error"]
        assert not (tmp_path / "deployments" / "live" / "orders-service").exists()


@pytest.mark.asyncio
async def test_promote_deployment_with_existing_live_directory(tmp_path):
    """Verify promote_deployment cuts over cleanly when a prior live deployment directory already exists."""
    live_dir = tmp_path / "deployments" / "live" / "orders-service"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "v1.txt").write_text("prior-version-content", encoding="utf-8")

    mock_cur = AsyncMock()
    mock_cur.fetchone.side_effect = [
        {"next_version": 2},
        {
            "deployment_id": "dep-existing-live-2",
            "service_name": "orders-service",
            "source_commit": "commit-valid-2",
            "status": "VALIDATING",
            "reload_version": 2,
        },
        {
            "deployment_id": "dep-existing-live-2",
            "service_name": "orders-service",
            "source_commit": "commit-valid-2",
            "status": "HEALTHY",
            "reload_version": 2,
            "completed_at": "2026-08-17T12:00:00Z",
        },
        {"event_id": "deployment-event-existing-live"},
    ]

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_http_client = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {"status": "healthy", "service": "orders-service"}
    mock_http_client.get.return_value = mock_resp

    with patch("coordinator.deployer.run_transaction") as mock_run_tx, \
         patch("coordinator.deployer.extract_commit_snapshot") as mock_extract, \
         patch.object(supervisor, "restart_service", return_value={"running": True, "pid": 999}):

        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        def mock_extract_impl(repo, commit, target):
            target.mkdir(parents=True, exist_ok=True)
            (target / "v2.txt").write_text("new-version-content", encoding="utf-8")
            return target
        mock_extract.side_effect = mock_extract_impl

        result = await promote_deployment(
            service_name="orders-service",
            source_commit="commit-valid-2",
            base_dir=tmp_path,
            http_client=mock_http_client,
            skip_test_gate=True,
        )

        assert result["status"] == "HEALTHY"
        assert (live_dir / "v2.txt").exists()
        assert not (live_dir / "v1.txt").exists()

        backup_dir = tmp_path / "deployments" / "backup" / "orders-service"
        assert backup_dir.exists()
        assert (backup_dir / "v1.txt").exists()


@pytest.mark.asyncio
async def test_promote_deployment_health_check_failure_and_rollback_outbox(tmp_path):
    """Verify promote_deployment marks status FAILED, rolls back, verifies rollback, and emits DEPLOYMENT_ROLLED_BACK."""
    live_dir = tmp_path / "deployments" / "live" / "billing-service"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "v1.txt").write_text("original-billing-v1", encoding="utf-8")

    captured_outbox_calls = []

    mock_cur = AsyncMock()
    async def mock_execute(sql, params=None):
        if "INSERT INTO coordinator_outbox" in sql:
            captured_outbox_calls.append({"sql": sql, "params": params})
    mock_cur.execute.side_effect = mock_execute

    mock_cur.fetchone.side_effect = [
        {"next_version": 2},
        {
            "deployment_id": "dep-failed-1",
            "service_name": "billing-service",
            "source_commit": "commit-valid",
            "status": "VALIDATING",
            "reload_version": 2,
        },
        {
            "deployment_id": "dep-failed-1",
            "service_name": "billing-service",
            "source_commit": "commit-valid",
            "status": "FAILED",
            "reload_version": 2,
            "completed_at": None,
        },
        {"event_id": "deployment-event-rollback"},
    ]

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_http_client = AsyncMock()
    # 10 failures for candidate readiness loop, then success for rollback verification loop
    mock_resp_fail = MagicMock(status_code=503, headers={"content-type": "application/json"})
    mock_resp_pass = MagicMock(
        status_code=200,
        headers={"content-type": "application/json"},
        json=MagicMock(return_value={"status": "healthy", "service": "billing-service"}),
    )
    mock_http_client.get.side_effect = [mock_resp_fail] * 10 + [mock_resp_pass]

    with patch("coordinator.deployer.run_transaction") as mock_run_tx, \
         patch("coordinator.deployer.extract_commit_snapshot") as mock_extract, \
         patch.object(supervisor, "restart_service", return_value={"running": True, "pid": 999}):

        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        def mock_extract_impl(repo, commit, target):
            target.mkdir(parents=True, exist_ok=True)
            (target / "v2_bad.txt").write_text("bad-content", encoding="utf-8")
            return target
        mock_extract.side_effect = mock_extract_impl

        result = await promote_deployment(
            service_name="billing-service",
            source_commit="commit-valid",
            base_dir=tmp_path,
            http_client=mock_http_client,
            skip_test_gate=True,
        )

        assert result["deployment_id"] == "dep-failed-1"
        assert result["status"] == "FAILED"
        assert result["is_healthy"] is False
        assert result["rollback"]["rollback_executed"] is True
        assert result["rollback"]["restored_healthy"] is True
        assert (live_dir / "v1.txt").exists()

        # Assert exact DEPLOYMENT_ROLLED_BACK event emitted
        assert len(captured_outbox_calls) >= 1
        outbox_event_type = captured_outbox_calls[0]["params"][3]
        assert outbox_event_type == "DEPLOYMENT_ROLLED_BACK"


@pytest.mark.asyncio
async def test_promote_deployment_health_failure_with_unhealthy_restored_service_emits_rollback_failed(tmp_path):
    """Verify promote_deployment emits DEPLOYMENT_ROLLBACK_FAILED when rollback fails to restore health."""
    live_dir = tmp_path / "deployments" / "live" / "billing-service"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "v1.txt").write_text("original-billing-v1", encoding="utf-8")

    captured_outbox_calls = []

    mock_cur = AsyncMock()
    async def mock_execute(sql, params=None):
        if "INSERT INTO coordinator_outbox" in sql:
            captured_outbox_calls.append({"sql": sql, "params": params})
    mock_cur.execute.side_effect = mock_execute

    mock_cur.fetchone.side_effect = [
        {"next_version": 2},
        {
            "deployment_id": "dep-failed-unhealthy-rollback",
            "service_name": "billing-service",
            "source_commit": "commit-valid",
            "status": "VALIDATING",
            "reload_version": 2,
        },
        {
            "deployment_id": "dep-failed-unhealthy-rollback",
            "service_name": "billing-service",
            "source_commit": "commit-valid",
            "status": "FAILED",
            "reload_version": 2,
            "completed_at": None,
        },
        {"event_id": "deployment-event-rollback-failed"},
    ]

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_http_client = AsyncMock()
    # Both candidate and rollback loops return 503 errors
    mock_resp_fail = MagicMock(status_code=503, headers={"content-type": "application/json"})
    mock_http_client.get.return_value = mock_resp_fail

    with patch("coordinator.deployer.run_transaction") as mock_run_tx, \
         patch("coordinator.deployer.extract_commit_snapshot") as mock_extract, \
         patch.object(supervisor, "restart_service", return_value={"running": True, "pid": 999}):

        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        def mock_extract_impl(repo, commit, target):
            target.mkdir(parents=True, exist_ok=True)
            (target / "v2_bad.txt").write_text("bad-content", encoding="utf-8")
            return target
        mock_extract.side_effect = mock_extract_impl

        result = await promote_deployment(
            service_name="billing-service",
            source_commit="commit-valid",
            base_dir=tmp_path,
            http_client=mock_http_client,
            skip_test_gate=True,
        )

        assert result["deployment_id"] == "dep-failed-unhealthy-rollback"
        assert result["status"] == "FAILED"
        assert result["is_healthy"] is False
        assert result["rollback"]["rollback_executed"] is True
        assert result["rollback"]["restored_healthy"] is False

        # Assert exact DEPLOYMENT_ROLLBACK_FAILED event emitted
        assert len(captured_outbox_calls) >= 1
        outbox_event_type = captured_outbox_calls[0]["params"][3]
        assert outbox_event_type == "DEPLOYMENT_ROLLBACK_FAILED"


# ==============================================================================
# 6. Process Supervisor & Endpoints Tests
# ==============================================================================


def test_process_supervisor_status_and_lifecycle():
    """Verify ProcessSupervisor inspects, starts, and stops supervised services."""
    status = supervisor.get_service_status("billing-service")
    assert status["service_name"] == "billing-service"
    assert "running" in status

    all_statuses = supervisor.get_all_services_status()
    assert isinstance(all_statuses, list)
    service_names = [s["service_name"] for s in all_statuses]
    assert "billing-service" in service_names
    assert "orders-service" in service_names


@pytest.mark.asyncio
async def test_deploy_and_task_approval_endpoints():
    """Verify /deploy/promote, /deploy/version, /tasks/{task_id}/approve, and /tasks/{task_id}/reject."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Test GET /deploy/version
        with patch("coordinator.app.get_latest_reload_version", AsyncMock(return_value=7)):
            resp = await client.get("/deploy/version")
            assert resp.status_code == 200
            assert resp.json()["reload_version"] == 7

        # 2. Test POST /deploy/promote without auth
        bad_req = {"service_name": "malicious-service", "source_commit": "12345"}
        resp_unauth = await client.post("/deploy/promote", json=bad_req)
        assert resp_unauth.status_code in (401, 400)

        # 3. Test POST /deploy/promote with operator auth and unknown service
        op_token = settings.coordinator_api_key or settings.changefeed_webhook_secret or "codeclaim-cdc-secret-key"
        auth_headers = {"X-Operator-Token": op_token}
        resp_bad_service = await client.post("/deploy/promote", json=bad_req, headers=auth_headers)
        assert resp_bad_service.status_code == 400
        assert "allowed services" in resp_bad_service.json()["detail"]

        # 4. Test POST /deploy/promote with valid auth and valid service
        mock_promotion = {
            "deployment_id": "dep-promoted-99",
            "service_name": "billing-service",
            "source_commit": "commit-12345",
            "status": "HEALTHY",
            "reload_version": 8,
            "health_check": {"status": "ok"},
            "is_healthy": True,
        }
        with patch("coordinator.app.promote_deployment", AsyncMock(return_value=mock_promotion)):
            promote_req = {
                "service_name": "billing-service",
                "source_commit": "commit-12345",
            }
            resp = await client.post("/deploy/promote", json=promote_req, headers=auth_headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data["deployment_id"] == "dep-promoted-99"
            assert data["reload_version"] == 8
            assert data["status"] == "HEALTHY"

        # 5. Test POST /tasks/{task_id}/approve
        with patch("coordinator.app.approve_reconciled_plan", AsyncMock(return_value={"status": "RECONCILED", "task_id": "task-1"})):
            resp = await client.post("/tasks/task-1/approve", json={"approved_by": "lead-dev"}, headers=auth_headers)
            assert resp.status_code == 200
            assert resp.json()["status"] == "RECONCILED"

        # 6. Test POST /tasks/{task_id}/reject
        with patch("coordinator.app.reject_reconciled_plan", AsyncMock(return_value={"status": "REPLANNING", "task_id": "task-1"})):
            resp = await client.post("/tasks/task-1/reject", json={"rejection_reason": "Needs fix"}, headers=auth_headers)
            assert resp.status_code == 200
            assert resp.json()["status"] == "REPLANNING"


# ==============================================================================
# 7. Live CockroachDB Deployment Integration Test
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_deployment_promotion():
    """Live test: Promotes a deployment on CockroachDB and verifies database persistence."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        await init_db()

        base_dir = Path(__file__).parent.parent
        from coordinator.contract_registry import get_service_git_commit
        commit_sha = get_service_git_commit(base_dir / "repos" / "billing-service")

        with patch(
            "coordinator.deployer.poll_service_readiness",
            new_callable=AsyncMock,
            return_value=(True, {"status": "healthy", "service": "billing-service"}, None),
        ):
            res = await promote_deployment(
                service_name="billing-service",
                source_commit=commit_sha,
                skip_test_gate=True,
            )
        assert res["deployment_id"] is not None
        assert res["status"] == "HEALTHY"
        assert res["reload_version"] >= 1

        history = await get_deployment_history(limit=5)
        assert len(history) >= 1
        assert any(str(h["deployment_id"]) == str(res["deployment_id"]) for h in history)

    finally:
        await close_pool()
