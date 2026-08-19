"""Section 9 Verification Suite: 3-Panel Control Dashboard UI, REST routes, Simulation & Failure Handling."""

import json
from unittest.mock import AsyncMock, patch
import httpx
import pytest

from coordinator.app import _get_agent_dependency_graph, app
from coordinator.config import settings


@pytest.mark.asyncio
async def test_dashboard_ui_html_rendering():
    """Verify both dashboard URLs serve the React production shell when built."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("coordinator.app.get_latest_reload_version", AsyncMock(return_value=3)), \
             patch("coordinator.app.check_health", AsyncMock(return_value={"status": "healthy"})):
            for path in ["/", "/control"]:
                resp = await client.get(path)
                assert resp.status_code == 200
                assert "text/html" in resp.headers["content-type"]
                html = resp.text

                if 'id="root"' in html:
                    assert "CodeClaim Control Mesh" in html
                    assert "/static/dashboard/assets/" in html
                else:
                    # Python-only environments can still use the legacy shell
                    # until the optional frontend build is generated.
                    assert "CodeClaim Control Mesh" in html
                    assert "Contract Mesh & Semantic Memory" in html
                    assert "Transactional Outbox & Audit Lineage" in html
                    assert "db-outage-banner" in html


@pytest.mark.asyncio
async def test_agent_dependency_graph_links_confirmed_api_operations():
    """The graph links a consumer task to an active provider task only for the exact confirmed operation."""
    active_tasks = [
        {
            "task_id": "task-orders",
            "agent_id": "agent-orders",
            "service_name": "orders-service",
            "task_summary": "Adapt checkout client",
            "plan_revision": 1,
            "status": "OPTIMISTIC_EXECUTING",
            "created_at": "2026-08-18T10:00:00Z",
            "updated_at": "2026-08-18T10:01:00Z",
        },
        {
            "task_id": "task-billing",
            "agent_id": "agent-billing",
            "service_name": "billing-service",
            "task_summary": "Publish charge contract",
            "plan_revision": 2,
            "status": "OPTIMISTIC_EXECUTING",
            "created_at": "2026-08-18T10:00:00Z",
            "updated_at": "2026-08-18T10:01:00Z",
        },
    ]
    confirmed_dependency = [{
        "task_id": "task-orders",
        "interface_dependency_id": "interface-1",
        "provider_service": "billing-service",
        "consumer_service": "orders-service",
        "contract_id": "contract-1",
        "assumed_revision": 1,
        "assumed_provider_revision": 1,
        "http_method": "POST",
        "endpoint_path": "/v1/charges",
        "confirmation_status": "CONFIRMED",
    }]
    compatibility_work = [{
        "task_id": "task-billing",
        "work_item_id": "work-billing",
        "source_contract_revision": 2,
        "provider_service": "billing-service",
        "http_method": "POST",
        "endpoint_path": "/v1/charges",
    }]

    with patch("coordinator.app.execute_query", AsyncMock(side_effect=[active_tasks, confirmed_dependency, compatibility_work])):
        graph = await _get_agent_dependency_graph()

    assert graph["active_agent_count"] == 2
    assert len(graph["edges"]) == 1
    edge = graph["edges"][0]
    assert edge["consumer_service"] == "orders-service"
    assert edge["provider_service"] == "billing-service"
    assert edge["http_method"] == "POST"
    assert edge["endpoint_path"] == "/v1/charges"
    nodes = {node["node_id"]: node for node in graph["nodes"]}
    assert nodes[edge["from"]]["agent_id"] == "agent-orders"
    assert nodes[edge["to"]]["agent_id"] == "agent-billing"


@pytest.mark.asyncio
async def test_static_assets_served_with_session_storage_only():
    """Verify static assets are served, app.js contains NO hardcoded operator secret tokens, and uses sessionStorage only."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Check style.css
        resp_css = await client.get("/static/style.css")
        assert resp_css.status_code == 200
        assert "text/css" in resp_css.headers.get("content-type", "")
        assert "--bg-main" in resp_css.text

        # Check app.css
        resp_app_css = await client.get("/static/app.css")
        assert resp_app_css.status_code == 200

        # Check app.js
        resp_js = await client.get("/static/app.js")
        assert resp_js.status_code == 200
        assert "javascript" in resp_js.headers.get("content-type", "")
        assert "ensureOperatorToken" in resp_js.text
        assert "safeJsonParse" in resp_js.text
        assert "renderContractTimeline" in resp_js.text
        assert "renderAuditLineage" in resp_js.text
        assert "renderServices" in resp_js.text
        assert "const depsHtml" in resp_js.text
        assert "updateDashboardClientError" in resp_js.text
        assert "Human decision required" in resp_js.text
        # Ensure sessionStorage is used and localStorage is NOT used for operator tokens
        assert "sessionStorage.getItem" in resp_js.text
        assert "localStorage" not in resp_js.text
        # Ensure NO hard-coded secret constants exist in client source
        assert 'const OPERATOR_TOKEN' not in resp_js.text


@pytest.mark.asyncio
async def test_api_dashboard_state_endpoint_health_and_all_views():
    """Verify GET /api/dashboard/state checks CockroachDB health and returns all 6 cross-service compatibility views."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}
        mock_services = [{
            "service_name": "billing-service",
            "repository_path": "repos/billing-service",
            "primary_region": "local",
            "entrypoint_module": "main",
            "entrypoint_app": "app",
            "registration_source": "ONBOARDING_CLI",
            "running": False,
            "pid": None,
        }]
        mock_tasks = [{"task_id": "task-test-1", "service_name": "orders-service", "status": "AWAITING_APPROVAL"}]
        mock_deps = [{"task_id": "task-test-1", "provider_service": "billing-service", "assumed_revision": 1, "dependency_kind": "EXACT_HTTP"}]
        mock_outbox = [{"event_id": "evt-1", "event_type": "CONTRACT_PUBLISHED", "payload": {}}]
        mock_drift = [{"drift_id": "drift-1", "is_breaking": True, "diff_payload": {}}]
        mock_deployments = [{"deployment_id": "dep-1", "service_name": "billing-service", "status": "HEALTHY"}]
        mock_contracts = [{"contract_revision_id": "rev-1", "service_name": "billing-service", "revision_number": 1, "status": "ACTIVE"}]
        mock_confirmed_deps = [{"dependency_id": "dep-link-1", "consumer_service": "orders-service", "provider_service": "billing-service"}]
        mock_dependency_candidates = [{"dependency_id": "dep-candidate-1", "confirmation_status": "DECLARED"}]
        mock_work = [{"work_item_id": "w-1", "target_service": "orders-service", "state": "BLOCKED"}]
        mock_work_history = [{"work_item_id": "w-0", "target_service": "orders-service", "state": "COMPLETED"}]
        mock_incidents = [{"incident_id": "inc-1", "incident_type": "INCOMPATIBLE_REQUIREMENT", "status": "HUMAN_DECISION_REQUIRED"}]
        mock_audit = [{"history_id": "aud-1", "event_type": "COMPATIBILITY_BLOCKED", "summary": "Blocked by missing customer_id"}]

        # 1. Healthy State Check
        with patch("coordinator.app.check_health", AsyncMock(return_value={"status": "healthy"})), \
             patch("coordinator.app.get_latest_reload_version", AsyncMock(return_value=4)), \
             patch("coordinator.app.supervisor.get_all_services_status", return_value=[
                 {"service_name": "billing-service", "running": False, "pid": None},
             ]), \
             patch("coordinator.app.execute_query", AsyncMock(side_effect=[
                 mock_services,
                 mock_tasks,
                 mock_deps,
                 mock_outbox,
                 mock_drift,
                 mock_contracts,
                 mock_confirmed_deps,
                 mock_dependency_candidates,
                 mock_work,
                 mock_work_history,
                 mock_incidents,
                 mock_audit,
                 [],
             ])), \
             patch("coordinator.app.get_deployment_history", AsyncMock(return_value=mock_deployments)):

            resp = await client.get("/api/dashboard/state", headers=auth_headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data["reload_version"] == 4
            assert data["db_healthy"] is True
            assert data["services"] == mock_services
            assert len(data["tasks"]) == 1
            assert data["tasks"][0]["declared_dependencies"] == [
                {"provider_service": "billing-service", "assumed_revision": 1, "dependency_kind": "EXACT_HTTP", "dependency_path": None}
            ]
            assert len(data["contracts"]) == 1
            assert len(data["dependencies"]) == 1
            assert data["dependency_candidates"] == mock_dependency_candidates
            assert len(data["drift_events"]) == 1
            assert len(data["compatibility_work"]) == 1
            assert len(data["compatibility_work_history"]) == 1
            assert len(data["compatibility_incidents"]) == 1
            assert len(data["audit_history"]) == 1
            assert data["agent_dependency_graph"]["nodes"] == []

        # 2. Database Outage in Non-Demo Mode -> Must return HTTP 503 (Fail-Closed)
        with patch("coordinator.app.check_health", AsyncMock(return_value={"status": "unhealthy", "error": "Connection refused"})), \
             patch.object(settings, "is_demo_mode", False):
            resp_outage = await client.get("/api/dashboard/state", headers=auth_headers)
            assert resp_outage.status_code == 503
            assert "CockroachDB database is unreachable" in resp_outage.text

        # 3. Outbox/Drift/Contract Query Failure in Non-Demo Mode -> Must return HTTP 503 (Fail-Closed)
        with patch("coordinator.app.check_health", AsyncMock(return_value={"status": "healthy"})), \
             patch("coordinator.app.get_latest_reload_version", AsyncMock(return_value=4)), \
             patch("coordinator.app.execute_query", AsyncMock(side_effect=[mock_services, mock_tasks, RuntimeError("Outbox table locked")])), \
             patch.object(settings, "is_demo_mode", False):
            resp_outbox_fail = await client.get("/api/dashboard/state", headers=auth_headers)
            assert resp_outbox_fail.status_code == 503
            assert "Database error fetching outbox events" in resp_outbox_fail.text


@pytest.mark.asyncio
async def test_dashboard_registered_services_are_database_truth_not_supervisor_allowlist():
    """A reset registry must not display statically allowed service names."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}
        # Service registry, tasks, outbox, drift, contracts, dependencies,
        # candidates, work, history, incidents, audit, and graph task query.
        query_results = [[] for _ in range(13)]

        with patch("coordinator.app.check_health", AsyncMock(return_value={"status": "healthy"})), \
             patch("coordinator.app.get_latest_reload_version", AsyncMock(return_value=1)), \
             patch("coordinator.app.supervisor.get_all_services_status", return_value=[
                 {"service_name": "billing-service", "running": False, "pid": None},
                 {"service_name": "orders-service", "running": False, "pid": None},
             ]), \
             patch("coordinator.app.execute_query", AsyncMock(side_effect=query_results)), \
             patch("coordinator.app.get_deployment_history", AsyncMock(return_value=[])):
            response = await client.get("/api/dashboard/state", headers=auth_headers)

        assert response.status_code == 200
        assert response.json()["services"] == []


@pytest.mark.asyncio
async def test_api_events_and_api_tasks_routes():
    """Verify /api/tasks and /api/events routes meet the documented Section 9 route contracts."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}
        mock_tasks = [{"task_id": "task-t1", "agent_id": "agent-b", "status": "OPTIMISTIC_EXECUTING"}]
        mock_outbox = [{"event_id": "e-1", "event_type": "CONTRACT_PUBLISHED"}]
        mock_drift = [{"drift_id": "d-1", "is_breaking": True}]

        with patch("coordinator.app.execute_query", AsyncMock(return_value=mock_tasks)):
            resp_tasks = await client.get("/api/tasks", headers=auth_headers)
            assert resp_tasks.status_code == 200
            assert len(resp_tasks.json()) == 1

        with patch("coordinator.app.execute_query", AsyncMock(side_effect=[mock_outbox, mock_drift])):
            resp_events = await client.get("/api/events", headers=auth_headers)
            assert resp_events.status_code == 200
            events_data = resp_events.json()
            assert "outbox" in events_data
            assert "drift" in events_data
            assert len(events_data["outbox"]) == 1


@pytest.mark.asyncio
async def test_operator_control_plane_detail_routes_preserve_hierarchy_and_payloads():
    """Verify the dedicated views read complete obligation, diff, lineage, and outbox detail models."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}
        work = [{
            "work_item_id": "work-1", "source_event_id": "evt-source", "source_contract_id": "contract-1",
            "source_contract_revision": 2, "target_service": "orders-service", "target_repository": "repos/orders-service",
            "harness_id": None, "state": "DISPATCHED", "coordination_key": "coord-1", "task_id": "task-1",
            "payload": {"source_service": "billing-service", "http_method": "POST", "endpoint_path": "/v1/charges"},
            "dispatch_attempts": 1, "failure_reason": None, "created_at": "2026-08-18T10:00:00Z", "updated_at": "2026-08-18T10:01:00Z",
            "endpoint_path": "/v1/charges", "http_method": "POST", "source_service": "billing-service",
        }]
        task = [{"task_id": "task-1", "agent_id": "claude:harness", "service_name": "orders-service", "task_summary": "Adapt charges client", "status": "EXECUTING", "plan_revision": 1}]
        checkpoints = [{"checkpoint_id": "checkpoint-1", "task_id": "task-1", "plan_revision": 1, "status": "EXECUTING", "checkpoint_state": {"phase": "IMPLEMENTING"}}]
        outbox = [{"event_id": "evt-1", "aggregate_type": "COMPATIBILITY_WORK", "aggregate_id": "work-1", "aggregate_revision": 2, "source_service": "coordinator", "event_type": "COMPATIBILITY_WORK_CLAIMED", "payload": {"work_item_id": "work-1", "task_id": "task-1"}}]
        audit = [{"history_id": "audit-1", "outbox_event_id": "evt-1", "causation_id": "evt-1", "correlation_id": "evt-1", "event_type": "COMPATIBILITY_WORK_CLAIMED", "summary": "Work claimed", "actor": "harness"}]

        with patch("coordinator.app.execute_query", AsyncMock(side_effect=[work, task, checkpoints, outbox, audit])):
            response = await client.get("/api/agent-runs", headers=auth_headers)
            assert response.status_code == 200
            obligation = response.json()["obligations"][0]
            assert obligation["obligation"]["work_item_id"] == "work-1"
            assert obligation["tasks"][0]["task_id"] == "task-1"
            assert obligation["events"][0]["outbox"]["payload"]["task_id"] == "task-1"

        diff = [{
            "drift_id": "drift-1", "outbox_event_id": "evt-drift", "source_service": "billing-service",
            "target_service": "orders-service", "old_contract_revision": 1, "new_contract_revision": 2,
            "breaking_diff": {"is_breaking": True, "breaking_changes": [{"field": "payment_method_id"}]},
            "status": "ACTIVE_INTERVENTION", "created_at": "2026-08-18T10:00:00Z",
            "source_event_type": "CONTRACT_CHANGED", "source_event_payload": {"endpoint_path": "/v1/charges"},
        }]
        with patch("coordinator.app.execute_query", AsyncMock(return_value=diff)):
            response = await client.get("/api/contract-diffs?from=2026-08-18T00:00", headers=auth_headers)
            assert response.status_code == 200
            assert response.json()["count"] == 1
            assert response.json()["diffs"][0]["breaking_diff"]["is_breaking"] is True

        with patch("coordinator.app.execute_query", AsyncMock(side_effect=[audit, outbox])):
            response = await client.get("/api/audit-trail", headers=auth_headers)
            assert response.status_code == 200
            assert response.json()["audit_count"] == 1
            assert response.json()["outbox_count"] == 1

        stored_outbox = {"event_id": "evt-1", "event_type": "COMPATIBILITY_WORK_CLAIMED", "payload": {"task_id": "task-1"}}
        with patch("coordinator.app.fetch_one", AsyncMock(return_value=stored_outbox)), \
             patch("coordinator.app.execute_query", AsyncMock(side_effect=[[audit[0]], []])):
            response = await client.get("/api/events/evt-1", headers=auth_headers)
            assert response.status_code == 200
            assert response.json()["outbox"]["payload"]["task_id"] == "task-1"
            assert response.json()["audit"][0]["outbox_event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_api_semantic_search_fail_closed_and_simulated_labeling():
    """Verify POST /api/semantic-search fails closed on DB error in non-demo mode and marks results simulated in demo mode."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}
        req = {"query": "process settlement payment with stripe token", "top_k": 2}

        # 1. DB Error in non-demo mode -> HTTP 503
        with patch("coordinator.app.search_candidate_contracts", AsyncMock(side_effect=RuntimeError("Vector index unavailable"))), \
             patch.object(settings, "is_demo_mode", False):
            resp_error = await client.post("/api/semantic-search", json=req, headers=auth_headers)
            assert resp_error.status_code == 503
            assert "semantic vector store is currently unreachable" in resp_error.text

        # 2. Demo Mode Fallback -> Explicitly marked simulated
        with patch("coordinator.app.search_candidate_contracts", AsyncMock(side_effect=RuntimeError("Vector index uninitialized"))), \
             patch.object(settings, "is_demo_mode", True):
            resp_demo = await client.post("/api/semantic-search", json=req, headers=auth_headers)
            assert resp_demo.status_code == 200
            demo_data = resp_demo.json()
            assert demo_data["simulated"] is True
            assert "[SIMULATED DEMO]" in demo_data["results"][0]["summary"]


@pytest.mark.asyncio
async def test_simulation_trigger_endpoints():
    """Verify /api/simulate/drift and /api/simulate/reconcile trigger full simulation workflows and enforce auth."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Unauthorized request without operator token in demo mode is rejected
        with patch.object(settings, "is_demo_mode", True), \
             patch.object(settings, "demo_allow_anonymous_mutations", False), \
             patch.object(settings, "coordinator_api_key", None), \
             patch.object(settings, "changefeed_webhook_secret", None):
            resp_unauth = await client.post("/api/simulate/drift")
            assert resp_unauth.status_code == 401

        auth_headers = {"X-Operator-Token": "demo-operator-token"}

        # 2. Authorized Simulate Drift with demo token
        with patch("coordinator.contract_registry.publish_contract_revision", AsyncMock(return_value={"status": "PUBLISHED", "revision": 2})), \
             patch.object(settings, "is_demo_mode", True), \
             patch.object(settings, "demo_allow_anonymous_mutations", False), \
             patch.object(settings, "coordinator_api_key", None), \
             patch.object(settings, "changefeed_webhook_secret", None):
            resp_drift = await client.post("/api/simulate/drift", headers=auth_headers)
            assert resp_drift.status_code == 200
            assert resp_drift.json()["status"] == "PUBLISHED"

        # 3. Authorized Simulate Reconcile with demo token
        with patch("coordinator.agent_runner.run_agent_a_publish_revision_1", AsyncMock(return_value={"contract_id": "contract-sim-1", "revision_number": 1})), \
             patch("coordinator.agent_runner.start_agent_b_checkout_task", AsyncMock(return_value={"task_id": "task-sim-1", "status": "AWAITING_APPROVAL"})), \
             patch.object(settings, "is_demo_mode", True), \
             patch.object(settings, "demo_allow_anonymous_mutations", False), \
             patch.object(settings, "coordinator_api_key", None), \
             patch.object(settings, "changefeed_webhook_secret", None):
            resp_recon = await client.post("/api/simulate/reconcile", headers=auth_headers)
            assert resp_recon.status_code == 200
            assert resp_recon.json()["task_id"] == "task-sim-1"


@pytest.mark.asyncio
async def test_public_demo_is_bounded_and_does_not_require_operator_token():
    """The public demo is opt-in and its launch path never invokes operator auth."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with patch.object(settings, "public_demo_enabled", False):
            disabled = await client.post("/api/demo/run")
            assert disabled.status_code == 404

        run_payload = {
            "run_id": "demo-run-1",
            "status": "RUNNING",
            "phase": "STARTING",
            "result": {},
        }
        with patch.object(settings, "public_demo_enabled", True), \
             patch("coordinator.public_demo.launch_public_demo", AsyncMock(return_value=run_payload)) as launch:
            response = await client.post("/api/demo/run")
            assert response.status_code == 202
            assert response.json()["run_id"] == "demo-run-1"
            launch.assert_awaited_once()


@pytest.mark.asyncio
async def test_deploy_read_endpoints_require_auth():
    """Verify /deploy/history and /deploy/services require operator token verification."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        auth_headers = {"X-Operator-Token": settings.coordinator_api_key}

        # 1. /deploy/history requires auth
        with patch("coordinator.app.get_deployment_history", AsyncMock(return_value=[])):
            resp_unauth = await client.get("/deploy/history")
            assert resp_unauth.status_code == 401

            resp_auth = await client.get("/deploy/history", headers=auth_headers)
            assert resp_auth.status_code == 200

        # 2. /deploy/services requires auth
        with patch("coordinator.app.supervisor.get_all_services_status", return_value=[]):
            resp_unauth = await client.get("/deploy/services")
            assert resp_unauth.status_code == 401

            resp_auth = await client.get("/deploy/services", headers=auth_headers)
            assert resp_auth.status_code == 200
