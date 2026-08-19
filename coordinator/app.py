"""CodeClaim FastAPI Coordinator & Changefeed Webhook Application.

Exposes:
1. GET /: React 3-Panel Control Mesh Interactive Dashboard (served from the FastAPI deployment)
2. GET /api/dashboard/state: Live dashboard state aggregator for zero-flicker UI hydration
3. POST /api/semantic-search: Vector similarity search across registered candidate contracts
4. POST /events/cockroach: Changefeed ingestion endpoint with secret-based auth and idempotent inbox storage
5. GET /health: Health check and database connectivity probe
6. GET /demo/version & GET /deploy/version: Live reload version endpoints for client UI polling
7. POST /deploy/promote: Secure, authenticated deployment promotion with test gating, atomic directory swap, and health checks
8. GET /deploy/history: Deployment audit ledger
9. GET /deploy/services: Status of supervised microservice processes
10. POST /tasks/{task_id}/approve: Human approval endpoint for awaiting reconciliation plans
11. POST /tasks/{task_id}/reject: Human rejection endpoint for awaiting reconciliation plans
12. GET /tasks & GET /tasks/{task_id}: Active agent tasks and drift status endpoints
13. Supervised background worker task for continuous drift processing
"""

from __future__ import annotations

import sys
import asyncio
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import base64
import json
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ConfigDict

from coordinator.config import settings
from coordinator.db import check_health, close_pool, execute_query, execute_statement, fetch_one, init_db
from coordinator.drift_worker import process_all_pending_events, process_single_inbox_event, run_drift_worker_loop
from coordinator.compatibility import (
    approve_compatibility_work,
    authenticate_harness,
    cancel_compatibility_work,
    claim_next_work_item,
    complete_harness_task,
    complete_compatibility_work,
    disable_harness,
    expire_compatibility_work,
    fail_compatibility_work,
    get_compatibility_work_item,
    record_compatibility_incident,
    record_harness_checkpoint,
    record_compatibility_result,
    register_harness,
    register_harness_task,
)
from coordinator.compatibility_dispatcher import run_compatibility_dispatcher_loop
from coordinator.slack_notifier import run_slack_notifier_loop
from coordinator.deployer import (
    ALLOWED_SERVICES,
    get_deployment_history,
    get_latest_reload_version,
    promote_deployment,
    resolve_service_app_module_async,
    supervisor,
)
from coordinator.memory import search_candidate_contracts
from coordinator.reconciliation import (
    approve_reconciled_plan,
    check_task_drift,
    reject_reconciled_plan,
)

logger = logging.getLogger(__name__)

# Supervised background worker task state
_drift_worker_task: Optional[asyncio.Task] = None
_drift_worker_stop_event: Optional[asyncio.Event] = None
_compatibility_dispatcher_task: Optional[asyncio.Task] = None
_compatibility_dispatcher_stop_event: Optional[asyncio.Event] = None
_slack_notifier_task: Optional[asyncio.Task] = None
_slack_notifier_stop_event: Optional[asyncio.Event] = None


# ==============================================================================
# 1. Lifespan & Supervised Worker Lifecycle
# ==============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup, runtime validation, supervised drift worker, and graceful shutdown."""
    global _drift_worker_task, _drift_worker_stop_event
    global _compatibility_dispatcher_task, _compatibility_dispatcher_stop_event
    global _slack_notifier_task, _slack_notifier_stop_event

    logger.info("Starting CodeClaim Coordinator (demo_mode=%s)...", settings.is_demo_mode)

    # 1. Enforce runtime safety: fail closed if configuration is invalid
    try:
        settings.validate_runtime(require_database=not settings.is_demo_mode)
    except Exception as e:
        logger.critical("Runtime validation failed on coordinator startup: %s", e)
        if not settings.is_demo_mode:
            raise

    # 2. Initialize database connection pool
    try:
        await init_db()
    except Exception as e:
        logger.critical("Database initialization failed on startup: %s", e)
        if not settings.is_demo_mode:
            raise

    # 3. Execute startup cutover recovery for any interrupted deployments and restart/verify service
    try:
        recovered = supervisor.journal.recover_if_needed()
        if recovered:
            recovered_service = recovered.get("service_name")
            logger.warning("Startup cutover recovery restored service: %s. Starting process...", recovered_service)
            if recovered_service in ALLOWED_SERVICES:
                recovered_dir = supervisor.base_dir / "deployments" / "live" / recovered_service
                recovered_app_module = await resolve_service_app_module_async(recovered_service, recovered_dir)
                supervisor.start_service(service_name=recovered_service, app_module=recovered_app_module)
                from coordinator.deployer import poll_service_readiness
                is_ready, data, err = await poll_service_readiness(service_name=recovered_service, max_retries=10)
                if not is_ready:
                    err_msg = f"Restored service {recovered_service} failed readiness check on startup: {err}"
                    logger.critical(err_msg)
                    if not settings.is_demo_mode:
                        raise RuntimeError(err_msg)
                else:
                    logger.info("Restored service %s passed readiness verification on startup.", recovered_service)
    except Exception as e:
        logger.critical("Cutover recovery failed on coordinator startup: %s", e)
        if not settings.is_demo_mode:
            raise

    # 4. Spawn supervised background drift worker loop
    _drift_worker_stop_event = asyncio.Event()
    _drift_worker_task = asyncio.create_task(
        run_drift_worker_loop(poll_interval=0.5, stop_event=_drift_worker_stop_event)
    )
    logger.info("Supervised CodeClaim Drift Worker background task launched.")
    _compatibility_dispatcher_stop_event = asyncio.Event()
    _compatibility_dispatcher_task = asyncio.create_task(
        run_compatibility_dispatcher_loop(_compatibility_dispatcher_stop_event)
    )
    logger.info("CodeClaim compatibility dispatcher background task launched.")
    _slack_notifier_stop_event = asyncio.Event()
    _slack_notifier_task = asyncio.create_task(run_slack_notifier_loop(_slack_notifier_stop_event))
    logger.info("CodeClaim Slack notifier background task launched (enabled=%s).", settings.slack_notifications_enabled)

    yield

    # 5. Graceful shutdown: cancel and drain worker task, stop subprocesses, close DB pool
    logger.info("Shutting down CodeClaim Coordinator and Drift Worker...")
    supervisor.stop_all()
    if _drift_worker_stop_event:
        _drift_worker_stop_event.set()
    if _drift_worker_task and not _drift_worker_task.done():
        _drift_worker_task.cancel()
        try:
            await asyncio.wait_for(_drift_worker_task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    if _compatibility_dispatcher_stop_event:
        _compatibility_dispatcher_stop_event.set()
    if _compatibility_dispatcher_task and not _compatibility_dispatcher_task.done():
        _compatibility_dispatcher_task.cancel()
        try:
            await asyncio.wait_for(_compatibility_dispatcher_task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    if _slack_notifier_stop_event:
        _slack_notifier_stop_event.set()
    if _slack_notifier_task and not _slack_notifier_task.done():
        _slack_notifier_task.cancel()
        try:
            await asyncio.wait_for(_slack_notifier_task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    await close_pool()
    logger.info("CodeClaim Coordinator shutdown complete.")


app = FastAPI(
    title="CodeClaim Coordinator",
    description="Transactional Semantic Memory & Contract Mesh for Collaborative Coding Agents",
    version="0.1.0",
    lifespan=lifespan,
)

# Constrained CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Static asset and template mounting
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
REACT_DASHBOARD_INDEX = STATIC_DIR / "dashboard" / "index.html"

STATIC_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ==============================================================================
# 2. Request Models
# ==============================================================================


class PromoteDeploymentRequest(BaseModel):
    service_name: str = Field(..., description="Name of microservice being deployed (must be in allowed list)")
    source_commit: str = Field(..., description="Git commit SHA of the candidate release")
    health_check_timeout: float = Field(default=3.0, description="Health check timeout in seconds")


class ApproveTaskPlanRequest(BaseModel):
    approved_by: str = Field(default="operator-human", description="Operator identity approving the reconciled plan")


class RejectTaskPlanRequest(BaseModel):
    rejection_reason: str = Field(..., description="Feedback explaining why the plan was rejected")
    rejected_by: str = Field(default="operator-human", description="Operator identity rejecting the reconciled plan")


class SemanticSearchRequest(BaseModel):
    query: str = Field(..., description="Natural language query string to search across candidate contracts")
    top_k: int = Field(default=4, ge=1, le=20, description="Maximum number of candidate contracts to return")


class RegisterHarnessRequest(BaseModel):
    harness_name: str
    harness_type: str = Field(description="For example: codex, claude_code, cursor, internal_runner")
    service_name: str
    repository_url: str
    dispatch_mode: str = Field(default="poll", pattern="^(poll|webhook)$")
    dispatch_url: Optional[str] = None
    capability_manifest: dict[str, Any] = Field(default_factory=dict)


class DisableHarnessRequest(BaseModel):
    actor: str = Field(default="operator", min_length=1, max_length=200)


class ClaimCompatibilityWorkRequest(BaseModel):
    worktree_path: str
    base_commit: str


class HarnessTaskCompletionRequest(BaseModel):
    summary: str = Field(default="Task completed by harness", max_length=500)
    test_results: Optional[dict[str, Any]] = None


class ExactHTTPDependencyRequest(BaseModel):
    provider_service: str = Field(min_length=1, max_length=200)
    contract_id: str = Field(min_length=1, max_length=100)
    assumed_revision: int = Field(ge=1)
    http_method: str = Field(min_length=3, max_length=7)
    endpoint_path: str = Field(min_length=1, max_length=1000)
    path_parameters: dict[str, Any] = Field(default_factory=dict)
    query_parameters: dict[str, Any] = Field(default_factory=dict)
    declared_headers: dict[str, Any] = Field(default_factory=dict)
    request_body_schema: dict[str, Any] = Field(default_factory=dict)
    response_schemas: dict[str, Any] = Field(min_length=1)
    consumer_source_file: str = Field(min_length=1, max_length=1000)
    consumer_source_evidence: dict[str, Any]
    confirmation_status: str = Field(pattern="^(DECLARED|CONFIRMED|REJECTED)$")
    confirmed_by: Optional[str] = Field(default=None, max_length=200)


class RegisterHarnessTaskRequest(BaseModel):
    task_summary: str = Field(min_length=1, max_length=200)
    worktree_path: str = Field(min_length=1, max_length=2000)
    base_commit: str = Field(min_length=1, max_length=200)
    dependencies: list[ExactHTTPDependencyRequest] = Field(min_length=1)
    task_id: Optional[str] = Field(default=None, max_length=100)


class PublishHarnessContractRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    service_name: str = Field(min_length=1, max_length=200)
    endpoint_path: str = Field(min_length=1, max_length=1000)
    http_method: str = Field(min_length=3, max_length=10)
    revision_number: int = Field(ge=1)
    schema_json: dict[str, Any]
    source_commit: str = Field(min_length=1, max_length=200)
    semantic_summary: str = Field(default="", max_length=2000)
    publisher_compatibility: Optional[dict[str, Any]] = None


class TestResultsEvidence(BaseModel):
    returncode: int
    all_passed: bool
    passed_count: int = 0
    failed_count: int = 0
    duration_seconds: float = 0.0
    framework: str = Field(default="pytest", max_length=50)
    summary: str = Field(default="Test execution completed", max_length=500)


class CompatibilityResultRequest(BaseModel):
    test_results: dict[str, Any]
    summary: str = Field(min_length=1, max_length=1000)


class HarnessCheckpointRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    task_id: Optional[str] = None
    plan_revision: Optional[int] = None
    phase: str
    files_changed: list[str] = Field(default_factory=list)
    changed_files: list[str] = Field(default_factory=list)
    assumed_contract_revisions: dict[str, int] = Field(default_factory=dict)
    test_status: str = "NOT_RUN"
    summary: str = Field(default="", max_length=1000)


class CompatibilityIncidentRequest(BaseModel):
    outcome: str = Field(pattern="^(BLOCKED|INCOMPATIBLE)$")
    missing_requirement: str = Field(default="", max_length=1000)
    unavailable_required_input: Optional[str] = Field(default=None, max_length=500)
    reason_code: str = Field(default="UNAVAILABLE_REQUIRED_INPUT", max_length=100)
    provider_service: Optional[str] = Field(default=None, max_length=200)
    provider_contract_revision: Optional[int] = None
    sources_checked: list[str] = Field(default_factory=list)
    worktree_path: Optional[str] = None
    source_commit: Optional[str] = None
    changed_files: list[str] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    requested_resolution: str = Field(min_length=1, max_length=2000)


class CompatibilityApprovalRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=200)


class CompatibilityCancelRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=1000)
    actor: str = Field(default="operator", min_length=1, max_length=200)


class CompatibilityFailRequest(BaseModel):
    failure_reason: str = Field(min_length=1, max_length=1000)
    actor: str = Field(default="coordinator", min_length=1, max_length=200)


class RetireContractRequest(BaseModel):
    service_name: str = Field(min_length=1, max_length=200)
    endpoint_path: str = Field(min_length=1, max_length=1000)
    http_method: str = Field(min_length=3, max_length=7)
    source_commit: str = Field(min_length=1, max_length=200)
    migration_note: str = Field(min_length=1, max_length=2000)
    retired_by: str = Field(min_length=1, max_length=200)
    replacement_contract_key: Optional[str] = None


class ContractInventoryRequest(BaseModel):
    service_name: str = Field(min_length=1, max_length=200)
    source_commit: str = Field(min_length=1, max_length=200)
    contracts: list[dict[str, str]] = Field(default_factory=list)
    published_by: str = Field(min_length=1, max_length=200)


# ==============================================================================
# 3. Authentication Helpers
# ==============================================================================


def verify_changefeed_auth(
    authorization: Optional[str] = Header(default=None),
    x_webhook_secret: Optional[str] = Header(default=None),
) -> bool:
    """Verify incoming changefeed webhook against configured secret using constant-time comparison."""
    configured_secret = settings.changefeed_webhook_secret

    if settings.is_demo_mode and not configured_secret:
        return True

    if not configured_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CHANGEFEED_WEBHOOK_SECRET is not configured on server",
        )

    if x_webhook_secret and secrets.compare_digest(x_webhook_secret, configured_secret):
        return True

    if authorization:
        if authorization.startswith("Bearer "):
            token = authorization[len("Bearer "):].strip()
            if secrets.compare_digest(token, configured_secret):
                return True
        elif authorization.startswith("Basic "):
            try:
                b64_val = authorization[len("Basic "):].strip()
                decoded = base64.b64decode(b64_val).decode("utf-8")
                if secrets.compare_digest(decoded, configured_secret) or decoded.endswith(f":{configured_secret}"):
                    return True
            except Exception:
                pass

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing changefeed webhook authorization",
    )


def verify_operator_auth(
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> bool:
    """Verify operator authority for administrative actions like deployment promotion and plan approvals."""
    configured_key = settings.coordinator_api_key

    if settings.is_demo_mode:
        if not configured_key and settings.demo_allow_anonymous_mutations:
            return True
        effective_key = configured_key or "demo-operator-token"
    else:
        if not configured_key:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="COORDINATOR_API_KEY is not configured on server",
            )
        effective_key = configured_key

    if x_operator_token and secrets.compare_digest(x_operator_token, effective_key):
        return True

    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):].strip()
        if secrets.compare_digest(token, effective_key):
            return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing operator authorization token",
    )


async def require_harness_auth(harness_id: str, x_harness_token: Optional[str]) -> dict[str, Any]:
    """Authenticate a registered harness without exposing database credentials to it."""
    if not x_harness_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-Harness-Token")
    try:
        return await authenticate_harness(harness_id, x_harness_token)
    except PermissionError as ex:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(ex)) from ex



def extract_changefeed_records(body: Any) -> list[dict[str, Any]]:
    """Extract individual changefeed event records from all standard CockroachDB changefeed envelopes."""
    if isinstance(body, dict):
        if "payload" in body and isinstance(body["payload"], list):
            return body["payload"]
        if "events" in body and isinstance(body["events"], list):
            return body["events"]
        if "records" in body and isinstance(body["records"], list):
            return body["records"]
        return [body]

    if isinstance(body, list):
        flattened: list[dict[str, Any]] = []
        for item in body:
            if isinstance(item, dict):
                flattened.extend(extract_changefeed_records(item))
        return flattened

    return []


# ==============================================================================
# 4. 3-Panel Control Dashboard UI & State Aggregator Endpoints
# ==============================================================================


async def _get_registered_service_statuses() -> list[dict[str, Any]]:
    """Return CockroachDB-registered services enriched with process status.

    ``ALLOWED_SERVICES`` is a deployment safety allowlist, not the service
    registry. The dashboard therefore reads ``microservices`` as its source
    of truth and only enriches those rows with local supervisor state.
    """
    registered_rows = await execute_query(
        """
        SELECT service_name, repository_path, primary_region,
               entrypoint_module, entrypoint_app, registration_source,
               registered_by, registration_event_id, created_at
        FROM microservices
        WHERE registration_source = 'ONBOARDING_CLI'
        ORDER BY service_name ASC;
        """
    )
    supervised_by_name = {
        str(row.get("service_name")): row
        for row in supervisor.get_all_services_status()
    }

    services: list[dict[str, Any]] = []
    for row in registered_rows:
        # Keep the application-side guard as defense in depth for older
        # deployments or mocked/read-model queries that omit the SQL filter.
        if row.get("registration_source") != "ONBOARDING_CLI":
            continue
        service = dict(row)
        supervised = supervised_by_name.get(str(service.get("service_name")), {})
        service["running"] = bool(supervised.get("running", False))
        service["pid"] = supervised.get("pid")
        if supervised.get("returncode") is not None:
            service["returncode"] = supervised["returncode"]
        services.append(service)
    return services


@app.get("/", response_class=HTMLResponse)
@app.get("/control", response_class=HTMLResponse)
async def get_dashboard_ui(request: Request) -> Response:
    """Serve the production React dashboard, with the legacy shell as a build fallback."""
    if REACT_DASHBOARD_INDEX.is_file():
        return FileResponse(REACT_DASHBOARD_INDEX, media_type="text/html")

    # Keep the server-rendered shell available when the optional frontend build
    # has not been generated yet (for example, during Python-only development).
    reload_ver = await get_latest_reload_version()
    try:
        services_status = await _get_registered_service_statuses()
    except Exception as ex:
        logger.warning("Error fetching registered services for legacy dashboard: %s", ex)
        services_status = []
    db_health = await check_health()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "reload_version": reload_ver,
            "services": services_status,
            "is_demo_mode": settings.is_demo_mode,
            "db_healthy": db_health.get("status") == "healthy",
        },
    )


@app.get("/api/dashboard/state")
async def get_dashboard_state(
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
    graph_minutes: int = Query(default=30, ge=1, le=60, description="Live graph activity window in minutes"),
) -> dict[str, Any]:
    """Consolidated state payload for live UI dashboard hydration and zero-flicker polling."""
    db_health = await check_health()
    is_db_healthy = db_health.get("status") == "healthy"

    if not is_db_healthy and not settings.is_demo_mode:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"CockroachDB database is unreachable: {db_health.get('error')}",
        )

    reload_ver = await get_latest_reload_version()
    services: list[dict[str, Any]] = []

    # Active Agent Tasks with declared dependencies (strictly no raw prompts or scratchpads)
    tasks_sql = """
    SELECT task_id, agent_id, service_name, task_summary, worktree_path, branch_name,
           base_commit, plan_revision, status, checkpoint_state, created_at, updated_at
    FROM active_agent_tasks
    ORDER BY created_at DESC
    LIMIT 20;
    """
    tasks = []
    fetch_error = None
    try:
        services = await _get_registered_service_statuses()
        tasks = await execute_query(tasks_sql)
        # Enrich all tasks in one query. The dashboard polls frequently; an
        # individual dependency query per task made a healthy CockroachDB
        # connection look unavailable whenever the live task list was large.
        task_ids = [t.get("task_id") for t in tasks if t.get("task_id")]
        dependencies_by_task: dict[str, list[dict[str, Any]]] = {str(task_id): [] for task_id in task_ids}
        if task_ids:
            try:
                dependency_rows = await execute_query(
                    """SELECT task_id, provider_service, assumed_revision, dependency_kind, dependency_path
                       FROM task_contract_dependencies WHERE task_id = ANY(%s);""",
                    (task_ids,),
                )
                for dependency in dependency_rows:
                    dependencies_by_task.setdefault(str(dependency["task_id"]), []).append(
                        {
                            "provider_service": dependency.get("provider_service"),
                            "assumed_revision": dependency.get("assumed_revision"),
                            "dependency_kind": dependency.get("dependency_kind"),
                            "dependency_path": dependency.get("dependency_path"),
                        }
                    )
            except Exception as dependency_error:
                logger.warning("Error fetching task dependencies for dashboard: %s", dependency_error)
        for task in tasks:
            task["declared_dependencies"] = dependencies_by_task.get(str(task.get("task_id")), [])
    except Exception as ex:
        fetch_error = str(ex)
        logger.warning("Error fetching registered services/tasks for dashboard: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=fetch_error)

    # Coordinator Outbox Feed
    outbox_sql = """
    SELECT event_id, aggregate_type, aggregate_id, aggregate_revision,
           source_service, event_type, payload, created_at
    FROM coordinator_outbox
    ORDER BY created_at DESC
    LIMIT 20;
    """
    outbox_events = []
    try:
        outbox_events = await execute_query(outbox_sql)
    except Exception as ex:
        fetch_error = fetch_error or str(ex)
        logger.warning("Error fetching outbox events for dashboard: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database error fetching outbox events: {ex}")

    # Drift Events with causal lineage
    drift_sql = """
    SELECT drift_id, outbox_event_id, causation_id, correlation_id, source_service, target_service,
           old_contract_revision, new_contract_revision, breaking_diff, status, created_at
    FROM drift_events
    ORDER BY created_at DESC
    LIMIT 10;
    """
    drift_events = []
    try:
        drift_events = await execute_query(drift_sql)
    except Exception as ex:
        fetch_error = fetch_error or str(ex)
        logger.warning("Error fetching drift events for dashboard: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database error fetching drift events: {ex}")

    # Deployments
    deployments = []
    try:
        deployments = await get_deployment_history(limit=10)
    except Exception as ex:
        fetch_error = fetch_error or str(ex)
        logger.warning("Error fetching deployment history for dashboard: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database error fetching deployment history: {ex}")

    # Contract Revisions & Publication Timeline
    contracts_sql = """
    SELECT r.contract_revision_id, c.service_name, r.revision_number, c.endpoint_path, c.http_method,
           r.source_commit, r.is_active, r.published_at AS created_at
    FROM service_contract_revisions r
    JOIN service_contracts c ON c.contract_id = r.contract_id
    ORDER BY c.service_name ASC, r.revision_number DESC;
    """
    contracts = []
    try:
        contracts = await execute_query(contracts_sql)
    except Exception as ex:
        fetch_error = fetch_error or str(ex)
        logger.warning("Error fetching contracts for dashboard: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database error fetching contracts: {ex}")

    # Confirmed Service Dependencies Matrix
    dependencies_sql = """
    SELECT d.dependency_id, d.consumer_service, d.provider_service, d.contract_id,
           d.assumed_provider_revision, d.http_method, d.endpoint_path,
           d.confirmation_status, d.consumer_source_file, d.consumer_repository, d.created_at
    FROM http_interface_dependencies d
    WHERE d.confirmation_status = 'CONFIRMED'
    ORDER BY d.consumer_service ASC, d.provider_service ASC;
    """
    dependencies = []
    dependency_candidates = []
    try:
        dependencies = await execute_query(dependencies_sql)
        dependency_candidates = await execute_query(
            """
            SELECT d.dependency_id, d.consumer_service, d.provider_service, d.contract_id,
                   d.assumed_provider_revision, d.http_method, d.endpoint_path,
                   d.confirmation_status, d.consumer_source_file, d.consumer_repository, d.created_at
            FROM http_interface_dependencies d
            WHERE d.confirmation_status IN ('DECLARED', 'REJECTED')
            ORDER BY d.created_at DESC
            LIMIT 50;
            """
        )
    except Exception as ex:
        logger.warning("Error fetching dependencies for dashboard: %s", ex)
        dependencies = []

    compatibility_work = []
    compatibility_work_history = []
    compatibility_incidents = []
    audit_history = []
    agent_dependency_graph: dict[str, Any] = {"nodes": [], "edges": [], "active_task_count": 0, "active_agent_count": 0}
    try:
        compatibility_work = await execute_query(
            """SELECT work_item_id, source_contract_id, target_service, target_repository,
                      source_contract_revision, payload->>'source_service' AS source_service,
                      payload->>'http_method' AS http_method,
                      payload->>'endpoint_path' AS endpoint_path,
                      payload->>'interface_dependency_id' AS interface_dependency_id,
                      payload->>'consumer_assumed_revision' AS consumer_assumed_revision,
                      state, payload, task_id, dispatch_attempts, failure_reason, created_at, updated_at
               FROM compatibility_work_items
               WHERE state NOT IN ('COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED')
               ORDER BY created_at DESC LIMIT 20;"""
        )
        compatibility_work_history = await execute_query(
            """SELECT work_item_id, source_contract_id, target_service, target_repository,
                      source_contract_revision, payload->>'source_service' AS source_service,
                      payload->>'http_method' AS http_method,
                      payload->>'endpoint_path' AS endpoint_path,
                      payload->>'interface_dependency_id' AS interface_dependency_id,
                      payload->>'consumer_assumed_revision' AS consumer_assumed_revision,
                      state, payload, task_id, dispatch_attempts, failure_reason, created_at, updated_at
               FROM compatibility_work_items
               WHERE state IN ('COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED')
                ORDER BY updated_at DESC LIMIT 20;"""
        )
        compatibility_incidents = await execute_query(
            """SELECT i.incident_id, i.work_item_id, i.incident_type, i.missing_requirement,
                      i.evidence, i.requested_resolution, i.status, i.created_at,
                      w.target_service, w.source_contract_revision, w.payload
               FROM compatibility_incidents i
               JOIN compatibility_work_items w ON w.work_item_id = i.work_item_id
               ORDER BY i.created_at DESC LIMIT 20;"""
        )
        audit_history = await execute_query(
            """SELECT history_id, outbox_event_id, causation_id, correlation_id,
                      event_type, source_service, target_service, summary, actor, created_at
               FROM contract_audit_history ORDER BY created_at DESC LIMIT 30;"""
        )
        agent_dependency_graph = await _get_agent_dependency_graph(graph_minutes)
    except Exception as ex:
        fetch_error = fetch_error or str(ex)
        logger.warning("Error fetching compatibility workflow state: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database error fetching compatibility work: {ex}")

    return {
        "reload_version": reload_ver,
        "services": services,
        "tasks": tasks,
        "outbox_events": outbox_events,
        "drift_events": drift_events,
        "deployments": deployments,
        "contracts": contracts,
        "dependencies": dependencies,
        "dependency_candidates": dependency_candidates,
        "compatibility_work": compatibility_work,
        "compatibility_work_history": compatibility_work_history,
        "compatibility_incidents": compatibility_incidents,
        "audit_history": audit_history,
        "agent_dependency_graph": agent_dependency_graph,
        "agent_dependency_graph_minutes": graph_minutes,
        "db_healthy": is_db_healthy,
        "db_error": fetch_error or db_health.get("error"),
        "is_demo_mode": settings.is_demo_mode,
        "public_demo_enabled": settings.public_demo_enabled,
    }


@app.get("/api/demo/run")
async def get_public_demo_run_status() -> dict[str, Any]:
    """Return the current bounded public demo run without requiring an operator token."""
    if not settings.public_demo_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Public demo is disabled")
    from coordinator.public_demo import get_public_demo_run

    row = await get_public_demo_run()
    if not row:
        return {"status": "IDLE", "phase": "NOT_STARTED", "run_id": None, "result": {}}
    row["run_id"] = str(row["run_id"])
    return row


@app.post("/api/demo/run")
async def launch_public_demo_run() -> JSONResponse:
    """Launch the safe public demo workflow; this route never accepts arbitrary code or prompts."""
    if not settings.public_demo_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Public demo is disabled")
    from coordinator.public_demo import launch_public_demo

    try:
        run = await launch_public_demo()
    except Exception as ex:
        logger.exception("Unable to launch public demo")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Public demo is temporarily unavailable") from ex
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=jsonable_encoder(run))


@app.get("/api/events")
async def get_events_feed(
    limit: int = 50,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Retrieve combined outbox and drift event records."""
    verify_operator_auth(authorization, x_operator_token)
    outbox = []
    drift = []
    try:
        outbox = await execute_query(
            "SELECT * FROM coordinator_outbox ORDER BY created_at DESC LIMIT %s;",
            (limit,),
        )
        drift = await execute_query(
            "SELECT * FROM drift_events ORDER BY created_at DESC LIMIT %s;",
            (limit,),
        )
    except Exception as ex:
        logger.warning("Error querying events: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(ex))

    return {
        "outbox": outbox,
        "drift": drift,
    }


def _time_window_clauses(
    column: str,
    from_time: Optional[datetime],
    to_time: Optional[datetime],
) -> tuple[str, list[Any]]:
    """Build bounded timestamp predicates for the operator read models."""
    clauses: list[str] = []
    params: list[Any] = []
    if from_time is not None:
        clauses.append(f"{column} >= %s")
        params.append(from_time)
    if to_time is not None:
        clauses.append(f"{column} <= %s")
        params.append(to_time)
    return (f" AND {' AND '.join(clauses)}" if clauses else "", params)


_GRAPH_TERMINAL_TASK_STATES = {"COMPLETED", "FAILED", "RECONCILED"}


async def _get_agent_dependency_graph(minutes: Optional[int] = None) -> dict[str, Any]:
    """Build the live agent/API dependency graph from authoritative task state.

    Nodes represent active agent work on an exact API operation when the task is
    bound to a confirmed HTTP dependency.  A task without an operation is still
    represented, but its endpoint is explicitly marked as unavailable rather than
    inferred from its service name.  Edges are created only from confirmed
    ``task_contract_dependencies`` joined to ``http_interface_dependencies``.
    """
    task_query = """
        SELECT task_id, agent_id, service_name, task_summary, plan_revision,
               status, created_at, updated_at, COALESCE(heartbeat_at, updated_at) AS activity_at
        FROM active_agent_tasks
        WHERE status NOT IN ('COMPLETED', 'FAILED', 'RECONCILED')
    """
    task_params: tuple[Any, ...] = ()
    if minutes is not None:
        task_query += " AND COALESCE(heartbeat_at, updated_at) >= %s"
        task_params = (datetime.now(timezone.utc) - timedelta(minutes=minutes),)
    task_query += " ORDER BY updated_at DESC LIMIT 100;"
    task_rows = await execute_query(task_query, task_params)
    if not task_rows:
        return {"nodes": [], "edges": [], "active_task_count": 0}

    task_ids = [str(row["task_id"]) for row in task_rows]
    dependency_rows = await execute_query(
        """
        SELECT td.task_id, td.interface_dependency_id, td.provider_service,
               td.contract_id, td.assumed_revision, d.consumer_service,
               d.http_method, d.endpoint_path, d.assumed_provider_revision,
               d.confirmation_status
        FROM task_contract_dependencies td
        JOIN http_interface_dependencies d
          ON d.dependency_id = td.interface_dependency_id
        WHERE td.task_id = ANY(%s)
          AND d.confirmation_status = 'CONFIRMED'
        ORDER BY d.provider_service, d.endpoint_path, d.http_method;
        """,
        (task_ids,),
    )
    work_rows = await execute_query(
        """
        SELECT w.task_id, w.work_item_id, w.source_contract_revision,
               c.service_name AS provider_service, c.http_method,
               c.endpoint_path
        FROM compatibility_work_items w
        JOIN service_contracts c ON c.contract_id = w.source_contract_id
        WHERE w.task_id = ANY(%s)
          AND w.state NOT IN ('COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED')
        ORDER BY w.updated_at DESC;
        """,
        (task_ids,),
    )

    dependencies_by_task: dict[str, list[dict[str, Any]]] = {}
    for row in dependency_rows:
        dependencies_by_task.setdefault(str(row["task_id"]), []).append(row)
    work_by_task: dict[str, list[dict[str, Any]]] = {}
    for row in work_rows:
        work_by_task.setdefault(str(row["task_id"]), []).append(row)

    def operation_key(operation_service: Any, method: Any, path: Any) -> tuple[str, str, str] | None:
        if not operation_service or not method or not path:
            return None
        return (str(operation_service), str(method).upper(), str(path))

    nodes: list[dict[str, Any]] = []
    nodes_by_operation: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    dependency_nodes: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    task_operation_nodes: dict[tuple[str, tuple[str, str, str] | None], dict[str, Any]] = {}

    for task in task_rows:
        task_id = str(task["task_id"])
        operations: list[dict[str, Any]] = []

        # A confirmed task dependency is the strongest source for the operation
        # the consumer agent is currently adapting.
        for dependency in dependencies_by_task.get(task_id, []):
            operations.append(
                {
                    "operation_service": dependency.get("provider_service"),
                    "http_method": dependency.get("http_method"),
                    "endpoint_path": dependency.get("endpoint_path"),
                    "provider_revision": dependency.get("assumed_provider_revision"),
                    "interface_dependency_id": str(dependency.get("interface_dependency_id")),
                    "source": "confirmed_dependency",
                }
            )

        # Compatibility work can describe the provider operation even before a
        # task dependency row is available (for example while a harness is being
        # dispatched). It is shown as an agent node but does not create an edge by
        # itself; edges still require a confirmed dependency row above.
        for work in work_by_task.get(task_id, []):
            operation = {
                "operation_service": work.get("provider_service"),
                "http_method": work.get("http_method"),
                "endpoint_path": work.get("endpoint_path"),
                "provider_revision": work.get("source_contract_revision"),
                "work_item_id": str(work.get("work_item_id")),
                "source": "compatibility_work",
            }
            if not any(
                operation_key(item.get("operation_service"), item.get("http_method"), item.get("endpoint_path"))
                == operation_key(operation.get("operation_service"), operation.get("http_method"), operation.get("endpoint_path"))
                for item in operations
            ):
                operations.append(operation)

        if not operations:
            operations.append({"operation_service": task.get("service_name"), "source": "task"})

        for index, operation in enumerate(operations):
            op_key = operation_key(
                operation.get("operation_service"),
                operation.get("http_method"),
                operation.get("endpoint_path"),
            )
            identity = operation.get("interface_dependency_id") or operation.get("work_item_id") or f"generic-{index}"
            node_id = f"task:{task_id}:{identity}"
            node = {
                "node_id": node_id,
                "kind": "agent",
                "task_id": task_id,
                "agent_id": task.get("agent_id"),
                "service_name": task.get("service_name"),
                "operation_service": operation.get("operation_service"),
                "http_method": operation.get("http_method"),
                "endpoint_path": operation.get("endpoint_path"),
                "provider_revision": operation.get("provider_revision"),
                "status": task.get("status"),
                "task_summary": task.get("task_summary"),
                "plan_revision": task.get("plan_revision"),
                "created_at": task.get("created_at"),
                "updated_at": task.get("updated_at"),
                "is_active": True,
            }
            nodes.append(node)
            task_operation_nodes[(task_id, op_key)] = node
            if op_key:
                nodes_by_operation.setdefault(op_key, []).append(node)

    edges: list[dict[str, Any]] = []
    for dependency in dependency_rows:
        task_id = str(dependency["task_id"])
        op_key = operation_key(
            dependency.get("provider_service"),
            dependency.get("http_method"),
            dependency.get("endpoint_path"),
        )
        consumer_node = task_operation_nodes.get((task_id, op_key))
        if not consumer_node or not op_key:
            continue

        provider_nodes = [
            node for node in nodes_by_operation.get(op_key, [])
            if node.get("service_name") == dependency.get("provider_service")
            and node.get("node_id") != consumer_node.get("node_id")
        ]
        if not provider_nodes:
            ghost_key = (
                str(dependency.get("interface_dependency_id")),
                str(dependency.get("provider_service")),
                str(dependency.get("http_method")).upper(),
                str(dependency.get("endpoint_path")),
            )
            provider_node = dependency_nodes.get(ghost_key)
            if provider_node is None:
                provider_node = {
                    "node_id": f"dependency:{ghost_key[0]}",
                    "kind": "dependency",
                    "task_id": None,
                    "agent_id": None,
                    "service_name": dependency.get("provider_service"),
                    "operation_service": dependency.get("provider_service"),
                    "http_method": dependency.get("http_method"),
                    "endpoint_path": dependency.get("endpoint_path"),
                    "provider_revision": dependency.get("assumed_provider_revision"),
                    "status": "NOT_ACTIVE",
                    "task_summary": "No active agent is currently working on this provider operation.",
                    "plan_revision": None,
                    "created_at": None,
                    "updated_at": None,
                    "is_active": False,
                }
                dependency_nodes[ghost_key] = provider_node
                nodes.append(provider_node)
            provider_nodes = [provider_node]

        for provider_node in provider_nodes:
            edges.append(
                {
                    "edge_id": f"{consumer_node['node_id']}->{provider_node['node_id']}",
                    "from": consumer_node["node_id"],
                    "to": provider_node["node_id"],
                    "dependency_id": str(dependency.get("interface_dependency_id")),
                    "consumer_service": dependency.get("consumer_service"),
                    "provider_service": dependency.get("provider_service"),
                    "http_method": dependency.get("http_method"),
                    "endpoint_path": dependency.get("endpoint_path"),
                    "assumed_provider_revision": dependency.get("assumed_provider_revision"),
                    "status": "CONFIRMED",
                }
            )

    return {
        "nodes": nodes,
        "edges": edges,
        "active_task_count": len(task_rows),
        "active_agent_count": len({str(row.get("agent_id")) for row in task_rows if row.get("agent_id")}),
    }


@app.get("/api/agent-runs")
async def get_agent_runs(
    from_time: Optional[datetime] = Query(default=None, alias="from"),
    to_time: Optional[datetime] = Query(default=None, alias="to"),
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return every compatibility obligation with its tasks, checkpoints, and causal events."""
    verify_operator_auth(authorization, x_operator_token)
    work_window, work_params = _time_window_clauses("w.created_at", from_time, to_time)
    work_items = await execute_query(
        f"""
        SELECT w.work_item_id, w.source_event_id, w.source_contract_id,
               w.source_contract_revision, w.target_service, w.target_repository,
               w.harness_id, w.state, w.coordination_key, w.task_id,
               w.payload, w.dispatch_attempts, w.failure_reason,
               w.created_at, w.updated_at,
               c.endpoint_path, c.http_method, c.service_name AS source_service
        FROM compatibility_work_items w
        JOIN service_contracts c ON c.contract_id = w.source_contract_id
        WHERE TRUE{work_window}
        ORDER BY w.created_at DESC;
        """,
        tuple(work_params),
    )

    work_ids = [str(row["work_item_id"]) for row in work_items]
    task_ids = [str(row["task_id"]) for row in work_items if row.get("task_id")]
    tasks: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    outbox_events: list[dict[str, Any]] = []
    audit_events: list[dict[str, Any]] = []

    if task_ids:
        tasks = await execute_query(
            """SELECT task_id, agent_id, service_name, task_summary,
                      worktree_path, branch_name, base_commit, plan_revision,
                      status, checkpoint_state, last_reconciled_at,
                      created_at, updated_at
               FROM active_agent_tasks
               WHERE task_id = ANY(%s)
               ORDER BY created_at ASC;""",
            (task_ids,),
        )
        checkpoints = await execute_query(
            """SELECT checkpoint_id, task_id, plan_revision, status,
                      checkpoint_state, created_at
               FROM agent_checkpoints
               WHERE task_id = ANY(%s)
               ORDER BY created_at ASC;""",
            (task_ids,),
        )

    if work_ids or task_ids:
        outbox_events = await execute_query(
            """SELECT event_id, aggregate_type, aggregate_id, aggregate_revision,
                      source_service, event_type, payload, event_version, created_at
               FROM coordinator_outbox
               WHERE aggregate_id = ANY(%s)
                  OR payload->>'work_item_id' = ANY(%s)
                  OR payload->>'task_id' = ANY(%s)
               ORDER BY created_at ASC;""",
            (work_ids, work_ids, task_ids),
        )
        outbox_ids = [str(row["event_id"]) for row in outbox_events]
        if outbox_ids:
            audit_events = await execute_query(
                """SELECT history_id, outbox_event_id, causation_id,
                          correlation_id, event_type, source_service,
                          target_service, summary, actor, created_at
                   FROM contract_audit_history
                   WHERE outbox_event_id = ANY(%s)
                   ORDER BY created_at ASC;""",
                (outbox_ids,),
            )

    tasks_by_id = {str(task["task_id"]): task for task in tasks}
    checkpoints_by_task: dict[str, list[dict[str, Any]]] = {}
    for checkpoint in checkpoints:
        checkpoints_by_task.setdefault(str(checkpoint["task_id"]), []).append(checkpoint)

    events_by_work: dict[str, list[dict[str, Any]]] = {work_id: [] for work_id in work_ids}
    for event in outbox_events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_work_id = payload.get("work_item_id")
        event_task_id = payload.get("task_id")
        for work in work_items:
            work_id = str(work["work_item_id"])
            if (
                str(event.get("aggregate_id")) == work_id
                or str(event_work_id) == work_id
                or (work.get("task_id") and str(event_task_id) == str(work["task_id"]))
            ):
                events_by_work[work_id].append(event)
                break

    audits_by_event: dict[str, list[dict[str, Any]]] = {}
    for audit in audit_events:
        audits_by_event.setdefault(str(audit["outbox_event_id"]), []).append(audit)

    obligations = []
    for work in work_items:
        work_id = str(work["work_item_id"])
        task_id = str(work["task_id"]) if work.get("task_id") else None
        obligation_events = []
        for event in events_by_work.get(work_id, []):
            obligation_events.append({
                "outbox": event,
                "audit": audits_by_event.get(str(event["event_id"]), []),
            })
        obligations.append({
            "obligation": work,
            "tasks": [tasks_by_id[task_id]] if task_id and task_id in tasks_by_id else [],
            "checkpoints": checkpoints_by_task.get(task_id, []) if task_id else [],
            "events": obligation_events,
        })

    return {"obligations": obligations, "count": len(obligations)}


@app.get("/api/contract-diffs")
async def get_contract_diffs(
    from_time: Optional[datetime] = Query(default=None, alias="from"),
    to_time: Optional[datetime] = Query(default=None, alias="to"),
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return the complete persisted contract-diff/drift history in the selected time window."""
    verify_operator_auth(authorization, x_operator_token)
    window, params = _time_window_clauses("d.created_at", from_time, to_time)
    diffs = await execute_query(
        f"""
        SELECT d.drift_id, d.outbox_event_id, d.causation_id,
               d.correlation_id, d.source_service, d.target_task_id,
               d.target_service, d.old_contract_revision,
               d.new_contract_revision, d.breaking_diff, d.status,
               d.acknowledged, d.resolved_by, d.resolution_summary,
               d.created_at, d.reconciled_at, d.updated_at,
               o.event_type AS source_event_type, o.payload AS source_event_payload
        FROM drift_events d
        LEFT JOIN coordinator_outbox o ON o.event_id = d.outbox_event_id
        WHERE TRUE{window}
        ORDER BY d.created_at DESC;
        """,
        tuple(params),
    )
    return {"diffs": diffs, "count": len(diffs)}


@app.get("/api/contract-diffs/{drift_id}")
async def get_contract_diff_details(
    drift_id: str,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return the persisted diff, source outbox payload, and linked audit rows."""
    verify_operator_auth(authorization, x_operator_token)
    diff = await fetch_one(
        """SELECT d.*, o.event_type AS source_event_type,
                  o.payload AS source_event_payload, o.created_at AS source_event_created_at
           FROM drift_events d
           LEFT JOIN coordinator_outbox o ON o.event_id = d.outbox_event_id
           WHERE d.drift_id = %s;""",
        (drift_id,),
    )
    if not diff:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Contract diff {drift_id} not found")
    audits = await execute_query(
        """SELECT history_id, outbox_event_id, causation_id, correlation_id,
                  event_type, source_service, target_service, summary,
                  schema_diff, actor, created_at
           FROM contract_audit_history
           WHERE outbox_event_id = %s
           ORDER BY created_at ASC;""",
        (diff.get("outbox_event_id"),),
    )
    return {"diff": diff, "audit": audits}


@app.get("/api/events/{event_id}")
async def get_event_details(
    event_id: str,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return one transactional outbox JSON payload and its linked lineage."""
    verify_operator_auth(authorization, x_operator_token)
    outbox = await fetch_one(
        """SELECT event_id, aggregate_type, aggregate_id, aggregate_revision,
                  source_service, event_type, payload, event_version, created_at
           FROM coordinator_outbox WHERE event_id = %s;""",
        (event_id,),
    )
    if not outbox:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Event {event_id} not found")
    audit = await execute_query(
        """SELECT history_id, outbox_event_id, causation_id, correlation_id,
                  event_type, source_service, target_service, summary,
                  schema_diff, actor, created_at
           FROM contract_audit_history
           WHERE outbox_event_id = %s
           ORDER BY created_at ASC;""",
        (event_id,),
    )
    drift = await execute_query(
        """SELECT drift_id, outbox_event_id, causation_id, correlation_id,
                  source_service, target_task_id, target_service,
                  old_contract_revision, new_contract_revision,
                  breaking_diff, status, created_at, updated_at
           FROM drift_events WHERE outbox_event_id = %s
           ORDER BY created_at ASC;""",
        (event_id,),
    )
    return {"outbox": outbox, "audit": audit, "drift": drift}


@app.get("/api/audit-trail")
async def get_audit_trail(
    from_time: Optional[datetime] = Query(default=None, alias="from"),
    to_time: Optional[datetime] = Query(default=None, alias="to"),
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return audit and outbox records so the UI can group them by causal correlation."""
    verify_operator_auth(authorization, x_operator_token)
    audit_window, audit_params = _time_window_clauses("a.created_at", from_time, to_time)
    outbox_window, outbox_params = _time_window_clauses("o.created_at", from_time, to_time)
    audit = await execute_query(
        f"""SELECT a.history_id, a.outbox_event_id, a.causation_id,
                   a.correlation_id, a.event_type, a.source_service,
                   a.target_service, a.summary, a.schema_diff, a.actor,
                   a.created_at
            FROM contract_audit_history a
            WHERE TRUE{audit_window}
            ORDER BY a.created_at DESC;""",
        tuple(audit_params),
    )
    outbox = await execute_query(
        f"""SELECT o.event_id, o.aggregate_type, o.aggregate_id,
                   o.aggregate_revision, o.source_service, o.event_type,
                   o.payload, o.event_version, o.created_at
            FROM coordinator_outbox o
            WHERE TRUE{outbox_window}
            ORDER BY o.created_at DESC;""",
        tuple(outbox_params),
    )
    return {"audit": audit, "outbox": outbox, "audit_count": len(audit), "outbox_count": len(outbox)}


@app.post("/api/simulate/drift")
async def simulate_breaking_drift(
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Simulation trigger endpoint: Publishes billing-service v2 breaking schema change on active endpoint /v1/charges."""
    verify_operator_auth(authorization, x_operator_token)
    from coordinator.contract_registry import publish_contract_revision, get_service_git_commit
    root_dir = Path(__file__).parent.parent
    billing_repo = root_dir / "repos" / "billing-service"
    source_commit = get_service_git_commit(billing_repo)
    v2_schema = {
        "type": "object",
        "properties": {
            "amount": {"type": "integer", "description": "Payment amount in cents"},
            "currency": {"type": "string", "description": "ISO 4217 currency code"},
            "card_token": {"type": "string", "description": "Legacy card token"},
            "token_id": {"type": "string", "description": "Token identifier already available in Orders"},
        },
        "required": ["amount", "card_token", "token_id"],
    }
    return await publish_contract_revision(
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        revision_number=2,
        schema_json=v2_schema,
        semantic_summary="Billing Service v2 charges endpoint now requires token_id in addition to the existing card_token",
        published_by="simulation-operator",
        source_commit=source_commit,
    )


@app.post("/api/simulate/reconcile")
async def simulate_reconcile_task(
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Simulation trigger endpoint: Starts Agent B orders adaptation task and runs test gates."""
    verify_operator_auth(authorization, x_operator_token)
    from coordinator.agent_runner import start_agent_b_checkout_task
    # Resolve active contract ID for billing-service:POST:/v1/charges
    row = await fetch_one(
        "SELECT contract_id FROM service_contracts WHERE service_name = 'billing-service' AND endpoint_path = '/v1/charges' AND http_method = 'POST' LIMIT 1;"
    )
    if row and row.get("contract_id"):
        contract_id = str(row["contract_id"])
    else:
        from coordinator.agent_runner import run_agent_a_publish_revision_1
        v1_res = await run_agent_a_publish_revision_1()
        contract_id = str(v1_res["contract_id"])

    return await start_agent_b_checkout_task(contract_id=contract_id)


@app.post("/api/semantic-search")
async def perform_semantic_search(
    req: SemanticSearchRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Search candidate contracts using CockroachDB native semantic vector embeddings."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        results = await search_candidate_contracts(prompt=req.query, limit=req.top_k)
        if results:
            return {"query": req.query, "count": len(results), "results": results, "simulated": False}
    except Exception as ex:
        logger.warning("Semantic vector search database query failed: %s", ex)
        if not settings.is_demo_mode:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"CockroachDB semantic vector store is currently unreachable: {ex}",
            )

    # In explicit demo mode, return clearly labeled simulated results
    query_lower = req.query.lower()
    is_v2_match = any(w in query_lower for w in ["idempotency", "token", "v2", "stripe", "charge"])
    mock_results = [
        {
            "service_name": "billing-service",
            "revision": 2 if is_v2_match else 1,
            "route_path": "/v2/charges" if is_v2_match else "/v1/charges",
            "summary": "[SIMULATED DEMO] Stripe/Payment settlement contract with idempotency key and amount in decimal dollars" if is_v2_match else "[SIMULATED DEMO] Legacy charges contract with integer cents",
            "score": 0.942 if is_v2_match else 0.812,
            "distance": 0.058 if is_v2_match else 0.188,
            "simulated": True,
        },
        {
            "service_name": "billing-service",
            "revision": 1 if is_v2_match else 2,
            "route_path": "/v1/charges" if is_v2_match else "/v2/charges",
            "summary": "[SIMULATED DEMO] Legacy charges contract with integer cents" if is_v2_match else "[SIMULATED DEMO] Stripe/Payment settlement contract with idempotency key",
            "score": 0.784,
            "distance": 0.216,
            "simulated": True,
        },
    ]
    return {"query": req.query, "count": len(mock_results), "results": mock_results, "simulated": True}



# ==============================================================================
# 5. Core System Health, Deployment & Task Endpoints
# ==============================================================================


@app.get("/health")
async def get_health_status() -> dict[str, Any]:
    """Health check endpoint: Verifies coordinator process and CockroachDB database connectivity."""
    db_health = await check_health()
    is_db_ok = db_health.get("status") == "healthy"
    overall_status = "healthy" if (is_db_ok or settings.is_demo_mode) else "degraded"

    return {
        "status": overall_status,
        "coordinator": "healthy",
        "database": db_health,
        "demo_mode": settings.is_demo_mode,
        "timestamp": db_health.get("timestamp"),
    }


@app.get("/demo/version")
@app.get("/deploy/version")
async def get_live_reload_version() -> dict[str, Any]:
    """Retrieve current reload version from CockroachDB for zero-flicker client UI polling."""
    latest_version = await get_latest_reload_version()
    return {
        "reload_version": latest_version,
        "status": "ok",
        "demo_mode": settings.is_demo_mode,
    }


@app.post("/deploy/promote", status_code=status.HTTP_200_OK)
async def promote_service_deployment(
    req: PromoteDeploymentRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator endpoint to promote candidate git commit to live microservice deployment."""
    verify_operator_auth(authorization, x_operator_token)

    if req.service_name not in ALLOWED_SERVICES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Service '{req.service_name}' is not in allowed services: {sorted(ALLOWED_SERVICES)}",
        )

    try:
        result = await promote_deployment(
            service_name=req.service_name,
            source_commit=req.source_commit,
            health_check_timeout=req.health_check_timeout,
        )
        return result
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as ex:
        logger.error("Deployment promotion failed for %s: %s", req.service_name, ex)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex))


@app.get("/deploy/history")
async def get_deploy_history_endpoint(
    limit: int = 20,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> list[dict[str, Any]]:
    """Retrieve recent deployment history records from the audit ledger."""
    verify_operator_auth(authorization, x_operator_token)
    return await get_deployment_history(limit=limit)


@app.get("/deploy/services")
async def get_services_status(
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> list[dict[str, Any]]:
    """Retrieve process supervision status of local microservice instances."""
    verify_operator_auth(authorization, x_operator_token)
    return supervisor.get_all_services_status()


# ==============================================================================
# 6. Agent Task Management & Human Approval Endpoints
# ==============================================================================


@app.post("/contracts/retire")
async def retire_contract_endpoint(
    req: RetireContractRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Append a breaking endpoint tombstone; this never infers retirement from absence."""
    verify_operator_auth(authorization, x_operator_token)
    from coordinator.contract_registry import retire_contract
    try:
        return await retire_contract(**req.model_dump())
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/contracts/inventory")
async def publish_contract_inventory_endpoint(
    req: ContractInventoryRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Compare a service's discovered endpoint inventory with active authoritative contracts."""
    verify_operator_auth(authorization, x_operator_token)
    from coordinator.contract_registry import publish_contract_inventory
    try:
        return await publish_contract_inventory(**req.model_dump())
    except (ValueError, KeyError) as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/register", status_code=status.HTTP_201_CREATED)
async def register_harness_endpoint(
    req: RegisterHarnessRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator-only registration. The returned harness token is shown once and never stored in plaintext."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await register_harness(**req.model_dump())
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/disable")
async def disable_harness_endpoint(
    harness_id: str,
    req: DisableHarnessRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Invalidate a harness token while preserving its task and audit history."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await disable_harness(harness_id, actor=req.actor)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/compatibility-work/claim")
async def claim_compatibility_work_endpoint(
    harness_id: str,
    req: ClaimCompatibilityWorkRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Authenticated long-poll/claim endpoint used by Codex, Claude Code, or any registered runner."""
    await require_harness_auth(harness_id, x_harness_token)
    claimed = await claim_next_work_item(
        harness_id, worktree_path=req.worktree_path, base_commit=req.base_commit
    )
    return {"work": claimed}


@app.post("/harnesses/{harness_id}/tasks", status_code=status.HTTP_201_CREATED)
async def register_harness_task_endpoint(
    harness_id: str,
    req: RegisterHarnessTaskRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Register an optimistic task and its explicit upstream contract assumptions."""
    harness = await require_harness_auth(harness_id, x_harness_token)
    try:
        return await register_harness_task(harness, **req.model_dump())
    except (ValueError, KeyError) as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/tasks/{task_id}/complete")
async def complete_harness_task_endpoint(
    harness_id: str,
    task_id: str,
    req: HarnessTaskCompletionRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Close a normal owned task; compatibility work uses its separate approval lifecycle."""
    await require_harness_auth(harness_id, x_harness_token)
    try:
        return await complete_harness_task(
            harness_id, task_id, summary=req.summary, test_results=req.test_results
        )
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.get("/harnesses/{harness_id}/tasks/{task_id}/drift")
async def get_harness_task_drift_endpoint(
    harness_id: str,
    task_id: str,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Authenticated endpoint for external harness to query pending drift at a checkpoint boundary."""
    await require_harness_auth(harness_id, x_harness_token)
    drift = await check_task_drift(task_id)
    if not drift:
        return {"instruction": "CONTINUE", "drift": None}
    schema_diff = drift.get("breaking_diff") or {}
    migration_notes = (
        drift.get("migration_notes")
        or (schema_diff.get("migration_note") if isinstance(schema_diff, dict) else "")
        or (schema_diff.get("diff_summary") if isinstance(schema_diff, dict) else "")
        or ""
    )
    audit_ids = drift.get("audit_ids") or {
        "drift_id": str(drift.get("drift_id")),
        "task_id": str(task_id),
        "source_service": drift.get("source_service"),
        "target_service": drift.get("target_service"),
    }
    return {
        "instruction": "REPLAN_REQUIRED",
        "new_contract_revision": drift.get("new_contract_revision"),
        "old_contract_revision": drift.get("old_contract_revision"),
        "schema_diff": schema_diff,
        "migration_notes": migration_notes,
        "audit_ids": audit_ids,
        "drift": drift,
    }


@app.get("/tasks/{task_id}/drift")
async def get_task_drift_endpoint(task_id: str) -> dict[str, Any]:
    """Retrieve pending drift instruction for any task."""
    drift = await check_task_drift(task_id)
    if not drift:
        return {"instruction": "CONTINUE", "drift": None}
    schema_diff = drift.get("breaking_diff") or {}
    migration_notes = (
        drift.get("migration_notes")
        or (schema_diff.get("migration_note") if isinstance(schema_diff, dict) else "")
        or (schema_diff.get("diff_summary") if isinstance(schema_diff, dict) else "")
        or ""
    )
    audit_ids = drift.get("audit_ids") or {
        "drift_id": str(drift.get("drift_id")),
        "task_id": str(task_id),
        "source_service": drift.get("source_service"),
        "target_service": drift.get("target_service"),
    }
    return {
        "instruction": "REPLAN_REQUIRED",
        "new_contract_revision": drift.get("new_contract_revision"),
        "old_contract_revision": drift.get("old_contract_revision"),
        "schema_diff": schema_diff,
        "migration_notes": migration_notes,
        "audit_ids": audit_ids,
        "drift": drift,
    }


@app.post("/harnesses/{harness_id}/contracts/publish")
async def publish_harness_contract_endpoint(
    harness_id: str,
    req: PublishHarnessContractRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Publish a versioned OpenAPI contract revision from an authenticated external harness."""
    harness = await require_harness_auth(harness_id, x_harness_token)
    if harness["service_name"] != req.service_name:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Harness is registered for service '{harness['service_name']}', cannot publish for '{req.service_name}'",
        )
    from coordinator.contract_registry import publish_contract_revision
    try:
        return await publish_contract_revision(
            service_name=req.service_name,
            endpoint_path=req.endpoint_path,
            http_method=req.http_method,
            revision_number=req.revision_number,
            schema_json=req.schema_json,
            source_commit=req.source_commit,
            semantic_summary=req.semantic_summary,
            published_by=f"{harness['harness_type']}:{harness['harness_name']}",
            publisher_compatibility=req.publisher_compatibility,
        )
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/tasks/{task_id}/checkpoint")
async def checkpoint_harness_task_endpoint(
    harness_id: str,
    task_id: str,
    req: HarnessCheckpointRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Persist only typed operational checkpoint metadata and return CONTINUE/REPLAN_REQUIRED."""
    await require_harness_auth(harness_id, x_harness_token)
    try:
        return await record_harness_checkpoint(harness_id, task_id, req.model_dump())
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/compatibility-work/{work_item_id}/result")
async def submit_compatibility_result_endpoint(
    harness_id: str,
    work_item_id: str,
    req: CompatibilityResultRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Persist harness test evidence; production deployment still requires a separate approval."""
    await require_harness_auth(harness_id, x_harness_token)
    work = await fetch_one(
        "SELECT harness_id FROM compatibility_work_items WHERE work_item_id = %s;", (work_item_id,)
    )
    if not work or str(work["harness_id"]) != harness_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Compatibility work is not assigned to this harness")
    try:
        return await record_compatibility_result(
            work_item_id, test_results=req.test_results, summary=req.summary
        )
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/harnesses/{harness_id}/compatibility-work/{work_item_id}/incident")
async def submit_compatibility_incident_endpoint(
    harness_id: str,
    work_item_id: str,
    req: CompatibilityIncidentRequest,
    x_harness_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    await require_harness_auth(harness_id, x_harness_token)
    work = await fetch_one("SELECT harness_id FROM compatibility_work_items WHERE work_item_id=%s;", (work_item_id,))
    if not work or str(work["harness_id"]) != harness_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Compatibility work is not assigned to this harness")
    try:
        return await record_compatibility_incident(work_item_id, **req.model_dump())
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/compatibility-work/{work_item_id}/approve")
async def approve_compatibility_work_endpoint(
    work_item_id: str,
    req: CompatibilityApprovalRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await approve_compatibility_work(work_item_id, req.actor)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/compatibility-work/{work_item_id}/complete")
async def complete_compatibility_work_endpoint(
    work_item_id: str,
    req: CompatibilityApprovalRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Mark a separately verified/merged change complete; never deploys anything itself."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await complete_compatibility_work(work_item_id, req.actor)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/compatibility-work/{work_item_id}/cancel")
async def cancel_compatibility_work_endpoint(
    work_item_id: str,
    req: CompatibilityCancelRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator cancel endpoint for compatibility work items."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await cancel_compatibility_work(work_item_id, reason=req.reason, actor=req.actor)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/compatibility-work/{work_item_id}/fail")
async def fail_compatibility_work_endpoint(
    work_item_id: str,
    req: CompatibilityFailRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator / coordinator endpoint to mark compatibility work as FAILED."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await fail_compatibility_work(work_item_id, failure_reason=req.failure_reason, actor=req.actor)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.post("/compatibility-work/{work_item_id}/expire")
async def expire_compatibility_work_endpoint(
    work_item_id: str,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Expire a work item whose lease timed out without progress."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await expire_compatibility_work(work_item_id)
    except ValueError as ex:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex)) from ex


@app.get("/compatibility-work/{work_item_id}")
async def get_compatibility_work_item_endpoint(
    work_item_id: str,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Retrieve full details of a specific compatibility work item."""
    verify_operator_auth(authorization, x_operator_token)
    item = await get_compatibility_work_item(work_item_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Work item {work_item_id} not found")
    return item


@app.get("/compatibility-work")
async def list_compatibility_work(
    limit: int = 50,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> list[dict[str, Any]]:
    verify_operator_auth(authorization, x_operator_token)
    return await execute_query(
        """SELECT work_item_id, source_contract_id, source_contract_revision, target_service,
                  target_repository, harness_id, state, dispatch_attempts, task_id, failure_reason,
                  coordination_key, payload->>'source_service' AS source_service,
                  payload->>'http_method' AS http_method, payload->>'endpoint_path' AS endpoint_path,
                  payload, created_at, updated_at
           FROM compatibility_work_items ORDER BY created_at DESC LIMIT %s;""", (min(limit, 100),)
    )


@app.get("/tasks")
@app.get("/api/tasks")
async def list_agent_tasks(
    limit: int = 50,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> list[dict[str, Any]]:
    """Retrieve all active in-flight agent tasks and their current state."""
    verify_operator_auth(authorization, x_operator_token)
    sql = """
    SELECT 
        task_id, agent_id, service_name, task_summary,
        worktree_path, branch_name, base_commit, plan_revision, status,
        checkpoint_state, created_at, updated_at
    FROM active_agent_tasks
    ORDER BY created_at DESC
    LIMIT %s;
    """
    try:
        return await execute_query(sql, (limit,))
    except Exception as ex:
        logger.warning("Error fetching agent tasks: %s", ex)
        return []


@app.get("/tasks/{task_id}")
async def get_task_details(
    task_id: str,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Retrieve details for a specific agent task including active drift intervention if present."""
    verify_operator_auth(authorization, x_operator_token)
    sql = """
    SELECT 
        task_id, agent_id, service_name, task_summary,
        worktree_path, branch_name, base_commit, plan_revision, status,
        checkpoint_state, last_reconciled_at, created_at, updated_at
    FROM active_agent_tasks WHERE task_id = %s;
    """
    task = await fetch_one(sql, (task_id,))
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")

    drift = await check_task_drift(task_id)
    return {
        "task": task,
        "active_drift": drift,
    }


@app.post("/tasks/{task_id}/approve", status_code=status.HTTP_200_OK)
async def approve_task_reconciliation(
    task_id: str,
    req: ApproveTaskPlanRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator human approval endpoint: Transitions task from AWAITING_APPROVAL to RECONCILED."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await approve_reconciled_plan(task_id=task_id, approved_by=req.approved_by)
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as ex:
        logger.error("Error approving task %s: %s", task_id, ex)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex))


@app.post("/tasks/{task_id}/reject", status_code=status.HTTP_200_OK)
async def reject_task_reconciliation(
    task_id: str,
    req: RejectTaskPlanRequest,
    authorization: Optional[str] = Header(default=None),
    x_operator_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Operator human rejection endpoint: Transitions task from AWAITING_APPROVAL to REPLANNING with feedback."""
    verify_operator_auth(authorization, x_operator_token)
    try:
        return await reject_reconciled_plan(
            task_id=task_id,
            rejection_reason=req.rejection_reason,
            rejected_by=req.rejected_by,
        )
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as ex:
        logger.error("Error rejecting task %s: %s", task_id, ex)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex))


# ==============================================================================
# 7. Webhook Ingestion Endpoint
# ==============================================================================


@app.post("/events/cockroach", status_code=status.HTTP_200_OK)
async def receive_changefeed_event(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_webhook_secret: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Idempotently ingest CockroachDB changefeed events into event_inbox."""
    verify_changefeed_auth(authorization, x_webhook_secret)

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid JSON payload: {e}")

    records = extract_changefeed_records(body)
    if not records and body:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid event records with event_id found in changefeed payload",
        )

    ingested_events = []

    for rec in records:
        event_id = rec.get("event_id")
        if not event_id:
            continue

        payload_data = rec.get("payload") or rec
        if isinstance(payload_data, str):
            try:
                payload_data = json.loads(payload_data)
            except Exception:
                pass

        insert_query = """
        INSERT INTO event_inbox (event_id, processing_status, attempt_count, payload)
        VALUES (%s, 'RECEIVED', 0, %s::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id;
        """
        try:
            row = await fetch_one(insert_query, (event_id, json.dumps(payload_data)))
            is_new = bool(row is not None)
        except Exception as ex:
            logger.error("Failed to durably persist changefeed event %s into event_inbox: %s", event_id, ex)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database failure persisting event '{event_id}' into event_inbox: {ex}",
            )

        ingested_events.append({"event_id": str(event_id), "is_new": is_new})

    # In demo mode, trigger immediate processing of inbox events
    if settings.is_demo_mode or settings.demo_auto_reconcile:
        try:
            await process_all_pending_events(max_count=10)
        except Exception as ex:
            logger.warning("Immediate demo inbox drain error: %s", ex)

    return {
        "status": "received",
        "count": len(ingested_events),
        "events": ingested_events,
    }
