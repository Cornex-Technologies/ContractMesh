# CodeClaim: Section-Level Execution & Test Plan (Production MVP)

This document outlines the dependency-ordered, step-by-step phased construction of **CodeClaim**. Each section is strictly ordered by prerequisites, introduces concrete deliverables, and concludes with automated and manual verification suites.

---

## 1. Dependency-Ordered Execution Roadmap

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   SECTION-LEVEL EXECUTION ROADMAP                                │
├───────────┬─────────────────────────────────────────────────┬────────────────────────────────────┤
│ SECTION   │ DELIVERABLES                                    │ VERIFICATION SUITE                 │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 0 │ Environment, Pinned `pyproject.toml`, Config    │ `pytest tests/test_environment.py` │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 1 │ CockroachDB Schema & Async Connection Pool      │ `pytest tests/test_schema_and_db.py│
│           │ (`infra/cockroach/changefeed.sql` + retry loop) │ (Unit + `-m integration`)         │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 2 │ Scaffold Sibling Repos: `billing` & `orders`    │ `pytest repos/orders-service/tests/│
│           │ (Git repos, Pydantic schemas v1/v2, clients)    │ test_contract_scenarios.py`        │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 3 │ Deterministic Schema Differencing Engine        │ `pytest tests/test_differencer.py` │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 4 │ Atomic Contract Publication & Outbox Engine     │ `pytest tests/test_atomic_outbox.py│
│           │ (Extracts schemas from actual repo commits)     │ (Unit + `-m integration`)         │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 5 │ LangChain × CockroachDB Semantic Memory & Check │ `pytest tests/test_semantic_memory.│
│           │ (`semantic_memory` vector table + LangGraph)    │ py` (`-m integration`)             │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 6 │ Changefeed Receiver, Inbox Lease & Drift Worker │ `pytest tests/test_drift_worker.py`│
│           │ (Atomic event claim, deduplication & retries)   │ (Unit + `-m integration`)         │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 7 │ Checkpoint-Aware Agent Runner & Reconciliation  │ `pytest tests/test_reconcile.py`   │
│           │ (`AWAITING_APPROVAL` state + `DEMO_AUTO_RECON`) │ (Unit + `-m integration`)         │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 8 │ Deployment Manager, Supervision & Live Reload   │ `pytest tests/test_deployment.py`  │
│           │ (Gated promotion, Uvicorn supervisor, `/demo`)  │ (Unit + `-m integration`)         │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 9 │ 3-Panel Control UI (FastAPI + Jinja2 + CSS/JS)  │ `pytest tests/test_control_api.py` │
│           │ (`/control`, `/api/tasks`, `/api/events`)       │                                    │
├───────────┼─────────────────────────────────────────────────┼────────────────────────────────────┤
│ Section 10│ Managed MCP Audit, ccloud Evidence & Packaging  │ `pytest tests/test_mcp_audit.py` + │
│           │ (Read-only views, non-blocking S3 receipts)     │ Manual Claude/Cursor MCP check     │
└───────────┴─────────────────────────────────────────────────┴────────────────────────────────────┘
```

---

## 2. Test Execution & Pytest Markers

To ensure developer velocity and CI reliability without requiring live cloud credentials for every unit test:

```bash
# 1. Run offline unit tests (mocked DB, schema differencing, config validation)
pytest

# 2. Run CockroachDB integration tests (requires local or test CockroachDB instance)
pytest -m integration

# 3. Run live Cloud & Bedrock end-to-end tests (requires CockroachDB Cloud + AWS credentials)
pytest -m cloud
```

---

## 3. Detailed Section Breakdown

### Section 0: Environment, Dependencies & Configuration
* **Objective:** Establish the pure-Python runtime environment, pinned dependency configuration, directory structure, and environment validation.
* **Files Created/Modified:**
  * `pyproject.toml`: Pinned dependencies (`fastapi`, `uvicorn[standard]`, `jinja2`, `psycopg[binary,pool]`, `langchain-cockroachdb`, `langchain-core`, `langgraph`, `boto3`, `httpx`, `pytest`, `pytest-asyncio`).
  * `coordinator/config.py`: Typed Pydantic configuration validating database connection strings, AWS Bedrock model IDs, S3 bucket names, MCP cluster IDs, and `DEMO_AUTO_RECONCILE` flag.
  * `.env.example`: Template for environment variables.
* **Automated Verification (`pytest tests/test_environment.py`):**
  * Verifies all third-party package imports.
  * Verifies config validation, default values, and missing-secret error handling.

---

### Section 1: CockroachDB Schema & Async Connection Pool
* **Objective:** Implement the version-verified CockroachDB native schema, connection pool management, serializable retry handling, and changefeed DDL.
* **Files Created/Modified:**
  * `coordinator/schema.sql`: Full SQL schema with:
    - `service_contracts` (stable identity) & `service_contract_revisions` (immutable revisions with native `VECTOR INDEX`)
    - `service_contract_consumers` & `task_contract_dependencies` (authoritative relational dependency graph)
    - `active_agent_tasks` & `drift_events`
    - `coordinator_outbox` & `event_inbox`
    - `contract_audit_history` & `deployments`
    - `contract_drift_audit` & `contract_publication_audit` read-only views for MCP.
  * `coordinator/db.py`: Async connection pool (`psycopg.AsyncConnectionPool`) with automatic retry wrapper for CockroachDB serializable transaction retries (error code `40001`).
  * `infra/cockroach/changefeed.sql`: Version-verified `CREATE CHANGEFEED FOR TABLE coordinator_outbox INTO 'webhook-https://...' WITH ...` DDL with webhook auth headers.
* **Automated Verification (`pytest tests/test_schema_and_db.py`):**
  * Verifies schema DDL execution and view definitions.
  * Verifies serializable retry wrapper on simulated concurrency conflicts.

---

### Section 2: Scaffold Sibling Repositories (`billing-service` & `orders-service`)
* **Objective:** Create the genuine sibling Git repositories with real Pydantic schemas, endpoints, clients, and contract compatibility test scenarios.
* **Files Created/Modified:**
  * `repos/billing-service/`: Git repository initialized with:
    - `schemas_v1.py`: `ChargeRequest(amount: int, currency: str, card_token: str)`
    - `schemas_v2.py`: `ChargeRequest(amount: int, currency: str, payment_method_id: str)`
    - `main.py`: FastAPI payment service.
  * `repos/orders-service/`: Git repository initialized with:
    - `clients/billing_client.py`: Python client calling `billing-service:POST /v1/charges` (v1 and v2 implementations).
    - `main.py`: FastAPI checkout service.
    - `tests/test_contract_scenarios.py`: 3 explicit contract test scenarios:
      1. Scenario 1: `billing_v1 + orders_v1` $\rightarrow$ **PASS**
      2. Scenario 2: `billing_v2 + orders_v1` $\rightarrow$ **Expected Compatibility Failure** (asserts 422 validation error on missing `payment_method_id`)
      3. Scenario 3: `billing_v2 + orders_v2` $\rightarrow$ **PASS**
* **Automated Verification (`pytest repos/orders-service/tests/test_contract_scenarios.py`):**
  * Asserts: `12 contract assertions passed, 3 integration tests passed, 0 unresolved breaking changes`.

---

### Section 3: Deterministic Schema Differencing Engine
* **Objective:** Build a structural Pydantic and JSON schema differencing engine that classifies breaking vs non-breaking contract evolutions.
* **Files Created/Modified:**
  * `coordinator/differencer.py`: Schema differencer detecting:
    - Removed fields (`card_token`)
    - Newly added required fields (`payment_method_id`)
    - Field type mutations.
* **Automated Verification (`pytest tests/test_differencer.py`):**
  * Verifies detection of breaking changes between `schemas_v1.py` and `schemas_v2.py`.
  * Verifies non-breaking changes (optional fields with defaults).

---

### Section 4: Atomic Contract Publication & Outbox Engine
* **Objective:** Implement single-transaction contract publication that extracts schemas directly from service Git commits and atomically writes contract revisions, audit logs, and outbox events.
* **Files Created/Modified:**
  * `coordinator/contract_registry.py`:
    - Inspects service repo Git commit to extract live Pydantic models.
    - Single transaction:
      ```sql
      BEGIN;
        INSERT INTO service_contract_revisions (...) VALUES (...);
        INSERT INTO coordinator_outbox (event_type, payload) VALUES ('CONTRACT_CHANGED', ...);
        INSERT INTO contract_audit_history (...) VALUES (...);
      COMMIT;
      ```
* **Automated Verification (`pytest tests/test_atomic_outbox.py`):**
  * Verifies atomic commit across `service_contract_revisions`, `coordinator_outbox`, and `contract_audit_history`.
  * Verifies that a simulated database failure rolls back all three tables with zero partial state.

---

### Section 5: LangChain × CockroachDB Semantic Memory & Checkpoints
* **Objective:** Connect `langchain-cockroachdb` using a dedicated LangChain-managed vector table (`semantic_memory`) for candidate discovery and LangGraph checkpoint persistence, while keeping `psycopg` relational queries authoritative.
* **Files Created/Modified:**
  * `coordinator/memory.py`:
    - `semantic_memory` vector table (`memory_id`, `text`, `embedding`, `metadata JSONB`).
    - `AsyncCockroachDBVectorStore`: Populates contract summaries with metadata `{"memory_type": "service_contract", "contract_revision_id": "...", "service_name": "billing-service", "endpoint": "POST /v1/charges"}`.
    - Candidate discovery: Vector search retrieves candidate `contract_revision_id` $\rightarrow$ `psycopg` queries `service_contract_consumers` to authoritatively confirm dependency.
    - `AsyncCockroachDBSaver`: Persists LangGraph execution checkpoints so paused runs can resume safely.
* **Automated Verification (`pytest tests/test_semantic_memory.py`):**
  * Verifies semantic vector search discovers Billing charge contract given an Orders prompt.
  * Verifies LangGraph agent checkpoint persistence and resumption from CockroachDB.

---

### Section 6: Changefeed Receiver, Inbox Lease & Drift Worker
* **Objective:** Build the asynchronous event spine with an explicit worker lease claiming protocol, deduplication, and derived drift generation.
* **Files Created/Modified:**
  * `coordinator/drift_worker.py`: Background worker implementing the atomic lease claiming protocol:
    1. Select `RECEIVED` event and atomically mark it `PROCESSING` (`FOR UPDATE SKIP LOCKED` or version CAS).
    2. Query `task_contract_dependencies` for in-flight tasks assuming older revisions.
    3. Run `differencer.py` to compute breaking schema diffs.
    4. Insert `drift_events` in `ACTIVE_INTERVENTION` state and write `TASK_REPLAN_REQUIRED` outbox event.
    5. Mark inbox event `PROCESSED`. On failure, increment `attempt_count` and record `last_error`.
  * `coordinator/app.py` (Endpoint `POST /events/cockroach`): Webhook endpoint authenticating incoming changefeed payloads and durably inserting into `event_inbox` (`ON CONFLICT (event_id) DO NOTHING`) before returning HTTP 200.
* **Automated Verification (`pytest tests/test_drift_worker.py`):**
  * Verifies duplicate changefeed deliveries result in exactly one processed drift event.
  * Verifies atomic event claiming and retry increment on simulated worker failure.

---

### Section 7: Checkpoint-Aware Agent Runner & Reconciliation
* **Objective:** Implement the agent runner that executes optimistically in isolated Git worktrees, pauses at safe milestones upon detecting drift, and supports human approval for breaking changes.
* **Files Created/Modified:**
  * `coordinator/reconciliation.py`: Checkpoint evaluator managing the state machine:
    $$\text{REPLAN\_REQUIRED} \longrightarrow \text{REPLANNING} \longrightarrow \text{AWAITING\_APPROVAL} \longrightarrow \text{RECONCILED}$$
    - If `DEMO_AUTO_RECONCILE=true`, automatically applies the approved adaptation for the 3-minute video demo.
    - In standard mode, requires human/maintainer approval before write claims/promotion.
  * `coordinator/agent_runner.py`: Worktree-isolated execution runner (supporting deterministic scripted mode for repeatable demo and Amazon Bedrock mode).
* **Automated Verification (`pytest tests/test_reconcile.py`):**
  * Verifies Agent B pauses at checkpoint when Billing Revision 2 is published.
  * Verifies structured `REPLAN_REQUIRED` diff payload delivery and transition to `RECONCILED`.

---

### Section 8: Deployment Promotion, Process Supervision & Live Reload
* **Objective:** Implement the promotion pipeline that validates test gates, atomically promotes commits to `/live/{service}`, manages process restarts, and triggers browser reload.
* **Files Created/Modified:**
  * `coordinator/deployer.py`:
    - Runs unit and contract tests in worktree.
    - Promotes tested commit to `/live/{service}`.
    - Manages live process supervision (controlled Uvicorn process restart / signal reload).
    - Runs health checks on live endpoints; rolls back to previous commit if health check fails.
    - Bumps `reload_version` in `deployments` and writes `DEPLOYMENT_COMPLETED`.
  * `coordinator/app.py` (Endpoint `GET /demo/version`): Polled by client browsers to trigger automated reload.
* **Automated Verification (`pytest tests/test_deployment.py`):**
  * Verifies tested commit promotion and `reload_version` increment.
  * Verifies rollback to prior commit on simulated health check failure.

---

### Section 9: 3-Panel Public Control Dashboard (FastAPI + Jinja2 + CSS/JS)
* **Objective:** Build the responsive, obsidian/slate 3-panel mission control interface served directly by FastAPI at `/control` and `/`.
* **Files Created/Modified:**
  * `coordinator/templates/index.html` & `base.html`: 3-panel UI layout (Contract Mesh & Semantic Memory | Agent Execution & Checkpoints | Transactional Outbox & Audit Ledger).
  * `coordinator/static/style.css`, `app.css` & `app.js`: Secure operator tokens, real-time polling of `/api/dashboard/state`, `/api/tasks`, `/api/events`, failure-aware database status banners, semantic search, simulation triggers, and auto-reload on `/deploy/version` bumps.
  * `coordinator/app.py`: REST & UI endpoints:
    - `GET /control` & `GET /`: Serves 3-panel mission control dashboard.
    - `GET /api/dashboard/state`: Live state aggregator for zero-flicker UI hydration with DB health checks.
    - `GET /api/tasks` & `GET /tasks`: Active in-flight agent tasks and checkpoint states.
    - `GET /api/events`: Combined outbox and drift events feed.
    - `POST /api/semantic-search`: CockroachDB native vector search (fail-closed in non-demo mode).
    - `POST /api/simulate/drift`: Simulation trigger for billing v2 breaking drift.
    - `POST /api/simulate/reconcile`: Simulation trigger for orders agent reconciliation task.
    - `POST /tasks/{task_id}/approve` & `POST /tasks/{task_id}/reject`: Human-in-the-loop approval endpoints.
    - `GET /deploy/version` & `GET /demo/version`: Monotonic reload version endpoints.
* **Automated Verification (`pytest tests/test_ui.py`):**
  * Verifies all dashboard HTML endpoints, static assets, state aggregators, simulation triggers, vector search, and human approval flows.


---

### Section 10: Managed MCP Audit Role, ccloud Evidence & Packaging
* **Objective:** Configure the cluster-scoped read-only MCP audit role, create reproducible `ccloud` JSON evidence scripts, non-blocking S3 receipt archiver, and final documentation.
* **Files Created/Modified:**
  * `coordinator/receipt_archiver.py`: Asynchronous, non-blocking S3 audit receipt archiver (S3 failure never blocks coordinator path).
  * `infra/ccloud/inspect_cluster.py`: Script capturing sanitized `ccloud` cluster JSON evidence.
  * `infra/skills/`: Runbook notes for CockroachDB Agent Skills.
  * `README.md` & `LICENSE`: Comprehensive hackathon submission documentation.
* **Automated Verification (`pytest tests/test_mcp_audit.py`):**
  * Verifies read-only SQL queries against `contract_drift_audit` and `contract_publication_audit`.
  * Verifies S3 receipt generation and non-blocking failure recovery.
* **Manual Acceptance Verification:**
  * Connect Claude Code / Cursor / VS Code to `https://cockroachlabs.cloud/mcp` with `mcp-cluster-id`.
  * Execute natural language query: *"Show all Billing contract revisions that caused an Orders task to re-plan today."*
  * Verify live audit response directly from CockroachDB.
