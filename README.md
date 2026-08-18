# CodeClaim: CockroachDB Semantic Outbox × Checkpoint-Aware Multi-Agent Code Repair

[![Tests](https://img.shields.io/badge/pytest-164%20passed-10b981.svg)](tests/)
[![CockroachDB](https://img.shields.io/badge/CockroachDB-Transactional%20CDC%20%26%20Vector-6933ff.svg)](https://www.cockroachlabs.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **CockroachDB Hackathon 2026 Submission**  
> *A distributed transactional semantic memory plane and checkpoint-aware coordinator for multi-agent microservice code repair.*

---

## 🌟 The Problem: Multi-Agent Microservice Chaos

When autonomous coding agents work on interdependent microservices in parallel, traditional coordination paradigms fail:
1. **Silent Contract Drift**: Agent A mutates an upstream service API (e.g., `billing-service` v1 $\rightarrow$ v2); Agent B on `orders-service` continues writing code against outdated assumptions, causing runtime cascading failures.
2. **Dual-Write Vulnerabilities**: Publishing contract revisions and event notifications across separate database and message bus operations leads to inconsistent distributed state upon crashes.
3. **Destructive Worktree Collisions**: Multiple agents modifying the same repository overwrite uncommitted changes without isolated sandbox worktrees or rollback checkpoints.
4. **Missing Proof of Correctness**: Automated code generation without rigorous AST static analysis, strict test execution gates, and cryptographic audit trails cannot be safely deployed.

---

## 💡 The Solution: CockroachDB as Distributed Agent Memory

**CodeClaim** transforms **CockroachDB Dedicated/Serverless** into an active, multi-agent coordination plane:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        COCKROACHDB TRANSACTIONAL MEMORY PLANE                          │
│                                                                                        │
│  ┌───────────────────────────────┐   ┌───────────────────────────┐   ┌──────────────────────┐  │
│  │  service_contract_revisions   │   │      semantic_memory      │   │  coordinator_outbox  │  │
│  │ (Serializable Immut.)         │   │ (1536-dim Vector Cosine)  │   │  (Transactional CDC) │  │
│  └───────────┬───────────────────┘   └─────────────┬─────────────┘   └──────────┬───────────┘  │

└──────────────┼─────────────────────────────┼────────────────────────────┼──────────────┘
               │ 1. Atomic Publication       │ 2. Vector Semantic Query   │ 3. CDC Stream
               ▼                             ▼                            ▼
┌──────────────────────────────┐ ┌──────────────────────────┐ ┌──────────────────────────┐
│ Agent A (Billing Producer)   │ │ 3-Panel Control Mesh UI  │ │ Agent B (Orders Consumer)│
│ • Sandboxed AST extraction   │ │ • Semantic Vector Search │ │ • LangGraph Checkpointer │
│ • Single-TX contract commit  │ │ • Live Outbox Timeline   │ │ • Rewind & Re-plan       │
│ • Git commit SHA provenance  │ │ • Human Sign-off Modals  │ │ • Isolated UUID Worktree │
└──────────────────────────────┘ └──────────────────────────┘ └────────────┬─────────────┘
                                                                           │ 4. Test Gate & Cutover
                                                                           ▼
                                                              ┌──────────────────────────┐
                                                              │ Deployer & Process Sup.  │
                                                              │ • Journaled directory cut│
                                                              │ • Readiness verification │
                                                              │ • SHA-256 Audit Receipt  │
                                                              └──────────────────────────┘
```

---

## 🎯 System Boundaries & Protocol Support

### ✅ Supported Now (Production-Grade v1)
- **Internal Python Services**: Microservices built with **FastAPI** and **Pydantic** models.
- **Protocols & Formats**: Synchronous **HTTP/JSON** APIs.
- **Contract Extraction**: Deterministic **OpenAPI-based contract extraction** via `codeclaim onboard` (importing `app.openapi()` or querying loopback `/openapi.json`).
- **Harness Integrations**: External coding harnesses (**Codex, Claude Code, Cursor**, or internal runners) integrated deterministically through the authenticated **CodeClaim REST API & MCP Server**.
- **Transactional Memory Plane**: **CockroachDB** is the sole authoritative transactional memory plane for contracts, confirmed dependencies, checkpoint metadata, append-only audit lineage, and outbox streams.

### 🔮 Future Roadmap Only (Explicitly Deferred)
- **gRPC / Protobuf**: Interface extraction and protocol buffer compatibility analysis.
- **GraphQL**: Schema and operation AST extraction.
- **Events & Message Queues**: Event-driven architectures and **AsyncAPI** contract schemas.
- **Polyglot & Framework Adapters**: Non-Python ecosystems (TypeScript/Node.js, Go, Rust, Java/Kotlin) and other Python frameworks (Django, Flask).
- **Third-Party Dependencies**: External SaaS/vendor SDK monitoring and public API documentation scraping.

> [!IMPORTANT]
> **No Automatic Support Claims**: CodeClaim does not claim automatic support for unsupported frameworks, languages, or protocols without a tested, deterministic machine-readable contract adapter.  
> **Authoritative Memory vs. Semantic Discovery**: CockroachDB is the **authoritative transactional system of record** for all contracts, dependencies, audit events, and outbox queues. Bedrock vector embeddings (Cohere Embed v4) provide **optional semantic discovery only** (surfacing candidate endpoints via natural language search). Embeddings **never prove compatibility** and **never register dependencies automatically**.

---

## 🔑 Key Invariants & Capabilities

### 1. Authoritative CockroachDB Memory Plane & Transactional Outbox
CockroachDB provides serializable transactions for multi-statement atomic writes. Contract revisions, AST-extracted schema definitions, confirmed dependencies, append-only audit history, and outbox notification records are written in a **single serializable CockroachDB transaction**. If any validation step fails, the entire transaction rolls back—guaranteeing zero dual-write inconsistencies.

### 2. Optional Semantic Discovery (Bedrock Embeddings)
Microservice schemas and natural language capability descriptions are converted to embeddings (via AWS Bedrock Cohere Embed v4) and indexed using CockroachDB's native **vector data types (`VECTOR(1536)`)** and **Cosine Distance (`<=>`) operators**. This allows developers to discover candidate contracts via natural language search. **Embeddings are auxiliary discovery only**; exact HTTP paths, Pydantic types, and deterministic OpenAPI schema diffs in CockroachDB remain the authoritative source of truth.

### 3. CDC Changefeed & Real-Time Drift Worker
CockroachDB transactional rangefeeds stream outbox mutations into an idempotent `event_inbox`. The **Drift Worker** runs an AST structural JSON schema differencer to detect breaking changes (field removals, required parameter additions, enum mutations) and interrupts downstream agents.

### 4. Checkpoint-Aware Agent Recovery (LangGraph Checkpointer)
When breaking drift is intercepted, the consumer agent (Agent B) rewinds to its last verified checkpoint, generates a UUID-isolated git worktree, synthesizes adaptive client code, and runs a sandboxed `pytest` test gate. It enters an `AWAITING_APPROVAL` state requiring operator sign-off before promotion.

### 5. Durable Cutover Journal & Fail-Closed Startup Recovery
Promotions use a filesystem cutover journal (`.cutover_journal.json` with `os.fsync`) and atomic directory renames. If a crash occurs during deployment, the coordinator recovers previous backups on startup and readiness-checks the service before accepting traffic.

### 6. Managed Model Context Protocol (MCP) Read-Only Audit Role
Provisions a cluster-scoped read-only `mcp_audit_agent` role with tailored relational views (`contract_drift_audit`, `contract_publication_audit`). Developers connect Cursor or Claude Code to CockroachDB Managed MCP for natural language queries over the entire distributed coordination history.

### 7. Cryptographic SHA-256 Execution Receipts & Non-Blocking S3 Archival
Generates tamper-evident execution receipts binding contract versions, git commit SHAs, test evidence, and operator sign-offs with SHA-256 hashes, persisted locally and asynchronously uploaded to S3.

### 8. Harness-Neutral Compatibility Dispatch
CodeClaim coordinates compatibility work between distinct, internal Python microservice repositories (FastAPI + Pydantic HTTP/JSON). External harnesses operate in parallel in isolated worktrees, while CockroachDB records the exact provider/consumer interface, revision assumption, source-file evidence, and confirmation state. The coordinator never source-locks symbols, auto-merges, or deploys a harness result.

Compatibility work is created transactionally while the coordinator processes the committed contract-change outbox event. Uncertain semantic compatibility is fail-closed as `REVIEW_REQUIRED`. When a harness reports `BLOCKED` or `INCOMPATIBLE` (e.g., guest checkout cannot supply required `customer_id`), worktrees are preserved, audit events are recorded, and the incident transitions to `Human decision required`.

### 9. Asynchronous Slack Notifications (Hackathon Scope)
Slack is an optional asynchronous projection of `coordinator_outbox`, not a source of truth. Deliveries are filtered strictly for: (1) breaking contract published, (2) compatibility work created / replan required, and (3) compatibility blocked. Failures only update the delivery retry ledger in CockroachDB and never roll back transactions. Sensitive data (source code, customer records, secrets, prompts, CoT) is strictly scrubbed.

---

## 📂 Repository Architecture

```text
code-claim/
├── coordinator/
│   ├── app.py                   # FastAPI Control Mesh coordinator & webhook ingestion
│   ├── config.py                # Environment configuration & fail-closed runtime validation
│   ├── contract_registry.py     # Sandboxed AST extractor & atomic publication engine
│   ├── db.py                    # psycopg pool management & serialization retry wrappers
│   ├── deployer.py              # Journaled atomic cutover, test gates & process supervisor
│   ├── differencer.py           # Deterministic AST JSON schema differencing engine
│   ├── drift_worker.py          # CDC changefeed consumer & drift detection loop
│   ├── compatibility.py         # Harness registrations, work-item state, evidence recording
│   ├── compatibility_dispatcher.py # Durable poll/webhook delivery loop
│   ├── mcp_server.py            # Trusted local CodeClaim MCP integration surface
│   ├── memory.py                # LangChain & CockroachDB native vector semantic memory
│   ├── receipt_archiver.py      # Cryptographic SHA-256 execution receipt generator & S3 archiver
│   ├── reconciliation.py        # LangGraph checkpointer & human-in-the-loop state machine
│   ├── schema.sql               # CockroachDB schema with vector indexes & CDC changefeeds
│   ├── static/                  # Vanilla CSS/JS frontend (secure sessionStorage tokens)
│   └── templates/               # Jinja2 3-panel control dashboard template
├── infra/
│   ├── ccloud/                  # CockroachDB Cloud cluster setup & inspection scripts
│   │   ├── inspect_cluster.py   # Sanitized cluster metadata & evidence extractor
│   │   ├── provision_changefeed.sql # MCP audit role & CDC changefeed definitions
│   │   └── setup_cluster.sh     # Dedicated cluster provisioning automation
│   └── skills/
│       └── cockroach_mcp_audit.md # Managed MCP audit role runbook for Claude Code / Cursor
├── repos/
│   ├── billing-service/         # Upstream microservice (FastAPI + Pydantic v1/v2 contracts)
│   └── orders-service/          # Downstream consumer microservice (Client adaptors & tests)
├── scripts/
│   └── run_demo.py              # Autonomous 7-step end-to-end scenario runner
└── tests/                       # Comprehensive verification suite (88 passing tests)
    ├── test_agent_runner.py     # Worktree isolation & reconciliation tests
    ├── test_atomic_outbox.py    # AST extraction & atomic transactional outbox tests
    ├── test_deployer.py         # Cutover crash recovery & process supervision tests
    ├── test_differencer.py      # Breaking vs non-breaking schema diff tests
    ├── test_drift_worker.py     # CDC changefeed auth, idempotency & drift detection tests
    ├── test_environment.py      # Config validation & directory structure tests
    ├── test_mcp_audit.py        # Cryptographic receipts & MCP audit role tests
    ├── test_schema_and_db.py    # Migration checksums & transaction retry tests
    ├── test_semantic_memory.py  # Native vector search & LangGraph checkpointer tests
    └── test_ui.py               # 3-panel dashboard HTML, static assets & API tests
```

---

## 🚀 Quickstart & Local Setup

### 1. Prerequisites
- Python 3.11+
- Git
- (Optional for Live DB) CockroachDB v24.1+ (or CockroachDB Serverless/Dedicated instance)

### 2. Installation
```bash
# Clone repository
git clone https://github.com/your-org/code-claim.git
cd code-claim

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .\.venv\Scripts\activate

# Install dependencies
pip install -e .
```

### 3. Run the Comprehensive Test Suite
```bash
# Run all unit tests
pytest -v
```

---

## 🎬 End-to-End Demo Execution

To see CodeClaim run the full 7-step autonomous reconciliation cycle (Billing v2 breaking mutation $\rightarrow$ CDC Changefeed $\rightarrow$ Agent B Worktree Adaptation $\rightarrow$ Pytest Gate $\rightarrow$ Human Approval $\rightarrow$ Atomic Cutover $\rightarrow$ Cryptographic Receipt):

```bash
python scripts/run_demo.py
```

The scripted command is the offline demonstration path. It intentionally uses
the local simulation adapter and may inject a synthetic changefeed event. The
live harness path is explicit and never enables demo mode:

```powershell
$env:IS_DEMO_MODE="false"
$env:DEMO_AUTO_RECONCILE="false"
$env:CODECLAIM_BASE_URL="https://your-coordinator.example"
$env:COORDINATOR_API_KEY="<operator-secret>"
$env:BILLING_HARNESS_ID="<billing-harness-id>"
$env:BILLING_HARNESS_TOKEN="<billing-one-time-token>"
$env:ORDERS_HARNESS_ID="<orders-harness-id>"
$env:ORDERS_HARNESS_TOKEN="<orders-one-time-token>"
# For the checked-in Billing demo service, run the deployed v2 process.
$env:BILLING_CONTRACT_REVISION="v2"
$env:ORDERS_WORKTREE_PATH="C:\worktrees\orders-live"
python scripts/live_harness_scenario.py --manual
```

The complete live setup, harness registration, token rotation, changefeed,
Antigravity, and Codex integration runbook is in
[`LIVE_TESTING.md`](LIVE_TESTING.md). Fresh harness identities can be created
without running the scenario by using:

```powershell
python scripts/live_harness_scenario.py --register-only
```

Before any live workflow writes, run the read-only baseline guard:

```powershell
python scripts/live_preflight.py --public-base-url https://<ngrok-host>
```

The live script uses only coordinator REST operations: it registers/uses two
harnesses, creates and completes the historical Orders task, publishes Billing
revision 2, claims the late compatibility work, records evidence, proves that
provider deployment is rejected before approval, atomically rebinds the
dependency on approval, and retries deployment. With `--manual`, a real Codex
or Antigravity session performs the Orders edit between the claim and evidence
steps. Set `CODECLAIM_REGISTER_HARNESSES=true` instead of supplying harness
IDs/tokens when registering fresh harnesses; the returned tokens are shown once.

### Output Preview:
```text
================================================================================
🚀 CodeClaim: CockroachDB Semantic Outbox × Multi-Agent Code Repair Demo
================================================================================

▶ [Step 1/7] Initializing Baseline Microservice Mesh...
[INFO] Billing-Service base commit: 3f9a12c4
[INFO] Orders-Service base commit:  8b1c90ef
[INFO] Current Reload Version:       v1

▶ [Step 2/7] Publishing Billing-Service v2 (Atomic Transactional Outbox)...
[INFO] Contract v2 Published! Revision: 2 | Status: PUBLISHED | Outbox Event ID: evt-48f1...

▶ [Step 3/7] Processing CDC Changefeed Stream & Detecting Breaking Drift...
[INFO] CDC Events Processed: 1 | Succeeded: 1 | Failed: 0

▶ [Step 4/7] Launching Agent B (Orders Consumer) in Isolated Worktree...
[INFO] Agent Task ID: task-90ab... | Status: AWAITING_APPROVAL
[INFO] Worktree Path: worktrees/task-orders-checkout-90ab
[INFO] Pytest Verification Evidence: Returncode 0 | All Passed: True

▶ [Step 5/7] Human Operator Review & Sign-off on Reconciled Plan...
[INFO] Human Approval Applied! Task task-90ab... transitioned to RECONCILED (Plan Rev: 2)

▶ [Step 6/7] Executing Atomic Cutover & Supervised Deployment Promotion...
[INFO] Deployment Cutover Complete! Status: HEALTHY | Reload Version: v1 -> v2

▶ [Step 7/7] Generating & Archiving Cryptographic Audit Receipt...
[INFO] Audit Receipt Generated! ID: rcpt-12ef84a92bc1
[INFO] SHA-256 Integrity Hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
[INFO] Local Receipt Path:      receipts/rcpt-12ef84a92bc1.json

================================================================================
🎉 DEMONSTRATION SUCCESSFUL: All 7 lifecycle phases executed cleanly!
================================================================================
```

---

## 🖥️ React Control Mesh Dashboard UI

The dashboard is now a React/Vite application using source-owned shadcn-compatible
primitives, with the 7Ovr registry configured in `frontend/components.json` for
future block installation. FastAPI remains the same-origin host and CockroachDB
remains the only coordination source of truth.

Build the frontend after installing Python dependencies:

```powershell
cd frontend
npm install
npm run build
cd ..
```

The production bundle is emitted to `coordinator/static/dashboard/` and is served
at `http://localhost:8000/control` and `http://localhost:8000/`.

For frontend-only development, run Vite in one terminal and the coordinator in another:

```powershell
cd frontend
npm run dev
```

Then open `http://localhost:3000`. Vite proxies coordinator APIs to port 8000.

Launch the coordinator to access the interactive dashboard:

**Windows (PowerShell):** use the project launcher so Psycopg runs on a
selector event loop. Psycopg's async driver cannot run on Windows'
default Proactor event loop.

```powershell
.\.venv\Scripts\python.exe scripts\start_server.py
```

**Linux/macOS:**

```bash
uvicorn coordinator.app:app --host 0.0.0.0 --port 8000 --reload
```

If a Windows server is started with the `uvicorn` console command directly,
startup can fail with `Psycopg cannot use the 'ProactorEventLoop'` and the
dashboard will report the coordinator as unavailable. The launcher creates
the compatible selector loop before Uvicorn starts.

Navigate to **`http://localhost:8000/control`** or **`http://localhost:8000/`** to interact with:
- **Panel 1 (Left)**: Registered microservice hierarchy, Contract Publication Timeline, and Semantic Vector Explorer.
- **Panel 2 (Center)**: In-flight multi-agent tasks, Compatibility Work state machine, and Structured Blocked Incidents with **Approve/Reject** human sign-off modals.
- **Panel 3 (Right)**: Real-time transactional outbox timeline, visual breaking diff viewer, Cross-Service Audit Lineage, and deployment ledger.

---

## 🌐 Hosting & Deployment Topology

The dashboard is hosted directly by the same **CodeClaim Coordinator** deployment on AWS:
- **Unified FastAPI Deployment**: FastAPI serves both the coordinator REST APIs / background workers and the dashboard HTML / static assets (`/static`, `/`, `/control`).
- **Same-Origin Access**: The dashboard communicates exclusively through same-origin relative paths (e.g. `/api/dashboard/state`, `/deploy/version`), eliminating the need for a separate frontend server or CORS exposure.
- **Background Worker Co-location**: The real-time drift worker, compatibility dispatcher, and Slack notifier run as supervised background tasks within the single coordinator process lifecycle.
- **External Source of Truth**: **CockroachDB Cloud** remains the authoritative external transactional memory plane.
- **Reverse Proxy Protection**: The coordinator is exposed through an HTTPS reverse proxy (Nginx or AWS Application Load Balancer); the internal FastAPI Uvicorn port (`8000`) is kept private on loopback.

```text
https://codeclaim.example.com
          |
          v
AWS EC2 / container
  ├─ HTTPS reverse proxy (Nginx / ALB on port 443)
  └─ FastAPI CodeClaim coordinator (127.0.0.1:8000)
      ├─ React dashboard bundle (served from /static/dashboard)
      ├─ coordinator APIs (/api/dashboard/state, /tasks, /deploy, etc.)
      ├─ drift worker (supervised background task)
      ├─ compatibility dispatcher (supervised background task)
      └─ Slack notifier (supervised background task)
          |
          v
CockroachDB Cloud
```

Nginx reverse proxy configuration template is available in [`infra/nginx.conf`](infra/nginx.conf), and the unified containerfile is in [`infra/Dockerfile`](infra/Dockerfile).

---

## 🛡️ Managed CockroachDB MCP Audit Setup

To connect Claude Code or Cursor to the cluster-scoped read-only audit plane:

1. Follow the runbook in [`infra/skills/cockroach_mcp_audit.md`](infra/skills/cockroach_mcp_audit.md).
2. Configure MCP server connection string:
   ```json
   {
     "mcpServers": {
       "cockroach-codeclaim-audit": {
         "command": "npx",
         "args": [
           "-y",
           "@cockroachdb/mcp-server",
           "--connection-string",
           "postgresql://mcp_audit_agent:<MCP_AUDIT_PASSWORD>@codeclaim-prod.cockroachlabs.cloud:26257/codeclaim_db?sslmode=verify-full"
         ]
       }
     }
   }
   ```
3. Ask your AI assistant:
   > *"Show all Billing-Service contract revisions that caused Orders-Service to re-plan today."*

## 🔌 Connecting a Coding Harness

There are two MCP surfaces with deliberately different authority:

- **CockroachDB Managed MCP:** read-only audit and lineage inspection.
- **CodeClaim MCP:** trusted local coordination tools for a coding harness. It does not expose direct database writes.

For an HTTPS-based harness integration, an operator first registers the service owner at `POST /harnesses/register`. Store the returned one-time harness token in that runner's secret store. The runner calls `POST /harnesses/{harness_id}/compatibility-work/claim`, works in its own isolated worktree, then sends passing test evidence to the result endpoint. Webhook registration is optional; polling is the safe default.

The local CodeClaim MCP server is a trusted stdio process that authenticates
one harness from `MCP_HARNESS_ID` and `MCP_HARNESS_TOKEN` and connects directly
to CockroachDB. It is not a proxy for `CODECLAIM_BASE_URL`; use the REST
surface when agent machines must not receive database credentials. Generated
MCP configurations are redacted by default and should be stored outside the
repository.

To use Cohere Embed v4 through Bedrock Runtime, set:

```dotenv
BEDROCK_EMBEDDING_PROVIDER="cohere_v4"
BEDROCK_EMBEDDING_MODEL_ID="cohere.embed-v4:0"
EMBEDDING_DIMENSION=1536
```

### Endpoint retirement is explicit

CodeClaim never infers that a removed endpoint is harmless. A provider harness must publish an inventory for each source commit and must retire a known endpoint deliberately through `retire_endpoint` (MCP) or `POST /contracts/retire`. Retirement appends an immutable tombstone revision, records the migration note and optional replacement, then emits `ENDPOINT_RETIRED` through the transactional outbox.

If an active contract is absent from the commit inventory and has no tombstone, `POST /contracts/inventory` emits `ENDPOINT_RETIREMENT_REVIEW_REQUIRED`. The drift worker creates review-required compatibility work and re-plans even agents that currently depend on the latest known revision. It does not automatically retire the endpoint: a human or provider harness must make that decision explicitly.

### Onboard an internal FastAPI service

`codeclaim onboard` supports Python FastAPI services only. It imports the configured FastAPI application to obtain its generated OpenAPI document, or reads `/openapi.json` from a loopback service. It prints the normalized HTTP contract plan and any dynamic route/header findings before doing anything. Without confirmation, it makes no database or filesystem changes.

```powershell
codeclaim onboard --service-name billing-service `
  --repository-path C:\work\billing-service `
  --endpoint-code-dir app\api `
  --app-entry app.main:app
```

For automation, pass `--yes`; the plan is still printed. After approval, the command registers the service, publishes revision 1 for each OpenAPI operation, writes only `.codeclaim/service.json`, and appends onboarding audit/outbox events. It never edits application source code.

### Confirm internal HTTP dependencies

Use `codeclaim dependencies` on a consumer repository to review literal Python HTTP client calls. The command shows the consumer, possible provider, source-file evidence, confidence, and exact provider-operation candidates. Choose **confirm**, **ignore**, or **edit** for each suggestion.

```powershell
codeclaim dependencies --consumer-service orders-service `
  --repository-path C:\work\orders-service `
  --endpoint-code-dir clients `
  --provider-service billing-service `
  --confirmed-by orders-owner
```

Only an explicit confirmation persists a dependency. A confirmed record binds one consumer to one provider contract ID, method/path, and assumed revision. Similarity search can surface candidates but never registers a dependency or decides compatibility.

---

## 📜 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
