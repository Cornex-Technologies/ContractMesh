# CodeClaim: Implementation Plan & Technical Specification (Production MVP)

**Project Name:** CodeClaim — Transactional Semantic Memory & Contract Mesh for Collaborative Coding Agents  
**Hackathon:** CockroachDB × AWS Hackathon — Build with Agentic Memory  
**Target Repository:** `C:\Users\dell\Desktop\Projects\code-claim`  
**License:** Apache 2.0  
**Technology Stack:** 100% Python (FastAPI + Jinja2 + Vanilla JS/CSS + psycopg + CockroachDB Cloud + Amazon Bedrock)  

---

## 0. Required Resources and How the Project Uses Them

The following resources are part of the implementation and judging evidence, not incidental reference links.

| Resource | Required use in CodeClaim |
| :--- | :--- |
| [Hackathon CockroachDB resources](https://cockroachdb-ai.devpost.com/resources#cockroachdb) | Verify the available CockroachDB capabilities and submission requirements. |
| [Hackathon AWS resources](https://cockroachdb-ai.devpost.com/resources#aws) | Select and document the AWS services used by the demo. |
| [About the sponsors](https://cockroachdb-ai.devpost.com/resources#about-the-sponsors) | Confirm sponsor-specific tooling and judging expectations. |
| [Hackathon FAQ](https://cockroachdb-ai.devpost.com/resources#faq) | Confirm eligibility, required APIs, and deployment constraints before submission. |
| [CockroachDB Cloud](https://cockroachlabs.cloud/signup) | Host the durable contract, dependency, agent-memory, audit, and CDC state. |
| [CockroachDB Cloud MCP Server](https://www.cockroachlabs.com/docs/cockroachcloud/connect-to-the-cockroachdb-cloud-mcp-server) | Give a human engineer a cluster-scoped, read-only view of contract drift and reconciliation history from Claude Code, Cursor, or VS Code. |
| [ccloud CLI](https://www.cockroachlabs.com/docs/cockroachcloud/ccloud-get-started) | Authenticate with a service account, inspect cluster state, apply infrastructure/bootstrap operations, and capture reproducible deployment evidence. |
| [CockroachDB Agent Skills](https://github.com/cockroachlabs/cockroachdb-skills/tree/main?tab=readme-ov-file) | Use the operational skills for onboarding, schema review, performance, security, observability, and resilience checks. |
| [CockroachDB vector/AI documentation](https://www.cockroachlabs.com/docs/v26.2/cockroachdb-and-ai.html) | Verify native `VECTOR` and vector-index behavior against the target cluster version. |
| [LangChain × CockroachDB](https://docs.langchain.com/oss/python/integrations/providers/cockroachdb) | Use `langchain-cockroachdb` for semantic contract retrieval and persisted agent/checkpoint memory while retaining direct `psycopg` transactions for coordination invariants. |

The application remains Python-only. Operational tooling such as `ccloud` and the CockroachDB Agent Skills installer is infrastructure/developer tooling, not a second application runtime.

### Application runtime dependencies

The initial `pyproject.toml` should pin compatible versions for:

```text
fastapi
uvicorn[standard]
jinja2
psycopg[binary,pool]
langchain-cockroachdb
langchain-core
langgraph
boto3
httpx
pytest
```

The Bedrock model ID, embedding model ID, CockroachDB connection string, AWS region, S3 bucket, and MCP cluster ID are environment configuration. They must not be embedded in source code.

### Required proof of usage

The final README and demo must show:

1. A CockroachDB Cloud cluster created and inspected using `ccloud`.
2. A vector retrieval query that discovers the Billing contract from an Orders request.
3. A persisted agent checkpoint or semantic memory record stored through `langchain-cockroachdb`.
4. A real CockroachDB changefeed event reaching the coordinator.
5. A real Claude Code, Cursor, or VS Code MCP query against the audit view using a cluster-scoped read-only connection.

## 1. Executive Summary & Master Positioning Statement

> **Master Positioning Statement:**  
> *"CodeClaim uses CockroachDB as a distributed transactional memory plane for agent coordination. Contract publication, contract audit history, and CDC outbox records are committed atomically. The coordinator then derives downstream compatibility findings from the committed contract event. CockroachDB changefeeds deliver those events, LangChain retrieves semantic contract memory, and relational dependency metadata verifies actual consumers. The managed MCP Server provides controlled, human-readable inspection of the contract and drift history."*

CodeClaim replaces fragile single-file pessimistic locks with **optimistic parallel execution across distinct microservice repositories**. When an upstream agent updates an API contract in `billing-service`, CockroachDB atomically records the mutation and outbox event. CockroachDB's changefeed streams the event to the coordinator, which identifies consuming downstream tasks (e.g. in `orders-service`), marks them `REPLAN_REQUIRED` at clean execution checkpoints, and provides a typed schema diff so the agent can adapt its client and pass integration tests prior to deployment.

---

## 2. Hackathon Requirements & Tools Compliance

| Requirement | Implementation in CodeClaim |
| :--- | :--- |
| **CockroachDB Tool 1: Native Vector Search** | Semantic candidate discovery using CockroachDB `VECTOR` data and the version-appropriate native vector index. The SQL migration is verified against the target Cloud cluster before use. |
| **CockroachDB Tool 2: Cloud Managed MCP Server** | Cluster-scoped, read-only inspection of `contract_drift_audit`, `service_contract_revisions`, and reconciliation history from Claude Code, Cursor, or VS Code. The MCP connection is explicitly scoped with `mcp-cluster-id`. |
| **CockroachDB Tool 3: `ccloud` CLI** | Service-account authentication, cluster inspection, bootstrap-setting verification, and reproducible infrastructure evidence. Exact flags are checked against the installed CLI version. |
| **CockroachDB Tool 4: Agent Skills Repo** | The CockroachDB skills repository is used during setup and review for schema, security, observability, and performance checks; it is not treated as an application runtime dependency. |
| **LangChain × CockroachDB** | `langchain-cockroachdb` provides the semantic vector store and persisted LangGraph checkpoint memory. Direct `psycopg` remains the authority for claims, contract publication, and outbox transactions. |
| **AWS Service 1: Amazon Bedrock** | Configurable Bedrock model ID for agent reasoning and code editing; configurable Bedrock embedding model for contract vectorization. Model names are environment configuration, not hard-coded product guarantees. |
| **AWS Service 2: AWS EC2** | Single-host deployment hosting the coordinator, sibling Git repositories, isolated task worktrees, live service checkouts, and demo services. |
| **AWS Service 3: Amazon S3** | Asynchronous archive of signed audit receipts and contract-diff snapshots after successful deployment. S3 failure never blocks the transactional coordinator path. |

---

## 3. System Architecture & Directory Layout

```
C:\Users\dell\Desktop\Projects\code-claim/
├── coordinator/                    # The Central Coordinator & Drift Engine (Python / FastAPI)
│   ├── app.py                      # FastAPI server (/control, /api/tasks, /events/cockroach, /demo/version)
│   ├── db.py                       # CockroachDB connection pool, native vector queries & atomic outbox
│   ├── memory.py                    # LangChain CockroachDB vector store & LangGraph checkpoint memory
│   ├── contract_registry.py         # Contract extraction, publication, and revision management
│   ├── reconciliation.py           # Checkpoint drift detector & re-planning manager
│   ├── differencer.py              # Deterministic Pydantic / JSON schema differencer
│   ├── drift_worker.py              # Idempotent event-inbox consumer and drift-finding worker
│   ├── deployer.py                 # Live checkout patch promotion, health check & reload manager
│   ├── receipt_archiver.py          # Non-blocking S3 audit-receipt archive worker
│   ├── agent_runner.py             # Checkpoint-aware Agent Execution Loop (Amazon Bedrock)
│   ├── schema.sql                  # Version-verified CockroachDB SQL schema & MCP views
│   ├── templates/                  # Jinja2 HTML templates for the 3-Panel Control UI
│   │   ├── base.html
│   │   └── control.html            # 3-Panel Dashboard (Fleet | Drift Stream | CockroachDB State)
│   └── static/                     # Vanilla CSS & JavaScript (polling /demo/version & /api/events)
│       ├── app.css
│       └── app.js
├── infra/
│   ├── cockroach/                   # Version-verified bootstrap, schema, and changefeed SQL
│   ├── ccloud/                      # Reproducible ccloud commands and captured JSON evidence
│   └── skills/                      # Setup notes for cockroachdb-skills
├── repos/                          # Separate Sibling Git Repositories
│   ├── billing-service/            # Independent git repo: billing API & schemas
│   │   ├── .git/
│   │   ├── schemas_v1.py           # ChargeRequest(amount, currency, card_token)
│   │   ├── schemas_v2.py           # ChargeRequest(amount, currency, payment_method_id)
│   │   └── main.py
│   └── orders-service/             # Independent git repo: orders checkout & client
│       ├── .git/
│       ├── clients/
│       │   └── billing_client.py   # Consumes billing service
│       ├── main.py
│       └── tests/
│           └── test_integration.py # Cross-service pytest suite
├── worktrees/                      # Isolated Server Worktrees (Task-Specific Working Trees)
│   ├── task-billing-401/           # Agent A worktree (isolated from live)
│   └── task-orders-102/            # Agent B worktree (isolated from live)
├── live/                           # Canonical Live Checkouts (Target of Approved Deployments)
│   ├── billing-service/
│   └── orders-service/
├── tests/
│   ├── test_differencer.py         # Schema differencer unit tests
│   ├── test_atomic_outbox.py        # Atomic publication transaction tests
│   ├── test_changefeed_inbox.py     # Duplicate delivery and retry behavior
│   ├── test_checkpoint_reconcile.py # Stale dependency checkpoint behavior
│   └── test_deployment.py           # Health check, version bump, and rollback behavior
├── pyproject.toml                   # Pinned Python runtime dependencies and test configuration
├── IMPLEMENTATION_PLAN.md          # This technical specification
├── README.md                       # Comprehensive open-source documentation
└── LICENSE                         # Apache 2.0 Open Source License
```

---

## 4. CockroachDB Native SQL Schema (`coordinator/schema.sql`)

```sql
-- Bootstrap separately through ccloud/SQL administration after verifying the
-- target CockroachDB Cloud version. Do not run cluster-setting statements as
-- ordinary application migrations:
-- SET CLUSTER SETTING feature.vector_index.enabled = true;

-- 1. Microservice Registry
CREATE TABLE microservices (
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING UNIQUE NOT NULL,
    repository_path STRING NOT NULL,
    primary_region STRING NOT NULL DEFAULT 'us-east-1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Stable Service Contract Identity
CREATE TABLE service_contracts (
    contract_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    endpoint_path STRING NOT NULL,
    http_method STRING NOT NULL,
    contract_key STRING NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Immutable Contract Revisions + Native Vector Memory
CREATE TABLE service_contract_revisions (
    contract_revision_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    revision_number INT8 NOT NULL,
    source_commit STRING NOT NULL,
    schema_json JSONB NOT NULL,
    semantic_summary STRING NOT NULL,
    summary_embedding VECTOR(1536),
    embedding_model STRING,
    embedding_dimension INT8,
    is_active BOOL NOT NULL DEFAULT true,
    published_by STRING NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_contract_revision UNIQUE (contract_id, revision_number),
    VECTOR INDEX (summary_embedding)
);

-- 4. Explicit Relational Dependency Graph
CREATE TABLE service_contract_consumers (
    consumer_service STRING NOT NULL,
    provider_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    consumer_repository STRING NOT NULL,
    consumer_file_path STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_service, provider_service, contract_id, consumer_repository)
);

-- 5. Active In-Flight Tasks & Intent
CREATE TABLE active_agent_tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id STRING NOT NULL,
    service_name STRING NOT NULL,
    task_prompt STRING NOT NULL,
    worktree_path STRING NOT NULL,
    base_commit STRING NOT NULL,
    plan_revision INT8 NOT NULL DEFAULT 1,
    status STRING NOT NULL DEFAULT 'OPTIMISTIC_EXECUTING', -- OPTIMISTIC_EXECUTING, REPLAN_REQUIRED, RECONCILED, COMPLETED, FAILED
    heartbeat_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    failure_reason STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6. Multi-Service Task Dependencies
CREATE TABLE task_contract_dependencies (
    task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    provider_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    assumed_revision INT8 NOT NULL,
    dependency_kind STRING NOT NULL DEFAULT 'HTTP_REST', -- HTTP_REST, GRPC, EVENT_PAYLOAD
    dependency_path STRING,
    PRIMARY KEY (task_id, provider_service, contract_id)
);

-- 7. Cross-Service Drift Events, derived asynchronously from CONTRACT_CHANGED
CREATE TABLE drift_events (
    drift_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_service STRING NOT NULL,
    target_task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id),
    target_service STRING NOT NULL,
    old_contract_revision INT8 NOT NULL,
    new_contract_revision INT8 NOT NULL,
    breaking_diff JSONB NOT NULL,
    status STRING NOT NULL DEFAULT 'ACTIVE_INTERVENTION', -- ACTIVE_INTERVENTION, RECONCILED, DISMISSED
    acknowledged BOOL NOT NULL DEFAULT false, -- human/UI acknowledgement; does not resolve the drift
    resolved_by STRING,
    resolution_summary STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 8. Transactional Outbox (CDC Changefeed Source)
CREATE TABLE coordinator_outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type STRING NOT NULL,       -- 'SERVICE_CONTRACT', 'TASK_STATE', 'DEPLOYMENT'
    aggregate_id UUID NOT NULL,
    aggregate_revision INT8 NOT NULL,
    source_service STRING NOT NULL,
    event_type STRING NOT NULL,           -- CONTRACT_CHANGED, DRIFT_DETECTED, TASK_REPLAN_REQUIRED, DEPLOYMENT_COMPLETED
    payload JSONB NOT NULL,
    event_version INT8 NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. Idempotent Ingestion Inbox
CREATE TABLE event_inbox (
    event_id UUID PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status STRING NOT NULL DEFAULT 'RECEIVED', -- RECEIVED, PROCESSED, FAILED
    attempt_count INT8 NOT NULL DEFAULT 0,
    last_error STRING,
    processed_at TIMESTAMPTZ
);

-- 10. Append-Only Contract Audit History
CREATE TABLE contract_audit_history (
    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type STRING NOT NULL,
    source_service STRING NOT NULL,
    target_service STRING,
    summary STRING NOT NULL,
    schema_diff JSONB,
    actor STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 11. Deployment State and Browser Reload Version
CREATE TABLE deployments (
    deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    source_commit STRING NOT NULL,
    status STRING NOT NULL, -- VALIDATING, DEPLOYING, HEALTHY, FAILED, ROLLED_BACK
    reload_version INT8 NOT NULL,
    health_check JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- 12. Dedicated Read-Only View for CockroachDB Managed MCP Server
CREATE VIEW contract_drift_audit AS
SELECT 
    d.drift_id,
    d.source_service,
    d.target_service,
    d.old_contract_revision,
    d.new_contract_revision,
    d.breaking_diff,
    d.status,
    d.created_at,
    d.reconciled_at
FROM drift_events d;

CREATE VIEW contract_publication_audit AS
SELECT
    history_id,
    event_type,
    source_service,
    target_service,
    summary,
    schema_diff,
    actor,
    created_at
FROM contract_audit_history;

-- Grant SELECT on this view to the MCP audit role only. The application role
-- must not receive UPDATE or DELETE privileges on contract_audit_history.
```

---

## 5. Memory, Changefeed, and Event-Processing Design

### LangChain × CockroachDB integration

Install the Python integration:

```text
pip install langchain-cockroachdb
```

Use it for the agentic-memory paths:

- `AsyncCockroachDBVectorStore` retrieves semantically related service contracts, prior drift findings, and resolution summaries.
- `AsyncCockroachDBSaver` persists LangGraph execution checkpoints so a worker can resume after a drift pause or process restart.
- `CockroachDBChatMessageHistory` may persist the user request and agent rationale for the audit timeline.

Use direct `psycopg` transactions for correctness-critical operations such as contract publication, task state transitions, inbox deduplication, deployment state, and outbox insertion. LangChain retrieval must never grant a claim or authorize a deployment.

### Changefeed configuration

The changefeed watches `coordinator_outbox`, not every operational table. Use an external connection or secret-managed sink in real deployment; the following is illustrative:

```sql
CREATE CHANGEFEED FOR TABLE coordinator_outbox
INTO 'webhook-https://coordinator.example.com/events/cockroach'
WITH
    initial_scan = 'no',
    envelope = 'wrapped',
    updated,
    webhook_auth_header = 'Basic <secret-managed-value>';
```

The webhook handler only authenticates and durably inserts into `event_inbox`. A background `drift_worker.py` claims `RECEIVED` events, computes affected consumers, writes drift findings, and emits `DRIFT_DETECTED` / `TASK_REPLAN_REQUIRED` outbox events. The handler returns HTTP 200 after durable ingestion, not necessarily after drift analysis.

CockroachDB changefeeds are at-least-once and do not provide total ordering across all messages, so `event_id` deduplication, aggregate revisions, stale-event checks, and reconciliation polling are mandatory.

### ccloud and operational skills

The infrastructure runbook must:

1. Authenticate `ccloud` with a dedicated service account.
2. Capture cluster status as JSON without committing credentials.
3. Verify the cluster version, SQL address, regions, and vector-index setting.
4. Apply schema/bootstrap SQL using the least-privileged operational role.
5. Use the relevant CockroachDB Agent Skills for schema review, security review, observability, and performance checks.

### Managed MCP audit workflow

Configure the MCP connection for a single cluster using `mcp-cluster-id`, and grant the connected user/service account only read access to `contract_drift_audit` and approved supporting views. The MCP server supports broader operations depending on permissions, so “read-only” is an authorization configuration, not an inherent MCP property.

The demo query is executed from a real MCP client:

```text
Show all Billing contract revisions that caused an Orders task to re-plan today.
```

## 6. Execution Flow: Atomic Mutation $\rightarrow$ Async Drift $\rightarrow$ Checkpoint Reconciliation

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                             END-TO-END EXECUTION & RECONCILIATION FLOW                           │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘

 1. Agent B Starts (Orders Service):
    • LangChain CockroachDB vector retrieval discovers `billing-service:POST /v1/charges`.
    • Relational graph in `service_contract_consumers` confirms active dependency.
    • Registers in-flight task in `active_agent_tasks` and `task_contract_dependencies` (Assumed Revision: 1).
    • Agent B codes optimistically in `/worktrees/task-orders-102`.

 2. Agent A Mutates Upstream (Billing Service):
    • Agent A finishes upgrading Billing API to Revision 2 (`payment_method_id` required).
    • Single CockroachDB transaction commits:
        - Insert Revision 2 into `service_contract_revisions` (Revision 1 deactivated)
        - Insert `CONTRACT_CHANGED` event into `coordinator_outbox`
        - Insert audit record into `contract_audit_history`
      COMMIT;

 3. Changefeed Stream & Async Drift Detection:
    • Changefeed delivers `CONTRACT_CHANGED` to `/events/cockroach`.
    • Coordinator inserts event into `event_inbox` (`ON CONFLICT DO NOTHING`) and returns HTTP 200.
    • `drift_worker.py` claims the `RECEIVED` inbox event idempotently.
    • Worker queries `task_contract_dependencies` for tasks assuming Revision 1.
    • Differencer computes structural diff: `[{"field": "payment_method_id", "change": "new required field"}]`.
    • Coordinator creates `drift_events` and emits `DRIFT_DETECTED` / `TASK_REPLAN_REQUIRED` events.

 4. Checkpoint Reconciliation (reconciliation.py):
    • Agent B reaches its next execution checkpoint (prior to testing or drafting the final PR).
    • LangGraph checkpoint state is persisted in CockroachDB so the run can resume safely.
    • Coordinator pauses Agent B and returns a structured re-plan payload:
      ```json
      {
        "status": "REPLAN_REQUIRED",
        "dependency": "billing-service",
        "expected_contract": "Revision 1",
        "current_contract": "Revision 2",
        "breaking_changes": [
          {
            "field": "payment_method_id",
            "change": "new required field",
            "deprecated_field": "card_token"
          }
        ]
      }
      ```
    • Agent B adapts its client code in `clients/billing_client.py` to use `payment_method_id`.

 5. Test Gate & Deployment Promotion (deployer.py):
    • Agent B runs `pytest` in its worktree $\rightarrow$ Integration tests turn GREEN.
    • Deployer applies tested commit to `/live/orders-service`.
    • Health check verifies live endpoint.
    • Coordinator increments the deployment `reload_version` and writes `DEPLOYMENT_COMPLETED`.
    • Open browser polls `/demo/version` and reloads automatically.
```

---

## 7. Public Control UI (FastAPI + Jinja2 + Vanilla CSS/JS)

The UI is served directly from the FastAPI coordinator (`/control`), eliminating node/npm build dependencies:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 CODECLAIM CONTROL DASHBOARD (/control)                           │
├────────────────────────────────┬────────────────────────────────┬────────────────────────────────┤
│ 1. MICROSERVICE FLEET & AGENTS │ 2. LIVE DRIFT & INTENT MESH    │ 3. COCKROACHDB STATE & MCP     │
├────────────────────────────────┼────────────────────────────────┼────────────────────────────────┤
│ 🟢 `billing-service`           │ ⚡ REAL-TIME DRIFT ALERTS:      │ 🪳 LIVE SQL STATE:             │
│ Worktree: `task-billing-401`   │ [ALERT] billing-svc Rev 1 -> 2 │ Table: `service_contract_revisions`│
│ Target: live/billing-service   │ Affected: `task-orders-102`    │ [billing-svc | /charges | Rev2]│
│ Status: DEPLOYED (Rev 2)       │ Status: REPLAN_REQUIRED        │                                │
│                                │ Diff: +payment_method_id       │ 🔍 CDC RANGEFEED TIMELINE:     │
│ 🔵 `orders-service`            │                                │ [Ack] CONTRACT_CHANGED (Rev 2) │
│ Worktree: `task-orders-102`    │ 📊 DEPENDENCY MATRIX:          │ [Ack] DEPLOYMENT_COMPLETED     │
│ Target: live/orders-service    │ • orders -> billing (Rev 2)    │                                │
│ Status: RECONCILED & GREEN     │ • Contract tests: 12 passed    │ 🤖 MANAGED MCP READ-ONLY AUDIT:│
│                                │ • Breaking changes: 0 unresolved│                                │
│                                │                                │ Direct Claude/Cursor endpoint  │
└────────────────────────────────┴────────────────────────────────┴────────────────────────────────┘
```

---

## 8. Verification Plan & Test Suite

### Automated Tests:
1. **Schema Differencer Unit Test**:
   - `pytest tests/test_differencer.py`
   - Verifies structural diffing: removed fields, new required fields, and type changes.
2. **Atomic Outbox Publication Test**:
   - `pytest tests/test_atomic_outbox.py`
   - Verifies that contract mutation, outbox event, and audit record are committed in a single serializable transaction.
3. **Cross-Service End-to-End Test**:
   - `pytest repos/orders-service/tests/test_integration.py`
   - Pre-reconciliation: Fails with 422 Unprocessable Entity (missing `payment_method_id`).
   - Post-reconciliation: Passes with the updated client and contract test suite.
4. **Changefeed Inbox Idempotency Test**:
   - `pytest tests/test_changefeed_inbox.py`
   - Delivers the same event twice and verifies one durable processing result.
5. **Checkpoint Reconciliation Test**:
   - `pytest tests/test_checkpoint_reconcile.py`
   - Verifies that a stale Orders task pauses, receives a typed diff, and resumes only after its plan revision changes.
6. **Deployment Safety Test**:
   - `pytest tests/test_deployment.py`
   - Verifies health-check failure triggers rollback and does not increment the live reload version.

### 3-Minute Video Demo Script:
1. **0:00 - 0:40 | The Problem**: Show `billing-service` and `orders-service` repos. Explain the danger of silent cross-service API drift during parallel agent execution.
2. **0:40 - 1:15 | Vector Discovery & Optimistic Parallelism**: Agent B prompts for 1-click checkout $\rightarrow$ LangChain's CockroachDB vector store discovers the Billing endpoint $\rightarrow$ Agent B persists its checkpoint and begins coding in `/worktrees/task-orders-102`.
3. **1:15 - 2:00 | Atomic Publication & Changefeed Drift Alert**: Agent A publishes Billing Revision 2 $\rightarrow$ CockroachDB atomically commits contract + outbox $\rightarrow$ Changefeed delivers event $\rightarrow$ Coordinator flags Drift $\rightarrow$ Agent B hits checkpoint and receives `REPLAN_REQUIRED` diff.
4. **2:00 - 2:35 | Reconciliation & Live Deployment**: Agent B adapts its client $\rightarrow$ `pytest` passes $\rightarrow$ Deployer updates `/live/orders-service` $\rightarrow$ Browser auto-reloads to new version.
5. **2:35 - 3:00 | Real CockroachDB Managed MCP Query**: Open Claude Code, Cursor, or VS Code connected to the cluster-scoped `https://cockroachlabs.cloud/mcp` connection $\rightarrow$ Execute a read-only query over `contract_drift_audit` to display the reconciliation history.

---

## 9. Construction Plan and Dependency Graph

The build is staged so every phase produces a demonstrable increment.

### Phase 0 — Tool and infrastructure bootstrap

**Context:** The project requires a real CockroachDB Cloud cluster, AWS host, Bedrock access, and a reproducible operations path.

- Create the CockroachDB Cloud cluster.
- Install and authenticate `ccloud` with a dedicated service account.
- Capture reproducible cluster evidence with the installed CLI, for example `ccloud cluster get <cluster-id> --format json` after verifying the current command syntax.
- Capture cluster ID, version, region, and connection metadata without committing secrets.
- Verify the vector-index capability on the chosen cluster version.
- Install or configure the relevant CockroachDB Agent Skills locally.
- Provision the EC2 instance and restrict inbound traffic to HTTPS/SSH administration.
- Configure AWS secrets for CockroachDB, Bedrock, MCP audit access, and S3.

**Exit criteria:** `ccloud` can inspect the cluster, the application can connect with `psycopg`, and the EC2 host can reach Bedrock and CockroachDB Cloud.

### Phase 1 — Schema and transaction foundation

**Context:** CockroachDB must become the authority before any agent execution is added.

- Apply the version-verified schema in `coordinator/schema.sql`.
- Create stable contract identities and immutable contract revisions.
- Implement serializable contract publication: revision, audit row, and outbox event.
- Implement task creation, dependency registration, and deployment records.
- Add application-role and MCP-audit-role permissions.
- Add transaction retry handling for CockroachDB serializable retries.

**Exit criteria:** a test proves that a failed transaction leaves no contract, audit, or outbox partial state.

### Phase 2 — Semantic memory and contract discovery

**Context:** Agents need to discover related upstream contracts without claiming the entire service graph.

- Install `langchain-cockroachdb`.
- Build the vector store for contract summaries and prior drift-resolution summaries.
- Add a LangGraph CockroachDB checkpointer for agent-run state.
- Generate embeddings during contract publication or through an asynchronous embedding worker.
- Combine vector retrieval with relational dependency confirmation.

**Exit criteria:** the Orders task retrieves the Billing charge contract and records the dependency revision it assumed.

### Phase 3 — Changefeed, inbox, and drift worker

**Context:** Contract changes must reach the coordinator durably without coupling the publisher to downstream analysis.

- Create the webhook changefeed for `coordinator_outbox`.
- Implement authenticated webhook ingestion.
- Implement `event_inbox` deduplication and retry state.
- Implement `drift_worker.py` with a claim/lease on inbox rows.
- Compute deterministic Pydantic/JSON-schema diffs.
- Emit `DRIFT_DETECTED` and `TASK_REPLAN_REQUIRED` events.

**Exit criteria:** a duplicated changefeed delivery creates one drift finding and one re-plan state.

### Phase 4 — Checkpoint-aware agent runner

**Context:** Agents work optimistically and pause only at safe checkpoints.

- Create an isolated Git worktree per task.
- Persist plan revision, base commit, dependency revisions, heartbeat, and checkpoint state.
- Implement a checkpoint before tests/finalization.
- On `REPLAN_REQUIRED`, retrieve the typed diff and resume only after the plan revision is updated.
- Add a deterministic scripted agent mode for the demo and an optional Bedrock mode for real agent execution.

**Exit criteria:** Agent B continues parallel work, pauses at a checkpoint after Billing revision 2, updates its client, and resumes successfully.

### Phase 5 — Deployment and live reload

**Context:** Approved work must reach the live services safely on the EC2 host.

- Implement tested-commit promotion from worktree to `/live/{service}`.
- Run unit tests, contract tests, and integration tests before promotion.
- Perform atomic checkout replacement or a controlled process restart.
- Run health checks for both services.
- Roll back to the prior commit on health-check failure.
- Increment `reload_version` only after successful deployment.
- Make the browser poll `/demo/version` and reload after a version change.

**Exit criteria:** the live Orders page changes after deployment and automatically reloads; a failed health check restores the previous version.

### Phase 6 — UI, MCP, S3 receipts, and judging evidence

**Context:** The system must be understandable and verifiable in a short demo.

- Complete the three-panel FastAPI/Jinja2 control dashboard.
- Show task plans, assumed revisions, drift findings, checkpoint state, test evidence, and deployment version.
- Configure a real cluster-scoped read-only MCP connection.
- Demonstrate a natural-language audit query from Claude Code, Cursor, or VS Code.
- Archive signed deployment and drift receipts to S3 asynchronously.
- Run the CockroachDB Agent Skills checks and include results in the README.
- Capture `ccloud` JSON cluster evidence without secrets.

**Exit criteria:** the three-minute demo proves vector discovery, atomic publication, changefeed delivery, drift reconciliation, live deployment, MCP inspection, and AWS hosting.

### Dependency graph and parallelism

```text
Phase 0
  ↓
Phase 1
  ├──→ Phase 2: semantic memory
  ├──→ Phase 3: changefeed and drift worker
  └──→ Phase 5: deployment harness with stubbed events

Phase 2 + Phase 3
  ↓
Phase 4: checkpoint-aware agent runner
  ↓
Phase 5: integrated promotion and live reload
  ↓
Phase 6: UI, MCP, S3 receipts, and final evidence
```

The critical path is Phase 0 → Phase 1 → Phase 3 → Phase 4 → Phase 5. Vector retrieval and deployment harness work can proceed in parallel after the database foundation exists.
