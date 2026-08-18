# CodeClaim Project Charter and Validation Specification

## 1. Purpose

CodeClaim is a **harness-agnostic, transactional compatibility coordinator for agent-driven internal microservices**.

It prevents independently operating coding agents from silently producing incompatible integrations across service boundaries. It does this by recording versioned API contracts, confirmed service dependencies, task assumptions, checkpoints, compatibility work, and immutable audit events in CockroachDB.

CodeClaim is not:

- a general-purpose agent orchestration platform;
- a replacement coding agent or LLM provider;
- a pessimistic source-code/function locking system;
- a direct database-write tool for coding agents;
- an automatic merge or deployment system.

The principal demo story is:

1. Team A launches Agent 1 to change `billing-service`.
2. Team B launches Agent 2 to change `orders-service`.
3. Both work optimistically and in parallel in separate repositories/worktrees.
4. Agent 1 publishes a new Billing HTTP contract revision.
5. CodeClaim detects that the revision invalidates a contract revision used by Orders.
6. Agent 2 receives a deterministic replan instruction at its next safe checkpoint, or receives a new compatibility task if its original work is already complete.
7. Agent 2 either adapts and proves the result with tests, or reports a structured incompatibility requiring a human design decision.
8. The complete causal history is visible in the dashboard, durable in CockroachDB, and optionally announced to Slack.

## 2. Current Product Scope

### Supported Now (Production-Grade v1)

- Internal Python services built with **FastAPI** and **Pydantic**.
- Synchronous **HTTP/JSON** APIs.
- Deterministic **OpenAPI-based contract extraction** (via `codeclaim onboard`, importing `app.openapi()` or querying loopback `/openapi.json`).
- External harness integration (**Codex, Claude Code, Cursor**, or internal runners) through the authenticated **CodeClaim REST API & MCP Server**.
- **CockroachDB Cloud** as the sole **authoritative transactional memory plane** for contracts, dependencies, checkpoints, audit lineage, and outbox event streams.
- A FastAPI coordinator hosted on AWS with same-origin control dashboard.
- Optional **AWS Bedrock embeddings** (Cohere Embed v4) for **semantic contract discovery only**.
- Optional asynchronous Slack webhook notifications driven from committed outbox events.

### Future Roadmap Only (Explicitly Deferred)

- **gRPC / Protobuf** interface extraction and protocol buffer compatibility analysis.
- **GraphQL** schema and operation AST extraction.
- **Events & Message Queues** / **AsyncAPI** compatibility analysis.
- **Other languages and framework adapters** (TypeScript/Node.js, Go, Rust, Java/Kotlin, Django, Flask).
- **Third-party SDK / external vendor API dependency monitoring** and public API documentation scraping.
- Automatic merge or deployment of agent-produced work.

### Essential Boundaries & Guarantees

1. **No Automatic Support Claims**: Do not claim automatic support for unsupported frameworks, languages, or protocols without a tested, deterministic machine-readable contract adapter.
2. **Authoritative Memory vs. Semantic Discovery**: CockroachDB is the **authoritative transactional system of record** for all contracts, dependencies, checkpoint assumptions, and audit events. Bedrock embeddings provide **auxiliary semantic discovery only** (surfacing candidate endpoints). **Embeddings never prove compatibility** and **never register dependencies automatically**.

## 3. Core Architectural Principles

1. **Optimistic parallel work.** Agents are not blocked simply because another agent is active. They work against explicitly recorded contract assumptions.
2. **Deterministic coordination.** The coordinator is a deterministic workflow/control service, not another non-deterministic LLM.
3. **Contract boundaries, not code locks.** The coordination unit is an exposed interface dependency, not a source-code symbol or function lock.
4. **Authoritative transactional truth.** CockroachDB is the single transactional source of truth. Contract publication, relevant audit records, compatibility work creation, and outbox events are written atomically in CockroachDB.
5. **Auxiliary semantic discovery.** Bedrock vector embeddings allow natural language discovery of candidate contracts, but never decide dependencies or prove compatibility.
6. **Structured evidence.** Schemas, declared dependencies, checkpoint metadata, test evidence, commits, and approval records are durable. Raw prompts, hidden reasoning, scratchpads, and chain-of-thought are not stored or displayed.
7. **Human decision for ambiguity.** Uncertain semantic compatibility and blocked integration are escalated to human engineers; they are not silently treated as safe.
8. **No direct agent database writes.** Coding agents interact through validated CodeClaim API/MCP operations, not direct CockroachDB credentials.
9. **Notifications are secondary.** Slack and other notifications are asynchronous observability mechanisms; CockroachDB remains the source of truth.

## 4. Deployment Topology

The dashboard must be hosted by the same deployment as the coordinator.

```text
Codex / Claude Code / Cursor / internal runner / web users
                         |
                         v
https://codeclaim.example.com
                         |
                         v
AWS-hosted CodeClaim coordinator
  - HTTPS reverse proxy
  - FastAPI dashboard and same-origin APIs
  - deterministic drift worker
  - compatibility dispatcher
  - optional Slack notifier
                         |
          +--------------+--------------+
          |                             |
          v                             v
CockroachDB Cloud              Bedrock embeddings (optional)
          |
          v
S3 audit receipts (optional)
```

Requirements:

- Run the FastAPI coordinator, dashboard assets, and background workers together on the same AWS server/container.
- Serve dashboard and coordinator APIs from the same origin, such as `/api/dashboard/state`.
- Put HTTPS reverse proxy/ALB in front of the coordinator; do not publicly expose its internal application port.
- Use an EC2/IAM role for Bedrock/S3 where applicable; do not store AWS access keys in source code.
- Store CockroachDB URL, coordinator/operator secret, changefeed secret, and Slack webhook in protected environment configuration or a secret manager.

## 5. Service Contracts and Dependency Matrix

### 5.1 Contract extraction source

For v1, FastAPI-generated OpenAPI is the authoritative contract source. Each registered service declares:

```yaml
service:
  name: billing-service
  language: python
  framework: fastapi
  protocol: http
contract_source:
  endpoint_directory: app/api
  application_entrypoint: app.main:app
```

The endpoint directory helps onboarding and review, but the exact FastAPI application entry point is required. The CLI may use `app.openapi()` or fetch `/openapi.json` from a local running service.

Extraction should run in an isolated/sanitized subprocess or container where practical because importing application code can have side effects. It must use a timeout and must not receive production secrets.

### 5.2 Exact HTTP contract contents

Each versioned operation contract must include:

- provider service;
- operation identity: HTTP method and path;
- path parameters, schemas, and requiredness;
- query parameters, schemas, defaults, and requiredness;
- declared HTTP header parameters, schemas, and requiredness;
- JSON request body content type and schema;
- response schemas by status code/content type;
- declared security/auth requirements;
- source commit/revision and publisher;
- optional publisher compatibility declaration and migration notes.

Example:

```text
POST /v1/charges/{charge_id}
  path: charge_id: string, required
  query: currency: string, required
  header: idempotency-key: string, required
  body: ChargeRequest
  responses: 200 ChargeResponse, 422 ValidationError
```

### 5.3 Dependency records

A dependency must represent the exact interface edge, not a vague service-to-service relationship.

```text
orders-service
  consumes billing-service
  HTTP POST /v1/charges
  assumed revision 1
  evidence: clients/billing_client.py
  status: confirmed
```

Store at minimum:

- consumer and provider service;
- `dependency_kind` (currently `HTTP_REST`);
- contract/operation ID and assumed revision;
- consumer source-file/client evidence;
- ownership (`internal` now; `third_party` reserved for future);
- confirmation status: `detected`, `confirmed`, `rejected`, `review_required`;
- exact consumer-used subset of query/header/body/response contract where available.

Only confirmed dependencies are authoritative for automatic coordination. Semantic/vector discovery can suggest candidates but must never independently create a dependency or decide compatibility.

### 5.4 Dynamic or unresolved behavior

Dynamic routes, dynamically constructed headers, runtime configuration, or conditional router inclusion must be surfaced as `REVIEW_REQUIRED`. The system must never claim that it fully understands a dynamic interface it cannot derive from OpenAPI.

## 6. One-Time Project Onboarding CLI

Implement a `codeclaim onboard` CLI for one-time registration of an internal FastAPI service or multi-service manifest.

Required inputs:

- service name;
- repository path;
- exact endpoint-code directory;
- FastAPI application entry point, e.g. `app.main:app`;
- optional manifest for multiple services.

Expected workflow:

1. Validate the repository and Python/FastAPI configuration.
2. Load or fetch the generated OpenAPI contract.
3. Normalize discovered HTTP operations into CodeClaim contracts.
4. Detect explicit client evidence where possible and suggest internal dependencies.
5. Present a human-readable plan before any writes.
6. Flag ambiguous/dynamic findings as `REVIEW_REQUIRED`.
7. Ask for explicit confirmation: `Apply this onboarding plan? [y/N]`.
8. Only after confirmation, register services/contracts/dependencies in CockroachDB and create CodeClaim configuration/instruction files.
9. Write onboarding audit records.

The CLI may provide `--yes` for CI/non-interactive usage, but it must still print the plan. It must never alter application source code.

Example manifest:

```yaml
services:
  - name: billing-service
    repo: ./repos/billing-service
    contract:
      framework: fastapi
      endpoint_directory: app/api
      application_entrypoint: app.main:app
    harness:
      type: codex
      dispatch: poll

  - name: orders-service
    repo: ./repos/orders-service
    contract:
      framework: fastapi
      endpoint_directory: app/api
      application_entrypoint: app.main:app
    consumes:
      - provider: billing-service
        method: POST
        path: /v1/charges
        client_path: clients/billing_client.py
    harness:
      type: claude_code
      dispatch: poll
```

## 7. Deterministic Compatibility Detection

The coordinator must determine structural compatibility by comparing normalized old and new OpenAPI contracts, not by asking an LLM.

Default breaking changes include:

- required path/query/header parameter added, removed, or renamed;
- required request body field added;
- request or response field removed/renamed;
- field type, format, or validation constraint changed incompatibly;
- enum value removed;
- HTTP method/path removed or renamed;
- response status/schema removed or tightened;
- declared auth/security requirement tightened.

Normally non-breaking:

- optional field or optional parameter added;
- response field added;
- enum value added, subject to explicit policy.

Some semantic changes cannot be inferred from schema alone. The publisher must be able to add a versioned structured declaration:

```json
{
  "classification": "breaking",
  "reason": "amount changed from integer cents to decimal dollars",
  "migration_notes": "Consumers must divide legacy cents by 100",
  "consumer_impact": "Orders checkout client must migrate"
}
```

Classification rule:

```text
BREAKING        if deterministic diff is breaking OR publisher declares breaking
REVIEW_REQUIRED if behavior is uncertain/unclassified
NON_BREAKING    only when explicit rules support it
```

## 8. Parallel Agent Workflow and Checkpoints

### 8.1 Agent roles

Agent 1 and Agent 2 may be Codex, Claude Code, Cursor, or an internal runner. They own planning, edits, local tools, and tests within their own repositories/worktrees.

The coordinator owns durable state and deterministic coordination.

### 8.2 Agent 2 lifecycle

1. Receive a task, such as implementing checkout in Orders.
2. Register task, service, worktree, base commit, and upstream contract assumptions.
3. Discover and record confirmed dependencies relevant to the task.
4. Work normally in an isolated worktree.
5. At safe boundaries, submit a structured checkpoint.
6. Receive `CONTINUE` or `REPLAN_REQUIRED`.
7. If replan is required, read the supplied authoritative contract/diff/migration notes and independently adapt its service.
8. Run tests and submit evidence, or report a structured incompatibility.
9. Await human approval where policy requires it; never deploy automatically.

Safe checkpoints include:

- after dependency discovery;
- after a logical component change;
- before editing an external-service client;
- after tests;
- before PR/deployment candidate creation.

Example safe checkpoint payload:

```json
{
  "task_id": "...",
  "plan_revision": 1,
  "phase": "integrate_billing_client",
  "files_changed": ["clients/billing_client.py"],
  "assumed_contracts": [
    {"service": "billing-service", "method": "POST", "path": "/v1/charges", "revision": 1}
  ],
  "test_status": "not_run"
}
```

No raw prompt, hidden reasoning, private chat history, scratchpad, credential, or customer data may be persisted as checkpoint state.

### 8.3 Coordinator response

```text
CONTINUE
```

or:

```json
{
  "instruction": "REPLAN_REQUIRED",
  "old_revision": 1,
  "new_revision": 2,
  "breaking_diff": {},
  "migration_notes": "...",
  "audit_ids": ["..."]
}
```

The existing scripted Agent A/B flow is a reliable demo adapter only. It must not be the required platform execution model.

## 9. Harness Integration and Compatibility Work

CodeClaim must expose validated API/MCP operations such as:

- register task and contract assumptions;
- discover relevant confirmed contracts;
- submit checkpoint;
- retrieve pending drift;
- claim compatibility work;
- submit test evidence and compatibility result.

Register service-owning harnesses with:

- harness ID/name/type;
- owned service and repository;
- capability manifest;
- dispatch mode: polling or webhook;
- status/heartbeat;
- secure token hash or external credential reference.

CodeClaim can automatically create and durably dispatch **compatibility work**, but it cannot assume it can remotely wake every commercial agent. Registered runners either poll/claim their work or accept authenticated webhooks.

Use a separate compatibility-work state machine; drift events are observations, not jobs:

```text
PENDING → DISPATCHED → ACKNOWLEDGED → EXECUTING → AWAITING_APPROVAL → VERIFIED → COMPLETED
```

Terminal/escalation states:

```text
BLOCKED, INCOMPATIBLE, FAILED, EXPIRED, CANCELLED
```

Every work item must carry idempotency key, causation ID, correlation ID, source contract/event/revision, bounded retry state, and bounded hop count. This prevents duplicate jobs and cyclic cascades.

## 10. Incompatibility and Preserved Work

If Agent 2 cannot satisfy a new required contract input, it must not fabricate data, suppress validation, or silently produce an unsafe workaround.

Example: Billing v2 requires `customer_id`; Orders supports guest checkout and has no valid customer identity.

Agent 2 reports a structured result:

```json
{
  "status": "BLOCKED",
  "reason_code": "REQUIRED_INPUT_UNAVAILABLE",
  "missing_input": "customer_id",
  "contract_revision": 2,
  "evidence": {"sources_checked": ["checkout request", "session", "customer profile"]},
  "requested_resolution": "Support guest checkout or add an identity prerequisite"
}
```

The coordinator must:

- preserve Agent 2 worktree/branch/commit and changed-file evidence;
- mark the work `BLOCKED` or `INCOMPATIBLE`;
- create durable audit/incident records;
- prevent merge/deployment;
- surface a human design/API decision.

`BLOCKED` means **preserve and escalate**, never rollback/delete.

## 11. CockroachDB Responsibilities

CockroachDB is central to the product and must be materially used for:

- registered services and harnesses;
- immutable contract revisions and compatibility declarations;
- confirmed dependency matrix;
- semantic memory/vector entries where enabled;
- active tasks and checkpoints;
- drift events and compatibility work;
- transactional outbox and idempotent inbox/changefeed processing;
- audit/causal timeline;
- deployment records and tamper-evident receipts;
- notification-delivery attempts.

Critical publication sequence must occur in one serializable transaction:

```text
contract revision
+ normalized compatibility diff/declaration
+ audit record
+ compatibility work for affected confirmed consumers
+ coordinator_outbox event
```

Use the outbox for asynchronous processing. External delivery failures must not roll back committed contract or coordination state.

## 12. Bedrock and Semantic Discovery

Amazon Bedrock is optional. It may generate embeddings for semantic contract discovery, such as finding candidate payment-related contracts from a natural-language task.

Rules:

- embeddings may suggest candidates only;
- relationally confirmed dependencies and deterministic contract diff are the proof for coordination;
- semantic search must be clearly labelled simulated/fallback in demo mode;
- no LLM or embedding result may independently decide that a change is non-breaking or create an authoritative dependency.

If Cohere Embed v4 is enabled, use its Bedrock Runtime-specific request/response format rather than Titan's payload format. Keep embedding dimension aligned with CockroachDB vector columns.

## 13. Dashboard Requirements

The coordinator FastAPI application serves the dashboard and same-origin APIs. CockroachDB is the dashboard data source.

The dashboard must answer immediately:

1. What are agents/harnesses working on?
2. Which internal services depend on one another?
3. Did a contract revision create risk?
4. What needs human action?
5. What is the complete causal audit trail?

### Primary layout

```text
Left:   Service Dependency Map
Center: Active Agent and Compatibility Work
Right:  Events, Incidents, and Human Actions
```

On smaller screens, prioritize incidents/actions, then work, then map, then timeline.

### Dependency map

Show internal services and confirmed HTTP dependency edges. Edge states:

- green: compatible;
- neutral/blue: active work, no known conflict;
- amber: compatibility work or replan required;
- red: blocked/incompatible;
- gray: review-required/unknown.

Clicking an edge must reveal method/path, path/query/header/body/response summary, current/assumed revisions, source evidence, confirmation status, and related tasks/incidents.

### Active work

Show harness/agent, owner/team if present, service/repository, task summary, current checkpoint, assumed upstream revision, test status, timestamps, and preserved worktree/branch/commit link. Do not display raw prompts or scratchpads.

### Prominent compatibility incidents

`REPLAN_REQUIRED`, `BLOCKED`, and `INCOMPATIBLE` must be visually obvious and accessible, not buried in a generic event log.

Example:

```text
Compatibility Blocked
orders-service → billing-service
POST /v1/charges, revision 1 → 2

Billing now requires customer_id.
Orders guest checkout cannot supply customer_id.

Status: Human decision required.
```

Show source/target, revision diff, migration notes, evidence, preserved work, owner, and approved human actions. Do not provide an auto-deploy action.

### Diff viewer

Present readable normalized HTTP diffs for method/path, parameters, headers, request body, response statuses/schemas, and security. Default to a human-readable diff; allow expanded normalized schema as an advanced view.

### Causal timeline and audit drawer

Provide a readable event chain linking event/audit IDs, actors, timestamps, correlation/causation IDs, contracts, tasks, commits, tests, approvals, and deployment outcome.

Example:

```text
Agent 1 task registered
→ Billing v2 published
→ Orders dependency affected
→ compatibility work created
→ Agent 2 checkpointed
→ replan required
→ Agent 2 reported incompatibility
→ Slack notified
→ human decision pending
```

### Dashboard safety and usability

- bounded/paginated APIs; do not load unbounded audit history;
- authenticated mutation endpoints only;
- escape displayed values and avoid XSS;
- accessible labels, keyboard navigation, readable contrast, and state labels in addition to color;
- harmless UI preferences only in browser storage;
- no secrets, raw prompts, chain-of-thought, customer data, or full source code in dashboard payloads.

## 14. Slack Notifications

Slack is optional and uses a standard incoming webhook/custom app for the hackathon.

Notifications come asynchronously from committed outbox events and must have durable delivery-attempt records. They must not alter or roll back coordinator state on failure.

For the demo, notify only:

1. breaking contract published;
2. compatibility work created/replan required;
3. compatibility blocked/incompatible.

Messages must be concise and actionable. They must not include secrets, source code, customer data, raw prompts, or private reasoning.

Example:

```text
Compatibility Blocked
orders-service cannot adopt billing-service v2
Breaking change: customer_id is now required
Reason: guest checkout cannot supply customer_id
Action required: choose guest-checkout support or add an Orders identity prerequisite
```

## 15. Human Review and Approval

Detection can be automatic; registration and material actions require explicit human review.

Human confirmation is required for:

- ambiguous detected dependencies;
- onboarding writes after CLI scan;
- incompatible/blocked design decisions;
- reconciliation results where policy requires approval;
- deployment promotion.

No result may be automatically merged or deployed merely because an agent claims success.

## 16. Required Test Coverage

The implementation must include tests that demonstrate:

1. FastAPI/OpenAPI extraction captures method/path, query/header/body/response contracts.
2. Dynamic/unresolved behavior is `REVIEW_REQUIRED`.
3. Onboarding prints a plan and does not write before explicit confirmation.
4. A confirmed Orders → Billing dependency is persisted correctly.
5. An unrelated Billing endpoint change does not create false compatibility work for Orders.
6. A breaking change creates drift/replan work transactionally and idempotently.
7. Active Agent 2 receives replan state only at a checkpoint boundary.
8. A completed/inactive consumer receives a new compatibility work item.
9. Blocked/incompatible work preserves branch/worktree/commit evidence and cannot deploy.
10. Harness API/MCP operations require authentication and do not grant direct DB access.
11. Dashboard incident, dependency detail, timeline, and safety/redaction requirements are met.
12. Slack delivery failure does not roll back coordinator state and is recorded.
13. API payloads do not expose secrets, raw prompts, scratchpads, or chain-of-thought.
14. Existing scripted Agent A/B demo remains functional as a demo adapter.

## 17. Hackathon Demonstration Acceptance Scenario

The final demo should show:

1. Billing and Orders are registered internal FastAPI services.
2. Agent 1 and Agent 2 start tasks in parallel in separate repositories/worktrees.
3. Agent 2 records a dependency on Billing `POST /v1/charges` revision 1.
4. Agent 1 publishes Billing revision 2 containing a breaking HTTP contract change.
5. CockroachDB records the revision, diff, audit record, compatibility work, and outbox event atomically.
6. Agent 2 reaches a checkpoint and receives `REPLAN_REQUIRED`, or a new work item is created if it already completed.
7. Agent 2 either adapts/tests successfully or reports a structured incompatibility.
8. If incompatible, its work remains preserved and deployment is blocked.
9. The same-server dashboard clearly shows the service edge, breaking diff, agent state, incident, causal timeline, and human action requirement.
10. Optional Slack receives the concise alert.
11. CockroachDB Managed MCP can be used separately for read-only audit inspection; CodeClaim MCP/API provides the controlled operational integration surface.

## 18. Validator Checklist

A validator should reject or flag the implementation if it:

- presents CodeClaim as a generic job runner rather than an inter-service compatibility coordinator;
- implements pessimistic code/symbol locking as the central mechanism;
- claims arbitrary Python framework, language, gRPC, GraphQL, event, or third-party support in v1;
- uses LLM/embedding output as proof of dependency or breaking compatibility;
- allows direct coding-agent writes to CockroachDB;
- persists raw prompts, chain-of-thought, private scratchpads, secrets, or customer data;
- auto-merges or auto-deploys a compatibility result;
- deletes/rolls back Agent 2 work after incompatibility;
- treats Slack as a source of truth;
- hides breaking or blocked incidents in generic event logs;
- creates database/configuration writes during onboarding without explicit approval;
- fails to use CockroachDB transactionally for contract, dependency, audit, work-item, and outbox state.

The implementation aligns with this charter when it demonstrably coordinates real, versioned HTTP contract compatibility between independently operating agents across internal FastAPI microservices, with deterministic decisions, preserved evidence, and a complete CockroachDB-backed audit trail.
