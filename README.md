# CodeClaim: CockroachDB Compatibility Control Plane for Agentic Microservices

[![Tests](https://img.shields.io/badge/tested%20with-pytest-10b981.svg)](tests/)
[![CockroachDB](https://img.shields.io/badge/CockroachDB-transactional%20CDC%20%26%20vector%20search-6933ff.svg)](https://www.cockroachlabs.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.141.1-009688.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **CockroachDB Hackathon 2026 submission**
> A compatibility control plane that lets independent coding harnesses change
> internal FastAPI services in parallel without silently breaking their HTTP
> contracts.

## What CodeClaim does

CodeClaim is the coordination layer between coding agents and separate internal
microservice repositories. It is not a source-code lock manager, a generic job
queue, or a replacement for Codex, Antigravity, Claude Code, or another coding
harness.

The v1 boundary is deliberately narrow:

- Python services using FastAPI and Pydantic.
- Internal HTTP/JSON APIs.
- Deterministic contracts normalized from FastAPI's generated OpenAPI schema.
- Exact, human-confirmed consumer-to-provider dependencies.
- External coding harnesses operating in their own repositories or worktrees.

The central demonstration is:

1. `billing-service` publishes `POST /v1/charges` revision 1.
2. `orders-service` confirms that it consumes that exact operation and records
   its assumed provider revision.
3. Antigravity changes Billing by adding a required `token_id` field and
   publishes revision 2.
4. CockroachDB commits the contract revision, drift finding, audit record, and
   outbox event transactionally.
5. A CockroachDB changefeed delivers the outbox event to the coordinator.
6. CodeClaim creates compatibility work for Orders and exposes the exact
   provider operation and revision that must be addressed.
7. Codex reads the updated contract through CodeClaim, updates the Orders client
   to pass its existing global `TOKEN_ID`, runs tests, and submits sanitized
   evidence.
8. The dashboard shows the contract diff, compatibility obligation, checkpoints,
   audit lineage, and transactional outbox events.

Human approval remains a deliberate gate before any deployment promotion.

## Architecture

```text
                           CockroachDB Cloud
     ┌─────────────────────────────────────────────────────────────┐
     │ contracts · revisions · dependencies · tasks · checkpoints  │
     │ drift · compatibility work · audit · transactional outbox   │
     │ semantic_memory (optional VECTOR(1536) discovery)           │
     └───────────────┬───────────────────────────────┬─────────────┘
                     │                               │
             SQL/MCP coordination              changefeed CDC
                     │                               │
       ┌─────────────▼─────────────┐     ┌─────────▼──────────────┐
       │ CodeClaim Coordinator      │     │ POST /events/cockroach │
       │ FastAPI + drift worker     │◄────┤ idempotent event inbox  │
       │ dispatcher + audit APIs    │     └─────────────────────────┘
       └─────────────┬─────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
   Antigravity / provider       Codex / consumer
   billing-service              orders-service
   publishes contract           claims work, replans,
   revision 2                   updates client, submits evidence
                     │
                     ▼
             React control dashboard
```

CockroachDB is the authoritative coordination store. The dashboard is a
projection of that state. Slack, if enabled, is an asynchronous notification
projection of the transactional outbox and is never the source of truth.

## Important invariants

### CockroachDB is authoritative

Contract publication, compatibility work creation, audit lineage, and the
coordinator outbox are written in the same serializable transaction. A failed
compatibility-work creation must roll back the related contract mutation rather
than leaving an untracked breaking change.

### Dependencies are exact and confirmed

A dependency identifies:

- consumer and provider service;
- HTTP method and path;
- path, query, and declared header parameters;
- JSON request-body schema;
- response schemas and status codes;
- the consumer's assumed provider revision;
- source-file/client evidence; and
- explicit confirmation status.

Embeddings may suggest candidates, but they never confirm dependencies or decide
compatibility. Unconfirmed candidates are not coordination truth.

### Compatibility is deterministic and fail-closed

The differencer handles required parameter changes, request and response schema
changes, field removals or renames, type/format changes, enum removals,
endpoint removal or renaming, response status changes, and tightened security
requirements. Optional additions are normally non-breaking. Unknown semantic
changes are `REVIEW_REQUIRED`, not silently classified as safe.

Contract revisions are immutable. Canonical comparison preserves nested
schemas, `$defs`, `$ref`, arrays, formats, enums, required fields, headers,
parameters, and HTTP-interface metadata.

### Agents work in parallel

CodeClaim does not lock source symbols. Provider and consumer agents can work
in parallel in separate repositories or worktrees. When a confirmed provider
contract changes, the consumer receives a compatibility obligation and must
replan against the new revision before its result can be accepted.

### Prompts and sensitive evidence are not coordination data

Task registration uses bounded operational summaries. Raw prompts, chain of
thought, source snippets, stack traces, environment logs, credentials, and
database URLs must not be persisted. Test evidence is validated and redacted
before persistence. Do not commit secret-bearing MCP configuration files.

## Repository layout

```text
code-claim/
├── coordinator/
│   ├── app.py                    # FastAPI coordinator, dashboard APIs, webhook
│   ├── cli.py                    # codeclaim onboard/dependency commands
│   ├── config.py                 # fail-closed environment configuration
│   ├── db.py                     # psycopg pool and transaction helpers
│   ├── service_registry.py       # explicit service registration
│   ├── onboarding.py             # deterministic FastAPI/OpenAPI extraction
│   ├── contract_registry.py      # immutable contract publication/retirement
│   ├── differencer.py            # deterministic HTTP compatibility rules
│   ├── http_dependencies.py      # exact confirmed dependencies
│   ├── compatibility.py          # work items, harnesses, evidence, approvals
│   ├── reconciliation.py         # checkpoints and task state transitions
│   ├── drift_worker.py           # changefeed/outbox event processing
│   ├── compatibility_dispatcher.py # polling/webhook work delivery
│   ├── mcp_server.py             # trusted local CodeClaim MCP surface
│   ├── memory.py                 # optional CockroachDB vector discovery
│   ├── slack_notifier.py         # optional asynchronous notifications
│   ├── deployer.py               # optional journaled promotion machinery
│   ├── migrations/               # append-only CockroachDB migrations
│   └── static/dashboard/         # built React dashboard bundle
├── frontend/                     # React/Vite/shadcn-compatible source UI
├── repos/
│   ├── billing-service/          # provider FastAPI fixture
│   └── orders-service/           # consumer FastAPI fixture/client
├── infra/
│   ├── Dockerfile                # container deployment
│   ├── nginx.conf                # HTTPS reverse-proxy template
│   ├── cockroach/changefeed.sql  # changefeed template
│   └── skills/                   # CockroachDB MCP audit runbook
├── scripts/
│   ├── start_server.py           # selector-loop launcher, especially Windows
│   ├── reset_demo_data.py        # explicit demo-data reset
│   ├── register_agents.py        # harness registration/config templates
│   ├── live_preflight.py         # read-only live readiness checks
│   ├── live_harness_scenario.py  # REST-backed live scenario helper
│   └── run_demo.py               # offline deterministic demo path
├── tests/                        # coordinator and integration-oriented tests
├── LIVE_TESTING.md               # detailed live Antigravity/Codex runbook
└── pyproject.toml
```

The migrations directory is the database source of truth. `coordinator/schema.sql`
is retained for compatibility/reference; do not edit an already-applied
migration in place.

## Local setup

### Prerequisites

- Python 3.10 or newer; Python 3.12 is recommended.
- Git.
- Node.js/npm if rebuilding the React dashboard.
- A CockroachDB Cloud or local CockroachDB database for live mode.
- AWS Bedrock access only if semantic embeddings are enabled.

### Install the project

```powershell
git clone https://github.com/Cornex-Technologies/ContractMesh
Set-Location code-claim

py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

On Linux/macOS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

### Configure live-mode secrets

Create a local, untracked `.env` or set environment variables in the service
manager. Never paste real values into this README, a committed JSON file, or a
screen recording.

```dotenv
COCKROACH_DATABASE_URL="postgresql://<user>:<password>@<cluster-host>:26257/codeclaim_db?sslmode=verify-full"
CHANGEFEED_WEBHOOK_SECRET="<long-random-webhook-secret>"
COORDINATOR_API_KEY="<long-random-operator-secret>"
IS_DEMO_MODE=false
DEMO_AUTO_RECONCILE=false

# Optional semantic discovery
AWS_REGION=ap-southeast-1
BEDROCK_EMBEDDING_PROVIDER=cohere_v4
BEDROCK_EMBEDDING_MODEL_ID=cohere.embed-v4:0
EMBEDDING_DIMENSION=1536
```

Outside demo mode, the coordinator fails closed if the database URL or operator
key is missing. On startup it applies pending migrations through `init_db()`.
Verify the database before starting the server:

```powershell
$env:PYTHONPATH = (Get-Location).Path
\.venv\Scripts\python.exe -c "import asyncio, selectors; from coordinator.db import check_health, close_pool; loop=asyncio.SelectorEventLoop(selectors.SelectSelector()); print(loop.run_until_complete(check_health())); loop.run_until_complete(close_pool()); loop.close()"
```

### Build and serve the React dashboard

```powershell
Set-Location frontend
npm install
npm run build
Set-Location ..
```

The build is emitted to `coordinator/static/dashboard/` and served by FastAPI.

### Start the coordinator

Windows PowerShell:

```powershell
$env:PYTHONPATH = (Get-Location).Path
\.venv\Scripts\python.exe scripts\start_server.py
```

The launcher uses an asyncio selector event loop because Psycopg async mode is
not compatible with Windows' default Proactor loop.

Linux/macOS:

```bash
export PYTHONPATH="$PWD"
python scripts/start_server.py
# or, when a selector-loop launcher is not needed:
uvicorn coordinator.app:app --host 127.0.0.1 --port 8000
```

Verify the coordinator and open the dashboard:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health | ConvertTo-Json -Depth 10
Start-Process http://127.0.0.1:8000/control
```

Expected live health includes `status: healthy`, `coordinator: healthy`, and a
healthy CockroachDB result. `demo_mode` should be `false` for the real workflow.

### Optional one-click public demo

For an EC2 showcase, set these values in the untracked `.env` before starting
the coordinator:

```dotenv
PUBLIC_DEMO_ENABLED=true
PUBLIC_DEMO_RUN_TIMEOUT_SECONDS=900
COORDINATOR_BIND_HOST=0.0.0.0
```

After the pending migration is applied on startup, the Overview page shows
`Run complete demo`. A visitor can click it without an operator token. The
workflow is bounded and idempotent: it registers the two demo services if
needed, creates the Billing v1 baseline and confirmed Orders dependency,
publishes a breaking Billing v2 requiring `token_id`, processes drift through
the outbox/inbox path, dispatches compatibility work, records a checkpoint,
and leaves the task at `AWAITING_APPROVAL`. The dashboard then shows the
contract diff, work item, task, checkpoint, audit lineage, and outbox events.

This public route is deliberately a deterministic demonstration adapter. It
does not accept prompts, run arbitrary visitor code, launch Antigravity/Codex,
or write application source files. The real harness flow remains the protected
workflow documented in [`LIVE_TESTING.md`](LIVE_TESTING.md). The scenario
expects a clean Billing revision-1 baseline without `token_id`; if an older
incompatible revision-2 scenario is already in the database, the run fails
closed with a setup message instead of rewriting immutable history.

The launcher remains loopback-only by default. `COORDINATOR_BIND_HOST=0.0.0.0`
is an explicit EC2 choice; pair it with an AWS security-group rule for the
chosen port, or put Nginx/HTTPS in front of it for a public deployment.

### Run tests

```powershell
$env:PYTHONPATH = (Get-Location).Path
\.venv\Scripts\pytest -q
```

The Orders and Billing fixture tests can be run independently:

```powershell
\.venv\Scripts\python.exe -m pytest repos\billing-service\tests -q
\.venv\Scripts\python.exe -m pytest repos\orders-service\tests -q
```

## Register services and dependencies

Registration is intentionally explicit in v1. In a production organization,
these commands belong in CI; the resulting contract and audit records remain in
CockroachDB.

### Onboard a FastAPI service

`codeclaim onboard` supports Python FastAPI only. It imports the configured
application entry point to obtain `app.openapi()`, or can read `/openapi.json`
from a locally running service. It prints the normalized plan, flags unresolved
dynamic behavior as `REVIEW_REQUIRED`, and asks for confirmation before writing.

```powershell
\.venv\Scripts\python.exe -m coordinator.cli onboard `
  --service-name billing-service `
  --repository-path .\repos\billing-service `
  --endpoint-code-dir . `
  --app-entry main:app

\.venv\Scripts\python.exe -m coordinator.cli onboard `
  --service-name orders-service `
  --repository-path .\repos\orders-service `
  --endpoint-code-dir . `
  --app-entry main:app
```

Use `--yes` for CI after reviewing the printed plan. The command writes only
`.codeclaim/service.json` in the service repository and writes service,
contract, audit, and outbox records to CockroachDB. It never edits application
source code.

### Confirm the exact consumer dependency

For the demo, confirm that Orders consumes Billing's exact operation:

```powershell
\.venv\Scripts\python.exe -m coordinator.cli dependencies `
  --consumer-service orders-service `
  --repository-path .\repos\orders-service `
  --endpoint-code-dir clients `
  --provider-service billing-service `
  --confirmed-by orders-owner
```

The CLI may suggest candidates from explicit Python HTTP client calls. Ambiguous
matches require a human action. Only a confirmed record becomes coordination
truth; the dependency is not automatically rebound just because a newer
provider revision exists.

## Live provider-to-consumer demonstration

The complete operational runbook, including harness registration, ngrok,
CockroachDB changefeed setup, Antigravity, and Codex, is in
[`LIVE_TESTING.md`](LIVE_TESTING.md). The concise flow is:

1. Start the coordinator in live mode and verify `/health`.
2. Ensure Billing and Orders are registered and the Orders dependency is
   confirmed against Billing revision 1.
3. Register one harness for Billing/Antigravity and one for Orders/Codex. The
   one-time tokens belong in the harness secret store, not in Git.
4. Configure the CockroachDB changefeed to POST `coordinator_outbox` events to
   the public coordinator endpoint `POST /events/cockroach`.
5. In Antigravity, add the required `token_id` field to Billing's
   `POST /v1/charges`, run its tests, and publish the normalized revision 2.
6. Confirm in the dashboard and database that the breaking diff and one Orders
   compatibility work item were created.
7. In Codex, claim the work, call the checkpoint tools, read the updated Billing
   contract, update Orders to pass its existing global `TOKEN_ID`, run tests,
   and submit sanitized compatibility evidence.
8. Review the resulting `AWAITING_APPROVAL` state and approve or reject using an
   authenticated operator action if promotion is part of the demo.
9. Show the dashboard's contract diff, grouped compatibility obligation,
   checkpoints, audit lineage, and outbox event.

The offline command below is a separate deterministic simulation. It is useful
for local regression testing but does not prove that an external Antigravity or
Codex session completed the live workflow:

```powershell
\.venv\Scripts\python.exe scripts\run_demo.py
```

Before a real run, use the read-only guard:

```powershell
\.venv\Scripts\python.exe scripts\live_preflight.py `
  --public-base-url https://<public-coordinator-host>
```

## CockroachDB changefeed and ngrok

The coordinator consumes CockroachDB's transactional outbox through a webhook
changefeed. Use [`infra/cockroach/changefeed.sql`](infra/cockroach/changefeed.sql)
as the template and substitute the current public host and webhook secret. Run
`CREATE CHANGEFEED` with a CockroachDB SQL client that is allowed to create jobs;
some Cloud SQL Console contexts reject job statements with `disallowed
statement type`.

Do not use the old `infra/ccloud/provision_changefeed.sql` option
`protect_data_from_gc_on_sink_failure` with clusters that reject it. The generic
`infra/cockroach/changefeed.sql` template is the safer current template.

### Why ngrok is used in the EC2 demo

Ngrok is not part of CodeClaim's coordination model. It is a temporary public
HTTPS ingress path:

```text
CockroachDB Cloud changefeed ──HTTPS POST──> ngrok ──> EC2 127.0.0.1:8000
Browser ───────────────────────HTTPS───────> ngrok ──> EC2 dashboard
```

CockroachDB Cloud is outside the EC2 host and cannot call `127.0.0.1`; it needs
a reachable HTTPS URL for `/events/cockroach`. Ngrok supplies that URL without
changing the coordinator's private loopback binding or opening port 8000 to the
Internet.

If a public domain is available, the same can be used in cockroach db change feed

## Explicit endpoint retirement

CodeClaim does not infer that a missing endpoint is harmless. A provider must
publish an inventory and deliberately retire an operation through the retirement
API/MCP tool. Retirement creates an immutable tombstone, records migration notes
and an optional replacement, and emits an outbox event. If an endpoint disappears
from an inventory without a tombstone, CodeClaim creates
`ENDPOINT_RETIREMENT_REVIEW_REQUIRED` work instead of silently declaring it
removed or safe.

## Further documentation

- [`LIVE_TESTING.md`](LIVE_TESTING.md): complete live Antigravity/Codex runbook.
- [`infra/cockroach/README.md`](infra/cockroach/README.md): CockroachDB setup notes.
- [`infra/cockroach/changefeed.sql`](infra/cockroach/changefeed.sql): webhook
  changefeed template.
- [`infra/nginx.conf`](infra/nginx.conf): stable HTTPS reverse-proxy template.
- [`infra/skills/cockroach_mcp_audit.md`](infra/skills/cockroach_mcp_audit.md):
  managed MCP audit setup.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.
