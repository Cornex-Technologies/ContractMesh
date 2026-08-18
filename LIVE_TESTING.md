# CodeClaim Live Testing and Agent Integration

This runbook validates the hosted coordinator through REST, then connects one
Antigravity harness to `billing-service` and one Codex harness to
`orders-service` through the local CodeClaim MCP server.

The live scenario is intentionally different from `scripts/run_demo.py`:

- `IS_DEMO_MODE=false` is required;
- the coordinator is the authority for REST operations;
- CockroachDB changefeed delivery is used instead of direct event injection;
- the existing Orders dependency remains historical truth after its task ends;
- provider deployment is rejected until compatibility work is approved.

For the real-harness recording, do not run `scripts/run_demo.py` or
`scripts/live_harness_scenario.py --manual`. The former injects simulated
events, while the latter runs its own REST scenario. Use only
`--register-only` from the latter to create fresh harness identities.

## 0. Prepare the two fixture repositories

The checked-in fixtures are intentionally prepared for a two-stage demo:

- Billing v1 exposes `POST /v1/charges` without `token_id`.
- Orders already contains the global `TOKEN_ID = "demo-token-id"` in
  `main.py`, but the initial Billing client request does not send it.
- Codex must add that value to the Billing request after CodeClaim reports the
  required-field change.

Verify both fixture repositories before changing the database:

```powershell
Set-Location C:\Users\dell\Desktop\Projects\code-claim
$env:PYTHONPATH = (Get-Location).Path

.\.venv\Scripts\python.exe -m pytest repos\billing-service\tests -q
.\.venv\Scripts\python.exe -m pytest repos\orders-service\tests -q
```

Both commands must pass before continuing. Do not add `token_id` to the
Billing schema yet; that is the Antigravity change that creates the breaking
revision.

## 1. Configure a clean live database

Use a dedicated CockroachDB database for the demo. Do not repeatedly run the
scenario against a database containing old contract revisions or unresolved
compatibility work.

From the project root:

```powershell
Set-Location C:\Users\dell\Desktop\Projects\code-claim
```

For a dedicated demo database, stop the coordinator first and reset only the
runtime records. The migration ledger is preserved:

```powershell
.\.venv\Scripts\python.exe scripts\reset_demo_data.py --database-name codeclaim_db --yes
```

The command refuses to run against a database with a different name. Do not
run it against a shared or production database.

Configure `.env` without committing it:

```dotenv
COCKROACH_DATABASE_URL=<CockroachDB URL with sslmode=verify-full>
AWS_REGION=asia-southeast-1
BEDROCK_EMBEDDING_PROVIDER=cohere_v4
BEDROCK_EMBEDDING_MODEL_ID=cohere.embed-v4:0
EMBEDDING_DIMENSION=1536
CHANGEFEED_WEBHOOK_SECRET=<strong-secret>
COORDINATOR_API_KEY=<operator-secret>
IS_DEMO_MODE=false
DEMO_AUTO_RECONCILE=false
```

On EC2, use an IAM role with Bedrock `InvokeModel` permission. Do not store
AWS access keys in the repository.

Apply versioned migrations:

```powershell
.\.venv\Scripts\python.exe -m coordinator.db
```

On Windows, use a Selector event loop for one-off psycopg checks:

```powershell
@'
import asyncio, selectors
from coordinator.db import check_health, close_pool

loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
try:
    print(loop.run_until_complete(check_health()))
finally:
    loop.run_until_complete(close_pool())
    loop.close()
'@ | .\.venv\Scripts\python.exe
```

## 2. Start and verify the coordinator, browser, and public callback path

From the project root, use the Windows launcher. It configures the Selector
event loop required by Psycopg:

```powershell
$env:PYTHONPATH=(Get-Location).Path
$env:IS_DEMO_MODE="false"
$env:DEMO_AUTO_RECONCILE="false"
.\.venv\Scripts\python.exe scripts\start_server.py
```

Keep this terminal open. The coordinator is the FastAPI backend; the browser
is only its client.

For AWS, expose only HTTPS through Nginx or an Application Load Balancer and
keep Uvicorn private on `127.0.0.1:8000`.

Verify the process:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

The live response must contain:

```json
{
  "status": "healthy",
  "demo_mode": false,
  "database": {"status": "healthy"}
}
```

Open the browser dashboard at:

```text
http://127.0.0.1:8000/control
```

The browser should show the dashboard shell and a healthy CockroachDB status.

In a separate terminal, start the public callback tunnel:

```powershell
ngrok http 8000
```

Copy the HTTPS forwarding URL and verify the public health endpoint:

```powershell
$env:CODECLAIM_BASE_URL="http://127.0.0.1:8000"
$env:CHANGEFEED_PUBLIC_BASE_URL="https://your-ngrok-url.ngrok-free.app"
Invoke-RestMethod "$env:CHANGEFEED_PUBLIC_BASE_URL/health"
```

The public health response must be healthy and must not be `502 Bad Gateway`.
The callback URL is the same public URL with `/events/cockroach` appended. Its
real end-to-end verification happens when the first onboarding outbox event is
delivered; a persistent `502` there means ngrok is not forwarding to port
8000.

Do not run `live_preflight.py` yet. It intentionally requires the onboarded
contracts and confirmed dependency from the next sections.

## 3. Configure the real changefeed

Use [infra/ccloud/provision_changefeed.sql](infra/ccloud/provision_changefeed.sql).
Replace these placeholders before executing the SQL:

```text
${MCP_AUDIT_PASSWORD}
${COORDINATOR_WEBHOOK_URL}
${CHANGEFEED_WEBHOOK_SECRET}
```

The coordinator webhook must be public HTTPS:

```text
https://your-coordinator.example/events/cockroach
```

The CockroachDB Managed MCP audit role is read-only. It is separate from the
trusted local CodeClaim MCP process used by coding harnesses.

## 4. Onboard Billing and Orders

The checked-in services use `main:app`:

```powershell
.\.venv\Scripts\python.exe -m coordinator.cli onboard `
  --service-name billing-service `
  --repository-path C:\Users\dell\Desktop\Projects\code-claim\repos\billing-service `
  --endpoint-code-dir . `
  --app-entry main:app `
  --yes

.\.venv\Scripts\python.exe -m coordinator.cli onboard `
  --service-name orders-service `
  --repository-path C:\Users\dell\Desktop\Projects\code-claim\repos\orders-service `
  --endpoint-code-dir . `
  --app-entry main:app `
  --yes
```

Onboarding publishes initial revisions, writes only `.codeclaim/service.json`
in each service repository, and records audit events. It does not modify
application source code.

Confirm the exact Orders dependency:

```powershell
.\.venv\Scripts\python.exe -m coordinator.cli dependencies `
  --consumer-service orders-service `
  --repository-path C:\Users\dell\Desktop\Projects\code-claim\repos\orders-service `
  --endpoint-code-dir clients `
  --provider-service billing-service `
  --confirmed-by orders-owner
```

Confirm only `billing-service POST /v1/charges` at revision 1.

Now run the read-only live preflight:

```powershell
.\.venv\Scripts\python.exe scripts\live_preflight.py
```

It must print `READY: yes`. A non-zero exit means the live scenario must not
be started.

After Antigravity edits the Billing FastAPI code, publish the next immutable
revision from the generated OpenAPI document. Do not run `onboard` again; that
command is intentionally one-time only:

```powershell
.\.venv\Scripts\python.exe -m coordinator.cli publish-revision `
  --service-name billing-service `
  --repository-path C:\Users\dell\Desktop\Projects\code-claim\repos\billing-service `
  --endpoint-code-dir . `
  --app-entry main:app `
  --endpoint-path /v1/charges `
  --http-method POST `
  --published-by antigravity `
  --yes
```

This extracts the current FastAPI OpenAPI contract, resolves the next revision
from CockroachDB, and atomically writes the contract diff, outbox event, audit
record, and compatibility work. The coordinator still needs a configured
CockroachDB changefeed for the drift worker to receive the outbox event.

## 5. Register fresh harnesses

The REST scenario can register fresh identities and print each token once:

```powershell
$env:CODECLAIM_BASE_URL="https://your-coordinator.example"
$env:COORDINATOR_API_KEY=<operator-secret>
$env:CODECLAIM_REGISTER_HARNESSES="true"

.\.venv\Scripts\python.exe scripts/live_harness_scenario.py --register-only
```

The registrations are:

```text
live-antigravity-billing → billing-service
live-codex-orders        → orders-service
```

Store the printed values in a secret manager or process environment. Do not
commit them to `mcp_*.json`.

Alternatively, register through the operator endpoint `POST /harnesses/register`.

If a token is exposed, invalidate it without deleting historical tasks:

```powershell
$headers = @{ "X-Operator-Token" = $env:COORDINATOR_API_KEY }
Invoke-RestMethod -Method Post `
  -Uri "$env:CODECLAIM_BASE_URL/harnesses/<harness-id>/disable" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body '{"actor":"operator-token-rotation"}'
```

## 6. Run the REST-backed live scenario

```powershell
$env:IS_DEMO_MODE="false"
$env:DEMO_AUTO_RECONCILE="false"
$env:CODECLAIM_BASE_URL="https://your-coordinator.example"
$env:COORDINATOR_API_KEY=<operator-secret>
$env:BILLING_HARNESS_ID=<billing-harness-id>
$env:BILLING_HARNESS_TOKEN=<billing-harness-token>
$env:ORDERS_HARNESS_ID=<orders-harness-id>
$env:ORDERS_HARNESS_TOKEN=<orders-harness-token>
$env:BILLING_CONTRACT_REVISION="v2"
$env:ORDERS_WORKTREE_PATH="C:\worktrees\orders-live"

.\.venv\Scripts\python.exe scripts/live_harness_scenario.py --manual
```

The script performs the durable sequence:

```text
Orders task registered and completed
→ Billing revision 2 published
→ historical dependency creates late compatibility work
→ Orders harness claims the work
→ deployment is rejected before approval
→ operator approves and rebinds the dependency
→ deployment succeeds
```

When `--manual` pauses, edit `clients/billing_client.py` in the Orders
worktree with Codex, run its tests, and press Enter. The script then submits
checkpoint and test evidence through REST.

The live script refuses `IS_DEMO_MODE=true`, refuses automatic reconciliation,
and requires HTTPS for non-local coordinator URLs. It never calls the internal
`ingest_changefeed_event()` simulation adapter.

## 7. Connect Antigravity through CodeClaim MCP

The local MCP process authenticates exactly one registered harness through
`MCP_HARNESS_ID` and `MCP_HARNESS_TOKEN`. Configure Antigravity with the
redacted template in [mcp_antigravity.json](mcp_antigravity.json), replacing
placeholders through its secret/environment settings:

```json
{
  "mcpServers": {
    "codeclaim": {
      "command": "C:\\Users\\dell\\Desktop\\Projects\\code-claim\\.venv\\Scripts\\python.exe",
      "args": ["-m", "coordinator.mcp_server"],
      "env": {
        "PYTHONPATH": "C:\\Users\\dell\\Desktop\\Projects\\code-claim",
        "AWS_REGION": "asia-southeast-1",
        "MCP_HARNESS_ID": "<billing-harness-id>",
        "MCP_HARNESS_TOKEN": "<billing-harness-token>",
        "COCKROACH_DATABASE_URL": "<trusted-database-url>"
      }
    }
  }
}
```

First call the read-only `get_harness_identity` tool. Then instruct the
provider harness to run tests and call `publish_contract_revision` for
`billing-service POST /v1/charges`. Use `retire_endpoint` for an explicit
endpoint removal; do not silently omit an endpoint.

If Antigravity cannot run local stdio MCP servers, use the authenticated REST
endpoints instead. The REST path is the remote-coordinator-safe integration.

## 8. Connect Codex through CodeClaim MCP

Configure Codex with the Orders identity. A TOML equivalent is:

```toml
[mcp_servers.codeclaim]
command = "C:\\Users\\dell\\Desktop\\Projects\\code-claim\\.venv\\Scripts\\python.exe"
args = ["-m", "coordinator.mcp_server"]
cwd = "C:\\Users\\dell\\Desktop\\Projects\\code-claim"
enabled = true

[mcp_servers.codeclaim.env]
PYTHONPATH = "C:\\Users\\dell\\Desktop\\Projects\\code-claim"
AWS_REGION = "asia-southeast-1"
MCP_HARNESS_ID = "<orders-harness-id>"
MCP_HARNESS_TOKEN = "<orders-harness-token>"
COCKROACH_DATABASE_URL = "<trusted-database-url>"
```

The first Codex tool call should be `get_harness_identity`. The normal
consumer sequence is:

```text
discover_relevant_contracts
→ register_task with the exact confirmed dependency
→ complete_task when baseline work ends
→ claim_compatibility_work after Billing publishes v2
→ checkpoint_task at the testing boundary
→ submit_compatibility_evidence after pytest passes
```

Codex must not approve compatibility work or deploy. Those are operator REST
operations.

## 9. Current provider-task limitation

The current task-registration contract requires at least one dependency. A
provider-only Billing task therefore cannot currently be represented through
`register_task` without inventing a dependency. The demo correctly represents
provider work through `publish_contract_revision` and consumer work through the
Orders task/compatibility lifecycle.

## 10. Validation

Run the normal suite:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -k "not live"
```

Run the opt-in live test only after the coordinator and changefeed are ready:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -m live
```

The final dashboard should show:

```text
TASK_COMPLETED
→ CONTRACT_PUBLISHED
→ COMPATIBILITY_WORK_CREATED
→ COMPATIBILITY_WORK_CLAIMED
→ TESTS_PASSED
→ PLAN_APPROVED
→ DEPLOYMENT_COMPLETED
```
