"""Command-line helper for coding agents (Codex / Antigravity) to communicate with CodeClaim Coordinator."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from coordinator.contract_registry import publish_contract_revision, extract_pydantic_schema_from_repo
from coordinator.reconciliation import check_task_drift, submit_reconciled_plan
from coordinator.agent_runner import start_agent_b_checkout_task, adapt_agent_b_code_and_reconcile
from coordinator.db import init_db


async def handle_publish(args):
    await init_db()
    repo_path = ROOT_DIR / args.repo
    schema = extract_pydantic_schema_from_repo(repo_path, args.schema_file, args.model_name)
    res = await publish_contract_revision(
        service_name=args.service,
        endpoint_path=args.path,
        http_method=args.method,
        revision_number=args.revision,
        schema_json=schema,
        semantic_summary=args.summary,
        published_by=args.agent_id,
    )
    print(json.dumps(res, indent=2))


async def handle_drift_check(args):
    await init_db()
    drift = await check_task_drift(args.task_id)
    if drift:
        print(json.dumps({"drift_detected": True, "drift": drift}, indent=2))
    else:
        print(json.dumps({"drift_detected": False, "instruction": "CONTINUE"}, indent=2))


async def handle_reconcile(args):
    await init_db()
    res = await adapt_agent_b_code_and_reconcile(
        task_id=args.task_id,
        drift_id=args.drift_id,
        worktree_path=args.worktree,
        auto_reconcile=args.auto_approve,
        base_dir=ROOT_DIR,
    )
    print(json.dumps(res, indent=2))


def main():
    parser = argparse.ArgumentParser(description="CodeClaim Agent Communication CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Publish contract (Codex)
    p_pub = subparsers.add_parser("publish", help="Publish a contract revision")
    p_pub.add_argument("--service", default="billing-service")
    p_pub.add_argument("--repo", default="repos/billing-service")
    p_pub.add_argument("--path", default="/v1/charges")
    p_pub.add_argument("--method", default="POST")
    p_pub.add_argument("--revision", type=int, required=True)
    p_pub.add_argument("--schema-file", default="schemas_v2.py")
    p_pub.add_argument("--model-name", default="ChargeRequest")
    p_pub.add_argument("--summary", default="Updated charge contract")
    p_pub.add_argument("--agent-id", default="codex-billing-agent")

    # Check drift (Antigravity)
    p_drift = subparsers.add_parser("check-drift", help="Check for active contract drift")
    p_drift.add_argument("--task-id", required=True)

    # Reconcile code & submit test gate (Antigravity)
    p_recon = subparsers.add_parser("reconcile", help="Adapt client code, run pytest, and submit plan")
    p_recon.add_argument("--task-id", required=True)
    p_recon.add_argument("--drift-id", required=True)
    p_recon.add_argument("--worktree", required=True)
    p_recon.add_argument("--auto-approve", action="store_true", default=False)

    args = parser.parse_args()
    if args.command == "publish":
        asyncio.run(handle_publish(args))
    elif args.command == "check-drift":
        asyncio.run(handle_drift_check(args))
    elif args.command == "reconcile":
        asyncio.run(handle_reconcile(args))


if __name__ == "__main__":
    main()
