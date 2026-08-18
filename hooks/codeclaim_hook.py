"""CodeClaim Deterministic Hook Interceptor.

Fires mechanically before coding agent tool calls, test executions, or git commits.
If active breaking drift is detected in CockroachDB, it blocks the action and injects
the exact schema diff and migration notes into the agent's observation space.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Optional

ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from coordinator.reconciliation import check_task_drift
from coordinator.db import init_db, fetch_one


async def evaluate_pre_tool_hook(
    task_id: str,
    tool_name: str,
    tool_args: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Inspects task drift before allowing the agent to write files or run commands."""
    await init_db()
    drift = await check_task_drift(task_id)

    # 1. If NO drift: Allow the tool execution to proceed normally
    if not drift or drift.get("drift_status") != "ACTIVE_INTERVENTION":
        return {
            "allowed": True,
            "status": "ALLOW",
            "message": f"Tool '{tool_name}' allowed. No contract drift.",
        }

    # 2. If BREAKING DRIFT is active: Mechanically BLOCK tool execution
    breaking_diff = drift.get("breaking_diff") or {}
    migration_notes = drift.get("migration_notes") or "Breaking change detected in upstream contract."
    
    error_payload = {
        "allowed": False,
        "status": "BLOCKED_BY_CODECLAIM_DRIFT",
        "instruction": "REPLAN_REQUIRED",
        "drift_id": drift["drift_id"],
        "source_service": drift["source_service"],
        "old_contract_revision": drift["old_contract_revision"],
        "new_contract_revision": drift["new_contract_revision"],
        "breaking_diff": breaking_diff,
        "migration_notes": migration_notes,
        "message": (
            f"[CODECLAIM DRIFT INTERCEPT] Action '{tool_name}' blocked! "
            f"Upstream service '{drift['source_service']}' upgraded to Revision {drift['new_contract_revision']}. "
            f"You must adapt your client to the new contract before continuing."
        ),
    }
    return error_payload


def main():
    parser = argparse.ArgumentParser(description="CodeClaim Pre-Tool Interceptor Hook")
    parser.add_argument("--task-id", required=True, help="Active task UUID")
    parser.add_argument("--tool-name", default="edit_file", help="Name of requested tool")
    parser.add_argument("--fail-fast", action="store_true", help="Exit with code 1 if blocked")

    args = parser.parse_args()
    result = asyncio.run(evaluate_pre_tool_hook(args.task_id, args.tool_name))

    print(json.dumps(result, indent=2))
    if args.fail_fast and not result["allowed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
