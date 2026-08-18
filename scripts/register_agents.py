"""Register demo harnesses and emit safe local MCP configuration templates.

The coordinator stores only token hashes.  This helper prints each one-time
token once, but deliberately writes redacted configuration files by default so
credentials do not end up in the repository or in a generated artifact that is
accidentally committed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from coordinator.compatibility import register_harness
from coordinator.db import close_pool, init_db


def build_mcp_config(
    *,
    python_executable: str,
    project_root: Path,
    harness_id: str,
    access_token: str,
    include_secrets: bool = False,
) -> dict[str, Any]:
    """Build a generic ``mcpServers`` configuration for local MCP clients.

    Safe mode includes explicit placeholders for secrets. The local CodeClaim
    MCP process requires database access, but the database URL and harness token
    must be supplied through the client's secret store or trusted environment
    rather than committed JSON.
    """
    env: dict[str, str] = {
        "PYTHONPATH": str(project_root),
        "AWS_REGION": os.environ.get("AWS_REGION", "asia-southeast-1"),
        "MCP_HARNESS_ID": harness_id,
        "MCP_HARNESS_TOKEN": access_token if include_secrets else "<inject-one-time-harness-token>",
        "COCKROACH_DATABASE_URL": (
            os.environ.get("COCKROACH_DATABASE_URL", "<inject-trusted-cockroach-url>")
            if include_secrets
            else "<inject-trusted-cockroach-url>"
        ),
    }
    return {
        "mcpServers": {
            "codeclaim": {
                "command": python_executable,
                "args": ["-m", "coordinator.mcp_server"],
                "env": env,
            }
        }
    }


def _harness_specs(name_suffix: str = "") -> list[dict[str, Any]]:
    suffix = f"-{name_suffix.strip('-')}" if name_suffix.strip("-") else ""
    return [
        {
            "config_name": "antigravity",
            "harness_name": f"live-antigravity-billing{suffix}",
            "harness_type": "antigravity",
            "service_name": "billing-service",
            "repository_url": str((ROOT_DIR / "repos" / "billing-service").resolve()),
            "capability_manifest": {"language": "python", "framework": "fastapi", "tools": ["mcp", "terminal"]},
        },
        {
            "config_name": "codex",
            "harness_name": f"live-codex-orders{suffix}",
            "harness_type": "codex",
            "service_name": "orders-service",
            "repository_url": str((ROOT_DIR / "repos" / "orders-service").resolve()),
            "capability_manifest": {"language": "python", "framework": "fastapi", "tools": ["mcp", "terminal", "worktree"]},
        },
    ]


async def register_agents(
    *,
    config_dir: Path,
    include_secrets: bool = False,
    name_suffix: str = "",
) -> list[dict[str, Any]]:
    """Register the provider/consumer demo identities and write safe configs."""
    config_dir.mkdir(parents=True, exist_ok=True)
    await init_db()
    try:
        registrations: list[dict[str, Any]] = []
        for spec in _harness_specs(name_suffix):
            registration = await register_harness(
                harness_name=spec["harness_name"],
                harness_type=spec["harness_type"],
                service_name=spec["service_name"],
                repository_url=spec["repository_url"],
                dispatch_mode="poll",
                capability_manifest=spec["capability_manifest"],
            )
            registrations.append(registration)
            config = build_mcp_config(
                python_executable=sys.executable,
                project_root=ROOT_DIR,
                harness_id=str(registration["harness_id"]),
                access_token=str(registration["access_token"]),
                include_secrets=include_secrets,
            )
            config_path = config_dir / f"mcp_{spec['config_name']}.json"
            config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
            print(f"Registered {spec['service_name']} for {spec['harness_type']}: {registration['harness_id']}")
            print(f"One-time harness token: {registration['access_token']}")
            print(f"Safe MCP config: {config_path}")
        if not include_secrets:
            print("MCP configs are redacted. Inject MCP_HARNESS_TOKEN and COCKROACH_DATABASE_URL through the client secret store.")
        else:
            print("WARNING: secret-bearing MCP configs were requested; keep them outside version control.")
        return registrations
    finally:
        await close_pool()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register CodeClaim demo harnesses and generate MCP configs")
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path.home() / ".codeclaim",
        help="Directory for generated MCP configs (default: user-local ~/.codeclaim)",
    )
    parser.add_argument(
        "--include-secrets",
        action="store_true",
        help="Write one-time harness tokens and the current DB URL into configs; avoid for normal use",
    )
    parser.add_argument(
        "--name-suffix",
        default="",
        help="Optional suffix for fresh registrations, useful when retaining prior demo identities",
    )
    args = parser.parse_args(argv)
    asyncio.run(
        register_agents(
            config_dir=args.config_dir,
            include_secrets=args.include_secrets,
            name_suffix=args.name_suffix,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
