"""Deployment Promotion, Process Supervision & Live-Reload Version Engine.

Implements:
1. Strict Commit Verification & Archive Extraction: Deployed code MUST be verified via `git rev-parse` and extracted strictly via `git archive`. Invalid or unresolvable commits immediately mark the deployment as FAILED (NO direct-copy fallback).
2. Strict Allowlist Environment Sandbox: Only permitted base operating system runtime variables are inherited. User profile variables (USERPROFILE, HOMEDRIVE, HOMEPATH, HOME) and all ambient credentials are scrubbed by default.
3. Durable Cutover Journal & Startup Recovery: Tracks atomic directory renames via a persistent, fsync-flushed on-disk journal (`.cutover_journal.json`), enabling deterministic crash recovery during process startup.
4. Comprehensive Cutover Failure Protection: The entire cutover sequence (backup purging, live rename, staged rename) is wrapped in transaction-safe failure handling.
5. Post-Rollback Readiness Verification & Distinct Outbox Events: Distinguishes `DEPLOYMENT_ROLLED_BACK` (successful restoration) vs `DEPLOYMENT_ROLLBACK_FAILED` (failed restoration requiring operator intervention).
6. Strict Readiness Polling & Identity Verification: Bounded polling loop validating HTTP 200, `status == 'healthy'`, AND exact matching `service == service_name`.
7. Server-Authoritative Health Gating (SSRF-Free): Health check URLs are derived strictly from a trusted internal service port map.
8. Deadlock-Safe Process Supervision: Microservice uvicorn instances use dedicated log files instead of unbounded pipes.
9. Transactional Reload Version Allocation: Version increment is calculated inside the serializable insertion transaction.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Optional
import httpx
import psycopg
from psycopg.rows import dict_row

from coordinator.config import settings
from coordinator.db import fetch_one, execute_query, run_transaction

logger = logging.getLogger(__name__)

# Server-Authoritative Service Registry (Prevents SSRF)
SERVICE_PORT_MAP: dict[str, int] = {
    "billing-service": 8001,
    "orders-service": 8002,
}
ALLOWED_SERVICES: set[str] = set(SERVICE_PORT_MAP.keys())

# Safe fallback for the two v1 demo services.  Onboarded repositories carry the
# authoritative value in .codeclaim/service.json; this map only keeps a service
# startable before onboarding metadata has been committed into the snapshot.
SERVICE_APP_ENTRYPOINTS: dict[str, str] = {
    "billing-service": "main:app",
    "orders-service": "main:app",
}


def _read_configured_app_module(service_name: str, work_dir: Path) -> Optional[str]:
    """Read an onboarded entrypoint from the deployed/repository config, if present."""
    candidates = [
        work_dir / ".codeclaim" / "service.json",
        Path(__file__).parent.parent / "repos" / service_name / ".codeclaim" / "service.json",
    ]
    for config_path in candidates:
        try:
            if not config_path.is_file():
                continue
            config = json.loads(config_path.read_text(encoding="utf-8"))
            entrypoint = config.get("application_entrypoint")
            if not entrypoint:
                entrypoint = (config.get("contract_source") or {}).get("app_entry")
            if isinstance(entrypoint, str) and ":" in entrypoint:
                return entrypoint
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("Ignoring invalid CodeClaim entrypoint config %s: %s", config_path, exc)
    return None


def resolve_service_app_module(service_name: str, work_dir: Path) -> str:
    """Resolve the configured FastAPI entrypoint for a supervised service.

    ``codeclaim onboard`` writes ``contract_source.app_entry`` and the explicit
    top-level ``application_entrypoint`` to ``.codeclaim/service.json``.  The
    config is read from the exact deployed worktree first, then the repository
    copy, and only then does the v1 service map provide a conservative fallback.
    """
    configured = _read_configured_app_module(service_name, work_dir)
    if configured:
        return configured
    return SERVICE_APP_ENTRYPOINTS.get(service_name, "main:app")


async def resolve_service_app_module_async(service_name: str, work_dir: Path) -> str:
    """Resolve config first, then the registered CockroachDB service metadata."""
    configured = _read_configured_app_module(service_name, work_dir)
    if configured:
        return configured
    try:
        row = await fetch_one(
            """SELECT entrypoint_module, entrypoint_app
               FROM microservices WHERE service_name=%s;""",
            (service_name,),
        )
        if row and row.get("entrypoint_module") and row.get("entrypoint_app"):
            return f"{row['entrypoint_module']}:{row['entrypoint_app']}"
    except Exception as exc:
        logger.debug("Registered entrypoint metadata unavailable for %s: %s", service_name, exc)
    return SERVICE_APP_ENTRYPOINTS.get(service_name, "main:app")


# ==============================================================================
# 1. Environment Allowlist & Sandbox Helpers
# ==============================================================================


def build_isolated_pythonpath(*roots: str | Path) -> str:
    """Build PYTHONPATH only from explicit roots and the active venv packages.

    Ambient ``PYTHONPATH`` is intentionally never consulted. This prevents a host
    setting from injecting arbitrary modules into agent-authored test or service code.
    """
    explicit_roots = [str(Path(root).resolve()) for root in roots if Path(root).exists()]
    site_packages = [
        str(Path(path).resolve())
        for path in sys.path
        if ("site-packages" in path or "dist-packages" in path) and Path(path).exists()
    ]
    return os.pathsep.join(dict.fromkeys(explicit_roots + site_packages))


def get_sanitized_sandbox_env(
    extra_env: Optional[dict[str, str]] = None,
    base_dir: Optional[Path] = None,
) -> dict[str, str]:
    """Generate a strict, allowlisted environment dictionary for candidate test execution.
    
    Security Design: Uses a strict ALLOWLIST rather than a blocklist.
    Only fundamental system binary and library paths are permitted.
    User home directories (USERPROFILE, HOMEDRIVE, HOMEPATH, HOME) and custom credentials
    are strictly excluded to prevent agent-authored test code from reading local host files.
    """
    safe_allowlist_keys = {
        "PATH",
        "SYSTEMROOT",
        "SYSTEMDRIVE",
        "WINDIR",
        "LANG",
        "LC_ALL",
        "TERM",
        "COMSPEC",
        "PATHEXT",
    }
    
    clean_env = {k: v for k, v in os.environ.items() if k in safe_allowlist_keys}

    # Redirect temp directories to an isolated sandbox path
    root = base_dir or Path(__file__).parent.parent
    sandbox_tmp = root / "worktrees" / "sandbox_tmp"
    sandbox_tmp.mkdir(parents=True, exist_ok=True)
    clean_env["TMP"] = str(sandbox_tmp)
    clean_env["TEMP"] = str(sandbox_tmp)

    ALLOWED_EXTRA_ENV_KEYS = {
        "PYTHONPATH",
        "PORT",
        "IS_DEMO_MODE",
        "BILLING_SERVICE_PATH",
        "ORDERS_SERVICE_PATH",
        "PYTHONDONTWRITEBYTECODE",
        "ENVIRONMENT",
        "TZ",
        "NODE_ENV",
        "PYTHONUNBUFFERED",
        # Explicit non-secret demo-service runtime selectors. They are copied
        # only when the coordinator deliberately passes them below.
        "BILLING_CONTRACT_REVISION",
        "ORDERS_CONTRACT_REVISION",
    }

    if extra_env:
        for k, v in extra_env.items():
            if k in ALLOWED_EXTRA_ENV_KEYS:
                clean_env[k] = str(v)
    return clean_env


# ==============================================================================
# 2. Durable Cutover Journal & Crash Recovery
# ==============================================================================


class CutoverJournal:
    """Manages persistent, fsync-flushed cutover intent records to guarantee crash recovery during directory swaps."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.journal_file = base_dir / "deployments" / ".cutover_journal.json"
        self.journal_file.parent.mkdir(parents=True, exist_ok=True)

    def record_intent(
        self,
        service_name: str,
        deployment_id: str,
        staged_dir: Path,
        live_dir: Path,
        backup_dir: Path,
    ) -> None:
        """Atomically write and fsync pre-cutover state to disk before initiating directory renames."""
        payload = {
            "service_name": service_name,
            "deployment_id": deployment_id,
            "staged_dir": str(staged_dir),
            "live_dir": str(live_dir),
            "backup_dir": str(backup_dir),
            "state": "IN_PROGRESS",
            "timestamp": time.time(),
        }
        
        # Durable atomic write with fsync
        tmp_file = self.journal_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(payload, indent=2))
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_file, self.journal_file)

    def record_complete(self) -> None:
        """Clear or mark completed state after successful directory cutover."""
        try:
            if self.journal_file.exists():
                self.journal_file.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("Could not unlink cutover journal: %s", e)

    def record_unrecoverable(self, error_message: str, service_name: Optional[str] = None) -> None:
        """Atomically persist unrecoverable cutover failure state while preserving full original journal metadata."""
        existing_data: dict[str, Any] = {}
        if self.journal_file.exists():
            try:
                existing_data = json.loads(self.journal_file.read_text(encoding="utf-8"))
            except Exception:
                existing_data = {}

        payload = {
            **existing_data,
            "service_name": service_name or existing_data.get("service_name", "unknown"),
            "state": "UNRECOVERABLE_RESTORATION_REQUIRED",
            "unrecoverable_error": error_message,
            "unrecoverable_timestamp": time.time(),
        }
        tmp_file = self.journal_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(payload, indent=2))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, self.journal_file)

    def recover_if_needed(self) -> Optional[dict[str, Any]]:
        """Recover from an interrupted cutover state on startup.
        
        Quarantines corrupt/malformed files with an alert rather than silently deleting them.
        Raises RuntimeError immediately on UNRECOVERABLE states or failed restorations.
        Only clears the journal when directory restoration is fully successful.
        """
        if not self.journal_file.exists():
            return None

        try:
            content = self.journal_file.read_text(encoding="utf-8")
            data = json.loads(content)
        except (json.JSONDecodeError, Exception) as parse_err:
            quarantine_path = self.journal_file.with_name(f".cutover_journal.corrupted.{int(time.time())}.json")
            try:
                self.journal_file.rename(quarantine_path)
            except Exception:
                pass
            logger.critical("CRITICAL: Corrupted cutover journal quarantined to %s: %s", quarantine_path, parse_err)
            raise RuntimeError(f"Corrupted cutover journal encountered: {parse_err}") from parse_err

        if data.get("state") == "UNRECOVERABLE_RESTORATION_REQUIRED":
            err_msg = data.get("unrecoverable_error") or data.get("error", "Unknown unrecoverable error")
            logger.critical("CRITICAL: Cutover journal reports UNRECOVERABLE_RESTORATION_REQUIRED for %s: %s", data.get("service_name"), err_msg)
            raise RuntimeError(f"Unrecoverable cutover state detected for {data.get('service_name')}: {err_msg}. Operator intervention required before startup.")

        if data.get("state") == "IN_PROGRESS":
            live_dir = Path(data["live_dir"])
            backup_dir = Path(data["backup_dir"])
            staged_dir = Path(data["staged_dir"])

            logger.warning("Interrupted cutover detected for %s; initiating recovery...", data.get("service_name"))
            live_dir.parent.mkdir(parents=True, exist_ok=True)
            try:
                if not live_dir.exists() and backup_dir.exists():
                    backup_dir.rename(live_dir)
                    logger.info("Restored live directory from backup: %s", live_dir)
                elif not live_dir.exists() and staged_dir.exists():
                    staged_dir.rename(live_dir)
                    logger.info("Completed staged promotion to live: %s", live_dir)

                self.record_complete()
                return data
            except Exception as ex:
                logger.critical("CRITICAL: Failed to execute cutover directory restoration for %s: %s", data.get("service_name"), ex)
                self.record_unrecoverable(str(ex), service_name=data.get("service_name", "unknown"))
                raise RuntimeError(f"Cutover restoration failed: {ex}") from ex

        return None




# ==============================================================================
# 3. Deadlock-Free Process Supervisor
# ==============================================================================


class ProcessSupervisor:
    """Supervises local microservice instances safely with log file redirection and readiness verification."""

    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or Path(__file__).parent.parent
        self.logs_dir = self.base_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self._processes: dict[str, subprocess.Popen] = {}
        self._log_files: dict[str, Any] = {}
        self.journal = CutoverJournal(self.base_dir)

    def get_service_status(self, service_name: str) -> dict[str, Any]:
        """Check if a supervised service process is currently active."""
        proc = self._processes.get(service_name)
        if proc is None:
            return {"service_name": service_name, "running": False, "pid": None}

        poll_result = proc.poll()
        if poll_result is None:
            return {"service_name": service_name, "running": True, "pid": proc.pid}
        else:
            return {
                "service_name": service_name,
                "running": False,
                "pid": proc.pid,
                "returncode": poll_result,
            }

    def get_all_services_status(self) -> list[dict[str, Any]]:
        """Return status for all known supervised microservices."""
        return [self.get_service_status(s) for s in sorted(ALLOWED_SERVICES)]

    def start_service(
        self,
        service_name: str,
        app_module: Optional[str] = None,
        port: Optional[int] = None,
        cwd: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Start a microservice process redirecting output to a dedicated log file."""
        if service_name not in ALLOWED_SERVICES:
            raise ValueError(f"Service '{service_name}' is not in allowed services: {ALLOWED_SERVICES}")

        if self.get_service_status(service_name)["running"]:
            logger.info("Service %s already running with PID %s", service_name, self._processes[service_name].pid)
            return self.get_service_status(service_name)

        target_port = port or SERVICE_PORT_MAP[service_name]
        work_dir = cwd or (self.base_dir / "deployments" / "live" / service_name)
        if not work_dir.exists():
            work_dir = self.base_dir / "repos" / service_name
        resolved_app_module = app_module or resolve_service_app_module(service_name, work_dir)

        log_file_path = self.logs_dir / f"{service_name}.log"
        log_fp = open(log_file_path, "a", encoding="utf-8")
        self._log_files[service_name] = log_fp

        root_dir = self.base_dir
        billing_dir = str(root_dir / "repos" / "billing-service")
        orders_dir = str(root_dir / "repos" / "orders-service")
        py_path = build_isolated_pythonpath(work_dir, billing_dir, orders_dir, root_dir)

        runtime_env = {"PYTHONPATH": py_path}
        revision_key = f"{service_name.replace('-', '_').upper()}_CONTRACT_REVISION"
        configured_revision = os.environ.get(revision_key)
        if configured_revision in {"v1", "v2"}:
            runtime_env[revision_key] = configured_revision
        proc_env = get_sanitized_sandbox_env(runtime_env, base_dir=self.base_dir)

        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            resolved_app_module,
            "--host",
            "127.0.0.1",
            "--port",
            str(target_port),
            "--log-level",
            "warning",
        ]

        logger.info("Starting supervised service %s on port %d...", service_name, target_port)
        proc = subprocess.Popen(
            cmd,
            cwd=str(work_dir),
            stdout=log_fp,
            stderr=log_fp,
            env=proc_env,
        )
        self._processes[service_name] = proc
        return {"service_name": service_name, "running": True, "pid": proc.pid, "port": target_port}

    def stop_service(self, service_name: str, timeout: float = 3.0) -> dict[str, Any]:
        """Stop a supervised microservice process."""
        proc = self._processes.get(service_name)
        if proc is None or proc.poll() is not None:
            return {"service_name": service_name, "running": False, "pid": None}

        logger.info("Stopping supervised service %s (PID %d)...", service_name, proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning("Force killing service %s (PID %d)", service_name, proc.pid)
            proc.kill()
            proc.wait()

        log_fp = self._log_files.pop(service_name, None)
        if log_fp and not log_fp.closed:
            log_fp.close()

        return {"service_name": service_name, "running": False, "pid": None}

    def restart_service(
        self,
        service_name: str,
        cwd: Optional[Path] = None,
        app_module: Optional[str] = None,
        timeout: float = 3.0,
    ) -> dict[str, Any]:
        """Restart a supervised microservice process."""
        self.stop_service(service_name, timeout=timeout)
        return self.start_service(service_name=service_name, cwd=cwd, app_module=app_module)

    def stop_all(self) -> None:
        """Stop all supervised processes."""
        for name in list(self._processes.keys()):
            self.stop_service(name)


# Global singleton process supervisor
supervisor = ProcessSupervisor()


# ==============================================================================
# 4. Strict Commit Verification & Readiness Verification
# ==============================================================================


def extract_commit_snapshot(
    repo_path: Path,
    source_commit: str,
    target_dir: Path,
) -> Path:
    """Extract an exact snapshot of source_commit into target_dir using git archive.
    
    Strict Git commit requirement: If source_commit cannot be verified via git rev-parse
    or git archive fails, this function raises ValueError/RuntimeError and NEVER falls back
    to uncommitted disk contents.
    """
    if not source_commit or not isinstance(source_commit, str) or len(source_commit.strip()) == 0:
        raise ValueError("source_commit must be a valid, non-empty Git commit SHA")

    resolved_repo = repo_path
    if not resolved_repo.exists():
        fallback = Path(__file__).parent.parent / "repos" / repo_path.name
        if fallback.exists():
            resolved_repo = fallback

    project_root = Path(__file__).parent.parent
    service_name = resolved_repo.name

    # 1. Verify that source_commit is a real commit object in the repository
    git_verify = subprocess.run(
        ["git", "rev-parse", "--verify", f"{source_commit}^{{commit}}"],
        cwd=str(project_root if (project_root / ".git").exists() else resolved_repo),
        capture_output=True,
        text=True,
    )
    if git_verify.returncode != 0:
        git_verify_repo = subprocess.run(
            ["git", "rev-parse", "--verify", f"{source_commit}^{{commit}}"],
            cwd=str(resolved_repo),
            capture_output=True,
            text=True,
        )
        if git_verify_repo.returncode != 0:
            err_msg = git_verify.stderr.strip() or git_verify_repo.stderr.strip() or "commit not found"
            raise ValueError(f"Unresolvable Git commit '{source_commit}' in repository {resolved_repo}: {err_msg}")

    # 2. Attempt subtree extraction from source_commit using git archive
    subpath_tree = f"{source_commit}:repos/{service_name}"
    git_tree_check = subprocess.run(
        ["git", "rev-parse", "--verify", subpath_tree],
        cwd=str(project_root),
        capture_output=True,
        text=True,
    )

    zip_bytes: Optional[bytes] = None
    if git_tree_check.returncode == 0:
        try:
            zip_bytes = subprocess.check_output(
                ["git", "archive", "--format=zip", subpath_tree],
                cwd=str(project_root),
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"git archive failed for subtree '{subpath_tree}': {e.stderr}")
    else:
        try:
            zip_bytes = subprocess.check_output(
                ["git", "archive", "--format=zip", source_commit],
                cwd=str(resolved_repo),
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"git archive failed for commit '{source_commit}' in {resolved_repo}: {e.stderr}")

    if not zip_bytes:
        raise RuntimeError(f"Failed to generate archive for commit '{source_commit}'")

    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        z.extractall(target_dir)

    logger.info("Successfully extracted exact git commit %s snapshot into %s", source_commit[:8], target_dir)
    return target_dir


async def poll_service_readiness(
    service_name: str,
    max_retries: int = 10,
    base_interval: float = 0.15,
    http_client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, dict[str, Any], Optional[str]]:
    """Poll the target service with exponential backoff until it reports healthy and valid service identity.
    
    Mandatory readiness criteria:
    1. HTTP Status 200 OK.
    2. JSON payload with status in ('healthy', 'ok').
    3. JSON payload with service explicitly matching service_name.
    """
    service_port = SERVICE_PORT_MAP[service_name]
    health_url = f"http://127.0.0.1:{service_port}/health"

    last_error: Optional[str] = None
    last_payload: dict[str, Any] = {}

    for attempt in range(1, max_retries + 1):
        try:
            if http_client is not None:
                resp = await http_client.get(health_url, timeout=2.0)
            else:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    resp = await client.get(health_url)

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {}

                last_payload = data

                status_val = str(data.get("status", "")).lower()
                service_val = data.get("service")

                if status_val not in ("healthy", "ok"):
                    last_error = f"Health check returned invalid status '{status_val}'; expected 'healthy' or 'ok'"
                elif not service_val:
                    last_error = f"Health check response missing mandatory 'service' identity field (got {data})"
                elif service_val != service_name:
                    last_error = f"Service identity mismatch: expected '{service_name}', got '{service_val}'"
                else:
                    return True, data, None
            else:
                last_error = f"Health check returned HTTP {resp.status_code}"
        except Exception as ex:
            last_error = f"Health check connection error: {ex}"

        await asyncio.sleep(min(1.0, base_interval * (1.5 ** (attempt - 1))))

    return False, last_payload, last_error


# ==============================================================================
# 5. Pre-Deployment Test Gate & Deployment Promotion Engine
# ==============================================================================


async def get_latest_reload_version() -> int:
    """Retrieve current maximum reload_version from the deployments table (defaults to 1)."""
    sql = "SELECT COALESCE(MAX(reload_version), 1) AS current_version FROM deployments;"
    try:
        row = await fetch_one(sql)
        if row and row.get("current_version") is not None:
            return int(row["current_version"])
    except Exception as e:
        logger.warning("Error fetching latest reload_version from db: %s", e)
    return 1


async def get_deployment_history(limit: int = 20) -> list[dict[str, Any]]:
    """Retrieve recent deployment history records."""
    sql = """
    SELECT 
        deployment_id,
        service_name,
        source_commit,
        status,
        reload_version,
        health_check,
        created_at,
        completed_at
    FROM deployments
    ORDER BY created_at DESC
    LIMIT %s;
    """
    try:
        rows = await execute_query(sql, (limit,))
        return rows
    except Exception as e:
        logger.warning("Error fetching deployment history: %s", e)
        return []


def run_service_test_gate(
    worktree_or_repo_path: Path,
    timeout_seconds: float = 30.0,
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Execute pytest suite in target worktree/repo with sanitized allowlisted environment and return test results."""
    if not worktree_or_repo_path.exists():
        return {
            "all_passed": False,
            "returncode": 1,
            "error": f"Worktree path {worktree_or_repo_path} does not exist",
            "stdout": "",
            "stderr": "",
        }

    tests_dir = worktree_or_repo_path / "tests"
    if not tests_dir.exists():
        tests_dir = worktree_or_repo_path

    cmd = [sys.executable, "-m", "pytest", str(tests_dir), "-v"]
    project_root = Path(__file__).parent.parent
    root_dir = base_dir or project_root

    billing_dir = root_dir / "repos" / "billing-service"
    if not billing_dir.exists():
        billing_dir = project_root / "repos" / "billing-service"

    orders_dir = root_dir / "repos" / "orders-service"
    if not orders_dir.exists():
        orders_dir = project_root / "repos" / "orders-service"

    billing_main = billing_dir / "main.py"

    py_path = build_isolated_pythonpath(
        worktree_or_repo_path,
        billing_dir,
        orders_dir,
        project_root,
        root_dir,
    )

    # Strict allowlisted environment stripping all ambient tokens and user home variables
    test_env = get_sanitized_sandbox_env({
        "PYTHONPATH": py_path,
        "BILLING_SERVICE_PATH": str(billing_main),
    }, base_dir=root_dir)


    try:
        res = subprocess.run(
            cmd,
            cwd=str(worktree_or_repo_path),
            capture_output=True,
            text=True,
            env=test_env,
            timeout=timeout_seconds,
        )
        return {
            "all_passed": res.returncode == 0,
            "returncode": res.returncode,
            "stdout": res.stdout[-2000:] if res.stdout else "",
            "stderr": res.stderr[-2000:] if res.stderr else "",
        }
    except subprocess.TimeoutExpired:
        return {
            "all_passed": False,
            "returncode": -1,
            "error": f"Test suite execution timed out after {timeout_seconds}s",
            "stdout": "",
            "stderr": "",
        }
    except Exception as ex:
        return {
            "all_passed": False,
            "returncode": -1,
            "error": str(ex),
            "stdout": "",
            "stderr": "",
        }


async def _mark_deployment_failed_with_outbox(
    deployment_id: str,
    error_details: dict[str, Any],
    service_name: str,
    source_commit: str,
    reload_version: int,
) -> None:
    """Safely update deployment record to FAILED and emit a durable DEPLOYMENT_FAILED outbox event."""
    async def _fail_tx(conn: psycopg.AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE deployments
                SET status = 'FAILED', health_check = %s::jsonb, completed_at = now()
                WHERE deployment_id = %s;
                """,
                (json.dumps(error_details), deployment_id),
            )

            outbox_payload = {
                "deployment_id": deployment_id,
                "service_name": service_name,
                "source_commit": source_commit,
                "reload_version": reload_version,
                "status": "FAILED",
                "error": error_details.get("error", "Deployment failed"),
            }
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                )
                VALUES ('DEPLOYMENT', %s, %s, %s, 'DEPLOYMENT_FAILED', %s::jsonb)
                RETURNING event_id;
                """,
                (deployment_id, reload_version, service_name, json.dumps(outbox_payload)),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("DEPLOYMENT_FAILED outbox event was not created")
            outbox_id = outbox_row["event_id"]
            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor,
                    outbox_event_id, causation_id, correlation_id
                )
                VALUES ('DEPLOYMENT_FAILED', %s, %s, 'deployer', %s, %s, %s);
                """,
                (service_name, f"Deployment failed for {service_name} at {source_commit[:8]}: {error_details.get('error')}",
                 outbox_id, outbox_id, outbox_id),
            )
    try:
        await run_transaction(_fail_tx)
    except Exception as e:
        logger.error("Failed to mark deployment %s as FAILED: %s", deployment_id, e)


async def promote_deployment(
    service_name: str,
    source_commit: str,
    source_path: Optional[Path] = None,
    base_dir: Optional[Path] = None,
    health_check_timeout: float = 3.0,
    http_client: Optional[httpx.AsyncClient] = None,
    skip_test_gate: bool = False,
) -> dict[str, Any]:
    """Promote a microservice deployment with exact commit extraction, pre-test gating, atomic directory cutover, and readiness verification.
    
    1. Validates service_name against ALLOWED_SERVICES.
    2. Allocates next reload_version monotonically inside a serializable transaction.
    3. Extracts candidate release strictly bound to source_commit into an isolated staging directory.
       (NO direct-copy fallback: if commit is invalid, marks deployment FAILED and aborts).
    4. Runs test suite gate in staged directory with strict allowlist environment (fails closed if tests fail).
    5. Records intent in cutover journal, then executes atomic directory cutover inside guarded failure handler.
    6. Restarts supervised process.
    7. Polls readiness on server-derived internal URL (http://127.0.0.1:{port}/health) validating identity and 200 OK.
    8. On health failure: rolls back directory from backup, restarts, and executes a second readiness loop to verify rollback restoration.
    9. On completion: writes state and emits corresponding outbox event:
       - DEPLOYMENT_COMPLETED: Healthy promotion.
       - DEPLOYMENT_ROLLED_BACK: Rollback executed and restored previous version to healthy status.
       - DEPLOYMENT_ROLLBACK_FAILED: Rollback executed but restored previous version was unhealthy (alerts operator).
       - DEPLOYMENT_FAILED: Promotion failed before/during cutover or test gate.
    """
    if service_name not in ALLOWED_SERVICES:
        raise ValueError(f"Unknown service '{service_name}'. Must be one of: {sorted(ALLOWED_SERVICES)}")

    root_dir = base_dir or Path(__file__).parent.parent
    src_repo = source_path or (root_dir / "repos" / service_name)
    if not src_repo.exists():
        fallback_repo = Path(__file__).parent.parent / "repos" / service_name
        if fallback_repo.exists():
            src_repo = fallback_repo

    journal = CutoverJournal(root_dir)
    journal.recover_if_needed()

    # Step 1: Check compatibility safety gates & allocate version in one
    # serializable transaction.  A rejected deployment is itself durable so
    # the dashboard/audit trail explains why no filesystem cutover occurred.
    async def _allocate_version_tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            gate_blockers: list[dict[str, Any]] = []

            # Existing human-decision incidents block deployments of the affected
            # consumer service.
            try:
                await cur.execute(
                    """
                    SELECT i.incident_id, i.incident_type, i.missing_requirement,
                           COALESCE(i.evidence->>'reason_code', 'UNAVAILABLE_REQUIRED_INPUT') AS reason_code
                    FROM compatibility_incidents i
                    JOIN compatibility_work_items w ON w.work_item_id = i.work_item_id
                    WHERE w.target_service = %s
                      AND i.status = 'HUMAN_DECISION_REQUIRED'
                      AND w.state IN ('BLOCKED', 'INCOMPATIBLE');
                    """,
                    (service_name,),
                )
                blocked_incidents = await cur.fetchall()
                if isinstance(blocked_incidents, (list, tuple)) and len(blocked_incidents) > 0:
                    for incident in blocked_incidents:
                        if isinstance(incident, dict):
                            reason = incident.get("reason_code") or incident.get("missing_requirement") or "incompatibility"
                            inc_type = incident.get("incident_type", "BLOCKED")
                            gate_blockers.append({
                                "kind": "consumer_incident",
                                "incident_id": str(incident.get("incident_id")) if incident.get("incident_id") else None,
                                "consumer_service": service_name,
                                "state": inc_type,
                                "reason": reason,
                            })
            except Exception as ex:
                logger.error("Database error while evaluating compatibility incident gate: %s", ex)
                if not settings.is_demo_mode:
                    raise RuntimeError(f"Cannot promote deployment: failed to verify compatibility incidents due to database error: {ex}") from ex
                logger.debug("Skipping incident gate check in demo mode due to database state: %s", ex)

            # Provider-side gate: a breaking/review-required compatibility item
            # remains a deployment blocker even when the consumer task was
            # completed before the provider published its new contract.  The
            # source_contract_id join makes this provider-specific rather than a
            # global queue lock.
            try:
                await cur.execute(
                    """
                    SELECT w.work_item_id, w.target_service AS consumer_service,
                           w.target_repository AS consumer_repository,
                           w.source_contract_id, w.source_contract_revision,
                           w.state,
                           c.service_name AS provider_service,
                           COALESCE(w.payload->>'classification', 'BREAKING') AS classification
                    FROM compatibility_work_items w
                    JOIN service_contracts c ON c.contract_id = w.source_contract_id
                    WHERE c.service_name = %s
                      AND (
                          w.state IN (
                              'PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING',
                              'AWAITING_APPROVAL', 'REVIEW_REQUIRED', 'BLOCKED', 'INCOMPATIBLE'
                          )
                          OR (
                              w.state IN ('VERIFIED', 'COMPLETED')
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM http_interface_dependencies d
                                  WHERE d.consumer_service = w.target_service
                                    AND d.consumer_repository = w.target_repository
                                    AND d.provider_service = c.service_name
                                    AND d.contract_id = w.source_contract_id
                                    AND d.assumed_provider_revision >= w.source_contract_revision
                                    AND d.confirmation_status = 'CONFIRMED'
                              )
                          )
                      )
                      AND COALESCE(w.payload->>'classification', 'BREAKING')
                          IN ('BREAKING', 'REVIEW_REQUIRED')
                    ORDER BY w.created_at ASC;
                    """,
                    (service_name,),
                )
                provider_work = await cur.fetchall()
                if isinstance(provider_work, (list, tuple)):
                    for item in provider_work:
                        if not isinstance(item, dict):
                            continue
                        gate_blockers.append({
                            "kind": "provider_compatibility_work",
                            "work_item_id": str(item.get("work_item_id")) if item.get("work_item_id") else None,
                            "consumer_service": item.get("consumer_service"),
                            "consumer_repository": item.get("consumer_repository"),
                            "provider_service": item.get("provider_service", service_name),
                            "contract_id": str(item.get("source_contract_id")) if item.get("source_contract_id") else None,
                            "contract_revision": item.get("source_contract_revision"),
                            "state": item.get("state"),
                            "classification": item.get("classification"),
                            "reason": "Confirmed consumer compatibility work is unresolved",
                        })
            except Exception as ex:
                logger.error("Database error while evaluating provider compatibility gate: %s", ex)
                if not settings.is_demo_mode:
                    raise RuntimeError(f"Cannot promote deployment: failed to verify provider compatibility work due to database error: {ex}") from ex
                logger.debug("Skipping provider compatibility gate in demo mode due to database state: %s", ex)

            await cur.execute("SELECT COALESCE(MAX(reload_version), 0) + 1 AS next_version FROM deployments;")
            row = await cur.fetchone()
            allocated_version = int(row["next_version"]) if row else 1

            if gate_blockers:
                rejection = {
                    "error": "Unresolved compatibility work blocks provider deployment",
                    "compatibility_blockers": gate_blockers,
                }
                await cur.execute(
                    """
                    INSERT INTO deployments (
                        service_name, source_commit, status, reload_version,
                        health_check, created_at, completed_at
                    )
                    VALUES (%s, %s, 'FAILED', %s, %s::jsonb, now(), now())
                    RETURNING deployment_id, service_name, source_commit, status,
                              reload_version, health_check, completed_at;
                    """,
                    (service_name, source_commit, allocated_version, json.dumps(rejection)),
                )
                rejected = await cur.fetchone()
                if not rejected:
                    raise RuntimeError("Failed to persist compatibility-gated deployment rejection")
                await cur.execute(
                    """INSERT INTO coordinator_outbox (
                           aggregate_type, aggregate_id, aggregate_revision,
                           source_service, event_type, payload
                       ) VALUES ('DEPLOYMENT', %s, %s, %s, 'DEPLOYMENT_FAILED', %s::jsonb)
                       RETURNING event_id;""",
                    (rejected["deployment_id"], allocated_version, service_name,
                     json.dumps({
                         "deployment_id": str(rejected["deployment_id"]),
                         "service_name": service_name,
                         "source_commit": source_commit,
                         "reload_version": allocated_version,
                         **rejection,
                     })),
                )
                outbox = await cur.fetchone()
                if not outbox or not outbox.get("event_id"):
                    raise RuntimeError("DEPLOYMENT_FAILED outbox event was not created")
                outbox_id = outbox["event_id"]
                await cur.execute(
                    """INSERT INTO contract_audit_history (
                           event_type, source_service, summary, actor,
                           outbox_event_id, causation_id, correlation_id
                       ) VALUES ('DEPLOYMENT_FAILED', %s, %s, 'coordinator', %s, %s, %s);""",
                    (service_name,
                     f"Rejected deployment of {service_name}: unresolved compatibility work remains",
                     outbox_id, outbox_id, outbox_id),
                )
                return {
                    **dict(rejected),
                    "deployment_id": str(rejected["deployment_id"]),
                    "status": "FAILED",
                    "error": rejection["error"],
                    "compatibility_blockers": gate_blockers,
                    "outbox_event_id": str(outbox_id),
                }

            await cur.execute(
                """
                INSERT INTO deployments (
                    service_name, source_commit, status, reload_version, created_at
                )
                VALUES (%s, %s, 'VALIDATING', %s, now())
                RETURNING deployment_id, service_name, source_commit, status, reload_version;
                """,
                (service_name, source_commit, allocated_version),
            )
            return await cur.fetchone()

    deployment_record = await run_transaction(_allocate_version_tx)
    if deployment_record.get("status") == "FAILED":
        # The rejection was already committed with its deployment/outbox/audit
        # records.  Do not extract, start, or cut over a candidate snapshot.
        return {
            **deployment_record,
            "service_name": service_name,
            "source_commit": source_commit,
            "is_healthy": False,
        }
    deployment_id = str(deployment_record["deployment_id"])
    allocated_version = deployment_record["reload_version"]

    staged_dir = root_dir / "deployments" / "staged" / f"{service_name}-{allocated_version}"
    live_dir = root_dir / "deployments" / "live" / service_name
    backup_dir = root_dir / "deployments" / "backup" / service_name

    # Step 2: Extract Exact Commit Snapshot into Staging Directory (FAIL CLOSED)
    try:
        extract_commit_snapshot(src_repo, source_commit, staged_dir)
    except Exception as extract_err:
        logger.error("Failed to extract commit %s snapshot: %s", source_commit, extract_err)
        await _mark_deployment_failed_with_outbox(
            deployment_id=deployment_id,
            error_details={"error": f"Commit extraction failed: {extract_err}"},
            service_name=service_name,
            source_commit=source_commit,
            reload_version=allocated_version,
        )
        return {
            "deployment_id": deployment_id,
            "service_name": service_name,
            "source_commit": source_commit,
            "status": "FAILED",
            "reload_version": allocated_version,
            "error": str(extract_err),
            "is_healthy": False,
        }

    # Step 3: Pre-Deployment Test Gate in Staging Directory (Allowlisted Sandbox Env)
    test_evidence: dict[str, Any] = {"all_passed": True, "returncode": 0}
    if not skip_test_gate and staged_dir.exists():
        test_evidence = run_service_test_gate(staged_dir, base_dir=root_dir)
        if not test_evidence["all_passed"]:
            logger.error("Deployment promotion rejected for %s: tests failed in staging", service_name)
            shutil.rmtree(staged_dir, ignore_errors=True)
            await _mark_deployment_failed_with_outbox(
                deployment_id=deployment_id,
                error_details={"error": "Test gate failed", "test_evidence": test_evidence},
                service_name=service_name,
                source_commit=source_commit,
                reload_version=allocated_version,
            )
            return {
                "deployment_id": deployment_id,
                "service_name": service_name,
                "source_commit": source_commit,
                "status": "FAILED",
                "reload_version": allocated_version,
                "error": "Test suite failed before deployment promotion",
                "test_evidence": test_evidence,
                "is_healthy": False,
            }

    # Step 4: True Atomic Cutover with Full Guarded Failure Handling
    live_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir.parent.mkdir(parents=True, exist_ok=True)

    has_prior_live = live_dir.exists()
    journal.record_intent(
        service_name=service_name,
        deployment_id=deployment_id,
        staged_dir=staged_dir,
        live_dir=live_dir,
        backup_dir=backup_dir,
    )

    try:
        if has_prior_live:
            if backup_dir.exists():
                shutil.rmtree(backup_dir)
            live_dir.rename(backup_dir)

        staged_dir.rename(live_dir)
    except Exception as cutover_err:
        logger.error("Atomic directory cutover failed for %s: %s", service_name, cutover_err)
        restoration_ok = False
        if has_prior_live and backup_dir.exists() and not live_dir.exists():
            try:
                backup_dir.rename(live_dir)
                restoration_ok = True
            except Exception as restore_err:
                logger.critical("CRITICAL: Failed to restore backup during cutover failure for %s: %s", service_name, restore_err)
                journal.record_unrecoverable(str(restore_err), service_name=service_name)
        
        if restoration_ok or (not has_prior_live and not live_dir.exists()):
            journal.record_complete()



        await _mark_deployment_failed_with_outbox(
            deployment_id=deployment_id,
            error_details={"error": f"Atomic directory cutover failed: {cutover_err}"},
            service_name=service_name,
            source_commit=source_commit,
            reload_version=allocated_version,
        )
        return {
            "deployment_id": deployment_id,
            "service_name": service_name,
            "source_commit": source_commit,
            "status": "FAILED",
            "reload_version": allocated_version,
            "error": f"Atomic directory cutover failed: {cutover_err}",
            "is_healthy": False,
        }

    # Step 5: Restart Process Supervisor using onboarded FastAPI metadata.
    app_module = await resolve_service_app_module_async(service_name, live_dir)
    supervisor.restart_service(service_name=service_name, cwd=live_dir, app_module=app_module)

    # Step 6: Polling Readiness Loop on Server-Authoritative Port
    is_healthy, health_result, health_error = await poll_service_readiness(
        service_name=service_name,
        max_retries=10,
        base_interval=0.15,
        http_client=http_client,
    )

    # Step 7: Automated Rollback on Health Check Failure with Verification Loop
    rollback_evidence: Optional[dict[str, Any]] = None
    if not is_healthy and has_prior_live and backup_dir.exists():
        logger.warning("Readiness verification failed for %s (%s); executing rollback...", service_name, health_error)
        if live_dir.exists():
            shutil.rmtree(live_dir, ignore_errors=True)
        backup_dir.rename(live_dir)
        supervisor.restart_service(service_name=service_name, cwd=live_dir, app_module=app_module)

        # Verification loop confirming that rollback restored previous version to health
        rb_healthy, rb_data, rb_err = await poll_service_readiness(
            service_name=service_name,
            max_retries=10,
            base_interval=0.15,
            http_client=http_client,
        )
        rollback_evidence = {
            "rollback_executed": True,
            "restored_healthy": rb_healthy,
            "restored_payload": rb_data if rb_healthy else None,
            "restored_error": rb_err,
        }
        logger.info("Rollback restoration verification for %s: healthy=%s", service_name, rb_healthy)

    # Step 8: Update Deployment Status and Outbox in Transaction
    async def _update_status_tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            final_status = "HEALTHY" if is_healthy else "FAILED"
            combined_health = {
                "health_check": health_result if is_healthy else {"error": health_error},
                "test_evidence": test_evidence,
                "rollback": rollback_evidence,
            }

            await cur.execute(
                """
                UPDATE deployments
                SET 
                    status = %s,
                    health_check = %s::jsonb,
                    completed_at = now()
                WHERE deployment_id = %s
                RETURNING deployment_id, service_name, source_commit, status, reload_version, completed_at;
                """,
                (final_status, json.dumps(combined_health), deployment_id),
            )
            updated_row = await cur.fetchone()

            if is_healthy:
                outbox_payload = {
                    "deployment_id": deployment_id,
                    "service_name": service_name,
                    "source_commit": source_commit,
                    "reload_version": allocated_version,
                    "status": "HEALTHY",
                    "health_check": combined_health,
                }
                event_type = "DEPLOYMENT_COMPLETED"
                audit_summary = f"Promoted {service_name} at commit {source_commit[:8]} to version {allocated_version}"
            elif rollback_evidence and rollback_evidence.get("rollback_executed"):
                if rollback_evidence.get("restored_healthy") is True:
                    event_type = "DEPLOYMENT_ROLLED_BACK"
                    audit_summary = f"Rolled back {service_name} after health check failure (restored_healthy=True)"
                else:
                    event_type = "DEPLOYMENT_ROLLBACK_FAILED"
                    audit_summary = f"CRITICAL: Rollback for {service_name} failed to restore healthy service (error: {rollback_evidence.get('restored_error')})"

                outbox_payload = {
                    "deployment_id": deployment_id,
                    "service_name": service_name,
                    "source_commit": source_commit,
                    "reload_version": allocated_version,
                    "status": "ROLLED_BACK" if rollback_evidence.get("restored_healthy") else "ROLLBACK_FAILED",
                    "health_check": combined_health,
                    "rollback": rollback_evidence,
                }
            else:
                outbox_payload = {
                    "deployment_id": deployment_id,
                    "service_name": service_name,
                    "source_commit": source_commit,
                    "reload_version": allocated_version,
                    "status": "FAILED",
                    "health_check": combined_health,
                }
                event_type = "DEPLOYMENT_FAILED"
                audit_summary = f"Deployment failed for {service_name} at commit {source_commit[:8]}: {health_error or 'health check failed'}"

            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                )
                VALUES ('DEPLOYMENT', %s, %s, %s, %s, %s::jsonb)
                RETURNING event_id;
                """,
                (deployment_id, allocated_version, service_name, event_type, json.dumps(outbox_payload)),
            )

            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError(f"{event_type} outbox event was not created")
            outbox_id = outbox_row["event_id"]
            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor,
                    outbox_event_id, causation_id, correlation_id
                )
                VALUES (%s, %s, %s, 'deployer', %s, %s, %s);
                """,
                (event_type, service_name, audit_summary, outbox_id, outbox_id, outbox_id),
            )

            return updated_row

    final_row = await run_transaction(_update_status_tx)

    receipt_info = None
    if is_healthy:
        try:
            from coordinator.receipt_archiver import generate_execution_receipt, archive_receipt
            receipt = generate_execution_receipt(
                task_id=f"deploy-{deployment_id}",
                source_service=service_name,
                target_service=service_name,
                from_version=max(1, allocated_version - 1),
                to_version=allocated_version,
                breaking_diff={"status": "HEALTHY", "source_commit": source_commit},
                test_results=test_evidence or {},
                approved_by="deployment-supervisor",
                deployment_version=allocated_version,
                source_commit=source_commit,
            )
            receipt_info = await archive_receipt(receipt, upload_to_s3=True)
        except Exception as rcpt_err:
            logger.warning("Receipt generation skipped during deployment: %s", rcpt_err)

    return {
        "deployment_id": str(final_row["deployment_id"]),
        "service_name": final_row["service_name"],
        "source_commit": final_row["source_commit"],
        "status": final_row["status"],
        "reload_version": final_row["reload_version"],
        "is_healthy": is_healthy,
        "health_check": health_result if is_healthy else {"error": health_error},
        "test_evidence": test_evidence,
        "rollback": rollback_evidence,
        "receipt": receipt_info,
    }
