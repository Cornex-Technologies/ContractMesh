"""CodeClaim Database Management, Connection Pool & Versioned Migrations Runner."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import sys
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, TypeVar

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

import psycopg
from psycopg.errors import SerializationFailure
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from coordinator.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")

_pool: Optional[AsyncConnectionPool] = None
_pool_lock = asyncio.Lock()


def get_connection_string() -> str:
    """Retrieve CockroachDB connection string from config or raise clear error."""
    conn_str = settings.cockroach_database_url
    if not conn_str:
        raise ValueError(
            "COCKROACH_DATABASE_URL environment variable is not configured. "
            "Please configure it in .env or environment."
        )
    return conn_str


async def get_pool() -> AsyncConnectionPool:
    """Get or initialize the singleton asynchronous connection pool."""
    global _pool
    if _pool is not None and not _pool.closed:
        return _pool

    async with _pool_lock:
        if _pool is not None and not _pool.closed:
            return _pool

        conn_str = get_connection_string()
        _pool = AsyncConnectionPool(
            conninfo=conn_str,
            min_size=1,
            max_size=20,
            open=False,
            timeout=5.0,
            kwargs={"row_factory": dict_row, "autocommit": False, "connect_timeout": 3},
        )
        await _pool.open()
        logger.info("CockroachDB async connection pool initialized.")
        return _pool


async def close_pool() -> None:
    """Gracefully close the connection pool."""
    global _pool
    async with _pool_lock:
        if _pool is not None:
            await _pool.close()
            _pool = None
            logger.info("CockroachDB async connection pool closed.")


async def execute_query(
    query: str,
    params: Optional[tuple[Any, ...] | dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Execute a read/write query and return matching rows as dictionaries, or empty list."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            try:
                records = await cur.fetchall()
                return [dict(r) for r in records]
            except (psycopg.ProgrammingError, psycopg.OperationalError):
                return []


# Alias for consistency with fetch_one
fetch_all = execute_query


async def fetch_one(
    query: str,
    params: Optional[tuple[Any, ...] | dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    """Execute a query and return a single row or None."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            try:
                record = await cur.fetchone()
                return dict(record) if record is not None else None
            except (psycopg.ProgrammingError, psycopg.OperationalError):
                return None


async def run_transaction(
    transaction_fn: Callable[[psycopg.AsyncConnection], Coroutine[Any, Any, T]],
    max_retries: int = 5,
    base_backoff: float = 0.05,
    max_backoff: float = 1.0,
) -> T:
    """Execute an async callback inside a serializable transaction with automatic retry on conflict (SQLSTATE 40001).
    
    CockroachDB uses SERIALIZABLE isolation by default. When concurrent transactions
    conflict, CockroachDB raises a SerializationFailure (40001). This helper wraps
    the entire transaction block in an exponential backoff retry loop with jitter.
    """
    pool = await get_pool()
    for attempt in range(1, max_retries + 1):
        try:
            async with pool.connection() as conn:
                async with conn.transaction():
                    result = await transaction_fn(conn)
                    return result
        except SerializationFailure as ex:
            if attempt == max_retries:
                logger.error("Transaction failed after %d retries due to serialization conflict: %s", max_retries, ex)
                raise
            backoff = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
            jitter = random.uniform(0, backoff * 0.5)
            sleep_duration = backoff + jitter
            logger.warning(
                "Serialization conflict (attempt %d/%d). Retrying in %.3fs: %s",
                attempt,
                max_retries,
                sleep_duration,
                ex,
            )
            await asyncio.sleep(sleep_duration)
        except Exception:
            raise


async def execute_statement(
    statement: str,
    params: Optional[tuple[Any, ...] | dict[str, Any]] = None,
) -> int:
    """Execute an INSERT/UPDATE/DELETE statement wrapped in the serializable retry transaction."""
    async def _tx_exec(conn: psycopg.AsyncConnection) -> int:
        async with conn.cursor() as cur:
            await cur.execute(statement, params)
            return cur.rowcount

    return await run_transaction(_tx_exec)


# ==============================================================================
# Versioned Database Migrations System
# ==============================================================================


async def get_applied_migrations() -> list[dict[str, Any]]:
    """Retrieve all previously applied database migrations."""
    init_migrations_table_sql = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version STRING PRIMARY KEY,
        name STRING NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        checksum STRING NOT NULL
    );
    """
    await execute_statement(init_migrations_table_sql)
    return await execute_query("SELECT version, name, applied_at, checksum FROM schema_migrations ORDER BY version ASC;")


async def migrate_db(migrations_dir: Optional[str | Path] = None) -> list[str]:
    """Execute all pending numbered SQL migrations sequentially in versioned transactions.
    
    Guarantees that database schema evolves safely in production without relying on raw bootstrap DDL.
    Validates SHA256 checksums to guarantee applied migration immutability.
    """
    if migrations_dir is None:
        migrations_dir = Path(__file__).parent / "migrations"
    else:
        migrations_dir = Path(migrations_dir)

    if not migrations_dir.exists() or not migrations_dir.is_dir():
        raise FileNotFoundError(
            f"Required migrations directory not found at '{migrations_dir}'. "
            "CodeClaim operates strictly via versioned migrations to prevent schema drift."
        )

    # 1. Ensure migrations ledger exists and load applied map
    applied_records = await get_applied_migrations()
    applied_map = {r["version"]: r["checksum"] for r in applied_records}

    # 2. Discover migration files
    migration_files = sorted(migrations_dir.glob("*.sql"), key=lambda p: p.name)
    applied_now: list[str] = []

    for sql_file in migration_files:
        version = sql_file.stem.split("_")[0]
        name = sql_file.stem

        sql_content = sql_file.read_text(encoding="utf-8")
        disk_checksum = hashlib.sha256(sql_content.encode("utf-8")).hexdigest()

        # Check for immutability violation on already-applied migration
        if version in applied_map:
            recorded_checksum = applied_map[version]
            if disk_checksum != recorded_checksum:
                raise ValueError(
                    f"Checksum mismatch for already-applied migration {version} ('{name}'): "
                    f"recorded {recorded_checksum} != disk {disk_checksum}. "
                    "Applied migration scripts are immutable and cannot be modified after execution."
                )
            continue

        logger.info("Applying migration %s (%s)...", version, name)

        async def _apply_migration_tx(conn: psycopg.AsyncConnection) -> None:
            async with conn.cursor() as cur:
                await cur.execute(sql_content)
                await cur.execute(
                    """
                    INSERT INTO schema_migrations (version, name, checksum)
                    VALUES (%s, %s, %s);
                    """,
                    (version, name, disk_checksum),
                )

        await run_transaction(_apply_migration_tx)
        applied_now.append(version)
        logger.info("Successfully applied migration %s.", version)

    return applied_now


async def init_bootstrap_schema(schema_path: Optional[str | Path] = None) -> None:
    """Apply the fallback schema.sql DDL to CockroachDB."""
    if schema_path is None:
        schema_path = Path(__file__).parent / "schema.sql"
    else:
        schema_path = Path(schema_path)

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_path}")

    sql_script = schema_path.read_text(encoding="utf-8")
    
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql_script)
            await conn.commit()
    logger.info("Successfully applied fallback bootstrap schema from %s", schema_path)


async def init_db(schema_path: Optional[str | Path] = None) -> None:
    """Initialize or evolve database schema by running versioned migrations."""
    migrations_dir = Path(__file__).parent / "migrations"
    if migrations_dir.exists():
        await migrate_db(migrations_dir)
    else:
        await init_bootstrap_schema(schema_path)


async def check_health(timeout_seconds: float = 3.0) -> dict[str, Any]:
    """Verify database connectivity and return cluster ping status with bounded timeout."""
    try:
        conn_str = get_connection_string()
        async with asyncio.timeout(timeout_seconds):
            async with await psycopg.AsyncConnection.connect(conn_str, row_factory=dict_row, connect_timeout=int(timeout_seconds)) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT version(), current_database(), now();")
                    row = await cur.fetchone()
                    return {
                        "status": "healthy",
                        "version": row.get("version") if row else "unknown",
                        "database": row.get("current_database") if row else "unknown",
                        "timestamp": str(row.get("now")) if row else None,
                    }
    except Exception as ex:
        return {
            "status": "unhealthy",
            "error": str(ex),
        }


if __name__ == "__main__":
    import asyncio
    print("Running database migrations...")
    asyncio.run(init_db())
    print("Database migrations complete.")
