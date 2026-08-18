"""Service Contract Registry, Immutability & AST-Based Provenance Engine.

Implements:
1. Zero-Execution AST-based Pydantic schema extraction (100% sandboxed, zero code execution)
   - Supports Annotated[T, ...], Field(alias=...), Enum classes, and default_factory
   - Falls back to checked-in OpenAPI / JSON Schema artifacts when present
2. Cryptographically bound commit-to-schema extraction from Git object store
3. Single-transaction atomic revision publication + outbox + audit ledger
4. Strict revision immutability (idempotent replay vs conflict error)
5. Automated semantic diff computation for subsequent revisions
"""

from __future__ import annotations

import ast
import json
import logging
from pathlib import Path
import subprocess
from typing import Any, Optional
import psycopg
from psycopg.rows import dict_row

from coordinator.db import run_transaction
from coordinator.differencer import compute_schema_diff

logger = logging.getLogger(__name__)

_SUPPORTED_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}


def _contract_key(service_name: str, endpoint_path: str, http_method: str) -> str:
    """Build and validate the stable identity used by the contract ledger."""
    service_name = service_name.strip()
    endpoint_path = endpoint_path.strip()
    http_method = http_method.strip().upper()
    if not service_name:
        raise ValueError("service_name is required")
    if not endpoint_path.startswith("/"):
        raise ValueError("endpoint_path must start with '/'")
    if http_method not in _SUPPORTED_HTTP_METHODS:
        raise ValueError(f"http_method must be one of {sorted(_SUPPORTED_HTTP_METHODS)}")
    return f"{service_name}:{http_method}:{endpoint_path}"


def _canonicalize_schema(value: Any) -> Any:
    """Normalize JSON-compatible contract data without dropping schema structure.

    ``$defs`` and ``$ref`` are contract structure, not presentation metadata.  They
    must remain in the canonical form so a change inside a referenced Pydantic schema
    cannot be mistaken for an idempotent replay of an immutable revision.
    """
    if isinstance(value, dict):
        return {
            key: _canonicalize_schema(item)
            for key, item in sorted(value.items())
            if key not in {"title", "description"}
        }
    if isinstance(value, list):
        return [_canonicalize_schema(item) for item in value]
    return value


def _canonical_schema_json(value: Any) -> str:
    return json.dumps(_canonicalize_schema(value), sort_keys=True, separators=(",", ":"))


# ==============================================================================
# 1. Zero-Execution AST-Based Schema Extractor (Constrained Sandbox)
# ==============================================================================


def _extract_enums_from_ast(tree: ast.AST) -> dict[str, list[str]]:
    """Discover Enum classes defined in AST and map their allowed choice string/int values."""
    enums: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            is_enum = any(
                (isinstance(base, ast.Name) and "Enum" in base.id) or
                (isinstance(base, ast.Attribute) and base.attr == "Enum")
                for base in node.bases
            )
            if is_enum:
                values = []
                for stmt in node.body:
                    if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Constant):
                        values.append(str(stmt.value.value))
                    elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.value, ast.Constant):
                        values.append(str(stmt.value.value))
                if values:
                    enums[node.name] = values
    return enums


def _parse_ast_type_annotation(
    node: Optional[ast.AST],
    known_enums: Optional[dict[str, list[str]]] = None,
) -> tuple[dict[str, Any] | str, dict[str, Any]]:
    """Safely convert Python AST type annotations into JSON Schema type representations.
    
    Returns (type_repr, extra_field_constraints_from_annotated).
    """
    if node is None:
        return "string", {}

    extra_constraints: dict[str, Any] = {}
    known_enums = known_enums or {}

    if isinstance(node, ast.Name):
        if node.id in known_enums:
            return {"type": "string", "enum": known_enums[node.id]}, {}

        mapping = {
            "int": "integer",
            "str": "string",
            "float": "number",
            "bool": "boolean",
            "dict": "object",
            "list": "array",
            "Any": "string",
        }
        return mapping.get(node.id, "string"), {}

    elif isinstance(node, ast.Subscript):
        outer_name = getattr(node.value, "id", "")

        # Handle Annotated[T, Field(...), ...]
        if outer_name == "Annotated":
            if isinstance(node.slice, ast.Tuple) and node.slice.elts:
                base_type_node = node.slice.elts[0]
                base_repr, _ = _parse_ast_type_annotation(base_type_node, known_enums)
                
                # Check for metadata constraints in Annotated[T, Field(...)]
                for meta in node.slice.elts[1:]:
                    if isinstance(meta, ast.Call) and getattr(meta.func, "id", "") == "Field":
                        for kw in meta.keywords:
                            if kw.arg in ("gt", "lt", "ge", "le", "min_length", "max_length", "description", "alias") and isinstance(kw.value, ast.Constant):
                                extra_constraints[kw.arg] = kw.value.value
                return base_repr, extra_constraints
            else:
                return "string", {}

        if outer_name in ("Optional", "Union"):
            if outer_name == "Optional":
                inner, _ = _parse_ast_type_annotation(node.slice, known_enums)
                inner_dict = inner if isinstance(inner, dict) else {"type": inner}
                return {"anyOf": [inner_dict, {"type": "null"}]}, {}
            elif isinstance(node.slice, ast.Tuple):
                elements = [_parse_ast_type_annotation(elem, known_enums)[0] for elem in node.slice.elts]
                elem_dicts = [e if isinstance(e, dict) else {"type": e} for e in elements]
                return {"anyOf": elem_dicts}, {}

        elif outer_name in ("List", "list"):
            inner, _ = _parse_ast_type_annotation(node.slice, known_enums)
            inner_dict = inner if isinstance(inner, dict) else {"type": inner}
            return {"type": "array", "items": inner_dict}, {}

        elif outer_name in ("Dict", "dict"):
            return {"type": "object"}, {}

    elif isinstance(node, ast.Constant):
        if node.value is None:
            return {"type": "null"}, {}
        return "string", {}

    return "string", {}


def extract_pydantic_schema_from_source(source_code: str, model_name: str) -> dict[str, Any]:
    """Parse Python source code statically via AST to produce a standard JSON Schema without executing code.
    
    Guarantees zero remote-code-execution risk when analyzing agent-generated repository code.
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        raise ValueError(f"Failed to parse source code AST: {e}") from e

    known_enums = _extract_enums_from_ast(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == model_name:
            properties: dict[str, Any] = {}
            required: list[str] = []

            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    field_name = stmt.target.id
                    type_info, annotated_constraints = _parse_ast_type_annotation(stmt.annotation, known_enums)
                    field_def: dict[str, Any] = (
                        dict(type_info) if isinstance(type_info, dict) else {"type": type_info}
                    )
                    field_def.update(annotated_constraints)

                    is_required = True

                    if stmt.value is not None:
                        # Field(...) definition or direct default assignment
                        if isinstance(stmt.value, ast.Call) and getattr(stmt.value.func, "id", "") == "Field":
                            # Check positional arguments: Field(..., ...) or Field(default_val)
                            if stmt.value.args:
                                arg0 = stmt.value.args[0]
                                if isinstance(arg0, ast.Constant) and arg0.value == Ellipsis:
                                    is_required = True
                                elif isinstance(arg0, ast.Constant):
                                    field_def["default"] = arg0.value
                                    is_required = False

                            # Check keyword arguments (description, default, alias, default_factory, etc.)
                            for kw in stmt.value.keywords:
                                if kw.arg == "description" and isinstance(kw.value, ast.Constant):
                                    field_def["description"] = str(kw.value.value)
                                elif kw.arg == "alias" and isinstance(kw.value, ast.Constant):
                                    field_def["alias"] = str(kw.value.value)
                                elif kw.arg == "default" and isinstance(kw.value, ast.Constant):
                                    field_def["default"] = kw.value.value
                                    if kw.value.value is not Ellipsis:
                                        is_required = False
                                elif kw.arg == "default_factory":
                                    is_required = False
                                    field_def["default_factory"] = True
                                elif kw.arg in ("gt", "lt", "ge", "le", "min_length", "max_length") and isinstance(kw.value, ast.Constant):
                                    field_def[kw.arg] = kw.value.value
                        elif isinstance(stmt.value, ast.Constant):
                            field_def["default"] = stmt.value.value
                            is_required = False
                    else:
                        # No assignment value provided
                        if "anyOf" in field_def and any(item.get("type") == "null" for item in field_def.get("anyOf", [])):
                            is_required = False
                        else:
                            is_required = True

                    properties[field_name] = field_def
                    if is_required:
                        required.append(field_name)

            docstring = ast.get_docstring(node) or ""
            return {
                "title": model_name,
                "description": docstring.strip(),
                "type": "object",
                "properties": properties,
                "required": required,
            }

    raise AttributeError(f"Model class '{model_name}' not found in source code AST.")


# ==============================================================================
# 2. Git Commit-Bound Schema Extraction
# ==============================================================================


def extract_git_commit_sha(repo_path: str | Path) -> str:
    """Extract current HEAD Git commit SHA cleanly without modifying working tree."""
    path = Path(repo_path)
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(path),
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return res.stdout.strip()
    except Exception as e:
        logger.warning("Git CLI rev-parse failed in %s (%s); reading .git/HEAD directly", path, e)
        try:
            head_file = path / ".git" / "HEAD"
            if head_file.exists():
                head_content = head_file.read_text(encoding="utf-8").strip()
                if head_content.startswith("ref: "):
                    ref_path = path / ".git" / head_content[4:].strip()
                    if ref_path.exists():
                        return ref_path.read_text(encoding="utf-8").strip()
                elif len(head_content) == 40:
                    return head_content
        except Exception:
            pass

    return "commit-untracked-dev"


# Backward compatibility alias
get_service_git_commit = extract_git_commit_sha


def extract_pydantic_schema_from_git_commit(
    repo_path: str | Path,
    source_commit: str,
    module_filename: str,
    model_name: str,
) -> dict[str, Any]:
    """Extract a Pydantic model's JSON Schema from a specific Git commit SHA/ref using `git show <commit>:<file>` and static AST."""
    path = Path(repo_path)
    
    # Check if a checked-in JSON Schema artifact exists at that commit (e.g. schemas_v1.json)
    json_artifact_name = Path(module_filename).with_suffix(".json").name
    try:
        res_json = subprocess.run(
            ["git", "show", f"{source_commit}:{json_artifact_name}"],
            cwd=str(path),
            capture_output=True,
            text=True,
            check=True,
            timeout=3,
        )
        if res_json.stdout:
            data = json.loads(res_json.stdout)
            if model_name in data.get("$defs", {}):
                return data["$defs"][model_name]
            elif data.get("title") == model_name:
                return data
    except Exception:
        pass

    try:
        res = subprocess.run(
            ["git", "show", f"{source_commit}:{module_filename}"],
            cwd=str(path),
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        file_content = res.stdout
    except subprocess.CalledProcessError as e:
        raise FileNotFoundError(
            f"Module '{module_filename}' not found at git commit '{source_commit}' in {path}: {e.stderr}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Failed to extract '{module_filename}' from commit '{source_commit}': {e}") from e

    return extract_pydantic_schema_from_source(file_content, model_name)


def extract_pydantic_schema_from_repo(
    repo_path: str | Path,
    module_filename: str,
    model_name: str,
    source_commit: Optional[str] = None,
) -> dict[str, Any]:
    """Statically parse a Pydantic model from a service repository and return its JSON Schema via AST without executing code."""
    if source_commit and source_commit not in ("commit-unspecified", "commit-untracked-dev"):
        return extract_pydantic_schema_from_git_commit(repo_path, source_commit, module_filename, model_name)

    path = Path(repo_path) / module_filename
    if not path.exists():
        raise FileNotFoundError(f"Schema module not found at {path}")

    # Check for checked-in JSON artifact alongside Python file
    json_path = path.with_suffix(".json")
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            if model_name in data.get("$defs", {}):
                return data["$defs"][model_name]
            elif data.get("title") == model_name:
                return data
        except Exception:
            pass

    source_code = path.read_text(encoding="utf-8")
    return extract_pydantic_schema_from_source(source_code, model_name)


# ==============================================================================
# 3. High-Level Commit-Bound Atomic Publication Entrypoint
# ==============================================================================


async def publish_contract_from_commit(
    repo_path: str | Path,
    source_commit: str,
    module_filename: str,
    model_name: str,
    service_name: str,
    endpoint_path: str,
    http_method: str,
    revision_number: int,
    semantic_summary: str,
    published_by: str,
    summary_embedding: Optional[list[float]] = None,
    schema_diff: Optional[dict[str, Any]] = None,
    publisher_compatibility: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Derive schema directly from Git commit object store via AST and atomically publish revision."""
    extracted_schema = extract_pydantic_schema_from_git_commit(
        repo_path=repo_path,
        source_commit=source_commit,
        module_filename=module_filename,
        model_name=model_name,
    )

    return await publish_contract_revision(
        service_name=service_name,
        endpoint_path=endpoint_path,
        http_method=http_method,
        revision_number=revision_number,
        schema_json=extracted_schema,
        semantic_summary=semantic_summary,
        published_by=published_by,
        source_commit=source_commit,
        summary_embedding=summary_embedding,
        schema_diff=schema_diff,
        publisher_compatibility=publisher_compatibility,
    )


# ==============================================================================
# 4. Atomic Database Revision Ledger & Outbox Transaction
# ==============================================================================


async def publish_contract_revision(
    service_name: str,
    endpoint_path: str,
    http_method: str,
    revision_number: int,
    schema_json: dict[str, Any],
    semantic_summary: str,
    published_by: str,
    source_commit: Optional[str] = None,
    summary_embedding: Optional[list[float]] = None,
    schema_diff: Optional[dict[str, Any]] = None,
    publisher_compatibility: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Atomically publish a contract revision, outbox event, and audit record in a single transaction.
    
    Enforces strict revision immutability:
    - If the revision exists with identical schema and commit, returns existing record as an idempotent no-op.
    - If the revision exists with conflicting schema or commit, raises ValueError to prevent mutable drift.
    """
    contract_key = _contract_key(service_name, endpoint_path, http_method)
    
    if source_commit is None:
        source_commit = "commit-unspecified"

    vector_sql_val = None
    if summary_embedding is not None:
        vector_sql_val = "[" + ",".join(str(x) for x in summary_embedding) + "]"

    async def _atomic_publish_tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        nonlocal schema_diff
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Ensure stable contract identity exists
            await cur.execute(
                """
                INSERT INTO service_contracts (service_name, endpoint_path, http_method, contract_key)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (contract_key) DO UPDATE SET service_name = EXCLUDED.service_name
                RETURNING contract_id, lifecycle_state;
                """,
                (service_name, endpoint_path, http_method.upper(), contract_key),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError("Failed to resolve contract_id")
            if row.get("lifecycle_state") == "RETIRED":
                raise ValueError(
                    f"Contract '{contract_key}' is retired. Publish a new endpoint identity or "
                    "add an explicit restoration workflow; a tombstone may not be silently revived."
                )
            contract_id = row["contract_id"]

            # 2. Check if revision already exists (Enforce strict revision immutability)
            await cur.execute(
                """
                SELECT contract_revision_id, source_commit, schema_json, semantic_summary, is_active
                FROM service_contract_revisions
                WHERE contract_id = %s AND revision_number = %s;
                """,
                (contract_id, revision_number),
            )
            existing_rev = await cur.fetchone()
            if existing_rev:
                existing_schema = existing_rev.get("schema_json") or {}

                schema_matches = _canonical_schema_json(existing_schema) == _canonical_schema_json(schema_json)
                commit_matches = (existing_rev.get("source_commit") == source_commit or not source_commit)

                if schema_matches and commit_matches:
                    return {
                        "contract_id": str(contract_id),
                        "contract_revision_id": str(existing_rev["contract_revision_id"]),
                        "outbox_event_id": None,
                        "history_id": None,
                        "service_name": service_name,
                        "revision_number": revision_number,
                        "source_commit": source_commit,
                        "schema_diff": schema_diff,
                        "is_idempotent_noop": True,
                    }
                else:
                    raise ValueError(
                        f"Contract revision {revision_number} for '{contract_key}' is immutable and already exists "
                        f"with a different schema/commit (existing_commit='{existing_rev.get('source_commit')}', "
                        f"new_commit='{source_commit}'). Historical revisions cannot be overwritten."
                    )

            # 3. The coordinator, rather than a caller, determines compatibility for
            #    every revision with a predecessor.  ``schema_diff`` remains an
            #    accepted legacy argument for initial-import metadata, but must never
            #    bypass deterministic comparison or downgrade a breaking result.
            if revision_number > 1:
                await cur.execute(
                    """
                    SELECT schema_json
                    FROM service_contract_revisions
                    WHERE contract_id = %s AND revision_number = %s;
                    """,
                    (contract_id, revision_number - 1),
                )
                prev_row = await cur.fetchone()
                if prev_row and prev_row.get("schema_json"):
                    prev_s = prev_row["schema_json"]
                    if isinstance(prev_s, str):
                        try:
                            prev_s = json.loads(prev_s)
                        except Exception:
                            pass
                    diff_res = compute_schema_diff(
                        old_schema=prev_s,
                        new_schema=schema_json,
                        publisher_compatibility=publisher_compatibility,
                        old_revision=revision_number - 1,
                        new_revision=revision_number,
                    )
                    computed_schema_diff = diff_res.to_dict() if hasattr(diff_res, "to_dict") else dict(diff_res)
                    if schema_diff:
                        # Preserve non-authoritative provenance for forensic context
                        # without allowing legacy callers to control classification.
                        computed_schema_diff["legacy_caller_diff"] = dict(schema_diff)
                    schema_diff = computed_schema_diff
                else:
                    schema_diff = {
                        "is_breaking": True,
                        "breaking_changes": [],
                        "classification": "REVIEW_REQUIRED",
                        "review_reasons": ["Previous contract revision was unavailable for comparison"],
                        "old_revision": revision_number - 1,
                        "new_revision": revision_number,
                        "publisher_compatibility": publisher_compatibility,
                        "migration_notes": (publisher_compatibility or {}).get("migration_notes"),
                        "consumer_impact": (publisher_compatibility or {}).get("consumer_impact"),
                    }

            # Every persisted publication carries revision provenance, even for v1 or caller-supplied diffs.
            if schema_diff is None:
                schema_diff = {
                    "is_breaking": False,
                    "breaking_changes": [],
                    "classification": "NON_BREAKING",
                    "review_reasons": [],
                    "old_revision": None,
                    "new_revision": revision_number,
                    "publisher_compatibility": publisher_compatibility,
                    "migration_notes": (publisher_compatibility or {}).get("migration_notes"),
                    "consumer_impact": (publisher_compatibility or {}).get("consumer_impact"),
                    "diff_summary": "Initial contract revision",
                }
            else:
                schema_diff = dict(schema_diff)
                schema_diff.setdefault("old_revision", revision_number - 1 if revision_number > 1 else None)
                schema_diff.setdefault("new_revision", revision_number)
                schema_diff.setdefault("publisher_compatibility", publisher_compatibility)
                schema_diff.setdefault("migration_notes", (publisher_compatibility or {}).get("migration_notes"))
                schema_diff.setdefault("consumer_impact", (publisher_compatibility or {}).get("consumer_impact"))

            # 4. Deactivate prior revisions
            await cur.execute(
                """
                UPDATE service_contract_revisions
                SET is_active = false
                WHERE contract_id = %s AND revision_number < %s;
                """,
                (contract_id, revision_number),
            )

            # 5. Insert new immutable revision
            if vector_sql_val:
                await cur.execute(
                    """
                    INSERT INTO service_contract_revisions (
                        contract_id, revision_number, source_commit, schema_json,
                        semantic_summary, summary_embedding, embedding_model,
                        embedding_dimension, is_active, published_by
                    )
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s::vector(1536), %s, %s, true, %s)
                    RETURNING contract_revision_id;
                    """,
                    (
                        contract_id,
                        revision_number,
                        source_commit,
                        json.dumps(schema_json),
                        semantic_summary,
                        vector_sql_val,
                        "amazon.titan-embed-text-v1",
                        1536,
                        published_by,
                    ),
                )
            else:
                await cur.execute(
                    """
                    INSERT INTO service_contract_revisions (
                        contract_id, revision_number, source_commit, schema_json,
                        semantic_summary, is_active, published_by
                    )
                    VALUES (%s, %s, %s, %s::jsonb, %s, true, %s)
                    RETURNING contract_revision_id;
                    """,
                    (
                        contract_id,
                        revision_number,
                        source_commit,
                        json.dumps(schema_json),
                        semantic_summary,
                        published_by,
                    ),
                )

            rev_row = await cur.fetchone()
            if not rev_row:
                raise RuntimeError("Failed to insert contract revision")
            contract_revision_id = rev_row["contract_revision_id"]

            # 6. Insert transactional outbox event (CDC changefeed spine)
            outbox_payload = {
                "contract_id": str(contract_id),
                "contract_revision_id": str(contract_revision_id),
                "service_name": service_name,
                "endpoint_path": endpoint_path,
                "http_method": http_method.upper(),
                "revision_number": revision_number,
                "source_commit": source_commit,
                "published_by": published_by,
                "schema_diff": schema_diff,
            }

            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING event_id;
                """,
                (
                    "SERVICE_CONTRACT",
                    contract_id,
                    revision_number,
                    service_name,
                    "CONTRACT_CHANGED",
                    json.dumps(outbox_payload),
                ),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("CONTRACT_CHANGED outbox event was not created")
            outbox_event_id = outbox_row["event_id"]

            # 7. Insert audit history record with causal lineage
            audit_summary = (
                f"Published contract revision {revision_number} for {contract_key} "
                f"(commit {source_commit[:8] if source_commit else 'untracked'})"
            )
            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, schema_diff, actor,
                    outbox_event_id, causation_id, correlation_id
                )
                VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                RETURNING history_id;
                """,
                (
                    "CONTRACT_PUBLISHED",
                    service_name,
                    audit_summary,
                    json.dumps(schema_diff) if schema_diff else None,
                    published_by,
                    outbox_event_id,
                    outbox_event_id,
                    outbox_event_id,
                ),
            )
            audit_row = await cur.fetchone()
            history_id = audit_row["history_id"] if audit_row else None

            # 8. Create compatibility work atomically in the same serializable transaction
            from coordinator.compatibility import create_compatibility_work_for_contract_change
            compat_work = await create_compatibility_work_for_contract_change(
                conn,
                source_event_id=outbox_event_id,
                contract_id=contract_id,
                source_service=service_name,
                revision_number=revision_number,
                schema_diff=schema_diff,
            )

            return {
                "contract_id": str(contract_id),
                "contract_revision_id": str(contract_revision_id),
                "outbox_event_id": str(outbox_event_id) if outbox_event_id else None,
                "history_id": str(history_id) if history_id else None,
                "compatibility_work_created": len(compat_work),
                "service_name": service_name,
                "revision_number": revision_number,
                "source_commit": source_commit,
                "schema_diff": schema_diff,
                "is_idempotent_noop": False,
            }

    return await run_transaction(_atomic_publish_tx)


# ==============================================================================
# 5. Explicit Endpoint Retirement and Fail-Closed Inventory Reconciliation
# ============================================================================== 


async def retire_contract(
    *, service_name: str, endpoint_path: str, http_method: str, source_commit: str,
    migration_note: str, retired_by: str, replacement_contract_key: Optional[str] = None,
) -> dict[str, Any]:
    """Append an immutable tombstone revision and notify every known consumer.

    Endpoint absence is never treated as a harmless deletion. A retirement is an explicit,
    auditable breaking event that advances the contract revision and enters the normal drift path.
    """
    contract_key = _contract_key(service_name, endpoint_path, http_method)
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""SELECT contract_id, lifecycle_state FROM service_contracts
                                 WHERE contract_key=%s FOR UPDATE;""", (contract_key,))
            contract = await cur.fetchone()
            if not contract:
                raise ValueError(f"Cannot retire unknown contract '{contract_key}'")
            if contract.get("lifecycle_state") == "RETIRED":
                raise ValueError(f"Contract '{contract_key}' is already retired")
            contract_id = contract["contract_id"]
            replacement_id = None
            if replacement_contract_key:
                await cur.execute("SELECT contract_id, lifecycle_state FROM service_contracts WHERE contract_key=%s;", (replacement_contract_key,))
                replacement = await cur.fetchone()
                if not replacement:
                    raise ValueError("replacement_contract_key does not identify a known contract")
                if replacement["contract_id"] == contract_id:
                    raise ValueError("replacement_contract_key cannot refer to the retired contract")
                if replacement.get("lifecycle_state") != "ACTIVE":
                    raise ValueError("replacement_contract_key must identify an active contract")
                replacement_id = replacement["contract_id"]
            await cur.execute("SELECT COALESCE(MAX(revision_number), 0) AS revision FROM service_contract_revisions WHERE contract_id=%s;", (contract_id,))
            revision_row = await cur.fetchone()
            retirement_revision = int(revision_row["revision"]) + 1
            tombstone_schema = {"type": "object", "x-codeclaim-retired": True, "x-codeclaim-migration-note": migration_note}
            diff = {
                "is_breaking": True, "classification": "BREAKING",
                "breaking_changes": [{"field": "<endpoint>", "change": "endpoint removed"}],
                "diff_summary": f"Endpoint {http_method.upper()} {endpoint_path} retired. {migration_note}",
            }
            await cur.execute("UPDATE service_contract_revisions SET is_active=false WHERE contract_id=%s;", (contract_id,))
            await cur.execute("""INSERT INTO service_contract_revisions (
                    contract_id, revision_number, source_commit, schema_json, semantic_summary, is_active, published_by
                ) VALUES (%s, %s, %s, %s::jsonb, %s, false, %s) RETURNING contract_revision_id;""",
                (contract_id, retirement_revision, source_commit, json.dumps(tombstone_schema),
                 f"RETIRED: {migration_note}", retired_by))
            tombstone = await cur.fetchone()
            await cur.execute("""UPDATE service_contracts SET lifecycle_state='RETIRED', retired_at=now(),
                    retired_by=%s, retirement_reason=%s, replacement_contract_id=%s WHERE contract_id=%s;""",
                (retired_by, migration_note, replacement_id, contract_id))
            await cur.execute("""INSERT INTO contract_retirements (
                    contract_id, retirement_revision, source_commit, migration_note, replacement_contract_id, retired_by
                ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING retirement_id;""",
                (contract_id, retirement_revision, source_commit, migration_note, replacement_id, retired_by))
            retirement = await cur.fetchone()
            payload = {
                "contract_id": str(contract_id), "contract_revision_id": str(tombstone["contract_revision_id"]),
                "service_name": service_name, "endpoint_path": endpoint_path, "http_method": http_method.upper(),
                "revision_number": retirement_revision, "source_commit": source_commit,
                "migration_note": migration_note, "replacement_contract_key": replacement_contract_key,
                "schema_diff": diff,
            }
            await cur.execute("""INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                ) VALUES ('SERVICE_CONTRACT', %s, %s, %s, 'ENDPOINT_RETIRED', %s::jsonb) RETURNING event_id;""",
                (contract_id, retirement_revision, service_name, json.dumps(payload)))
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("ENDPOINT_RETIRED outbox event was not created")
            outbox_id = str(outbox["event_id"])
            await cur.execute("""INSERT INTO contract_audit_history (event_type, source_service, summary, schema_diff, actor, outbox_event_id, causation_id, correlation_id)
                VALUES ('ENDPOINT_RETIRED', %s, %s, %s::jsonb, %s, %s, %s, %s);""",
                (service_name, f"Retired {contract_key}: {migration_note}", json.dumps(diff), retired_by, outbox_id, outbox_id, outbox_id))
            
            # Atomically create compatibility work for active consumers in this transaction
            from coordinator.compatibility import create_compatibility_work_for_contract_change
            await create_compatibility_work_for_contract_change(
                conn,
                source_event_id=outbox_id,
                contract_id=contract_id,
                source_service=service_name,
                revision_number=retirement_revision,
                schema_diff=diff,
            )

            return {"contract_id": str(contract_id), "retirement_id": str(retirement["retirement_id"]),
                    "retirement_revision": retirement_revision, "outbox_event_id": str(outbox["event_id"])}
    return await run_transaction(_tx)


async def publish_contract_inventory(
    *, service_name: str, source_commit: str, contracts: list[dict[str, str]], published_by: str
) -> dict[str, Any]:
    """Fail closed when a previously active endpoint disappears without a tombstone."""
    keys = {
        _contract_key(service_name, item["endpoint_path"], item["http_method"])
        for item in contracts
    }
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""SELECT inventory_id, contract_keys
                FROM contract_inventory_publications
                WHERE service_name=%s AND source_commit=%s FOR UPDATE;""", (service_name, source_commit))
            existing_inventory = await cur.fetchone()
            if existing_inventory:
                previous_keys = existing_inventory["contract_keys"]
                if isinstance(previous_keys, str):
                    previous_keys = json.loads(previous_keys)
                if set(previous_keys) != keys:
                    raise ValueError("Contract inventory for a source_commit is immutable and differs from the existing publication")
                return {
                    "inventory_id": str(existing_inventory["inventory_id"]),
                    "missing_active_contracts": [],
                    "is_idempotent_noop": True,
                }
            await cur.execute("""INSERT INTO contract_inventory_publications (service_name, source_commit, contract_keys, published_by)
                VALUES (%s, %s, %s::jsonb, %s) RETURNING inventory_id;""",
                (service_name, source_commit, json.dumps(sorted(keys)), published_by))
            inventory = await cur.fetchone()
            await cur.execute("""SELECT c.contract_id, c.contract_key,
                    COALESCE(MAX(r.revision_number), 1) AS revision
                FROM service_contracts c LEFT JOIN service_contract_revisions r ON r.contract_id=c.contract_id
                WHERE c.service_name=%s AND c.lifecycle_state='ACTIVE'
                GROUP BY c.contract_id, c.contract_key;""", (service_name,))
            active = await cur.fetchall()
            findings = []
            for contract in active:
                if contract["contract_key"] in keys:
                    continue
                diff = {"is_breaking": False, "classification": "REVIEW_REQUIRED",
                        "review_reasons": ["Previously active endpoint missing from published contract inventory"],
                        "diff_summary": f"Active endpoint {contract['contract_key']} is absent from inventory at {source_commit}"}
                await cur.execute("""INSERT INTO contract_inventory_findings (inventory_id, contract_id)
                    VALUES (%s, %s) ON CONFLICT (inventory_id, contract_id) DO NOTHING RETURNING finding_id;""",
                    (inventory["inventory_id"], contract["contract_id"]))
                finding = await cur.fetchone()
                if not finding:
                    continue
                payload = {"contract_id": str(contract["contract_id"]), "service_name": service_name,
                           "revision_number": int(contract["revision"]), "source_commit": source_commit,
                           "schema_diff": diff, "inventory_id": str(inventory["inventory_id"])}
                await cur.execute("""INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                    ) VALUES ('SERVICE_CONTRACT', %s, %s, %s, 'ENDPOINT_RETIREMENT_REVIEW_REQUIRED', %s::jsonb)
                    RETURNING event_id;""", (contract["contract_id"], contract["revision"], service_name, json.dumps(payload)))
                outbox = await cur.fetchone()
                if not outbox or not outbox.get("event_id"):
                    raise RuntimeError("ENDPOINT_RETIREMENT_REVIEW_REQUIRED outbox event was not created")
                outbox_id = str(outbox["event_id"])
                await cur.execute("""INSERT INTO contract_audit_history (event_type, source_service, summary, schema_diff, actor, outbox_event_id, causation_id, correlation_id)
                    VALUES ('ENDPOINT_RETIREMENT_REVIEW_REQUIRED', %s, %s, %s::jsonb, %s, %s, %s, %s);""",
                    (service_name, diff["diff_summary"], json.dumps(diff), published_by, outbox_id, outbox_id, outbox_id))
                from coordinator.compatibility import create_compatibility_work_for_contract_change
                await create_compatibility_work_for_contract_change(
                    conn,
                    source_event_id=outbox_id,
                    contract_id=contract["contract_id"],
                    source_service=service_name,
                    revision_number=int(contract["revision"]),
                    schema_diff=diff,
                )
                findings.append({"finding_id": str(finding["finding_id"]), "contract_id": str(contract["contract_id"]), "outbox_event_id": str(outbox["event_id"])})
            return {"inventory_id": str(inventory["inventory_id"]), "missing_active_contracts": findings,
                    "is_idempotent_noop": False}
    return await run_transaction(_tx)
