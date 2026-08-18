"""Deterministic FastAPI-only OpenAPI extraction for ``codeclaim onboard``."""

from __future__ import annotations

import ast
import copy
import importlib
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI

HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")


@dataclass(frozen=True)
class OnboardingPlan:
    service_name: str
    repository_path: Path
    endpoint_code_dir: Path
    app_entry: str | None
    openapi_url: str | None
    contracts: list[dict[str, Any]]
    review_findings: list[str]


def load_openapi_from_entry(repository_path: Path, app_entry: str) -> dict[str, Any]:
    """Import a FastAPI application object and retrieve its generated OpenAPI schema via a sanitized subprocess."""
    if ":" not in app_entry:
        raise ValueError("FastAPI app entry must use module:attribute form, for example app.main:app")
    module_name, attribute = app_entry.split(":", 1)
    if not module_name or not attribute:
        raise ValueError("FastAPI app entry must use module:attribute form")
    repo_dir = str(repository_path.resolve())
    site_packages = [p for p in sys.path if "site-packages" in p or "dist-packages" in p]
    isolated_pythonpath = os.pathsep.join([repo_dir] + site_packages)

    from coordinator.deployer import get_sanitized_sandbox_env
    sanitized_env = get_sanitized_sandbox_env(
        extra_env={"PYTHONPATH": isolated_pythonpath},
        base_dir=repository_path.parent.parent if repository_path.parent.name == "repos" else repository_path,
    )

    script = f"""
import sys, json, importlib
from pathlib import Path

try:
    module = importlib.import_module({repr(module_name)})
    app = getattr(module, {repr(attribute)}, None)
    from fastapi import FastAPI
    if not isinstance(app, FastAPI):
        sys.stderr.write("ERROR: Configured entry point is not a FastAPI application\\n")
        sys.exit(2)
    schema = app.openapi()
    sys.stdout.write(json.dumps(schema))
    sys.stdout.flush()
except Exception as ex:
    sys.stderr.write(f"ERROR: {{type(ex).__name__}}: {{ex}}\\n")
    sys.exit(1)
"""
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_dir,
            env=sanitized_env,
            capture_output=True,
            text=True,
            timeout=15.0,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"OpenAPI extraction subprocess timed out after 15 seconds for {app_entry}") from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to spawn OpenAPI extraction subprocess: {exc}") from exc

    if proc.returncode != 0:
        err_msg = proc.stderr.strip() or f"Process exited with code {proc.returncode}"
        raise ValueError(f"Failed to extract OpenAPI from entry point {app_entry}: {err_msg}")

    try:
        return json.loads(proc.stdout)
    except Exception as exc:
        raise ValueError(f"OpenAPI extraction subprocess returned invalid JSON: {proc.stdout[:200]}") from exc


def load_openapi_from_local_url(openapi_url: str) -> dict[str, Any]:
    """Fetch an OpenAPI document only from a locally running service."""
    parsed = urllib.parse.urlparse(openapi_url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("--openapi-url must target a locally running service on localhost, 127.0.0.1, or ::1")
    if not parsed.path.endswith("/openapi.json"):
        raise ValueError("--openapi-url must end in /openapi.json")
    with urllib.request.urlopen(openapi_url, timeout=5) as response:  # noqa: S310 - loopback is enforced above
        if response.status != 200:
            raise ValueError(f"Unable to fetch OpenAPI document: HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def _resolve_schema(value: Any, document: dict[str, Any]) -> Any:
    if not isinstance(value, dict):
        return copy.deepcopy(value)
    if "$ref" in value:
        reference = value["$ref"]
        if not isinstance(reference, str) or not reference.startswith("#/components/"):
            raise ValueError(f"Unresolved or non-local OpenAPI reference: {reference!r}")
        target: Any = document
        for segment in reference[2:].split("/"):
            if not isinstance(target, dict) or segment not in target:
                raise ValueError(f"Unresolved OpenAPI reference: {reference}")
            target = target[segment]
        return _resolve_schema(target, document)
    return {key: _resolve_schema(item, document) for key, item in value.items()}


def _parameters(operation: dict[str, Any], path_item: dict[str, Any], document: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    grouped = {"path": {}, "query": {}, "header": {}}
    for raw in [*(path_item.get("parameters", []) or []), *(operation.get("parameters", []) or [])]:
        parameter = _resolve_schema(raw, document)
        location = parameter.get("in")
        name = parameter.get("name")
        if location not in grouped or not isinstance(name, str):
            continue
        grouped[location][name] = {
            "required": bool(parameter.get("required", False)),
            "schema": _resolve_schema(parameter.get("schema", {}), document),
            "description": parameter.get("description"),
        }
    return grouped["path"], grouped["query"], grouped["header"]


def extract_declared_http_contracts(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert an OpenAPI document into CodeClaim normalized exact HTTP contracts."""
    if not isinstance(document.get("paths"), dict):
        raise ValueError("OpenAPI document does not contain a paths object")
    contracts: list[dict[str, Any]] = []
    security_schemes = _resolve_schema(document.get("components", {}).get("securitySchemes", {}), document)

    for path in sorted(document["paths"]):
        path_item = document["paths"][path]
        if not isinstance(path_item, dict):
            continue
        for method in HTTP_METHODS:
            operation = path_item.get(method.lower())
            if not isinstance(operation, dict):
                continue
            path_params, query_params, headers = _parameters(operation, path_item, document)
            body = operation.get("requestBody", {})
            request_schemas_by_media: dict[str, Any] = {}
            if body:
                body = _resolve_schema(body, document)
                content = body.get("content", {}) if isinstance(body, dict) else {}
                for media_type, media_obj in (content.items() if isinstance(content, dict) else []):
                    resolved_media = _resolve_schema(media_obj, document) if isinstance(media_obj, dict) else {}
                    request_schemas_by_media[media_type] = _resolve_schema(resolved_media.get("schema", {}), document)
                json_media = content.get("application/json", {}) if isinstance(content, dict) else {}
                request_schema = _resolve_schema(json_media.get("schema", {}), document)
            else:
                request_schema = {}
            responses: dict[str, Any] = {}
            response_schemas_by_media: dict[str, dict[str, Any]] = {}
            for status_code, response in sorted((operation.get("responses", {}) or {}).items(), key=lambda pair: str(pair[0])):
                resolved = _resolve_schema(response, document)
                content = resolved.get("content", {}) if isinstance(resolved, dict) else {}
                status_media_map: dict[str, Any] = {}
                for media_type, media_obj in (content.items() if isinstance(content, dict) else []):
                    resolved_media = _resolve_schema(media_obj, document) if isinstance(media_obj, dict) else {}
                    status_media_map[media_type] = _resolve_schema(resolved_media.get("schema", {}), document)
                response_schemas_by_media[str(status_code)] = status_media_map
                json_media = content.get("application/json", {}) if isinstance(content, dict) else {}
                responses[str(status_code)] = _resolve_schema(json_media.get("schema", {}), document)
            contracts.append({
                "http_method": method,
                "endpoint_path": path,
                "schema_json": {
                    "type": "object",
                    "x-codeclaim-http-interface": {
                        "http_method": method,
                        "endpoint_path": path,
                        "path_parameters": path_params,
                        "query_parameters": query_params,
                        "declared_headers": headers,
                        "request_body_schema": request_schema,
                        "request_body_schemas_by_media": request_schemas_by_media,
                        "response_schemas": responses,
                        "response_schemas_by_media": response_schemas_by_media,
                        "security_requirements": _resolve_schema(operation.get("security", document.get("security", [])), document),
                        "security_schemes": security_schemes,
                    },
                },
                "semantic_summary": operation.get("summary") or operation.get("description") or f"{method} {path}",
            })
    return contracts


def find_dynamic_behavior(endpoint_code_dir: Path) -> list[str]:
    """Statically flag route/header constructs that generated OpenAPI cannot fully prove."""
    findings: list[str] = []
    for source in sorted(endpoint_code_dir.rglob("*.py")):
        try:
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        except (OSError, SyntaxError) as exc:
            findings.append(f"REVIEW_REQUIRED: unable to statically inspect {source}: {exc}")
            continue
        relative = source.relative_to(endpoint_code_dir)
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                for child in ast.walk(node):
                    if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute) and child.func.attr == "include_router":
                        findings.append(f"REVIEW_REQUIRED: dynamic conditional router inclusion in {relative}:{child.lineno}")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "include_router":
                    for kw in node.keywords:
                        if kw.arg == "prefix" and not (isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str)):
                            findings.append(f"REVIEW_REQUIRED: dynamic router prefix in {relative}:{node.lineno}")
                if node.func.attr in {"get", "post", "put", "patch", "delete", "api_route", "add_api_route"}:
                    if node.args and not (isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
                        findings.append(f"REVIEW_REQUIRED: dynamic route path in {relative}:{node.lineno}")
                if node.func.attr in {"get", "__getitem__"} and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "headers":
                    if not node.args or not isinstance(node.args[0], ast.Constant):
                        findings.append(f"REVIEW_REQUIRED: dynamic request header access in {relative}:{node.lineno}")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Header":
                for keyword in node.keywords:
                    if keyword.arg == "alias" and not isinstance(keyword.value, ast.Constant):
                        findings.append(f"REVIEW_REQUIRED: dynamic declared header alias in {relative}:{node.lineno}")
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute) and node.value.attr == "headers":
                if not isinstance(node.slice, ast.Constant):
                    findings.append(f"REVIEW_REQUIRED: dynamic request header access in {relative}:{node.lineno}")
    return sorted(set(findings))


normalize_openapi = extract_declared_http_contracts


def make_plan(*, service_name: str, repository_path: Path, endpoint_code_dir: Path, app_entry: str | None, openapi_url: str | None) -> OnboardingPlan:
    if bool(app_entry) == bool(openapi_url):
        raise ValueError("Provide exactly one of --app-entry or --openapi-url")
    if not repository_path.is_dir():
        raise ValueError("repository_path must be an existing directory")
    if not endpoint_code_dir.is_dir() or repository_path.resolve() not in {endpoint_code_dir.resolve(), *endpoint_code_dir.resolve().parents}:
        raise ValueError("endpoint_code_dir must be an existing directory inside repository_path")
    document = load_openapi_from_entry(repository_path, app_entry) if app_entry else load_openapi_from_local_url(openapi_url or "")
    return OnboardingPlan(service_name, repository_path.resolve(), endpoint_code_dir.resolve(), app_entry, openapi_url,
                          normalize_openapi(document), find_dynamic_behavior(endpoint_code_dir.resolve()))


def render_plan(plan: OnboardingPlan) -> str:
    lines = [
        "CodeClaim FastAPI onboarding plan",
        f"  Service: {plan.service_name}",
        f"  Repository: {plan.repository_path}",
        f"  Endpoint code: {plan.endpoint_code_dir}",
        f"  Source: {plan.app_entry or plan.openapi_url}",
        f"  Contracts to publish: {len(plan.contracts)}",
    ]
    lines.extend(f"    - {contract['http_method']} {contract['endpoint_path']}" for contract in plan.contracts)
    if plan.review_findings:
        lines.append("  REVIEW_REQUIRED findings:")
        lines.extend(f"    - {finding}" for finding in plan.review_findings)
    else:
        lines.append("  REVIEW_REQUIRED findings: none")
    lines.append("  Writes after approval: CockroachDB service/contracts/audit records and .codeclaim/service.json only.")
    return "\n".join(lines)
