"""Deterministic compatibility analysis for JSON Schema and CodeClaim FastAPI/OpenAPI contracts."""

from __future__ import annotations

from typing import Any, Optional, Union

from pydantic import BaseModel, Field


class SchemaDiffResult(BaseModel):
    is_breaking: bool
    breaking_changes: list[dict[str, Any]] = Field(default_factory=list)
    removed_fields: list[dict[str, Any]] = Field(default_factory=list)
    added_required_fields: list[dict[str, Any]] = Field(default_factory=list)
    added_optional_fields: list[dict[str, Any]] = Field(default_factory=list)
    type_changes: list[dict[str, Any]] = Field(default_factory=list)
    diff_summary: str
    classification: str = "NON_BREAKING"
    review_reasons: list[str] = Field(default_factory=list)
    old_revision: Optional[int] = None
    new_revision: Optional[int] = None
    publisher_compatibility: Optional[dict[str, Any]] = None
    migration_notes: Optional[str] = None
    consumer_impact: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


def _normalize(schema: Union[dict[str, Any], type[BaseModel]]) -> dict[str, Any]:
    if isinstance(schema, type) and issubclass(schema, BaseModel):
        return schema.model_json_schema()
    if isinstance(schema, dict):
        return schema.get("schema", schema) if isinstance(schema.get("schema"), dict) else schema
    raise TypeError(f"Expected Pydantic BaseModel class or dict, got {type(schema)}")


def _type_signature(value: dict[str, Any]) -> str:
    if "type" in value:
        return str(value["type"])
    for union_key in ("anyOf", "oneOf"):
        if isinstance(value.get(union_key), list):
            return "|".join(sorted(str(item.get("type", "unknown")) for item in value[union_key] if isinstance(item, dict)))
    if "$ref" in value:
        return f"ref:{value['$ref']}"
    return "unknown"


def _flatten(schema: dict[str, Any], prefix: str = "") -> tuple[dict[str, dict[str, Any]], set[str]]:
    properties: dict[str, dict[str, Any]] = {}
    required: set[str] = set()
    local_required = set(schema.get("required", []))
    for name, prop in sorted((schema.get("properties", {}) or {}).items()):
        path = f"{prefix}.{name}" if prefix else name
        if isinstance(prop, dict) and prop.get("type") == "object" and isinstance(prop.get("properties"), dict):
            nested, nested_required = _flatten(prop, path)
            properties.update(nested)
            if name in local_required:
                required.update(nested_required)
        elif isinstance(prop, dict):
            properties[path] = prop
            if name in local_required:
                required.add(path)
    return properties, required


def _compare_object_schema(old: dict[str, Any], new: dict[str, Any], *, scope: str, required_addition_breaking: bool,
                           changes: list[dict[str, Any]], removed: list[dict[str, Any]], required_added: list[dict[str, Any]],
                           optional_added: list[dict[str, Any]], type_changes: list[dict[str, Any]], review: list[str]) -> None:
    old_props, old_required = _flatten(old)
    new_props, new_required = _flatten(new)
    root_field = scope or "<schema>"
    old_root_type, new_root_type = _type_signature(old), _type_signature(new)
    if old_root_type != new_root_type:
        item = {"field": root_field, "change": "field type changed", "old_type": old_root_type, "new_type": new_root_type}
        type_changes.append(item); changes.append(item)
    if old.get("format") != new.get("format") and (old.get("format") or new.get("format")):
        changes.append({"field": root_field, "change": "field format changed", "old_format": old.get("format"), "new_format": new.get("format")})
    old_root_enum, new_root_enum = old.get("enum"), new.get("enum")
    if isinstance(old_root_enum, list) and isinstance(new_root_enum, list):
        root_removed_values = sorted(set(old_root_enum) - set(new_root_enum))
        if root_removed_values:
            changes.append({"field": root_field, "change": f"enum values removed: {root_removed_values}", "removed_values": root_removed_values})
    for field in sorted(old_props):
        label = f"{scope}.{field}" if scope else field
        if field not in new_props:
            item = {"field": label, "change": "field removed", "old_type": _type_signature(old_props[field]), "was_required": field in old_required}
            removed.append(item); changes.append(item)
            continue
        old_prop, new_prop = old_props[field], new_props[field]
        old_type, new_type = _type_signature(old_prop), _type_signature(new_prop)
        if old_type != new_type:
            item = {"field": label, "change": "field type changed", "old_type": old_type, "new_type": new_type}
            type_changes.append(item); changes.append(item)
        if old_prop.get("format") != new_prop.get("format") and (old_prop.get("format") or new_prop.get("format")):
            changes.append({"field": label, "change": "field format changed", "old_format": old_prop.get("format"), "new_format": new_prop.get("format")})
        old_enum, new_enum = old_prop.get("enum"), new_prop.get("enum")
        if isinstance(old_enum, list) and isinstance(new_enum, list):
            removed_values = sorted(set(old_enum) - set(new_enum))
            if removed_values:
                changes.append({"field": label, "change": f"enum values removed: {removed_values}", "removed_values": removed_values})
        if field not in old_required and field in new_required:
            changes.append({"field": label, "change": "optional field became required"})

        # Validation constraint tightening
        for bound, name, is_increase_breaking in [
            ("minimum", "minimum value increased", True),
            ("exclusiveMinimum", "exclusiveMinimum value increased", True),
            ("maximum", "maximum value decreased", False),
            ("exclusiveMaximum", "exclusiveMaximum value decreased", False),
            ("minLength", "minimum string length increased", True),
            ("maxLength", "maximum string length decreased", False),
            ("minItems", "minimum array items increased", True),
            ("maxItems", "maximum array items decreased", False),
        ]:
            if bound in old_prop and bound in new_prop:
                old_val, new_val = old_prop[bound], new_prop[bound]
                if is_increase_breaking and new_val > old_val:
                    changes.append({"field": label, "change": f"{name} ({old_val} -> {new_val})", "constraint": bound, "old_value": old_val, "new_value": new_val})
                elif not is_increase_breaking and new_val < old_val:
                    changes.append({"field": label, "change": f"{name} ({old_val} -> {new_val})", "constraint": bound, "old_value": old_val, "new_value": new_val})
            elif bound not in old_prop and bound in new_prop:
                changes.append({"field": label, "change": f"constraint {bound} added ({new_prop[bound]})", "constraint": bound, "new_value": new_prop[bound]})

        if old_prop.get("pattern") != new_prop.get("pattern") and new_prop.get("pattern"):
            changes.append({"field": label, "change": "regex pattern constraint changed", "old_pattern": old_prop.get("pattern"), "new_pattern": new_prop.get("pattern")})

        if old_prop.get("nullable", False) is True and new_prop.get("nullable", False) is False:
            changes.append({"field": label, "change": "nullable field became non-nullable"})
    for field in sorted(new_props):
        if field in old_props:
            continue
        label = f"{scope}.{field}" if scope else field
        item = {"field": label, "change": "required field added" if field in new_required else "optional field added", "new_type": _type_signature(new_props[field]), "required": field in new_required}
        if field in new_required and required_addition_breaking:
            required_added.append(item); changes.append(item)
        else:
            optional_added.append(item)
    if old != new and not old_props and not new_props:
        review.append(f"{scope or 'schema'} changed in an unstructured way")


def _security_tightened(old_security: Any, new_security: Any) -> bool:
    old, new = old_security or [], new_security or []
    if not old and new:
        return True
    if not isinstance(old, list) or not isinstance(new, list):
        return old != new
    old_requirements = [{scheme: set(scopes or []) for scheme, scopes in item.items()} for item in old if isinstance(item, dict)]
    new_requirements = [{scheme: set(scopes or []) for scheme, scopes in item.items()} for item in new if isinstance(item, dict)]
    if not old_requirements or not new_requirements:
        return False
    def stricter(new_requirement: dict[str, set[Any]], old_requirement: dict[str, set[Any]]) -> bool:
        if not set(old_requirement).issubset(new_requirement):
            return False
        scopes_preserved = all(old_requirement[scheme].issubset(new_requirement[scheme]) for scheme in old_requirement)
        return scopes_preserved and (set(old_requirement) != set(new_requirement) or any(old_requirement[scheme] != new_requirement[scheme] for scheme in old_requirement))
    return all(any(stricter(new_requirement, old_requirement) for old_requirement in old_requirements) for new_requirement in new_requirements)


def _publisher_declaration(value: Optional[dict[str, Any]]) -> tuple[Optional[dict[str, Any]], list[str]]:
    if value is None:
        return None, []
    if not isinstance(value, dict):
        raise ValueError("publisher compatibility declaration must be an object")
    normalized = dict(value)
    classification = normalized.get("classification")
    if classification is not None:
        classification = str(classification).upper()
        if classification not in {"BREAKING", "NON_BREAKING", "REVIEW_REQUIRED"}:
            raise ValueError("publisher compatibility classification must be breaking, non_breaking, or review_required")
        normalized["classification"] = classification
    for key in ("reason", "migration_notes", "consumer_impact"):
        if key in normalized and not isinstance(normalized[key], str):
            raise ValueError(f"publisher compatibility {key} must be a string")
    reasons = []
    if normalized.get("semantic_change") and not classification:
        reasons.append("Publisher marked a semantic change without a compatibility classification")
    if classification in {"BREAKING", "REVIEW_REQUIRED"} and not normalized.get("reason"):
        reasons.append("Publisher compatibility declaration is missing a reason")
    return normalized, reasons


def compute_schema_diff(old_schema: Union[dict[str, Any], type[BaseModel]], new_schema: Union[dict[str, Any], type[BaseModel]],
                        publisher_compatibility: Optional[dict[str, Any]] = None, *, old_revision: Optional[int] = None,
                        new_revision: Optional[int] = None) -> SchemaDiffResult:
    """Classify FastAPI/OpenAPI contract evolution without allowing unknown changes to look safe."""
    old, new = _normalize(old_schema), _normalize(new_schema)
    declaration, review_reasons = _publisher_declaration(publisher_compatibility)
    breaking: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    required_added: list[dict[str, Any]] = []
    optional_added: list[dict[str, Any]] = []
    type_changes: list[dict[str, Any]] = []
    old_interface, new_interface = old.get("x-codeclaim-http-interface"), new.get("x-codeclaim-http-interface")
    if bool(old_interface) != bool(new_interface):
        review_reasons.append("Only one revision contains a normalized HTTP interface")
    if old_interface and new_interface:
        for key in ("http_method", "endpoint_path"):
            if old_interface.get(key) != new_interface.get(key):
                breaking.append({"field": f"operation.{key}", "change": "endpoint or method changed", "old": old_interface.get(key), "new": new_interface.get(key)})
        for location, label in (("path_parameters", "path parameter"), ("query_parameters", "query parameter"), ("declared_headers", "header")):
            old_params, new_params = old_interface.get(location, {}) or {}, new_interface.get(location, {}) or {}
            if not isinstance(old_params, dict) or not isinstance(new_params, dict):
                review_reasons.append(f"{location} is not a normalized object")
                continue
            for name in sorted(set(old_params) | set(new_params)):
                old_param, new_param = old_params.get(name), new_params.get(name)
                field = f"{label}.{name}"
                if old_param is None:
                    if isinstance(new_param, dict) and new_param.get("required"):
                        breaking.append({"field": field, "change": f"required {label} added"})
                    else:
                        optional_added.append({"field": field, "change": f"optional {label} added"})
                elif new_param is None:
                    if isinstance(old_param, dict) and old_param.get("required"):
                        breaking.append({"field": field, "change": f"required {label} removed"})
                    else:
                        review_reasons.append(f"optional {label} removed: server tolerance is unknown")
                elif isinstance(old_param, dict) and isinstance(new_param, dict):
                    if not old_param.get("required") and new_param.get("required"):
                        breaking.append({"field": field, "change": f"optional {label} became required"})
                    _compare_object_schema(old_param.get("schema", {}), new_param.get("schema", {}), scope=field,
                                           required_addition_breaking=True, changes=breaking, removed=removed,
                                           required_added=required_added, optional_added=optional_added,
                                           type_changes=type_changes, review=review_reasons)
                else:
                    review_reasons.append(f"{field} is not a normalized parameter")
        _compare_object_schema(old_interface.get("request_body_schema", {}), new_interface.get("request_body_schema", {}),
                               scope="request_body", required_addition_breaking=True, changes=breaking, removed=removed,
                               required_added=required_added, optional_added=optional_added, type_changes=type_changes, review=review_reasons)
        old_responses, new_responses = old_interface.get("response_schemas", {}) or {}, new_interface.get("response_schemas", {}) or {}
        if not isinstance(old_responses, dict) or not isinstance(new_responses, dict):
            review_reasons.append("response_schemas is not a normalized object")
        else:
            for status in sorted(set(old_responses) | set(new_responses)):
                if status not in new_responses:
                    breaking.append({"field": f"response.{status}", "change": "response status removed"})
                elif status in old_responses:
                    _compare_object_schema(old_responses[status], new_responses[status], scope=f"response.{status}",
                                           required_addition_breaking=False, changes=breaking, removed=removed,
                                           required_added=required_added, optional_added=optional_added,
                                           type_changes=type_changes, review=review_reasons)
        if _security_tightened(old_interface.get("security_requirements"), new_interface.get("security_requirements")):
            breaking.append({"field": "security", "change": "security/auth requirement tightened"})
    else:
        _compare_object_schema(old, new, scope="", required_addition_breaking=True, changes=breaking, removed=removed,
                               required_added=required_added, optional_added=optional_added, type_changes=type_changes, review=review_reasons)
    if declaration and declaration.get("classification") == "BREAKING":
        breaking.append({"field": "<publisher>", "change": declaration.get("reason") or "publisher declared breaking compatibility"})
    if declaration and declaration.get("classification") == "REVIEW_REQUIRED":
        review_reasons.append(declaration.get("reason") or "publisher requested compatibility review")
    if old != new and not breaking and not review_reasons:
        # Check deep structural equality after removing recognized additions
        if not (optional_added and not removed and not required_added and not type_changes and set(old.keys()).issubset(set(new.keys()))):
            review_reasons.append("Contract changed without a recognized deterministic compatibility rule")
    breaking.sort(key=lambda item: (item.get("field", ""), item.get("change", "")))
    removed.sort(key=lambda item: item["field"]); required_added.sort(key=lambda item: item["field"])
    optional_added.sort(key=lambda item: item["field"]); type_changes.sort(key=lambda item: item["field"])
    review_reasons = sorted(set(review_reasons))
    classification = "BREAKING" if breaking else ("REVIEW_REQUIRED" if review_reasons else "NON_BREAKING")
    summary_parts = [item["change"] + f": {item['field']}" for item in breaking]
    summary_parts += [f"optional addition: {item['field']}" for item in optional_added]
    summary_parts += [f"review required: {reason}" for reason in review_reasons]
    return SchemaDiffResult(
        is_breaking=bool(breaking), breaking_changes=breaking, removed_fields=removed,
        added_required_fields=required_added, added_optional_fields=optional_added, type_changes=type_changes,
        diff_summary="; ".join(summary_parts) if summary_parts else "No structural changes detected",
        classification=classification, review_reasons=review_reasons, old_revision=old_revision,
        new_revision=new_revision, publisher_compatibility=declaration,
        migration_notes=declaration.get("migration_notes") if declaration else None,
        consumer_impact=declaration.get("consumer_impact") if declaration else None,
    )
