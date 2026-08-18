"""Compatibility matrix for normalized FastAPI/OpenAPI HTTP contracts."""

import copy

import pytest

from coordinator.differencer import compute_schema_diff


def contract(**overrides):
    interface = {
        "http_method": "POST",
        "endpoint_path": "/v1/charges",
        "path_parameters": {"charge_id": {"required": True, "schema": {"type": "string"}}},
        "query_parameters": {"expand": {"required": False, "schema": {"type": "boolean"}}},
        "declared_headers": {"x-request-id": {"required": False, "schema": {"type": "string"}}},
        "request_body_schema": {
            "type": "object", "properties": {
                "amount": {"type": "integer", "format": "int64"},
                "currency": {"type": "string", "enum": ["usd", "eur"]},
                "note": {"type": "string"},
            }, "required": ["amount", "currency"],
        },
        "response_schemas": {"200": {"type": "object", "properties": {
            "charge_id": {"type": "string", "format": "uuid"}, "status": {"type": "string"},
        }, "required": ["charge_id", "status"]}, "422": {"type": "object"}},
        "security_requirements": [],
    }
    interface.update(overrides)
    return {"type": "object", "x-codeclaim-http-interface": interface}


@pytest.mark.parametrize("location,name", [("path_parameters", "tenant_id"), ("query_parameters", "customer_id"), ("declared_headers", "x-tenant")])
def test_required_parameter_addition_is_breaking(location, name):
    old, new = contract(), contract()
    new["x-codeclaim-http-interface"][location][name] = {"required": True, "schema": {"type": "string"}}
    diff = compute_schema_diff(old, new)
    assert diff.classification == "BREAKING"
    assert any(name in item["field"] and "required" in item["change"] for item in diff.breaking_changes)


@pytest.mark.parametrize("location,name", [("path_parameters", "charge_id"), ("query_parameters", "customer_id"), ("declared_headers", "x-tenant")])
def test_required_parameter_removal_is_breaking(location, name):
    old, new = contract(), contract()
    old["x-codeclaim-http-interface"][location][name] = {"required": True, "schema": {"type": "string"}}
    new["x-codeclaim-http-interface"][location].pop(name, None)
    diff = compute_schema_diff(old, new)
    assert diff.classification == "BREAKING"


def test_optional_parameter_addition_is_non_breaking():
    old, new = contract(), contract()
    new["x-codeclaim-http-interface"]["query_parameters"]["trace"] = {"required": False, "schema": {"type": "string"}}
    diff = compute_schema_diff(old, new)
    assert diff.classification == "NON_BREAKING"
    assert not diff.is_breaking


def test_request_required_addition_and_field_removal_or_rename_are_breaking():
    old, new = contract(), contract()
    body = new["x-codeclaim-http-interface"]["request_body_schema"]
    body["properties"].pop("currency")
    body["properties"]["payment_currency"] = {"type": "string"}
    body["required"] = ["amount", "payment_currency"]
    diff = compute_schema_diff(old, new)
    assert diff.classification == "BREAKING"
    assert any("request_body.currency" == item["field"] for item in diff.removed_fields)
    assert any("request_body.payment_currency" == item["field"] for item in diff.added_required_fields)


@pytest.mark.parametrize("mutation", ["type", "format", "enum"])
def test_field_type_format_and_enum_tightening_are_breaking(mutation):
    old, new = contract(), contract()
    amount = new["x-codeclaim-http-interface"]["request_body_schema"]["properties"]["amount"]
    currency = new["x-codeclaim-http-interface"]["request_body_schema"]["properties"]["currency"]
    if mutation == "type":
        amount["type"] = "string"
    elif mutation == "format":
        amount["format"] = "int32"
    else:
        currency["enum"] = ["usd"]
    assert compute_schema_diff(old, new).classification == "BREAKING"


def test_endpoint_method_response_and_security_tightening_are_breaking():
    old, new = contract(), contract()
    interface = new["x-codeclaim-http-interface"]
    interface["http_method"] = "PUT"
    interface["endpoint_path"] = "/v2/charges"
    interface["response_schemas"].pop("422")
    interface["response_schemas"]["200"]["properties"].pop("status")
    interface["security_requirements"] = [{"InternalApiKey": []}]
    diff = compute_schema_diff(old, new)
    assert diff.classification == "BREAKING"
    changes = {item["change"] for item in diff.breaking_changes}
    assert "endpoint or method changed" in changes
    assert "response status removed" in changes
    assert "security/auth requirement tightened" in changes
    assert any(item["field"] == "response.200.status" for item in diff.removed_fields)


def test_security_scope_tightening_is_breaking():
    old, new = contract(), contract()
    old["x-codeclaim-http-interface"]["security_requirements"] = [{"OAuth": ["charges:read"]}]
    new["x-codeclaim-http-interface"]["security_requirements"] = [{"OAuth": ["charges:read", "charges:write"]}]
    assert compute_schema_diff(old, new).classification == "BREAKING"


def test_unknown_contract_change_is_review_required_never_non_breaking():
    old, new = contract(), contract()
    new["x-codeclaim-http-interface"]["request_body_schema"] = {"minimum": 1}
    diff = compute_schema_diff(old, new)
    assert diff.classification == "BREAKING"  # Existing fields disappeared, a deterministic break.
    unknown_old = {"type": "string", "x-behavior": "old"}
    unknown_new = {"type": "string", "x-behavior": "new"}
    assert compute_schema_diff(unknown_old, unknown_new).classification == "REVIEW_REQUIRED"


@pytest.mark.parametrize("declaration,expected", [
    ({"classification": "breaking", "reason": "idempotency behavior changed"}, "BREAKING"),
    ({"classification": "review_required", "reason": "ordering changed", "migration_notes": "replay safely", "consumer_impact": "sort results"}, "REVIEW_REQUIRED"),
    ({"classification": "non_breaking", "reason": "documentation only"}, "NON_BREAKING"),
])
def test_publisher_declaration_is_structured_and_cannot_downgrade_breaks(declaration, expected):
    old, new = contract(), contract()
    diff = compute_schema_diff(old, new, publisher_compatibility=declaration, old_revision=4, new_revision=5)
    assert diff.classification == expected
    assert diff.old_revision == 4 and diff.new_revision == 5
    assert diff.publisher_compatibility["classification"] == declaration["classification"].upper()
    if expected == "REVIEW_REQUIRED":
        assert diff.migration_notes == "replay safely"
        assert diff.consumer_impact == "sort results"
    breaking_new = copy.deepcopy(new)
    breaking_new["x-codeclaim-http-interface"]["request_body_schema"]["properties"].pop("amount")
    assert compute_schema_diff(old, breaking_new, publisher_compatibility={"classification": "non_breaking"}).classification == "BREAKING"
