"""Section 3 Verification Suite: Deterministic Schema Differencing Engine."""

import sys
from pathlib import Path
from typing import Optional, Union
import pytest
from pydantic import BaseModel, Field

# Ensure billing schemas can be imported for testing
billing_pkg_path = str(Path(__file__).parent.parent / "repos" / "billing-service")
if billing_pkg_path not in sys.path:
    sys.path.insert(0, billing_pkg_path)

import schemas_v1
import schemas_v2
from coordinator.differencer import SchemaDiffResult, compute_schema_diff


# ==============================================================================
# 1. Billing Service v1 vs v2 Contract Migration Tests
# ==============================================================================


def test_billing_v1_to_v2_breaking_diff():
    """Verify exact detection of breaking changes between Billing v1 and v2 schemas."""
    diff = compute_schema_diff(schemas_v1.ChargeRequest, schemas_v2.ChargeRequest)

    assert diff.is_breaking is True
    assert len(diff.breaking_changes) >= 2

    # Verify removed field 'card_token'
    removed_names = [f["field"] for f in diff.removed_fields]
    assert "card_token" in removed_names

    # Verify newly added required field 'payment_method_id'
    added_req_names = [f["field"] for f in diff.added_required_fields]
    assert "payment_method_id" in added_req_names

    # Verify newly added optional field 'description'
    added_opt_names = [f["field"] for f in diff.added_optional_fields]
    assert "description" in added_opt_names

    # Check summary string
    assert "card_token" in diff.diff_summary
    assert "payment_method_id" in diff.diff_summary


# ==============================================================================
# 2. Raw JSON-Schema Optional Fields Without Default Tests
# ==============================================================================


def test_raw_json_schema_optional_field_without_default_is_non_breaking():
    """Verify that in raw JSON Schema, a field omitted from 'required' is non-breaking even without default."""
    old_raw = {
        "type": "object",
        "properties": {
            "amount": {"type": "integer"},
        },
        "required": ["amount"],
    }
    new_raw = {
        "type": "object",
        "properties": {
            "amount": {"type": "integer"},
            "note": {"type": "string"},  # optional, no default specified
        },
        "required": ["amount"],  # 'note' is not in required
    }

    diff = compute_schema_diff(old_raw, new_raw)
    assert diff.is_breaking is False
    assert len(diff.breaking_changes) == 0
    assert len(diff.added_optional_fields) == 1
    assert diff.added_optional_fields[0]["field"] == "note"


# ==============================================================================
# 3. Deterministic Output Ordering Tests
# ==============================================================================


def test_deterministic_output_ordering_regardless_of_input_dict_order():
    """Verify that schemas with reversed property ordering produce identical diff results."""
    schema_a1 = {
        "type": "object",
        "properties": {
            "zebra": {"type": "string"},
            "alpha": {"type": "integer"},
            "beta": {"type": "string"},
        },
        "required": ["zebra", "alpha"],
    }
    schema_b1 = {
        "type": "object",
        "properties": {
            "beta": {"type": "string"},
            "gamma": {"type": "string"},
            "alpha": {"type": "integer"},
        },
        "required": ["beta", "gamma"],
    }

    schema_a2 = {
        "type": "object",
        "properties": {
            "alpha": {"type": "integer"},
            "beta": {"type": "string"},
            "zebra": {"type": "string"},
        },
        "required": ["alpha", "zebra"],
    }
    schema_b2 = {
        "type": "object",
        "properties": {
            "gamma": {"type": "string"},
            "alpha": {"type": "integer"},
            "beta": {"type": "string"},
        },
        "required": ["gamma", "beta"],
    }

    diff1 = compute_schema_diff(schema_a1, schema_b1)
    diff2 = compute_schema_diff(schema_a2, schema_b2)

    assert diff1.to_dict() == diff2.to_dict()
    assert diff1.diff_summary == diff2.diff_summary


# ==============================================================================
# 4. Existing Optional Field Becoming Required Tests
# ==============================================================================


def test_optional_field_becoming_required_is_breaking():
    """Verify that an existing optional field becoming required is flagged as breaking."""
    old_schema = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "tax_id": {"type": "string"},
        },
        "required": ["user_id"],  # tax_id is optional
    }
    new_schema = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "tax_id": {"type": "string"},
        },
        "required": ["user_id", "tax_id"],  # tax_id is now required
    }

    diff = compute_schema_diff(old_schema, new_schema)
    assert diff.is_breaking is True
    assert any(
        change.get("field") == "tax_id" and "became required" in change.get("change", "")
        for change in diff.breaking_changes
    )


# ==============================================================================
# 5. Nullable & anyOf Field Type Mutation Tests
# ==============================================================================


class ProfileV1(BaseModel):
    name: str
    phone: Optional[str] = None


class ProfileV2TypeMutated(BaseModel):
    name: str
    phone: Optional[int] = None


def test_nullable_anyof_type_mutation():
    """Verify that type mutation within an anyOf / Optional field is detected as breaking."""
    diff = compute_schema_diff(ProfileV1, ProfileV2TypeMutated)

    assert diff.is_breaking is True
    assert len(diff.type_changes) == 1
    assert diff.type_changes[0]["field"] == "phone"
    assert "string" in diff.type_changes[0]["old_type"]
    assert "integer" in diff.type_changes[0]["new_type"]


# ==============================================================================
# 6. Nested Object Recursive Differencing Tests
# ==============================================================================


def test_nested_object_recursive_differencing():
    """Verify that nested schema changes are diffed recursively with dot-notation."""
    old_schema = {
        "type": "object",
        "properties": {
            "customer": {
                "type": "object",
                "properties": {
                    "email": {"type": "string"},
                    "address": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string"},
                            "zip_code": {"type": "string"},
                        },
                        "required": ["city", "zip_code"],
                    },
                },
                "required": ["email", "address"],
            },
        },
        "required": ["customer"],
    }

    new_schema = {
        "type": "object",
        "properties": {
            "customer": {
                "type": "object",
                "properties": {
                    "email": {"type": "string"},
                    "address": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string"},
                            "postal_code": {"type": "string"},  # required zip_code replaced with postal_code
                        },
                        "required": ["city", "postal_code"],
                    },
                },
                "required": ["email", "address"],
            },
        },
        "required": ["customer"],
    }

    diff = compute_schema_diff(old_schema, new_schema)
    assert diff.is_breaking is True
    
    removed_fields = [f["field"] for f in diff.removed_fields]
    added_required = [f["field"] for f in diff.added_required_fields]
    
    assert "customer.address.zip_code" in removed_fields
    assert "customer.address.postal_code" in added_required


# ==============================================================================
# 7. Enum Choices Reduction Tests
# ==============================================================================


def test_enum_choices_removal_is_breaking():
    """Verify that removing supported Enum choices is flagged as breaking."""
    old_schema = {
        "type": "object",
        "properties": {
            "currency": {"type": "string", "enum": ["usd", "eur", "gbp"]},
        },
        "required": ["currency"],
    }
    new_schema = {
        "type": "object",
        "properties": {
            "currency": {"type": "string", "enum": ["usd", "eur"]},  # 'gbp' removed
        },
        "required": ["currency"],
    }

    diff = compute_schema_diff(old_schema, new_schema)
    assert diff.is_breaking is True
    assert any("gbp" in c.get("change", "") for c in diff.breaking_changes)


# ==============================================================================
# 8. Serialization & Dict Export Tests
# ==============================================================================


def test_diff_to_dict_export():
    """Verify diff exports a JSON-compliant dict ready for CockroachDB storage."""
    diff = compute_schema_diff(schemas_v1.ChargeRequest, schemas_v2.ChargeRequest)
    payload = diff.to_dict()

    assert isinstance(payload, dict)
    assert payload["is_breaking"] is True
    assert "breaking_changes" in payload
    assert "diff_summary" in payload
    assert isinstance(payload["breaking_changes"], list)
