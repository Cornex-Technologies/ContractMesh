"""Unit tests for the read-only local/cloud live preflight checks."""

from __future__ import annotations

from scripts.live_preflight import evaluate_baseline


def _baseline_rows():
    return (
        [
            {
                "service_name": "billing-service",
                "endpoint_path": "/v1/charges",
                "http_method": "POST",
                "latest_revision": 1,
            }
        ],
        [
            {
                "consumer_service": "orders-service",
                "provider_service": "billing-service",
                "endpoint_path": "/v1/charges",
                "http_method": "POST",
                "assumed_provider_revision": 1,
                "confirmation_status": "CONFIRMED",
            }
        ],
        [],
    )


def test_baseline_preflight_passes_for_revision_one_dependency():
    contracts, dependencies, work = _baseline_rows()

    result = evaluate_baseline(contracts, dependencies, work)

    assert result["ready"] is True
    assert all(check["passed"] for check in result["checks"])


def test_baseline_preflight_blocks_when_provider_is_already_revision_two():
    contracts, dependencies, work = _baseline_rows()
    contracts[0]["latest_revision"] = 2

    result = evaluate_baseline(contracts, dependencies, work)

    assert result["ready"] is False
    assert any(check["name"] == "billing_revision_one" and not check["passed"] for check in result["checks"])


def test_baseline_preflight_blocks_unresolved_provider_work():
    contracts, dependencies, work = _baseline_rows()
    work.append(
        {
            "work_item_id": "work-1",
            "provider_service": "billing-service",
            "target_service": "orders-service",
            "source_contract_revision": 2,
            "state": "PENDING",
        }
    )

    result = evaluate_baseline(contracts, dependencies, work)

    assert result["ready"] is False
    check = next(check for check in result["checks"] if check["name"] == "no_unresolved_billing_work")
    assert check["passed"] is False
    assert "work-1" in check["details"]["work_item_ids"]


def test_baseline_preflight_rejects_unconfirmed_or_wrong_revision_dependency():
    contracts, dependencies, work = _baseline_rows()
    dependencies[0]["confirmation_status"] = "DECLARED"
    dependencies[0]["assumed_provider_revision"] = 2

    result = evaluate_baseline(contracts, dependencies, work)

    assert result["ready"] is False
    check = next(check for check in result["checks"] if check["name"] == "orders_dependency_revision_one")
    assert check["passed"] is False
