"""Cross-Service HTTP Contract Compatibility Scenario Test Suite.

Executes real HTTP requests across sibling microservices via ASGI transport:
1. Scenario 1: Billing v1 App + Orders v1 Client -> PASS (HTTP 200)
2. Scenario 2: Billing v2 App + Orders v1 Client -> PASS (Asserts Expected HTTP 422 Unprocessable Entity)
3. Scenario 3: Billing v2 App + Orders v2 Client -> PASS (HTTP 200 Reconciled)
"""

import importlib.util
import json
from pathlib import Path
import httpx
from httpx import ASGITransport
import pytest

import os
import sys

# Explicitly load billing service main module by absolute path to avoid collision with orders-service main.py
_billing_main_path = None

# 1. Environment variable
if os.environ.get("BILLING_SERVICE_PATH"):
    env_candidate = Path(os.environ["BILLING_SERVICE_PATH"])
    if env_candidate.exists():
        _billing_main_path = env_candidate

# 2. Sibling directory relative to tests
if not _billing_main_path or not _billing_main_path.exists():
    candidate1 = Path(__file__).parent.parent.parent / "billing-service" / "main.py"
    if candidate1.exists():
        _billing_main_path = candidate1

# 3. sys.path search
if not _billing_main_path or not _billing_main_path.exists():
    for p in sys.path:
        cand = Path(p) / "main.py"
        if cand.exists() and "billing-service" in str(cand):
            _billing_main_path = cand
            break
        cand2 = Path(p) / "repos" / "billing-service" / "main.py"
        if cand2.exists():
            _billing_main_path = cand2
            break

# 4. Search parent directories for repos/billing-service
if not _billing_main_path or not _billing_main_path.exists():
    for parent in Path(__file__).resolve().parents:
        cand3 = parent / "repos" / "billing-service" / "main.py"
        if cand3.exists():
            _billing_main_path = cand3
            break

# 5. Fixed project path fallback
if not _billing_main_path or not _billing_main_path.exists():
    known_path = Path("C:/Users/dell/Desktop/Projects/code-claim/repos/billing-service/main.py")
    if known_path.exists():
        _billing_main_path = known_path

if not _billing_main_path or not _billing_main_path.exists():
    raise FileNotFoundError(f"Could not locate billing-service/main.py for contract tests (checked sys.path, parents, and known project path)")

# Billing's application imports its schemas as top-level modules. Add only the
# resolved Billing repository directory so the dynamically loaded fixture has
# the same import context it has when run from its own service directory.
_billing_service_dir = _billing_main_path.parent
if str(_billing_service_dir) not in sys.path:
    sys.path.insert(0, str(_billing_service_dir))

_spec = importlib.util.spec_from_file_location("billing_main_module", _billing_main_path)
_billing_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_billing_module)

create_billing_app = _billing_module.create_billing_app




from clients.billing_client import BillingClientV1, BillingClientV2


# ==============================================================================
# Scenario 1: Billing v1 App + Orders v1 Client (Compatible Baseline)
# ==============================================================================


@pytest.mark.asyncio
async def test_scenario_1_billing_v1_orders_v1_http_compatible():
    """Scenario 1: Orders v1 client sends card_token to Billing v1 app -> HTTP 200."""
    billing_app_v1 = create_billing_app(revision="v1")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=billing_app_v1),
        base_url="http://billing-service.local",
    ) as http_client:
        client_v1 = BillingClientV1(client=http_client)
        response = await client_v1.charge(
            amount=4999,
            currency="usd",
            card_token="tok_visa_4242_test",
        )

        assert response["status"] == "succeeded"
        assert response["amount"] == 4999
        assert response["currency"] == "usd"
        assert response["card_token"] == "tok_visa_4242_test"
        assert response["charge_id"].startswith("ch_v1_")


# ==============================================================================
# Scenario 2: Billing v2 App + Orders v1 Client (Asserted Breaking Drift HTTP 422)
# ==============================================================================


@pytest.mark.asyncio
async def test_scenario_2_billing_v2_orders_v1_http_drift_failure():
    """Scenario 2: Unreconciled Orders v1 client sends card_token to Billing v2 app on canonical '/v1/charges' -> HTTP 422."""
    billing_app_v2 = create_billing_app(revision="v2")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=billing_app_v2),
        base_url="http://billing-service.local",
    ) as http_client:
        client_v1 = BillingClientV1(client=http_client)

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client_v1.charge(
                amount=4999,
                currency="usd",
                card_token="tok_visa_4242_test",
            )

        # Verify exact HTTP 422 Unprocessable Entity status code
        error_response = exc_info.value.response
        assert error_response.status_code == 422

        # Verify structured error detail identifies missing 'payment_method_id'
        error_data = error_response.json()
        assert "detail" in error_data
        field_errors = error_data["detail"]
        assert len(field_errors) >= 1
        
        missing_fields = [err["loc"][-1] for err in field_errors if err["type"] == "missing"]
        assert "payment_method_id" in missing_fields


# ==============================================================================
# Scenario 3: Billing v2 App + Orders v2 Client (Reconciled & Compatible)
# ==============================================================================


@pytest.mark.asyncio
async def test_scenario_3_billing_v2_orders_v2_http_reconciled():
    """Scenario 3: Reconciled Orders v2 client sends payment_method_id to Billing v2 app -> HTTP 200."""
    billing_app_v2 = create_billing_app(revision="v2")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=billing_app_v2),
        base_url="http://billing-service.local",
    ) as http_client:
        client_v2 = BillingClientV2(client=http_client)
        response = await client_v2.charge(
            amount=4999,
            currency="usd",
            payment_method_id="pm_card_visa_reconciled_999",
            description="Order #4002 Reconciled Checkout",
        )

        assert response["status"] == "succeeded"
        assert response["amount"] == 4999
        assert response["currency"] == "usd"
        assert response["payment_method_id"] == "pm_card_visa_reconciled_999"
        assert response["description"] == "Order #4002 Reconciled Checkout"
        assert response["charge_id"].startswith("ch_v2_")


@pytest.mark.asyncio
async def test_billing_v1_request_uses_the_pre_change_shape():
    """The baseline Orders client has not yet added the new required token_id."""
    captured_payload = None

    async def capture_request(request: httpx.Request) -> httpx.Response:
        nonlocal captured_payload
        captured_payload = json.loads(request.content)
        return httpx.Response(
            status_code=200,
            json={
                "charge_id": "ch_v1_payload_test",
                "status": "succeeded",
                "amount": captured_payload["amount"],
                "currency": captured_payload["currency"],
                "card_token": captured_payload["card_token"],
            },
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(capture_request),
        base_url="http://billing-service.local",
    ) as http_client:
        client_v1 = BillingClientV1(client=http_client)
        await client_v1.charge(
            amount=4999,
            currency="usd",
            card_token="tok_visa_4242_test",
        )

    assert captured_payload == {
        "amount": 4999,
        "currency": "usd",
        "card_token": "tok_visa_4242_test",
    }
