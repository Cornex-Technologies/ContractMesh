"""Orders Microservice Application."""

import os
import uuid
from typing import Optional
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from clients.billing_client import BillingClientV1, BillingClientV2

app = FastAPI(
    title="Orders Service",
    version="1.0.0",
    description="E-commerce order placement microservice consuming Billing Service",
)

# Demo fixture for the CodeClaim compatibility workflow.  This value already
# exists in the consumer service before Billing changes its contract.  The
# consumer agent must discover and include it in the outgoing Billing request
# only after CodeClaim reports that ``token_id`` became required.
TOKEN_ID = "demo-token-id"


class CheckoutRequest(BaseModel):
    """Checkout request payload."""
    item_id: str
    amount: int = Field(..., gt=0, description="Amount in cents")
    currency: str = "usd"
    card_token: Optional[str] = None
    payment_method_id: Optional[str] = None


class CheckoutResponse(BaseModel):
    """Checkout response receipt."""
    order_id: str
    status: str
    charge_id: str
    amount: int
    currency: str


def get_billing_base_url() -> str:
    """Retrieve billing service base URL from environment."""
    return os.getenv("BILLING_SERVICE_URL", "http://localhost:8001")


def get_billing_client_v1(base_url: str = Depends(get_billing_base_url)) -> BillingClientV1:
    return BillingClientV1(base_url=base_url)


def get_billing_client_v2(base_url: str = Depends(get_billing_base_url)) -> BillingClientV2:
    return BillingClientV2(base_url=base_url)


@app.get("/health")
async def health_check():
    """Service health check endpoint."""
    return {"status": "healthy", "service": "orders-service"}


@app.post("/v1/checkout", response_model=CheckoutResponse)
async def checkout(
    request: CheckoutRequest,
    client_v1: BillingClientV1 = Depends(get_billing_client_v1),
    client_v2: BillingClientV2 = Depends(get_billing_client_v2),
):
    """Execute checkout flow by calling upstream Billing Service."""
    if request.payment_method_id:
        # V2 Client Path
        charge_res = await client_v2.charge(
            amount=request.amount,
            currency=request.currency,
            payment_method_id=request.payment_method_id,
            description=f"Order for item {request.item_id}",
        )
    elif request.card_token:
        # V1 Client Path
        charge_res = await client_v1.charge(
            amount=request.amount,
            currency=request.currency,
            card_token=request.card_token,
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Either 'payment_method_id' or 'card_token' must be provided.",
        )

    return CheckoutResponse(
        order_id=f"ord_{uuid.uuid4().hex[:12]}",
        status="confirmed",
        charge_id=charge_res["charge_id"],
        amount=charge_res["amount"],
        currency=charge_res["currency"],
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
