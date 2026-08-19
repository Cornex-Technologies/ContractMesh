"""Billing Microservice Application.

Supports deployment under specific contract revisions on the canonical '/v1/charges' route:
- Contract Revision 1.0: Expects 'card_token'; the public demo later adds 'token_id'
- Contract Revision 2.0: Expects 'payment_method_id' (Breaking Migration)
"""

import os
import uuid
from typing import Literal
from fastapi import FastAPI, HTTPException
import schemas_v1
import schemas_v2


def create_billing_app(revision: Literal["v1", "v2"] = "v1") -> FastAPI:
    """Factory creating Billing FastAPI app with the specified contract revision active on '/v1/charges'."""
    app = FastAPI(
        title="Billing Service",
        version="1.0.0" if revision == "v1" else "2.0.0",
        description=f"Payment processing microservice deployed on Contract Revision {revision.upper()}",
    )
    app.state.revision = revision

    @app.get("/health")
    async def health_check():
        """Service health and active contract revision endpoint."""
        return {
            "status": "healthy",
            "service": "billing-service",
            "active_contract_revision": app.state.revision,
        }

    if revision == "v1":
        @app.post("/v1/charges", response_model=schemas_v1.ChargeResponse)
        async def create_charge_v1(request: schemas_v1.ChargeRequest):
            """Canonical charge route enforcing Contract Revision 1.0 (Legacy Card Token)."""
            return schemas_v1.ChargeResponse(
                charge_id=f"ch_v1_{uuid.uuid4().hex[:12]}",
                status="succeeded",
                amount=request.amount,
                currency=request.currency,
                card_token=request.card_token,
            )
    else:
        @app.post("/v1/charges", response_model=schemas_v2.ChargeResponse)
        async def create_charge_v2(request: schemas_v2.ChargeRequest):
            """Canonical charge route enforcing Contract Revision 2.0 (Modern Payment Method ID)."""
            return schemas_v2.ChargeResponse(
                charge_id=f"ch_v2_{uuid.uuid4().hex[:12]}",
                status="succeeded",
                amount=request.amount,
                currency=request.currency,
                payment_method_id=request.payment_method_id,
                description=request.description,
            )

    return app


# Default app instance driven by environment variable BILLING_CONTRACT_REVISION
default_revision = os.getenv("BILLING_CONTRACT_REVISION", "v1").lower()
if default_revision not in ("v1", "v2"):
    default_revision = "v1"

app = create_billing_app(revision=default_revision)  # type: ignore


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
