"""Billing Service - Contract Revision 1.0 Pydantic Schemas."""

from pydantic import BaseModel, Field


class ChargeRequest(BaseModel):
    """Charge request payload expected by Billing Service v1.0."""
    amount: int = Field(..., gt=0, description="Amount in cents, e.g. 5000 for $50.00")
    currency: str = Field(default="usd", min_length=3, max_length=3, description="ISO 4217 currency code")
    card_token: str = Field(..., min_length=1, description="Legacy single-use card token, e.g. tok_visa_4242")
    token_id: str = Field(..., min_length=1, description="Token ID")


class ChargeResponse(BaseModel):
    """Charge response payload returned by Billing Service v1.0."""
    charge_id: str
    status: str = "succeeded"
    amount: int
    currency: str
    card_token: str
