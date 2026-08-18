"""Billing Service - Contract Revision 2.0 Pydantic Schemas (Breaking Migration)."""

from typing import Optional
from pydantic import BaseModel, Field


class ChargeRequest(BaseModel):
    """Charge request payload expected by Billing Service v2.0.
    
    Breaking Change:
    - 'card_token' has been removed.
    - 'payment_method_id' is now required (e.g. pm_card_visa_4242).
    - 'description' is an optional additive field.
    """
    amount: int = Field(..., gt=0, description="Amount in cents, e.g. 5000 for $50.00")
    currency: str = Field(default="usd", min_length=3, max_length=3, description="ISO 4217 currency code")
    payment_method_id: str = Field(..., min_length=1, description="Reusable modern payment method ID, e.g. pm_card_visa_4242")
    description: Optional[str] = Field(default=None, description="Optional charge description")


class ChargeResponse(BaseModel):
    """Charge response payload returned by Billing Service v2.0."""
    charge_id: str
    status: str = "succeeded"
    amount: int
    currency: str
    payment_method_id: str
    description: Optional[str] = None
