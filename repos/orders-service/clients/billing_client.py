"""Billing Service API Client for Orders Service.

Both V1 and V2 clients target the canonical '/v1/charges' route on the Billing Service:
- BillingClientV1: sends legacy 'card_token'
- BillingClientV2: sends modern 'payment_method_id'
"""

from typing import Any, Optional
import httpx


class BillingClientV1:
    """Client implementing Contract Revision 1.0 (Legacy Card Token) targeting canonical '/v1/charges'."""

    def __init__(self, base_url: str = "http://localhost:8001", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self._custom_client = client

    async def charge(self, amount: int, currency: str, card_token: str, token_id: str) -> dict[str, Any]:
        """Send charge request using Contract Revision 1.0."""
        payload = {
            "amount": amount,
            "currency": currency,
            "card_token": card_token,
            "token_id": token_id,
        }
        if self._custom_client is not None:
            response = await self._custom_client.post("/v1/charges", json=payload)
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient(base_url=self.base_url, timeout=5.0) as http_client:
            response = await http_client.post("/v1/charges", json=payload)
            response.raise_for_status()
            return response.json()


class BillingClientV2:
    """Client implementing Contract Revision 2.0 (Modern Payment Method ID) targeting canonical '/v1/charges'."""

    def __init__(self, base_url: str = "http://localhost:8001", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self._custom_client = client

    async def charge(
        self,
        amount: int,
        currency: str,
        payment_method_id: str,
        description: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send charge request using Contract Revision 2.0."""
        payload: dict[str, Any] = {
            "amount": amount,
            "currency": currency,
            "payment_method_id": payment_method_id,
        }
        if description is not None:
            payload["description"] = description

        if self._custom_client is not None:
            response = await self._custom_client.post("/v1/charges", json=payload)
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient(base_url=self.base_url, timeout=5.0) as http_client:
            response = await http_client.post("/v1/charges", json=payload)
            response.raise_for_status()
            return response.json()
