from fastapi import FastAPI, Header, Query, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

app = FastAPI(title="Onboarding Fixture")
internal_key = APIKeyHeader(name="X-Internal-Key")


class ChargeRequest(BaseModel):
    amount: int
    payment_method_id: str


class ChargeResponse(BaseModel):
    charge_id: str
    status: str


@app.post("/charges/{charge_id}", response_model=ChargeResponse, responses={422: {"description": "Invalid charge"}})
async def create_charge(
    charge_id: str,
    payload: ChargeRequest,
    expand: bool = Query(default=False),
    request_id: str = Header(alias="X-Request-ID"),
    _: str = Security(internal_key),
) -> ChargeResponse:
    return ChargeResponse(charge_id=charge_id, status="accepted")
