"""Small provider smoke tests used by the CodeClaim live-demo fixture."""

import importlib.util
import sys
from pathlib import Path


_service_dir = Path(__file__).resolve().parents[1]
if str(_service_dir) not in sys.path:
    sys.path.insert(0, str(_service_dir))

_spec = importlib.util.spec_from_file_location("billing_fixture_main", _service_dir / "main.py")
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load Billing fixture from {_service_dir / 'main.py'}")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
app = _module.app


def test_billing_v1_charge_operation_is_published_in_openapi() -> None:
    """The provider exposes the stable operation CodeClaim tracks."""
    openapi = app.openapi()
    operation = openapi["paths"]["/v1/charges"]["post"]

    assert operation["requestBody"]["content"]["application/json"]["schema"]
    assert operation["responses"]["200"]["content"]["application/json"]["schema"]

    schema = openapi["components"]["schemas"]["ChargeRequest"]
    assert "token_id" not in schema.get("required", [])
    assert "card_token" in schema.get("required", [])
