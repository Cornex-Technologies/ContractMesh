import asyncio
import selectors
from coordinator.db import fetch_all, close_pool
from coordinator.compatibility import authenticate_harness

async def verify_step3():
    print("--------------------------------------------------")
    print("CodeClaim Step 3: MCP Server & Harness Verification")
    print("--------------------------------------------------")
    
    # 1. Check all registered harnesses in database
    harnesses = await fetch_all("""
        SELECT harness_id, harness_name, harness_type, service_name, status, last_seen_at
        FROM harness_registrations
        ORDER BY service_name;
    """)
    print(f"Registered Agent Harnesses ({len(harnesses)}):")
    for h in harnesses:
        print(f"  [OK] {h['harness_name']} | Type: {h['harness_type']} | Service: {h['service_name']} | Status: {h['status']}")
    
    # 2. Test Antigravity credentials from mcp_antigravity.json
    antigravity_id = "d16e5604-8c21-4ab3-af3b-a279e2af72f9"
    antigravity_token = "AtE7-CbCspLFwV5jG7137OC4wWRESRXcIaWxGpAh_bs"
    try:
        ag_harness = await authenticate_harness(antigravity_id, antigravity_token)
        print(f"\n  [✓] Antigravity Auth Test: SUCCESS (bound to {ag_harness['service_name']})")
    except Exception as ex:
        print(f"\n  [✗] Antigravity Auth Test: FAILED ({ex})")

    # 3. Test Codex credentials from mcp_codex.json
    codex_id = "ba8c8250-e34d-4e6b-aef7-feecd673de3e"
    codex_token = "uneF9kQpbY4kSeuSH-XnZKVKwJxueCLpcXxxNiLkv-4"
    try:
        cd_harness = await authenticate_harness(codex_id, codex_token)
        print(f"  [✓] Codex Auth Test:       SUCCESS (bound to {cd_harness['service_name']})")
    except Exception as ex:
        print(f"  [✗] Codex Auth Test:       FAILED ({ex})")

    print("\n--------------------------------------------------")
    print("Available MCP Tools for Agents:")
    tools = [
        "get_harness_identity",
        "discover_relevant_contracts",
        "publish_contract_revision",
        "retire_endpoint",
        "register_task",
        "checkpoint_task",
        "claim_compatibility_work",
        "record_compatibility_incident",
        "submit_compatibility_evidence",
        "get_cluster_verification",
    ]
    for t in tools:
        print(f"  • {t}")
    print("--------------------------------------------------")
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(verify_step3())
    finally:
        loop.close()
