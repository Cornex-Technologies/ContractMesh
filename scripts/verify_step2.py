import asyncio
import selectors
from coordinator.db import fetch_all, close_pool

async def verify_step2():
    print("-----------------------------------------")
    print("CodeClaim Step 2: Mesh & Contracts Probe")
    print("-----------------------------------------")
    
    # 1. Probe Registered Microservices
    services = await fetch_all("SELECT service_name, repository_path, entrypoint_module, entrypoint_app FROM microservices ORDER BY service_name;")
    print(f"Registered Microservices ({len(services)}):")
    for s in services:
        print(f"  [OK] Service: {s['service_name']} | Path: {s['repository_path']} | App: {s.get('entrypoint_module', 'main')}:{s.get('entrypoint_app', 'app')}")
    
    # 2. Probe Service Contracts & Revisions
    contracts = await fetch_all("""
        SELECT c.service_name, c.endpoint_path, c.http_method, r.revision_number, r.semantic_summary, r.is_active
        FROM service_contracts c
        JOIN service_contract_revisions r ON c.contract_id = r.contract_id
        ORDER BY c.service_name, r.revision_number;
    """)
    print(f"\nPublished Contracts ({len(contracts)}):")
    for c in contracts:
        status = "ACTIVE" if c.get("is_active") else "INACTIVE"
        print(f"  [OK] {c['service_name']} {c['http_method']} {c['endpoint_path']} (v{c['revision_number']}) [{status}]")
    
    # 3. Probe Confirmed Dependencies
    deps = await fetch_all("""
        SELECT consumer_service, provider_service, assumed_provider_revision, endpoint_path, confirmation_status
        FROM http_interface_dependencies
        ORDER BY consumer_service;
    """)
    print(f"\nRegistered Dependencies ({len(deps)}):")
    if not deps:
        print("  [INFO] No dependencies registered yet (will be created during onboarding or first task).")
    else:
        for d in deps:
            print(f"  [OK] {d['consumer_service']} -> {d['provider_service']} (assumes v{d['assumed_provider_revision']}) [{d['confirmation_status']}]")
            
    print("-----------------------------------------")
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(verify_step2())
    finally:
        loop.close()
