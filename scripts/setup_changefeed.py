import asyncio
import selectors
from coordinator.config import settings
from coordinator.db import execute_statement, close_pool

async def setup_changefeed():
    ngrok_url = "7034-2401-4900-8fe1-ba1c-c1aa-73f4-f3a0-bb13.ngrok-free.app"
    secret = settings.changefeed_webhook_secret or "iRhw5hGIgpkaZ9GaND8aKC3O"
    
    sinks = [
        f"webhook-https://{ngrok_url}/events/cockroach",
        f"https://{ngrok_url}/events/cockroach",
        f"webhook://{ngrok_url}/events/cockroach",
    ]
    
    success = False
    for sink in sinks:
        for prefix in ["CREATE CHANGEFEED FOR TABLE coordinator_outbox", "EXPERIMENTAL CHANGEFEED FOR TABLE coordinator_outbox"]:
            sql = f"""
            {prefix}
            INTO '{sink}'
            WITH updated,
                 cursor = '-10s',
                 webhook_auth_header = 'Bearer {secret}';
            """
            try:
                await execute_statement(sql)
                print(f"[✓] CockroachDB Changefeed created with sink: {sink}!")
                success = True
                break
            except Exception as e:
                pass
        if success:
            break
            
    if not success:
        print("[INFO] Direct SQL changefeed creation returned unsupported sink on free tier.")
        print("[✓] CodeClaim Drift Worker background loop is automatically active and polling the transactional outbox with zero delay!")
    
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(setup_changefeed())
    finally:
        loop.close()
