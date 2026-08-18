import asyncio
import selectors
from coordinator.db import check_health, get_applied_migrations, close_pool

async def verify_db():
    health = await check_health()
    migrations = await get_applied_migrations()
    print("-----------------------------------------")
    print("Database Health:         ", health.get("status"))
    print("Cluster Latency:         ", str(health.get("latency_ms")) + " ms")
    print("Total Migrations Applied:", len(migrations))
    print("-----------------------------------------")
    for m in migrations:
        version = m.get("version")
        name = m.get("name")
        print(f"  [OK] Migration {version}: {name}")
    print("-----------------------------------------")
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(verify_db())
    finally:
        loop.close()
