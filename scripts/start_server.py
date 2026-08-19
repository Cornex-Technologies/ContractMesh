import sys
import asyncio
import selectors
import os
import uvicorn

if __name__ == "__main__":
    bind_host = os.getenv("COORDINATOR_BIND_HOST", "127.0.0.1")
    bind_port = int(os.getenv("COORDINATOR_PORT", "8000"))
    print(f"Starting CodeClaim Coordinator on http://{bind_host}:{bind_port} ...")
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.set_event_loop(loop)
    config = uvicorn.Config(
        "coordinator.app:app",
        host=bind_host,
        port=bind_port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    try:
        loop.run_until_complete(server.serve())
    finally:
        loop.close()
