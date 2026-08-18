import sys
import asyncio
import selectors
import uvicorn

if __name__ == "__main__":
    print("Starting CodeClaim Coordinator on http://127.0.0.1:8000 ...")
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.set_event_loop(loop)
    config = uvicorn.Config(
        "coordinator.app:app",
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )
    server = uvicorn.Server(config)
    try:
        loop.run_until_complete(server.serve())
    finally:
        loop.close()
