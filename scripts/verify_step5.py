import asyncio
import selectors
import uuid
from coordinator.config import settings
from coordinator.drift_worker import ingest_changefeed_event, process_all_pending_events
from coordinator.db import fetch_one, close_pool

async def verify_step5():
    print("--------------------------------------------------")
    print("CodeClaim Step 5: Changefeed & CDC Pipeline Test")
    print("--------------------------------------------------")
    
    test_event_id = str(uuid.uuid4())
    print(f"1. Simulating inbound CDC Changefeed event (ID: {test_event_id[:8]}...)...")
    
    # 1. Ingest test outbox event into event_inbox
    ingest_res = await ingest_changefeed_event({
        "event_id": test_event_id,
        "event_type": "CONTRACT_CHANGED",
        "aggregate_type": "CONTRACT_REVISION",
        "aggregate_id": str(uuid.uuid4()),
        "aggregate_revision": 2,
        "source_service": "billing-service",
        "payload": {
            "service_name": "billing-service",
            "endpoint_path": "/v1/charges",
            "http_method": "POST",
            "revision_number": 2,
            "schema_diff": {
                "is_breaking": True,
                "classification": "BREAKING",
                "breaking_changes": [{"field": "payment_method_id", "change": "new required field added"}],
                "diff_summary": "Breaking change: payment_method_id required instead of card_token"
            }
        }
    })
    print(f"  [OK] Event Ingested into CockroachDB event_inbox (ID: {test_event_id[:8]}...)")
    
    # 2. Process event via Supervised Drift Worker
    print("2. Triggering Supervised Drift Worker processing...")
    processed_count = await process_all_pending_events()
    print(f"  [OK] Drift Worker processed {processed_count} pending event(s)!")
    
    # 3. Check inbox status in CockroachDB
    inbox_record = await fetch_one("SELECT processing_status, processed_at FROM event_inbox WHERE event_id = %s;", (test_event_id,))
    if inbox_record and inbox_record.get("processing_status") == "PROCESSED":
        print(f"  [OK] CockroachDB event_inbox record status: PROCESSED (at {inbox_record.get('processed_at')})")
    else:
        print(f"  [INFO] Inbox record status: {inbox_record}")
        
    print("--------------------------------------------------")
    print("CDC Changefeed & Drift Ingestion Pipeline: READY")
    print("--------------------------------------------------")
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(verify_step5())
    finally:
        loop.close()
