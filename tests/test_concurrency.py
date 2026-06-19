import pytest
import httpx
import uuid
import asyncio
import datetime

BASE_URL = "http://localhost:8080"

@pytest.mark.asyncio
async def test_idempotency_and_concurrency():
    event_id = str(uuid.uuid4())
    payload = {
        "topic": "concurrency_test",
        "event_id": event_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {"value": 999}
    }

    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        # Wait for API to be available
        try:
            await client.get("/stats")
        except httpx.ConnectError:
            pytest.skip("API not running on localhost:8080. Start docker compose first.")

        # Get baseline stats
        stats_before = (await client.get("/stats")).json()
        
        # Publish the same event 20 times concurrently
        tasks = [client.post("/publish", json=payload) for _ in range(20)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 200
            
        # Give consumer time to process
        await asyncio.sleep(4)
        
        # Check stats
        stats_after = (await client.get("/stats")).json()
        
        # Ensure only 1 was processed and 19 were dropped
        assert stats_after["unique_processed"] - stats_before["unique_processed"] == 1
        assert stats_after["duplicate_dropped"] - stats_before["duplicate_dropped"] >= 19
        
        events_resp = await client.get(f"/events?topic=concurrency_test")
        events = events_resp.json()
        
        # Ensure exactly 1 event exists with this ID
        matching_events = [e for e in events if e["event_id"] == event_id]
        assert len(matching_events) == 1
