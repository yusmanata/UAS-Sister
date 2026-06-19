import pytest
import httpx
import uuid
import datetime
import time
import asyncio

BASE_URL = "http://localhost:8080"

@pytest.mark.asyncio
async def test_duplicate_processing_stats():
    event_id = str(uuid.uuid4())
    payload = {
        "topic": "dup_stats_test",
        "event_id": event_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "test",
        "payload": {}
    }
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            stats_before = (await client.get("/stats")).json()
        except httpx.ConnectError:
            pytest.skip("API not running")
            
        # Send same event twice
        await client.post("/publish", json=payload)
        await client.post("/publish", json=payload)
        
        await asyncio.sleep(3)
        
        stats_after = (await client.get("/stats")).json()
        
        assert stats_after["received"] - stats_before["received"] == 2
        assert stats_after["unique_processed"] - stats_before["unique_processed"] == 1
        assert stats_after["duplicate_dropped"] - stats_before["duplicate_dropped"] == 1

@pytest.mark.asyncio
async def test_z_stress_small_batch():
    batch = []
    for _ in range(500):
        batch.append({
            "topic": "stress_topic",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "stress_test",
            "payload": {"data": "x" * 100}
        })
        
    start_time = time.time()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.post("/publish", json=batch)
        except httpx.ConnectError:
            pytest.skip("API not running")
            
        assert response.status_code == 200
        
    execution_time = time.time() - start_time
    assert execution_time < 5.0  # Should be fast to enqueue to Redis
