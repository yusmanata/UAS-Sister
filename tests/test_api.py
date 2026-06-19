import pytest
import httpx
import uuid
import datetime
import asyncio

BASE_URL = "http://localhost:8080"

@pytest.mark.asyncio
async def test_publish_validation():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        # Invalid schema (missing fields)
        try:
            response = await client.post("/publish", json={"topic": "test"})
            assert response.status_code == 422 # Unprocessable Entity
        except httpx.ConnectError:
            pytest.skip("API not running on localhost:8080. Start docker compose first.")
        
        # Valid schema
        payload = {
            "topic": "test",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {"key": "value"}
        }
        response = await client.post("/publish", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"

@pytest.mark.asyncio
async def test_events_endpoint():
    topic_name = f"topic-{uuid.uuid4()}"
    event_id = str(uuid.uuid4())
    payload = {
        "topic": topic_name,
        "event_id": event_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {"data": 123}
    }
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            await client.post("/publish", json=payload)
        except httpx.ConnectError:
            pytest.skip("API not running on localhost:8080. Start docker compose first.")
            
        # Give consumer time to process
        await asyncio.sleep(2)
        
        response = await client.get(f"/events?topic={topic_name}")
        assert response.status_code == 200
        events = response.json()
        
        # Should have exactly 1 event
        assert len(events) == 1
        assert events[0]["event_id"] == event_id
        assert events[0]["payload"]["data"] == 123
