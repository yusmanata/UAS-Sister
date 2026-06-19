import pytest
import httpx
import uuid
import datetime
import asyncio

BASE_URL = "http://localhost:8080"

@pytest.fixture
def base_payload():
    return {
        "topic": "extra_tests",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {"key": "value"}
    }

@pytest.mark.asyncio
async def test_publish_single_dict(base_payload):
    # Test accepting a single dict instead of a list
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.post("/publish", json=base_payload)
        except httpx.ConnectError:
            pytest.skip("API not running on localhost:8080. Start docker compose first.")
        assert response.status_code == 200
        assert response.json()["count"] == 1

@pytest.mark.asyncio
async def test_publish_missing_field(base_payload):
    del base_payload["topic"]
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.post("/publish", json=base_payload)
        except httpx.ConnectError:
            pytest.skip("API not running")
        assert response.status_code == 422

@pytest.mark.asyncio
async def test_publish_invalid_timestamp(base_payload):
    base_payload["timestamp"] = "invalid-date"
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.post("/publish", json=base_payload)
        except httpx.ConnectError:
            pytest.skip("API not running")
        assert response.status_code == 422

@pytest.mark.asyncio
async def test_get_events_limit(base_payload):
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            for _ in range(3):
                p = base_payload.copy()
                p["event_id"] = str(uuid.uuid4())
                await client.post("/publish", json=p)
        except httpx.ConnectError:
            pytest.skip("API not running")
        
        await asyncio.sleep(2)
        response = await client.get("/events?limit=2")
        assert response.status_code == 200
        assert len(response.json()) <= 2

@pytest.mark.asyncio
async def test_stats_structure():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.get("/stats")
        except httpx.ConnectError:
            pytest.skip("API not running")
        assert response.status_code == 200
        data = response.json()
        assert "received" in data
        assert "unique_processed" in data
        assert "duplicate_dropped" in data
        assert "topics" in data

@pytest.mark.asyncio
async def test_method_not_allowed():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            response = await client.get("/publish")
        except httpx.ConnectError:
            pytest.skip("API not running")
        assert response.status_code == 405

@pytest.mark.asyncio
async def test_events_filtering():
    topic_a = f"topic-a-{uuid.uuid4()}"
    topic_b = f"topic-b-{uuid.uuid4()}"
    
    pa = {"topic": topic_a, "event_id": str(uuid.uuid4()), "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(), "source": "pytest", "payload": {}}
    pb = {"topic": topic_b, "event_id": str(uuid.uuid4()), "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(), "source": "pytest", "payload": {}}
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        try:
            await client.post("/publish", json=[pa, pb])
        except httpx.ConnectError:
            pytest.skip("API not running")
            
        await asyncio.sleep(2)
        
        res_a = await client.get(f"/events?topic={topic_a}")
        assert res_a.status_code == 200
        assert len(res_a.json()) == 1
        assert res_a.json()[0]["topic"] == topic_a
