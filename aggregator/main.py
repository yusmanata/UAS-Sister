import os
import json
from contextlib import asynccontextmanager
from typing import List, Union
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from database import engine, Base, get_db
from models import EventPayload, AppStats, ProcessedEvent
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "events_stream"

redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    # Init DB Schema
    import asyncio
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            break
        except Exception as e:
            print(f"Waiting for DB... {e}")
            await asyncio.sleep(3)
        
    # Seed stats row if not exists
    from database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(AppStats).where(AppStats.id == 1))
        stats = result.scalar_one_or_none()
        if not stats:
            session.add(AppStats(id=1, received=0, unique_processed=0, duplicate_dropped=0))
            await session.commit()
            
    # Init Redis Connection
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    yield
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/publish")
async def publish_event(events: Union[EventPayload, List[EventPayload]], db: AsyncSession = Depends(get_db)):
    if not isinstance(events, list):
        events = [events]
        
    for event in events:
        event_dict = event.model_dump()
        event_dict['timestamp'] = event_dict['timestamp'].isoformat()
        event_dict['payload'] = json.dumps(event_dict['payload'])
        
        # Publish to Redis Stream
        await redis_client.xadd(STREAM_NAME, event_dict)
        
    # Update received stat Atomically
    await db.execute(
        text("UPDATE app_stats SET received = received + :count WHERE id = 1"),
        {"count": len(events)}
    )
    await db.commit()
    
    return {"status": "accepted", "count": len(events)}

@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AppStats).where(AppStats.id == 1))
    stats = result.scalar_one_or_none()
    if not stats:
        return {"received": 0, "unique_processed": 0, "duplicate_dropped": 0, "topics": 0, "uptime": "N/A"}
        
    topics_count = await db.execute(text("SELECT COUNT(DISTINCT topic) FROM processed_events"))
    topics = topics_count.scalar()
    
    return {
        "received": stats.received,
        "unique_processed": stats.unique_processed,
        "duplicate_dropped": stats.duplicate_dropped,
        "topics": topics
    }

@app.get("/events")
async def get_events(topic: str = None, limit: int = 50, db: AsyncSession = Depends(get_db)):
    query = select(ProcessedEvent)
    if topic:
        query = query.where(ProcessedEvent.topic == topic)
    query = query.limit(limit)
    
    result = await db.execute(query)
    events = result.scalars().all()
    
    return [
        {
            "topic": e.topic,
            "event_id": e.event_id,
            "timestamp": e.timestamp,
            "source": e.source,
            "payload": e.payload
        }
        for e in events
    ]
