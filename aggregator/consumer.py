import os
import json
import asyncio
import redis.asyncio as redis_async
import redis.exceptions
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from database import AsyncSessionLocal, engine, Base
from models import ProcessedEvent, AppStats

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "events_stream"
GROUP_NAME = "aggregator_group"
CONSUMER_NAME = os.getenv("HOSTNAME", "consumer_default") # docker sets hostname

async def init_db():
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            break
        except Exception as e:
            print(f"Waiting for DB... {e}")
            await asyncio.sleep(3)
        
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT id FROM app_stats WHERE id = 1"))
        if not result.fetchone():
            session.add(AppStats(id=1, received=0, unique_processed=0, duplicate_dropped=0))
            await session.commit()

async def ensure_group(redis_client):
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise

async def process_messages():
    await init_db()
    redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
    await ensure_group(redis_client)
    
    print(f"[{CONSUMER_NAME}] Started consuming from stream '{STREAM_NAME}'...")
    while True:
        try:
            # Block and read from stream
            messages = await redis_client.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000
            )
            if not messages:
                continue
                
            for stream, stream_messages in messages:
                for message_id, message_data in stream_messages:
                    await handle_message(message_data, redis_client, message_id)
        except Exception as e:
            print(f"Error consuming: {e}")
            await asyncio.sleep(2)

async def handle_message(data, redis_client, message_id):
    topic = data.get("topic")
    event_id = data.get("event_id")
    timestamp = data.get("timestamp")
    source = data.get("source")
    payload = json.loads(data.get("payload", "{}"))
    
    async with AsyncSessionLocal() as session:
        try:
            # Atomic transaction boundary starts here automatically in SQLAlchemy
            new_event = ProcessedEvent(
                topic=topic,
                event_id=event_id,
                timestamp=timestamp,
                source=source,
                payload=payload
            )
            session.add(new_event)
            await session.flush() # Will raise IntegrityError if UniqueConstraint fails
            
            # If we reach here, it's a unique processed event
            await session.execute(
                text("UPDATE app_stats SET unique_processed = unique_processed + 1 WHERE id = 1")
            )
            await session.commit()
            print(f"[{CONSUMER_NAME}] Processed {event_id}")
            
        except IntegrityError:
            # Duplicate detected! Rollback session and update duplicate_dropped
            await session.rollback()
            await session.execute(
                text("UPDATE app_stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1")
            )
            await session.commit()
            print(f"[{CONSUMER_NAME}] Dropped duplicate {event_id}")
            
        # Ack message in redis to remove from pending entries
        await redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

if __name__ == "__main__":
    asyncio.run(process_messages())
