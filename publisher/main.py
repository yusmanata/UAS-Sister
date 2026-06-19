import httpx
import asyncio
import os
import random
from datetime import datetime, timezone
import uuid

TARGET_URL = os.getenv("TARGET_URL", "http://localhost:8080/publish")

async def spammer():
    print(f"Publisher started. Target: {TARGET_URL}")
    async with httpx.AsyncClient() as client:
        # Wait for the aggregator to be up
        while True:
            try:
                res = await client.get(TARGET_URL.replace("/publish", "/stats"))
                if res.status_code == 200:
                    break
            except Exception:
                pass
            print("Waiting for aggregator to be ready...")
            await asyncio.sleep(2)

        print("Aggregator is up. Starting spam...")
        
        while True:
            # Generate a batch of 50 unique events
            events = []
            for i in range(50):
                events.append({
                    "topic": "sensor-data",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "simulator",
                    "payload": {"value": random.randint(1, 100)}
                })
            
            # Deliberately add 15 duplicates from the generated list
            duplicates = random.choices(events, k=15)
            batch = events + duplicates # 65 items total, 50 unique, 15 dupes
            
            # Send batch
            try:
                response = await client.post(TARGET_URL, json=batch)
                print(f"Sent batch of {len(batch)} events. Status: {response.status_code}")
            except Exception as e:
                print(f"Error sending batch: {e}")
                
            await asyncio.sleep(2) # Send a batch every 2 seconds

if __name__ == "__main__":
    asyncio.run(spammer())
