from sqlalchemy import Column, String, JSON, Integer, UniqueConstraint
from pydantic import BaseModel, Field
from datetime import datetime
from database import Base

# --- SQLAlchemy Models ---

class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, index=True, nullable=False)
    event_id = Column(String, index=True, nullable=False)
    timestamp = Column(String, nullable=False)
    source = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('topic', 'event_id', name='uq_topic_event_id'),
    )

class AppStats(Base):
    __tablename__ = "app_stats"
    id = Column(Integer, primary_key=True, index=True)
    received = Column(Integer, default=0)
    unique_processed = Column(Integer, default=0)
    duplicate_dropped = Column(Integer, default=0)

# --- Pydantic Models ---

class EventPayload(BaseModel):
    topic: str = Field(..., description="Topic of the event")
    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(..., description="ISO8601 Timestamp")
    source: str = Field(..., description="Source of the event")
    payload: dict = Field(..., description="Arbitrary JSON payload")
