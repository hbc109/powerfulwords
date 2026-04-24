from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class NarrativeEvent(BaseModel):
    event_id: str
    event_time: datetime
    commodity: str = "crude_oil"
    theme: Optional[str] = None
    topic: str
    direction: str
    source_bucket: str
    source_name: str
    source_id: Optional[str] = None
    document_id: Optional[str] = None
    chunk_id: Optional[str] = None
    credibility: float = Field(ge=0.0, le=1.0)
    novelty: float = Field(ge=0.0, le=1.0)
    breadth: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    persistence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    crowding: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    price_confirmation: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    verification_status: str
    horizon: str
    rumor_flag: bool = False
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    entities: List[str] = []
    regions: List[str] = []
    asset_candidates: List[str] = []
    evidence_text: str
    evidence_spans: List[str] = []
    notes: str = ""
