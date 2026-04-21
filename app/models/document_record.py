from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class DocumentRecord(BaseModel):
    document_id: str
    source_id: str
    source_bucket: str
    file_path: Optional[str] = None
    title: Optional[str] = None
    source_name: str
    publisher_or_channel: Optional[str] = None
    language: Optional[str] = None
    region: Optional[str] = None
    commodity: str = "crude_oil"
    subtheme: Optional[str] = None
    access_mode: Optional[str] = None
    cost_level: Optional[str] = None
    rights_note: Optional[str] = None
    published_at: Optional[datetime] = None
    checksum: Optional[str] = None
    quality_tier: Optional[int] = None
    rumor_flag: bool = False
    verification_status: str = "unverified"
    raw_text: str
