from datetime import datetime
from typing import Literal, Optional, Union

from pydantic import BaseModel


# Specific payload models
class FlipAnomalyPayload(BaseModel):
    question: str
    change: Literal["NO_TO_YES", "YES_TO_NO"]
    actual_price: float
    slug: str


class FlipAnomaly(BaseModel):
    alert_id: str  # UUID
    sub_type: Literal["FLIP"]
    payload: FlipAnomalyPayload
    timestamp: datetime


class SuspectUserPayload(BaseModel):
    wallet: str
    total_earned: float
    positions: int

class SuspectTradePayload(BaseModel):
    wallet: str
    title: str
    size: float


class SuspectUserAnomaly(BaseModel):
    alert_id: str  # UUID
    sub_type: Literal["SUSPECT_USER"]
    payload: SuspectUserPayload
    timestamp: datetime

class SuspectTradeAnomaly(BaseModel):
    alert_id: str
    sub_type: Literal["SUSPECT_TRADE"]
    payload: SuspectTradePayload
    timestamp: datetime


class Notification(BaseModel):
    text: str
    image_path: Optional[str] = None
    # If image_path exists, it is the path to the image to send with the text. Otherwise, only text is sent.


# Add here the rest of the anomaly models:
Anomaly = Union[FlipAnomaly, SuspectUserAnomaly, SuspectTradeAnomaly]  # Add new anomaly types here
