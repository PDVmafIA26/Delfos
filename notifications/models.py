from datetime import datetime
from typing import Literal, Optional  # Union
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


class Notification(BaseModel):
    text: str
    image_path: Optional[str] = None
    # If image_path exists, it is the path to the image to send with the text. Otherwise, only text is sent.


# Add here the rest of the anomaly models:
Anomaly = FlipAnomaly  # AnomalyType = Union[FlipAnomaly, WhaleMovementAnomaly, etc.]
