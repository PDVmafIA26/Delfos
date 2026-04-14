import niquests
from config import BASE_URL, CHAT_ID, BASE_DIR, DEFAULT_IMAGE
from dataclasses import dataclass
from typing import Optional


@dataclass
class Notification:
    text: str
    image_path: Optional[str] = None
    # If image_path exists, it is the path to the image to send with the text. Otherwise, only text is sent.


def send_notification(notification: Notification, chat_id: str = CHAT_ID) -> dict:
    """Sends text + image based on the Notification."""

    image_path = notification.image_path or DEFAULT_IMAGE
    image = (BASE_DIR / image_path).resolve()

    with open(image, "rb") as img:
        # "rb" mode is required to read the image as binary for uploading, instead of trying to decode it as text.
        resp = niquests.post(
            f"{BASE_URL}/sendPhoto",
            data={
                "chat_id": chat_id,
                "caption": notification.text,
                "parse_mode": "Markdown",
            },
            files={"photo": img},
            timeout=20,
        )
    resp.raise_for_status()
    return resp.json()
