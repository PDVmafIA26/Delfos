import niquests

from config import (
    BASE_DIR,
    BASE_URL_DISCORD,
    BASE_URL_TELEGRAM,
    CHAT_ID_TELEGRAM,
    DEFAULT_IMAGES,
)
from models import Notification


def send_notification_telegram(
    notification: Notification, chat_id: str = CHAT_ID_TELEGRAM
) -> dict:
    """Sends text + image based on the Notification to a given Telegram chat."""

    text = notification.text.replace("_", "\\_")  # Escape underscores for Markdown
    image_path = notification.image_path or DEFAULT_IMAGES[0]
    image = (BASE_DIR / image_path).resolve()

    with open(image, "rb") as img:
        resp = niquests.post(
            f"{BASE_URL_TELEGRAM}/sendPhoto",
            data={
                "chat_id": chat_id,
                "caption": text,
                "parse_mode": "Markdown",
            },
            files={"photo": img},
            timeout=20,
        )
    resp.raise_for_status()
    return resp.json()


def send_notification_discord(notification: Notification) -> dict:
    """Sends text + image based on the Notification to a given Discord channel."""

    image_path = notification.image_path or DEFAULT_IMAGES[0]
    image = (BASE_DIR / image_path).resolve()

    with open(image, "rb") as img:
        resp = niquests.post(
            BASE_URL_DISCORD,
            data={
                "content": notification.text,
            },
            files={"file": img},
            timeout=20,
        )

    resp.raise_for_status()
    return resp.json()
