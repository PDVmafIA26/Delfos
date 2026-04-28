import niquests
from config import BASE_URL_TELEGRAM, CHAT_ID_TELEGRAM, BASE_DIR, DEFAULT_IMAGE
from notifications.models import Notification


def send_notification(
    notification: Notification, chat_id: str = CHAT_ID_TELEGRAM
) -> dict:
    """Sends text + image based on the Notification to a given Telegram chat."""

    image_path = notification.image_path or DEFAULT_IMAGE
    image = (BASE_DIR / image_path).resolve()

    with open(image, "rb") as img:
        resp = niquests.post(
            f"{BASE_URL_TELEGRAM}/sendPhoto",
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
