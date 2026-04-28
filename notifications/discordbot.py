import niquests
from config import BASE_URL_DISCORD, BASE_DIR, DEFAULT_IMAGE
from models import Notification


def send_notification(notification: Notification) -> dict:
    """Sends text + image based on the Notification to a given Discord channel."""

    image_path = notification.image_path or DEFAULT_IMAGE
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
