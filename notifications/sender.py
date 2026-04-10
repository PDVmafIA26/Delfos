import niquests
from config import BASE_URL, CHAT_ID, BASE_DIR
from templates import Notification


def send_notification(notification: Notification, chat_id: str = CHAT_ID) -> dict:
    """Sends either text only or text + image based on the Notification."""

    if notification.image_path:
        image = (BASE_DIR / notification.image_path).resolve()
        if image.exists():
            return _send_photo(
                chat_id=chat_id,
                caption=notification.text,
                image_path=image,
            )
        else:
            print(f"Image not found. Tried path: {image}")
    return _send_text(chat_id=chat_id, text=notification.text)


def _send_text(chat_id: str, text: str) -> dict:
    resp = niquests.post(
        f"{BASE_URL}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _send_photo(chat_id: str, caption: str, image_path: str) -> dict:
    with open(image_path, "rb") as img:
        # "rb" mode is required to read the image as binary for uploading, instead of trying to decode it as text.
        resp = niquests.post(
            f"{BASE_URL}/sendPhoto",
            data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "Markdown",
            },
            files={"photo": img},
            timeout=20,
        )
    resp.raise_for_status()
    return resp.json()
