import json

import niquests
from config import (
    BASE_URL_DISCORD,
    BASE_URL_TELEGRAM,
    CHAT_ID_TELEGRAM,
    BASE_DIR,
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

# TODO: si no hay una lista de imagenes, llamar a send_notification_telegram normal.
def send_notification_telegram_media_group(
    notification: Notification, chat_id: str = CHAT_ID_TELEGRAM
) -> dict:
    text = notification.text.replace("_", "\\_")
    
    # Supongamos que notification.image_paths es ahora una lista
    if hasattr(notification, 'image_paths') and notification.image_paths:
        image_paths = notification.image_paths
    else:
        image_paths = list(DEFAULT_IMAGES.values())
    
    media = []
    files = {}
    
    for i, path in enumerate(image_paths[:10]):  # Máximo 10 imágenes
        file_key = f"img{i}"
        full_path = (BASE_DIR / path).resolve()
        
        # El primer elemento lleva el caption (el texto del mensaje)
        media_item = {
            "type": "photo",
            "media": f"attach://{file_key}"
        }
        if i == 0:
            media_item["caption"] = text
            media_item["parse_mode"] = "Markdown"
            
        media.append(media_item)
        files[file_key] = open(full_path, "rb")

    try:
        resp = niquests.post(
            f"{BASE_URL_TELEGRAM}/sendMediaGroup",
            data={
                "chat_id": chat_id,
                "media": json.dumps(media)
            },
            files=files,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        # Siempre cierra los archivos después de la petición
        for f in files.values():
            f.close()

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
