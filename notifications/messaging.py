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

    if notification.is_url:
        # Case in which you receive the URL of an image
        try:
            # Attempt to send the image by passing the URL directly to Telegram's API
            resp = niquests.post(
                f"{BASE_URL_TELEGRAM}/sendPhoto",
                data={
                    "chat_id": chat_id,
                    "caption": text,
                    "parse_mode": "Markdown",
                    "photo": notification.image_path,
                },
                timeout=20,
            )
            resp.raise_for_status() 

        except Exception as e:
            # Case in which Telegram does not accept that type of image (example: .webp)
            print(f"[WARN] Telegram rechazó la URL de la imagen ({e}). Activando PLAN B...")
            
            # Fetch the image data directly from the URL into the server's RAM
            img_response = niquests.get(notification.image_path, timeout=10)
            
            if img_response.status_code == 200:
                # If download is successful, send the raw binary data to Telegram, forcing the .jpg extension
                resp = niquests.post(
                    f"{BASE_URL_TELEGRAM}/sendPhoto",
                    data={
                        "chat_id": chat_id,
                        "caption": text,
                        "parse_mode": "Markdown",
                    },
                    files={"photo": ("image.jpg", img_response.content)},
                    timeout=20,
                )
            else:
                # Case in which the image download fails
                print("[ERROR] No se pudo descargar la imagen. Enviando imagen predeterminada.")
                
                # Fallback: Open and send a default local image if the server couldn't download the original one
                with open(DEFAULT_IMAGES.get("FLIP"), "rb") as img: # Ahora mismo está puesto esta imagen por poner, habría q poner otra más conveniente :)
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
                # Validate that the fallback request (Plan B or Plan C) was successfully processed by Telegram
                resp.raise_for_status()
    else:
        # Process the request normally if the notification was already configured to use a local file
        with open(notification.image_path, "rb") as img:
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

    if notification.is_url:
        # Send notification to Discord using the image URL inside a rich embed format
        resp = niquests.post(
            BASE_URL_DISCORD,
            json={
                "content": notification.text,
                "embeds": [
                    {
                        "image": {"url": notification.image_path}
                    }
                ]
            },
            timeout=20,
        )
    else:
        # Upload a local image file directly to the Discord webhook using multipart/form-data
        with open(notification.image_path, "rb") as img:
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
