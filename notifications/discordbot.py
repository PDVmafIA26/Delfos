import niquests
from config import BASE_URL_DISCORD, BASE_DIR, DEFAULT_IMAGE
from dataclasses import dataclass
from typing import Optional


@dataclass
class Notification:
    text: str
    image_path: Optional[str] = None
    # If image_path exists, it is the path to the image to send with the text. Otherwise, only text is sent.


def send_notification(notification: Notification) -> dict:
    """Sends text + image based on the Notification to a Discord Webhook."""

    image_path = notification.image_path or DEFAULT_IMAGE
    image = (BASE_DIR / image_path).resolve()

    with open(image, "rb") as img:
        # Discord espera la imagen en 'file' y el texto en 'content'
        resp = niquests.post(
            BASE_URL_DISCORD,
            data={
                "content": notification.text, 
            },
            files={
                "file": img 
            },
            timeout=20,
        )
    
    resp.raise_for_status()
    
    # Discord devuelve 204 No Content por defecto si todo va bien.
    if resp.status_code == 204:
        return {"status": "success", "message": "Enviado a Discord correctamente"}
    
    # Por si le añades ?wait=true a la URL del webhook (que sí devuelve un JSON)
    return resp.json() if resp.text else {"status": "unknown"}