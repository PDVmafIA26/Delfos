from typing import Callable, Dict
from config import POLIMARKET_BASE_URL, DEFAULT_IMAGES
from models import Anomaly, FlipAnomaly, Notification


def format_flip_notification(data: FlipAnomaly) -> Notification:
    """Handler for FLIP anomalies. Returns a Notification object with formatted text."""
    text = (
        f"🚨 *Flip Anomaly Detected!*\n\n"
        f"🤵🏼‍♂️ *{data.payload.question}*\n"
        f"🔄 Change: {data.payload.change}\n"
        f"🔗 {POLIMARKET_BASE_URL}/event/{data.payload.slug}\n"
    )
    return Notification(text=text, image_path=DEFAULT_IMAGES.get("FLIP"))


HANDLERS: Dict[str, Callable[[Anomaly], Notification]] = {
    "FLIP": format_flip_notification,
    # "SPIKE": format_spike,etc.
}


def get_notification_message(anomaly: Anomaly) -> Notification:
    sub_type_handler = HANDLERS.get(anomaly.sub_type)

    if not sub_type_handler:
        raise ValueError(
            f"Tipo de anomalía (sub_type) no soportado: '{anomaly.sub_type}'. "
            f"Tipos soportados: {list(HANDLERS.keys())}"
        )

    return sub_type_handler(anomaly)
