from typing import Callable, Dict

from config import DEFAULT_IMAGES, POLIMARKET_BASE_URL
from models import Anomaly, FlipAnomaly, SuspectUserAnomaly, Notification, SuspectTradeAnomaly
from urllib.parse import urlparse

def is_valid_url(url: str) -> bool:
    """Checks if a given string is a structurally valid HTTP/HTTPS URL."""
    
    # Return False immediately if the input string is empty or None
    if not url:
        return False
        
    try:
        # Attempt to parse the URL string into its components (scheme, domain, path, etc.)
        result = urlparse(url)
        
        # Ensure the URL has both a valid scheme (http or https) and a valid domain/network location (netloc)
        return all([result.scheme in ["http", "https"], result.netloc])
        
    except ValueError:
        # Catch any parsing errors from severely malformed strings and flag them as invalid
        return False


def format_flip_notification(data: FlipAnomaly) -> Notification:
    """Handler for FLIP anomalies. Returns a Notification object with formatted text."""
    text = (
        f"🚨 *Flip Anomaly Detected!*\n\n"
        f"🤵🏼‍♂️ *{data.payload.question}*\n"
        f"🔄 Change: {data.payload.change}\n"
        f"🔗 {POLIMARKET_BASE_URL}/event/{data.payload.slug}\n"
    )
    
    image = data.payload.image_path

    if not is_valid_url(image):
        image = DEFAULT_IMAGES.get("FLIP")
        is_url = False
    else:
        is_url = True

    return Notification(text=text, image_path=image, is_url=is_url)


def format_suspect_user_notification(data: SuspectUserAnomaly) -> Notification:
    """Handler for SUSPECT_USER anomalies. Returns a Notification object with formatted text."""
    text = (
        f"🚨 *Suspect User Anomaly Detected!*\n\n"
        f"🤵🏼‍♂️ *{data.payload.wallet}*\n"
        f"🔄 Total Earned: {data.payload.total_earned}\n"
        f"💼 Positions: {data.payload.positions}\n"
    )

    image = data.payload.image_path

    if not is_valid_url(image):
        image = DEFAULT_IMAGES.get("SUSPECT_USER")
        is_url = False
    else:
        is_url = True

    return Notification(text=text, image_path=image, is_url=is_url)

def format_suspect_trade_notification(data: SuspectTradeAnomaly) -> Notification:
    """Handler for SUSPECT_USER anomalies. Returns a Notification object with formatted text."""
    text = (
        f"🚨 *Suspect Trade Anomaly Detected!*\n\n"
        f"🤵🏼‍♂️ *{data.payload.wallet}*\n"
        f"🔄 A suspicious user has placed a bet on the market called: {data.payload.title}\n"
        f"💼 He has bet a total of: {data.payload.size} $\n"
    )
    
    image = data.payload.image_path

    if not is_valid_url(image):
        image = DEFAULT_IMAGES.get("SUSPECT_USER")
        is_url = False
    else:
        is_url = True

    return Notification(text=text, image_path=image, is_url=is_url)


HANDLERS: Dict[str, Callable[[Anomaly], Notification]] = {
    "FLIP": format_flip_notification,
    "SUSPECT_USER": format_suspect_user_notification,
    "SUSPECT_TRADE": format_suspect_trade_notification
}


def get_notification_message(anomaly: Anomaly) -> Notification:
    sub_type_handler = HANDLERS.get(anomaly.sub_type)

    if not sub_type_handler:
        raise ValueError(
            f"Tipo de anomalía (sub_type) no soportado: '{anomaly.sub_type}'. "
            f"Tipos soportados: {list(HANDLERS.keys())}"
        )

    return sub_type_handler(anomaly)
