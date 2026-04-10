from dataclasses import dataclass
from typing import Optional


@dataclass
class Notification:
    text: str
    image_path: Optional[str] = None
    # If image_path exists, it is the path to the image to send with the text. Otherwise, only text is sent.


"""
TEMPLATES is a dictionary that maps a topic (like "politics", "economics", etc.) to a lambda function that takes a data dictionary and returns a Notification object.
Each function formats the text according to the topic and optionally includes an image path if provided in the data.
"""
TEMPLATES = {
    "politics": lambda data: Notification(
        text=(
            f"🤵🏼‍♂️ *{data['title']}*\n\n"
            f"{data['summary']}\n\n"
            f"Details: {data['details']}"
        ),
        image_path=data.get("image"),
    ),
    "economics": lambda data: Notification(
        text=(
            f"📈 *{data['title']}*\n\n"
            f"{data['summary']}\n\n"
            f"Details: {data['details']}"
        ),
        image_path=data.get("image"),
    ),
    "technology": lambda data: Notification(
        text=(
            f"💻 *{data['title']}*\n\n"
            f"{data['summary']}\n\n"
            f"Details: {data['details']}"
        ),
        image_path=data.get("image"),
    ),
    "finance-commodities": lambda data: Notification(
        text=(
            f"💰 *{data['title']}*\n\n"
            f"{data['summary']}\n\n"
            f"Details: {data['details']}"
        ),
        image_path=data.get("image"),
    ),
}


def get_template(topic: str, data: dict) -> Notification:
    """Returns a Notification object based on the given topic and data."""
    if topic not in TEMPLATES:
        raise ValueError(
            f"Topic not found: '{topic}'. "
            f"Available topics: {list(TEMPLATES.keys())}"
        )
    return TEMPLATES[topic](data)
