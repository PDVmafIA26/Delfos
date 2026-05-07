import json
import threading
import time
import requests
import websocket

from logger import get_logger
from markets import get_markets_info
from kafka_manager import get_producer

log = get_logger(__name__)


def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        log.error("Received message is not valid JSON: %.200s", message)
        return
    except Exception as e:
        log.exception("Unexpected error parsing WebSocket message: %s", e)
        return

    event_type = data.get("event_type")

    # Handling unsubscriptions
    if event_type == "market_resolved":
        resolved_tokens = data.get("assets_ids", [])
        if resolved_tokens:
            ws.send(
                json.dumps(
                    {
                        "operation": "unsubscribe",
                        "assets_ids": resolved_tokens,
                    }
                )
            )
            log.info("Automatically unsubscribed from resolved tokens: %s", resolved_tokens)

    try:
        producer = get_producer()
        if producer:
            producer.send_data(
                topic="websockets",
                data=data,
                key=event_type,
                headers=[
                    ("event_type", event_type),
                ],
            )
            log.debug("Message sent to Kafka | event_type=%s", event_type)

    except Exception as e:
        log.error("Error sending WebSocket message to Kafka: %s", e)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket closed | status=%s msg=%s", close_status_code, close_msg)


def on_open(ws, assets_ids):
    log.info("WebSocket connected. Sending subscription for %d assets...", len(assets_ids))
    subscribe_message = {
        "assets_ids": assets_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }
    ws.send(json.dumps(subscribe_message))


def run_websocket(assets_ids, stop_event: threading.Event = None):
    websocket_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    while not (stop_event and stop_event.is_set()):
        log.info("Connecting to WebSocket: %s", websocket_url)
        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=lambda ws: on_open(ws, assets_ids),
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        stop_event.ws = ws
        ws.run_forever(ping_interval=10, ping_timeout=5)

        if stop_event and stop_event.is_set():
            break

        log.info("WebSocket disconnected. Reconnecting in 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    http_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    assets_ids = [
        token
        for market in get_markets_info(
            session=http_session, tag_slug="tech", ids_categories_exclude=""
        ).values()
        for token in market
    ]
    run_websocket(assets_ids)
