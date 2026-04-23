import json
import logging
import threading
import time
import requests
import websocket
from markets import get_markets_info
from kafka_manager import get_producer

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

INTERESTING_EVENT_TYPES = [
    "price_change",
    "book",
    "last_trade_price",
    "best_bid_ask",
    "market_resolved",
]


def on_message(ws, message):
    logger.info("Message received from Polymarket WebSocket")
    try:
        data = json.loads(message)
        event_type = data.get("event_type")

        if event_type == "market_resolved":
            # Unsubscribe from resolved tokens to keep the feed clean
            resolved_tokens = data.get("assets_ids", [])

            if resolved_tokens:
                unsubscribe_message = {
                    "operation": "unsubscribe",
                    "assets_ids": resolved_tokens,
                }
                # Send unsubscribe order to Polymarket through the socket
                ws.send(json.dumps(unsubscribe_message))
                logger.info(
                    f"Automatically unsubscribed from resolved tokens: {resolved_tokens}"
                )

        # Forward relevant events to Kafka
        if event_type in INTERESTING_EVENT_TYPES:
            try:
                market_id = data.get("market_id")
                producer = get_producer()
                if producer:
                    get_producer().send_data(
                        topic="websockets",
                        data=data,
                        key=str(market_id) if market_id else None,
                    )
                    logger.info(f"Message sent to Kafka")
            except Exception as e:
                logger.error(f"Error sending message to Kafka: {e}")

    except json.JSONDecodeError:
        logger.error("Received message is not a valid JSON")
    except Exception as e:
        logger.error(f"Error processing the message: {e}")


def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning("### WebSocket closed ###")


def on_open(ws, assets_ids):
    logger.info("WebSocket connected. Sending subscription payload...")

    subscribe_message = {
        "assets_ids": assets_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }
    ws.send(json.dumps(subscribe_message))


def run_websocket(assets_ids, stop_event: threading.Event = None):
    # Polymarket WebSocket URL
    websocket_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Automatic reconnection loop — exits cleanly if stop_event is set
    while not (stop_event and stop_event.is_set()):
        logger.info(f"Connecting to {websocket_url}...")
        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=lambda ws: on_open(ws, assets_ids),
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        # Store ws reference so it can be closed externally
        stop_event.ws = ws
        
        # Ping-Pong keepalive required by Polymarket
        ws.run_forever(ping_interval=10, ping_timeout=5)
        
        if stop_event and stop_event.is_set():
            break

        logger.info("Waiting 5 seconds before reconnecting...")
        time.sleep(5)


if __name__ == "__main__":

    # Local test entry point
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
