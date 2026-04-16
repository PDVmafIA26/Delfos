import json
import logging
import time
import websocket
from kafka import KafkaProducer
from markets import get_markets_info

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC = "polymarket_raw_events"
INTERESTING_EVENT_TYPES = ["price_change", "book", "last_trade_price", "best_bid_ask", "market_resolved"]

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka Producer started successfully.")
except Exception as e:
    logger.error(f"Error starting Kafka Producer: {e}")
    producer = None

# Main Polymarket Categories (Slugs) for Event Classification
POLYMARKET_MAIN_CATEGORIES = [
    "politics",
    "crypto",
    "sports",
    "business",
    "science",
    "pop-culture",
    "tech"
]


def get_categories_for_message(message_data):
    """
    Determines which category or categories the event/market belongs to.
    
    If the 'message_data' (payload) already brings the categories from prior extraction, it uses them. Otherwise, it extracts them 
    based on the payload structure.
    """
    # If the message payload already includes pre-processed categories
    if "categories" in message_data:
        return [cat for cat in message_data["categories"] if cat.lower() in POLYMARKET_MAIN_CATEGORIES]
    
    # If it's a single category attached by the pipeline
    if "category" in message_data:
        cat = message_data["category"].lower()
        if cat in POLYMARKET_MAIN_CATEGORIES:
            return [cat]

    # Fallback: If we are tracking a specific category stream in this WebSocket, 
    # default to the tracked category slug (e.g., "tech")
    # This ensures no message is lost in the Bronze layer.
    return ["tech"]


def process_and_send_to_kafka(message_data):
    """
    Processes a message received via WebSocket and publishes it to Kafka.
    
    It fulfills the data lake architectural requirements by:
    - Sending only the category in the message header, leaving the rest (id, info) in the body.
    - If the message belongs to multiple categories, it sends an independent message for each one.
    """
    if not producer:
        logger.warning("Kafka producer is not available. Message not sent.")
        return

    categories = get_categories_for_message(message_data)
    
    if not categories:
        logger.warning("No valid categories found for this message. Discarding to protect Bronze Layer structure.")
        return

    # Send a duplicate message for each category to ensure correct folder partitioning in the Data Lake
    for category in categories:
        # Kafka headers are sent as a list of tuples: [('key', b'value_in_bytes')]
        headers = [
            ("category", category.encode('utf-8'))
        ]
        
        try:
            # Send the complete payload (the event/market containing all data)
            producer.send(
                KAFKA_TOPIC, 
                value=message_data, 
                headers=headers
            )
            logger.info(f"Message sent to Kafka -> Category (Header): {category}")
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            
    # Flush the buffer to ensure immediate delivery
    producer.flush()


def on_message(ws, message):
    logger.info("Message received from Polymarket WebSocket")
    try:
        data = json.loads(message)

        event_type = data.get("event_type")
        
        if event_type == "market_resolved":            
            # 1. Desuscribirse para limpiar el feed
            resolved_tokens = data.get("assets_ids", [])
            
            if resolved_tokens:
                unsubscribe_message = {
                    "operation": "unsubscribe", # La palabra mágica según la API
                    "assets_ids": resolved_tokens
                }
                # Enviamos la orden al servidor de Polymarket a través del socket
                ws.send(json.dumps(unsubscribe_message))
                logger.info(f"Desuscrito automáticamente de los tokens finalizados: {resolved_tokens}")

        # Process data and send to Kafka
        if event_type in INTERESTING_EVENT_TYPES:
            process_and_send_to_kafka(data)

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
        "custom_feature_enabled": True
    }
    ws.send(json.dumps(subscribe_message))


def run_websocket(assets_ids):
    # Polymarket WebSocket URL (CLOB API example)
    websocket_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market" 
    
    # Infinite automatic reconnection system in case of socket disconnection
    while True:
        logger.info(f"Connecting to {websocket_url}...")
        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        ws.run_forever(ping_interval=10, ping_timeout=5) # Ping-Pong system required by Polymarket
        logger.info("Waiting 5 seconds before reconnecting...")
        time.sleep(5)


if __name__ == "__main__":
    assets_ids = [token for market in get_markets_info("tech").values() for token in market]
    run_websocket(assets_ids)
