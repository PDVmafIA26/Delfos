import json
import sqlite3
import time
from confluent_kafka import Consumer, KafkaError

# --- DATABASE INITIALIZATION ---
def init_db():
    """
    Initializes the SQLite database and creates the 'markets' table if it doesn't exist.
    """
    conn = sqlite3.connect('mercados_tech_full.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            market_id TEXT PRIMARY KEY,
            ingestion_timestamp TEXT,
            question TEXT,
            description TEXT,
            outcomes TEXT,
            price_yes REAL,
            price_no REAL,
            volume REAL,
            liquidity REAL,
            yes_token_id TEXT,
            order_book_json TEXT
        )
    ''')
    conn.commit()
    return conn

# --- KAFKA CONSUMER CONFIGURATION ---
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'full_data_saver_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)

def subscribe_with_retry(topic):
    """
    Attempts to subscribe to a topic, retrying if the broker or topic is unavailable.
    """
    print(f"Attempting to connect to Kafka at localhost:9092...")
    while True:
        try:
            # Check if topic exists in broker metadata
            metadata = consumer.list_topics(timeout=5)
            if topic in metadata.topics:
                consumer.subscribe([topic])
                print(f"Successfully subscribed to topic: {topic}")
                break
            else:
                print(f"Topic '{topic}' not found. Retrying in 5s...")
        except Exception:
            print(f"Broker unavailable. Retrying in 5s...")
        
        time.sleep(5)

# Initialize resources
db_conn = init_db()
cursor = db_conn.cursor()

# Suspend script until Kafka connectivity is established
subscribe_with_retry('tech_markets')

print("Consumer active. Listening for messages...")
print("Press Ctrl+C to terminate.")

# --- MAIN CONSUMPTION LOOP ---
try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka Error: {msg.error()}")
                break

        # 1. Safe JSON Decoding
        try:
            payload = msg.value().decode('utf-8')
            if not payload:
                continue
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Invalid message format skipped: {e}")
            continue

        # 2. Data Extraction and Transformation
        try:
            # Parse outcome_prices from string representation of a list
            raw_prices = data.get('outcome_prices', '["0", "0"]')
            prices = json.loads(raw_prices)
            
            # Map JSON fields to database columns
            values = (
                str(data.get('market_id')),
                data.get('ingestion_timestamp'),
                data.get('question'),
                data.get('description'),
                str(data.get('outcomes')),
                float(prices[0]) if len(prices) > 0 else 0.0,
                float(prices[1]) if len(prices) > 1 else 0.0,
                float(data.get('volume', 0)) if data.get('volume') else 0.0,
                float(data.get('liquidity', 0)) if data.get('liquidity') else 0.0,
                data.get('yes_token_id'),
                json.dumps(data.get('order_book'))
            )

            # 3. UPSERT Operation in SQLite
            cursor.execute('''
                INSERT OR REPLACE INTO markets 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', values)
            
            db_conn.commit()
            print(f"Record saved: {values[0]} - {values[2][:40]}...")

        except Exception as e:
            print(f"Data processing error: {e}")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
finally:
    # Graceful shutdown
    db_conn.close()
    consumer.close()
    print("Connections closed.")