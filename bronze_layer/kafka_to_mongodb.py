"""
Kafka Consumer for Bronze Layer (MongoDB)
Consumes messages from Kafka topics and stores them in MongoDB.
"""

import json
import time
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# ============================================
# CONFIGURACIÓN - Soporta variables de entorno para Docker
# ============================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = "polymarket_bronze"
TOPICS = ['events', 'websockets', 'top_wallets', 'user_info']
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "bronze-consumer-group")


# --- MONGODB INITIALIZATION (con retry infinito) ---
def init_mongodb(mongo_uri=MONGO_URI, db_name=MONGO_DB):
    """
    Initializes the MongoDB connection with infinite retry.
    """
    while True:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client[db_name]
            
            # Crear índices para cada colección que vayamos a usar
            for topic in TOPICS:
                collection = db[f'raw_{topic}']
                collection.create_index("_bronze_metadata.created_at")
                collection.create_index("_bronze_metadata.kafka_offset")
            
            print(f"[✓] Connected to MongoDB. Database: '{db_name}'")
            return client, db
            
        except Exception as e:
            print(f"[!] MongoDB unavailable: {e}")
            print("[!] Retrying in 5s...")
            time.sleep(5)


# --- KAFKA CONSUMER CONFIGURATION ---
def init_consumer(broker=KAFKA_BROKER, group_id=CONSUMER_GROUP, topics=None):
    """
    Initializes the Kafka consumer and subscribes to topics.
    """
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 600000,
    }
    
    target_topics = set(topics or TOPICS)
    
    print(f"[*] Connecting to Kafka broker: {broker}")
    
    while True:
        try:
            consumer = Consumer(conf)
            metadata = consumer.list_topics(timeout=5)
            existing_topics = set(metadata.topics.keys())
            
            # Find which topics exist
            available_topics = list(target_topics.intersection(existing_topics))
            
            if available_topics:
                consumer.subscribe(available_topics)
                print(f"[✓] Subscribed to available topics: {available_topics}")
                missing = target_topics - set(available_topics)
                if missing:
                    print(f"[!] Waiting for topics to appear: {missing}")
                return consumer
            else:
                print(f"[!] No topics found yet. Waiting 10s...")
                print(f"[!] Expected topics: {target_topics}")
                
        except Exception as e:
            print(f"[!] Broker unavailable: {e}. Retrying in 5s...")
        
        time.sleep(5)


def add_bronze_metadata(message, data):
    """Adds bronze layer metadata to the message before storing."""
    metadata = {
        "_bronze_metadata": {
            "kafka_topic": message.topic(),
            "kafka_partition": message.partition(),
            "kafka_offset": message.offset(),
            "kafka_timestamp": message.timestamp()[1] if message.timestamp() else None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source": "kafka_consumer"
        }
    }
    
    if isinstance(data, dict):
        data.update(metadata)
        return data
    else:
        return {"raw_data": data, **metadata}


def store_in_mongodb(collection, data):
    """Stores a single message in MongoDB."""
    try:
        collection.insert_one(data)
        return True
    except Exception as e:
        print(f"  [X] MongoDB insert error: {e}")
        return False


# --- MAIN CONSUMPTION LOOP ---
def run_consumer(
    topics=None,
    mongo_uri=MONGO_URI,
    db_name=MONGO_DB,
    kafka_broker=KAFKA_BROKER,
    group_id=CONSUMER_GROUP,
    max_messages=None
):
    """Main consumer loop. Polls Kafka and stores messages in MongoDB."""
    
    mongo_client, db = init_mongodb(mongo_uri, db_name)
    consumer = init_consumer(kafka_broker, group_id, topics)
    
    messages_consumed = 0
    
    print("\n[*] Consumer active. Listening for messages...")
    print("    Press Ctrl+C to terminate.\n")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    # No salgas del bucle, solo espera y reintenta
                    print(f"[!] Kafka error: {msg.error()}. Reconnecting...")
                    consumer.close()
                    consumer = init_consumer(kafka_broker, group_id, topics)
                    continue
            
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            
            try:
                payload = msg.value().decode('utf-8')
                if not payload:
                    continue
                data = json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"[-] Invalid message format: {e}")
                continue
            
            enriched_data = add_bronze_metadata(msg, data)
            
            # ✅ SOLUCIÓN: Colección dinámica por topic
            collection = db[f'raw_{topic}']
            
            success = store_in_mongodb(collection, enriched_data)
            
            if success:
                messages_consumed += 1
                print(f"[+] [{topic}] Partition {partition}, Offset {offset} -> Stored in raw_{topic}")
            else:
                print(f"[-] [{topic}] Partition {partition}, Offset {offset} -> FAILED")
            
            if max_messages and messages_consumed >= max_messages:
                print(f"\n[*] Reached limit of {max_messages} messages. Stopping.")
                break
                
    except KeyboardInterrupt:
        print("\n[!] Consumer stopped by user.")
    finally:
        consumer.close()
        mongo_client.close()
        print("[✓] Connections closed.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka to MongoDB Bronze Layer Consumer')
    parser.add_argument('--max-messages', type=int, default=None)
    parser.add_argument('--kafka-broker', type=str, default=KAFKA_BROKER)
    parser.add_argument('--mongo-uri', type=str, default=MONGO_URI)
    parser.add_argument('--group-id', type=str, default=CONSUMER_GROUP)
    parser.add_argument('--topics', type=str, nargs='+', default=TOPICS)
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("KAFKA TO MONGODB - BRONZE LAYER CONSUMER")
    print("=" * 60)
    print(f"Kafka broker: {args.kafka_broker}")
    print(f"MongoDB URI: {args.mongo_uri}")
    print(f"Topics: {args.topics}")
    print(f"Consumer group: {args.group_id}")
    print("=" * 60)
    
    run_consumer(
        topics=args.topics,
        mongo_uri=args.mongo_uri,
        kafka_broker=args.kafka_broker,
        group_id=args.group_id,
        max_messages=args.max_messages
    )