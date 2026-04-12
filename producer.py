import json
from confluent_kafka import Producer

# Kafka producer configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

producer = Producer(config)

def delivery_report(err, msg):
    """
    Optional per-message delivery callback to report success or failure.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Decode key for logging purposes
        key = msg.key().decode('utf-8') if msg.key() else "No Key"
        print(f"Market {key} delivered to partition {msg.partition()}")

# Source file path
json_file = 'bronze_layer_payload_tech.json'

try:
    # Open file with UTF-8 encoding to support special characters
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"File loaded. Sending {len(data)} records to Kafka...")

    for record in data:
        # Use market_id as the message key to ensure partition affinity
        key = str(record.get('market_id', 'unknown'))
        value = json.dumps(record)
        
        # Produce message to 'tech_markets' topic
        producer.produce(
            'tech_markets', 
            key=key, 
            value=value, 
            callback=delivery_report
        )
        
        # Serve delivery report callbacks from previous produce calls
        producer.poll(0)

    # Wait for all outstanding messages to be delivered
    print("Flushing outstanding messages...")
    producer.flush()
    print("Process completed. All data pushed to Kafka.")

except FileNotFoundError:
    print(f"Error: The file '{json_file}' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")