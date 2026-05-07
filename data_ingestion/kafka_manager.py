import json
from confluent_kafka import Producer
from typing import Any


class KafkaManager:
    def __init__(self, broker="localhost:9092"):
        self.config = {
            # TODO: Update to production broker address before deploying
            "bootstrap.servers": broker,  # Defaults to localhost:9092 (local development only)
            "client.id": "polymarket-ingestor",  # Producer identifier in Kafka logs
            "linger.ms": 10,  # Batch messages for up to 10ms to improve throughput
            "compression.type": "snappy",  # Fast, lightweight batch compression
            "acks": "all",  # Wait for all replicas to confirm before acknowledging
            "retries": 5,  # Automatically retry failed sends up to 5 times
            "enable.idempotence": True,  # Prevent duplicate messages on retry
        }
        try:
            self.producer = Producer(self.config)
            print("Successfully connected to Kafka broker.")
        except Exception as e:
            print(f"Failed to connect to Kafka broker at '{broker}': {e}")
            raise

    def _delivery_report(self, err, msg):
        """Callback invoked after each message is sent, whether successful or not."""
        if err is not None:
            print(f"Failed to deliver message to topic '{msg.topic()}': {err}")
        # else:
        #     logger.debug(f"Message delivered to '{msg.topic()}' [partition {msg.partition()}]")

    def send_data(
        self,
        topic: str,
        data: dict[str, Any],
        key: str | None = None,
        headers: list[tuple] | None = None,
    ) -> None:
        """
        Serializes and sends a data dictionary to the specified Kafka topic.
        :param topic: Target topic name (e.g. 'top_wallets', 'websockets')
        :param data: Dictionary to serialize and send as JSON
        :param key: Optional market ID to ensure chronological ordering within a partition
        :param headers: Optional metadata headers as a list of (key, value) tuples
        """
        try:
            # Serialize dictionary to bytes
            payload = json.dumps(data).encode("utf-8")

            # Produce is asynchronous — delivery is confirmed via _delivery_report callback
            self.producer.produce(
                topic=topic,
                key=(
                    str(key) if key else None
                ),  # If no key, message goes to a random partition
                value=payload,
                callback=self._delivery_report,
                headers=headers or [],
            )

            # Trigger delivery callbacks without blocking
            self.producer.poll(0)

        except Exception as e:
           print(f"Failed to produce message to topic '{topic}': {e}")

    def flush(self):
        """Blocks until all pending messages have been delivered to Kafka."""
        self.producer.flush()


_producer: KafkaManager | None = None

def get_producer() -> KafkaManager:
    """Returns the singleton KafkaManager instance, creating it if necessary."""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaManager()
        except Exception as e:
            print(f"Kafka is unavailable. The ingestion pipeline cannot start: {e}")
            raise
    return _producer
