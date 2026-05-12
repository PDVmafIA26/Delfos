import json
from confluent_kafka import Producer
from typing import Any

from logger import get_logger

log = get_logger(__name__)


class KafkaManager:
    def __init__(self, broker="localhost:9092"):
        self.config = {
            "bootstrap.servers": broker,
            "client.id": "polymarket-ingestor",
            "linger.ms": 10,
            "compression.type": "snappy",
            "acks": "all",
            "retries": 5,
            "enable.idempotence": True,
        }
        try:
            self.producer = Producer(self.config)
            log.info("Successfully connected to Kafka broker at '%s'", broker)
        except Exception as e:
            log.error("Failed to connect to Kafka broker at '%s': %s", broker, e)
            raise

    def _delivery_report(self, err, msg):
        """Callback invoked after each message is sent, whether successful or not."""
        if err is not None:
            log.error(
                "Failed to deliver message to topic '%s': %s", msg.topic(), err
            )
        else:
            log.debug(
                "Message delivered to '%s' [partition %d]",
                msg.topic(), msg.partition()
            )

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
            payload = json.dumps(data).encode("utf-8")
            self.producer.produce(
                topic=topic,
                key=str(key) if key else None,
                value=payload,
                callback=self._delivery_report,
                headers=headers or [],
            )
            self.producer.poll(0)

        except Exception as e:
            log.error("Failed to produce message to topic '%s': %s", topic, e)

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
            log.error("Kafka is unavailable. The ingestion pipeline cannot start: %s", e)
            raise
    return _producer
