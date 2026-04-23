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
            print("Conectado al Broker de Kafka")
        except Exception as e:
            print(f"Error al conectar con Kafka: {e}")
            raise

    def _delivery_report(self, err, msg):
        """Callback ejecutado tras cada envío exitoso o fallido."""
        if err is not None:
            print(f"Error al entregar mensaje al topic {msg.topic()}: {err}")
        # else:
        #     print(f"Mensaje entregado → {msg.topic()} [partición {msg.partition()}]")

    def send_data(
        self, topic: str, data: dict[str, Any], key: str | None = None
    ) -> None:
        """
        Método general para enviar info a Kafka de forma sencilla.
        :param topic: Nombre del topic (events, websockets, etc.)
        :param data: Diccionario con la info (JSON)
        :param key: (Opcional) ID del mercado para asegurar el orden cronológico
        """
        try:
            # 1. Serialización del diccionario a bytes
            payload = json.dumps(data).encode("utf-8")

            # 2. Enviar el mensaje (produce es asíncrono y ultra rápido)
            self.producer.produce(
                topic=topic,
                key=(
                    str(key) if key else None
                ),  # Si hay llave, se usa; si no, va a partición aleatoria
                value=payload,
                callback=self._delivery_report,
            )

            # 3. Empujar los mensajes a la red en segundo plano
            self.producer.poll(0)

        except Exception as e:
            print(f"Fallo interno al preparar el envío al topic {topic}: {e}")

    def flush(self):
        """Asegura que todo se envíe antes de que el script principal termine."""
        self.producer.flush()


_producer = None


def get_producer() -> KafkaManager:
    global _producer
    if _producer is None:
        _producer = KafkaManager()
    return _producer
