# Wraps the Confluent Cloud Kafka producer:
# credential wiring, JSON Schema serialisation via Schema Registry,
# delivery callback, and flush.
# The generator loop calls publish() and flush() only.

import sys
import os

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

import schema_registry

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]
TOPIC = os.environ.get("KAFKA_TOPIC", "banking.transactions")


def _delivery_callback(err, msg):
    if err is not None:
        print(f"Delivery error: {err}", file=sys.stderr)


def get_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "enable.metrics.push": False,
    })


# Serialiser is created once and reused for every message on TOPIC.
_serializer = schema_registry.get_serializer(TOPIC)
_ctx = SerializationContext(TOPIC, MessageField.VALUE)


def publish(producer, txn: dict):
    payload = _serializer(txn, _ctx)
    producer.produce(TOPIC, key=str(txn["account_id"]), value=payload, callback=_delivery_callback)
    producer.poll(0)


def flush(producer):
    producer.flush()


if __name__ == '__main__':
    producer = get_producer()
