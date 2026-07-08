# Wraps the Confluent Cloud Kafka producer: 
# credential wiring, JSON serialisation, delivery callback, and flush.  
# The generator loop calls publish() and flush() only.

import datetime
import json
import sys
import uuid

from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]
TOPIC = os.environ.get("KAFKA_TOPIC", "banking.transactions")


class _BankingEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


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


def publish(producer, txn: dict):
    payload = json.dumps(txn, cls=_BankingEncoder).encode()
    producer.produce(TOPIC, key=str(txn["account_id"]), value=payload, callback=_delivery_callback)
    producer.poll(0)


def flush(producer):
    producer.flush()



if __name__ == '__main__':
    producer = get_producer()