# Schema Registry client wrapper for Confluent Cloud.
#
# Provides:
#   get_client()              → SchemaRegistryClient (cached singleton)
#   get_serializer(topic)     → AvroSerializer for the given topic's value schema
#   get_deserializer(topic)   → AvroDeserializer for the given topic's value schema
#                               (None for topics whose schema is not locally registered)
#   load_schema_str(topic)    → raw Avro JSON string for the topic's value schema
#
# Schema files live in  <repo-root>/schemas/<topic>.avsc
# Subject naming convention: <topic>-value  (Confluent default)

import os
from functools import lru_cache
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()

# ── paths ────────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCHEMAS_DIR = _REPO_ROOT / "schemas"


# ── client ───────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_client() -> SchemaRegistryClient:
    """Return a cached SchemaRegistryClient built from environment variables."""
    url = os.environ["SCHEMA_REGISTRY_URL"]
    api_key = os.environ.get("SCHEMA_REGISTRY_API_KEY", "")
    api_secret = os.environ.get("SCHEMA_REGISTRY_API_SECRET", "")

    conf = {"url": url}
    if api_key and api_secret:
        conf["basic.auth.user.info"] = f"{api_key}:{api_secret}"

    return SchemaRegistryClient(conf)


# ── schema loading ────────────────────────────────────────────────────────────

def load_schema_str(topic: str) -> str:
    """Load and return the raw Avro schema string for *topic*.

    Raises FileNotFoundError if no schema file exists for the topic.
    """
    path = _SCHEMAS_DIR / f"{topic}.avsc"
    if not path.exists():
        raise FileNotFoundError(f"No schema file found for topic '{topic}': {path}")
    return path.read_text()


# ── serialiser / deserialiser ─────────────────────────────────────────────────

def get_serializer(topic: str) -> AvroSerializer:
    """Return an AvroSerializer for the value schema of *topic*.

    The schema is loaded from  schemas/<topic>.avsc  and registered under
    subject  <topic>-value  on first use (idempotent via Schema Registry).
    """
    schema_str = load_schema_str(topic)
    return AvroSerializer(get_client(), schema_str, conf={"auto.register.schemas": True})


def get_deserializer(topic: str) -> AvroDeserializer:
    """Return an AvroDeserializer for the value schema of *topic*.

    Falls back gracefully: if no local schema file exists the deserialiser
    uses the schema fetched from the registry by schema-id (Confluent wire format).
    """
    try:
        schema_str = load_schema_str(topic)
    except FileNotFoundError:
        schema_str = None
    return AvroDeserializer(get_client(), schema_str)


# ── convenience: serialise / deserialise bytes ───────────────────────────────

def serialize(topic: str, record: dict) -> bytes:
    """Serialise *record* to bytes using the Avro schema for *topic*."""
    ctx = SerializationContext(topic, MessageField.VALUE)
    return get_serializer(topic)(record, ctx)


def deserialize(topic: str, data: bytes) -> dict:
    """Deserialise *data* bytes to a dict using the Avro schema for *topic*."""
    ctx = SerializationContext(topic, MessageField.VALUE)
    return get_deserializer(topic)(data, ctx)
