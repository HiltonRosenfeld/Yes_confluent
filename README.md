# Streaming Banking Data

Desciprtion....

---

## Project Layout

```
src/
  data_generator.py         # Batch generator — reference tables + optional transactions
  transaction_generator.py  # Streaming entrypoint — publishes transactions to Kafka
  loader.py                 # Loads existing reference data from Astra DB
  publisher.py              # Confluent Cloud Kafka producer wrapper
  astra_client.py           # Shared Astra DB connection helper
  database_schema.cql       # CQL DDL for all tables
scripts/
  setup.sh                  # Create venv and install dependencies
tests/
  test_astra_connection.py   # Live Astra DB connectivity smoke test
  test_kafka_producer.py     # Live Kafka producer smoke test
```

---

## Prerequisites

- Python 3.12
- Astra DB secure connect bundle (`.zip` from the Astra console) — *only needed for Astra features*
- Confluent Cloud account with an API key and a `banking.transactions` topic — *only needed for Kafka streaming*

## Setup

```bash
cp .env.example .env
# Edit .env and fill in the values you need
./scripts/setup.sh
```

### Environment Variables

| Variable | Purpose | Required for |
|---|---|---|
| `ASTRA_TOKEN` | Astra DB application token (`AstraCS:...`) | Astra features |
| `ASTRA_SECURE_BUNDLE_PATH` | Absolute path to `secure-connect-<db>.zip` | Astra features |
| `ASTRA_KEYSPACE` | Target Cassandra keyspace | Astra features |
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Cloud bootstrap server (`host:port`) | Kafka streaming |
| `KAFKA_API_KEY` | Confluent Cloud API key | Kafka streaming |
| `KAFKA_API_SECRET` | Confluent Cloud API secret | Kafka streaming |
| `KAFKA_TOPIC` | Kafka topic to publish to | Kafka streaming (default: `banking.transactions`) |

---

## Data Generation

Python scripts to generate full banking data set and transactions. Optins to generate into files, DB, or streaming.

See src/README_dataset_generator.md for more details.
