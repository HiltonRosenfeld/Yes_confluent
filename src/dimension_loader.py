# Reads ODS dimension tables from AstraDB and publishes each row as JSON
# to the corresponding banking.dimensions.* Kafka topic.
# Also seeds the customer_account_count counter by publishing to the dedicated
# analytics.account_active_events topic (previously mixed into customer_quarterly_summary).

import os
import sys

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

import astra_client
import schema_registry

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]

TOPIC_ACCOUNT       = "banking.dimensions.account"
TOPIC_BRANCH        = "banking.dimensions.branch"
TOPIC_CUSTOMER      = "banking.dimensions.customer"
TOPIC_EMPLOYEE      = "banking.dimensions.employee"
TOPIC_ACCOUNT_SEED  = "analytics.account_active_events"   # dedicated seed topic


def _delivery_callback(err, msg):
    if err is not None:
        print(f"Delivery error: {err}", file=sys.stderr)


def _get_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "enable.metrics.push": False,
    })


# ── per-topic serialisers ─────────────────────────────────────────────────────

_serializers = {
    topic: schema_registry.get_serializer(topic)
    for topic in [TOPIC_ACCOUNT, TOPIC_BRANCH, TOPIC_CUSTOMER, TOPIC_EMPLOYEE]#, TOPIC_ACCOUNT_SEED]
}


def _publish(producer, topic, key, record):
    serializer = _serializers[topic]
    ctx = SerializationContext(topic, MessageField.VALUE)
    payload = serializer(record, ctx)
    producer.produce(topic, key=str(key), value=payload, callback=_delivery_callback)
    producer.poll(0)


# ── loaders ───────────────────────────────────────────────────────────────────

def load_accounts(session, producer):
    # Returns list of account dicts (reused for active-account seed events).
    rows = session.execute(
        "SELECT account_id, customer_id, account_type, status, branch_id, opened_at, closed_at FROM account"
    )
    accounts = []
    for row in rows:
        record = {
            "account_id":   str(row.account_id),
            "customer_id":  str(row.customer_id),
            "account_type": row.account_type,
            "status":       row.status,
            "branch_id":    str(row.branch_id),
            "opened_at":    row.opened_at.isoformat() if row.opened_at else None,
            "closed_at":    row.closed_at.isoformat() if row.closed_at else None,
        }
        _publish(producer, TOPIC_ACCOUNT, record["account_id"], record)
        accounts.append(record)
    return accounts


def load_branches(session, producer):
    for row in session.execute("SELECT branch_id, branch_name, city, region FROM branch"):
        record = {
            "branch_id":   str(row.branch_id),
            "branch_name": row.branch_name,
            "city":        row.city,
            "region":      row.region,
        }
        _publish(producer, TOPIC_BRANCH, record["branch_id"], record)


def load_customers(session, producer):
    for row in session.execute(
        "SELECT customer_id, name, dob, customer_segment, pan_hash, city, created_at FROM customer"
    ):
        record = {
            "customer_id":      str(row.customer_id),
            "name":             row.name,
            "dob":              str(row.dob),
            "customer_segment": row.customer_segment,
            "pan_hash":         row.pan_hash,
            "city":             row.city,
            "created_at":       row.created_at.isoformat() if row.created_at else None,
        }
        _publish(producer, TOPIC_CUSTOMER, record["customer_id"], record)


def load_employees(session, producer):
    for row in session.execute(
        "SELECT emp_id, name, role, branch_id, manager_id, hire_date FROM employee"
    ):
        record = {
            "emp_id":     str(row.emp_id),
            "name":       row.name,
            "role":       row.role,
            "branch_id":  str(row.branch_id),
            "manager_id": str(row.manager_id) if row.manager_id is not None else None,
            "hire_date":  str(row.hire_date),
        }
        _publish(producer, TOPIC_EMPLOYEE, record["emp_id"], record)


def seed_active_account_counts(producer, accounts):
    # Publish one seed event per active account to the dedicated seed topic so
    # the sink worker can initialise the customer_account_count counter table.
    for acc in accounts:
        if acc["status"] == "Active":
            event = {
                "event":       "account_active",
                "customer_id": acc["customer_id"],
                "account_id":  acc["account_id"],
            }
            _publish(producer, TOPIC_ACCOUNT_SEED, acc["customer_id"], event)


def main():
    session = astra_client.connect()
    producer = _get_producer()

    print("Loading accounts...")
    accounts = load_accounts(session, producer)
    print(f"  {len(accounts)} accounts published.")

    print("Loading branches...")
    load_branches(session, producer)

    print("Loading customers...")
    load_customers(session, producer)

    print("Loading employees...")
    load_employees(session, producer)

    #print("Seeding active-account counter events...")
    #seed_active_account_counts(producer, accounts)

    producer.flush()
    session.shutdown()
    print("Done.")


if __name__ == "__main__":
    main()
