# CLI entrypoint: generates synthetic banking transactions using the shared
# data_generator.py domain model, with optional DB loading, Kafka publishing,
# and Astra DB writes.

import argparse
import time
from dataclasses import asdict

from dotenv import load_dotenv

import loader
import publisher
from data_generator import build_dataset, generate_transactions, write_astra


def _make_transaction(ref_data: dict) -> dict:
    """Generate a single referentially-consistent transaction dict."""
    transaction = generate_transactions(
        1,
        ref_data["accounts"],
        ref_data["employees"],
        ref_data["products"],
    )[0]
    return asdict(transaction)


def _parse_args():
    parser = argparse.ArgumentParser(description="Synthetic banking transaction generator")
    parser.add_argument("--rate", type=float, default=1000.0, help="Transactions per second")
    parser.add_argument("--transactions", type=int, default=0, help="Total transactions to send, 0=unlimited")
    parser.add_argument("--duration", type=float, default=0, help="Run duration in seconds, 0=unlimited")
    parser.add_argument("--load-ref-data-from-db", action="store_true", help="Load existing non-transaction tables from Astra DB")
    parser.add_argument("--write-transactions-to-db", action="store_true", help="Write generated transactions to Astra DB")
    parser.add_argument("--publish", action="store_true", help="Publish generated transactions to Kafka")
    parser.add_argument("--branches", type=int, default=10, help="Number of branches when generating reference data locally")
    parser.add_argument("--customers", type=int, default=200, help="Number of customers when generating reference data locally")
    parser.add_argument("--employees", type=int, default=50, help="Number of employees when generating reference data locally")
    parser.add_argument("--products", type=int, default=12, help="Number of products when generating reference data locally")
    parser.add_argument("--accounts", type=int, default=500, help="Number of accounts when generating reference data locally")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load_dotenv()

    print(f"Starting generator: rate={args.rate} TPS, transaction_limit={args.transactions or 'unlimited'}, duration_limit={args.duration or 'unlimited'}s")

    session = None
    if args.load_ref_data_from_db or args.write_transactions_to_db:
        session = loader.get_session()

    if args.load_ref_data_from_db:
        ref_data = loader.load_reference_data(session)
        print(
            f"Loaded {len(ref_data['accounts'])} accounts, "
            f"{len(ref_data['employees'])} employees, "
            f"{len(ref_data['branches'])} branches, "
            f"{len(ref_data['products'])} products from Astra DB"
        )
    else:
        ref_data = build_dataset(
            n_branches=args.branches,
            n_customers=args.customers,
            n_employees=args.employees,
            n_products=args.products,
            n_accounts=args.accounts,
            skip_transactions=True,
        )
        print(
            f"Generated {len(ref_data['accounts'])} accounts, "
            f"{len(ref_data['employees'])} employees, "
            f"{len(ref_data['branches'])} branches, "
            f"{len(ref_data['products'])} products locally"
        )

    producer = publisher.get_producer() if args.publish else None
    transactions = []

    sent = 0
    start_time = time.monotonic()

    try:
        while True:
            loop_start = time.monotonic()

            txn = _make_transaction(ref_data)
            transactions.append(txn)
            if producer is not None:
                publisher.publish(producer, txn)
            print(f"[{sent + 1}] txn_id={txn['txn_id']} account_id={txn['account_id']} amount={txn['amount']:.2f} status={txn['status']}")
            sent += 1

            if args.transactions > 0 and sent >= args.transactions:
                break
            if args.duration > 0 and time.monotonic() - start_time >= args.duration:
                break

            time.sleep(max(0.0, (1.0 / args.rate) - (time.monotonic() - loop_start)))
    except KeyboardInterrupt:
        print("Interrupted.")

    if producer is not None:
        publisher.flush(producer)

    if args.write_transactions_to_db and transactions:
        write_astra({"transactions": transactions})

    if session is not None:
        session.shutdown()


    print("Done.", end="")
    if producer is not None:
        print(f"Sent {sent} transactions.")
