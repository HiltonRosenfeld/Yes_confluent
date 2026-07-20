# Synthetic Banking Dataset Generator

A two-script toolkit for generating referentially-consistent synthetic banking
data. Use it to seed a database with reference tables, produce batch transaction
files, or stream live transactions into Confluent Cloud Kafka.

---

## Tables

Tables are generated in dependency order so every foreign key always resolves
to a real record.

| Table | Foreign keys | Default count |
|---|---|---|
| `branch` | — | 10 |
| `customer` | — | 200 |
| `employee` | `branch_id → branch`, `manager_id → employee` | 50 |
| `product` | — | 12 |
| `account` | `customer_id → customer`, `branch_id → branch` | 500 |
| `transaction` | `account_id → account`, `product_id → product`, `approved_by_emp_id → employee` | 5,000 |

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

## Use Case 1 — Generate All Tables Except Transactions

Use **`src/data_generator.py`**. Transactions are skipped by default — simply
omit `--transactions`.

```bash
# Activate virtual environemnt
source .venv/bin/activate

# Dry run — print record counts only, write nothing
python src/data_generator.py

# Write JSON files to ./output/
python src/data_generator.py --out-dir ./output

# Write CSV files instead
python src/data_generator.py --out-dir ./output --format csv

# Custom record counts
python src/data_generator.py \
    --branches 20 \
    --customers 500 \
    --employees 80 \
    --products 30 \
    --accounts 2000 \
    --out-dir ./output

# Write directly to Astra DB
python src/data_generator.py --astra

# Write files AND load into Astra DB
python src/data_generator.py --out-dir ./output --astra

# Reproducible run with a fixed random seed
python src/data_generator.py --seed 42 --out-dir ./output
```

Output files (when `--out-dir` is specified):

```
output/
├── branches.json
├── customers.json
├── employees.json
├── products.json
└── accounts.json
```

---

## Use Case 2 — Generate Transactions

### Option A — Batch file of transactions

Still uses `data_generator.py`. Add `--transactions N` to include the
transaction table alongside all reference tables.

```bash
# Activate virtual environemnt
source .venv/bin/activate

# Write all six tables (including transactions) to ./output/
python src/data_generator.py --transactions 5000 --out-dir ./output

# CSV output with custom sizes
python src/data_generator.py \
    --accounts 2000 \
    --transactions 10000 \
    --out-dir ./output \
    --format csv

# Write all tables (including transactions) to Astra DB
python src/data_generator.py --transactions 5000 --astra
```

### Option B — Stream transactions to Confluent Kafka

Uses **`src/transaction_generator.py`** directly. It generates transactions
one-by-one at a controlled rate and optionally publishes each to the Kafka topic.
Reference data (accounts, employees, etc.) is either generated locally on-the-fly
or loaded from an existing Astra DB.

```bash
# Activate virtual environment
source .venv/bin/activate

# Generate and print continous transactions (no Kafka publishing)
python src/transaction_generator.py

# Generate and print 1 transaction (no Kafka publishing)
python src/transaction_generator.py --transactions 1

# Exactly 500 transactions published to Kafka
python src/transaction_generator.py --transactions 500 --publish

# 10 transactions/sec for 60 seconds published to Kafka
python src/transaction_generator.py --rate 10 --duration 60 --publish

# Load reference data from Astra DB instead of generating it locally
python src/transaction_generator.py --load-ref-data-from-db

# Write generated transactions back to Astra DB
python src/transaction_generator.py --transactions 1000 --write-transactions-to-db

# Full pipeline: load ref data from Astra, stream to Kafka, write back to Astra
python src/transaction_generator.py \
    --rate 5 \
    --transactions 1000 \
    --load-ref-data-from-db \
    --write-transactions-to-db \
    --publish
```

#### `transaction_generator.py` flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--rate` | float | `1000.0` | Transactions per second |
| `--transactions` | int | `1` | Total transactions to send (`0` = unlimited) |
| `--duration` | float | `0` | Run duration in seconds (`0` = unlimited) |
| `--load-ref-data-from-db` | flag | off | Load reference tables from Astra DB |
| `--write-transactions-to-db` | flag | off | Write generated transactions to Astra DB |
| `--publish` | flag | off | Publish transactions to Kafka (default: off) |
| `--branches` | int | `10` | Branches to generate (local ref data only) |
| `--customers` | int | `200` | Customers to generate (local ref data only) |
| `--employees` | int | `50` | Employees to generate (local ref data only) |
| `--products` | int | `12` | Products to generate (local ref data only) |
| `--accounts` | int | `500` | Accounts to generate (local ref data only) |

---

## `data_generator.py` CLI Reference

| Argument | Type | Default | Description |
|---|---|---|---|
| `--branches` | int | `10` | Number of branches |
| `--customers` | int | `200` | Number of customers |
| `--employees` | int | `50` | Number of employees (10% will be managers) |
| `--products` | int | `12` | Number of products |
| `--accounts` | int | `500` | Number of accounts |
| `--transactions` | int | *(omit to skip)* | Number of transactions; omit to skip the table |
| `--out-dir` | path | *(none)* | Directory to write output files |
| `--format` | `json`\|`csv` | `json` | Output file format |
| `--seed` | int | *(none)* | Random seed for reproducible output |
| `--astra` | flag | off | Write dataset to Astra DB |

---

## Script Comparison

| | `data_generator.py` | `transaction_generator.py` |
|---|---|---|
| **Primary output** | Files (JSON/CSV) and/or Astra DB | Confluent Kafka topic |
| **Transactions** | Optional batch (`--transactions N`) | Always — that is its only job |
| **Rate control** | No — generates all records at once | Yes (`--rate`, `--transactions`, `--duration`) |
| **Requires Kafka** | No | Only with `--publish` |
| **Requires Astra** | Only with `--astra` | Optional (`--load-ref-data-from-db`, `--write-transactions-to-db`) |
