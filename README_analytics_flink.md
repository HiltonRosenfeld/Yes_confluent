# Real-Time Banking Analytics — Flink + AstraDB

This document covers everything needed to run the analytics pipeline: dimension loading, Flink SQL job deployment, sink workers, and querying the AstraDB analytics tables.

---

## Architecture Overview

```
[AstraDB ODS]                      account, branch, customer, employee
      │
      │  dimension_loader.py  (one-shot)
      ▼
[Confluent Cloud Kafka]            banking.dimensions.{account,branch,customer,employee}
      │
      │  Flink lookup joins
      ▼
[Confluent Cloud Flink SQL]  ◄───  banking.transactions  (live stream)
  6 SQL jobs (Q1 – Q6)
      │
      │  enriched / aggregated events
      ▼
[Confluent Cloud Kafka]            analytics.{transactions_by_account, high_value_transaction_hourly,
                                              high_value_transaction_by_city, withdrawal_transaction_by_employee,
                                              customer_quarterly_summary, branch_daily_rollup}
      │
      │  astra_sink_worker.py  (one process per topic, long-running)
      ▼
[AstraDB Analytics Keyspace]       8 analytics tables, queried by dashboards
```

---

## Prerequisites

- Python 3.12 with the project venv already set up (`./scripts/setup.sh`)
- `.env` file populated (see [Environment Variables](#environment-variables))
- AstraDB analytics keyspace created and DDL applied (`src/analytics_schema.cql`)
- Confluent Cloud: `banking.transactions` topic exists and the transaction producer is running
- Confluent Cloud: Flink environment provisioned (Confluent Cloud Console → Environments → Flink)

---

## Environment Variables

Copy `.env.example` to `.env` and fill in every value:

| Variable | Description |
|---|---|
| `ASTRA_DB_APPLICATION_TOKEN` | AstraDB application token (`AstraCS:...`) |
| `ASTRA_SECURE_BUNDLE_PATH` | Absolute path to `secure-connect-<db>.zip` |
| `ASTRA_KEYSPACE` | Target keyspace (must contain the analytics tables) |
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Cloud bootstrap server (`host:port`) |
| `KAFKA_API_KEY` | Confluent Cloud API key |
| `KAFKA_API_SECRET` | Confluent Cloud API secret |
| `KAFKA_TOPIC` | Source transaction topic (default: `banking.transactions`) |

---

## Step 1 — Apply the Analytics Schema

Run this once against your AstraDB keyspace before starting anything else:

```bash
# Using cqlsh with the secure bundle
cqlsh -u token -p "$ASTRA_DB_APPLICATION_TOKEN" \
  --secure-connect-bundle "$ASTRA_SECURE_BUNDLE_PATH" \
  -k "$ASTRA_KEYSPACE" \
  -f src/analytics_schema.cql
```

This creates the 8 analytics tables:

| Table | Purpose |
|---|---|
| `transactions_by_account` | All transactions per account, sorted by time descending |
| `high_value_transaction_hourly` | Transactions `> $10,000`, partitioned by minute |
| `high_value_transaction_by_city` | Transactions `> $50,000`, partitioned by hour + city |
| `withdrawal_transaction_by_employee` | Withdrawals per employee per quarter |
| `customer_account_count` | Counter — active accounts per customer |
| `customer_quarterly_txn_total` | Running quarterly spend per customer |
| `high_value_customer_quarterly` | Customers with quarterly spend `> $1,000,000` |
| `branch_daily_rollup` | Daily transaction count + total amount per branch |

---

## Step 2 — Create Kafka Topics

Create the dimension and analytics sink topics in Confluent Cloud before running anything.

**Dimension topics** (compacted, used by Flink lookup joins):

```
banking.dimensions.account
banking.dimensions.branch
banking.dimensions.customer
banking.dimensions.employee
```

> **Tip:** Set the dimension topics to `cleanup.policy=compact` so Flink always has the latest value for each key.

---

## Step 3 — Load Dimensions into Kafka

Run once (or whenever ODS dimension data changes materially):

```bash
./scripts/run_dimension_loader.sh
```

This script:
1. Connects to AstraDB ODS and reads all rows from `account`, `branch`, `customer`, `employee`.
2. Publishes each row as a JSON message to the corresponding `banking.dimensions.*` topic, keyed by the primary key UUID.
3. Publishes `account_active` seed events to `analytics.customer_quarterly_summary` for each active account — these seed the `customer_account_count` counter table.

Re-run this script after bulk dimension changes (e.g. new branch added, customer data refresh). Individual real-time changes should be published directly to the dimension topics by the upstream system.

---

## Step 4 — Deploy Flink SQL Jobs

Each SQL file in `src/flink/` is a self-contained Flink SQL statement. Deploy all six to Confluent Cloud Flink.

### Via Confluent Cloud Console (UI)

1. Open **Confluent Cloud Console → Environments → \<your environment\> → Flink**.
2. Click **+ New statement**.
3. Paste the contents of the SQL file.
4. Click **Run**. The job runs continuously — do not stop it.
5. Repeat for each of the six SQL files.


### Job summary

| File | Input | Joins | Output topic |
|---|---|---|---|
| [`q1_txn_by_account.sql`](src/flink/q1_txn_by_account.sql) | `banking.transactions` | none | `analytics.transactions_by_account` |
| [`q2_high_value_hourly.sql`](src/flink/q2_high_value_hourly.sql) | `banking.transactions` | none | `analytics.high_value_transaction_hourly` |
| [`q3_high_value_by_city.sql`](src/flink/q3_high_value_by_city.sql) | `banking.transactions` | account → branch | `analytics.high_value_transaction_by_city` |
| [`q4_withdrawal_by_employee.sql`](src/flink/q4_withdrawal_by_employee.sql) | `banking.transactions` | employee → branch | `analytics.withdrawal_transaction_by_employee` |
| [`q5_customer_quarterly.sql`](src/flink/q5_customer_quarterly.sql) | `banking.transactions` | account | `analytics.customer_quarterly_summary` |
| [`q6_branch_daily_rollup.sql`](src/flink/q6_branch_daily_rollup.sql) | `banking.transactions` | account | `analytics.branch_daily_rollup` |

**Q5 and Q6 note:** These jobs use a stateful `GROUP BY`. Flink checkpointing preserves state across restarts — running sums and counts continue from the last committed checkpoint.



---

## Operational Order

```
1. apply analytics_schema.cql      (once)
2. create Kafka topics              (once)
3. run_dimension_loader.sh          (once, or on dimension refresh)
4. deploy all 6 Flink SQL jobs      (continuous, via Confluent Cloud)
```

---

## Querying the Analytics Tables

### Q1 — All transactions for an account in the last 30 days

```sql
-- Uses the SAI index on txn_day for efficient date range filtering
SELECT * FROM transactions_by_account
WHERE account_id = <uuid>
  AND txn_day >= '20250101'   -- YYYYMMDD, adjust to 30 days ago
  AND txn_day <= '20250131';
```

### Q2 — Transactions > $10,000 in the last 60 minutes

```sql
-- Scan the last 60 txn_minute partitions (YYYYMMDDHHmm)
SELECT * FROM high_value_transaction_hourly
WHERE txn_minute IN ('202501311400', '202501311401', /* ... */, '202501311459');
```

### Q3 — Count of transactions > $50,000 by city in the last 24 hours

```sql
-- Aggregate last 24 bucket_hour partitions (YYYYMMDDHH)
SELECT city, COUNT(*), SUM(amount)
FROM high_value_transaction_by_city
WHERE bucket_hour IN ('2025013109', '2025013110', /* ... */, '2025013108')
GROUP BY city;
```

### Q4 — Withdrawal transactions by manager (quarter)

```sql
-- First resolve the manager's direct reports from the employee cache,
-- then query each employee's partition
SELECT * FROM withdrawal_transaction_by_employee
WHERE employee_id = <emp_uuid>
  AND quarter = '2025Q1';
```

### Q5 — Customers with > 5 active accounts AND quarterly spend > $1,000,000

```sql
-- Step 1: customers with > 5 active accounts
SELECT customer_id FROM customer_account_count
WHERE active_account_count > 5;   -- filter in application layer (counter tables don't support secondary indexes)

-- Step 2: cross-reference with high-value quarterly table
SELECT customer_id, amount FROM high_value_customer_quarterly
WHERE quarter = '2025-Q1';
```

### Q6 — Daily branch totals for a date range

```sql
SELECT txn_date, total_amount, count_txn
FROM branch_daily_rollup
WHERE branch_id = <uuid>
  AND txn_year = '2025'
  AND txn_date >= '2025-01-01'
  AND txn_date <= '2025-01-31';
```

---

## File Reference

```
src/
  dimension_loader.py              Reads AstraDB ODS dimensions → publishes to Kafka
  flink/
    q1_txn_by_account.sql          Flink job: all transactions per account
    q2_high_value_hourly.sql       Flink job: filter amount > 10,000
    q3_high_value_by_city.sql      Flink job: filter amount > 50,000, enrich with city
    q4_withdrawal_by_employee.sql  Flink job: withdrawals, enrich with branch
    q5_customer_quarterly.sql      Flink job: running quarterly sum per customer
    q6_branch_daily_rollup.sql     Flink job: daily rollup per branch
  sink/
    sink_schemas.py                Topic → CQL registry and field extractors
    astra_sink_worker.py           Generic Kafka consumer → AstraDB writer (CLI)
  analytics_schema.cql             DDL for all 8 analytics tables
scripts/
  run_dimension_loader.sh          One-shot: load dimensions into Kafka
  run_sink_workers.sh              Start all 6 sink workers (with PID tracking)
```
