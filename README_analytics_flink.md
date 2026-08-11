# Real-Time Banking Analytics — Flink + AstraDB

This document covers everything needed to run the analytics pipeline: dimension loading, Flink SQL job deployment, sink workers, and querying the AstraDB analytics tables.

---

## Prerequisites

- Python 3.12 with the project venv already set up (`./scripts/setup.sh`)
- `.env` file populated (see [Environment Variables](#environment-variables))
- AstraDB ods keyspace created and banking data generated (`README_dataset_generator.md`)
- Confluent Cloud: `banking.transactions` topic exists and the transaction producer is running
- Confluent Cloud: Flink environment provisioned (Confluent Cloud Console → Environments → Flink)
- AWS: S3 bucket exists with access policy and access key

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
| `AWS_ACCESS_KEY_ID` | AWS S3 Access key |
| `AWS_SECRET_ACCESS_KEY` | AWS S3 Secret Access key |


---

## Step 1 — Define Flink watermark on banking.transactions

Run this once against banking.transactions:

```sql
ALTER TABLE `banking.transactions` 
MODIFY WATERMARK FOR txn_time AS txn_time - INTERVAL '5' SECOND;
```

---

## Step 2 — Create Kafka Topics

Create the dimension and analytics sink topics in Confluent Cloud before running anything.

**Dimension topics** (compacted, used by Flink lookup joins):

```txt
banking.dimensions.account
banking.dimensions.branch
banking.dimensions.customer
banking.dimensions.employee
```

> **Tip:** *Set the dimension topics to `cleanup.policy=compact` so Flink always has the latest value for each key.*

---

## Step 3 — Load Dimensions into Kafka

Run once (or whenever ODS dimension data changes materially):

```bash
# Activate virtual environemnt
source .venv/bin/activate

# Dry run — print record counts only, write nothing
python src/dimension_loader.py
```

This script:

1. Connects to AstraDB ODS and reads all rows from `account`, `branch`, `customer`, `employee`.
2. Publishes each row as an AVRO message to the corresponding `banking.dimensions.*` topic, keyed by the primary key UUID.
3. Publishes `account_active` seed events to `analytics.customer_quarterly_summary` for each active account — these seed the `customer_account_count` counter table.

Re-run this script after bulk dimension changes (e.g. new branch added, customer data refresh). Individual real-time changes should be published directly to the dimension topics by the upstream system.

---

## Step 4 — Deploy Flink Materialized Tables

We use Confluent Flink materialized tables instead of the older manual process of separately creating each of the workflow elements.

- Combines the table definition and the continuous background query into one manageable entity.
- Automatically spins up the backing Kafka topic and registers schemas in the Schema Registry.
 
Each SQL file in `src/flink/` is a self-contained Flink SQL statement. Deploy them all to Confluent Cloud Flink.

### Via Confluent Cloud Console (UI)

1. Open **Confluent Cloud Console → SQL Workspaces**.
2. Click **+ New statement**.
3. Paste the contents of the SQL file.
4. Click **Run**. The job runs and creates the materialized table.
5. Repeat for each of the SQL files.


### Job summary

| File | Input | Joins | Output topic |
|---|---|---|---|
| [`q1_txn_by_account.sql`](src/flink/q1_txn_by_account.sql) | `banking.transactions` | none | `analytics.transactions_by_account` |
| [`q2_high_value_hourly.sql`](src/flink/q2_high_value_hourly.sql) | `banking.transactions` | none | `analytics.high_value_transaction_hourly` |
| [`q3_high_value_by_city.sql`](src/flink/q3_high_value_by_city.sql) | `banking.transactions` | account → branch | `analytics.high_value_transaction_by_city` |
| [`q4_withdrawal_by_employee.sql`](src/flink/q4_withdrawal_by_employee.sql) | `banking.transactions` | employee → branch | `analytics.withdrawal_transaction_by_employee` |
| [`q5a_customer_account_count.sql`](src/flink/q5a_customer_account_count.sql) | `banking.transactions` | account | `analytics.customer_account_count` |
| [`q5b_customer_quarterly.sql`](src/flink/q5b_customer_quarterly.sql) | `banking.transactions` | account | `analytics.customer_quarterly_summary` |
| [`q6_branch_daily_rollup.sql`](src/flink/q6_branch_daily_rollup.sql) | `banking.transactions` | account | `analytics.branch_daily_rollup` |

---

## Step 5 — Confluent Tableflow + Zero-Copy Data Federation

The easiest way to integrate the two platforms is through Confluent Tableflow. Tableflow automatically materializes Kafka topics into Iceberg open-table formats residing in your cloud storage or in Confluent storage.

### 1. Enable Tableflow in Confluent Cloud

Configure Confluent Cloud to automatically materialize your streaming Kafka topics into Iceberg open-table formats.

1. Go to Topics in your Confluent Cloud Console.
2. Click on Enable Tableflow for each of the topics.
3. Choose Iceberg as your table format.
4. Select Use Confluent storage.

### 2: Generate Confluent Iceberg Catalog Credentials

Because the data resides in the Confluent Iceberg REST Catalog, you must generate access details so watsonx.data can look up the table layouts.

1. In Confluent Cloud, navigate to Tableflow
2. Copy the `API Access` `REST Catalog Endpoint`
3. Generate a new API Key and Secret specifically for the Iceberg Catalog.
    - Click `Create/View API keys`
    - Click `Add API key`
        - Name: `tableflow_key`
        - Select account: `My account`
        - Select key scope: `Tableflow`
4. Copy the following:
    - API Key
    - API Secret

### 3: Register the Confluent Catalog in IBM watsonx.data

Configure watsonx.data environment to look across to Confluent as a external data platform without actually duplicating or importing the storage footprint.

1. Log into your IBM watsonx.data instance console.
2. Open the Infrastructure manager tab on the navigation side-panel.
3. Click Add Component and choose Add catalog.
4. Fill out the catalog creation wizard with these parameters:
    - Catalog Type: Apache Iceberg
    - Catalog Target: External REST Catalog (Select this option to use Confluent's endpoint)
    - REST URI: Paste the Catalog URI copied from Confluent Cloud.
    - Authentication: Input the API Key and API Secret generated in Step 2.
5. Provide a memorable catalog name (e.g., confluent_iceberg_stream).
6. Click Save to establish the connection.

### 4: Associate the Catalog with Your Engines

To run SQL queries against your real-time Kafka tables, your query engines need access permissions to this new catalog metadata.

1. In the Infrastructure manager, locate your newly created confluent_iceberg_stream catalog.
2. Click the options menu (three dots) next to the catalog and choose Associate engine.
3. Select your active watsonx.data query engine—such as your Presto or Spark clusters.
4. Confirm the association.

---

## Querying the Analytics Tables

### Q1 — All transactions for an account in the last 30 days

```sql
SELECT *
FROM analytics_transactions_by_account
WHERE
  account_id = 'da88aee4-a69d-445c-b766-9b4910109241'
  AND txn_day >= '20260701'
  AND txn_day <= '20260731'
ORDER BY
  txn_timestamp DESC
```

### Q2 — Transactions > $10,000 in the last 60 minutes

```sql
SELECT *
FROM analytics_high_value_transaction_hourly
WHERE
  txn_time >= current_timestamp - interval '1' hour
  AND txn_time <= current_timestamp
```

### Q3 — Count of transactions > $50,000 by city in the last 24 hours

```sql
SELECT city, txn_count
FROM
  analytics_high_value_transaction_by_city
WHERE window_start >= current_timestamp - interval '24' hour
  AND window_start <= current_timestamp
-- option for arbitrary times
--  window_start >= timestamp '2026-07-01 00:09:00'
--  AND window_start < timestamp '2026-07-01 00:10:00'
```

### Q4 — Withdrawal transactions by manager (quarter)

```sql
-- First resolve the manager's direct reports from the employee cache,
-- then query each employee's partition
SELECT * FROM withdrawal_transaction_by_employee
WHERE employee_id = <emp_uuid>
  AND quarter = '2025Q1'
```

### Q5 - Customers with > 5 active accounts AND quarterly spend > $1,000,000

```sql
SELECT
  txn.customer_id,
  txn.quarter,
  txn.amount
FROM
  analytics_customer_quarterly_txn_total AS txn
  INNER JOIN analytics_customer_account_count AS acct ON txn.customer_id = acct.customer_id
WHERE
  acct.account_count > 5
  AND txn.amount >= 1000000
```

### Q6 - Daily branch totals for a date range

```sql
SELECT txn_date, total_amount, count_txn
FROM analytics_branch_daily_rollup
WHERE
  branch_id='16c92a43-cf71-47ba-bac6-b032315e647a'
  AND txn_date >= DATE '2026-07-25'
  AND txn_date <= DATE '2026-07-31'
ORDER BY txn_date DESC
```
