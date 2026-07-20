# Confluent Cloud Flink — Cassandra Sink Connector Handoff

This document lists every Materialized Table output topic produced by the Flink
processing layer, along with the message key, value schema fields, AstraDB target
table, and recommended Cassandra Sink Connector write mode.

Configure one Confluent Cloud fully-managed Cassandra Sink Connector per topic.
Refer to `src/analytics_schema.cql` for the authoritative AstraDB DDL.

---

## Q1 — Transactions by account

| Property | Value |
|---|---|
| **Topic** | `analytics.transactions_by_account` |
| **Message key** | `account_id` (STRING) |
| **AstraDB target table** | `transactions_by_account` |
| **Write mode** | `insert` (append-only; every row is new) |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `account_id` | STRING | UUID | Partition key |
| `txn_day` | STRING | text | YYYYMMDD UTC; SAI-indexed |
| `txn_timestamp` | TIMESTAMP_LTZ(3) | timestamp | Clustering key DESC |
| `txn_id` | STRING | UUID | |
| `currency` | STRING | text | |
| `txn_type` | STRING | text | |
| `channel` | STRING | text | |
| `status` | STRING | text | |
| `product_id` | STRING | UUID | |
| `approved_by_emp_id` | STRING | UUID | Nullable |
| `amount` | DOUBLE | double | |

---

## Q2 — High-value transactions > 10,000 (hourly)

| Property | Value |
|---|---|
| **Topic** | `analytics.high_value_transaction_hourly` |
| **Message key** | `txn_minute` (STRING) |
| **AstraDB target table** | `high_value_transaction_hourly` |
| **Write mode** | `insert` (append-only; filtered rows only) |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `txn_minute` | STRING | text | YYYYMMDDHHmm UTC; partition key |
| `txn_time` | TIMESTAMP_LTZ(3) | timestamp | Clustering key DESC |
| `txn_id` | STRING | UUID | Clustering key ASC |
| `account_id` | STRING | UUID | |
| `currency` | STRING | text | |
| `amount` | DOUBLE | double | Filter: > 10,000 |
| `channel` | STRING | text | |
| `status` | STRING | text | |

---

## Q3 — High-value transactions > 50,000 by city

| Property | Value |
|---|---|
| **Topic** | `analytics.high_value_transaction_by_city` |
| **Message key** | `bucket_hour` (STRING) |
| **AstraDB target table** | `high_value_transaction_by_city` |
| **Write mode** | `insert` (append-only; filtered + enriched rows) |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `bucket_hour` | STRING | text | YYYYMMDDHH UTC; partition key |
| `city` | STRING | text | From dim_branch; clustering key ASC |
| `account_id` | STRING | UUID | |
| `txn_id` | STRING | UUID | Clustering key ASC |
| `amount` | DOUBLE | double | Filter: > 50,000 |

---

## Q4 — Withdrawal transactions by employee

| Property | Value |
|---|---|
| **Topic** | `analytics.withdrawal_transaction_by_employee` |
| **Message key** | `employee_id` (STRING) |
| **AstraDB target table** | `withdrawal_transaction_by_employee` |
| **Write mode** | `insert` (append-only; filtered + enriched rows) |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `employee_id` | STRING | UUID | Compound partition key (with quarter) |
| `quarter` | STRING | text | YYYYQn UTC; compound partition key |
| `txn_time` | TIMESTAMP_LTZ(3) | timestamp | Clustering key DESC |
| `txn_id` | STRING | UUID | Clustering key ASC |
| `account_id` | STRING | UUID | |
| `currency` | STRING | text | |
| `amount` | DOUBLE | double | |
| `branch_name` | STRING | text | From dim_branch |

---

## Q5a — Customer quarterly transaction total

| Property | Value |
|---|---|
| **Topic** | `analytics.customer_quarterly_txn_total` |
| **Message key** | `customer_id` (STRING) |
| **AstraDB target table** | `customer_quarterly_txn_total` |
| **Write mode** | **`upsert`** — topic carries retract+upsert changelog; connector must UPDATE existing rows |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `customer_id` | STRING | UUID | Partition key |
| `quarter` | STRING | text | YYYYQn UTC; clustering key |
| `amount` | DOUBLE | double | Running SUM — updated on every new transaction |

> **Connector note:** configure `insert.mode = upsert` and set the primary key columns
> (`customer_id`, `quarter`) as the upsert key so UPDATE statements are issued.

---

## Q5b — High-value customers quarterly

| Property | Value |
|---|---|
| **Topic** | `analytics.high_value_customer_quarterly` |
| **Message key** | `customer_id` (STRING) |
| **AstraDB target table** | `high_value_customer_quarterly` |
| **Write mode** | **`upsert`** — derived from Q5a upsert stream; rows appear/disappear as thresholds are crossed |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `quarter` | STRING | text | YYYYQn UTC; partition key |
| `customer_id` | STRING | UUID | Clustering key |
| `amount` | DOUBLE | double | Filtered: > 1,000,000 AND active_account_count > 5 |

> **Connector note:** configure `insert.mode = upsert`. Retract messages (when a
> customer's total drops below threshold) translate to DELETE operations in AstraDB.

---

## Q6 — Branch daily rollup

| Property | Value |
|---|---|
| **Topic** | `analytics.branch_daily_rollup` |
| **Message key** | `branch_id` (STRING) |
| **AstraDB target table** | `branch_daily_rollup` |
| **Write mode** | **`upsert`** — continuous GROUP BY; running count and sum are updated on every transaction |

### Value schema fields

| Field | Flink type | AstraDB type | Notes |
|---|---|---|---|
| `branch_id` | STRING | UUID | Compound partition key (with txn_year) |
| `txn_year` | STRING | text | YYYY UTC; compound partition key |
| `txn_date` | DATE | date | Clustering key ASC |
| `total_amount` | DOUBLE | double | Running SUM(amount) for this branch+day |
| `count_txn` | BIGINT | bigint | Running COUNT(*) for this branch+day |

> **Connector note:** configure `insert.mode = upsert` with upsert key
> (`branch_id`, `txn_year`, `txn_date`) to issue UPDATE statements against
> the AstraDB table (which has compound partition key `(branch_id, txn_year)`
> and clustering column `txn_date`).

---

## Out-of-scope: customer_account_count

The `customer_account_count` counter table in AstraDB is **not written by Flink**.
It is seeded by `dimension_loader.py` via the `analytics.account_active_events`
Kafka topic (one event per active account). Flink reads this topic in Q5b to
derive `active_account_count` as a Flink-side filter; no Cassandra Sink Connector
is needed for this topic.
