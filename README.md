# Real-Time Banking Analytics

Pre-processes live banking transactions so dashboards can query low-latency, pre-aggregated results without reprocessing raw transactional data on every refresh.

## How It Works

![Dataflow Diagram](assets/dataflow_opt1.png)


## Components

| Component | Role |
|---|---|
| **Confluent Cloud Kafka** | Transport for both dimension snapshots and the live transaction stream |
| **Confluent Cloud Flink** | Stream processing — enriches and aggregates transactions in real time using Flink Materialized Tables |
| **IBM watsonx.data** | SQL query layer over analytics topics via Confluent Tableflow + Iceberg federation |
| **DataStax AstraDB (ODS)** | Source of truth for dimension data and optional destination for pre-aggregated analytics tables |
| **python scripts** | Script that generate synthetic data to simulate banking operational dataflow |

## Analytics Queries

| # | Flink Job | What it produces |
|---|---|---|
| Q1 | `flink/q1_txn_by_account.sql` | All transactions per account, indexed by day |
| Q2 | `flink/q2_high_value_hourly.sql` | Transactions > $10,000, partitioned by minute |
| Q3 | `flink/q3_high_value_by_city.sql` | Hourly count of transactions > $50,000 per city |
| Q4 | `flink/q4_withdrawal_by_employee.sql` | Withdrawal transactions per employee, by quarter |
| Q5a | `flink/q5a_customer_account_count.sql` | Active account count per customer |
| Q5b | `flink/q5b_customer_quarterly.sql` | Rolling quarterly spend total per customer |
| Q6 | `flink/q6_branch_daily_rollup.sql` | Daily transaction total and count per branch |

## Repository Layout

```
src/
  dimension_loader.py        Reads AstraDB ODS dimensions → publishes to Kafka
  database_schema.cql        ODS source schema (customer, account, transaction, …)
  analytics_schema.cql       Analytics query table DDL for AstraDB
flink/
  q1_txn_by_account.sql
  q2_high_value_hourly.sql
  q3_high_value_by_city.sql
  q4_withdrawal_by_employee.sql
  q5a_customer_account_count.sql
  q5b_customer_quarterly.sql
  q6_branch_daily_rollup.sql
scripts/
  setup.sh                   Create Python venv and install dependencies
```

## Setup

See [`README_analytics_flink.md`](README_analytics_flink.md) for full step-by-step instructions:
environment variables, topic creation, dimension loading, Flink job deployment, and sink worker setup.
