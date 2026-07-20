# Confluent Cloud Flink — State and Checkpoint Notes

## Checkpoint management

Confluent Cloud Flink (managed service) handles checkpointing automatically.
No user configuration is required for checkpoint interval, storage backend, or
state backend — these are managed by the platform. State is persisted to
durable cloud storage; jobs resume from the latest successful checkpoint after
any planned or unplanned interruption.

## Continuous aggregation durability (Q5 and Q6)

Q5 (`analytics_customer_quarterly_txn`) and Q6 (`analytics_branch_daily_rollup`)
use non-windowed `GROUP BY` aggregations. Flink maintains an in-state accumulator
per group key (e.g. `(customer_id, quarter)` for Q5; `(branch_id, txn_year, txn_date)`
for Q6). At each checkpoint, these accumulators are serialised and saved.

On restart, Flink restores the accumulators from the last checkpoint and replays
only the Kafka messages that arrived after that checkpoint's committed offset.
Running totals are **never reset to zero** — they resume from the checkpointed value.

## Upsert changelog semantics

`GROUP BY` aggregations emit a **retract+upsert changelog**: when a group's value
changes, Flink first emits a retract (DELETE) for the old value and then an upsert
(INSERT/UPDATE) for the new value. The Materialized Table topic carries these
changelog events.

The **Cassandra Sink Connector** consuming Q5 and Q6 topics must be configured with
`insert.mode = upsert` so that UPDATE events correctly overwrite the running total
in AstraDB rather than inserting duplicate rows.

Q1–Q4 topics are append-only (no aggregation); the connector for these can use
`insert.mode = insert`.

## State growth

Continuous `GROUP BY` state grows as new group keys are observed (new quarters,
new branch+day combinations). Old quarters and dates are **not automatically expired**
from Flink state. If long-running jobs accumulate excessive state, configure
idle state retention at the Confluent Cloud Flink job level:
`table.exec.state.ttl = <duration>` (e.g. `'100 d'` to retain 100 days of state).
