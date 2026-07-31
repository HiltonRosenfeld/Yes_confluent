# TODO

## Next Steps

Update dimension_loader.py to use avro schemas - MAYBE

## QUERY

Whats up with Flink query: -- Q5b — High-value customers quarterly

## README

create and update

## TEST

## REMEMBER

- Add watermark to transaction topic/table

```sql
ALTER TABLE `banking.transactions`
MODIFY (
    WATERMARK FOR txn_time AS txn_time - INTERVAL '5' SECOND
);
```
