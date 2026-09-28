# Day 11 — Broadcast and Accumulators

Broadcast a small product master map, count bad records with accumulators,
and validate a transaction file against the master table.

## Run

```bash
sbt "run local[4]"
```

## Concepts

- **Broadcast variable**: a read-only value shipped to each executor once
  and shared by all tasks there, instead of being serialized into every
  task's closure. Ideal for small lookup tables (a map-side join, no shuffle).
- **Accumulator**: a write-only-from-tasks counter. Tasks can only `add`;
  Spark merges the contributions on the driver, and only the driver reads
  the value. `sc.longAccumulator` counts, `sc.collectionAccumulator` gathers samples.
- **Why not a plain driver variable?** Spark serializes the closure, so every
  task increments its own copy. The driver's original never changes (it stays
  `0`), even in local mode.
- **Caveat**: accumulator updates inside *transformations* can be applied
  more than once if the RDD is recomputed (a later action or a task retry).
  The scenario calls `cache()` and reads the counters after a single action.

## Scenario

`transactions.txt` has 16 records. Validation rules: 4 fields, numeric
quantity and price, product must exist in the master table, quantity > 0,
price must match the master price.

Expected: 8 valid, 8 bad (2 malformed, 3 unknown product, 2 bad quantity,
1 price mismatch), total valid revenue Rs. 16285.00.

## Push to GitHub

```bash
git init && git add . && git commit -m "Day 11: Broadcast and accumulators"
git branch -M main
git remote add origin https://github.com/seemran13/day11-broadcast-accumulators.git
git push -u origin main
```
