# datatri

Triage data instead of failing on it. Pure Polars, lazy, one pass.

```python
import polars as pl
import datatri as dti

checks = [
    dti.unique("order_id"),                   # -> order_id.duplicated + order_id.null, each its own reason
    dti.not_null("ship_to"),
    dti.in_range("qty", 1, 999),
    dti.in_set("region", ["NA", "EU"], tags={"owner": "Planning"}),
    dti.sick("eu.needs_ship_to", (pl.col("region") == "EU") & pl.col("ship_to").is_null(),
             brief="EU orders need a ship-to"),   # say what a hit MEANS; builders fill a default
]

r = dti.triage(orders, checks, surface="orders-input")

r.report            # one row per check: brief, n_failed, n_total, frac_failed, passed, + tags
r.healthy           # LazyFrame — flows on
r.sick              # LazyFrame — carries `why: list[str]`, the checks that condemned each row
r.failures()        # one row per (check × condemned row)
dti.rollup(r.report, "owner")

r.sink("healthy.parquet", "sick.parquet")  # stream both sides; never a full-frame collect
r.raise_if_blocked()                        # opt-in: only `severity="block"` checks can stop you
```

## Validate the operation, not just the rows

A record-level suite says every row is healthy. A join can still silently fan out.

```python
joined, cons = dti.safe_join(fact, dim, on="sku", measures={"units": pl.col("units").sum()})
cons.ok                  # False: rows 5 -> 7, units 100 -> 150
cons.to_frame()
```

Quarantine the dirty dimension first, then join the healthy side — the total is conserved.

## The rules the design is built on

- **A check is an expression, never an executor.** The runner folds every marker into one lazy
  `select`; the report costs one scan regardless of check count.
- **Optimize peak memory, not pass count.** `healthy` and `sick` come back lazy; sink them.
- **Structural checks are free.** `check_schema` compares `collect_schema()` — zero data read.
- **Never fail the run.** Bad data is quarantined with a reason; blocking is opt-in per check.

## Layout

| module        | what it owns                                                   |
|---------------|----------------------------------------------------------------|
| `check`       | `Check` + builders (`unique`, `not_null`, `in_range`, `sick`, `healthy`, …) |
| `surface`     | `Surface` — a named group of checks bound to one boundary      |
| `triage`      | `triage()`, `TriageResult`, `rollup()`                         |
| `schema`      | `check_schema()` — dtype-exact, free                           |
| `conserve`    | `conserve()`, `safe_join()` — operation-level checks           |

```
uv run pytest
uv run python examples/walkthrough.py
```
