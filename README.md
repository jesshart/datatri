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
dti.rollup([r.report, other.report], "surface")   # many surfaces: stacked diagonally for you

r.sink("healthy.parquet", "sick.parquet")  # stream both sides; never a full-frame collect
r.raise_if_blocked()                        # opt-in: only `severity="block"` checks can stop you
```

## Validate the operation, not just the rows

A record-level suite says every row is healthy. A join can still silently fan out.

```python
joined, cons = dti.safe_join(fact, dim, on="sku", measures={"units": pl.col("units").sum()})
cons.ok                  # False: rows 5 -> 7, units 100 -> 150
cons.to_frame()          # measure, before, after, delta, ok
```

Quarantine the dirty dimension first, then join the healthy side — the total is conserved.

## Condemn the key, not just the row (optional)

A sick SKU in one table is suspect in every table that references it. Cascade is opt-in, one rung
at a time; use none of it and nothing changes.

```python
# rung 1 — two ordinary checks; you bring the keys
dti.poisoned("sku", recalled_skus)             # sku ∈ keys condemned elsewhere   -> sku.poisoned (the master's defect)
dti.orphan("sku", dim.select("sku"))           # non-null sku with no master row  -> sku.orphan   (the producer's defect)

# rung 2 — tag a check with the ENTITY it condemns, then harvest the keys from the result
rm = dti.triage(dim, [dti.unique("item_id", tags={"cascade": "sku"})], surface="dim")
dti.condemned_keys(rm, entity="sku", col="item_id")     # key, origin_check   (entity != column on a master)

# rung 3 — a ledger between surfaces, and between pipeline steps via parquet
led = dti.Ledger()
while True:                                              # the loop is yours — four lines keep the policy visible
    before = led.sizes()
    led.add(dti.triage(dim, [dti.unique("item_id", tags={"cascade": "sku"}),
                             dti.poisoned("item_id", led.keys("sku"), tags={"cascade": "sku"})], surface="dim"), "sku", "item_id")
    led.add(dti.triage(orders, [dti.poisoned("sku", led.keys("sku"), tags={"cascade": "order_id"})], surface="orders"), "order_id", "order_id")
    if led.sizes() == before: break
led.frame                                                # entity, key, origin_surface, origin_check — the provenance
led.write("ledger.parquet"); dti.Ledger.read("ledger.parquet")
```

Preview before you cascade: the report is a dry run, so `dti.triage(orders, [dti.poisoned("sku", candidates)]).report`
is the blast radius and nothing moves until you route `r.sick`. Only checks you tag cascade — a negative
quantity on one line must not poison the SKU.

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
| `cascade`     | `poisoned()`, `orphan()`, `condemned_keys()`, `Ledger` — condemn a key, not a row (optional) |

```
uv run pytest
uv run python examples/walkthrough.py
```
