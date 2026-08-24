---
name: using-datatri
description: Uses datatri to triage Polars frames instead of failing on bad data — folds named checks into one lazy pass, partitions rows into healthy/sick with a per-row `why`, rolls the report up by surface/owner/dimension, and brackets joins with conservation checks that catch silent fan-out. Use when validating a DataFrame or LazyFrame, quarantining bad rows, checking grain/uniqueness/nulls/ranges/referential integrity, guarding a join or aggregate against fan-out or row loss, or writing data-quality checks in Polars.
allowed-tools: Bash(uv:*) Bash(python:*) Read Write Edit
---

# Using datatri

Validate a Polars frame by *partitioning* it — healthy rows flow on, sick rows go to
quarantine carrying the checks that condemned them — and never fail the run unless asked.

**Reference**: [REFERENCE.md](REFERENCE.md) — full API, check semantics, where-to-validate guide.

## Quick start

```python
import polars as pl
import datatri as dti

r = dti.triage(frame, [
    dti.unique("order_id"),                      # -> order_id.duplicated + order_id.null
    dti.not_null("ship_to"),
    dti.in_range("qty", 1, 999),
    dti.in_set("region", ["NA", "EU"], tags={"owner": "Planning"}),
    dti.sick("eu.needs_ship_to", (pl.col("region") == "EU") & pl.col("ship_to").is_null(),
             brief="EU orders need a ship-to"),  # what a hit MEANS, for the owner
], surface="orders-input")

r.report                 # one row per check: brief, n_failed, n_total, frac_failed, passed, + tags
r.healthy                # LazyFrame — flows on
r.sick                   # LazyFrame — `why: list[str]` names every check that hit the row
r.failures()             # one row per (check × condemned row)
dti.rollup(r.report, "owner")
r.sink("healthy.parquet", "sick.parquet")   # stream both sides; never a full-frame collect
```

## Workflow

1. **Find the boundary.** Validate where an assumption is silently relied on: an input you
   don't control (attribute defects to its producer), an output before publish, or a *touch* —
   a join, concat, or aggregate. Not every intermediate frame; every load-bearing assumption.
2. **Write checks as expressions.** Built-ins for the common cases; `dti.sick(id, expr)` /
   `dti.healthy(id, expr)` for anything else — cross-field rules, business logic. Name them
   `<column>.<rule>`, tag them (`owner`, `severity`) so the report rolls up, and give any
   check whose mechanical name could mislead a `brief=` that says what a hit means
   (`arrival_date < ship_date` on a *requested* delivery date is "shipped late", not corruption).
3. **Triage.** `dti.triage(frame, checks, surface=...)` — one lazy pass for the report;
   `healthy` / `sick` come back lazy. Group checks into a `dti.Surface` when the set is a
   contract you'll reuse or gate.
4. **Route.** Sink `sick` (with `why`) where the owner can see it; let `healthy` continue;
   `dti.rollup(report, by)` for the scorecard. Only `severity="block"` + `raise_if_blocked()`
   stops the run.
5. **Guard the touch.** Wrap joins in `dti.safe_join(..., measures={...})` and read
   `cons.to_frame()`. A record-level suite on a joined output passes while a dirty dimension
   doubles a total; conservation (`rows`, plus a business measure) is what catches it.

## Where to validate — decision table

| Operation | Silently breaks | Guard |
|---|---|---|
| join | fan-out, orphans, null-key matches | `safe_join(..., measures)`; `validate="m:1"` to hard-stop; anti-join for orphans |
| concat / stack | schema drift, dup introduction | `check_schema` on each part; `unique` after |
| aggregate | grain change, double count | `unique` on the output key; `conserve` a measure |
| filter / dedup | silent row loss | `conserve(before, after)`; expect a bounded delta |
| read from a source you don't own | everything above | a `Surface` per input, tagged with its owner |

## Rules

- **A check is an expression, never a collect.** Never loop `.collect()` per check; hand the
  list to `triage` and it folds them into one scan.
- **Keep `healthy` / `sick` lazy.** `sink()` them or feed them onward; only `.collect()` for
  small frames or tests. Peak memory is the number to minimize, not pass count.
- **Never fail the run by default.** Bad data is quarantined with a reason. Blocking is opt-in
  per check (`severity="block"`) and explicit (`raise_if_blocked()`).
- **Grain checks condemn the whole group.** `unique()` flags every copy of a duplicate key
  (`<key>.duplicated`) and, separately, a null key (`<key>.null`) — that is intended; don't
  "fix" it to flag only the extra row, and don't merge the two reasons.
- **Structural checks are free — run them first.** `dti.check_schema` reads no data.
- **Referential integrity at scale is an anti-join,** not `is_in(column)`.
- `uv run pytest` and `uv run python examples/walkthrough.py` are the living spec.
