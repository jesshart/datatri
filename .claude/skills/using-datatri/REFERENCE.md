# datatri reference

1. [Model](#model)
2. [Checks](#checks)
3. [triage and TriageResult](#triage-and-triageresult)
4. [Report and rollup](#report-and-rollup)
5. [Surface](#surface)
6. [Schema](#schema)
7. [Conservation](#conservation)
8. [Cascade](#cascade)
9. [Where and when to validate](#where-and-when-to-validate)
10. [Memory rules](#memory-rules)
11. [The killer case](#the-killer-case)

## Model

- A **check** is a named marker expression: True for rows that are *sick*. It never executes
  itself.
- `triage` folds every marker into one lazy `select` (the report) and returns two lazy
  filters (`healthy`, `sick`). `healthy + sick == input`, always; the healthy side re-checks
  to zero.
- A **surface** is a named group of checks for one boundary. **Tags** are free-form facets
  (`owner`, `layer`, …) that become report columns.
- **Conservation** checks bracket an *operation* (before vs after) rather than a frame.

## Checks

```python
dti.Check(id, sick: pl.Expr, dimension="validity", severity="warn", tags={}, brief=None)
```

`severity` is `"warn"` (default) or `"block"`. `brief` is one plain-language line that rides
into the report — say what a hit *means* (`"shipped after requested delivery"`), not what the
expression does; builders fill a default. `Check.marker` is `sick.fill_null(False)`: a
predicate that can't be evaluated (`qty <= 0` on a null qty) is *not* sick by that check —
nullness belongs to `not_null`. That fill is what keeps the partition complete.

| builder | sick when | dimension | notes |
|---|---|---|---|
| `dti.sick(id, expr)` | `expr` | validity | escape hatch; no default brief |
| `dti.healthy(id, expr)` | `~expr` | validity | same, phrased positively |
| `dti.not_null(col)` | null | completeness | id `col.not_null` |
| `dti.unique(cols, nulls_sick=True)` | **returns two checks**: `<key>.duplicated` (a non-null key with copies; grain) and `<key>.null` (completeness) | grain + completeness | **all** copies flagged; two nulls are *not* copies of each other; `cols` may be a list (composite, id `a+b.…`); `id=` renames the key; `nulls_sick=False` drops the `.null` check |
| `dti.in_range(col, lo, hi, closed="both")` | outside bounds | validity | either bound optional |
| `dti.in_set(col, values)` | not in values | validity | |
| `dti.matches(col, pattern)` | regex miss | validity | Rust regex: no lookarounds/backrefs |

Every builder accepts `id=`, `severity=`, `tags=`, `dimension=`, `brief=`. Check lists may
be nested — `[dti.unique("id"), dti.not_null("qty")]` flattens — so builders that return
several checks drop straight in.

Why `unique` emits a separate `.null` reason: a null key is unjoinable, but calling it a
"duplicate" misleads the owner — on real data a composite-grain check once reported 2,542
"duplicates" that were all one null column.

## triage and TriageResult

```python
r = dti.triage(frame, checks, surface=None)   # frame: DataFrame or LazyFrame
```

Raises `ValueError` only for bad *plans*: duplicate check ids, or a tag key colliding with a
report column. Never raises on bad data.

| member | type | meaning |
|---|---|---|
| `r.healthy` | LazyFrame | rows no check flagged |
| `r.sick` | LazyFrame | flagged rows + `why: list[str]` (check ids, in check order) |
| `r.report` | DataFrame | one row per check |
| `r.n_total` | int | input row count |
| `r.n_sick_by_check` | dict | `{check_id: n_failed}` |
| `r.failures()` | LazyFrame | `sick` exploded: one row per (check × row), `check_id` column |
| `r.blocked` | bool | any `severity="block"` check failed |
| `r.raise_if_blocked()` | — | raises `dti.TriageBlocked(check_ids)` |
| `r.sink(healthy_path, sick_path)` | — | `sink_parquet` both sides (two bounded streamed scans) |

Cost: the report is exactly one aggregate scan, reading only the columns the checks reference.
`healthy` / `sick` scan nothing until you materialize them.

## Report and rollup

Report columns: `check_id, surface, dimension, severity, brief, <tags…>, n_failed, n_total,
frac_failed, passed`. Tag keys are unioned across checks; a check without a tag gets null.

```python
dti.rollup(report, by)              # by: str | list[str]  ->  checks, failing, failed_rows, pass_rate
dti.rollup([r1.report, r2.report], by)   # several surfaces: stacked diagonally for you
```

Any column works: `"surface"`, `"dimension"`, `"owner"`, `["surface", "owner"]`. Pass a list to
roll up several surfaces — tag columns differ from surface to surface, so a bare `pl.concat` raises;
`rollup` (or `pl.concat(..., how="diagonal")`) stacks them with nulls where a tag is absent.

## Surface

```python
s = dti.Surface("orders-input", [checks...])
r = s.triage(frame)          # == dti.triage(frame, s.checks, surface="orders-input")
len(s); list(s)
```

Gate one surface without touching others: `s.triage(frame).blocked`.

## Schema

```python
res = dti.check_schema(frame, expected, allow_extra=False)
res.ok; res.missing; res.extra; res.wrong   # wrong: {col: (actual, expected)}
res.describe()                              # human lines
```

`expected` is a `pl.Schema` or `{name: dtype}`. Comparison is Polars' own, so parametrized
types are exact: `Datetime("us","UTC") != Datetime("ns","UTC")`, `List(Int32) != List(Int64)`,
`Decimal(18,2) != Decimal(38,9)`. Reads metadata only — zero scans. Run it before value checks.

## Conservation

```python
cons = dti.conserve(before, after, measures={"units": pl.col("units").sum()}, rows=True, tol=0.0)
cons.ok; cons.violations; cons.to_frame()     # measure, before, after, delta, ok

joined, cons = dti.safe_join(left, right, on=None, how="left", measures=..., tol=0.0, **join_kwargs)
```

`on` is optional — `left_on=` / `right_on=` pass through to Polars.

- `conserve` compares one row of aggregates per side. `rows` adds `pl.len()`. `tol` applies to
  numeric measures. `to_frame()` types `before`/`after` by content (Int64 / Float64 / String) and
  adds `delta`; rows *down* while a measure goes *up* is the fan-out signature that a row count
  alone would read as "filtering".
- `safe_join` reports conservation of the **left** side and never raises. Pass Polars' own
  guard through to hard-stop instead: `validate="m:1"` raises `ComputeError` at collect with
  no key detail — cheap happy path; run `dti.triage(right, [dti.unique(key)])` only on
  failure to name the culprit rows.
- The conservation aggregate executes the join once (one row out). Materializing `joined`
  later runs it again — peak memory over pass count, by design.
- Inner joins legitimately drop rows; read `cons` as a report of what changed, not a verdict.

Operation → what it should conserve:

| operation | conserves | guard |
|---|---|---|
| join | driving rows neither drop nor fan out | rows before == after; anti-join = 0 orphans |
| concat | out rows = Σ in rows; schemas align | Σ heights; `check_schema` per part |
| aggregate | measure total; intended grain | Σ(measure) before == after; `unique` on output key |
| filter | only intended rows removed | row delta within a band |
| dedup | only true duplicates removed | removed == duplicate count |

## Cascade

Condemn a *key*, not just a row: a SKU sick in one table is suspect in every table that references
it. Lives in `datatri.cascade`; `triage`, `check` and `surface` do not know it exists. Three rungs,
each optional — use none and nothing changes.

**Rung 1 — two ordinary checks.** You bring the keys (a list, a Series, a one-column frame).

| builder | sick when | id | whose defect |
|---|---|---|---|
| `dti.poisoned(col, keys)` | `col ∈ keys` | `col.poisoned` | the master's — the key was condemned elsewhere |
| `dti.orphan(col, keys)` | non-null `col ∉ keys` | `col.orphan` | the child producer's — no master record |

Both are `is_in(keys.implode())`: a pure expression, so the one-scan report holds. A null key is
neither poisoned nor orphaned — that reason belongs to `not_null`. Measured: 500k keys against 5M
rows in 0.04 s with the lowest memory of the join-based alternatives; a materialized key set is
small by definition.

**Rung 2 — tag the entity, harvest the keys.**

```python
rm = dti.triage(dim, [dti.unique("item_id", tags={"cascade": "sku"}),   # condemns the SKU
                      dti.not_null("units_per_case")],                  # untagged: never cascades
                surface="dim_items")
dti.condemned_keys(rm, entity="sku", col="item_id")   # DataFrame: key, origin_check
```

The tag names the **entity**; `col` names the column that carries it on *this* surface — `item_id`
on the master, `sku` on a fact. An untagged result returns an empty frame: no obligation.

**Rung 3 — the ledger, and your loop.**

```python
led = dti.Ledger()                                   # frame: entity, key, origin_surface, origin_check
led.add(result, entity, col) -> int                  # keys the result condemned for entity; returns how many were new
led.add_keys(entity, keys, origin_surface=..., origin_check=...)   # from anywhere: a ticket, a plain-Polars step
led.keys(entity, dtype=None) -> pl.Series            # stored as String; pass dtype to cast back (Int64 ids)
led.sizes() -> {entity: n};  led.write(path);  dti.Ledger.read(path)   # read of a missing file is an empty ledger

while True:
    before = led.sizes()
    r = dti.triage(dim, [..., dti.poisoned("item_id", led.keys("sku"), tags={"cascade": "sku"})], surface="dim")
    led.add(r, "sku", "item_id")
    r = dti.triage(orders, [dti.orphan("sku", dim.select("sku"), tags={"cascade": "order_id"}),
                            dti.poisoned("sku", led.keys("sku"), tags={"cascade": "order_id"}),
                            dti.sick("sku.recalled", pl.col("recall"), tags={"cascade": "sku"})], surface="orders")
    led.add(r, "order_id", "order_id"); led.add(r, "sku", "sku")     # the up-edge is just another add
    if led.sizes() == before: break
```

The loop is deliberately not a `reconcile()`: it is four lines, and keeping it in your code keeps the
policy visible. It terminates because the ledger is monotone — a key, once in, stays in for the run —
so rounds are bounded by the longest dependency chain (2–3 in practice), and the result does not
depend on surface order. Union only: never remove a key mid-run.

**Across steps.** `led.write("ledger.parquet")` in one step, `dti.Ledger.read(...)` in the next. The
schema is public (`datatri.cascade.LEDGER_SCHEMA`): a plain-Polars step can append to the file and a
bare `is_in` can consume it. Orchestrate by re-running the steps until `sizes()` stops changing.

**Policy, not mechanism, is the risk.** Only checks *you* tag cascade; a row defect (a negative qty on
one line) must not poison the SKU. Preview first — the report is a dry run by construction:
`dti.triage(child, [dti.poisoned(col, candidates)]).report` is the blast radius and nothing moves
until you route `r.sick`. On one production dataset a correctly tagged cascade grew 474 quarantined
rows into ~7,650 across five tables; a mis-tagged one would have quarantined two-thirds of every table.

**Strong vs soft form.** Cascading *through* the master's `healthy` set (children run
`orphan(col, master.healthy)` alone) yields the same sick rows but conflates orphan and poisoned into
one reason. Run both — `orphan` against the full master, `poisoned` against the ledger — so each
reason points at its owner.

## Where and when to validate

Validate where you would otherwise trust an assumption that can break silently and that
something downstream depends on. Touches (join, concat, aggregate) are where those cluster.

- **Input you don't own** → a `Surface` per input, tagged with the producer. Highest value:
  defects attribute to someone who can fix them.
- **Precondition at the operation** → assert the assumption *before* the op (`validate=`,
  `unique` on the join key) so failure points at the cause, not a symptom 200 lines later.
- **Postcondition / before publish** → triage the output before it becomes someone's input.
- **Intermediate frames** → only when the invariant is load-bearing for the rest of the
  script *and* would fail silently. Don't validate every frame; validate every assumption
  that matters.
- **Structural checks anywhere** — they're free.

Two altitudes, one primitive: call `triage` ad hoc on line 5, or bind a `Surface` at the
I/O seam so coverage doesn't depend on anyone remembering.

## Memory rules

- Counts and row-extracts can't share one pass without materializing per-row flags; that is
  why `report` (aggregate) and `healthy`/`sick` (filters) are separate. Don't "optimize" by
  collecting flags for the whole frame.
- Prefer more small or streamed passes over one big materialization.
- `assert_frame_equal` materializes both sides — reference/parity only, behind `check_schema`.
- Referential integrity against another *column*: `join(how="anti")` / `"semi"`. Against a
  materialized key *set* (a ledger, a master's keys): `is_in(keys.implode())` — small by definition,
  measured fast to 500k keys. Never `is_in(pl.col(...))` (deprecated as ambiguous).

## The killer case

```python
fact = 5 orders, 100 units
dim  = sku dimension with sku2 listed twice

joined, cons = dti.safe_join(fact, dim, on="sku", how="left", measures={"units": pl.col("units").sum()})
dti.triage(joined, [dti.not_null("category"), dti.in_range("units", 1, 1000)]).report["passed"].all()  # True
cons.to_frame()   # rows 5 -> 7, units 100 -> 150   <- the join fanned out; every row still "healthy"

d = dti.triage(dim, [dti.unique("sku")])              # both sku2 rows -> d.sick
dti.safe_join(fact, d.healthy, on="sku", how="inner", measures=...)
fact.lazy().join(d.healthy, on="sku", how="anti")     # orphaned orders, routed upstream
# kept 50 + quarantined 50 == 100: conserved, corruption contained, run alive
```

Record-level validation checks that each row is valid; conservation checks that the
*operation* was. Only the second catches a silent fan-out.
