# datatri reference

1. [Model](#model)
2. [Checks](#checks)
3. [triage and TriageResult](#triage-and-triageresult)
4. [Report and rollup](#report-and-rollup)
5. [Surface](#surface)
6. [Schema](#schema)
7. [Conservation](#conservation)
8. [Where and when to validate](#where-and-when-to-validate)
9. [Memory rules](#memory-rules)
10. [The killer case](#the-killer-case)

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
dti.Check(id, sick: pl.Expr, dimension="validity", severity="warn", tags={})
```

`severity` is `"warn"` (default) or `"block"`. `Check.marker` is `sick.fill_null(False)`:
a predicate that can't be evaluated (`qty <= 0` on a null qty) is *not* sick by that check —
nullness belongs to `not_null`. That fill is what keeps the partition complete.

| builder | sick when | dimension | notes |
|---|---|---|---|
| `dti.sick(id, expr)` | `expr` | validity | escape hatch |
| `dti.healthy(id, expr)` | `~expr` | validity | same, phrased positively |
| `dti.not_null(col)` | null | completeness | id `col.not_null` |
| `dti.unique(cols, nulls_sick=True)` | key duplicated, or null | grain | **all** copies flagged; `cols` may be a list (composite key); id `a+b.unique` |
| `dti.in_range(col, lo, hi, closed="both")` | outside bounds | validity | either bound optional |
| `dti.in_set(col, values)` | not in values | validity | |
| `dti.matches(col, pattern)` | regex miss | validity | Rust regex: no lookarounds/backrefs |

Every builder accepts `id=`, `severity=`, `tags=`, `dimension=`.

Why `unique` flags null keys: `is_duplicated()` treats two nulls as duplicates but lets a
*single* null through, and a null key is still unjoinable.

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

Report columns: `check_id, surface, dimension, severity, <tags…>, n_failed, n_total,
frac_failed, passed`. Tag keys are unioned across checks; a check without a tag gets null.

```python
dti.rollup(report, by)   # by: str | list[str]  ->  checks, failing, failed_rows, pass_rate
```

Any column works: `"surface"`, `"dimension"`, `"owner"`, `["surface", "owner"]`. Concatenate
reports from several surfaces before rolling up.

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
cons.ok; cons.violations; cons.to_frame()     # measure, before, after, ok

joined, cons = dti.safe_join(left, right, on, how="left", measures=..., tol=0.0, **join_kwargs)
```

- `conserve` compares one row of aggregates per side. `rows` adds `pl.len()`. `tol` applies to
  numeric measures.
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
- Referential integrity: `join(how="anti")` / `"semi"`, not `is_in(column)` (deprecated as
  ambiguous, and it materializes the keys).

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
