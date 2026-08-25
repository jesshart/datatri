"""Conservation: validate the operation, not just the rows.

A record-level suite checks that each row is valid. A touch — a join, a
stack, an aggregate — can corrupt the *relationship between* rows while
every row stays individually valid. Conservation brackets the operation:
did the quantities that should survive it actually survive?

The guard is one row of aggregates per side. Cheap enough to ride every
operation wrapper by default.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from numbers import Number

import polars as pl


@dataclass(frozen=True)
class Conservation:
    measure: str
    before: object
    after: object
    tol: float = 0.0

    @property
    def ok(self) -> bool:
        if isinstance(self.before, Number) and isinstance(self.after, Number):
            return abs(self.after - self.before) <= self.tol
        return self.before == self.after


@dataclass(frozen=True)
class ConserveResult:
    measures: tuple[Conservation, ...]

    @property
    def ok(self) -> bool:
        return all(m.ok for m in self.measures)

    @property
    def violations(self) -> list[Conservation]:
        return [m for m in self.measures if not m.ok]

    def to_frame(self) -> pl.DataFrame:
        """One row per measure: ``measure, before, after, delta, ok``.

        ``before``/``after`` are typed by what the measures hold — Int64 when all are
        integers, Float64 when a row count sits next to a float sum (the common case),
        String otherwise. ``delta`` is ``after - before`` for numeric measures. A join
        that drops rows *and* grows a measure shows as a negative and a positive delta
        side by side: the fan-out signature a row count alone would call "filtering".
        """
        before = _typed([m.before for m in self.measures])
        after = _typed([m.after for m in self.measures])
        numeric = before.dtype.is_numeric() and after.dtype.is_numeric()
        delta = (after - before) if numeric else pl.Series([None] * len(self.measures), dtype=pl.Float64)
        return pl.DataFrame(
            {
                "measure": [m.measure for m in self.measures],
                "before": before,
                "after": after,
                "delta": delta,
                "ok": [m.ok for m in self.measures],
            }
        )


def conserve(
    before: pl.DataFrame | pl.LazyFrame,
    after: pl.DataFrame | pl.LazyFrame,
    measures: Mapping[str, pl.Expr] | None = None,
    *,
    rows: bool = True,
    tol: float = 0.0,
) -> ConserveResult:
    """Compare aggregate measures across an operation. Never raises on a violation."""
    exprs: dict[str, pl.Expr] = {}
    if rows:
        exprs["rows"] = pl.len()
    exprs.update(measures or {})
    if not exprs:
        raise ValueError("nothing to conserve: pass measures or rows=True")
    aggs = [e.alias(k) for k, e in exprs.items()]
    b = _lazy(before).select(aggs).collect().row(0, named=True)
    a = _lazy(after).select(aggs).collect().row(0, named=True)
    return ConserveResult(tuple(Conservation(k, b[k], a[k], tol) for k in exprs))


def safe_join(
    left: pl.DataFrame | pl.LazyFrame,
    right: pl.DataFrame | pl.LazyFrame,
    on: str | list[str] | None = None,
    *,
    how: str = "left",
    measures: Mapping[str, pl.Expr] | None = None,
    tol: float = 0.0,
    **join_kwargs,
) -> tuple[pl.LazyFrame, ConserveResult]:
    """Join, then report conservation of the driving (left) side.

    ``on`` may be omitted in favour of ``left_on=`` / ``right_on=``.
    Reports, never raises. To hard-stop instead, pass Polars' own guard
    through: ``validate="m:1"`` raises at collect with no offending-key
    detail — the cheap happy path; run the diagnostic only on failure.

    The conservation aggregate executes the join once (bounded: one row
    out). Materializing ``joined`` later runs it again — peak memory over
    pass count, by design.
    """
    lf, rf = _lazy(left), _lazy(right)
    joined = lf.join(rf, on=on, how=how, **join_kwargs)
    return joined, conserve(lf, joined, measures, tol=tol)


def _lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def _typed(values: list) -> pl.Series:
    present = [v for v in values if v is not None]
    if all(isinstance(v, int) and not isinstance(v, bool) for v in present):
        return pl.Series(values, dtype=pl.Int64)
    if all(isinstance(v, Number) and not isinstance(v, bool) for v in present):
        return pl.Series([None if v is None else float(v) for v in values], dtype=pl.Float64)
    return pl.Series([None if v is None else str(v) for v in values], dtype=pl.String)
