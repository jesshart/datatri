"""A Check is a named, tagged marker for sick rows.

The expression is True for rows that FAIL — the "sick marker". Group-level
markers such as ``is_duplicated`` condemn every row of the implicated group,
so both copies of a duplicate key go to the doctor, not just the extra one.

A check yields an *expression*, never an executor: the runner folds every
marker into one lazy pass. That is what keeps triage to a single scan.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

import polars as pl

SEVERITIES = ("warn", "block")


@dataclass(frozen=True, eq=False)
class Check:
    id: str
    sick: pl.Expr
    dimension: str = "validity"
    severity: str = "warn"
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("check id must be non-empty")
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}, got {self.severity!r}")
        object.__setattr__(self, "tags", dict(self.tags))

    @property
    def marker(self) -> pl.Expr:
        """The sick marker, null-safe.

        A predicate that cannot be evaluated (e.g. ``qty <= 0`` on a null qty)
        yields null; that row is *not* sick by this check — nullness is the
        completeness check's job. Filling here is what keeps the healthy/sick
        partition complete: a null mask would drop the row from both sides.
        """
        return self.sick.fill_null(False)


# --- builders ---------------------------------------------------------------


def sick(id: str, expr: pl.Expr, **kw) -> Check:
    """A check from an expression that is True for sick rows."""
    return Check(id, expr, **kw)


def healthy(id: str, expr: pl.Expr, **kw) -> Check:
    """A check from an expression that is True for healthy rows."""
    return Check(id, ~expr, **kw)


def not_null(col: str, *, id: str | None = None, **kw) -> Check:
    kw.setdefault("dimension", "completeness")
    return Check(id or f"{col}.not_null", pl.col(col).is_null(), **kw)


def unique(
    cols: str | Iterable[str],
    *,
    id: str | None = None,
    nulls_sick: bool = True,
    **kw,
) -> Check:
    """Grain check: every copy of a duplicated key is sick.

    ``is_duplicated`` flags all rows of a duplicate group. It treats two nulls
    as duplicates of each other but lets a *single* null key through — and a
    null key is still unjoinable — so by default a null key is sick too.
    """
    kw.setdefault("dimension", "grain")
    cols = [cols] if isinstance(cols, str) else list(cols)
    key = pl.col(cols[0]) if len(cols) == 1 else pl.struct(cols)
    marker = key.is_duplicated()
    if nulls_sick:
        marker = marker | pl.any_horizontal([pl.col(c).is_null() for c in cols])
    return Check(id or f"{'+'.join(cols)}.unique", marker, **kw)


def in_range(
    col: str,
    lo=None,
    hi=None,
    *,
    closed: str = "both",
    id: str | None = None,
    **kw,
) -> Check:
    if lo is None and hi is None:
        raise ValueError("in_range needs at least one bound")
    parts = []
    if lo is not None:
        parts.append(pl.col(col) >= lo if closed in ("both", "left") else pl.col(col) > lo)
    if hi is not None:
        parts.append(pl.col(col) <= hi if closed in ("both", "right") else pl.col(col) < hi)
    return Check(id or f"{col}.in_range", ~pl.all_horizontal(parts), **kw)


def in_set(col: str, values: Iterable, *, id: str | None = None, **kw) -> Check:
    return Check(id or f"{col}.in_set", ~pl.col(col).is_in(list(values)), **kw)


def matches(col: str, pattern: str, *, id: str | None = None, **kw) -> Check:
    """Format validity. Polars regex is the Rust crate: no lookarounds or backrefs."""
    return Check(id or f"{col}.matches", ~pl.col(col).str.contains(pattern), **kw)
