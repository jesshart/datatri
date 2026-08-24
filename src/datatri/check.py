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
    brief: str | None = None  # one plain-language line for the report; say what a hit MEANS

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


def flatten(checks: Iterable[Check | Iterable[Check]]) -> tuple[Check, ...]:
    """Accept checks or nested lists of checks (builders like ``unique`` return several)."""
    out: list[Check] = []
    for c in checks:
        if isinstance(c, Check):
            out.append(c)
        elif isinstance(c, (str, bytes)):
            raise TypeError(f"expected a Check, got {c!r}")
        else:
            out.extend(flatten(c))
    return tuple(out)


# --- builders ---------------------------------------------------------------


def sick(id: str, expr: pl.Expr, **kw) -> Check:
    """A check from an expression that is True for sick rows."""
    return Check(id, expr, **kw)


def healthy(id: str, expr: pl.Expr, **kw) -> Check:
    """A check from an expression that is True for healthy rows."""
    return Check(id, ~expr, **kw)


def not_null(col: str, *, id: str | None = None, **kw) -> Check:
    kw.setdefault("dimension", "completeness")
    kw.setdefault("brief", f"{col} must not be null")
    return Check(id or f"{col}.not_null", pl.col(col).is_null(), **kw)


def unique(
    cols: str | Iterable[str],
    *,
    id: str | None = None,
    nulls_sick: bool = True,
    **kw,
) -> tuple[Check, ...]:
    """Grain check, as two checks with their own reasons: ``<key>.duplicated``
    and (by default) ``<key>.null``.

    Every copy of a duplicated key is sick. A null key is unjoinable, so it is
    sick too — but as a *completeness* failure, never counted as a duplicate:
    two null keys are not copies of each other. ``id`` sets the key name used
    in both ids. Pass the result straight into a check list; it flattens.
    """
    cols = [cols] if isinstance(cols, str) else list(cols)
    name = id or "+".join(cols)
    label = "+".join(cols)
    user_dim = kw.pop("dimension", None)
    brief = kw.pop("brief", None)
    key = pl.col(cols[0]) if len(cols) == 1 else pl.struct(cols)
    is_null = pl.any_horizontal([pl.col(c).is_null() for c in cols])
    dup = Check(
        f"{name}.duplicated",
        key.is_duplicated() & ~is_null,
        dimension=user_dim or "grain",
        brief=brief or f"{label} must be unique",
        **kw,
    )
    if not nulls_sick:
        return (dup,)
    null = Check(
        f"{name}.null",
        is_null,
        dimension=user_dim or "completeness",
        brief=f"{label} must not be null",
        **kw,
    )
    return (dup, null)


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
    parts, words = [], []
    if lo is not None:
        inclusive = closed in ("both", "left")
        parts.append(pl.col(col) >= lo if inclusive else pl.col(col) > lo)
        words.append(f"{'>=' if inclusive else '>'} {lo}")
    if hi is not None:
        inclusive = closed in ("both", "right")
        parts.append(pl.col(col) <= hi if inclusive else pl.col(col) < hi)
        words.append(f"{'<=' if inclusive else '<'} {hi}")
    kw.setdefault("brief", f"{col} must be {' and '.join(words)}")
    return Check(id or f"{col}.in_range", ~pl.all_horizontal(parts), **kw)


def in_set(col: str, values: Iterable, *, id: str | None = None, **kw) -> Check:
    values = list(values)
    shown = values if len(values) <= 5 else values[:5] + ["…"]
    kw.setdefault("brief", f"{col} must be one of {shown}")
    return Check(id or f"{col}.in_set", ~pl.col(col).is_in(values), **kw)


def matches(col: str, pattern: str, *, id: str | None = None, **kw) -> Check:
    """Format validity. Polars regex is the Rust crate: no lookarounds or backrefs."""
    kw.setdefault("brief", f"{col} must match {pattern}")
    return Check(id or f"{col}.matches", ~pl.col(col).str.contains(pattern), **kw)
