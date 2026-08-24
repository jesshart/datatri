"""triage(frame, checks) -> (healthy, sick, report). Never fails the run.

Two phases, forced by memory rather than taste:

1. **Aggregate pass** — every marker folded into ONE lazy ``select`` that
   materializes a single row. This is the report. Its cost is one scan,
   regardless of how many checks, and only the referenced columns are read.
2. **Partition** — ``healthy`` and ``sick`` are returned *lazy*. Nothing is
   materialized until you sink or collect them, so the whole frame is never
   held in memory on the tool's account. Peak size, not pass count, is the
   number to minimize.

The sick side carries ``why: list[str]`` — the ids of every check that
condemned the row — which is what the upstream owner needs to act.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from datatri.check import Check

N_COL = "__n_total__"
WHY = "why"
RESERVED = frozenset(
    {"check_id", "surface", "dimension", "severity", "n_failed", "n_total", "frac_failed", "passed"}
)


class TriageBlocked(Exception):
    """Raised only on request — triage itself never raises on bad data."""

    def __init__(self, check_ids: list[str]) -> None:
        self.check_ids = check_ids
        super().__init__(f"blocking checks failed: {', '.join(check_ids)}")


@dataclass(frozen=True)
class TriageResult:
    healthy: pl.LazyFrame
    sick: pl.LazyFrame
    report: pl.DataFrame
    n_total: int

    @property
    def n_sick_by_check(self) -> dict[str, int]:
        return dict(zip(self.report["check_id"], self.report["n_failed"], strict=True))

    @property
    def blocked(self) -> bool:
        """True if any ``severity="block"`` check failed. The Proxy mode, opt-in."""
        return bool(self._blocking_failures())

    def raise_if_blocked(self) -> None:
        failed = self._blocking_failures()
        if failed:
            raise TriageBlocked(failed)

    def failures(self) -> pl.LazyFrame:
        """One row per (check x condemned row) — the keystone grain."""
        # every sick row carries >= 1 reason, so no empty lists to explode
        return self.sick.explode(WHY, empty_as_null=False).rename({WHY: "check_id"})

    def sink(self, healthy_path: str | Path, sick_path: str | Path) -> None:
        """Stream both sides to parquet. Two scans, each bounded — never a full-frame collect."""
        self.healthy.sink_parquet(healthy_path)
        self.sick.sink_parquet(sick_path)

    def _blocking_failures(self) -> list[str]:
        hit = self.report.filter((pl.col("severity") == "block") & ~pl.col("passed"))
        return hit["check_id"].to_list()


def triage(
    frame: pl.DataFrame | pl.LazyFrame,
    checks: Iterable[Check],
    *,
    surface: str | None = None,
) -> TriageResult:
    lf = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    checks = tuple(checks)
    _validate(checks)

    # Phase 1 — one folded pass, one row out.
    aggs = [c.marker.sum().alias(c.id) for c in checks] + [pl.len().alias(N_COL)]
    row = lf.select(aggs).collect().row(0, named=True)
    report = _report(checks, row, surface)

    # Phase 2 — lazy partition; the caller decides how (and whether) to materialize.
    if checks:
        mask = pl.any_horizontal([c.marker for c in checks])
        why = pl.concat_list(
            [
                pl.when(c.marker).then(pl.lit(c.id)).otherwise(pl.lit(None, dtype=pl.String))
                for c in checks
            ]
        ).list.drop_nulls()
    else:
        mask = pl.lit(False)
        why = pl.lit(None).cast(pl.List(pl.String))

    healthy = lf.filter(~mask)
    sick = lf.filter(mask).with_columns(why.alias(WHY))
    return TriageResult(healthy=healthy, sick=sick, report=report, n_total=int(row[N_COL]))


def rollup(report: pl.DataFrame, by: str | list[str]) -> pl.DataFrame:
    """Score any facet of the report: a surface, a dimension, an owner tag."""
    return (
        report.group_by(by)
        .agg(
            checks=pl.len(),
            failing=(~pl.col("passed")).sum(),
            failed_rows=pl.col("n_failed").sum(),
        )
        .with_columns(pass_rate=1 - pl.col("failing") / pl.col("checks"))
        .sort(by)
    )


# --- internals --------------------------------------------------------------


def _validate(checks: tuple[Check, ...]) -> None:
    ids = [c.id for c in checks]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ValueError(f"duplicate check ids: {dupes}")
    if N_COL in ids:
        raise ValueError(f"{N_COL!r} is reserved")
    collisions = sorted(RESERVED & {k for c in checks for k in c.tags})
    if collisions:
        raise ValueError(f"tag keys collide with report columns: {collisions}")


def _report(checks: tuple[Check, ...], row: dict, surface: str | None) -> pl.DataFrame:
    tag_keys = sorted({k for c in checks for k in c.tags})
    schema = {
        "check_id": pl.String,
        "surface": pl.String,
        "dimension": pl.String,
        "severity": pl.String,
        **{k: pl.String for k in tag_keys},
        "n_failed": pl.UInt32,
        "n_total": pl.UInt32,
    }
    records = [
        {
            "check_id": c.id,
            "surface": surface,
            "dimension": c.dimension,
            "severity": c.severity,
            **{k: c.tags.get(k) for k in tag_keys},
            "n_failed": row[c.id],
            "n_total": row[N_COL],
        }
        for c in checks
    ]
    df = pl.from_dicts(records, schema=schema) if records else pl.DataFrame(schema=schema)
    return df.with_columns(
        frac_failed=pl.when(pl.col("n_total") > 0)
        .then(pl.col("n_failed") / pl.col("n_total"))
        .otherwise(0.0),
        passed=pl.col("n_failed") == 0,
    )
