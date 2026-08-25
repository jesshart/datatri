"""Optional cascade support: condemn a KEY, not just a row.

Nothing in ``check``, ``triage`` or ``surface`` knows this module exists; it composes
over the public API. Use none of it and datatri behaves exactly as before. The
ladder, each rung optional:

1. ``poisoned(col, keys)`` / ``orphan(col, keys)`` — ordinary checks. You manage the
   key set yourself; they are just ``is_in`` expressions with good ids and briefs.
2. ``tags={"cascade": "<entity>"}`` on any check + ``condemned_keys(result, entity, col)``
   — turns a triage result into the keys that check condemned. The tag names the
   *entity* (``sku``); ``col`` names the column that carries it on *this* surface
   (``item_id`` on a master, ``sku`` on a fact).
3. ``Ledger`` — where those keys accumulate: ``(entity, key, origin_surface,
   origin_check)``, union-only (monotone), parquet round-trip so separate pipeline
   steps can share it.

The reconciliation loop is deliberately *not* here. It is four lines of your code —
re-triage until ``ledger.sizes()`` stops changing — so the policy stays visible.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from datatri.check import Check
from datatri.triage import TriageResult

CASCADE = "cascade"  # the tag key
LEDGER_SCHEMA = {
    "entity": pl.String,
    "key": pl.String,
    "origin_surface": pl.String,
    "origin_check": pl.String,
}


def _keys(keys) -> pl.Series:
    if isinstance(keys, pl.LazyFrame):
        keys = keys.collect()
    if isinstance(keys, pl.DataFrame):
        keys = keys.to_series(0)
    if not isinstance(keys, pl.Series):
        keys = pl.Series(list(keys))
    return keys.drop_nulls().unique()


# --- rung 1: two ordinary checks ----------------------------------------------------------


def poisoned(col: str, keys, *, id: str | None = None, **kw) -> Check:
    """Sick when ``col`` is one of ``keys`` — a key condemned elsewhere. Null keys are not sick
    here; nullness is ``not_null``'s job."""
    ks = _keys(keys)
    kw.setdefault("dimension", "consistency")
    kw.setdefault("brief", f"{col} was condemned elsewhere ({len(ks)} keys)")
    return Check(id or f"{col}.poisoned", pl.col(col).is_in(ks.implode()), **kw)


def orphan(col: str, keys, *, id: str | None = None, **kw) -> Check:
    """Sick when a non-null ``col`` is not in ``keys`` — no master record. Distinct from
    ``poisoned`` on purpose: an orphan is the child producer's defect, a poisoned key the master's."""
    ks = _keys(keys)
    kw.setdefault("dimension", "consistency")
    kw.setdefault("brief", f"{col} has no master record")
    return Check(id or f"{col}.orphan", ~pl.col(col).is_in(ks.implode()) & pl.col(col).is_not_null(), **kw)


# --- rung 2: from a result to keys ----------------------------------------------------------


def condemned_keys(result: TriageResult, entity: str, col: str) -> pl.DataFrame:
    """Distinct ``col`` values condemned by checks tagged ``cascade=<entity>`` in ``result``,
    with the first check that hit each: columns ``key``, ``origin_check``. Empty when no check
    carries the tag — so calling it on a plain triage costs nothing and changes nothing."""
    empty = pl.DataFrame(schema={"key": pl.String, "origin_check": pl.String})
    rep = result.report
    if CASCADE not in rep.columns:
        return empty
    ids = rep.filter(pl.col(CASCADE) == entity)["check_id"].to_list()
    if not ids:
        return empty
    return (
        result.failures()
        .filter(pl.col("check_id").is_in(ids) & pl.col(col).is_not_null())
        .group_by(pl.col(col).cast(pl.String).alias("key"), maintain_order=True)
        .agg(pl.col("check_id").first().alias("origin_check"))
        .collect()
    )


# --- rung 3: the ledger -----------------------------------------------------------------------


@dataclass
class Ledger:
    """Condemned keys across surfaces. Union-only: a key, once in, stays in for the run —
    that monotonicity is what makes the reconciliation loop terminate."""

    frame: pl.DataFrame = field(default_factory=lambda: pl.DataFrame(schema=LEDGER_SCHEMA))

    def add(self, result: TriageResult, entity: str, col: str) -> int:
        """Fold the keys ``result`` condemned for ``entity`` in. Returns how many were new."""
        ck = condemned_keys(result, entity, col)
        surface = result.report["surface"][0] if result.report.height else None
        return self.add_keys(entity, ck["key"], origin_surface=surface, origin_check=ck["origin_check"])

    def add_keys(self, entity: str, keys, *, origin_surface: str | None = None, origin_check=None) -> int:
        """Add keys from anywhere — a plain-Polars step, a ticket, a hand-typed list."""
        if isinstance(keys, pl.LazyFrame):
            keys = keys.collect()
        if isinstance(keys, pl.DataFrame):
            keys = keys.to_series(0)
        ks = pl.Series(list(keys)) if not isinstance(keys, pl.Series) else keys
        ks = ks.cast(pl.String)
        per_key = isinstance(origin_check, Iterable) and not isinstance(origin_check, str)
        oc = pl.Series(list(origin_check), dtype=pl.String) if per_key else pl.Series([origin_check] * len(ks), dtype=pl.String)
        if len(oc) != len(ks):
            raise ValueError(f"origin_check has {len(oc)} entries for {len(ks)} keys")
        # Dedupe keys and origins TOGETHER, in order — never re-sort one column without the other.
        new = (
            pl.DataFrame({"key": ks, "origin_check": oc})
            .drop_nulls("key")
            .unique(subset=["key"], keep="first", maintain_order=True)
            .select(
                pl.lit(entity, dtype=pl.String).alias("entity"),
                "key",
                pl.lit(origin_surface, dtype=pl.String).alias("origin_surface"),
                "origin_check",
            )
        )
        before = self.frame.height
        self.frame = pl.concat([self.frame, new]).unique(subset=["entity", "key"], keep="first", maintain_order=True)
        return self.frame.height - before

    def keys(self, entity: str, dtype: pl.DataType | None = None) -> pl.Series:
        s = self.frame.filter(pl.col("entity") == entity)["key"]
        return s.cast(dtype) if dtype is not None else s

    def sizes(self) -> dict[str, int]:
        return {r["entity"]: r["n"] for r in self.frame.group_by("entity").agg(n=pl.len()).sort("entity").to_dicts()}

    def __len__(self) -> int:
        return self.frame.height

    def write(self, path: str | Path) -> None:
        self.frame.write_parquet(path)

    @classmethod
    def read(cls, path: str | Path) -> Ledger:
        p = Path(path)
        return cls(pl.read_parquet(p)) if p.exists() else cls()
