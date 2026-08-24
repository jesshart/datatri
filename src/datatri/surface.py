"""A Surface is the named contract for one boundary.

An input, an output, or an operation gets a surface: a Composite of checks
that is bound, run, scored, and gated as a unit. One check belongs to one
surface; cross-cutting facets (dimension, owner, severity) live in tags and
slice the same checks other ways.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import polars as pl

from datatri.check import Check, flatten
from datatri.triage import TriageResult, triage


@dataclass(frozen=True)
class Surface:
    name: str
    checks: tuple[Check, ...]

    def __init__(self, name: str, checks: Iterable[Check | Iterable[Check]]) -> None:
        if not name:
            raise ValueError("surface name must be non-empty")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "checks", flatten(checks))

    def triage(self, frame: pl.DataFrame | pl.LazyFrame) -> TriageResult:
        return triage(frame, self.checks, surface=self.name)

    def __iter__(self) -> Iterator[Check]:
        return iter(self.checks)

    def __len__(self) -> int:
        return len(self.checks)
