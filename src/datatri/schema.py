"""Structural checks. Free: ``collect_schema()`` reads metadata, never data.

Polars' own ``Schema`` is the most faithful validator of Polars types that
can exist, because it *is* the types — exact and parametrized, so
``Datetime("us", "UTC") != Datetime("ns", "UTC")`` and
``List(Int64) != List(Int32)`` for free.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class SchemaResult:
    missing: frozenset[str]
    extra: frozenset[str]
    wrong: dict[str, tuple[pl.DataType, pl.DataType]]  # col -> (actual, expected)

    @property
    def ok(self) -> bool:
        return not (self.missing or self.extra or self.wrong)

    def describe(self) -> list[str]:
        out = [f"missing column {c!r}" for c in sorted(self.missing)]
        out += [f"unexpected column {c!r}" for c in sorted(self.extra)]
        out += [f"{c!r}: got {a}, expected {e}" for c, (a, e) in sorted(self.wrong.items())]
        return out


def check_schema(
    frame: pl.DataFrame | pl.LazyFrame,
    expected: pl.Schema | Mapping[str, pl.DataType | type[pl.DataType]],
    *,
    allow_extra: bool = False,
) -> SchemaResult:
    actual = frame.collect_schema()
    expected = pl.Schema(expected)
    missing = frozenset(expected.names()) - frozenset(actual.names())
    extra = frozenset() if allow_extra else frozenset(actual.names()) - frozenset(expected.names())
    wrong = {
        c: (actual[c], expected[c])
        for c in expected.names()
        if c in actual and actual[c] != expected[c]
    }
    return SchemaResult(missing=missing, extra=extra, wrong=wrong)
