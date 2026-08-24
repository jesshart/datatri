import polars as pl
import pytest


@pytest.fixture
def tap():
    """Wrap a frame so every real scan of `col` bumps a counter.

    `collect_schema()` must not bump it (metadata only). Checks in the test
    must reference `col`, or projection pushdown prunes the tap entirely.
    """

    def _make(df: pl.DataFrame, col: str):
        counter = {"n": 0}

        def _t(s: pl.Series) -> pl.Series:
            counter["n"] += 1
            return s

        lf = df.lazy().with_columns(
            pl.col(col).map_batches(_t, return_dtype=df.schema[col]).alias(col)
        )
        return lf, counter

    return _make


@pytest.fixture
def dirty():
    """A small frame with one of every kind of trouble."""
    return pl.DataFrame(
        {
            "id": [1, 2, 2, 3, None, 5],  # dup 2, null key
            "amount": [10, 2000, 30, 40, 50, -1],  # 2000 and -1 out of range
            "status": ["A", "B", "X", "A", "B", "C"],  # X out of set
            "qty": [1, None, 3, 4, 5, 6],  # one null
        }
    )
