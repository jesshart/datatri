import polars as pl
import pytest

from datatri.schema import check_schema


def test_exact_match_is_ok():
    df = pl.DataFrame({"a": [1], "b": ["x"]})
    r = check_schema(df, {"a": pl.Int64, "b": pl.String})
    assert r.ok and r.describe() == []


def test_missing_extra_wrong():
    df = pl.DataFrame({"a": [1], "b": ["x"]}).with_columns(pl.col("a").cast(pl.Int32))
    r = check_schema(df, {"a": pl.Int64, "c": pl.Float64})
    assert r.missing == {"c"}
    assert r.extra == {"b"}
    assert r.wrong == {"a": (pl.Int32, pl.Int64)}
    assert not r.ok
    assert check_schema(df, {"a": pl.Int32}, allow_extra=True).ok


def test_parametrized_dtypes_are_compared_exactly():
    df = pl.DataFrame({"t": [0], "l": [[1]], "d": [1.5]}).with_columns(
        pl.col("t").cast(pl.Datetime("us")).dt.replace_time_zone("UTC"),
        pl.col("l").cast(pl.List(pl.Int32)),
        pl.col("d").cast(pl.Decimal(18, 2)),
    )
    exp = {"t": pl.Datetime("ns", "UTC"), "l": pl.List(pl.Int64), "d": pl.Decimal(38, 9)}
    r = check_schema(df, exp)
    assert set(r.wrong) == {"t", "l", "d"}
    assert check_schema(df, df.schema).ok


def test_schema_check_never_scans_data():
    def boom(s: pl.Series) -> pl.Series:
        raise AssertionError("schema check must not execute the plan")

    lf = pl.DataFrame({"a": [1, 2]}).lazy().with_columns(
        pl.col("a").map_batches(boom, return_dtype=pl.Int64)
    )
    assert check_schema(lf, {"a": pl.Int64}).ok  # would raise if it collected
    with pytest.raises((AssertionError, pl.exceptions.PolarsError)):
        lf.collect()  # prove the trap is live
