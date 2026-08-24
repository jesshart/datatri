import polars as pl
import pytest

from datatri.check import Check, healthy, in_range, in_set, matches, not_null, sick, unique


def flags(df: pl.DataFrame, check: Check) -> list[bool]:
    return df.select(check.marker).to_series().to_list()


def test_unique_condemns_every_copy_with_its_own_reason():
    df = pl.DataFrame({"sku": ["sku1", "sku2", "sku2", "sku3"]})
    dup, null = unique("sku")
    assert flags(df, dup) == [False, True, True, False]
    assert flags(df, null) == [False] * 4
    assert (dup.id, null.id) == ("sku.duplicated", "sku.null")
    assert (dup.dimension, null.dimension) == ("grain", "completeness")


def test_unique_null_key_is_completeness_not_duplication():
    df = pl.DataFrame({"sku": ["a", None, "b", None]})
    dup, null = unique("sku")
    assert flags(df, dup) == [False] * 4  # two nulls are NOT copies of each other
    assert flags(df, null) == [False, True, False, True]
    (only,) = unique("sku", nulls_sick=False)
    assert only.id == "sku.duplicated"


def test_unique_composite_key():
    df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "x"]})
    dup, _ = unique(["a", "b"])
    assert flags(df, dup) == [True, True, False]
    assert [c.id for c in unique(["a", "b"])] == ["a+b.duplicated", "a+b.null"]
    assert [c.id for c in unique(["a", "b"], id="grain")] == ["grain.duplicated", "grain.null"]
    assert [c.dimension for c in unique("a", dimension="key")] == ["key", "key"]


def test_briefs_default_and_override():
    assert not_null("c").brief == "c must not be null"
    assert in_range("v", 0, 10).brief == "v must be >= 0 and <= 10"
    assert in_range("v", hi=5, closed="none").brief == "v must be < 5"
    assert in_set("s", ["A", "B"]).brief == "s must be one of ['A', 'B']"
    assert matches("e", "^x$").brief == "e must match ^x$"
    assert [c.brief for c in unique("k")] == ["k must be unique", "k must not be null"]
    assert sick("x", pl.lit(True)).brief is None
    assert sick("x", pl.lit(True), brief="shipped after requested delivery").brief == "shipped after requested delivery"
    assert not_null("c", brief="custom").brief == "custom"


def test_not_null():
    df = pl.DataFrame({"c": [1, None]})
    c = not_null("c")
    assert flags(df, c) == [False, True]
    assert c.dimension == "completeness"


def test_in_range_bounds_and_closedness():
    df = pl.DataFrame({"v": [0, 5, 10, 11]})
    assert flags(df, in_range("v", 0, 10)) == [False, False, False, True]
    assert flags(df, in_range("v", 0, 10, closed="none")) == [True, False, True, True]
    assert flags(df, in_range("v", hi=5)) == [False, False, True, True]
    with pytest.raises(ValueError):
        in_range("v")


def test_in_set_and_matches():
    df = pl.DataFrame({"s": ["A", "X"], "e": ["a@b.co", "nope"]})
    assert flags(df, in_set("s", ["A", "B"])) == [False, True]
    assert flags(df, matches("e", r"^[^@]+@[^@]+\.[^@]+$")) == [False, True]


def test_healthy_is_negated_sick():
    df = pl.DataFrame({"v": [1, -1]})
    assert flags(df, sick("neg", pl.col("v") < 0)) == [False, True]
    assert flags(df, healthy("pos", pl.col("v") > 0)) == [False, True]


def test_marker_is_null_safe():
    # `qty <= 0` on a null qty is not sick by THIS check; completeness owns nullness.
    df = pl.DataFrame({"qty": [1, None, -1]})
    assert flags(df, sick("qty.pos", pl.col("qty") <= 0)) == [False, False, True]


def test_check_validation():
    with pytest.raises(ValueError):
        Check("", pl.lit(True))
    with pytest.raises(ValueError):
        Check("x", pl.lit(True), severity="fatal")


def test_tags_are_copied_and_defaults():
    tags = {"owner": "OMS"}
    c = Check("x", pl.lit(True), tags=tags)
    tags["owner"] = "changed"
    assert c.tags == {"owner": "OMS"}
    assert c.severity == "warn" and c.dimension == "validity"
