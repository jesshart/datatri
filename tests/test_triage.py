import polars as pl
import pytest

from datatri.check import Check, in_range, in_set, not_null, sick, unique
from datatri.triage import TriageBlocked, rollup, triage

CHECKS = [
    unique("id"),
    not_null("qty"),
    in_range("amount", 0, 1000),
    in_set("status", ["A", "B", "C"]),
]


def test_partition_is_complete_and_disjoint(dirty):
    r = triage(dirty, CHECKS)
    healthy, sick_ = r.healthy.collect(), r.sick.collect()
    assert healthy.height + sick_.height == dirty.height
    assert r.n_total == dirty.height
    # the healthy side re-checks to zero violations
    assert triage(healthy, CHECKS).report["n_failed"].sum() == 0


def test_report_counts(dirty):
    r = triage(dirty, CHECKS)
    assert r.n_sick_by_check == {
        "id.duplicated": 2,  # two copies of 2
        "id.null": 1,  # the null key, its own reason
        "qty.not_null": 1,
        "amount.in_range": 2,
        "status.in_set": 1,
    }
    assert set(r.report.columns) >= {
        "check_id", "surface", "dimension", "severity", "brief", "n_failed", "n_total", "frac_failed", "passed"
    }
    assert r.report["passed"].to_list() == [False] * 5
    assert r.report.filter(pl.col("check_id") == "id.duplicated")["frac_failed"][0] == pytest.approx(2 / 6)


def test_sick_rows_carry_every_reason(dirty):
    r = triage(dirty, CHECKS)
    sick_ = r.sick.collect()
    why = dict(zip(sick_["amount"], sick_["why"].to_list(), strict=True))
    # derived row by row from the `dirty` fixture; reasons follow CHECKS order
    assert why == {
        2000: ["id.duplicated", "qty.not_null", "amount.in_range"],  # id 2 dup, qty null, out of range
        30: ["id.duplicated", "status.in_set"],  # id 2 dup, status X
        50: ["id.null"],  # null key — a completeness reason, not a duplicate
        -1: ["amount.in_range"],  # id 5 is unique; only the range trips
    }
    assert r.healthy.collect()["amount"].to_list() == [10, 40]


def test_sick_reasons_exact():
    df = pl.DataFrame({"id": [1, 1, 2], "v": [5, 500, -5]})
    r = triage(df, [unique("id"), in_range("v", 0, 100)])
    got = {(i, v, tuple(w)) for i, v, w in r.sick.collect().select("id", "v", "why").rows()}
    assert got == {
        (1, 5, ("id.duplicated",)),
        (1, 500, ("id.duplicated", "v.in_range")),
        (2, -5, ("v.in_range",)),
    }


def test_failures_is_one_row_per_check_x_row():
    df = pl.DataFrame({"id": [1, 1, 2], "v": [5, 500, -5]})
    r = triage(df, [unique("id"), in_range("v", 0, 100)])
    f = r.failures().collect()
    assert f.height == 4  # 2 (unique) + 2 (range)
    assert f["check_id"].value_counts().sort("check_id").rows() == [("id.duplicated", 2), ("v.in_range", 2)]


def test_null_predicates_do_not_lose_rows():
    # `v <= 0` is null on the null row; without fill_null the row would vanish from BOTH sides.
    df = pl.DataFrame({"v": [1, None, -1]})
    r = triage(df, [sick("v.pos", pl.col("v") <= 0)])
    assert r.healthy.collect().height + r.sick.collect().height == 3
    assert r.healthy.collect()["v"].to_list() == [1, None]


def test_zero_checks_everything_is_healthy(dirty):
    r = triage(dirty, [])
    assert r.report.height == 0
    assert r.n_total == dirty.height
    assert r.healthy.collect().height == dirty.height
    assert r.sick.collect().height == 0
    assert r.sick.collect().schema["why"] == pl.List(pl.String)


def test_empty_frame():
    df = pl.DataFrame({"v": []}, schema={"v": pl.Int64})
    r = triage(df, [in_range("v", 0, 1)])
    assert r.n_total == 0
    assert r.report["frac_failed"][0] == 0.0
    assert r.report["passed"][0]


def test_duplicate_ids_and_reserved_tags_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        triage(pl.DataFrame({"v": [1]}), [sick("x", pl.lit(True)), sick("x", pl.lit(False))])
    with pytest.raises(ValueError, match="collide"):
        triage(pl.DataFrame({"v": [1]}), [Check("x", pl.lit(True), tags={"passed": "y"})])


def test_never_raises_but_can_block(dirty):
    checks = [unique("id", severity="block"), in_set("status", ["A", "B", "C"])]
    r = triage(dirty, checks)  # no exception on bad data
    assert r.blocked
    with pytest.raises(TriageBlocked) as e:
        r.raise_if_blocked()
    assert e.value.check_ids == ["id.duplicated", "id.null"]
    assert not triage(dirty, [in_set("status", ["A", "B", "C"])]).blocked


def test_tags_become_report_columns_and_roll_up(dirty):
    checks = [
        unique("id", tags={"owner": "OMS"}),
        not_null("qty", tags={"owner": "OMS"}),
        in_range("amount", 0, 1000, tags={"owner": "Data"}),
        in_set("status", ["A", "B", "C"]),  # no owner tag -> null
    ]
    r = triage(dirty, checks, surface="orders-input")
    assert r.report["surface"].unique().to_list() == ["orders-input"]
    by_owner = rollup(r.report, "owner")
    assert by_owner.filter(pl.col("owner") == "OMS").row(0, named=True) == {
        "owner": "OMS", "checks": 3, "failing": 3, "failed_rows": 4, "pass_rate": 0.0
    }  # unique("id") contributes two checks (duplicated + null), tags reach both
    assert by_owner.filter(pl.col("owner").is_null())["checks"][0] == 1
    by_dim = rollup(r.report, "dimension")
    assert by_dim["dimension"].to_list() == ["completeness", "grain", "validity"]


def test_report_is_a_single_pass(tap):
    df = pl.DataFrame({"id": [1, 2, 2, None], "amount": [1, 5000, 3, 4], "status": ["A", "X", "A", "B"]})
    lf, counter = tap(df, "id")

    lf.select(pl.col("id").is_null().sum()).collect()  # baseline: one aggregate's worth of scans
    base = counter["n"]
    assert base >= 1

    counter["n"] = 0
    r = triage(lf, [not_null("id"), unique("id"), in_range("amount", 0, 1000), in_set("status", ["A", "B"])])
    assert counter["n"] == base, "four checks + grain must cost exactly one aggregate pass"

    # healthy / sick are lazy: nothing scanned until asked
    assert counter["n"] == base
    r.sick.collect()
    assert counter["n"] == 2 * base


def test_sink_streams_both_sides(dirty, tmp_path):
    r = triage(dirty, CHECKS)
    r.sink(tmp_path / "healthy.parquet", tmp_path / "sick.parquet")
    healthy = pl.read_parquet(tmp_path / "healthy.parquet")
    sick_ = pl.read_parquet(tmp_path / "sick.parquet")
    assert healthy.height + sick_.height == dirty.height
    assert "why" in sick_.columns and "why" not in healthy.columns


def test_report_carries_brief(dirty):
    r = triage(dirty, [not_null("qty"), sick("late", pl.lit(False), brief="shipped after requested delivery")])
    assert r.report["brief"].to_list() == ["qty must not be null", "shipped after requested delivery"]
    assert triage(dirty, [sick("x", pl.lit(False))]).report["brief"].to_list() == [None]


def test_nested_check_lists_flatten_and_strings_are_rejected(dirty):
    r = triage(dirty, [unique("id"), [not_null("qty"), [in_range("amount", 0, 1000)]]])
    assert r.report["check_id"].to_list() == ["id.duplicated", "id.null", "qty.not_null", "amount.in_range"]
    with pytest.raises(TypeError):
        triage(dirty, ["id.unique"])


def test_accepts_lazy_and_eager(dirty):
    a = triage(dirty, CHECKS)
    b = triage(dirty.lazy(), CHECKS)
    assert a.report.equals(b.report) and a.n_total == b.n_total == dirty.height
