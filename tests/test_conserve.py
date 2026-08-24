import polars as pl
import pytest

from datatri.check import in_range, in_set, not_null, unique
from datatri.conserve import conserve, safe_join
from datatri.triage import triage

UNITS = {"units": pl.col("units").sum()}


@pytest.fixture
def fact():
    return pl.DataFrame(
        {"order_id": [1, 2, 3, 4, 5], "sku": ["sku1", "sku2", "sku2", "sku3", "sku1"], "units": [10, 20, 30, 25, 15]}
    )


@pytest.fixture
def dirty_dim():
    # sku2 twice: a dirty dimension that fans out any join against it
    return pl.DataFrame(
        {"sku": ["sku1", "sku2", "sku2", "sku3"], "category": ["A", "B", "B", "C"], "price": [1.0, 2.0, 2.0, 3.0]}
    )


def test_conserve_passes_when_nothing_changes(fact):
    r = conserve(fact, fact.filter(pl.lit(True)), UNITS)
    assert r.ok and r.violations == []
    assert r.to_frame()["ok"].all()


def test_conserve_reports_every_measure(fact):
    r = conserve(fact, fact.head(3), UNITS)
    got = {m.measure: (m.before, m.after, m.ok) for m in r.measures}
    assert got == {"rows": (5, 3, False), "units": (100, 60, False)}
    assert not r.ok


def test_conserve_tolerance_and_validation(fact):
    f = fact.with_columns(pl.col("units").cast(pl.Float64))
    r = conserve(f, f.with_columns(pl.col("units") + 1e-9), {"u": pl.col("units").sum()}, rows=False, tol=1e-6)
    assert r.ok
    with pytest.raises(ValueError):
        conserve(fact, fact, rows=False)


def test_killer_case_record_checks_pass_but_conservation_fails(fact, dirty_dim):
    joined, cons = safe_join(fact, dirty_dim, on="sku", how="left", measures=UNITS)

    # a full record-level suite on the output: every row is individually valid
    suite = [not_null("category"), not_null("price"), in_range("units", 1, 1000), in_set("category", ["A", "B", "C"])]
    assert triage(joined, suite).report["passed"].all()

    # ...and the data is corrupt: silently fanned out, measure doubled where it matters
    assert {m.measure: (m.before, m.after) for m in cons.violations} == {"rows": (5, 7), "units": (100, 150)}
    by_cat = joined.group_by("category").agg(pl.col("units").sum()).sort("category").collect()
    assert by_cat.filter(pl.col("category") == "B")["units"][0] == 100  # truth is 50


def test_validate_passthrough_is_the_blocking_guard(fact, dirty_dim):
    # Polars' own guard rides through: raises at the conservation collect, no key detail.
    with pytest.raises(pl.exceptions.ComputeError):
        safe_join(fact, dirty_dim, on="sku", validate="m:1")
    # the diagnostic runs only on failure and names the culprit rows
    culprits = triage(dirty_dim, [unique("sku")]).sick.collect()
    assert culprits["sku"].to_list() == ["sku2", "sku2"]


def test_triage_the_dim_then_join_healthy_conserves_the_total(fact, dirty_dim):
    dim = triage(dirty_dim, [unique("sku")])
    assert dim.sick.collect().height == 2  # both copies to the doctor

    # `on` is optional: left_on/right_on flow through to Polars
    j, c = safe_join(fact, dim.healthy, how="inner", left_on="sku", right_on="sku")
    assert c.measures[0].after == 3

    joined_healthy, cons = safe_join(fact, dim.healthy, on="sku", how="inner", measures=UNITS)
    orphans = fact.lazy().join(dim.healthy, on="sku", how="anti")

    kept = joined_healthy.select(pl.col("units").sum()).collect().item()
    quarantined = orphans.select(pl.col("units").sum()).collect().item()
    assert kept + quarantined == 100  # conserved: corruption contained, sku2 orders routed upstream
    assert not cons.ok  # and the inner join honestly reports the rows it dropped
    assert [m.measure for m in cons.violations] == ["rows", "units"]
