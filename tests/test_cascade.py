import polars as pl
import pytest

import datatri as dti


def test_poisoned_and_orphan_are_plain_checks():
    lf = pl.LazyFrame({"sku": ["A", "B", None, "Z"]})
    r = dti.triage(lf, [dti.poisoned("sku", ["B"]), dti.orphan("sku", ["A", "B"]), dti.not_null("sku")])
    assert r.n_sick_by_check == {"sku.poisoned": 1, "sku.orphan": 1, "sku.not_null": 1}
    why = dict(r.sick.select("sku", "why").collect().iter_rows())
    assert why == {"B": ["sku.poisoned"], None: ["sku.not_null"], "Z": ["sku.orphan"]}  # null is never an orphan


def test_condemned_keys_is_empty_without_the_tag():
    r = dti.triage(pl.LazyFrame({"sku": ["A", "A"]}), [dti.unique("sku")])
    assert r.n_sick_by_check["sku.duplicated"] == 2
    assert dti.condemned_keys(r, "sku", "sku").height == 0
    assert "cascade" not in r.report.columns


def test_condemned_keys_entity_is_not_the_column():
    master = pl.LazyFrame({"item_id": ["A", "A", "B", "C"], "upc": [1, 1, 0, 2]})
    checks = [dti.unique("item_id", tags={"cascade": "sku"}), dti.in_range("upc", 1, tags={"cascade": "sku"}),
              dti.not_null("upc")]  # untagged: never cascades
    r = dti.triage(master, checks, surface="dim_items")
    ck = dti.condemned_keys(r, "sku", "item_id").sort("key")
    assert ck.to_dicts() == [{"key": "A", "origin_check": "item_id.duplicated"}, {"key": "B", "origin_check": "upc.in_range"}]
    assert dti.condemned_keys(r, "other_entity", "item_id").height == 0


def test_ledger_is_monotone_and_round_trips(tmp_path):
    led = dti.Ledger()
    master = pl.LazyFrame({"item_id": ["A", "A", "B"]})
    r = dti.triage(master, [dti.unique("item_id", tags={"cascade": "sku"})], surface="dim_items")
    assert led.add(r, "sku", "item_id") == 1
    assert led.add(r, "sku", "item_id") == 0  # union only
    assert led.add_keys("sku", ["Q"], origin_surface="hand", origin_check="ticket-42") == 1
    assert led.sizes() == {"sku": 2}
    assert led.keys("sku").sort().to_list() == ["A", "Q"]
    led.write(tmp_path / "ledger.parquet")
    back = dti.Ledger.read(tmp_path / "ledger.parquet")
    assert back.frame.equals(led.frame)
    assert dti.Ledger.read(tmp_path / "missing.parquet").sizes() == {}
    # keys feed straight back into a check on another surface
    fact = pl.LazyFrame({"sku": ["A", "B", "Q"]})
    r2 = dti.triage(fact, [dti.poisoned("sku", back.keys("sku"))])
    assert r2.n_sick_by_check["sku.poisoned"] == 2


def test_int_keys_cast_back():
    led = dti.Ledger(); led.add_keys("id", [1, 2])
    r = dti.triage(pl.LazyFrame({"id": [1, 3]}), [dti.poisoned("id", led.keys("id", dtype=pl.Int64))])
    assert r.n_sick_by_check["id.poisoned"] == 1


def test_ledger_keeps_key_and_origin_aligned():
    """Regression: many keys with distinct origins must stay paired (unique() must not reorder one column)."""
    orders = pl.LazyFrame({"order_id": [f"O{i}" for i in range(1, 11)],
                           "sku": ["S1","S3","S2","S9","S1","S2","S5",None,"S6","S6"],
                           "customer": ["C1","C1","C2","C2","C1","C3","C2","C1","C2","C1"]})
    C = {"cascade": "order_id"}
    r = dti.triage(orders, [dti.orphan("sku", ["S1","S2","S3","S5","S6"], tags=C),
                            dti.poisoned("sku", ["S3","S5","S6"], tags=C), dti.poisoned("customer", ["C3"], tags=C)], surface="orders")
    truth = dict(r.failures().select("order_id", "check_id").collect().iter_rows())
    led = dti.Ledger(); led.add(r, "order_id", "order_id")
    got = dict(led.frame.select("key", "origin_check").iter_rows())
    assert got == truth == {"O2": "sku.poisoned", "O4": "sku.orphan", "O6": "customer.poisoned",
                            "O7": "sku.poisoned", "O9": "sku.poisoned", "O10": "sku.poisoned"}
    with pytest.raises(ValueError):
        led.add_keys("x", ["a", "b"], origin_check=["only-one"])
