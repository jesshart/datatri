import polars as pl
import pytest

from datatri.check import in_set, not_null, unique
from datatri.surface import Surface
from datatri.triage import rollup


def test_surface_stamps_its_name(dirty):
    s = Surface("orders-input", [unique("id"), not_null("qty")])
    r = s.triage(dirty)
    assert r.report["surface"].to_list() == ["orders-input"] * 3
    assert len(s) == 3 and [c.id for c in s] == ["id.duplicated", "id.null", "qty.not_null"]


def test_surfaces_roll_up_and_gate_independently(dirty):
    inp = Surface("orders-input", [unique("id", severity="block"), not_null("qty")])
    out = Surface("shipment-output", [in_set("status", ["A", "B", "C", "X"])])
    report = pl.concat([inp.triage(dirty).report, out.triage(dirty).report])

    by_surface = rollup(report, "surface")
    assert by_surface.rows() == [
        ("orders-input", 3, 3, 4, 0.0),  # id.duplicated (2 rows) + id.null (1) + qty.not_null (1)
        ("shipment-output", 1, 0, 0, 1.0),
    ]
    # gate one named surface, leave the other alone
    assert inp.triage(dirty).blocked
    assert not out.triage(dirty).blocked


def test_surface_requires_name():
    with pytest.raises(ValueError):
        Surface("", [])
