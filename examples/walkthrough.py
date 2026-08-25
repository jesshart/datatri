"""datatri walkthrough — run with `uv run python examples/walkthrough.py`."""

import tempfile
from pathlib import Path

import polars as pl

import datatri as dti

pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_fmt_str_lengths(80)


def hr(title: str) -> None:
    print(f"\n{'─' * 72}\n{title}\n{'─' * 72}")


# A messy orders feed from an upstream system ────────────────────────────────
orders = pl.DataFrame(
    {
        "order_id": [101, 102, 102, 103, None, 105],   # 102 twice, one null key
        "ship_to": ["NYC", None, "LAX", "CHI", "SEA", "DEN"],
        "qty": [5, 0, 7, 9, 12, 3],                   # 0 is not a valid quantity
        "region": ["NA", "EU", "XX", "NA", "NA", "EU"],  # XX is not a region
    }
)

hr("1. The input")
print(orders)


# 1 ── triage: partition instead of fail ──────────────────────────────────────
hr("2. dti.triage(frame, checks) → healthy, sick, report   (one lazy pass)")
checks = [
    dti.unique("order_id"),                       # -> order_id.duplicated (both copies of 102) + order_id.null
    dti.not_null("ship_to"),
    dti.in_range("qty", 1, 999),
    dti.in_set("region", ["NA", "EU"]),
    # the escape hatch: any Polars expression, here a cross-field rule — with a brief that says what a hit MEANS
    dti.sick("eu.needs_ship_to", (pl.col("region") == "EU") & pl.col("ship_to").is_null(),
             brief="EU orders need a ship-to"),
]
r = dti.triage(orders, checks)

print("r.report — one row per check:")
print(r.report.select("check_id", "dimension", "n_failed", "n_total", "frac_failed", "passed"))
print("\n…and each check's brief (builders fill a default; you override when the name would mislead):")
print(r.report.select("check_id", "brief"))

print("\nr.sick — every row carries `why`, the checks that condemned it:")
print(r.sick.collect())

print("\nr.healthy — flows on downstream, untouched:")
print(r.healthy.collect())

print(f"\nhealthy + sick = {r.healthy.collect().height} + {r.sick.collect().height} = {r.n_total}  (nothing lost)")


# 2 ── failures(): the keystone grain ─────────────────────────────────────────
hr("3. r.failures() — one row per (check × condemned row)")
print(r.failures().collect().select("check_id", "order_id", "ship_to", "qty", "region"))


# 3 ── surfaces + tags: name it, group it, roll it up ─────────────────────────
hr("4. dti.Surface + tags — name the contract, slice it any way you like")
orders_input = dti.Surface(
    "orders-input",
    [
        dti.unique("order_id", tags={"owner": "OMS"}),
        dti.not_null("ship_to", tags={"owner": "OMS"}),
        dti.in_range("qty", 1, 999, tags={"owner": "Planning"}),
        dti.in_set("region", ["NA", "EU"], tags={"owner": "Planning"}),
    ],
)
rs = orders_input.triage(orders)
print("report now carries surface + owner columns:")
print(rs.report.select("check_id", "surface", "owner", "dimension", "n_failed", "passed"))

print("\ndti.rollup(report, 'owner') — who has to fix what:")
print(dti.rollup(rs.report, "owner"))

print("\ndti.rollup(report, 'dimension') — what kind of trouble:")
print(dti.rollup(rs.report, "dimension"))


# 4 ── schema: free structural check ──────────────────────────────────────────
hr("5. dti.check_schema — exact dtypes, zero data read")
expected = {"order_id": pl.Int64, "ship_to": pl.String, "qty": pl.Int32, "region": pl.String, "carrier": pl.String}
s = dti.check_schema(orders, expected)
print(f"ok: {s.ok}")
for line in s.describe():
    print(f"   • {line}")


# 5 ── blocking is opt-in, per check ──────────────────────────────────────────
hr("6. Blocking is opt-in — triage never raises on its own")
strict = dti.triage(orders, [dti.unique("order_id", severity="block"), dti.in_set("region", ["NA", "EU"])])
print(f"strict.blocked: {strict.blocked}   (a severity='block' check failed)")
try:
    strict.raise_if_blocked()
except dti.TriageBlocked as e:
    print(f"strict.raise_if_blocked() → {type(e).__name__}: {e}")


# 6 ── conservation: validate the operation, not just the rows ────────────────
hr("7. dti.safe_join + conservation — the killer case")
fact = pl.DataFrame(
    {"order_id": [1, 2, 3, 4, 5], "sku": ["sku1", "sku2", "sku2", "sku3", "sku1"], "units": [10, 20, 30, 25, 15]}
)
dim = pl.DataFrame(  # a dirty dimension: sku2 listed twice
    {"sku": ["sku1", "sku2", "sku2", "sku3"], "category": ["A", "B", "B", "C"]}
)
units = {"units": pl.col("units").sum()}

joined, cons = dti.safe_join(fact, dim, on="sku", how="left", measures=units)

print("record-level suite on the JOINED output:")
suite = dti.triage(joined, [dti.not_null("category"), dti.in_range("units", 1, 1000), dti.in_set("category", ["A", "B", "C"])])
print(suite.report.select("check_id", "n_failed", "passed"))
print("   → all healthy. And yet:\n")

print("cons.to_frame() — conservation across the join:")
print(cons.to_frame())
print("   → rows 5→7, units 100→150. The join fanned out; every row is still 'healthy'.\n")

print("what a planner would see (category B is doubled):")
print(joined.group_by("category").agg(pl.col("units").sum()).sort("category").collect())


# 7 ── the fix: triage the dim, join the healthy side, total conserved ────────
hr("8. The fix — quarantine the dirty dim, join the healthy side, books balance")
d = dti.triage(dim, [dti.unique("sku")])
print("d.sick — both sku2 rows go to the doctor:")
print(d.sick.collect())

joined_healthy, cons2 = dti.safe_join(fact, d.healthy, on="sku", how="inner", measures=units)
orphans = fact.lazy().join(d.healthy, on="sku", how="anti")   # orders that lost their parent

kept = joined_healthy.select(pl.col("units").sum()).collect().item()
held = orphans.select(pl.col("units").sum()).collect().item()
print(f"\nkept {kept} + quarantined {held} = {kept + held}  (original 100 — conserved)")
print("orphaned orders, routed upstream with the reason:")
print(orphans.collect())


# 8 ── sinks: never hold the whole frame ──────────────────────────────────────
hr("9. r.sink() — stream both sides to parquet, no full-frame collect")
with tempfile.TemporaryDirectory() as tmp:
    p = Path(tmp)
    r.sink(p / "healthy.parquet", p / "sick.parquet")
    print(f"healthy.parquet: {pl.scan_parquet(p / 'healthy.parquet').select(pl.len()).collect().item()} rows")
    print(f"sick.parquet:    {pl.scan_parquet(p / 'sick.parquet').select(pl.len()).collect().item()} rows  (with `why`)")


# 9 ── cascade: condemn the key, not just the row (optional) ──────────────────
hr("10. Cascade — a sick SKU in one place is suspect everywhere (opt-in, per check)")
dim = pl.DataFrame({"sku": ["sku1", "sku2", "sku2", "sku3", "sku4"], "units_per_case": [12, 6, 6, 0, 24]})
orders2 = pl.DataFrame({"order_id": [1, 2, 3, 4, 5], "sku": ["sku1", "sku2", "sku3", "sku4", "sku9"],
                        "recall": [False, False, False, True, False]})
ship = pl.DataFrame({"shipment_id": [10, 11, 12, 13], "order_id": [1, 2, 4, 5]})

led, rounds = dti.Ledger(), 0
while True:                                                   # the loop is yours: four lines, policy visible
    rounds += 1
    before = led.sizes()
    rd = dti.triage(dim, [dti.unique("sku", tags={"cascade": "sku"}),
                          dti.in_range("units_per_case", 1, tags={"cascade": "sku"}),
                          dti.poisoned("sku", led.keys("sku"), tags={"cascade": "sku"})], surface="dim")
    led.add(rd, "sku", "sku")
    ro = dti.triage(orders2, [dti.orphan("sku", dim.select("sku"), tags={"cascade": "order_id"}),
                              dti.poisoned("sku", led.keys("sku"), tags={"cascade": "order_id"}),
                              dti.sick("sku.recalled", pl.col("recall"), tags={"cascade": "sku"},
                                       brief="recalled: the SKU is suspect everywhere")], surface="orders")
    led.add(ro, "order_id", "order_id")
    led.add(ro, "sku", "sku")                                 # the up-edge (order -> SKU) is just another add
    rs = dti.triage(ship, [dti.poisoned("order_id", led.keys("order_id", dtype=pl.Int64))], surface="shipments")
    if led.sizes() == before:
        break

print(f"converged in {rounds} rounds — the ledger is the provenance:")
print(led.frame)
print("\nshipments condemned by cascade, with why:")
print(rs.sick.collect())
print("\none scorecard across surfaces (rollup stacks the reports diagonally):")
print(dti.rollup([rd.report, ro.report, rs.report], "surface"))
