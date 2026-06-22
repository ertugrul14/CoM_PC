"""One-off demo: curbside_dining on street 20009, matched to the restrict_park
file (t_start=10476, duration=16, rollout=16). magnitude=12 bays ≈ same occupancy
drop as restrict_park→0.30, so the only new ingredient is the land-use uplift.
Prints a compact baseline-vs-treated comparison; not part of the pipeline."""
import json
from pathlib import Path
from melbourne_pipeline.steps import step_11_scenario as s

res = s.run_scenario(
    street_id="20009",
    t_start=10476,
    duration=16,
    rollout_steps=16,
    intervention_type="curbside_dining",
    magnitude=12.0,
    save=False,
)

base_ped = res["baseline"]["ped_flow_treated_street"]
d_ped    = res["delta"]["ped_flow_treated_street"]
d_occ    = res["delta"]["occ_rate_treated_street"]

def mean(x): return sum(x) / len(x)

print("\n=== curbside_dining(12 bays) vs restrict_park(0.30) on street 20009 ===")
print(f"target mean ped delta : {mean(d_ped):+.2f} ped/15min  (restrict_park was -17.6)")
print(f"target mean occ delta : {mean(d_occ):+.3f}            (restrict_park was -0.099)")
print(f"target ped end-of-roll: {d_ped[-1]:+.2f}")

# network-wide signed mean (sensor streets only, confidence==1)
ad = res["network_summary"]["all_deltas"]
sensor = [v["mean_ped_delta"] for v in ad.values() if v["ped_confidence"] == 1]
allnet = [v["mean_ped_delta"] for v in ad.values()]
pos = sum(1 for v in allnet if v > 0)
print(f"\nnetwork sensor-street mean ped delta : {mean(sensor):+.3f}  (n={len(sensor)})")
print(f"network all-street mean ped delta    : {mean(allnet):+.3f}  (n={len(allnet)})")
print(f"streets with POSITIVE ped delta      : {pos}/{len(allnet)}")

# step-by-step target ped delta
print("\nstep:  ", " ".join(f"{i:6d}" for i in range(16)))
print("d_ped: ", " ".join(f"{v:6.1f}" for v in d_ped))

Path("melbourne_pipeline/scratch/curbside_20009_demo.json").write_text(json.dumps(res))

# --- Variant: stronger uplift channel (isolate the calibration lever) ---
# Same 12-bay occupancy shock, but each bay reads as more cafe/bar frontage.
# This does NOT change the occupancy drop — only the land-use push.
s.BAYS_PER_NEW_CAFE = 1.0   # was 4.0  → 4x more cafe frontage per bay
s.BAYS_PER_NEW_BAR  = 2.0   # was 8.0  → 4x more bar frontage per bay
s.SEATS_PER_BAY     = 14.0  # was 7.0  → 2x seats per bay
res2 = s.run_scenario(
    street_id="20009", t_start=10476, duration=16, rollout_steps=16,
    intervention_type="curbside_dining", magnitude=12.0, save=False,
)
d_ped2 = res2["delta"]["ped_flow_treated_street"]
print(f"\n=== STRONG-uplift variant (same occ shock, 4x cafe/2x seats) ===")
print(f"target mean ped delta : {mean(d_ped2):+.2f}  (was -13.71 at default constants)")
print(f"target ped end-of-roll: {d_ped2[-1]:+.2f}")
print("d_ped: ", " ".join(f"{v:6.1f}" for v in d_ped2))
