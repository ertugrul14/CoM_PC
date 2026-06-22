"""D-026 Route 1 sweep: reallocate_kerb on street 20009, matched to the original
restrict_park file (t_start=10476, duration=rollout=16). Imposes an external
footfall elasticity on the treated street's baseline ped and lets the GNN
propagate it. Reports target + network response across the agreed band.
Edit UPLIFTS to adjust the bounds — bounds are a one-line list, by design."""
from melbourne_pipeline.steps import step_11_scenario as s

UPLIFTS = [0.0, 0.18, 0.30, 0.39]   # placebo / conservative / central / optimistic (D-026, sourced:
                                    # Cambra & Moura 2020 +18%, Aldred & Croft 2019 +39%)
T_START, DUR, ROLL, SID = 10476, 16, 16, "20009"

def mean(x): return (sum(x) / len(x)) if x else 0.0

rows = []
for u in UPLIFTS:
    res = s.run_scenario(
        street_id=SID, t_start=T_START, duration=DUR, rollout_steps=ROLL,
        intervention_type="reallocate_kerb", magnitude=u, save=False,
    )
    base = res["baseline"]["ped_flow_treated_street"]
    d_ped = res["delta"]["ped_flow_treated_street"]
    ad = res["network_summary"]["all_deltas"]
    nbr = [v["mean_ped_delta"] for k, v in ad.items() if k != "20009"]   # neighbours only
    city_sum = sum(v["mean_ped_delta"] for v in ad.values())            # net across network
    neg = sum(1 for v in nbr if v < -0.01)
    rows.append((u, mean(base), mean(d_ped), sum(nbr), city_sum, neg, len(nbr)))

print("\n=== reallocate_kerb (conservation) - street 20009 (baseline mean ped ~"
      f"{rows[0][1]:.0f}/15min) ===")
print(f"{'uplift':>7} | {'target dPed':>12} | {'target d%':>9} | "
      f"{'nbr loss sum':>12} | {'city net':>9} | {'city/target':>11} | {'#neg/N':>8}")
print("-" * 92)
for u, base, dped, nbrsum, city, neg, n in rows:
    ratio = (city / dped) if dped else 0.0
    print(f"{u*100:>5.0f}% | {dped:>+12.1f} | {dped/base*100:>+8.1f}% | "
          f"{nbrsum:>+12.1f} | {city:>+9.1f} | {ratio:>10.2f} | {neg:>4}/{n}")
print("\nConservation check: neighbours should LOSE (nbr loss sum < 0, #neg high);")
print(f"city net / target should ~= the NEW share ({s.REALLOCATE_NEW_FRACTION:.2f}) — i.e. ~69% of")
print("the treated street's gain is redistributed FROM neighbours, ~31% is genuinely new.")
