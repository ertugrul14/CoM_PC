# Build Spec — Parking-Displacement Conservation Model (handoff)

**Status:** NOT built. This is an implementation brief for a fresh chat.
**Owner decision needed before coding:** see §7 (open decisions).
**Sibling already built:** the pedestrian-diversion conservation model in `reallocate_kerb`
(D-026). This is its occupancy-side twin. Read that first — same pattern, different conserved unit.

---

## 1. Goal (one sentence)

When an intervention reclaims kerb parking on a street, model the displaced cars relocating to
neighbouring streets' kerbs (their occupancy goes **UP**), conserving vehicle demand — instead of the
current behaviour, where the GNN parking head makes neighbour occupancy go **DOWN**.

## 2. Why (the problem, same class as the ped bug)

- The joint GNN parking head does **diffusion/smoothing**: lowering occupancy on the treated street
  pulls neighbours' predicted occupancy *down* too (it reads occupancy as an area "vitality" field).
- Real parking is **conserved**: cars removed from one kerb don't vanish — they re-park nearby, so
  neighbour occupancy should *rise* (the standard "parking spillover / displacement" externality).
- This is the exact analogue of the pedestrian diversion sign-error fixed in D-026. There we replaced
  GNN diffusion with a mass-conserving redistribution. Do the same here, on occupancy.

**History to know:** an earlier *first-order displacement heuristic* existed in step_11 and was REPLACED
by the joint parking head on 2026-04-10 (see decisions.md, and the `_rollout` docstring lines ~36/51,
"This replaces the prior first-order displacement…"). We are re-introducing displacement, but now as an
explicit conservation model that OVERRIDES the head's parking spillover for parking interventions —
mirroring how D-026 overrides the ped diffusion. Document why the head's version was insufficient
(wrong sign for displacement).

## 3. Key differences from the pedestrian model (do NOT copy blindly)

| Aspect | Pedestrian (built) | Parking (this spec) |
|---|---|---|
| Conserved unit | pedestrian **counts** | **vehicles** = capacity × occupancy_rate (NOT the rate itself) |
| Treated effect | +U% uplift (gain) | occupancy **reduction** (cars removed) |
| Neighbour sign | LOSE (diversion) | **GAIN** (displacement) |
| Hard cap | none needed | occupancy ≤ 1.0 → spare-capacity cap + overflow cascade |
| Leakage term | "new" fraction (0.308) stays put | **RETENTION fraction**: share of displaced demand that stays on-street vs leaks to garages / mode-shift / trip-suppression |

**Critical:** you must convert occupancy_rate → vehicles before redistributing, then back to rate per
neighbour using *each street's* capacity. Redistributing the rate directly is wrong (ignores that
streets have different bay counts).

## 4. Mechanism (formulas)

Let the treated street remove parking. Per active rollout step:

```
cap_t        = bays on treated street                      # capacity (see §6 — DATA GAP)
removed_veh  = cap_t * (occ_baseline_t - occ_treated_t)    # vehicles leaving the treated kerb
  # pedestrianise:   occ_treated_t = 0
  # restrict_park:   occ_treated_t = magnitude
  # curbside_dining: occ_treated_t = occ_baseline_t * (1 - bays/DEFAULT_STREET_BAYS)

retained_veh = removed_veh * RETENTION_FRACTION            # stays on-street (re-parks nearby)
leaked_veh   = removed_veh * (1 - RETENTION_FRACTION)      # garages / mode shift / suppressed (leaves system)

# Distribute retained_veh across SPATIAL neighbours by learned edge weight w_i,
# capped at each neighbour's spare capacity; overflow cascades to next neighbours
# (or adds to leaked_veh if all full):
for neighbour i (sorted by w_i desc):
    spare_veh_i  = cap_i * (1 - occ_baseline_i)            # room before hitting occ=1.0
    add_i        = min(retained_veh * w_i_norm, spare_veh_i)
    Δocc_i       = +add_i / cap_i                          # convert back to RATE
    (track overflow = requested - add_i; cascade or leak)
```

Treated street Δocc = (occ_treated_t − occ_baseline_t)  (negative, the imposed reduction).
Neighbours Δocc = +add_i/cap_i (positive). Everyone else 0. Mass check: Σ add_i + leaked_veh = removed_veh.

## 5. Where it plugs in (code integration)

File: `melbourne_pipeline/steps/step_11_scenario.py`. Mirror the ped implementation:

1. **Constants** (near `REALLOCATE_NEW_FRACTION`, ~line 135):
   ```python
   PARK_RETENTION_FRACTION = ???   # TO BE SOURCED (§6) — fraction of displaced cars staying on-street
   ```
2. **Override `park_delta_raw`** in `run_scenario`, right after it's computed
   (~line 1017, `park_delta_raw = treated_park_raw - baseline_park_raw`). Add an
   `if intervention_type in PARKING_INTERVENTIONS:` branch that:
   - reads `occ_baseline` / `occ_treated` on the treated node from `baseline_park_raw`/`treated_park_raw`,
   - computes `removed_veh`, `retained_veh`, redistributes to `_get_spatial_neighbours(adj_s, node_idx)`
     with `_get_edge_weights(...)` (reuse the same helpers the ped model uses),
   - rebuilds `park_delta_raw` from scratch (treated reduction + neighbour increases, 0 elsewhere),
   - resyncs `treated_park_raw = np.clip(baseline_park_raw + park_delta_raw, 0, 1)`.
   Note `PARKING_INTERVENTIONS` already exists (line ~118): {pedestrianise, restrict_park, curbside_dining}.
3. The existing `top_affected_park_series` and `network_summary` consume `park_delta_raw`, so they will
   reflect displacement automatically (no further change).

**Do NOT touch the rollout itself** — like the ped model, this is a post-rollout redistribution on the
raw deltas. The GNN parking head still runs; we override its spillover for these interventions only.

## 6. DATA GAP — per-street parking capacity (resolve first)

There is **no per-street bay-count field** in the cube/features (verified: only `DEFAULT_STREET_BAYS=20`
constant exists; `*_capacity` features are bar/dining, not parking). Options for the new chat:
- (a) Use `DEFAULT_STREET_BAYS=20` for all streets (simplest; crude — overstates small streets).
- (b) Derive per-street capacity from raw parking data: count distinct bays/sensors per street_id in the
  parking source (Supabase / `parking_occupancy.parquet` provenance). **Check first whether bay counts
  exist** — if so this is the honest choice. (Token discipline: ask before opening parquet.)
- (c) Proxy capacity from street length (`area_m2` or geometry) × an assumed bays-per-metre.
Recommendation: try (b); fall back to (a) with the limitation stated.

## 7. Open decisions (get user sign-off before coding)

1. **Does `reallocate_kerb` also trigger parking displacement?** It currently perturbs ONLY ped. If it
   represents physical kerb reclamation, it should also remove parking → displacement. If yes, it becomes
   a two-sided intervention (ped diversion + parking displacement). Decision needed.
2. **`PARK_RETENTION_FRACTION` value + SOURCE.** This is the parking analogue of the ped new/diverted
   split and MUST be sourced, not invented (same discipline as D-026: find primary, verify, don't fake).
   Candidate literature: Shoup, *The High Cost of Free Parking*; SFpark evaluation; cruising-for-parking
   studies. **Do a real source search and verify at primary before committing a number.**
3. **Capacity source** (§6 a/b/c).
4. **Overflow handling** when nearest neighbours hit occ=1.0: cascade to next ring, or dump to leakage?
5. **1-hop vs multi-hop** neighbours (ped model is 1-hop).
6. **Interaction with the joint parking head:** full override (recommended, matches ped model) vs blend.

## 8. Verification plan (mirror `run_reallocate_sweep.py`)

Write `melbourne_pipeline/scratch/run_park_displacement_sweep.py`. Assert:
- **Sign:** treated occ DOWN, neighbour occ UP (#pos neighbours high).
- **Mass conservation (in vehicles):** Σ neighbour vehicle gains + leaked = removed vehicles (exact,
  pre-cap; post-cap overflow accounted in leaked).
- **Capacity:** no neighbour occ_treated > 1.0.
- **Placebo:** an intervention that removes 0 parking → all-zero deltas.
- Sweep restrict_park magnitude (e.g. {baseline, 0.5, 0.3, 0.0}) on a parking-sensor street (e.g. 20009).

## 9. Deliverables checklist for the new chat

- [ ] Resolve §7 open decisions with the user.
- [ ] Source + verify `PARK_RETENTION_FRACTION` at primary (web search → fetch → record in a new
      `docs/references/evidence/*.md`, add to `docs/references/index.md`). NO unsourced numbers.
- [ ] Resolve capacity data gap (§6).
- [ ] Implement the override in `step_11_scenario.py` (§5).
- [ ] Write + run the verification sweep (§8); confirm mass conservation + correct sign.
- [ ] Update `decisions.md` D-026 (or a new D-0xx) and `kerb_reallocation_scenario_findings.md`.
- [ ] Frontend: the parking interventions already exist in the UI; add a caveat note like the ped one
      ("displaced cars re-park nearby — neighbour occupancy rises; X% assumed to stay on-street").
- [ ] State limitations: capacity assumption, 1-hop, modal-filter-vs-parking-removal proxy, retention
      source/context.

## 10. Pointers
- Pedestrian sibling (the working pattern to copy): `step_11_scenario.py`, `reallocate_kerb` branch in
  `run_scenario` (search `Conservation / redistribution`) + constants `REALLOCATE_*_FRACTION`.
- Decision record: `docs/notes/decisions.md` → D-026 (incl. 2026-06-21 conservation update).
- Findings: `docs/notes/kerb_reallocation_scenario_findings.md` §7.
- Evidence-sourcing discipline to follow: `docs/references/evidence/aldred2019.md` (verify-at-primary).
- Helpers to reuse: `_get_spatial_neighbours`, `_get_edge_weights`, `PARKING_INTERVENTIONS`.
