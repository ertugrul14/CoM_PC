# Why we removed XGBoost — presentation notes (plain language)

One-sentence takeaway:
> We tested whether the second model (XGBoost) actually helped the main model (the GNN)
> predict foot traffic on streets without sensors. It didn't — so we removed it and kept
> one simpler model that performs just as well.

---

## The setup (the problem)
- We have pedestrian sensors on only **74 streets**, but we need an estimate for **all ~1,400**.
- The old design stacked **two models**:
  1. **XGBoost** — guesses the missing streets first.
  2. **The GNN (graph network)** — uses those guesses to make the final prediction.
- Question: does the GNN actually *need* XGBoost's guesses, or can it figure the missing
  streets out by itself from the street map and each street's characteristics?

## How we tested it (the fair experiment)
- We took streets we *do* have sensors for, **hid them**, and pretended they were unmeasured.
- We let the GNN predict them **two ways**, identical except for one thing:
  - **With XGBoost:** the hidden streets were filled in by XGBoost's smart guess.
  - **Without XGBoost:** the hidden streets were filled in with a **dumb city-average** instead.
- Then we checked both against the **real sensor readings** we had hidden.
- We repeated this on 3 different groups of streets so it wasn't a fluke.

## The result (they're the same)
Average error (lower = better; "error" = how many people per 15 min we're off by):

| Approach                         | Error | How good (R²) |
|----------------------------------|:-----:|:-------------:|
| GNN **with** XGBoost             | 64.5  | 0.45          |
| GNN **without** XGBoost (alone)  | 64.6  | 0.44          |

- The difference is **0.1** — basically zero, and smaller than the random wobble between test groups.
- In other words: **giving the GNN XGBoost's smart guess vs. a dumb average made no difference.**

## What it means
- The GNN already reconstructs missing streets on its own, from the street network and each
  street's features (jobs, cafés, size, time of day).
- XGBoost was doing work the GNN was already doing — so it was **redundant**.
- **Decision:** remove XGBoost from the prediction pipeline. One model instead of two →
  **simpler, faster, easier to explain, and nothing is lost.**

## Analogy (for the slide)
> XGBoost was a co-pilot whispering directions — but the GNN already knew the route.
> Removing the co-pilot didn't change where we arrived.

## Honest footnote (good to mention if asked)
- Predicting a street with **no sensor at all** is genuinely hard for *any* method here
  (both approaches had high error) — that's a limit of the data, not the model.
- So the value of estimating every street is **city-wide coverage**, not pinpoint accuracy —
  and the *method* used to fill the gaps turned out not to matter for the final prediction.
