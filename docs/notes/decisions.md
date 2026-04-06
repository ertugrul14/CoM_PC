# Key Decisions Log

Use this file to record important design decisions.
When a Claude session gets long, summarize conclusions here and start a fresh session.

## Format
### [Date] — [Topic]
- Decision:
- Reason:
- Impact on pipeline:

---

### 2026-04-06 — Post-training phase lock
- Decision: Steps 01–09 frozen. Active work is Steps 10–12.
- Reason: Training preparation is sufficiently validated. Focus shifts to interpretation and scenario design.
- Impact: Claude will not modify pre-training code unless OVERRIDE is stated.

### 2026-04-06 — Semantic graph threshold
- Decision: min_sim = 0.99 (near-identical functional profiles only)
- Reason: Lower threshold created spurious connections between dissimilar streets
- Impact: 564 streets have no semantic edges (isolated)