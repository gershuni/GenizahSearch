# Discovery Band Labels & Precision Presentation — Contract v1 (BAND-01/BAND-04)

**Status:** ACTIVE. Version 1, created 2026-07-23 (Phase 134 remediation; the
"(B) band-label honesty" lane of the discovery-spine / SEED-029 split).

**Tunable ONLY by versioning this artifact** (same discipline as
`docs/specs/discovery-budgets.md`). This file is the SINGLE source of truth for
how confidence bands and precision numbers are WORDED on every discovery
surface (the Phase 135 methods page, the Phase 136 browse panel + `/work/{id}`,
the atlas, the public API, and the internal inspection tool). Code renders
labels from a values module generated from this file — it never hardcodes a
competing string.

## 0. Why this exists

Owner review of the `discovery-v1` frame found the band vocabulary
**overclaims**: the stored band `expert_verified` reads as "a human verified this
identification," but it is populated with **121 `human_confirmed` + 1,067
`unreviewed`** rows, and `tier_a` is **238,618 rows, all `unreviewed`** (~89% of
the spine). The owner approved **only neutral titles** (134-07), never
individual identifications. A sampled band-precision estimate (0.889) was, in
effect, stamped onto a ~10× larger unreviewed population as if it were per-item
confidence. This contract fixes the wording so no surface ever calls an
unreviewed, algorithmically-scored row "verified."

## 1. The two orthogonal axes (never conflate them)

| Axis | Field | What it means |
|---|---|---|
| **Scoring tier** | `evidence_source` + `confidence_band` | An ALGORITHMIC score. Says nothing about human review. |
| **Review state** | `adjudication_status` | Whether a human adjudicated THIS row (`human_confirmed` / `provisional` / `unreviewed`). |

**Rule 1 — the word gate.** "Verified", "confirmed", "reviewed", "certified"
appear ONLY as a function of `adjudication_status='human_confirmed'`. They are
**PROHIBITED** as names or labels for any `confidence_band` value. ("Certified"
is prohibited outright — pre-existing frame rule.)

## 2. Band display labels (bilingual EN / HE)

The stored `confidence_band` keys are a FROZEN enum in `discovery-v1`; the
`expert_verified` **key rename** to `high_confidence_algorithmic` lands in the
`discovery-v2` re-distillation (see §5). Until then the DISPLAY label below is
used over the current stored key — a surface never shows the raw key.

| evidence_source | stored band (v1) | v2 stored key | EN display label | HE display label |
|---|---|---|---|---|
| track1_direct | `expert_verified` | `high_confidence_algorithmic` | High-confidence match (algorithmic) | התאמה בוודאות גבוהה (אלגוריתמית) |
| track1_direct | `tier_a` | `tier_a` (unchanged) | Algorithmic match — tier A | התאמה אלגוריתמית — דרגה א׳ |
| track1_direct | `screening_rb` | `screening_rb` | Screening — rule-based | סינון — מבוסס כללים |
| track1_direct | `screening_canon` | `screening_canon` | Screening — canon | סינון — קנון |
| propagated | `corroborated` | `corroborated` | Corroborated by matching witnesses | מאושש בעדים תואמים |
| propagated | `weak` | `weak` | Matching witnesses (weak) | עדים תואמים (חלש) |
| propagated | `not_evaluated` | `not_evaluated` | Shared text — not evaluated | טקסט משותף — לא הוערך |

**Review overlay (the ONLY human-review signal), orthogonal to the band:**

| `adjudication_status` | EN | HE |
|---|---|---|
| `human_confirmed` | Expert-reviewed ✓ | נבדק בידי מומחה ✓ |
| `provisional` / `unreviewed` | *(no badge — must not imply review)* | — |

Every algorithmic-band row shown WITHOUT the review badge must carry an explicit
"unreviewed · algorithmic estimate" / "לא נבדק · הערכה אלגוריתמית" marker so the
absence of review is visible, not merely implied.

## 3. Precision presentation

`band_precision` numbers are **estimated band-population precisions**, not
per-item probabilities. Present them under these rules:

1. Always phrase as **"estimated band precision"** (+ CI when available), never
   a bare percentage and never per-item ("this identification is 89% likely" is
   FORBIDDEN).
2. Never apply a band's sampled estimate to an individual row as its confidence.
3. All current numbers are **pre-registered / prior-adjudication estimates**,
   explicitly **provisional pending the Phase 135 owner grading** (CERT-01 /
   BAND-02), which MEASURES them on a fresh stratified draw and MAY reband.

| scope | band | estimated precision | note |
|---|---|---|---|
| collection (propagated witness) | corroborated ∪ weak | **0.926 [0.875, 0.968]** | the only measured propagated number; corroborated/weak carry NO separate band interval |
| band (track1_direct) | high_confidence_algorithmic (was expert_verified) | **0.889** (pre-registered; CI pending Ph135) | band-population estimate, NOT per-item |
| band (track1_direct) | tier_a | *not yet measured* | 89% of the spine; no estimate exists — must be shown as "precision not yet measured" |
| band (track1_direct) | screening_rb | **0.859** (pre-registered; CI pending Ph135) | screening tier |
| band (track1_direct) | screening_canon | **0.647** (pre-registered; CI pending Ph135) | screening tier — genuinely low by design |

## 4. Default-shown policy (BAND-03 precursor)

On any default (non-expanded) surface:

- **Show:** rows with `adjudication_status='human_confirmed'` (any band), PLUS
  the single **top algorithmic band's** `routing_status='shipped'` rows.
- **Behind an explicit "show screening / algorithmic matches" toggle:** the
  lower algorithmic tiers, `routing_status='review_only'`, and shadowed rows.
- Each shown algorithmic row is marked per §2 ("unreviewed · algorithmic
  estimate") unless it carries the review badge.

This is the yardstick-not-evidence / provisional-not-certified discipline the
whole discovery program runs under; it does not delete any row (everything stays
queryable) — it governs what the DEFAULT view asserts.

## 5. v2 enum-rename lockstep

When the stored `expert_verified` key renames to `high_confidence_algorithmic`
at the `discovery-v2` bake, ALL of these change together (one commit / one bake)
or the frozen-enum invariant breaks:

- `scripts/discovery_ids.py` (frozen enum vocab)
- `scripts/build_discovery_sidecar.py` (band assignment + `band_precision` rows)
- `scripts/verify_discovery_sidecar.py` (enum invariant + any hardcoded literal)
- `web/discovery_assets.py` (`_CONFIDENCE_BANDS_BY_SOURCE` spot-check)
- `shared/discovery_service.py` (`_BAND_RANK_ORDER` + `_BAND_RANK_CASE_SQL`)
- `docs/specs/discovery-sidecar-schema-v1.md` + `discovery-frames.md` (frozen vocab + C-7)
- the label map in §2 of this file

Only `expert_verified` renames; the other stored keys are unchanged. The DISPLAY
labels in §2 are usable immediately (no re-distill needed) since surfaces never
show the raw key.

## 6. Cross-references

- `docs/specs/discovery-budgets.md` — PERF-01 caps (same versioning discipline).
- `docs/specs/discovery-frames.md` §7 — the C-7 `band_precision` reporting the
  numbers in §3 come from (and the "certified" prohibition).
- SEED-029 / R-source track — owns the (A) canonical merge + (C) direction/
  shadow router; this file owns (B) labels only.

---

*Phase: 134 remediation — (B) band-label honesty lane.*
*Exit artifact for BAND-01 (band display) + BAND-04 (per-surface disclaimer wording).*
