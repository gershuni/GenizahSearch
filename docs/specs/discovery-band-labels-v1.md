# Discovery Band Labels & Precision Presentation — Contract v1 (BAND-01/BAND-04)

**Status:** ACTIVE. Version 1, created 2026-07-23 (Phase 134 remediation; the
"(B) band-label honesty" lane of the discovery-spine / SEED-029 split).
Amended 2026-07-23: §4 gains the **multi-register invariant** (a page may carry
N shipped witnesses; the band filter must never suppress a shipped co-register) —
surfaced by SEED-029 live adjudication of multi-register targum MSS.

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

**Multi-register invariant (frame-level, NOT an edge case).** A page can
legitimately carry MULTIPLE shipped witnesses at once — owner-confirmed on real
Genizah targum MSS: Bible + Targum verse-after-verse (two), Bible + Onkelos +
Judeo-Arabic Tafsir (three), all correct, occupying different non-overlapping
spans / scripts / languages. The claim key is `(page_id, work_id)` and
`display_evidence_id` is **per-claim, never per-page**, so N shipped works per
page is native to the schema (~20% of pages already carry ≥2 shipped
witness-works; some 5+). **No surface may collapse a page to one work, and the
band filter below MUST NOT suppress a shipped co-register.** (Producer-side, the
relation-aware shadow does not shadow across non-overlapping-span registers —
they co-exist, they don't compete.)

On any default (non-expanded) surface:

- **Show every `routing_status='shipped'` witness claim on the page** (all
  registers), PLUS every `adjudication_status='human_confirmed'` row (any band).
  A shipped claim is **never** hidden merely because a sibling claim on the same
  page sits in a higher band — the band governs per-claim DISCLOSURE, not
  per-page collapse. (Earlier drafts said "top algorithmic band only"; that would
  have hidden a legit co-register on ~3,241 multi-band multi-work pages — removed.)
- **Behind an explicit "show screening / algorithmic matches" toggle:** the
  `screening_rb` / `screening_canon` tiers, `routing_status='review_only'`, and
  shadowed rows.
- Each shown algorithmic row is marked per §2 ("unreviewed · algorithmic
  estimate") unless it carries the review badge.

This default is trustworthy **only in concert with (C)**: the direction-aware
shadow router demotes anthology/quotation false positives to
`routing_status='review_only'` (hidden here), so "show all shipped" is safe
because the shipped set has already had the wrong-direction claims routed out.
Band disclosure (this §) and false-positive routing (C) are complementary, not
substitutes.

This is the yardstick-not-evidence / provisional-not-certified discipline the
whole discovery program runs under; it does not delete any row (everything stays
queryable) — it governs what the DEFAULT view asserts.

## 4a. Bake-time integrity checks (v2 verifier — from the SEED-029 handoff)

Two page-coverage checks the v2 bake/verifier owns (counts supplied by the
router output):

- **Shadow-orphan (HARD FAIL):** any page with ≥1 claim but **0 shipped** claims
  → the bake dies. A base was shadowed with nothing surviving on the shipped
  surface; that is a real bug (`review_only` must never dominate `shipped`).
- **Coverage-gap / P4 "no base target" (LOG + REPORT, not fatal):** pages that
  drop from ≥1 claim pre-merge to **0 claims post-merge** — a base witness fell
  out and nothing caught it. Different cause from the orphan; reported, not
  hard-failed.

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
