# A1 — `shadowed_by` filter into remaining consumers (2026-07-08)

Spike A1 of `SPIKE-BRIEFS-2026-07-08.md`. Executed inline by the orchestrator.

## Patches (all with the PRAGMA compat gate from `track1_testimonies.py`)

| File | Site | Change |
|---|---|---|
| `df_damage.py` | census read (line 41) | `WHERE shadowed_by IS NULL` when column exists; "Reading" note 4 updated (suspects now historical) |
| `passage_units.py` | label-propagation read (line ~206) | `AND shadowed_by IS NULL` appended to the batched IN-query |
| `map_with_ref_edges.py` | witness-web read (line ~77) | `WHERE shadowed_by IS NULL` when column exists |

The gate matters: `track1_match.py` CREATEs `track1_matches` WITHOUT the column
(`track1_shadow.py` ALTERs it in later), and `liturgy.db`'s copy predates shadowing.

## df_damage before/after (`df_damage_full_preshadow.md` vs regenerated `df_damage_full.md`)

**Headline: the 0% "catastrophe cohort" was mostly a shadowing artifact, not DF-cap damage.**

- Works (≥2 witness MSS): → 1,651; identified pages → 145,723 (live rows only).
- מגילת המקדש (11QT): **176 MSS / 355 pages → 23 MSS / 28 pages** — matches
  `track1_shadow_full.md` (176→23) exactly. פיירברג לאן: 96 → 34 MSS.
- The pre-shadow 0%-pairing rows are GONE from the damage list: יקים משא (39 MSS,
  0%), ברכת נישואין לאלמן ואלמנה (25, 0%), מגילת פשר ישעיהו (35, 0%), קדושתא
  לסוכות (34, 0%), המספיק לעובדי השם (25, 0%), חיבור נגד המינים (21, 0%). These
  were span-competition LOSERS — pages whose text really belongs to another work —
  so their "unpaired witnesses" never existed.
- Aggregate pairing rates ROSE across the high-witness buckets (edited 101-200:
  65%→76%; 201-500: 67%→81%) — the denominators were polluted by phantom members.
- **The real cap-damage floor (short-work cohort, <2K letters, ≥10 MSS): 60 works,
  77% overall (was 68 / 65%), floor now 14–30%** — תפילת פסוקים לאחר ערבית 14%
  (41 MSS), תפילה ליום כיפור 15%, צלותא לרס"ג 21%, קידוש לרגלים 30%. Still real,
  still liturgy, but ~3× milder than the pre-shadow census claimed.

**Consequence for A2 (DF-policy v2):** the target cohort and acceptance gate must
be re-anchored on the regenerated census (sub-30% short works → ≥60-70%), and the
expected win is smaller than the pre-shadow numbers implied. Brief updated.

## map_with_ref_edges before/after

| | pre-shadow | live-only |
|---|---|---|
| ref webs added | 1,595 works | 1,561 works (18,514 memberships) |
| connected MSS (T2∪T1) | 53,495 | 53,225 |
| components ≥2 | 1,772 | 1,785 |
| giant component | 48,652 | 48,326 |
| BH witnesses in ≥2 components | 210 | 211 |
| top suspect (ספר אהבה) | 2,275 MSS | 1,955 MSS |

All deltas directionally correct: shadowed memberships no longer bridge components.

**Side-find for Track-3/C2:** the suspect list now cleanly shows רס"ג בראשית תרגום
(תפסיר תורה) at **259 witness MSS** — Saadia's Tafsīr is already IN the JA
reference corpus with a large identified Genizah witness base. The cross-lingual
gold build (RamBERT plan §4) can verse-align against the reference text directly;
"is a digital Tafsīr reachable?" is partially answered from our own data.

## passage_units rerun — DONE (2026-07-08 late night, 232s)

Structure-invariance assertion HELD: **81,365 units, identical to the pre-patch
build** (Track-1 is label-propagation-only there). Only labels moved — 14
changed lines in `units_full.md`; label propagation rose to 26,304 inherited
pages (pre-patch ~20K), consistent with shadowing noise removal: phantom-work
labels no longer split units' label votes, so more units clear the ≥2-direct
confidence gate. `units_full.html` regenerated.

## Mask-side consumers — semantic decision: KEEP ALL SPANS (no patch)

- `rehearsal_run.py:86` (mask loading, incl. `maskcanon`): masking removes
  identified text from Track-2's view. A shadowed row's span still covers REAL
  text on the page (the winner overlaps ≥0.6 of it — but up to 40% + tails would
  become UNMASKED if we filtered to live rows, re-admitting canonical text into
  the discovery map). Over-masking is conservative (loses a little recall);
  under-masking creates false discoveries. Keep all spans.
- `mask_severity.py`: a diagnostic of what masking does — must mirror the actual
  masking semantics. Keep consistent with the above.
- `classify_canonical_edges.py`: explains WHY a Track-2 edge exists (shared
  canonical text present on both pages). Presence is a property of the text, not
  of the census assignment; shadowed spans still explain shared text. Keep all
  spans.

Traced cases: 11QT — its shadowed rows are EDITED-cat (מגילת המקדש), not
canonical, so `maskcanon` never masked them; filtering them would change nothing
there. The Deut spans that WON are canonical and live — masked either way.
Ibn-Tibbon vs JA-original: both edited-cat; irrelevant to canonical masking;
census double-count already fixed by the consumers patched above.

## Acceptance check (per the brief)

- df_damage: totals/deltas explained directionally above; regenerated report diffs cleanly. ✔
- map_with_ref_edges: edge/component deltas explained. ✔
- passage_units: structure-invariance assertion to be verified at the queued rerun. ◐ (queued)
- Mask-side decision documented with case traces. ✔
