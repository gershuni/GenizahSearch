# Track-1 identification grades — round 1 (Hillel, 2026-07-07 night)

Source: `review/grades_track1_id_round1_2026-07-07.json` (61 grades over the
v1 review sample; graded before the v2 bib/translation rebuild).

## Verdict distribution

| grade | n | reading |
|---|---|---|
| correct | **58** | the page carries the identified work |
| citation | 3 | real match, but an embedded quotation — not a copy of the work |
| wrong / formula / junk | **0** | — |

**Identification precision ≈ 100%** across the hardest strata: every graded
`new_witness` (39), `title_mismatch` (22) card was either correct or a real
match reclassified as citation. No spurious identifications at all.

## The 3 citation verdicts — all boundary-coverage cases

| coverage | cls | work |
|---|---|---|
| 0.316 | partial | M:Ytext355005 |
| 0.427 | partial | M:Ytext86000 |
| 0.475 / 0.503 | testimony | M:Ytext86000 ×2 |

Two sat *above* the 0.45 testimony threshold (0.475, 0.503) — same work
(`Ytext86000`), suggesting a per-work quotation profile rather than a global
threshold problem: other 0.47–0.60-coverage cards were graded correct.
No threshold change made; the full-scale census keeps 0.45 and the review
tool exposes coverage for eyeball calibration.

## Structural findings (Hillel) → implemented in v2 (`42f8dc3a`)

1. **"Title mismatch is usually just translation and so on"** — the
   mismatch bucket was dominated by JA works displayed under Hebrew titles
   (catalog: פראיץ אלקלוב ↔ work: תורת חובות הלבבות) and author acronyms
   (רמב"ם, ריב"ג, רס"ג…). Fixed: `track1_bib.title_bucket2` matches against
   FJMS catalog titles + `GenizahTitleOrgTitle` normalized identifications,
   expands acronyms via `genizah_persons.HebDescAc↔HebDesc` (112 tokens),
   and applies a hand table of 17 classic JA↔Hebrew title pairs.
2. **"Some 'new?' parallels are already discussed in the research"** — the
   FJMS `bibliography` table (427K rows/AlmaId) names publications per MS,
   incl. `TranscriptionType` Full/Partial (= the fragment is already
   edited). Fixed: `bib_signal` demotes tier `new?` → `new?known`
   (review stratum `new_discussed`); confirmed on the first graded card
   (Antonin B 122 — Natronai responsa transcription in its bibliography).

Post-v2 sample: 120 `new_witness` cards with NO bib naming the work and no
published transcription = the genuinely-new queue; 30 `new_discussed`
control cards validate the demotion.
