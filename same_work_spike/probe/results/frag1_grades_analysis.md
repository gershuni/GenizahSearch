# FRAG-1 grading — Hillel's verdicts (2026-07-09)

Source: `review/grades_frag1_2026-07-09.json` (33 of 45 cards graded).

## Breakdown

| type | graded | verdicts |
|---|---|---|
| **density_fail** | 10 / 10 | **candidate-correct 10/10** |
| **crop_recovered** | 20 / 20 | **correct-id 20/20** |
| ambiguous | 1 / 3 | none-correct 1 |
| no_reference | 2 / 12 | known-other 1, too-little 1 |

## What each confirms

**density_fail = real recall loss (10/10).** Every density_fail card is a
CORRECT match the acceptance boundary wrongly rejected. This is the strongest
result: the ~17% density_fail slice of the orphan short-fragment population is
genuine same-work recall recoverable by A5 length-conditional loosening (into
a candidate/review tier). At ~17% of ~71,176 <200-letter orphans ≈ ~12K pages,
high-precision-recoverable. **A5 length-conditional thresholds are validated as
a real fragment lever.**

**crop_recovered = precision holds (20/20).** Every truncated crop the engine
recovered at the 60–100-letter knee was correctly identified. Confirms FRAG-1's
top-wrong ≤1.4% precision reading empirically: when the engine fires at short
length it is right, so pushing recall down toward the knee (via A5 loosening)
does NOT sacrifice precision. One card catalog-confirmed ("מפורש במטא-דטה").

**ambiguous at short length = verse co-citation, not work-parallel (1/1
none-correct).** Hillel: "this is a liturgical unit… none of the parallels are
work-parallels — they're parallels only in that both quote the verse." So the
short-length ambiguous class is the shared-canonical-text trap (the exact thing
canonical masking / flank-contrast exist to filter) — these must NOT enter the
same-work census; route through canonical masking.

**no_reference under-graded (2/12) — the gate is still open.** Of the two:
- one **known-other**: identified from the NLI catalog title as **ספר המצוות
  של ר' חננאל בן שמואל (רחב"ש)** — a KNOWN work absent from our references =
  a **reference GAP** (a real, catalogued JA work not in our 92-doc JA set),
  NOT a genuine unknown and NOT a too-variant miss.
- one **too-little** (insufficient to judge).
Two datapoints can't quantify the absent-vs-reference-gap split. The one
substantive verdict shows at least part of the 82% no_reference mass is
identifiable KNOWN works missing from our (small) JA reference set — i.e.
recoverable IF those works were added, which reopens targeted JA reference
expansion for CATALOGUED works (distinct from the truly-unknown residue).
**Action: grade more no_reference cards to size the split** (Hillel flagged
these as the hardest/slowest — they need the manuscript image + expertise).

## Two data-quality findings (actionable, from the notes)

1. **Two-page-merge extraction artifact.** Two density_fail notes report the
   fragment text is garbled because "it reads two pages in one index as
   continuous text" (e.g. `990051584290…`: parallels are to the right-hand page
   only; `990051926210…`: "one page = R. Yoḥanan's derasha Bavli Megillah 31a,
   the second = R. Ḥama Bavli Sotah 14a"). Some corpus "pages" concatenate two
   physical manuscript pages → corrupted stream, hurts matching + splits one
   real match across a merged blob. A stage-0 / extraction review item: detect
   and split two-page-per-image records.
2. **Prefer FGP transcriptions over HTR where available.** Hillel: "if you
   looked at the FGP transcription you'd see…". FGP clean transcriptions
   (~45K PDFs, `fgp_transcriptions.db`) beat raw HTR — matching (esp. for
   short fragments where every letter counts) should use the FGP text when a
   page has one. Potential material recall/precision gain for FRAG-2.

## Consequence for FRAG-2 (updated lever confidence)

- **A5 length-conditional two-tier: HIGH confidence** (density_fail 10/10 real;
  crop precision 20/20). Build it → recovers the ~17% near-miss slice as
  candidate-tier IDs. Now the clearest, best-validated next build.
- **Canonical masking for ambiguous/short co-citations:** keep them out of the
  census (confirmed necessary).
- **no_reference:** split still unquantified; needs more grading. Early signal =
  mix of genuine-unknowns (discovery) AND catalogued-JA-works-not-in-refs
  (targeted reference gap). 
- **NEW: two-page-merge stage-0 fix + FGP-text-preferred matching** — two
  data-quality levers surfaced by grading, both plausibly boosting short-
  fragment matching before any threshold change.
