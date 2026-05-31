# LOCAL PDF de-space — WIP handoff (2026-05-31)

State snapshot so the conversation can be compacted without losing context.

## DONE & TESTED this session (NOT yet committed)

De-space rewrite in `shared/local_indexer_rtl.py::despace_line_to_word_units`:
1. **Edge-gap metric** (`next.x0 − prev.x1`), not center-to-center. Center conflated
   letter width with spacing → wide letters (מ/ש/ה) shattered off justified words.
2. **Combining-mark test = Unicode category `Mn`** (`_is_nikud`), not the range
   0x05B0–0x05C7 (which mis-classified maqaf ־/sof-pasuq and missed te'amim).
3. **Per-line 1-D Otsu valley** (`_word_gap_fraction`) — replaced the first-cut fixed
   floor 0.45 + median-adaptive (which MERGED tight books). Clipped at
   `_GAP_OUTLIER_CAP=1.2`, bounded `[_GAP_MIN_FRACTION=0.12, _GAP_MAX_FRACTION=1.10]`,
   unimodal guard `_GAP_MIN_SPREAD=0.12`.
4. **Dropped embedded-space-glyph as a boundary signal** (justified Hebrew puts U+0020
   between every letter).

Plus `shared/local_indexer.py`:
- `_ltr_damage_guard` gated count/Jaccard to LTR pages (was discarding good RTL de-space
  for the shattered blocks fallback — the real production blocker).
- `extraction_format_version` 2→3.
- `startup_recovery(reextract_pending=True)`; desktop `_init_indexer` passes `False` to
  defer bulk pending re-extraction off the UI thread (fixed launch freeze).

Measured (full `_extract_one_page_rawdict`, identical pages, before→after):
- אוצר הגאונים ברכות: 73.5% → 3.0% single-letter (shatter).
- רביצקי (tight): word-merge 15.8% → 0.07% (mean tok-len 12.3→4.5).
- איגרות הרמב״ם: 5.2% → 0.17%.

Tests: 122 targeted local-PDF tests pass, ruff clean, docs check green. New fixtures
`tests/fixtures/local_indexer/glyph_traces/real_*.json` (pirush, hakdamot, maqaf_range,
otzar_heading, ravitzky_tight). Unit test `test_word_gap_fraction_otsu_valley`.

## NEW ISSUES reported by Hillel (2026-05-31) — ALL RESOLVED 2026-05-31

**RESOLUTION SUMMARY (see OPEN_ISSUES D-F13d):**
- **N1 FIXED** — root cause was NOT the Otsu outlier (a probe DISPROVED it: word
  gaps were ~0.02 em, identical to intra-word gaps — no gap signal at all). The
  words carry a **zero-WIDTH space glyph** at each boundary. Fix = re-introduced
  the space glyph as a secondary boundary signal, gated LOCALLY (a space is a
  word break only if neither immediately-adjacent inter-base position also has a
  space — so letter-spacing, which spaces every letter, is suppressed). Purely
  additive; kill-switch `_SPACE_BOUNDARY_ENABLED`. A/B: Du-Siach merge
  0.13%→0.04%, Hakdamot 0.14%→0.00%, ZERO added shatter (Otzar 5.37→5.36).
- **N2 FIXED** — `דו־שיח`/`ובעת־ובעונה`/`ארץ־ישראל`/`לב־לבה`/`על־ידו` all preserve
  the maqaf in the new extractor (the user's `דושיח` was the OLD format-v2 output;
  the D-F13b `Mn` fix already resolved it — needs Re-index All).
- **N3 FIXED** — `_order_unit_text_rtl` now re-flips embedded LTR runs (digits/
  Latin/numeric separators) back to ascending (bidi "reverse level run"). Years:
  OLD `3191,5191,6191` → NEW `1913,1915,1916`.

Tests: `test_zero_width_space_glyph_forces_boundary`,
`test_letter_spaced_run_spaces_suppressed`,
`test_order_unit_keeps_embedded_ltr_run_ascending`,
`test_year_in_rtl_line_not_reversed` + real fixtures `real_dusiach_packed_names`
(N1) / `real_dusiach_year` (N3). 132 local-PDF tests pass, ruff clean.

---

### N1 — Over-merge persists in STRUCTURED / mixed-content lines  (RESOLVED — see above)
Otsu still merges words on lines that contain a large STRUCTURAL gap (column / tab /
indent / abbreviation-table 2nd column). Examples:
- `דו שיח בין חכמים - תיאודור דרייפוס.pdf` p.103: `פרנץ רוזנצווייגושמואלהוגוברגמן`
  (should be `פרנץ רוזנצווייג ושמואל הוגו ברגמן`); `׳אורותהתשובה`.
- `הקדמות הרמבם למשנה - שילת.pdf` p.22 (abbreviation table): `פיהמ״ש פירושהמשנה`,
  `ע״ע עייןעוד`, `הל׳ הלכות(במשנהתורה)`, `רה״י כתביהיד` — the EXPANSION column merges.
- `ירחי משוח מלחמה.pdf` p.225 (bibliography): `משנהתורהלרמב"ם,מהדורתהרביוסףקאפח`
  merged, while sibling lines with the same content are correct.
**Root-cause hypothesis (HIGH confidence):** the known Otsu OUTLIER sensitivity — a
single large structural gap on the line dominates 1-D Otsu's between-class variance,
pushing the valley ABOVE the small real word-gaps → they merge. This is NOT rare; it
hits tables/bibliographies/title lines, which are common.
**Fix directions to try (validate on these 3 PDFs):**
  (a) Outlier-robust valley: down-weight / exclude an ISOLATED top outlier before Otsu,
      BUT must NOT break the legitimate short 2-word line where the single largest gap
      IS the word boundary. Distinguish by "is there a middle word-gap cluster between
      the intra bulk and the outlier?" (if yes → the outlier is structural, re-run Otsu
      on gaps ≤ some robust upper bound; if no → keep it).
  (b) Largest-jump in the LOWER region (gaps ≤ ~1.2×em) instead of global Otsu —
      but earlier this mis-handled multi-scale Otzar headings (tracking/word/big-word).
  (c) Robust clip = min(_GAP_OUTLIER_CAP, median + k·MAD of positive gaps) with a
      short-line guard (n < ~6 → skip the relative clip).
  Whichever: re-run `_tmp/tune_otsu.py` style harness adding דו־שיח + הקדמות-שילת +
  ירחי as books, measuring merge% (>15-letter tokens) AND single% on all 5 books.

### N2 — Maqaf (־ U+05BE) dropped
`דו שיח בין חכמים` p.101: title `דו־שיח` → `דושיח` (maqaf GONE, and the two words
merged). Need to PROBE whether the rawdict actually contains U+05BE here, or the PDF
renders the maqaf as a different codepoint (ASCII hyphen / drawn rule) that gets
dropped. If U+05BE is present, the Mn fix should keep it as a base glyph — so a drop
would be a real bug. (Earlier maqaf ranges סב־סג / פה־פו extract correctly in the Igrot
fixtures, so this PDF may encode the maqaf differently.)

### N3 — Digit-run reversal  (likely DOWNSTREAM of N1)
`ירחי` p.225: `ירושלים 7791` (=1977 reversed), `2991`(1992), `8991`(1998), `6791`(1976),
`491-212`. Hypothesis: when a line over-merges (N1), digit runs get swept into RTL units
and `_order_unit_text_rtl`'s descending-center-x reverses them, while
`reorder_word_units_rtl`'s F-A "re-reverse digit-only units" (`_is_digit_unit` =
`t.strip().isdigit()`) doesn't fire because the digits aren't an isolated pure-digit
unit. CHECK whether N3 disappears once N1 is fixed; if not, fix F-A to also flip
digit runs inside otherwise-mixed units, or detect digit runs pre-merge.

## Probe scripts (all in `_tmp/`, gitignored)
- `probe_overmerge.py <pdf> <page>` — dumps per-line font size, edge gaps, the Otsu
  gap_fraction, and OUT. Best tool for N1/N2 diagnosis. (Currently imports
  `_WORD_GAP_FONT_FRACTION`/`_ADAPTIVE_GAP_MULT` which were REMOVED — update it to import
  `_word_gap_fraction` + the `_GAP_*` constants before re-running.)
- `tune_otsu.py`, `tune_floor.py` — threshold tuning harnesses (3 books; add the new ones).
- `final_measure.py` / `corpus_final.py` — production-pipeline outcome measurement.
- `extract_real_fixtures.py` / `extract_otzar_heading.py` — fixture extractors.

## Gotcha
Full `pytest tests/` segfaults on Windows (pre-existing `genizah_core._build_fl_id_index`
daemon-thread access violation, unrelated). Use targeted runs with `PYTHONUTF8=1`.
