# FIST Gap Fill Statistics (v7.1.0)

**Date:** 2026-03-19
**Source:** `fist_data/FIST.db` (Friedberg Genizah Project database)
**Script:** `scripts/generate_fist_gap_csv.py`

## Summary

| Metric | Value |
|--------|-------|
| Records added | 38,673 |
| Previous libraries.csv | 216,942 |
| New libraries.csv | 255,615 |
| Growth | +17.8% |
| Records with titles | 7,804 (20.2%) |
| Records without titles | 30,869 (79.8%) |
| Distinct libraries | 52 |
| New library codes added | 7 |

## New Library Codes

| Code | Name | Hebrew | Records |
|------|------|--------|---------|
| Solomon | Solomon Halberstam Collection | אוסף שלמה הלברשטם | 15 |
| Reinach | Reinach Collection | אוסף ריינך | 6 |
| Vatican | Vatican Library | ספריית הוותיקן | 1 |
| CentralArch | Central Archives for the History of the Jewish People | הארכיון המרכזי לתולדות העם היהודי | 1 |
| JCMainz | Jewish Community of Mainz | הקהילה היהודית של מיינץ | 1 |
| Corwin | Corwin Collection | אוסף קורווין | 1 |
| Mehlman | Mehlman Collection | אוסף מהלמן | 1 |

## Library Distribution

| Library | Records | % |
|---------|---------|---|
| JTS | 13,520 | 35.0% |
| CUL | 12,641 | 32.7% |
| Mosseri | 4,862 | 12.6% |
| BL | 2,982 | 7.7% |
| Manchester | 1,741 | 4.5% |
| AIU | 809 | 2.1% |
| Oxford | 602 | 1.6% |
| RNL | 455 | 1.2% |
| HAS | 294 | 0.8% |
| Westminster | 158 | 0.4% |
| Sofer | 112 | 0.3% |
| RSL | 71 | 0.2% |
| Senckenberg | 57 | 0.1% |
| Strasbourg | 34 | 0.1% |
| Columbia | 32 | 0.1% |
| Katz | 30 | 0.1% |
| IOM | 30 | 0.1% |
| Leeds | 28 | 0.1% |
| Duke | 22 | 0.1% |
| Vienna | 21 | 0.1% |
| Birmingham | 20 | 0.1% |
| JCBerlin | 20 | 0.1% |
| Solomon | 15 | <0.1% |
| Harkavy | 13 | <0.1% |
| HUC | 12 | <0.1% |
| NLI | 11 | <0.1% |
| Bisno | 11 | <0.1% |
| Haifa | 9 | <0.1% |
| Geneva | 7 | <0.1% |
| UPenn | 7 | <0.1% |
| Schoeyen | 6 | <0.1% |
| Chetham | 6 | <0.1% |
| Reinach | 6 | <0.1% |
| Heidelberg | 4 | <0.1% |
| Sassoon | 3 | <0.1% |
| Lehmann | 3 | <0.1% |
| SBB | 2 | <0.1% |
| Freer | 2 | <0.1% |
| Warsaw | 2 | <0.1% |
| + 13 more | 1 each | <0.1% |

## Deduplication

Records were deduplicated by AlmaId (NLI system number). Each AlmaId appears exactly once in `fist_gap_rows.csv`. When multiple FIST inventory records shared the same AlmaId:
- Same library: shortest non-empty shelfmark selected, variants collapsed to pipe-separated `call_numbers`
- Cross-library (3 cases): resolved via hardcoded override table

## Title Extraction

Titles were extracted from `fjms_enrichment.db` catalog table (`GenizahTitleOrgTitle` column). For AlmaIds with multiple distinct title values (828 cases), the title was left empty and logged to `fist_gap_ambiguous_titles.txt` for human review.

## Shelfmark Normalization

Two new aliases added to `normalize_shelfmark()`:
- **Yevr → EVR**: Russian National Library "Yevr." prefix normalized to "EVR" (FIST uses Yevr., CSV uses EVR)
- **Halper → Genizah**: CAJS "Halper" prefix normalized to "Genizah" (FIST uses Halper, CSV uses Genizah), with guard to exclude "Halpern" collection

## Known Limitations

- **No transcription text**: These records are metadata-only (no Tantivy index entries). They appear in title/shelfmark search and catalog browse but not in text/word search.
- **FJMS bibliography**: Some records show incorrect bibliography associations. Needs separate investigation.
- **Ambiguous titles**: 828 AlmaIds with conflicting titles were left without titles. See `fist_gap_ambiguous_titles.txt`.
