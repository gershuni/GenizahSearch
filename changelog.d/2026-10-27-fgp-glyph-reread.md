## [Unreleased] -- FGP: letters restored by reading the PDF fonts' own glyph names (web + desktop, branch `master-main`)

### Improvements
- **Letters restored in 1,435 FGP transcriptions that keep their PDF text** (GS-276). FGP's PDFs name every glyph
  in their embedded fonts, but most carry no Unicode map, so extraction showed final letters, vowel points and Arabic
  letters as boxes, left whitespace-coded letters out, or showed wrong symbols. Gate 3 now decodes each drawn glyph by
  its font and byte code -- through a reviewed glyph-name registry (`scripts/fgp_glyph_registry.py`) and an exact
  outline match against pinned reference fonts -- re-extracts a repaired copy of the PDF and checks every change glyph
  by glyph (`scripts/fgp_glyph_reread.py`; checks T11-T14). 85.4% of the Hebrew letters missing from these texts are
  recovered; against the Word texts the decoded letters agree 99.97%. Pointed letters are stored as letter plus points,
  Arabic as plain letters. Rows the checks cannot clear keep today's text with the vocabulary repair of the entry
  below (161 rows) or as they were. Rules: `docs/plans/FGP_TEXT_PIPELINE_RULES.md` §9; candidate sidecar rebuilt
  (gate 3 `9ae25fa8`, gate 4 `08669002`, sha256 `cdcc2268…`, still 59,056 rows); gate 4 attaches a re-read only to the
  stored text and the PDF gate 3 read.
