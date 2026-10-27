## [Unreleased] -- FGP: final letters restored in the transcriptions that keep their PDF text (web + desktop, branch `master-main`)

### Improvements
- **Final letters restored in 1,174 FGP transcriptions** (GS-276). FGP's PDF copies left the final letters
  ך ם ן ף ץ as control characters, so readers saw boxes; the rows with no Word or line text kept them. Gate 3 now reads
  each row's code-to-letter mapping off the row's own words against the vocabulary of the clean FGP texts
  (`scripts/fgp_final_letters.py`) and applies a code only on clear evidence (at least two votes, 70% of them);
  measured on the 5,392 rows that exist both ways, 6,421 of 6,426 applied codes are right (check T10, every run).
  1,967 codes (119,522 occurrences) are restored; codes with weaker evidence and codes standing inside words (vowel
  points and dots) stay as typed. Rules: `docs/plans/FGP_TEXT_PIPELINE_RULES.md` §8. On 1,013 of these rows the
  glyph re-read of the next entry now replaces the vocabulary repair; 161 keep it.
