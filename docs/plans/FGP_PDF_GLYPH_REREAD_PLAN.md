# FGP: re-reading the PDF-text transcriptions through their fonts' glyph names (GS-276)

Status: **plan, revision 3, for owner approval** (2026-10-27). Revision 2 folded in Codex's first plan review
(`_tmp/codex_gs276_reread_plan.log`); revision 3 the three fixes from its second (`_tmp/codex_gs276_reread_plan_r2.log`:
GO WITH CHANGES, no blockers, no further plan round). Release v9.11.0 waits for this work. **Steps 1-6 done
2026-10-27** (runs, the owner's exceptions and rulings, and the gate integration recorded in
FGP_TEXT_PIPELINE_RULES.md section 9). The candidate is rebuilt (1,435 rows re-read, sha256 cdcc2268...). Step 7 done the same day (checks in both apps,
docs, Codex review in two rounds, fixes folded in); the release resumes on the owner's word. **2026-10-28**: the owner
ruled the name rule and the tie rule in (step 3, amended); the candidate is rebuilt through gates 3-4 (rules §9).

## The problem, measured

10,856 candidate rows (9,961 distinct texts) ship text extracted from FGP's PDFs (`text_source` `pdf_rtl`,
`original`, and their `+final_letters` variants). Their PDFs embed fonts whose glyphs are named in the font's
`/Differences` list (`nunfinal`, `shinwithshindot`, `vavwithholam`, `alefisolated`, `hyphenminus`, ...), but most of
these fonts carry no Unicode map, and the extractor does not know those names, so it falls back to the raw byte
code. Census over all of them (`_tmp/gs276_glyphnames/fl_loss_census.py`, exact font + byte code per glyph,
pdfminer; Codex reproduced the totals):

- 1,425 distinct texts lack Hebrew letters the PDF draws: **79,030 letters, 75,605 of them final letters**.
- At most 9,442 of those show as boxes. The rest are **dropped** (the byte code is a tab, line feed, form feed or
  space, so the extractor removes it) or **shown as a wrong symbol** (`#` `"` `)` `,` `%` ...). 783 texts show no box.
- 48,576 Arabic-letter glyphs are decoded wrong the same way.

Examples: row 39645 "פרקא יכו אלד ... שמ" for פרק אן יכון אלדם ... שמן; row 31 "ואיל"" for ואילך.

The vocabulary repair in 3d62c5ad only sees boxes. Gate 3 recorded 1,176 repaired rows; the candidate ships 1,174 of
them (1,123 `pdf_rtl`, 51 `original`; rows 5404 and 35933 are not in the candidate). On the codes it replaced, the
glyph names agree in 1,910 of 1,913 checkable row-codes and are right in the other 3 (rows 17436, 24583, 24586); 54
applied row-codes have no single name and must be resolved through their actual fonts.

## The method

For each PDF, build a scratch copy in which every simple font gets the Unicode map that its certified glyph
identities imply, then run **the same extraction sequence that made the stored text**:
`scripts/fgp_rebuild_text_and_credit.py::reextract_pdf` (`shared.local_indexer.extract_pdf_pages`, pages joined with
a blank line, stripped), then the caller's `_collapse_blanks` (`scripts/fgp_rebuild_text_and_credit.py:312`).

The extractor's code is unchanged, but **its reconstruction can change**: restored characters feed decisions taken
for a whole line or page, namely the RTL/LTR classification at the 0.4 threshold, vowel-point re-attachment and
baseline grouping, the adaptive word-spacing threshold, the ordering of Latin and digit runs, the page-wide LTR
fallback and the ten-character page threshold (`shared/local_indexer.py:1159`, `shared/local_indexer_rtl.py:148`,
`:436`). The point re-attachment step can also drop a mark it cannot attach (`ephraim_meiri_pdf_converter/pdf_to_docx.py:787`).
The checks therefore trace every drawn glyph through both texts (C2); they do not assume the change stays local.

Spike on four rows (`_tmp/gs276_glyphnames/fl_tounicode_spike.py`): on the untouched PDFs of rows 39645 and 31 the
extractor reproduces the stored text exactly; on the repaired copies it restores 1,892 / 214 / 35 / 92 letters in rows
39645, 31, 3485 and 24583. Row 31 shows both sides of the caveat: its recovered hyphen correctly gives `A35-37` (Codex
checked the glyph positions in the PDF), and the digits around it move, so that row fails a strict C2 and needs a
recorded exception.

`shared/local_indexer.py` is not changed, so My Library is unaffected (the same defect may exist in users' PDFs; that
is a separate change with an `extraction_format_version` bump and a Re-index).

## Texts kept per row

| Text | What it is | Use |
|---|---|---|
| S₀ | today's stored text, before the vocabulary repair | C1 (the extractor reproduces it) |
| S | the current candidate text (S₀ plus the vocabulary repair on 1,174 rows) | what a held row keeps; rollback; regression checks |
| U | the extractor on the untouched PDF | baseline for C2 |
| R | the extractor on the repaired copy | what an accepted row ships |

A **held** row keeps S unchanged, with its `sections` and `text_source`, including the vocabulary repair; it is never
silently reverted. A row whose R leaves some glyph undecoded while S has a vocabulary letter at that place is held
unless the two agree. Rows that share one PDF (rows 24583 and 24586 have identical files) share the cached reading,
keyed by PDF hash, but keep their own decisions.

## Steps

1. **Glyph-name registry.** List every glyph name in every font of the PDFs in scope (step 5's benchmark included);
   map each to a Unicode sequence in one versioned table with tests. Pointed letters map to the base letter plus
   canonically ordered marks; Arabic positional forms map to base letters, with the source form kept in provenance.
   Names it cannot map (so far `shinwithdageshandsindot`, `qibuts`, some `afii627xx`) are listed for review, never
   guessed.
2. **Decoder.** Read each font through PyMuPDF's object API (indirect or inline `/Encoding`, any generation number;
   fonts identified by their PDF resource, never by display name). Classify existing `/ToUnicode` maps as complete,
   incomplete or conflicting: keep their valid entries; fill a missing entry only when step 3 certifies it (row 3389's
   map lacks byte 35, which draws a sheva); hold the row on a conflict. Write the repaired copy to a gitignored cache
   keyed by PDF hash, registry version, certification version and extractor version. Tests on synthetic PDFs,
   including the parser cases Codex reproduced.
3. **Glyph certification.** A name is evidence, not proof: for the TrueType fonts here the viewer draws through the
   font's `cmap`, so a name could be wrong (Type 1 fonts select the glyph by name, which narrows but does not remove
   the question). Certification binds one **occurrence class**: PDF font resource + byte code + the glyph actually
   drawn + the Unicode sequence. A match means equivalent unhinted geometry within an identified typeface and style,
   after decomposing composite glyphs, normalising units per em and contour start/order, and keeping all contours,
   relative mark positions and advance width. The result must be **one supported Unicode interpretation**
   (several reference glyphs may support it; two interpretations hold it); empty outlines and `.notdef` never count.
   Arabic compares against the contextual or ligature glyph after shaping, not the bare letter. Reference fonts
   (David, Narkisim, Frank Ruehl, Miriam, Times New Roman, Arial ...) are pinned by file hash. Near-misses such as row
   21159's shin dot go to a recorded review by eye, never a similarity threshold. A glyph with no name is decoded only
   by a certified match.
   *Amended by the owner's ruling of 2026-10-28* (rules §9): where the outline matches no reference glyph of its
   typeface (the typeface is not installed here), the name decides, after every such shape was checked by eye on a
   review page and with no private-use reading; and a plain Hebrew letter or point tied with only its variant forms
   reads as the plain one.
4. **Occurrence tracing harness.** The re-read script calls the extractor through a tracing wrapper inside its own
   process (no change to `shared/`): each glyph the PDF draws gets an identity (PDF hash, page, content-stream position,
   font resource, byte, glyph id), and the wrapper records which identities the extractor emits, in what order, for U
   and for R. Failing fixtures prove the checks can fail: an unrelated deletion, intact words reordered, a detached
   point, a missing page, and a run that applies no mapping. If tracing cannot attribute an occurrence, the row is held.
5. **Measure before touching the candidate.** A standalone script computes S₀, S, U and R for every row in scope and
   for the benchmark, runs C1-C7, and produces a review page for you (before/after lines, held rows with reasons,
   every C2 exception and every C3 disagreement).
6. **Gate integration.** Gate 3 gains a re-read step (rules `FGP_TEXT_PIPELINE_RULES.md` §9, written before the run)
   that replaces the vocabulary step as the repair; the vocabulary votes remain only as a flag where they disagree.
   Tables `glyph_map` (font resource, byte, name, sequence, certification evidence), `glyph_reread` (row, decision,
   reasons, counts) and the C2 exceptions. Gate 3's checks, outputs and digest take the step in; gate 4's attachment
   loop (`scripts/fgp_gate4_chooser_validation.py:1276`) writes accepted rows (`text_provenance` `gate3:glyph_reread`,
   `text_source` `pdf_rtl+glyphs`) and leaves held rows as S.
7. **Runs and review.** Gates 3-4 twice each with fresh caches, the release check, search and display checks in both
   apps (pointed Hebrew, Arabic joining and ligatures, mixed-script headings, copy and paste, search highlighting),
   docs (rules §9, GS-276 rewritten, handoff, changelog, release-text numbers), a Codex review with a two-round
   budget, then the release resumes.

## Checks, declared before the run

| Check | Passes when |
|---|---|
| C1 extractor reproduces | U equals S₀. A row where it does not is held, not re-read. |
| C2 only decoding edits (per occurrence) | Let D be the occurrences whose decoded value changes. Every occurrence outside D keeps its characters, count and relative order from U to R; every D occurrence emits exactly its certified sequence; spaces change only at the boundaries next to a D occurrence; no line, page or mark attachment changes otherwise. Any other edit holds the row, unless it is a recorded exception tied to that row's U and R hashes with the evidence (row 31 is the first). |
| C3 against the Word text | Benchmark: every PDF row whose text gate 3 replaces with Word or line text, deduplicated by PDF hash (not only the 5,392 rows with boxes, which are 4,225 distinct texts). Each decoded occurrence is aligned to its place in the Word text and compared, marks, punctuation, the spaces around it and its order among its neighbours included, under a declared normalisation. Passes when at least 90% of decoded occurrences are aligned and judged, at least 99.5% of those agree, and every disagreement is listed and classified (the PDF and Word genuinely differ, or a decoding error). **Every row with an identified decoding error is held**, however isolated; a repeated cause also holds that font mapping everywhere. |
| C4 certified | Every applied occurrence class (font resource + byte + glyph + sequence) is certified under step 3. |
| C5 accounted for, and recovered | **Accounting:** every glyph the PDF draws is emitted in R as its certified sequence or recorded as undecoded or held; no character in R lacks a source glyph, except spaces and line breaks the extractor generates; reported separately for Hebrew letters, marks, Arabic and punctuation, plus skipped pages. **Recovery:** against fixed denominators taken from the census before the run (79,030 missing Hebrew letters in 1,425 distinct texts, 75,605 of them finals; 48,576 Arabic occurrences), report the share recovered, held and left undecoded. Accounting alone never passes: the minimum recovered share and the maximum unresolved share are written into rules §9 at the end of step 4, before the measured run, and a run that recovers nothing (the zero-mapping fixture) fails. |
| C6 agrees with today's repair | Resolved per occurrence through the actual fonts: R matches the vocabulary letters where the names agree (1,910 row-codes), takes the names in rows 17436, 24583 and 24586, and decides the 54 unresolved row-codes or holds those rows. Gate 3's population (1,176) and the shipped rows (1,174) are reported apart. |
| C7 reproducible | Two runs with fresh caches give one digest. The digest covers R and S texts, `sections`, `text_source`, `text_provenance`, decisions, held reasons, mappings, certification evidence and the input versions (PDF hashes, registry, reference-font hashes, extractor code and PyMuPDF version). Gate 4's digest is extended to `sections` and `text_source`, which it omits today (`scripts/fgp_gate4_chooser_validation.py:1375`). |

## Decisions for you

1. **Pointed letters** (`vavwithholam`, `betwithdagesh`, `shinwithshindot`): keep the vowel point or dot as the
   transcriber typed it, stored as the letter plus the point (recommended), or the bare letter? The FGP matching code
   ignores separate points (`shared/fgp_service.py:544`) but not the precomposed forms, so letter plus point is also
   the searchable form.
2. **Arabic letters** drawn in their joined shapes (`alefisolated`, `meemmedial`): store plain Arabic letters, which the
   display joins again (recommended; searchable; a deliberately isolated form or a particular ligature may render
   differently, so the source form is kept in provenance), or the shape characters?
3. **Glyphs with no name:** decode only by a certified match (recommended), otherwise leave them as they are now;
   never by vocabulary.
4. **The 215 `original` rows** (text from FGP's first extraction, with a per-page split): hold them with their
   current content and split, the vocabulary repair on 51 of them included, for a separate pass (recommended), or
   include them with C1 relaxed?

## Not in scope

The Word, line and XML texts (not from PDFs); My Library extraction; normalisation of anything except the decoded
sequences (no blanket NFKC over the transcriptions); `#` used as a real Genuzot mark (kept: only a glyph certified as
a letter or mark changes).
