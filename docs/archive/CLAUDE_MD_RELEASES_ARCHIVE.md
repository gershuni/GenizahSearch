# CLAUDE.md release one-liners, archived

> **Moved out of `CLAUDE.md` ("Recently Changed" > "Releases") on 2026-11-04**, the day that
> file reached its 40,000-byte ceiling (`scripts/check_docs.py`, `CONTEXT_BUDGET`). The lines
> below are verbatim. They were always condensations: the full release notes are the
> `## [X.Y.Z]` sections of [`CHANGELOG.md`](../../CHANGELOG.md), which remains the authority
> where the two disagree. Newer one-liners stay in `CLAUDE.md` until they are moved here in
> turn; the facts that outlive a release stay in `CLAUDE.md` for good.

- **v9.2.0 — Citations, Princeton links, and a round of repairs (2026-09-06)** — NOTE the
  span: this tag covers 67 commits, including PR #333 and PR #334, not one branch.
  Desktop gained **PGP links on three surfaces** and **separate Exclude Manuscripts
  lists** for Search vs Composition Search (they shared one); composition header-click
  sorting had NEVER fired (`setHeader()` discards every width, mode and signal), two
  composition crashes fixed, Browse showed hits as literal asterisks, Oxford images died
  silently, and a crawler could take `/browse` down. On citations: one shared decision
  (`shared/transcription_credits.py`) drives the printed sheet, the web "How to cite" chip,
  Word exports, the desktop citation bar and the new Cite buttons, so a citation copied from
  the screen, printed, or exported cannot disagree about who made the text. MiDRASH is cited
  for the automatic transcription and ONLY that; a Princeton/Friedberg edition, a translation
  or a community correction is credited to those people. **The desktop cites the APPLICATION**
  ("Dicta Genizah Search Pro V<version>", name untranslated in both UIs, no address and no
  retrieval date) rather than the website. New: print/Save-as-PDF on the web reading view
  (`contain: none !important` is load-bearing — Quasar's `contain: strict` defeats every
  height rule in print), "Cite this page" on the desktop Browse toolbar and in ResultDialog,
  and a calmer homepage (one strip, not four overlays; the two big promotion cards removed).
  `page_citation` RAISES if given both a software clause and a URL/date — a citation is one
  kind of thing or the other. 15 production defects were found across four Codex review rounds,
  two of them the INVERSE of the original bug (a .docx shipping HTR text credited to the
  selected scholar; an FGP translation headed "Transcription"). (both)
- **v9.1.0 — Several Witnesses of One Work (2026-08-27)** — desktop multi-witness letter-level search: a Witnesses panel in the Composition tab (paste one, or a file split on blank lines), promote manuscripts from your own results, and optional auto-expand rounds; results are rank-fused (RRF k=60), NEVER concatenated — 17 Birkat Hamazon witnesses fused find 455 of 614 census manuscripts (74.1%) against 348 (56.7%) for the best single witness and 296 (48.2%) concatenated. Stop now works BETWEEN witnesses (the one in flight finishes; "New" defers and never terminates), so the witness cap is a flat 25 at every depth. "Full Recursive Search" runs fusion in letter-level mode and still concatenates in chunk mode, which is correct for that engine. Letter-level search is now the DEFAULT method once an index exists -- but never over a stored choice; the orange "New" markers are one-shot (`config.pkl`, not the session). In the manuscript viewer, letter-level matches are HIGHLIGHTED (passage rows carry `highlight_pattern` + `source_highlight_pattern`, literal alternations of what each side matched -- the viewer re-marks a whole page from a regex and had none), and Prev/Next Manuscript skips the remaining pages of the fragment you are reading. **Desktop-only.** (desktop)
- **v8.6.0 — Pause/Resume, and Stop that actually stops (2026-08-20)** — desktop Pause/Resume for regular, Lab and Composition searches (parks the worker at a checkpoint; Resume continues at the same index, paused time excluded from timings); Stop repaired in Lab Mode (three causes, incl. a bare `except Exception` eating `InterruptedError`, an `OSError` subclass) and stopped runs now keep partial results in Title/Shelfmark/My Library; monotonic elapsed timing; search toolbar no longer overlaps itself (row 2 needed 1423 px on a 1440 px screen). **Desktop-only — the desktop line continues from v8.5.2, since 9.0.0 was web-only and never tagged.** (desktop)
- **v9.0.0 — Computed Identifications, the Visual Atlas & Start Here (public beta, 2026-08-16)** — `DISCOVERY_ENABLED` flipped ON in production 2026-08-08; `/computed-identifications`, the browse connections panel and the `/help` methods section are publicly reachable. Then a fast post-launch run: catalogue-divergent rows un-hidden (3,570, 12.5% of the main pool), a frozen relation-precedence matrix, a "View text match" excerpt view, a `/start` guided launchpad, homepage discovery entry points, and beta community identification reviews. **Desktop 9.0.0 followed 2026-08-26** — letter-level (passage) search in the Composition tab, on a locally built index; that half does carry a GitHub Release + installer. One REL-01 gate item (the cross-surface masking re-sweep) is still open; see ROADMAP.md. (both)
