# Codex code-review — Quick Task 260714-9jc (SEED-030)

Model: gpt-5.5 (xhigh). 4 rounds. Briefs/diffs under `tmp/CODEX-*seed030*` + `tmp/seed030_codediff_v*.patch`.

## Round 1 — CHANGES REQUESTED → fixed (`1d3ef49c`)
- HIGH: `get_fgp_section_for_page(ed, page_num)` used the global page as a recto/verso flag → full
  recto row on page ≥2 measured empty → wrong demote. Fixed: measure displayed whole-row `content`.
- MEDIUM: desktop selected first FGP edition, not the policy's best. Fixed (source_id match).
- LOW: static-only wiring tests.

## UAT revision (`fd331e43`)
Real shelfmark 990000925330205171 still kept a whole-document *selective* FGP (772 letters = 2.6%
of a 26-folio MS): v1 compared it against a single folio's HTR. Added the whole-MS baseline for
whole-doc rows (lazy `full_htr_getter`), keeping foliated rows on the folio baseline.

## Round 2 — CHANGES REQUESTED → fixed (`6166fdf5`)
- HIGH F1: whole-doc detection used `_fgp_match_folio` only → 5,822 c-numbered per-image editions
  misclassified as whole-doc. Fixed: `_fgp_is_whole_doc` = no folio AND no c_number.
- HIGH F2: web Advanced view (`search_results.py`) auto-defaulted to `editions[0]` and rendered the
  selector without the whole-MS baseline. Fixed: route through `choose_default_source` + pass
  `full_original_text`.
- MEDIUM F3: `_auto_select_pgp_edition`/`_populate_pgp_combo` read `self.browse_*`; ResultDialog
  (which calls both) scored against the Browse tab's manuscript. Fixed: caller passes
  `sys_id`/`htr_text`.

## Rounds 3 & 4 — APPROVE, no findings
> "F1 resolved … F2 resolved … F3 resolved … No new defect found in desktop param threading,
> Advanced display_text fallback, or lazy getter scope. Verdict: APPROVE."

(Codex could not run pytest in its sandbox — logon-session error. Tests were run locally:
26 helper/wiring tests + 165 in the fgp/version-selector set, all green.)

## User decisions
- Reject the "keep FGP on a blank-HTR folio" refinement — selective whole-doc rows demote on every
  folio (2026-07-14).
