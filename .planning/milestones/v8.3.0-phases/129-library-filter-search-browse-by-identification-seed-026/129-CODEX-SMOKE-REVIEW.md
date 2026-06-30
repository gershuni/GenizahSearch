---
phase: 129-library-filter-search-browse-by-identification-seed-026
gate: codex-code-review (smoke-round redesign + restore/label fixes)
reviewer: codex (gpt-5.x via codex exec, source-based)
reviewed: 2026-06-29
verdict: APPROVE WITH CHANGES (0 blocker, 0 high, 1 medium, 1 low — all fixed)
status: resolved
---

# Phase 129 — Codex review of the smoke-round redesign (cross-AI gate)

Independent Codex review of the post-UAT work in this session: the chips→button-state
redesign, the HTTP-500 ordering fix, the language-change-restore button-reveal fix
(incl. lifting `collect_fjms_enrichment` / `_process_domain_data` to page level), and the
label-consistency change. Brief + full source/test diffs handed to Codex; review was
source-based (Codex's local pytest launchers were stale, so it did not run tests — ours did).

## Verdict: APPROVE WITH CHANGES

Codex confirmed:
- **GUARD-02 preserved** — empty/None `library_codes` add no SQL condition in `fjms_service`.
- **Lifted FJMS enrichment helpers are closure-safe**; restore re-enrichment runs through
  `_after_delay`, which catches deleted-slot RuntimeErrors (no repeat of the earlier race).
- Changed web persistence paths use `safe_user_*` / `persist_value`; **no `search_library_filter`
  leak** from Parallels back into `/search`.

## Findings & resolutions

### MEDIUM — web catalog→Parallels handoff silently dropped the library filter — FIXED (`cb2bd5b5`)
`catalog_browse._build_incoming_filters` carried `library_filter` to BOTH targets, and the
"Parallels in these results" button was enabled by a library-only selection. But web Parallels
has no library scoping — `consume_incoming_filters(...,'parallels')` popped the incoming signal
and ignored library → **unscoped parallels** (misleading). (Desktop is unaffected:
`_catalog_parallels_in_results` intersects `restrict_sys_ids`.) Chose Codex option (b) — keep the
deliberate search-only library design rather than porting the whole filter (state + persistence +
UI) to Parallels:
- new `_has_active_filters_excluding_library()` gates the Parallels button (library-only no longer
  enables it);
- `_build_incoming_filters(include_library=False)` for the Parallels handoff;
- `_parallels_in_results()` notifies the user when a library selection won't carry over.
- Search handoff still carries library (default `include_library=True`).
New test: `test_parallels_handoff_excludes_library`.

### LOW — web `/search` Apply not initialized disabled at zero-checked — FIXED (`cb2bd5b5`)
When a persisted `library_filter` matches none of a later result set, the dialog opens
all-unchecked but Apply started enabled (the Python guard already blocked the `[]` commit, so the
sentinel was safe, but the client invariant was incomplete). Now runs
`libFilterUpdateApply(container_id)` immediately after `dialog.open()`.

## Design recommendation (desktop SEARCH library filter)
Codex AND the inline analysis agree: **do NOT add a dedicated library-filter button to the desktop
SEARCH toolbar now.** The desktop results table's per-column filter is a TEXT filter over rendered
rows — not equivalent to the web library filter (which scopes the FULL pre-cap result set / counts /
history), but a new toolbar button would add clutter and duplicate the row-filter mental model. If
first-class desktop library scoping is wanted later, put it in the existing pre-search ("Focus
Search") filter dialog as a scope control — NOT in "Exclude manuscripts" and NOT as another
post-search toolbar button — so it gets true full-set correctness without expanding the toolbar.
DEFERRED (not in this phase).

## Note: web Parallels library parity (deferred)
Desktop catalog→parallels applies library (via `restrict_sys_ids`); web Parallels does not. Option
(b) makes the web behavior honest (no silent drop) but does not close that parity gap. True web
parallels library scoping would need parallels-side state + persistence + a library UI indicator +
threading into `get_filter_sys_ids` — a separate enhancement, not a review fix.
