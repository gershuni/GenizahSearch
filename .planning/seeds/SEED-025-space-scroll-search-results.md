---
id: SEED-025
status: dormant
planted: 2026-06-26
planted_during: mid-Phase-125 (v8.3.0 god-file decomposition) — parked as a seed rather than implemented inline ([[feedback_seed_midphase_fixes_to_cloud]]); UNRELATED to the decomposition milestone.
trigger_when: A keyboard-nav / UX polish pass or a standalone /gsd-quick. Self-contained, no dependency on v8.3.0. Web (NiceGUI) is the likely primary surface; assess desktop (PyQt6) parity.
scope: small (one keydown handler on the search-results scroll container + a focus/selection guard)
---

# SEED-025: Scroll search results with the Space key (when no result action is focused)

> User request (2026-06-26): "I want to be able to scroll search results with Space (if nothing
> was selected to be checked/opened/closed)."

## Intent

Pressing **Space** should **page-scroll the search-results area** — the normal "scroll down a
page" affordance — **but only when no result element is focused for an action**. If a result row
is focused for a check (checkbox), open (expand accordion / open detail), or close (collapse)
action, Space should keep doing that action (don't steal the keystroke). The fix is: when nothing
actionable holds focus, Space falls through to scrolling the results list instead of being a no-op
(or instead of being swallowed by a focused-but-inert element).

## Why it's not trivially already-working

- **Web (NiceGUI / browser):** the browser scrolls the *page* with Space by default — but only when
  focus is on a non-interactive element. Once the user has Tab-focused a result's checkbox / expand
  button / link, Space toggles/activates that control instead of scrolling. Also, if the results live
  in their own `overflow:auto` scroll container (not the document body), Space may scroll the page,
  not the results pane. Need: a keydown handler on the results scroll container that, when the active
  element is NOT one of the actionable result controls, prevents-default and scrolls the container by
  ~one viewport (and Shift+Space scrolls up, matching the platform convention).
- **Desktop (PyQt6):** in a focused list/table/tree, Space typically toggles the current item's
  checkbox or activates it. Need: when no item is in a "checkable/open/close" actionable focus state,
  route Space to the results scroll area's page-down (and Shift+Space page-up).

## Open questions (resolve at trigger time)

1. **Which app(s)?** The phrasing (checked / opened / closed) fits the search-results accordion+checkbox
   pattern present in BOTH web and desktop. Confirm whether the user means web, desktop, or both
   (default assumption: web primary, desktop parity if cheap).
2. **"Nothing selected" definition** — precisely which focus/selection states suppress the scroll
   (focused checkbox? expanded row? an open detail dialog? a multi-select set?). Enumerate the
   actionable states; everything else → scroll.
3. **Scroll amount / direction** — one viewport page (and Shift+Space = up)? Or a fixed row count?
4. **Accessibility** — Space-to-scroll must not break screen-reader / keyboard semantics on the
   actionable controls (don't preventDefault when a control legitimately wants Space).

## Pointers (verify at trigger time — code locations drift; this is mid-decomposition)

- Web search results UI: `web/pages/search.py` + the results card/accordion components in
  `web/components/` (grep for the result-card render + checkbox/expand handlers).
- Desktop results: the results table/list in `genizah_app.py` (or its post-v8.3.0 decomposed panel,
  e.g. `desktop/search_results_panel.py` once Phase 126 lands).
- NiceGUI keydown wiring: `ui.keyboard` / element `.on('keydown', ...)` with a `js_handler` for
  `preventDefault` + container `.scrollBy(...)`.
