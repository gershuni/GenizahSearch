---
status: resolved
trigger: "Prod log 2026-06-12 12:09:37/38: AttributeError: 'GenericEventArguments' object has no attribute 'stop_propagation' at web/components/visual_similarity_dialog.py:602 click handler"
created: 2026-06-12
updated: 2026-06-12
---

## Symptoms

DATA_START
- **Expected:** Clicking the element wired at `web/components/visual_similarity_dialog.py:602` should stop the click event from propagating to parent elements (e.g. so a click on an inner control doesn't trigger the card/row's own click action), with no error.
- **Actual:** Every click raises an AttributeError server-side; the stop-propagation intent is a no-op (and the click likely DOES propagate to the parent handler).
- **Error (verbatim from prod journalctl, fired twice 12:09:37 and 12:09:38):**
  ```
  Traceback (most recent call last):
    File ".../nicegui/events.py", line 446, in handle_event
      result = cast(Callable[[EventT], Any], handler)(arguments)
    File "/home/ubuntu/GenizahSearch/web/components/visual_similarity_dialog.py", line 602, in <lambda>
      'click', lambda e: e.stop_propagation()
  AttributeError: 'GenericEventArguments' object has no attribute 'stop_propagation'
  ```
- **Timeline:** Observed live on genizah-web prod 2026-06-12. NiceGUI `GenericEventArguments` has never had a `stop_propagation` method — propagation control in NiceGUI is done client-side (e.g. Quasar/Vue event modifier `.stop`, registering the event as `'click.stop'`, or a `js_handler`). The Python-side lambda fires twice per click in the log, suggesting the event bubbles and is handled at two levels.
- **Reproduction:** Open the web Visual Similarity dialog on genizahsearch.com and click the element registered at visual_similarity_dialog.py:602 (likely an inner button/checkbox inside a clickable card).
DATA_END

## Evidence

- timestamp: 2026-06-12
  checked: .planning/debug/knowledge-base.md
  found: file does not exist
  implication: no known-pattern candidates; proceed with normal investigation

- timestamp: 2026-06-12
  checked: web/components/visual_similarity_dialog.py lines 540-654
  found: TWO identical buggy sites, not one. Line 574 — shelfmark `ui.link(...).on('click', lambda e: e.stop_propagation())`; lines 601-602 — `ui.link(target=browse_url, new_tab=True).on('click', lambda e: e.stop_propagation())` wrapping the open_in_new button. Both are inside the main row (line 555-557) which registers `.on('click', _on_row_click)` — the parent handler the lambdas were meant to shield. Same file ALREADY uses the correct client-side pattern at lines 609, 629, 647: `.on('click.stop', handler)`.
  implication: intent is clear (clicking a link must not trigger the row's expand/collapse click) and the correct in-codebase pattern exists right next to the bug. Server-side stop_propagation is impossible by design — the DOM event has already propagated client-side before the server hears about it.

- timestamp: 2026-06-12
  checked: repo-wide grep for stop_propagation (excluding .claude/worktrees mirrors)
  found: THREE buggy Python-side sites in main checkout — visual_similarity_dialog.py:574, :601-602, AND web/components/version_selector.py:344 (`with ui.link(target=pgp_url, new_tab=True)...on('click', lambda e: e.stop_propagation())`). All other stopPropagation matches are inside raw JS strings (browse.py, puzzle.py, static/manuscript_viewer.js) — correct client-side usage.
  implication: fix must cover all three sites; version_selector.py:344 throws the identical AttributeError on click (its shielded parent is `ui.menu_item(on_click=select_pgp)` — the inner "View on PGP" external link must not select the PGP version)

- timestamp: 2026-06-12
  checked: git log -S "stop_propagation" on both files
  found: visual_similarity_dialog.py sites introduced by 3de30765 "release: v7.9.3 — Visual Similarity Dialog Fixes" (2026-04-24); version_selector.py site introduced by e0dd6482 "fix(05): resolve PGP transcription navigation and display bugs" (2026-02-06)
  implication: the broken pattern originated in version_selector.py (Feb 2026) and was copy-propagated into the VS dialog in v7.9.3; the matching '.tooltip(Browse manuscript)' + comment at line 568 ("browser-native Ctrl/Cmd-click and middle-click") confirm the links were deliberately real anchors — fix must NOT preventDefault, only stopPropagation

- timestamp: 2026-06-12
  checked: installed NiceGUI version + Element.on() signature
  found: NiceGUI 3.8.0; `.on()` accepts `js_handler` (pure client-side handler, no server emit when no Python handler given); default js_handler is the emit shim. GenericEventArguments indeed has no stop_propagation — the server hears about the event only AFTER it has already propagated in the DOM, so server-side stop is impossible by design.
  implication: `.on('click', js_handler='(e) => e.stopPropagation()')` is the precise fix — stops propagation client-side, preserves anchor default navigation, zero per-click websocket traffic (unlike 'click.stop' + no-op Python handler)

## Eliminated

## Current Focus

reasoning_checkpoint:
  hypothesis: "All three `.on('click', lambda e: e.stop_propagation())` sites call a method that has never existed on NiceGUI's GenericEventArguments; every click raises AttributeError server-side AND the click propagates unshielded to the parent handler (VS dialog: _on_row_click toggles row expansion while the link navigates; version_selector: menu_item select_pgp fires while opening the PGP external link)."
  confirming_evidence:
    - "Prod traceback points exactly at visual_similarity_dialog.py:602 lambda; identical lambdas exist at :574 and version_selector.py:344"
    - "Installed NiceGUI 3.8.0 inspected: GenericEventArguments has no stop_propagation; Element.on() documents js_handler as the client-side mechanism"
    - "Same file already uses the correct client-side pattern ('click.stop') at lines 609/629/647 for the puzzle/list/join buttons"
  falsification_test: "If GenericEventArguments had a stop_propagation method in nicegui 3.8.0, the AttributeError could not occur — inspection confirms it does not; if the lambda were not the registered handler, the traceback line would differ"
  fix_rationale: "Replace the server-side lambda with `.on('click', js_handler='(e) => e.stopPropagation()')` at all three sites: stops propagation in the browser BEFORE it reaches the parent (the only place it can be stopped), removes the AttributeError, keeps anchor default behavior (href navigation, Ctrl/Cmd-click, middle-click) since no preventDefault, and sends zero websocket traffic per click. Addresses root cause (wrong layer for propagation control), not symptom."
  blind_spots: "Cannot click-test in prod from here; js_handler arg receives the native MouseEvent for DOM events per NiceGUI docs — verified signature but not live-rendered. Mitigation: add a static AST regression guard test forbidding Python-side .stop_propagation() in web/, matching the codebase's existing AST-guard convention."
next_action: Awaiting human verification on prod/local web app — click the shelfmark link and open_in_new button in the VS dialog (row must NOT expand/collapse, link must navigate, no AttributeError in logs), and the "View on PGP" icon in the version selector menu (must open PGP without selecting the PGP version). On confirmation: archive session, commit, append knowledge base entry.

## Resolution

root_cause: Three `.on('click', lambda e: e.stop_propagation())` registrations called a method that does not exist on NiceGUI's GenericEventArguments (verified on installed NiceGUI 3.8.0). Server-side propagation control is impossible by design — the DOM event has already propagated in the browser before the server hears about it. Net effect per click - AttributeError raised server-side AND the click propagated unshielded to the parent handler (VS dialog - `_on_row_click` toggled row expand/collapse while the link navigated; version_selector - `select_pgp` menu_item handler fired while the PGP external link opened). Pattern originated in version_selector.py (e0dd6482, 2026-02-06) and was copy-propagated into visual_similarity_dialog.py in v7.9.3 (3de30765, 2026-04-24).
fix: Replaced the server-side lambda with NiceGUI's client-side mechanism `.on('click', js_handler='(e) => e.stopPropagation()')` at all three sites — visual_similarity_dialog.py shelfmark link (~line 574) + open_in_new ui.link wrapper (~line 601), version_selector.py "View on PGP" link (~line 344). stopPropagation runs in the browser BEFORE the event reaches the parent; default anchor behavior preserved (href navigation, Ctrl/Cmd-click, middle-click — no preventDefault); zero per-click websocket traffic (no Python handler registered). Added static AST regression guard tests/test_no_server_side_stop_propagation.py (forbids Python-side .stop_propagation() anywhere in web/ + positive assertion the three js_handler shields exist), matching the codebase AST-guard convention.
verification: Self-verified — ruff clean on all 3 changed/new files; AST parse OK; pytest green - tests/test_no_server_side_stop_propagation.py (3) + tests/test_visual_similarity_dialog.py + tests/test_version_selector_pending.py (14 total) + tests/test_visual_similarity.py (14). Grep confirms zero remaining Python-side stop_propagation in web/ (only explanatory comments). Codex cross-AI review (gpt-5.5, _tmp/codex-debug-fixes-CRITIQUE-2026-06-12.md): REQUEST-CHANGES with 1 MEDIUM — a FOURTH unshielded link at version_selector.py:268 (PGP link inside the select_edition menu_item; no broken handler so the grep hunt missed it) — shield added same day + positive-assertion test extended to require >=2 js_handler shields in version_selector.py; 18 targeted tests green, ruff clean. Human verification: Hillel confirmed fixed 2026-06-12.
files_changed:
  - web/components/visual_similarity_dialog.py
  - web/components/version_selector.py
  - tests/test_no_server_side_stop_propagation.py (new)
