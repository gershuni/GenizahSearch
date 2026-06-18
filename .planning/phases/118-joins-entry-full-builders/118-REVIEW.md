---
phase: 118-joins-entry-full-builders
reviewed: 2026-06-18T00:00:00Z
depth: standard
files_reviewed: 11
files_reviewed_list:
  - web/components/joins_builder.py
  - web/components/joins_panel.py
  - web/components/known_joins_group.py
  - web/pages/browse.py
  - web/pages/joins_lab.py
  - web/pages/search_results.py
  - tests/test_builder_modifier_hoist.py
  - tests/test_joins_anc05_rls.py
  - tests/test_known_joins_group.py
  - tests/test_merge_globals_web.py
  - tests/test_other_side_page_contract.py
findings:
  critical: 2
  warning: 6
  info: 4
  total: 12
status: issues_found
---

# Phase 118: Code Review Report

**Reviewed:** 2026-06-18
**Depth:** standard
**Files Reviewed:** 11
**Status:** issues_found

## Summary

Reviewed the Phase 118 "joins entry + full builders" implementation: the new web
line-builder widget (`joins_builder.py`), the confirmed-only / community-merge
extension to `joins_panel.py`, the known-joins group renderer
(`known_joins_group.py`), the Joins Lab page wiring (`joins_lab.py`), and the
search-results / browse joins-icon entry points. The Phase 87 multitenant
invariant holds (zero raw `app.storage.user` in changed files; ANC-05 confirmed-only
filter + `:confirmed` cache-key isolation are correctly implemented and tested). The
Phase 98 NLI breaker convention is not violated (all image fetches go through
existing proxy/AnchorViewer paths; no new direct IIIF fetches were added).

However the adversarial pass surfaced **two correctness defects that crash or
mis-route on realistic user input**, several robustness/UX defects in the search
orchestration and re-anchor flows, and bilingual (`tr()`) coverage gaps in the new
builder widget. Details below.

## Critical Issues

### CR-01: `compose(side)` can raise an uncaught `ValueError` and crash the Run-Search handler

**File:** `web/pages/joins_lab.py:791`
**Issue:** `compose(side)` is called OUTSIDE any try/except (the search `try`
block does not begin until line 831). `compose()` raises `ValueError` when
`page_position='start'` and the first row is empty, or `page_position='end'` and
the last row is empty (`shared/joins_lab.py:722-732`). This is reachable with
ordinary user input:

1. User selects Text Position = "Start of text" (or "End of text").
2. User clears row 1's text but types content in row 2 (so `is_empty()` returns
   `False` and the empty-builder guard at `joins_builder.py:117` does NOT return
   `None` — a `SideQuery` is produced with an empty first `BuilderRow` and
   `page_position='start'`).
3. `compose(side)` at line 791 raises `ValueError("page_position 'start' requires
   a non-empty first row to anchor")`, which propagates uncaught out of the
   NiceGUI click handler — a silent broken search (no notify, no recovery), and an
   exception logged on the server.

**Fix:** Wrap the `compose()` call and surface a friendly notify instead of letting
it bubble:
```python
try:
    query_str, ro, page_position = compose(side)
except ValueError:
    ui.notify(
        tr('Text Position requires content on that line. '
           'Add a word to the first/last line or set Text Position to Anywhere.'),
        type='warning',
    )
    return
```

### CR-02: Community-sourced known joins get an empty `sys_id`, breaking the re-anchor pin

**File:** `web/components/joins_panel.py:282-318` (resolution no-op) →
`web/components/known_joins_group.py:129,175-191` → `web/pages/joins_lab.py:546-548,565`
**Issue:** In the community-puzzle merge, the `member_sys_id` resolution block is a
dead no-op: it calls `state.meta_mgr.get_meta_for_id(member_shelfmark)` (which the
inline comment itself admits expects a sys_id, NOT a shelfmark), and the body of
the `if _resolved ...` branch is just `pass`. `member_sys_id` therefore is always
`None`, so every community member is appended with `document_id: ''` (line 308).

Downstream, `render_known_joins_group` builds `shelfmark_to_sys` from those empty
`document_id`s, so `member_sys_id` is `''` for community members
(`known_joins_group.py:129`). The re-anchor pin button is rendered unconditionally
(`known_joins_group.py:186-191`) and `_make_reanchor` captures `member_sys_id=''`.
Clicking it calls `on_reanchor('', shelfmark)` →
`_load_known_joins._on_reanchor` → `load_anchor('')` (`joins_lab.py:548`).
`load_anchor` has no empty-`sys_id` guard: it sets `_anchor_state['sys_id']=''`,
builds an `AnchorViewer(sys_id='')`, and persists `write_anchor('')` — re-anchoring
to nothing and corrupting the stored anchor.

**Fix:** (a) Resolve the member sys_id properly (e.g. via
`get_service().search_by_shelfmark(member_shelfmark)` off-loop, or skip resolution
and let the row fall back to open-in-browse only); and (b) guard the re-anchor pin
so it is disabled/hidden when `member_sys_id` is falsy, and add an early return in
`load_anchor` for empty sys_id:
```python
# known_joins_group._render_member_row — only render the pin when a sys_id exists
if member_sys_id:
    ui.button(icon='push_pin', on_click=_make_reanchor()) ...
# joins_lab.load_anchor — defensive guard
async def load_anchor(sys_id, ...):
    if not sys_id:
        return
```
Also remove or fix the dead resolution block at `joins_panel.py:282-291`.

## Warnings

### WR-01: Search button is re-enabled while the cross-side (other-side) leg is still running

**File:** `web/pages/joins_lab.py:852-858` vs `878-933`
**Issue:** The `finally` block runs after the FIRST await (anchor search) but
BEFORE the cross-side block. It sets `_is_running['value'] = False` and
`search_btn.props(remove='loading disabled')`. When the other-side builder is
enabled, the cross-side leg (lines 906-918) then runs for up to
`_SEARCH_TIMEOUT_SECONDS` (120s) more, during which the button is fully enabled and
shows no loading state. A user can click Run Search again mid-cross-side, and the
loading affordance is wrong. The cross-side leg also has no `finally` to restore the
button if it raises before line 935.
**Fix:** Move the `_is_running` / button-restore out of the inner `finally` into a
single outer `finally` that wraps the whole function (both legs), or re-assert the
loading state at the top of the cross-side block and clear it in Step 9.

### WR-02: Re-anchoring via the known-joins pin leaves stale candidates and a stale summary bar

**File:** `web/pages/joins_lab.py:565-631` (`load_anchor`)
**Issue:** `load_anchor` deliberately does not reset builder state (D-16), but it
also never clears `candidates_container` nor restores the collapsed builder /
summary bar. After running a search (which collapses the builder to a summary bar
via `_collapse_builder` and renders candidates), if the user re-anchors to a
known-join member via the pin, the page shows the NEW anchor image alongside the
OLD anchor's candidate grid and the OLD summary bar — misleading and incorrect.
`_on_change_anchor` resets this state, but the re-anchor path does not.
**Fix:** In `load_anchor`, clear `candidates_container.clear()` and reset the
builder visibility (re-expand builder, hide summary bar) on each anchor swap, while
preserving the typed builder rows per D-16.

### WR-03: Text Position dropdown shows wrong-language labels (module-time `tr()`)

**File:** `web/components/joins_builder.py:131-137`
**Issue:** `_TEXT_POSITION_OPTIONS` calls `tr()` eagerly at module import time.
`web/translations.tr()` reads the process-global `_current_lang`, which is set
per-request by `web/main.py:861` (`set_language(resolved_lang)`). Because the dict
is frozen at first import (default `_current_lang='he'`), an English-language
visitor sees Hebrew labels in the Text Position `ui.select`, and the labels never
update when the language is switched. (Note: `_MODIFIER_KEYS` does this correctly
with deferred `lambda: tr(...)`.)
**Fix:** Build the options dict inside `create_joins_builder()` (per render) so
`tr()` runs at request time:
```python
text_position_options = {
    'anywhere': tr('Anywhere'),
    'start': tr('Start of text'),
    ...
}
```

### WR-04: Collapsed-builder summary bar is hardcoded English (bypasses `tr()`)

**File:** `web/components/joins_builder.py:182-197` (`_get_summary`)
**Issue:** `_get_summary()` builds the summary string with hardcoded English:
`mode.capitalize()`, `'{n} line'/'{n} lines'`, `'Text Position: ...'`, and an
English-only `tp_label_map`. None of it goes through `tr()`. For the default
Hebrew UI, the collapsed summary bar (shown after every search via
`_collapse_builder`, `joins_lab.py:808`) is entirely in English, violating the
bilingual requirement called out for this phase.
**Fix:** Route every literal through `tr()` and use the `tr()`-mapped Text Position
labels (or reuse the request-time options dict from WR-03). Pluralization should
also use `tr()` keys, e.g. `tr('{n} lines').format(n=n)` with a singular variant.

### WR-05: Gap-number input re-renders all rows on every keystroke, losing focus

**File:** `web/components/joins_builder.py:313-318` (`_on_gap_change`)
**Issue:** `_on_gap_change` fires on `update:model-value` (every change) and calls
`_render_rows(rows_container['el'])`, which clears and rebuilds the entire rows
section (including all `ui.input` term fields). This destroys and recreates the gap
input mid-edit just to recolor its border, causing focus loss and a janky typing
experience; any unsynced keystrokes in sibling term inputs that haven't fired their
own change event yet can also be dropped on the rebuild.
**Fix:** Update only the border color in place rather than re-rendering all rows.
Capture the gap element and set its style directly:
```python
def _on_gap_change(v, el=gap_input, i=idx):
    rows_state[i]['gap_to_next'] = int(v or 0)
    color = 'var(--border-focus)' if rows_state[i]['gap_to_next'] > 0 else 'var(--neutral-300)'
    el.style(f'width: 56px; border-color: {color};')
```

### WR-06: `create_joins_dialog` from the search-result joins icon omits `pgpid`

**File:** `web/pages/search_results.py:652-657`
**Issue:** `_open_joins_for_card` calls `create_joins_dialog(shelfmark=sm,
document_id=s, find_joins_url=url)` without passing `pgpid`. `fetch_connected_fragments`
will then resolve pgpid from `document_id` via an extra Supabase round-trip
(`get_document_for_fragment`, `joins_panel.py:130-134`). The search card already
has access to PGP metadata in many cases, and the joins panel/button elsewhere in
the codebase pass `pgpid` to avoid this. Functionally correct but a redundant
network call on every joins-dialog open from the results list.
**Fix:** Thread the result's pgpid through (where available on the search-result
dict) and pass it to `create_joins_dialog(pgpid=...)`.

## Info

### IN-01: Dead resolution block in community merge should be removed

**File:** `web/components/joins_panel.py:282-291`
**Issue:** Independent of CR-02, the `member_sys_id` block is dead code — it performs
a wrong-argument `get_meta_for_id(shelfmark)` lookup (the method takes a sys_id),
ignores the result (`pass`), and leaves `member_sys_id=None`. It is pure overhead
inside a per-member loop and misleads readers.
**Fix:** Delete the block, or replace it with a real shelfmark→sys_id resolution
(see CR-02 fix).

### IN-02: No test covers combined per-row modifiers

**File:** `tests/test_builder_modifier_hoist.py`
**Issue:** `_apply_modifiers_to_term` mirrors the desktop ordering plene → prefix →
suffix → wildcard_prefix → wildcard_suffix (verified against
`desktop/join_workbench.py:1307-1334`), but the tests only exercise one modifier at
a time. The interaction order (e.g. `plene`+`prefix`+`suffix` → `#%שלום#`,
`wildcard_prefix`+`wildcard_suffix` on a non-group → `*שלום*`) is the most
regression-prone part and is untested.
**Fix:** Add a combined-modifier test, e.g.
`assert fn('שלום', {'plene': True, 'prefix': True, 'suffix': True}) == '#%שלום#'`.

### IN-03: `_apply_modifiers_to_term` slash-group detection is order-sensitive on negation+plene

**File:** `web/components/joins_builder.py:69-84`
**Issue:** For a slash-group with multiple modifiers, the web wraps once as
`(a/b)` then prepends/appends. This matches desktop. But note the design limitation
(worth a code comment, not a bug): a user who types a literal term containing `/`
that is NOT meant as an OR-group (rare in Hebrew shelfmark search but possible) will
have it silently wrapped in parens. There is no escape mechanism. Document the
limitation near the `is_group` computation.
**Fix:** Add a comment documenting that any `/` in the term is treated as an
OR-group boundary (no escaping), matching the desktop multi-box semantics.

### IN-04: `asyncio.get_event_loop().call_later` is deprecated (pre-existing, adjacent)

**File:** `web/components/joins_panel.py:465`
**Issue:** `asyncio.get_event_loop()` emits a DeprecationWarning (and can raise) on
Python 3.10+ when there is no running loop. This is pre-existing code in
`create_joins_button` (not introduced this phase), but it is on the joins entry
path this phase newly relies on. The newer `search_results.py`/`joins_lab.py` code
correctly uses `ui.timer(..., once=True)`.
**Fix:** Migrate to `ui.timer(0.1, _safe_load_count, once=True)` for consistency
with the rest of the phase's deferral pattern.

---

_Reviewed: 2026-06-18_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
