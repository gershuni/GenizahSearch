---
phase: 117-vertical-spine
reviewed: 2026-06-17T00:00:00Z
depth: standard
files_reviewed: 10
files_reviewed_list:
  - web/joins_executor.py
  - web/joins_lab_storage.py
  - web/components/image_resolution.py
  - web/components/typography.py
  - web/components/candidate_grid.py
  - web/components/anchor_viewer.py
  - web/pages/joins_lab.py
  - web/pages/browse.py
  - web/pages/browse_enrichment.py
  - web/main.py
findings:
  critical: 0
  warning: 4
  info: 4
  total: 8
status: issues_found
---

# Phase 117: Code Review Report

**Reviewed:** 2026-06-17
**Depth:** standard
**Files Reviewed:** 10
**Status:** issues_found

## Summary

Reviewed the Phase 117 "Web Joins Lab" vertical-spine implementation: the search
adapter (`joins_executor.py`), per-user storage chokepoint (`joins_lab_storage.py`),
extracted image/typography components, the candidate grid, the AnchorViewer, the
page wiring (`joins_lab.py`), the route (`main.py`), and the two refactored browse
modules.

The four project-critical invariants HOLD and are well-defended:

- **Multitenant (Phase 87):** zero raw `app.storage.user` access in any reviewed
  file; all per-user state funnels through `web/joins_lab_storage.py` → `safe_user_*`.
- **Off-event-loop:** `execute_search` appears ONLY inside the synchronous
  `run_search_core` closure dispatched via `run.io_bound`; the shelfmark lookup and
  AnchorViewer resolution likewise run off-loop. Latest-wins (generation counter +
  task cancel + cooperative `InterruptedError` progress_cb) + `asyncio.wait_for`
  timeout are all present and correct.
- **SSRF / NLI breaker:** no direct `iiif.nli.org.il` URL is constructed anywhere;
  every NLI image path is a `/api/<provider>_image*` proxy. Oxford-direct Bodleian
  is the documented intentional exception.
- **Adapter wraps `state.searcher` directly**, not `/api/search`.

The browse.py / browse_enrichment.py extractions are essentially behavior-preserving.
No Critical defects found. The findings below are correctness/robustness/quality
issues, the most material being a non-functional accessibility touch-target and a
non-functional responsive grid breakpoint (both silent CSS/props mistakes).

## Warnings

### WR-01: 44px touch-target classes are placed in `.props()` instead of `.classes()` — accessibility sizing never applies

**File:** `web/components/anchor_viewer.py:360, 369, 378, 387, 394`
**Issue:** All five control buttons emit `min-h-[44px] min-w-[44px]` (Tailwind utility
*classes*) inside `.props(...)`, mixed with Quasar props (`flat round dense`) and
`aria-label`. NiceGUI's `.props()` maps to Quasar component props/attributes — it does
NOT add CSS classes. So `min-h-[44px]`/`min-w-[44px]` are emitted as bogus boolean
attributes on the `<q-btn>` and never produce the intended 44px minimum touch target.
Every other site in the codebase that uses `min-w-[...]` does so via `.classes(...)`
(e.g. `web/components/project_tree.py:429`), and `/browse` enforces the same 44px target
via real CSS (`browse.py:368-369/402-403/445-446`). The explicit `44px` value indicates
a deliberate WCAG 2.5.5 / mobile-tap-target goal that is currently unmet.
**Fix:**
```python
self._prev_btn = (
    ui.button(icon="chevron_left", on_click=self._on_prev_folio)
    .props(f'flat round dense aria-label="{tr("Previous folio")}"')
    .classes("text-white min-h-[44px] min-w-[44px]")
)
```
Apply the same split (move the two `min-*-[44px]` tokens out of `.props` into
`.classes`) to all five buttons.

### WR-02: `@media` rule inside an inline `style()` is invalid CSS — responsive single-column collapse never works

**File:** `web/components/candidate_grid.py:250-251`
**Issue:** The candidate grid sets `.style("@media (max-width:639px) { grid-template-columns: 1fr; }")`.
`.style()` writes the element's inline `style=""` attribute, and `@media` at-rules are
not valid inside an inline style attribute — browsers ignore the entire declaration.
The documented "single column on narrow (<640px)" behavior therefore does not happen;
the grid stays 2-column on phones, which on a 380px-wide anchor pane layout will produce
cramped/overflowing candidate cards. The class docstring (line 249) advertises this
responsiveness as delivered.
**Fix:** Move the breakpoint into a real stylesheet rule (e.g. inject once via
`ui.add_head_html` with a class selector) or use Tailwind responsive grid classes:
```python
with ui.element('div').classes(
    "w-full grid gap-3 grid-cols-1 sm:grid-cols-2"
):
    for cand in candidates:
        _create_candidate_card(cand, on_browse_click=on_browse_click)
```

### WR-03: AnchorViewer folio navigation has no latest-wins / re-entrancy guard — rapid prev/next can render a stale folio

**File:** `web/components/anchor_viewer.py:410-470, 514-518`
**Issue:** `update_content()` runs `await run.io_bound(_resolve)` and then mutates shared
viewer state (`_p_num`, page label, image container, transcription) with the result.
Unlike `joins_lab.execute_joins_search` (which has a generation counter + task cancel),
folio navigation has NO generation guard and NO button debounce. If a user clicks Next
twice quickly (or Next then Prev), two `update_content` coroutines run concurrently; the
one that finishes LAST wins the UI regardless of which the user intended, and `_p_num`
can end up inconsistent with the rendered image/transcription. The search path was
specifically hardened for this exact race (HIGH-3); the viewer's own nav was not.
**Fix:** Add a per-instance generation counter mirroring the search path:
```python
self._nav_gen = 0
...
async def update_content(self, p_num=None, direction=0):
    self._nav_gen += 1
    my_gen = self._nav_gen
    self._show_loading()
    ...
    result = await run.io_bound(_resolve)
    if my_gen != self._nav_gen:
        return  # superseded by a newer nav click
    ...
```
(Disabling the prev/next buttons while a nav is in flight is a reasonable alternative.)

### WR-04: Production navigation link interpolates `sys_id` into a JS string literal (`js_handler`) without escaping

**File:** `web/components/candidate_grid.py:205`
**Issue:** When `on_browse_click` is provided, the browse link uses
`js_handler=f"() => {{ window.location.href='{browse_url}'; }}"`, embedding
`build_browse_url(cand)` (which contains `cand.sys_id`) unescaped inside a single-quoted
JS string. A `sys_id`/`page` containing `'` or `</script>`-style content would break out
of the string literal — a JS-injection vector. The values originate from the corpus index
(normally `99…`-digit Alma numbers), so practical exploitability is low, and the default
production path (`create_joins_lab_page` calls `create_candidate_grid(candidates)` with no
`on_browse_click`, using the safe `ui.link(..., browse_url)` href form). It is still an
unescaped-input-into-executable-context pattern that should not be relied on as "test only."
**Fix:** Build the URL via NiceGUI navigation rather than string-interpolated JS, e.g.
`.on("click", lambda c=cand: ui.navigate.to(build_browse_url(c)))`, or at minimum
`json.dumps(browse_url)` to produce a safely-quoted JS literal.

## Info

### IN-01: `resolve_external_images` resets `cached` to `{}` on enrich failure — subtle behavior drift from the extracted original

**File:** `web/components/image_resolution.py:271-273`
**Issue:** The pre-extraction code (browse_enrichment.py, original) left `cached` at its
pre-enrich value when `enrich_metadata` raised; the extracted helper sets `cached = {}` in
the `except`. This branch is only reached when `not cached.get('images_ext')`, so the
difference can only affect `external_provider` / `cambridge_alignment` for an entry that
has those set but no `images_ext` — an unlikely state. Effectively inconsequential but a
documented deviation from "extraction preserves behavior."
**Fix:** Drop the `cached = {}` line in the `except` to mirror the original exactly, or add
a comment noting the intentional reset.

### IN-02: Empty-candidates message is duplicated between the page and the grid

**File:** `web/pages/joins_lab.py:587-590` and `web/components/candidate_grid.py:238-242`
**Issue:** `execute_joins_search` renders its own "No candidates found. Try different lines."
label when `candidates` is empty, while `create_candidate_grid` already renders an empty
state ("No candidates found. Try different lines or broader terms.") for an empty list. The
page never calls the grid in the empty case, so the grid's empty branch is dead on this
path, and the two strings diverge. Minor maintainability/i18n smell.
**Fix:** Call `create_candidate_grid(candidates)` unconditionally and delete the page-level
empty branch, or remove the unused empty branch from one of the two.

### IN-03: `anchor_matched` is unpacked but never used in the spine

**File:** `web/pages/joins_lab.py:580`
**Issue:** `candidates, anchor_matched = dedup_candidates(...)` discards `anchor_matched`.
This is expected (self-match UI is Phase 119 scope) but currently reads as an unused
variable.
**Fix:** Either prefix as `_anchor_matched` to signal intentional non-use, or add a comment
that it is reserved for Phase 119.

### IN-04: `ui.html(img_html, sanitize=False)` interpolates the resolved URL into an HTML attribute without escaping

**File:** `web/components/anchor_viewer.py:314-335, 489`
**Issue:** `_build_img_html` builds `<img src="{img_url}" ...>` and renders it with
`sanitize=False`. `img_url` is composed from `sys_id` (Alma number), integer `page`, and —
for Oxford — a regex-parsed Bodleian path (single letter + digits only). None of these can
realistically carry a `"` to break out of the `src` attribute, and this mirrors the
pre-existing `/browse` img-tag pattern. Noted as defense-in-depth only.
**Fix:** Prefer building the `<img>` via a NiceGUI element (`ui.image(img_url)`) so the
attribute is escaped by the framework, or HTML-escape `img_url` before interpolation.

---

_Reviewed: 2026-06-17_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
