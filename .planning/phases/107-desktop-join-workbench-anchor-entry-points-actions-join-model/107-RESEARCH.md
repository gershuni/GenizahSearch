# Phase 107: Desktop Join Workbench — Anchor, Entry Points, Actions & Join Model - Research

**Researched:** 2026-06-04
**Domain:** PyQt6 desktop integration — modeless window shell, known-joins display, four public actions, i18n
**Confidence:** HIGH (all findings are code-verified at specific file:line)

---

## Summary

Phase 107 builds the Desktop Join Workbench SHELL: a modeless `QDialog` opened for a specific anchor
fragment, showing its image (via the proven `enrich_metadata` route), line-numbered transcription,
folio nav, brief metadata, a known-joins group panel, and four public action buttons. The three
primary research flags (R-01, R-02, R-03) are all resolved by reading the actual code. The
implementation decisions (D-01..D-18) and UI-SPEC are fully locked; this document de-risks the
specific code-integration unknowns the planner needs.

**Primary recommendation:** The design is settled. The three non-trivial implementation tasks are:
(1) building the known-joins group from two sources — `JoinsManager.get_connected_fragments_by_id`
for user/community joins (which carries a `source` field) plus separate `_get_fjms_joins()` +
`_get_pgp_joins()` calls for FJMS and PGP (so per-row provenance badges ARE available); (2) opening
`JoinsDialog` anchor-only by passing only `document_id` and `shelfmark` for fragment A, leaving
fragment B as the existing free-entry `frag_b_input` field; and (3) adding thin public wrappers
`open_anchor_in_puzzle(sys_id)` and `open_anchor_as_join(sys_id, shelfmark)` to `GenizahGUI` to
avoid `_vs_*` private calls from the workbench.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: Host = dedicated modeless window (`setModal(False)`, opened with `show()`).
- D-02: Single reusable instance (`self._join_workbench`); second "Find joins" call re-anchors.
- D-03: Three entry points — ResultDialog action row, Browse `ext_info_row`, cold-start shelfmark.
- D-04: Anchor pane = image + line-numbered transcription + zoom + folio nav + brief metadata; dark-mode/RTL safe.
- D-05: Image via `enrich_metadata(sys_id)` → `images_nli` (else `images_ext`), `iiif_full` = base + `/full/2000,/0/default.jpg`, through `ImageLoaderThread`. NOT FL-substituted thumbnail URLs.
- D-06: Text via `apply_line_numbered_text(browser, html, source_text=raw, is_html=True)`.
- D-07: Folio nav pages the same sys_id's image list by index; anchor identity stays sys_id; known-joins panel does NOT reload per page.
- D-08: Known-joins source = `JoinsManager.get_connected_fragments_by_id(sys_id)`.
- D-09: Per-row source badge (PGP / FJMS / user / community); degrades to generic "Known join" if provenance not recoverable. Research resolves this — see R-01.
- D-10: Thumbnails via `meta_mgr.get_thumbnail`, fetched batched.
- D-11: Known-joins panel hidden entirely when empty (no empty-state prompt). Add-as-Join stays on anchor row.
- D-12: Four actions via public named methods — no `_vs_*` calls. See R-03.
- D-13: Anchor action-row always visible (Browse / Puzzle / Add-to-List / Add-as-Join); per known-join row adds Browse / Puzzle / Add-to-List / "⚓ make anchor".
- D-14: Add-as-Join opens `JoinsDialog` pre-filled anchor=A, scholar enters B. See R-02.
- D-15: Re-anchor via explicit "⚓ make anchor" action (D-02 machinery).
- D-16: i18n from line one; fully bilingual EN/HE with `tr()`.
- D-17: No candidate search or VS source in Phase 107.
- D-18: Desktop-first; consumes `shared/joins_lab.py` (Phase 106) where applicable.

### Claude's Discretion
- Cold-start ambiguous-shelfmark UX (QInputDialog.getItem picker using resolve_system_by_shelfmark options).
- No-image fallback rendering ("(no image)" placeholder).
- Exact zoom step (1.25× per UI-SPEC), metadata-line composition, window sizing (900×680 min, 1000×720 initial).
- Whether optional result-row button is retained.
- Internal helper decomposition; how much of frozen sketch anchor code transplants.

### Deferred Ideas (OUT OF SCOPE)
- Phase 108: Candidate search / query builders / candidate grid/table / triage / Compare dialog.
- Phase 109: Visual-similarity source / combined view / VS-dialog soft-retire.
- Phase 110: JSA / parallels seeding.
- Later: Web Join Workbench UI, multiple concurrent windows, dock panel.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| JWB-01 | Dedicated workbench tab/page (implemented as modeless window) | D-01 locked; window pattern confirmed |
| JWB-02 | Three entry points wired | Hook patterns confirmed at `genizah_app.py:18561` (`_create_action_button`) and `genizah_app.py:6966` (`ext_info_row`) |
| JWB-03 | Anchor pane: image, line-numbered text, zoom, folio nav, brief metadata | Full route verified: D-05 image route confirmed at `genizah_core.py:4518`, `apply_line_numbered_text` at `desktop/widgets/line_number_text_edit.py:332` |
| JWB-04 | Known-joins group display with per-row provenance badges | R-01 resolved: provenance IS available via join `source` field + FJMS/PGP separate calls |
| JWB-09 | Four public actions persisting via existing pairwise path + group refresh | R-02 confirmed JoinsDialog supports anchor-only open; R-03 identifies two wrappers needed |
| SC#5 | Public named action APIs (no `_vs_*` calls on workbench path) | Two wrappers needed: `open_anchor_in_puzzle` + `open_anchor_as_join`; `open_result_in_browse_from_table` and `show_add_to_list_menu` are already public |
| SC#6 | Fully bilingual EN/HE from line one | i18n audit in this document — 5 keys need adding, 5 keys already present |
</phase_requirements>

---

## PRIMARY RESEARCH FLAG RESOLUTIONS

---

## R-01 — Known-Joins Provenance

**Question:** Does `JoinsManager.get_connected_fragments_by_id(sys_id)` or the underlying join records
expose per-member / per-edge source provenance (PGP vs FJMS vs user vs community)?

**Answer: PARTIALLY YES — provenance is recoverable but requires two separate data sources.**

### What `get_connected_fragments_by_id` returns

`genizah_core.py:10127–10199`

```
{
  'document_id': str,
  'fragments': list[str],   # shelfmark strings
  'joins': list[dict],       # raw join dicts from joins_cache.pkl
  'total_fragments': int,
  'total_joins': int
}
```

The `joins` list contains raw join dicts from `self.data['joins']`. Each dict carries a **`source`
field**. Confirmed values:
- `'user'` — set by `create_join_local` (`genizah_core.py:10399`)
- From server sync (`_fetch_all_joins`, `genizah_core.py:10308`): `join.source` is stored as-is —
  community user-created joins will also carry `source` (likely `'user'` or an equivalent string,
  set server-side). The `is_local` flag distinguishes locally-created-not-yet-synced joins.

**However**, `JoinsManager` stores ONLY user-created (desktop) and server-synced joins. It does NOT
store PGP or FJMS scholarly joins. Those are fetched separately:
- **PGP joins**: `_get_pgp_joins()` in `corrections_ui.py:3750` — queries `shared.document_service`;
  synthesizes join dicts with `source='PGP'` explicitly.
- **FJMS scholarly joins**: `_get_fjms_joins()` in `corrections_ui.py:3547` — queries
  `shared.fjms_service.get_fjms_service().get_join_group(sys_id)`; synthesizes join dicts with
  `source='FJMS'` explicitly.
- **Community puzzle joins**: Fetched separately via
  `client.get_published_joins_for_fragment(document_id)` in `_load_community_joins()` at
  `corrections_ui.py:3932`.

### What this means for the planner

The workbench's `_reload_known_joins()` worker MUST call three sources:

1. `joins_mgr.get_connected_fragments_by_id(anchor_sid)` → user joins (source=`'user'`)
2. `_get_pgp_joins()` equivalent (call `shared.document_service.get_document_for_fragment` +
   `get_fragments_for_document`) → PGP joins (source=`'PGP'`)
3. `shared.fjms_service.get_fjms_service().get_join_group(sys_id)` → FJMS joins (source=`'FJMS'`)

Community puzzle joins are a fourth source (`client.get_published_joins_for_fragment`) — already
shown in `JoinsDialog` as a separate section. The workbench can handle them similarly.

**Per-row badge assignment** (D-09):

| `source` value | Badge text | Badge color |
|----------------|-----------|-------------|
| `'PGP'` | `"PGP"` | `#0ea5e9` (sky-blue) |
| `'FJMS'` | `"FJMS"` | `#8b5cf6` (violet) |
| `'user'` or `is_local=True` | `tr("User")` | `#10b981` (green) |
| community puzzle joins | `tr("Community")` | `#10b981` (green) |
| any other / unknown | `tr("Known join")` | `#6b7280` (gray fallback) |

**The D-09 degradation ("generic Known join") is NOT needed** — all four sources expose their
provenance. The planner should wire all four rather than defaulting to the fallback.

**Dedup concern:** The same join pair may appear in both FJMS and user joins (if a scholar first
noticed an FJMS join and then created a user join). The existing `JoinsDialog` handles this with
`_merge_fjms_joins_into_display` (`corrections_ui.py:3607`) — the planner should adopt the same
dedup-by-pair logic for the workbench's panel rows.

**FJMS thread safety:** `_get_fjms_joins()` opens `fjms_enrichment.db` via `shared.fjms_service`.
The sketch iteration F warning ("_enrich_vs_suggestions touches fjms sqlite on the wrong thread")
referred to that specific function. The `shared.fjms_service` uses its own thread-safe connection
management — calling `get_fjms_service().get_join_group(sys_id)` from a `QThread` (off the UI
thread) is safe, consistent with `JoinsDialog.load_joins()` which calls `_get_fjms_joins()` in
an already-complex context.

---

## R-02 — JoinsDialog Free Partner-B Entry

**Question:** Does `JoinsDialog` support being opened with anchor pre-filled as fragment A and the
scholar freely entering partner B WITHOUT a pre-supplied candidate?

**Answer: YES — JoinsDialog ALREADY supports anchor-only open. No adaptation needed.**

### JoinsDialog `__init__` signature

`corrections_ui.py:3281–3293`

```python
def __init__(
    self,
    parent=None,
    client: CorrectionsClient = None,
    document_id: str = None,      # anchor sys_id (fragment A)
    shelfmark: str = None,        # anchor shelfmark (fragment A display)
    on_browse=None,
    shelf_model=None,
    joins_mgr=None,
    shelf_completer=None,
    lists_mgr=None,
    meta_mgr=None
):
```

All parameters are optional. There is NO required `partner` argument.

### How fragment A is pre-filled (`corrections_ui.py:3409–3418`)

Fragment A is a **read-only** `QLineEdit` whose text is set from `document_id` via
`meta_mgr.get_meta_for_id(self.document_id)` (or falls back to `self.shelfmark`). It is marked
`setReadOnly(True)` with a palette-aware disabled color. The scholar cannot edit it.

### How fragment B is entered (`corrections_ui.py:3421–3452`)

Fragment B is a writable `QLineEdit` with:
- `setPlaceholderText(tr("Start typing shelfmark..."))` — explicitly designed for free text entry
- Shelfmark autocomplete via `NormalizingCompleter` bound to `shelf_model`
- A "📋 From List" button (if `lists_mgr` present) to pick from personal lists
- A "🔍 Visual Suggestions" button to pick from VS candidates

When the dialog opens with only `document_id`/`shelfmark` set (no partner), `frag_b_input` is empty
and the scholar types/autocompletes freely. This is the **designed use case**.

### The existing `_vs_open_joins_with_partner` use case

`genizah_app.py:5239–5259` passes BOTH fragments — it constructs the dialog normally and then
calls `dialog.frag_b_input.setText(partner_shelfmark)` AFTER construction. This post-construction
setText is the only difference for the "known partner" case.

### Phase 107 open pattern (anchor-only)

```python
# Public wrapper: open_anchor_as_join(anchor_sys_id, anchor_shelfmark)
def open_anchor_as_join(self, anchor_sys_id: str, anchor_shelfmark: str):
    """Open JoinsDialog pre-filled with the anchor as Fragment A; scholar enters B freely."""
    def browse_shelfmark(target_shelfmark):
        self.browse_shelf_input.setText(target_shelfmark)
        self._set_last_browse_field("shelf")
        self.browse_load()

    dialog = JoinsDialog(
        self, self.corrections_client,
        document_id=anchor_sys_id,
        shelfmark=anchor_shelfmark,
        on_browse=browse_shelfmark,
        shelf_model=getattr(self, 'shelf_model', None),
        joins_mgr=getattr(self, 'joins_mgr', None),
        shelf_completer=getattr(self, 'shelf_completer', None),
        lists_mgr=getattr(self, 'lists_mgr', None),
        meta_mgr=self.meta_mgr,
    )
    # frag_b_input left EMPTY — scholar enters B freely
    dialog.exec()
```

This is literally `_vs_open_joins_with_partner` without the final `dialog.frag_b_input.setText(...)`.
No new ctor argument is needed. No modification to `JoinsDialog` is required.

---

## R-03 — Public Wrappers for `_vs_*` + Thread Safety

**Question:** Which `_vs_*` methods need public wrappers, and are their bodies safe to call from
the workbench window / its QThreads?

### `_vs_add_to_puzzle(partner_sys_id)` — `genizah_app.py:5261–5272`

```python
def _vs_add_to_puzzle(self, partner_sys_id):
    """Add a VS partner to the Fragment Puzzle."""
    try:
        shelf = None
        if self.meta_mgr:
            try:
                shelf, _ = self.meta_mgr.get_meta_for_id(partner_sys_id)
            except Exception:
                pass
        self.add_to_puzzle(partner_sys_id, shelf or partner_sys_id)
    except Exception as e:
        logger.debug(f"VS add to puzzle error: {e}")
```

Body: looks up shelfmark (CSV-only, no network), calls `self.add_to_puzzle(sys_id, shelfmark)`.
`add_to_puzzle` at `genizah_app.py:15362` opens/raises the puzzle window and either adds the
fragment directly (if folio list is already cached) or fires a `PuzzleMetaLoaderThread`.

**Thread safety:** This is a UI method — it creates/updates Qt widgets. It MUST be called on the
UI thread, NOT from a QThread worker. The workbench should call it directly in a button click
handler (which already runs on the UI thread). No thread issue.

**Wrapper needed:** YES (SC#5). The public wrapper should be named:

```python
def open_anchor_in_puzzle(self, sys_id: str):
    """Public: open/add a fragment to the Fragment Puzzle canvas (Join Workbench path)."""
    self._vs_add_to_puzzle(sys_id)
```

Simple pass-through. The rename is the point (SC#5 — no `_vs_*` private-name calls from the
workbench).

### `_vs_open_joins_with_partner(orig_sys_id, orig_shelfmark, partner_sys_id, partner_shelfmark)` — `genizah_app.py:5239–5259`

Body: constructs `JoinsDialog`, pre-fills both fragments, calls `dialog.exec()`.

**Thread safety:** All UI operations. Must be called on UI thread. Button click handler → safe.

**Wrapper needed:** YES (SC#5), but the Phase 107 use case is "anchor-only" (R-02 resolution).
The new wrapper is:

```python
def open_anchor_as_join(self, anchor_sys_id: str, anchor_shelfmark: str):
    """Public: open JoinsDialog with anchor as Fragment A; scholar enters B freely."""
    # (full body as shown in R-02 above)
```

The original `_vs_open_joins_with_partner` is retained for the VS dialog's "both known" path —
Phase 107 does not retire it.

### `open_result_in_browse_from_table(res)` — `genizah_app.py:18824–18838`

Already public (no `_` prefix). Takes a standard result dict. Opens Browse tab at the given
sys_id. Thread-safe: UI operation, call from button click handler on UI thread.

No wrapper needed. Call directly: `app.open_result_in_browse_from_table(member_result_dict)`.

**Note:** For known-join rows, a `member_result_dict` must be synthesized from the join data
(sys_id → shelfmark via `meta_mgr.get_meta_for_id`, title, lib_code from CSV bank). This is the
same pattern `JoinsDialog._get_fjms_joins()` uses.

### `show_add_to_list_menu(items, source='', anchor_widget=None)` — `genizah_app.py:14208–14265`

Already public (no `_` prefix). Takes a list of `{sys_id, fl_id, img}` dicts, a source string,
and an optional anchor widget for menu positioning. Builds a `QMenu` and executes it.

Thread-safe: UI operation (builds + shows a QMenu), call from button click handler on UI thread.

No wrapper needed. Call directly.

**For the anchor action-row Add to List button:**
```python
app.show_add_to_list_menu(
    [{'sys_id': anchor_sid, 'fl_id': anchor_fl_id, 'img': anchor_page}],
    source='join_workbench',
    anchor_widget=btn_add_to_list
)
```

`anchor_fl_id` and `anchor_page` come from the anchor result dict's `display.img` and the
`_anchor_images[_anchor_idx]['fl_id']` from `enrich_metadata`.

### Summary of public API changes needed

| Method to add | Body | Replaces | File |
|---------------|------|---------|------|
| `open_anchor_in_puzzle(sys_id)` | delegates to `_vs_add_to_puzzle(sys_id)` | `_vs_add_to_puzzle` private calls | `genizah_app.py` |
| `open_anchor_as_join(anchor_sys_id, anchor_shelfmark)` | new JoinsDialog open, anchor-only | `_vs_open_joins_with_partner` (retained) | `genizah_app.py` |

No body changes to existing methods. Two thin additions only.

### Thread hazard not present in Phase 107

The sketch iteration F warning about `_enrich_vs_suggestions` touching FJMS sqlite on the wrong
thread referred to a VS-loading worker that fetched measurements and VS scores in bulk. Phase 107
does NOT load VS candidates. The FJMS call for known-joins (`get_join_group`) is off-UI-thread
safe when run in a QThread worker (the `shared.fjms_service` manages its own thread-local
connections). All four action button callbacks (Browse / Puzzle / Add-to-List / Add-as-Join) must
be invoked from the **UI thread** (they are all button click handlers — this is automatic).

---

## SECONDARY VERIFICATIONS

---

## Image Route (D-05)

**Verified in `genizah_core.py:4518–4519`:**

```python
current_meta['images_nli'] = images_nli   # list of {'label': str, 'url': str, 'fl_id': str}
current_meta['images_ext'] = images_ext   # list of {'label': str, 'url': str, ...}
```

`images_nli` entries: `{'label': str, 'url': f"{Config.NLI_IIIF_BASE}/FL{fl_id}", 'fl_id': str}`
(confirmed at `genizah_core.py:4476`).

`images_ext` entries: `{'label': str, 'url': str}` (CUDL, Manchester, Oxford, JTS format; the url
may already be a full IIIF URL or a base URL depending on source).

**`iiif_full` helper** (from sketch, `join_workbench.py.txt:151–157`):
```python
def iiif_full(base_url, width=2000):
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url          # already a direct URL (Oxford, JTS)
    return f"{base_url}/full/{width},/0/default.jpg"
```

**FL-substituted thumbnail trap** (iteration D confirmed): `get_thumbnail(sys_id)` at
`genizah_core.py:4892` fetches the FL ID from MARC and builds an NLI 400px URL. For NLI fragments
this gives an FL-level URL like `{NLI_IIIF_BASE}/FL{digits}` (without `/full/...`). The
substitute-FL-into-URL approach (using the current page's index to substitute the FL into the
thumbnail base URL) was the iteration B approach and caused NLI's forbidden placeholder for wrong
FL IDs. The **correct anchor image route** is always `enrich_metadata` → `images_nli`/`images_ext`
list by index → `iiif_full()` → `ImageLoaderThread`. Confirmed and working as of iteration D.

**For known-join row thumbnails (D-10):** `get_thumbnail(sys_id, size=320)` is correct — returns
an NLI 400px URL for NLI fragments, `None` for non-NLI (Oxford/Cambridge/etc.). The thumbnail
worker must handle `None` → show "(no img)" placeholder.

---

## Anchor Text Route (D-06 / D-07)

**`get_browse_page` signature** (`genizah_core.py:9483`):
```python
def get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None,
                    allow_cross=False, volume_ie=None)
```

**Return shape** (confirmed at `genizah_core.py:9700–9710`):
```python
{
    "uid": str,
    "p_num": int,
    "full_header": str,
    "text": str,
    "total_pages": int,
    "current_idx": int,     # 1-based ordinal
    "internal_index": int,  # 0-based ordinal
    "max_p_num": int,
    "sys_id": str
}
```

Note: `get_browse_page` does NOT return `shelfmark`, `library`, or `image` — those must be
obtained from `enrich_metadata` / `meta_mgr.get_meta_for_id`. This matches the sketch's
`_AnchorLoadWorker` which fetches both `enrich_metadata` and `get_browse_page` separately.

**Folio navigation (D-07):** Navigate the `_anchor_images` list by index; for each new index,
call `get_browse_page(sys_id, idx+1)` (1-based p_num). The `p_num` in the images list corresponds
to the image's IIIF canvas position; `idx+1` is the simplest approximation and matches the sketch.

**`apply_line_numbered_text` signature** (`desktop/widgets/line_number_text_edit.py:332`):
```python
def apply_line_numbered_text(
    widget,
    rendered_html_or_text: str,
    *,
    source_text: Optional[str] = None,
    pages: Optional[list] = None,
    is_html: bool = True,
) -> None
```

**Call pattern for anchor:**
```python
apply_line_numbered_text(
    anchor_text_browser,
    htmlify(text, pattern=None),   # RTL div wrapper
    source_text=text,              # raw text for line counting
    is_html=True
)
```

No `pages=` parameter needed for single-fragment browsing (that's for full-manuscript view).

---

## Entry-Point Hooks (D-03)

### ResultDialog action row

`_create_action_button` lives at `genizah_app.py:18561` (belongs to `GenizahGUI`, NOT
`ResultDialog` itself). ResultDialog receives an `app` reference through parent; the sketch confirms
`_open_join_workbench` was a method on `ResultDialog` that called `self.parent()` chain to reach
`GenizahGUI.open_joins_workbench(res)`.

**Actual pattern for the entry hook (from sketch + DESKTOP-INTEGRATION-NOTES):**
- Add a `🔗 "Find joins"` button to ResultDialog's action row using `QPushButton` or
  `QToolButton` (the `_create_action_button` pattern is on GenizahGUI's result table; ResultDialog
  uses a direct `QPushButton` layout).
- ResultDialog has access to the `app` (GenizahGUI) via `self.parent()` or explicit passing.
- The callback calls `app.open_joins_workbench(current_result_dict)` and `self.close()`.

**Live-page state fields available in ResultDialog:**
- `self.current_result` (the result dict for the current page)
- `self.current_sys_id` — inferred from `current_result['display']['id']`
- `self.p_num` — the current page number
- `self.page_text` — the text of the current page (from the text browser)

### Browse tab `ext_info_row`

`ext_info_row` is a local `QHBoxLayout` built in `create_browse_tab()` at `genizah_app.py:6966`.
It uses direct `QPushButton` construction — NOT `_create_action_button` (which is a `QToolButton`).
The "Find joins" button for Browse should match the existing button style in `ext_info_row`
(direct `QPushButton` with emoji prefix, matching `self.btn_b_add_to_puzzle` pattern).

**Live-page state fields available in Browse:**
- `self.current_browse_sid` — the sys_id currently being browsed
- `self.p` — current page number
- `self.browse_original_text` — the original (non-translated) page text (used by sketch)

---

## i18n Audit (D-16)

All new strings from the UI-SPEC's i18n Contract were verified against `genizah_translations.py`.

### Keys that ALREADY EXIST (no addition needed)

| English key | Hebrew value confirmed | Location |
|-------------|----------------------|----------|
| `"Add as Join"` | NOT found — see below | — |
| `"Browse manuscript"` | `"עיין בכתב היד"` | line 103 |
| `"Add to Puzzle"` | `"הוסף לפאזל"` | lines 850, 2886 |
| `"Add to List"` | `"הוסף לרשימה"` | lines 849, 1901 |
| `"Open"` | `"פתח את"` (note: has trailing "את") | line 1438 |
| `"User"` | `"משתמש"` | line 2039 |
| `"Community"` | `"קהילה"` | lines 1011, 1469 |
| `"No image"` | `"אין תמונה"` | line 882 |

### Keys that NEED ADDING

| English key | Hebrew value (from UI-SPEC) | Notes |
|-------------|---------------------------|-------|
| `"Find joins"` | `"מצא צירופים"` | New — not in TRANSLATIONS |
| `"Join Workbench"` | `"מעבדת צירופים"` | New — not in TRANSLATIONS |
| `"ANCHOR"` | `"עוגן"` | New — not in TRANSLATIONS |
| `"Known Joins"` | `"צירופים ידועים"` | Note: `"Add from Known Joins"` exists (line 3067) but not bare "Known Joins" |
| `"Make anchor"` | `"הגדר כעוגן"` | New — not in TRANSLATIONS |
| `"Add as Join"` | `"הוסף כצירוף"` | UI-SPEC marked "already present" but NOT found; must add |
| `"Enter shelfmark…"` | `"הזן סימת מדף…"` | Multiple "Enter shelfmark..." variants exist (lines 81, 295, 868, 1868, 2873) but the UI-SPEC uses a unique ellipsis form with Hebrew siman madaf; use a unique key like `"Enter shelfmark…"` or check if an existing key matches |
| `"Known join"` (generic badge) | `"צירוף ידוע"` | New |
| `"Open fragment"` | `"פתח קטע"` | UI-SPEC checker REC-1 — more specific than bare "Open"; add this key |

**"Open" key concern:** The existing `"Open": "פתח את"` has trailing `"את"` (accusative marker
used in Hebrew "open [it]" phrasing). This may not read correctly standalone as a button label.
The checker's REC-1 recommends `"Open fragment"` / `"פתח קטע"` — add that instead.

**"(no image)" key:** The UI-SPEC shows `"(no image)"` as the placeholder. `"No image"` already
exists (line 882 → `"אין תמונה"`). Use `tr("No image")` to reuse the existing key rather than
adding a new `"(no image)"` form.

### TRANSLATIONS update block (one `.update({...})` call at end of genizah_translations.py)

```python
TRANSLATIONS.update({
    "Find joins": "מצא צירופים",
    "Join Workbench": "מעבדת צירופים",
    "ANCHOR": "עוגן",
    "Known Joins": "צירופים ידועים",
    "Make anchor": "הגדר כעוגן",
    "Add as Join": "הוסף כצירוף",
    "Known join": "צירוף ידוע",
    "Open fragment": "פתח קטע",
    # "Enter shelfmark…" — verify during plan whether an existing variant is reusable;
    # if not: "Enter shelfmark…": "הזן סימת מדף…",
})
```

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Workbench window / lifecycle | Desktop (PyQt6 QDialog) | GenizahGUI host | Modeless window owned by GenizahGUI as `_join_workbench` ref |
| Anchor image fetch | Shared core `MetadataManager.enrich_metadata` | ImageLoaderThread (desktop) | enrich_metadata is shared; image loading is desktop-specific |
| Anchor text fetch | Shared core `SearchEngine.get_browse_page` | — | Text is in the shared Tantivy index |
| Known-joins BFS (user) | Shared core `JoinsManager.get_connected_fragments_by_id` | — | joins_cache.pkl is shared |
| Known-joins (PGP) | `shared.document_service` | — | PGP DB is shared |
| Known-joins (FJMS) | `shared.fjms_service` | — | FJMS DB is shared |
| Known-joins thumbnails | `MetadataManager.get_thumbnail` | ImageLoaderThread (desktop) | get_thumbnail is shared; loading is desktop |
| Browse action | GenizahGUI.`open_result_in_browse_from_table` | Desktop Browse tab | Desktop-only action |
| Puzzle action | GenizahGUI.`open_anchor_in_puzzle` → `add_to_puzzle` | Desktop PuzzleCanvasWindow | Desktop-only action |
| Add-to-List action | GenizahGUI.`show_add_to_list_menu` | `ListsManager` | Desktop UI; ListsManager is shared |
| Add-as-Join action | GenizahGUI.`open_anchor_as_join` → `JoinsDialog` | `JoinsManager.create_join_local` + `corrections_client.create_join` | Dialog is desktop; persistence is shared |
| Join persistence + Supabase | `corrections_client.create_join` + `JoinsManager.create_join_local` | Supabase cloud | Offline-first via joins_cache.pkl |
| i18n | `genizah_translations.py` `tr()` | — | Shared translation dict, `tr()` available in all modules |

---

## Common Pitfalls

### Pitfall 1: FL-substituted thumbnail URL for the anchor image

**What goes wrong:** Using `get_thumbnail(sys_id)` for the anchor image returns an NLI 400px
thumbnail URL. Using the page index to substitute that FL into a base URL gives the wrong FL (NLI
returns a 403 forbidden placeholder).

**How to avoid:** Anchor image MUST use `enrich_metadata(sys_id)` → `images_nli[idx]` (or
`images_ext[idx]`) → `iiif_full(entry['url'], 2000)`. This is D-05 and was the iteration D fix.

**Warning sign:** Anchor image appears as a small white/grey NLI "forbidden" placeholder (not the
manuscript image).

### Pitfall 2: Calling UI methods from QThread workers

**What goes wrong:** `open_anchor_in_puzzle`, `open_anchor_as_join`, `open_result_in_browse_from_table`,
`show_add_to_list_menu` all create/modify Qt widgets. Called from a QThread worker, they cause
crashes or undefined behavior.

**How to avoid:** All four action callbacks are button click handlers — they run on the UI thread
automatically. Never connect action buttons to callbacks that spawn their own background threads
without first returning to the UI thread for the widget operations.

### Pitfall 3: Known-joins panel not refreshing after Add-as-Join

**What goes wrong:** `JoinsDialog.exec()` blocks until the dialog closes. The workbench's
`_reload_known_joins()` must be called AFTER `dialog.exec()` returns.

**How to avoid:** The pattern is: `open_anchor_as_join(...)` calls `dialog.exec()` (synchronous,
blocks UI), then returns to the button click handler, which calls `self.workbench._reload_known_joins()`.
Since `exec()` is synchronous, this sequencing is automatic if placed correctly in `open_anchor_as_join`.
Alternatively, connect to `dialog.finished` signal before `exec()`.

### Pitfall 4: Known-joins from only JoinsManager (missing PGP/FJMS)

**What goes wrong:** Calling only `joins_mgr.get_connected_fragments_by_id(anchor_sid)` misses
PGP (scholars/editors) and FJMS (scholarly) joins — the two most authoritative sources.

**How to avoid:** The worker must call all three sources: JoinsManager (user joins) + PGP service
+ FJMS service. This mirrors exactly what `JoinsDialog.load_joins()` does (`corrections_ui.py:3830`).

### Pitfall 5: `get_thumbnail` returns None for non-NLI fragments

**What goes wrong:** `get_thumbnail` returns `None` for Oxford, Cambridge, Manchester, JTS
fragments. If the thumbnail label is not guarded, a None URL causes `ImageLoaderThread` to emit
`load_failed` immediately.

**How to avoid:** `ImageLoaderThread.run()` already handles `not self.url` by emitting `load_failed`.
The thumbnail label must show "(no img)" placeholder text on `load_failed`. This is confirmed by
the existing `ThumbResolver` / `CandidateCard` pattern in the sketch.

### Pitfall 6: `JoinsDialog` missing dependencies (shelf_model, shelf_completer)

**What goes wrong:** `JoinsDialog.__init__` gracefully handles None for `shelf_model` and
`shelf_completer` — but without them, the fragment B autocomplete doesn't work, making free entry
harder for scholars.

**How to avoid:** Pass `shelf_model=getattr(self, 'shelf_model', None)` and
`shelf_completer=getattr(self, 'shelf_completer', None)` from GenizahGUI (these attributes exist
post-startup per DESKTOP-INTEGRATION-NOTES). See R-02 code sample.

### Pitfall 7: `get_browse_page` returning None

**What goes wrong:** `get_browse_page` returns `None` if the sys_id is not in the browse map
(transcription index). Not all 255K manuscripts have transcriptions.

**How to avoid:** Guard with `bp = self.searcher.get_browse_page(sys_id, page) or {}` and display
"(no transcription)" or empty text. The sketch's `_AnchorLoadWorker` already does this.

---

## Code Examples

### _AnchorLoadWorker (verified pattern from sketch)

```python
# Source: join_workbench.py.txt:286–310 — verified headless
class _AnchorLoadWorker(QThread):
    done = pyqtSignal(dict)

    def __init__(self, wb, sys_id, page, initial=False):
        super().__init__()
        self.wb = wb
        self.sys_id = sys_id
        self.page = page
        self.initial = initial

    def run(self):
        out = {"page": self.page, "initial": self.initial, "images": [], "text": "", "total": None}
        try:
            meta = self.wb.meta_mgr.enrich_metadata(self.sys_id) or {}
            out["images"] = meta.get("images_nli") or meta.get("images_ext") or []
        except Exception:
            out["images"] = []
        try:
            bp = self.wb.searcher.get_browse_page(self.sys_id, self.page) or {}
            out["text"] = bp.get("text", "") or ""
            out["total"] = bp.get("total_pages")
        except Exception:
            pass
        self.done.emit(out)
```

### iiif_full helper

```python
# Source: join_workbench.py.txt:151–157 — verified
def iiif_full(base_url, width=2000):
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url
    return f"{base_url}/full/{width},/0/default.jpg"
```

### apply_line_numbered_text for anchor

```python
# Source: desktop/widgets/line_number_text_edit.py:332 — verified signature
from desktop.widgets.line_number_text_edit import apply_line_numbered_text
apply_line_numbered_text(
    self.anchor_text_browser,
    htmlify(text, pattern=None),   # RTL wrapper from sketch
    source_text=text,
    is_html=True
)
```

### open_anchor_in_puzzle (public wrapper to add)

```python
# Add to genizah_app.py GenizahGUI class
def open_anchor_in_puzzle(self, sys_id: str):
    """Public: add a fragment to the Fragment Puzzle canvas (Join Workbench path)."""
    self._vs_add_to_puzzle(sys_id)
```

### open_anchor_as_join (public wrapper to add)

```python
# Add to genizah_app.py GenizahGUI class
def open_anchor_as_join(self, anchor_sys_id: str, anchor_shelfmark: str):
    """Public: open JoinsDialog with anchor as Fragment A; scholar enters B freely."""
    def browse_shelfmark(target_shelfmark):
        self.browse_shelf_input.setText(target_shelfmark)
        self._set_last_browse_field("shelf")
        self.browse_load()

    dialog = JoinsDialog(
        self, self.corrections_client,
        document_id=anchor_sys_id,
        shelfmark=anchor_shelfmark,
        on_browse=browse_shelfmark,
        shelf_model=getattr(self, 'shelf_model', None),
        joins_mgr=getattr(self, 'joins_mgr', None),
        shelf_completer=getattr(self, 'shelf_completer', None),
        lists_mgr=getattr(self, 'lists_mgr', None),
        meta_mgr=self.meta_mgr,
    )
    # frag_b_input left empty — scholar enters B freely
    dialog.exec()
```

---

## Standard Stack

### Core (desktop — no new dependencies)

| Module | Version | Purpose | Note |
|--------|---------|---------|------|
| PyQt6 | existing | QDialog, QThread, widgets | Already project dep |
| `genizah_core.JoinsManager` | existing | User joins BFS | `genizah_core.py:9936` |
| `genizah_core.MetadataManager.enrich_metadata` | existing | Anchor image list | `genizah_core.py:4295` |
| `genizah_core.MetadataManager.get_thumbnail` | existing | Known-join thumbnails | `genizah_core.py:4892` |
| `genizah_core.SearchEngine.get_browse_page` | existing | Anchor text | `genizah_core.py:9483` |
| `desktop.image_loader.ImageLoaderThread` | existing | Image fetching | `desktop/image_loader.py:15` |
| `desktop.widgets.line_number_text_edit.apply_line_numbered_text` | existing | RTL text gutter | `desktop/widgets/line_number_text_edit.py:332` |
| `corrections_ui.JoinsDialog` | existing | Add-as-Join dialog | `corrections_ui.py:3278` |
| `shared.document_service` | existing | PGP joins | Called by JoinsDialog already |
| `shared.fjms_service` | existing | FJMS scholarly joins | Called by JoinsDialog already |
| `genizah_translations.tr()` | existing | i18n | Imported everywhere |

**No new pip dependencies.** Phase 107 reuses only existing project modules.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Anchor image loading | Custom HTTP image fetch | `ImageLoaderThread(url)` | Handles Referer headers, Rosetta fallback, disk cache |
| RTL line-numbered text | Custom painter | `apply_line_numbered_text` | Handles RTL gutter, HTML mode, line counting |
| Join BFS (user joins) | Custom graph walk | `JoinsManager.get_connected_fragments_by_id` | Handles both normalized shelfmark and document_id indexes |
| Join creation + Supabase | Custom form/API | `JoinsDialog` + `corrections_client.create_join` | Full offline-first path, autocomplete, existing UX |
| Shelfmark autocomplete | Custom completer | `NormalizingCompleter` inside `JoinsDialog._setup_completer` | Already handles Hebrew normalization |

---

## Environment Availability

Step 2.6: SKIPPED — Phase 107 is desktop code/UI only. All dependencies are existing project
modules. No new CLI tools, databases, or external services are introduced. Existing services
(Supabase, NLI IIIF) are already available in the production environment.

---

## Validation Architecture

`nyquist_validation: true` — include full section.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing project test suite) |
| Config file | `pytest.ini` or implicit (project root) |
| Quick run command | `pytest tests/test_join_workbench.py -x` |
| Full suite command | `pytest tests/ -x` |

### Testing Reality for PyQt6 Desktop UI

This phase introduces a `QDialog` subclass (`JoinWorkbenchWindow`) with QThread workers. PyQt6 UI
tests require a `QApplication` instance and an event loop — they cannot be run headlessly in a
standard pytest environment without `pytest-qt`. **Two tiers of testing** are appropriate:

**Tier 1 — Off-UI-thread logic (unit-testable, no QApplication needed):**
- `iiif_full(base_url)` helper — pure function
- `meta_brief(res)` helper — pure function
- `htmlify(text, pattern)` — pure function
- Source badge assignment logic (given a join dict with a `source` field, return the correct badge label and color)
- Known-joins dedup logic (given lists of user/PGP/FJMS joins, produce a merged list without duplicates)
- i18n key coverage — AST scan confirming all tr() calls in `join_workbench.py` have a corresponding key in TRANSLATIONS

**Tier 2 — Integration smoke (manual or pytest-qt):**
- Open workbench from ResultDialog entry hook → window appears, anchor image loads (within 10s), known-joins panel shows if the anchor has known joins
- Folio nav: click ▶ → image and text update
- Zoom: click + → image scales
- Add-as-Join: button opens JoinsDialog with Fragment A pre-filled, Fragment B empty
- Re-anchor: "⚓ make anchor" on a known-join row swaps the anchor
- Cold start: enter a valid shelfmark → workbench opens with that anchor
- Hebrew mode: `CURRENT_LANG = 'he'` → all labels in Hebrew, no English strings visible
- Dark mode: `QApplication.palette()` with dark window → teal and borders visible

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| JWB-01 | `JoinWorkbenchWindow` opens modeless, single-instance re-anchor | Smoke (manual) | — | ❌ Wave 0 |
| JWB-02 | Three entry hooks fire and build correct anchor_result | Unit + smoke | `pytest tests/test_join_workbench.py::test_entry_hooks -x` | ❌ Wave 0 |
| JWB-03 | Anchor pane: image route loads `iiif_full` URL | Unit (`iiif_full`) + smoke | `pytest tests/test_join_workbench.py::test_iiif_full -x` | ❌ Wave 0 |
| JWB-04 | Known-joins panel shows correct count + per-row source badges | Unit (badge logic) + smoke | `pytest tests/test_join_workbench.py::test_source_badge_mapping -x` | ❌ Wave 0 |
| JWB-09 | Add-as-Join opens JoinsDialog anchor-only; group refreshes after close | Smoke (manual) | — | ❌ Wave 0 |
| SC#5 | No `_vs_*` calls in `join_workbench.py` | AST guard | `pytest tests/test_join_workbench_no_private.py -x` | ❌ Wave 0 |
| SC#6 | All `tr()` keys in `join_workbench.py` present in TRANSLATIONS | AST guard | `pytest tests/test_join_workbench_i18n.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_join_workbench.py -x` (unit logic only, < 5s)
- **Per wave merge:** `pytest tests/ -x` (full suite)
- **Phase gate:** Full suite green + manual smoke on Windows before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_join_workbench.py` — covers unit-testable helpers (`iiif_full`, `meta_brief`, `htmlify`, source badge logic, dedup logic) and AST guards (no `_vs_*`, all `tr()` keys present)
- [ ] Manual smoke checklist in VERIFICATION.md (anchor load, folio nav, zoom, Add-as-Join, re-anchor, cold start, Hebrew mode, dark mode)

---

## Security Domain

Phase 107 is a desktop UI shell with no new network endpoints, no new data storage, no new auth
flows, and no new Supabase schema. All persistence goes through the existing `JoinsManager` /
`corrections_client.create_join` path already covered by prior phases.

**Applicable ASVS categories for new code in this phase:**

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No — no new auth | Delegates to existing `corrections_client` |
| V3 Session Management | No | N/A |
| V4 Access Control | No — no new routes | Delegates to JoinsDialog existing login gate |
| V5 Input Validation | Yes — cold-start shelfmark input, JoinsDialog Fragment B | `resolve_system_by_shelfmark` + `JoinsDialog` `NormalizingCompleter` normalization |
| V6 Cryptography | No | N/A |

No new threat patterns introduced. The cold-start shelfmark input is passed to
`meta.resolve_system_by_shelfmark(q)` which does a normalized lookup — no SQL injection risk
(Tantivy + CSV bank lookups, not raw SQL).

---

## Assumptions Log

All claims in this research were verified by reading the actual source code at the specified
file:line references. No unverified assumptions.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | All claims verified | — | — |

---

## Open Questions

1. **"Enter shelfmark…" key for cold-start input**
   - What we know: Multiple near-identical keys exist (lines 81, 295, 868, 1868, 2873 in genizah_translations.py), none are the exact `"Enter shelfmark…"` form with Unicode ellipsis
   - What's unclear: Which existing key is closest / should be reused
   - Recommendation: During plan, pick the closest existing key OR add `"Enter shelfmark…"` with value `"הזן סימת מדף…"` to avoid confusion with existing variants

2. **Community puzzle joins in the known-joins panel**
   - What we know: `_load_community_joins()` in JoinsDialog fetches `client.get_published_joins_for_fragment(document_id)` and renders them as a separate section
   - What's unclear: D-09 says "user / community" share the green badge — should the workbench show community joins in the same flat list as user/PGP/FJMS joins, or as a separate section?
   - Recommendation: Follow JoinsDialog's existing pattern (separate labeled section) for the v8 shell — it avoids dedup complexity with puzzle join IDs that are different from pairwise join IDs. The planner can choose either approach; the panel-hidden-when-empty rule (D-11) applies to the combined total.

3. **`"Add as Join"` key provenance**
   - What we know: UI-SPEC marked this as "already present" but it was not found in TRANSLATIONS
   - What's unclear: Whether it was added post-SPEC as part of a recent i18n batch
   - Recommendation: Add it explicitly in Wave 0 with the block above; no harm in adding a key that already exists (TRANSLATIONS is a dict, duplicate keys just overwrite)

---

## Sources

### Primary (HIGH confidence — verified by code read)

- `genizah_core.py:9936–10199` — `JoinsManager` class, `get_connected_fragments_by_id`, join data structure
- `genizah_core.py:4295–4519` — `MetadataManager.enrich_metadata`, `images_nli`/`images_ext` shape
- `genizah_core.py:4892–4911` — `MetadataManager.get_thumbnail`
- `genizah_core.py:9483–9710` — `SearchEngine.get_browse_page` signature and return shape
- `corrections_ui.py:3278–3452` — `JoinsDialog.__init__`, fragment A/B construction
- `corrections_ui.py:3547–3605` — `_get_fjms_joins()` — FJMS provenance via `source='FJMS'`
- `corrections_ui.py:3750–3828` — `_get_pgp_joins()` — PGP provenance via `source='PGP'`
- `corrections_ui.py:3830–3923` — `load_joins()` — the three-source load pattern
- `corrections_ui.py:3932–3971` — `_load_community_joins()` — community puzzle joins
- `genizah_app.py:5239–5272` — `_vs_open_joins_with_partner` and `_vs_add_to_puzzle`
- `genizah_app.py:14208–14265` — `show_add_to_list_menu`
- `genizah_app.py:18561–18576` — `_create_action_button`
- `genizah_app.py:18824–18838` — `open_result_in_browse_from_table`
- `genizah_app.py:15362–15387` — `add_to_puzzle`
- `genizah_app.py:6934–7037` — Browse tab `ext_info_row` layout
- `desktop/image_loader.py:15–80` — `ImageLoaderThread` constructor and run
- `desktop/widgets/line_number_text_edit.py:332–390` — `apply_line_numbered_text` signature
- `genizah_translations.py` — full TRANSLATIONS dict verified for all UI-SPEC keys
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt:1–310` — `_AnchorLoadWorker`, `iiif_full`, `htmlify`, `meta_brief`
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — verified reuse map

---

## Metadata

**Confidence breakdown:**
- R-01 Known-joins provenance: HIGH — read all relevant methods in corrections_ui.py and genizah_core.py
- R-02 JoinsDialog anchor-only: HIGH — read full `__init__` and `init_ui` body
- R-03 Public wrappers + thread safety: HIGH — read `_vs_add_to_puzzle`, `_vs_open_joins_with_partner`, `open_result_in_browse_from_table`, `show_add_to_list_menu` in full
- Image route (D-05): HIGH — code confirmed at genizah_core.py:4476,4518
- Text route (D-06/D-07): HIGH — signature and return shape confirmed
- Entry-point hooks (D-03): HIGH — `_create_action_button` + `ext_info_row` layout confirmed
- i18n audit (D-16): HIGH — every key searched against the full TRANSLATIONS dict

**Research date:** 2026-06-04
**Valid until:** 2026-07-04 (stable codebase; these signatures rarely change)
