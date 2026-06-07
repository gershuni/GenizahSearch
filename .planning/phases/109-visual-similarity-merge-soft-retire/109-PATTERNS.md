# Phase 109: Visual-Similarity Merge & Soft-Retire — Pattern Map

**Mapped:** 2026-06-07
**Files analyzed:** 6 (1 new, 5 modified)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/join_workbench.py` — `_vs_to_norm_dict` shim + VS/Combined wiring in `JoinCandidatePane` | service + component | event-driven / CRUD | `JoinCandidatePane._maybe_assemble` + `_EnrichWorker` + `ThumbResolver` (same file, text path) | exact (same file, same flow) |
| `genizah_app.py` — reroute `_browse_view_visual_similarity:4708` + `source=` param on `open_joins_workbench:15464` | controller | request-response | `_browse_open_join_workbench:9868` + `open_joins_workbench:15464` (same file) | exact |
| `desktop/result_dialog.py:758` — reroute `_rd_search_visual_similarity` | controller | request-response | `result_dialog.py:740` `_open_join_workbench` (same file, same pattern) | exact |
| `shared/joins_lab.py` — `normalize_candidate:248`, `merge_candidates:511`, `Candidate` dataclass | service | transform | itself (Phase 106 — already unit-tested, reused as-is) | reuse |
| `genizah_translations.py` — add EN+HE keys for Phase 109 strings | config | transform | existing Phase 108 keys `:3829-3994` | exact |
| `tests/test_join_workbench_vs.py` — NEW test file (parity invariant + adapter + grey-out) | test | CRUD | `tests/test_visual_similarity.py` (`tmp_vs_db` fixture) + `tests/test_join_workbench_i18n.py` (AST pattern) + `tests/test_join_workbench.py` (`_make_result` helper) | role-match |

---

## Pattern Assignments

### `desktop/join_workbench.py` — `_vs_to_norm_dict` shim (new module-level function)

**Analog:** `shared/joins_lab.py:248` `normalize_candidate` (the downstream consumer of the shim)

**Key contract verified** (`shared/joins_lab.py:258-277`):
```python
# normalize_candidate reads these exact keys:
sys_id  = _r_sid(res)                  # reads res.get("display", {}).get("id")
page    = page_of(res)                 # reads display.get("img"); None when img=None
via_vs  = bool(res.get("_via_vs"))     # underscore-prefixed sentinel
vs_rank = res.get("vs_rank")           # NOT "rank"
vs_score= res.get("svm_score")         # key name matches service output
```

**VS service output shape** (`shared/visual_similarity_service.py:122-124`):
```python
{'alma_id': str(row[0]), 'svm_score': row[1], 'rank': i + 1}
```

**Required shim** (module-level in `desktop/join_workbench.py`, before `JoinWorkbenchWindow`):
```python
def _vs_to_norm_dict(row: dict) -> dict:
    """Map get_suggestions() row → normalize_candidate()-compatible dict.

    Key renames: alma_id → display.id, rank → vs_rank.
    display.img = None → page_of() returns None → Candidate.page = None (RR-12).
    _via_vs = True → Candidate.via_vs = True (normalize_candidate reads underscore key).
    """
    return {
        "display": {
            "id": row["alma_id"],
            "shelfmark": "",       # enriched later by _EnrichWorker
            "title": "",
            "library_code": "",
            "img": None,           # page=None (VS is manuscript-level)
        },
        "uid": f"{row['alma_id']}|vs",
        "vs_rank": row["rank"],    # normalize_candidate reads "vs_rank"
        "svm_score": row["svm_score"],  # passthrough — same key name
        "_via_vs": True,
        "full_text": "",
        "scope": "",
    }
```

---

### `desktop/join_workbench.py` — `JoinCandidatePane` VS/Combined source wiring

**Analog:** the existing TEXT source path in `JoinCandidatePane` (same class, same file)

**Source selector UI stub** (current state, `join_workbench.py:2073-2083`):
```python
# Find Candidates action row — Phase 108 stub (only btn_find)
src_row = QHBoxLayout()
src_row.setSpacing(4)
src_row.addStretch()
self.btn_find = QPushButton(tr("Find Candidates"))
self.btn_find.clicked.connect(self.do_search)
src_row.addWidget(self.btn_find)
rv.addLayout(src_row)
```

**Provenance badge pattern** (`join_workbench.py:1667-1676`) — copy for ★both / ⊙VS:
```python
# Existing pattern in CandidateCard.__init__:
shelf_text = c.shelfmark
if c.is_anchor_self:
    shelf_text += tr("  ⚓ self")
elif c.via_other_side:
    shelf_text += tr("  ⇄ other side")
shelf_lbl = QLabel(shelf_text)
# Phase 109 adds: elif c.via_text and c.via_vs:  shelf_text += tr("  ★ both")
# Phase 109 adds: elif c.via_vs and not c.via_text: shelf_text += tr("  ⊙ VS")
```

**`_maybe_assemble` plug point** (`join_workbench.py:2398-2404`) — replace the `[]` second arg:
```python
def _maybe_assemble(self):
    """Merge sources and start enrichment (RR-2: merge_candidates returns a LIST)."""
    from shared.joins_lab import merge_candidates
    # Phase 108 stub (second arg hardcoded []):
    self.results = list(merge_candidates(self._text_cands or [], []))
    # Phase 109 change:
    self.results = list(merge_candidates(self._text_cands or [], self._vs_cands or []))
    self._page = 0
    self._start_enrich()
```

**`_start_enrich` pattern** (`join_workbench.py:2406-2443`) — the batched enrichment handoff (copy for `_load_vs` teardown):
```python
def _start_enrich(self):
    """Start the batched enrichment worker."""
    from shared.fjms_service import get_fjms_service
    if self._enrich_worker is not None:
        try:
            self._enrich_worker.cancel()
        except Exception:
            pass
        self._enrich_worker = None
    # ... build fjms_svc, anchor_meas ...
    if fjms_svc is not None and self.results:
        self._enrich_worker = _EnrichWorker(fjms_svc, self.results, anchor_meas)
        self._enrich_worker.enriched.connect(self._on_enriched)
        self._enrich_worker.start()
    else:
        self._enrich = {}
        self.apply_filters()
```

**`ThumbResolver` page-lazy pattern** (`join_workbench.py:2575-2584`) — already handles VS-only (`page=None`) rows because it receives only `(gidx, c.sys_id)` with no page arg:
```python
# In _render_grid_page — passes only visible 20 items:
items_for_thumbs = []
for i, c in enumerate(page_cands):
    gidx = start + i
    card = CandidateCard(self, c, gidx, enrich)
    self.cards[gidx] = card
    self.grid_layout.addWidget(card, i // _GRID_COLS, i % _GRID_COLS)
    items_for_thumbs.append((gidx, c.sys_id))   # sys_id only — no page

if items_for_thumbs and self.wb.meta_mgr is not None:
    if self._resolver is not None:
        try:
            self._resolver.cancel()
        except Exception:
            pass
    self._resolver = ThumbResolver(self.wb.meta_mgr, items_for_thumbs)
    self._resolver.resolved.connect(self._on_thumb_url)
    self._resolver.start()
```

**`_on_anchor_loaded` hook** (`join_workbench.py:4082-4109`) — the right place to call `_on_anchor_set` for VS greying-out (after anchor data is known):
```python
def _on_anchor_loaded(self, gen: int, out: dict):
    """Handle anchor metadata + text result. Drop if generation is stale."""
    if gen != self._gen:
        return  # stale guard
    # ... update UI labels, text, load image ...
    self._load_current_image()
    # Phase 109: call self.candidate_pane._on_anchor_set() here
    # (after anchor sid is confirmed, so has_suggestions() is meaningful)
```

**`set_anchor` sequence** (`join_workbench.py:4031-4072`) — confirms that `_anchor_sid` is set BEFORE `_start_anchor_load` fires (so `_on_anchor_set` can safely call `has_suggestions(self.wb._anchor_sid)`):
```python
def set_anchor(self, res: dict):
    self._gen += 1
    gen = self._gen
    self._cancel_workers()
    self._anchor_res = dict(res)
    self._anchor_sid = r_sid(res)    # set here — safe for has_suggestions
    # ...
    self._start_anchor_load(gen, page=page, initial=True)  # fires _on_anchor_loaded
```

**Grey-out pattern** — base from `QRadioButton.setEnabled()` (same PyQt6 pattern used by `other_enable` toggle at line 2047):
```python
self.other_enable.toggled.connect(
    lambda v: self.other_box.setVisible(v)
)
# Phase 109 analog:
# self.rb_visual.setEnabled(has_vs)
# self.rb_combined.setEnabled(has_vs)
```

**`get_vs_service` threading model** (`shared/visual_similarity_service.py:76-91`):
```python
if thread_safe:   # default True
    from shared.thread_local_db import ThreadLocalConnection
    self._conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row, timeout=10.0)
else:
    self._conn = sqlite3.connect(uri, uri=True, check_same_thread=True, timeout=10.0)
# Use thread_safe=True (default) from any QThread worker.
# Use thread_safe=False only on the UI thread (matching genizah_app.py:4727).
```

---

### `genizah_app.py` — reroute `_browse_view_visual_similarity` + `source=` param

**Analog:** `_browse_open_join_workbench:9868` (exact blueprint for the replacement body)

**Blueprint** (`genizah_app.py:9868-9887`):
```python
def _browse_open_join_workbench(self):
    sid = getattr(self, "current_browse_sid", None)
    if not sid:
        return
    p = getattr(self, "current_browse_p", 1) or 1   # REAL attr (not self.p)
    text = getattr(self, "browse_original_text", "") or ""
    shelf = ""
    try:
        shelf, _ = self.meta_mgr.get_meta_for_id(sid)
    except Exception:
        shelf = ""
    res = {
        "display": {"id": sid, "shelfmark": shelf, "img": p, "library_code": "", "title": ""},
        "full_text": text,
        "uid": f"{sid}_P{int(p):03d}",
    }
    self.open_joins_workbench(res)
    # Browse tab stays open
```

**Current `open_joins_workbench` signature** (`genizah_app.py:15464-15474`):
```python
def open_joins_workbench(self, res: dict):
    from desktop.join_workbench import JoinWorkbenchWindow
    if self._join_workbench is None or not self._join_workbench.isVisible():
        self._join_workbench = JoinWorkbenchWindow(self, self)
    self._join_workbench.set_anchor(res)
    self._join_workbench.show()
    self._join_workbench.raise_()
    self._join_workbench.activateWindow()
```
Phase 109 adds `source: str = "text"` param and calls `self._join_workbench.set_source("visual")` before `show()` when `source == "visual"`. Order constraint (RESEARCH `set_anchor` note): call `set_anchor(res)` FIRST (sets `_anchor_sid` which VS needs), then `set_source("visual")`.

**Deprecation marker pattern for `_show_vs_dialog`** — add at the top of the method body before any logic (`genizah_app.py:4788`):
```python
def _show_vs_dialog(self, sys_id, shelfmark, data, parent_dialog=None, on_pick=None):
    # DEPRECATED (Phase 109): The normal-mode path (on_pick is None) is no longer
    # reachable from standard entry points — _browse_view_visual_similarity and
    # _rd_search_visual_similarity now open the Join Workbench directly.
    # This code is retained as a safety net for one cycle (D-11) and WILL BE
    # DELETED in a future cleanup phase.
    #
    # EXCEPTION: The pick-mode branch (on_pick is not None) remains ACTIVE —
    # called from JoinsDialog; must not be removed (D-12).
    ...  # rest of method unchanged
```

**Pick-mode branch to KEEP UNTOUCHED** (`genizah_app.py:5107-5117`):
```python
def _add_as_join(_pid=partner_id, _shelf=shelf, _dlg=dlg):
    if on_pick:
        # Pick mode: fill fragment B in the calling JoinsDialog
        on_pick(_pid, _shelf)
        _dlg.accept()
    else:
        # Normal mode (DEPRECATED): open JoinsDialog with partner pre-filled
        _dlg.accept()
        if parent_dialog:
            parent_dialog.close()
        self._vs_open_joins_with_partner(sys_id, shelfmark, _pid, _shelf)
```

---

### `desktop/result_dialog.py:758` — reroute `_rd_search_visual_similarity`

**Analog:** `result_dialog.py:740` `_open_join_workbench` (same file, same close-after-open pattern)

**`_open_join_workbench` pattern** (`result_dialog.py:740-756`):
```python
def _open_join_workbench(self):
    res = {"display": {"id": self.current_sys_id, "shelfmark": ..., "img": ..., ...}}
    # ... build res from current result context ...
    app = getattr(self, "_app", None) or self.parent()
    if app is not None and hasattr(app, "open_joins_workbench"):
        app.open_joins_workbench(res)
    self.close()   # ResultDialog closes after launching the workbench (D-03 #1)
```

**Current shelfmark extraction** (`result_dialog.py:766-771`) — reuse this in the rerouted body:
```python
shelfmark = str(sys_id)
if self.all_results and 0 <= self.current_result_idx < len(self.all_results):
    result = self.all_results[self.current_result_idx]
    shelfmark = (result.get('display', {}).get('shelfmark')
                 or result.get('shelfmark')
                 or str(sys_id))
```

Phase 109 replacement body: build `res` from `self.current_sys_id` + shelfmark, call `app.open_joins_workbench(res, source="visual")`, then `self.close()`.

---

### `shared/joins_lab.py` — reused as-is

**No changes.** `normalize_candidate:248`, `merge_candidates:511`, and the `Candidate` dataclass `:76-128` are the domain model. Phase 109 only CALLS these functions from `desktop/join_workbench.py`. The shim `_vs_to_norm_dict` lives in `join_workbench.py`, not here.

**`merge_candidates` return type reminder** (`joins_lab.py:511-559`):
```python
def merge_candidates(text_cands: list, vs_cands: list) -> list:
    # Returns a plain list[Candidate] — NOT a MergeResult.
    # RR-2: do NOT call .candidates on the return value.
    ...
    return merged
```

**`vs_score=None` sentinel** (`joins_lab.py:119`, Pitfall 6):
```python
vs_score: Optional[float] = None   # None == "no VS data" (NOT 0.0 dissimilar)
# Guard display with: if c.vs_score is not None:
```

---

### `genizah_translations.py` — add Phase 109 EN+HE keys

**Analog:** the Phase 108 block pattern at `genizah_translations.py:3829-3994`

**Pattern for adding a new block** (exact style):
```python
# === Phase 109 — Visual-Similarity source wiring ===
TRANSLATIONS.update({
    # --- Provenance badges in CandidateCard ---
    "  ★ both":               "  ★ שניהם",
    "  ⊙ VS":                 "  ⊙ דמיון",
    "  ✎ text":               "  ✎ טקסט",

    # --- Source selector labels (replaces stubs from Phase 108) ---
    "Text":                   "טקסט",
    "Visual source":          "מקור חיצוני",
    "Combined source":        "מקור משולב",

    # --- Visual source state ---
    "Visual source loaded":   "מקור חיצוני נטען",
    "No visual similarity data for this manuscript":
        "אין נתוני דמיון חיצוני עבור כתב יד זה",
})
```

**Pre-registered stubs from Phase 108** (already in TRANSLATIONS, can be REUSED or replaced):
- `"Visual similarities"` → `"דמיון חיצוני"` (line 3832)
- `"Search + visual"` → `"חיפוש + חיצוני"` (line 3833)
- `"Visual source (coming soon)"` (line 3836-3837) — stub, replace label if wording changes
- `"Combined source (coming soon)"` (line 3838-3839) — stub, replace label if wording changes
- `"Text source (active)"` → `"מקור טקסט (פעיל)"` (line 3840)

**i18n guard test contract** (`tests/test_join_workbench_i18n.py:55-59`) — every `tr("key")` in `desktop/join_workbench.py` must be in `TRANSLATIONS`:
```python
def test_all_tr_keys_in_translations():
    from genizah_translations import TRANSLATIONS
    source = TARGET.read_text(encoding="utf-8")
    keys = _extract_tr_keys(source)
    # Fails if ANY key is missing from TRANSLATIONS
```
**Rule:** update `genizah_translations.py` in the SAME plan that introduces new `tr()` calls.

---

### `tests/test_join_workbench_vs.py` — NEW test file

**Analog 1:** `tests/test_visual_similarity.py` — `tmp_vs_db` fixture (copy verbatim for the parity test)

**`tmp_vs_db` fixture** (`tests/test_visual_similarity.py:11-40`):
```python
@pytest.fixture
def tmp_vs_db(tmp_path):
    """Create a temporary visual_similarity.db with known test data."""
    db_path = str(tmp_path / 'visual_similarity.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE visual_suggestions (
        alma_id_a INTEGER NOT NULL,
        alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL,
        PRIMARY KEY (alma_id_a, alma_id_b)
    )''')
    conn.execute('CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a)')
    # Insert test pairs for alma_id_a=100
    test_pairs = [
        (100, 201, 15.5), (100, 202, 12.3), (100, 203, 10.1),
        (100, 204, 8.7), (100, 205, 5.2),
    ]
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)', test_pairs)
    conn.commit()
    conn.close()
    return db_path
```

**Analog 2:** `tests/test_join_workbench.py:29-44` — `_make_result` helper (reuse for building Candidate inputs):
```python
def _make_result(sys_id: str, page: int = 1, **extra) -> dict:
    d = {
        "display": {
            "id": extra.pop("id", sys_id),
            "shelfmark": extra.pop("shelfmark", f"T-S 12.{sys_id[-3:]}"),
            "title": extra.pop("title", ""),
            "library_code": extra.pop("library_code", "CUL"),
            "img": page,
        },
        "uid": extra.pop("uid", f"{sys_id}_FGP_P{page:03d}"),
        "full_text": extra.pop("full_text", ""),
        "sys_id": sys_id,
    }
    d.update(extra)
    return d
```

**Analog 3:** `tests/test_join_workbench_no_private.py:28-41` — static AST guard pattern (reuse structure for any new static guard tests):
```python
def test_no_vs_private_calls_in_join_workbench():
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source)
    offenders = [
        (name, lineno)
        for name, lineno in _iter_calls(tree)
        if name.startswith("_vs_")
    ]
    assert not offenders, "SC#5 violation ..."
```

**New test file structure for `test_join_workbench_vs.py`:**
```python
# Header pattern (matches test_join_workbench.py:1-6):
# -*- coding: utf-8 -*-
"""Tests for Phase 109 VS adapter, parity invariant, and grey-out behavior."""
import pytest
import sqlite3
from shared.visual_similarity_service import VisualSimilarityService
from shared.joins_lab import normalize_candidate, merge_candidates
from desktop.join_workbench import _vs_to_norm_dict  # new pure helper

# JWB-12a: adapter unit test (no Qt needed)
def test_vs_adapter_maps_fields():
    row = {"alma_id": "990001234500205171", "svm_score": 14.7, "rank": 1}
    d = _vs_to_norm_dict(row)
    c = normalize_candidate(d)
    assert c.sys_id == "990001234500205171"
    assert c.page is None           # VS is manuscript-level
    assert c.via_vs is True
    assert c.vs_rank == 1
    assert c.vs_score == 14.7

# JWB-12b: parity invariant (uses tmp_vs_db fixture)
def test_vs_parity_invariant(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    raw = svc.get_suggestions("100", 200)
    vs_cands = [normalize_candidate(_vs_to_norm_dict(r)) for r in raw]
    expected_sids = {r["alma_id"] for r in raw}
    actual_sids = {c.sys_id for c in vs_cands}
    assert actual_sids == expected_sids   # same sys_id set

# JWB-12g: grey-out when has_suggestions=False (no Qt — tests the logic branch)
def test_no_suggestions_returns_empty_vs_cands(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert not svc.has_suggestions("999999")   # not in DB
    # When has_suggestions=False, _load_vs should produce []
```

---

## Shared Patterns

### QThread worker pattern (error handling + cancel guard)
**Source:** `desktop/join_workbench.py:1550-1598` (`_EnrichWorker.run`)
**Apply to:** `_VSFetchWorker` (if VS fetch is moved off UI thread)
```python
def run(self):
    # Pattern: check self._cancel before each expensive step
    sys_ids = [c.sys_id for c in self.candidates]
    meas = {}
    try:
        meas = self.fjms_svc.get_measurement_summaries_batch(sys_ids)
    except Exception as exc:
        logger.warning("_EnrichWorker.get_measurement_summaries_batch: %s", exc)
    for c in self.candidates:
        if self._cancel:
            return
        ...
    self.enriched.emit(out)
```

### Deleted-widget safety (RuntimeError guard)
**Source:** `desktop/join_workbench.py:4046-4048`, `4066-4069`, `4094-4095`
**Apply to:** ALL QLabel/QWidget writes in slots connected to worker signals
```python
try:
    self.anchor_shelf.setText(r_shelf(res))
except RuntimeError:
    pass
```

### Generation token (stale-result guard)
**Source:** `desktop/join_workbench.py:4037-4039`, `4084-4085`
**Apply to:** any new `_VSFetchWorker` that carries a gen token
```python
self._gen += 1
gen = self._gen
# In the worker's done slot:
if gen != self._gen:
    return  # stale — a newer set_anchor() fired
```

### i18n: `tr()` + `TRANSLATIONS.update({})` in same wave
**Source:** `genizah_translations.py:3974-3994` (Phase 108 round 3 pattern)
**Apply to:** every new `tr("string")` call added in `desktop/join_workbench.py`
```python
# Same-plan rule: add key to genizah_translations.TRANSLATIONS in the same
# plan that introduces the tr() call, or test_join_workbench_i18n.py will fail.
TRANSLATIONS.update({
    "  ★ both": "  ★ שניהם",
    ...
})
```

---

## No Analog Found

No files in this phase lack a close analog. All modifications mirror existing patterns in the same files.

---

## Metadata

**Analog search scope:** `desktop/join_workbench.py`, `genizah_app.py`, `desktop/result_dialog.py`, `shared/joins_lab.py`, `shared/visual_similarity_service.py`, `genizah_translations.py`, `tests/test_join_workbench_*.py`, `tests/test_visual_similarity.py`
**Files scanned:** 10
**Pattern extraction date:** 2026-06-07
