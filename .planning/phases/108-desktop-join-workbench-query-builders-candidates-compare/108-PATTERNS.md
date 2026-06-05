# Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare — Pattern Map

**Mapped:** 2026-06-05
**Files analyzed:** 8 new/modified surfaces

**Analogs found:** 8 / 8

---

## File Classification

| New / Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------------|------|-----------|----------------|---------------|
| `desktop/join_workbench.py` — `JoinQueryBuilder` (QWidget) | component | request-response | `TabularQueryBuilderDialog` (`genizah_app.py:1543`) | role-match |
| `desktop/join_workbench.py` — `ThumbResolver` (QThread) | service | request-response | Spike `ThumbResolver` (sketch L258-283) + Phase 107 `ThumbBatchWorker` (`:522-566`) | exact |
| `desktop/join_workbench.py` — `_CrossSideWorker` (QThread) | service | event-driven | Spike `_CrossSideWorker` (sketch L332-429) | exact |
| `desktop/join_workbench.py` — `_EnrichWorker` (QThread) | service | batch | Phase 107 `ThumbBatchWorker` (`:522-566`) + `FjmsService.get_domains_for_sys_ids` | role-match |
| `desktop/join_workbench.py` — `CandidateCard` (QFrame) | component | request-response | Spike `CandidateCard` (sketch L584-689) | exact |
| `desktop/join_workbench.py` — `JoinCandidatePane` (QWidget) | component | event-driven | Spike `_build_results_pane()` (sketch L785-905) | exact |
| `desktop/join_workbench.py` — `CompareDialog` (QDialog) | component | request-response | Spike `CompareDialog` (sketch L1473-1563) | exact |
| `desktop/join_workbench.py` — `DesktopSearchExecutor` (adapter) | service | request-response | `shared/joins_lab.py::SearchExecutor` Protocol (`:149-193`) | exact |
| `shared/fjms_service.py` — `get_measurements_batch()` | service | CRUD | `FjmsService.get_domains_for_sys_ids()` (`fjms_service.py:866`) | exact |
| `genizah_app.py:1555` — RTL-chrome fix | utility | n/a | `TabularQueryBuilderDialog.__init__` (`:1550-1555`) | exact |

---

## Pattern Assignments

### `JoinQueryBuilder` (QWidget) — new widget in `desktop/join_workbench.py`

**Analog:** `TabularQueryBuilderDialog` (`genizah_app.py:1543`) for chrome; spike `QueryBuilder` (sketch L478-581) for row model

**Transplant vs net-new:** Modifier-checkbox row pattern (checkboxes + options) transplants from TabularQueryBuilderDialog; the row model (vertical rows, per-row start/end/gap) transplants from the spike. **Net-new vs the spike:** the spike uses ONE `term` QLineEdit per row; D-04/D-05 (USER-CONFIRMED 2026-06-05) require a horizontal strip of MULTIPLE clickable OR word-boxes per row with a `[+ or]` button — `build_side_query()` joins each row's non-empty box texts with `|` into the single `BuilderRow.term`. `compose()` call and 3-tuple return replaces `get_syntax()`.

**Imports pattern** — mirror `desktop/join_workbench.py` top-of-file (line 1-10):
```python
from genizah_core import CURRENT_LANG, get_logger, tr
from shared.joins_lab import htmlify, page_of, snippet_html, snippet_plain
from shared.joins_lab import BuilderRow, SideQuery, compose
```

**Multi-box row construction pattern** (EXTENDS spike sketch L507-535 single-box `add_row` → N OR word-boxes; then wrap in `tr()`):
```python
def _make_box(self, placeholder: str) -> QLineEdit:
    box = QLineEdit()
    box.setLayoutDirection(Qt.LayoutDirection.RightToLeft)   # RTL content, LTR chrome
    box.setPlaceholderText(placeholder)
    box.returnPressed.connect(self.on_enter)
    box.textChanged.connect(self._update_preview)
    return box

def add_row(self):
    rw = QWidget()
    row = QHBoxLayout(rw)
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(2)                       # UI-SPEC: 2px internal density
    # RTL: in Hebrew a line STARTS on the right and ENDS on the left, so
    # "ends line" sits left of the boxes strip and "starts line" sits right of it.
    end = QCheckBox(tr("ends line ⊣"))  # ⊣ — line END (left edge in Hebrew)
    end.setToolTip(tr("Last word must be at the END of the line (left edge in Hebrew)"))
    # boxes_strip: the horizontal strip of OR word-boxes (one BuilderRow.term, |-joined)
    boxes_strip = QHBoxLayout()
    boxes_strip.setSpacing(2)
    first_box = self._make_box(self.first_hint if not self.rows else tr("word(s) on this line…"))
    boxes = [first_box]
    boxes_strip.addWidget(first_box, 1)
    add_or = QPushButton(tr("+ or"))                       # ← appends another OR-alternative box
    add_or.setAccessibleName(tr("Add an OR alternative to this line"))
    add_or.setToolTip(tr("Add another word that may appear INSTEAD (OR) on this same line"))
    start = QCheckBox(tr("⊢ starts line"))  # ⊢ — line START (right edge in Hebrew)
    start.setToolTip(tr("First word must be at the START of the line (right edge in Hebrew)"))
    gap = QSpinBox()
    gap.setRange(0, 40)
    gap.setPrefix(tr("↓ "))   # ↓
    gap.setSuffix(tr(" ln"))
    gap.setToolTip(tr("Lines to skip before the next row (0 = consecutive lines)"))
    rm = QPushButton("×")     # ×
    rm.setFixedWidth(24)
    rm.setAccessibleName(tr("Remove row"))
    entry = {"start": start, "boxes": boxes, "boxes_strip": boxes_strip,
             "end": end, "gap": gap, "rm": rm, "widget": rw}
    add_or.clicked.connect(lambda: self.add_or_box(entry))
    rm.clicked.connect(lambda: self.remove_row(entry))
    row.addWidget(end)
    row.addLayout(boxes_strip, 1)
    row.addWidget(add_or)
    row.addWidget(start)
    row.addWidget(gap)
    row.addWidget(rm)
    self.rows.append(entry)
    self.rows_box.addWidget(rw)
    self._sync()

def add_or_box(self, entry: dict):
    """Append another OR-alternative word-box to this row (each added box is removable, keep >= 1)."""
    box = self._make_box(tr("or…"))
    entry["boxes"].append(box)
    entry["boxes_strip"].addWidget(box, 1)
    self._update_preview()
```

**Modifier-checkbox row pattern** (TabularQueryBuilderDialog `:1627-1685` — borrow structure, adapt to Lines-first scope):
```python
# Search Options Row — transplant from TabularQueryBuilderDialog:1674-1685
opts_row = QHBoxLayout()
opts_row.addWidget(QLabel(tr("Search Options") + ":"))
self.chk_opt_variants = QCheckBox(tr("Variants"))
self.chk_opt_ja       = QCheckBox(tr("Judeo-Arabic"))
self.chk_opt_flex     = QCheckBox(tr("Flex Spacing"))
self.chk_opt_bidir    = QCheckBox(tr("Bidirectional"))
for chk in (self.chk_opt_variants, self.chk_opt_ja, self.chk_opt_flex, self.chk_opt_bidir):
    opts_row.addWidget(chk)
    chk.stateChanged.connect(self._update_preview)
opts_row.addStretch()
```

**`_responsa_opts()` pattern** (spike sketch L555-558 — net-new extended version vs spike's 2-option version):
```python
def _responsa_opts(self) -> dict:
    v = self.chk_opt_variants.isChecked()
    return {
        "responsa_mode": True,
        "variants": v,
        "ja": self.chk_opt_ja.isChecked(),
        "flex_spacing": self.chk_opt_flex.isChecked(),
        "bidirectional": self.chk_opt_bidir.isChecked(),
        "variant_mode": "variants" if v else "exact",
    }
```

**`is_empty()` and `build_side_query()` call pattern** (multi-box: each row's non-empty box texts join with `|` into the single term; calls `shared/joins_lab.compose()`):
```python
def is_empty(self) -> bool:
    return not any(b.text().strip() for e in self.rows for b in e["boxes"])

def build_side_query(self) -> SideQuery | None:
    """Build a SideQuery from the current widget state. Returns None if empty.

    Each row holds multiple OR word-boxes (D-04/D-05); join the non-empty box texts
    with '|' (no spaces) into the single BuilderRow.term. A single-box row therefore
    yields the bare term (additive, identical to the single-box design); whitespace
    WITHIN a box is preserved as multi-word proximity on that line.
    """
    if self.is_empty():
        return None
    rows = tuple(
        BuilderRow(
            term="|".join(b.text().strip() for b in e["boxes"] if b.text().strip()),
            line_start=e["start"].isChecked(),
            line_end=e["end"].isChecked(),
            gap_to_next=e["gap"].value(),
        )
        for e in self.rows          # ALL rows (empty gaps preserved for gap_to_next semantics)
    )
    return SideQuery(
        rows=rows,
        variants=self.chk_opt_variants.isChecked(),
        page_position=self._page_position(),  # None | 'start' | 'end'
    )
```

> `compose()` (Phase 106) applies the start-anchor `|` prepend to the FIRST token of the first box's
> term and the end-anchor `|` append to the LAST token — the multi-box UI produces exactly the same
> `term` string the single-box design would have, so the downstream `compose()` 3-tuple contract is
> UNCHANGED (`(query_str, responsa_options, page_position)`), `BuilderRow` still carries one `term`,
> and the engine path (`text_position` forward, ONE line-break engine call) is untouched.

**Dark-mode detection pattern** (TabularQueryBuilderDialog `:1572-1574` — copy verbatim):
```python
palette = self.palette()
self._is_dark = palette.color(palette.ColorRole.Window).lightness() < 128
```

**Preview field pattern** (TabularQueryBuilderDialog `:1688-1706` — copy styling):
```python
preview_bg     = '#2d2d2d' if self._is_dark else '#f8f9fa'
preview_border = '#555'    if self._is_dark else '#dee2e6'
self._preview_label = QLabel("")
self._preview_label.setStyleSheet(
    f"font-family:'Consolas','Courier New',monospace;font-size:13px;"
    f"padding:4px 8px;background:{preview_bg};border:1px solid {preview_border};"
    f"border-radius:4px;min-height:22px;"
)
self._preview_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
self._preview_label.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
```

**RTL-chrome bug to NOT repeat (Pitfall 6 / R-04 / D-06):**
Do NOT call `self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` on the `JoinQueryBuilder` or its row `QHBoxLayout`. Only set RTL on individual `QLineEdit` (the OR word-boxes) — already shown above in `_make_box()` at `box.setLayoutDirection(...)`.

---

### RTL-chrome fix — `TabularQueryBuilderDialog` (`genizah_app.py:1555`)

**Analog:** `TabularQueryBuilderDialog.__init__` (`:1550-1577`)

**Transplant vs net-new:** One-line removal. Separate commit before the new builder widget.

**The line to remove** (`genizah_app.py:1555`):
```python
self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)   # REMOVE THIS LINE
```

**Why it's safe:** Each word input already has `inp.setLayoutDirection(RightToLeft)` (`:1779`); the preview label already has its own RTL directive (`:1704`). Removing the dialog-level directive restores correct LTR chrome for checkboxes and labels.

---

### `ThumbResolver` (QThread) — new class in `desktop/join_workbench.py`

**Analog:** Spike `ThumbResolver` (sketch L258-283); Phase 107 `ThumbBatchWorker` (`desktop/join_workbench.py:522-566`)

**Transplant vs net-new:** Nearly verbatim transplant from the spike. Emits URL strings (not QImage/QPixmap) to avoid GUI-thread QPixmap construction (Phase 107 Pitfall 4 / must-fix #8).

**Core pattern** (sketch L258-283 — transplant with `tr()` guard for any user-visible strings):
```python
class ThumbResolver(QThread):
    """Resolve NLI thumbnail URLs for candidate cards (one MARC fetch per sys_id)."""
    resolved = pyqtSignal(int, str)  # (card_index_in_current_page, url or '')

    def __init__(self, meta_mgr, items: list):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.items = items   # list of (idx, sys_id)
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        for idx, sid in self.items:
            if self._cancel:
                return
            url = ""
            try:
                if sid:
                    url = self.meta_mgr.get_thumbnail(sid) or ""
            except Exception:
                url = ""
            if self._cancel:
                return
            self.resolved.emit(idx, url)
```

**QPixmap must be on GUI thread** (Phase 107 `ThumbBatchWorker` `:522-566` — worker emits QImage; slot on GUI thread converts):
```python
# Worker emits QImage (ok off-thread):
candidate = QImage()
candidate.loadFromData(resp.content)
self.resolved.emit(self._gen, i, candidate)

# GUI-thread slot converts to QPixmap:
pix = QPixmap.fromImage(qimg)
```

For Phase 108 `ThumbResolver`, the worker emits a URL string only; the GUI-thread `_on_thumb_url` slot then calls `_enqueue_image(card.img, url)` which starts `ImageLoaderThread` — keeping QPixmap construction fully on the GUI thread.

---

### `_CrossSideWorker` (QThread) — new class in `desktop/join_workbench.py`

**Analog:** Spike `_CrossSideWorker` (sketch L332-429)

**Transplant vs net-new:** The spike's QThread wrapper transplants; the inner logic is now delegated to `shared/joins_lab.apply_cross_side()` (Phase 106 pure function). The production class is a thin QThread shell.

**Signal/init pattern** (sketch L332-350):
```python
class _CrossSideWorker(QThread):
    """Run the cross-side AND/OR membership check off the UI thread."""
    done = pyqtSignal(object)   # emits MergeResult from shared/joins_lab

    def __init__(self, executor, base_candidates, b_query: SideQuery,
                 combine: str, a_pattern: str | None):
        super().__init__()
        self.executor = executor
        self.base = base_candidates
        self.b_query = b_query
        self.combine = combine        # 'AND' | 'OR'
        self.a_pattern = a_pattern
        self._cancel = False

    def cancel(self):
        self._cancel = True
```

**`run()` delegation pattern** (delegates to `shared/joins_lab.apply_cross_side()`; NOT the sketch's inline logic):
```python
def run(self):
    from shared.joins_lab import apply_cross_side, compose
    try:
        b_str, b_ro, b_pos = compose(self.b_query)
    except ValueError:
        self.done.emit(MergeResult(candidates=tuple(self.base), note=""))
        return
    if not b_str:
        self.done.emit(MergeResult(candidates=tuple(self.base), note=""))
        return
    try:
        result = apply_cross_side(
            self.executor, self.base, self.b_query,
            b_ro, self.combine, self.a_pattern,
        )
    except Exception as exc:
        logger.warning("_CrossSideWorker error: %s", exc)
        result = MergeResult(candidates=tuple(self.base), note="")
    self.done.emit(result)
```

**Deleted-widget safety guard** (Phase 107 pattern — wrap all QLabel/QWidget writes in try/except RuntimeError):
```python
try:
    self.status.setText(f"this side: {n} · checking other side ({self.combine})…")
except RuntimeError:
    pass
```

---

### `_EnrichWorker` (QThread) — new class in `desktop/join_workbench.py`

**Analog:** Phase 107 `ThumbBatchWorker` (`desktop/join_workbench.py:522-566`) for QThread shell; `FjmsService.get_domains_for_sys_ids()` (`shared/fjms_service.py:866-907`) for the batch IN-query pattern.

**Transplant vs net-new:** Entirely net-new (the spike did serial per-card calls at render time — Pitfall 3). Signal shape and worker lifecycle pattern transplant from `ThumbBatchWorker`.

**Signal / init pattern** (modeled on `ThumbBatchWorker`):
```python
class _EnrichWorker(QThread):
    """Batch-enrich all candidates: measurements + snippets + size-mismatch hints.

    Runs AFTER dedup_candidates() but BEFORE render. Never call get_measurements()
    per-card at render time (Pitfall 3: ~200 serial SQLite round-trips).
    """
    enriched = pyqtSignal(dict)  # {sys_id: {"w", "h", "material", "lines", "cat",
                                 #            "snippet_html", "snippet_plain", "mismatch"}}

    def __init__(self, fjms_svc, candidates: list, anchor_meas: dict | None):
        super().__init__()
        self.fjms_svc = fjms_svc
        self.candidates = candidates        # list of normalized result dicts
        self.anchor_meas = anchor_meas or {}
        self._cancel = False

    def cancel(self):
        self._cancel = True
```

**Batch measurement call pattern** (modeled on `get_domains_for_sys_ids` `:866-907`):
```python
def run(self):
    from shared.joins_lab import snippet_html, snippet_plain
    sys_ids = [r_sid(c) for c in self.candidates]
    meas = {}
    try:
        meas = self.fjms_svc.get_measurements_batch(sys_ids)
    except Exception as exc:
        logger.warning("_EnrichWorker.get_measurements_batch: %s", exc)
    out = {}
    for c in self.candidates:
        if self._cancel:
            return
        sid = r_sid(c)
        m = meas.get(sid) or {}
        snip_h = snippet_html(r_text(c), c.get("highlight_pattern"), max_lines=6)
        snip_p = snippet_plain(r_text(c), c.get("highlight_pattern"), max_chars=220)
        mismatch = False
        if m.get("w") and self.anchor_meas.get("w"):
            ratio = max(m["w"], self.anchor_meas["w"]) / max(min(m["w"], self.anchor_meas["w"]), 0.01)
            mismatch = ratio > 1.4
        out[sid] = {
            "w": m.get("w"), "h": m.get("h"), "material": m.get("material"),
            "lines": m.get("lines"), "cat": m.get("cat"),
            "snippet_html": snip_h, "snippet_plain": snip_p,
            "mismatch": mismatch,
        }
    self.enriched.emit(out)
```

---

### `FjmsService.get_measurements_batch()` — new method in `shared/fjms_service.py`

**Analog:** `FjmsService.get_domains_for_sys_ids()` (`shared/fjms_service.py:866-907`)

**Transplant vs net-new:** Structural transplant of the batch IN-query pattern; the SELECT columns and table are new.

**Batch IN-query pattern** (`:866-907` — copy this structure exactly):
```python
def get_measurements_batch(self, sys_ids: list[str]) -> dict:
    """Batch-fetch manuscript_measurements for multiple AlmaIds.

    Returns {sys_id: {"w": float|None, "h": float|None, "material": str|None,
                       "lines": float|None, "cat": str|None}}.
    Missing sys_ids are absent from the result dict — caller treats as None.
    Uses parameterized IN-query (no SQL injection risk).
    """
    if not self._conn or not sys_ids:
        return {}
    try:
        result = {}
        batch_size = 500    # same as get_domains_for_sys_ids
        for i in range(0, len(sys_ids), batch_size):
            batch = sys_ids[i:i + batch_size]
            placeholders = ','.join('?' * len(batch))
            cursor = self._conn.execute(
                f"SELECT AlmaId, catalog_width_cm, catalog_height_cm, material, "
                f"avg_num_lines, size_category "
                f"FROM manuscript_measurements WHERE AlmaId IN ({placeholders})",
                batch,
            )
            for row in cursor:
                sid = row["AlmaId"]
                result[sid] = {
                    "w": row["catalog_width_cm"],
                    "h": row["catalog_height_cm"],
                    "material": row["material"],
                    "lines": row["avg_num_lines"],
                    "cat": row["size_category"],
                }
        return result
    except Exception as e:
        logger.error(f"FjmsService.get_measurements_batch error: {e}")
        return {}
```

---

### `CandidateCard` (QFrame) — new class in `desktop/join_workbench.py`

**Analog:** Spike `CandidateCard` (sketch L584-689)

**Transplant vs net-new:** Overall structure, triage button row, `_restyle()` border, `set_pixmap()` transplant. Net-new: `setAccessibleName()` on all buttons (UI-SPEC Dim 2 / D-19), provenance badge rendering from `badge_for_source()` (Phase 107 helper), dimension evidence line using pre-fetched enrichment dict (not serial `meas_for(sid)`), size-mismatch hint, all strings `tr()`-wrapped.

**Card init pattern** (sketch L585-608 — transplant skeleton):
```python
class CandidateCard(QFrame):
    def __init__(self, pane, res: dict, global_idx: int, enrich: dict):
        super().__init__()
        self.pane = pane           # JoinCandidatePane (not dialog — D-20)
        self.res = res
        self.global_idx = global_idx
        self.sid = r_sid(res)
        self.setFixedWidth(232)    # UI-SPEC: card fixed width 232px
        self.setFrameShape(QFrame.Shape.Box)
        self._restyle()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(2)          # UI-SPEC: xs=4px internally but cards use 2px density
```

**Thumbnail label pattern** (sketch L599-603):
```python
self.img = QLabel(tr("loading…"))
self.img.setFixedSize(220, 130)   # UI-SPEC: 220×130 px
self.img.setAlignment(Qt.AlignmentFlag.AlignCenter)
self.img.setStyleSheet("background:#e2e8f0;color:#64748b;")
lay.addWidget(self.img)
```

**Triage border `_restyle()` pattern** (sketch L673-678 — transplant, use `TRI_COLOR` dict):
```python
TRI_COLOR = {None: "#94a3b8", "yes": "#16a34a", "maybe": "#d97706", "no": "#dc2626"}

def _restyle(self):
    if self.res.get("_is_anchor_self"):
        self.setStyleSheet("QFrame{border:3px solid #14b8a6;border-radius:4px;}")
        return
    tri = self.pane.wb.triage.get(self.sid)
    self.setStyleSheet(f"QFrame{{border:3px solid {TRI_COLOR[tri]};border-radius:4px;}}")
```

**Triage + action buttons pattern** (sketch L642-671 — transplant + add `setAccessibleName`, replace `self.dialog.act_*` with `self.pane.wb.act_*` per D-20):
```python
trow = QHBoxLayout()
for emoji, val, name in (("Y", "yes", tr("Mark yes")),
                         ("?", "maybe", tr("Mark maybe")),
                         ("N", "no", tr("Mark no"))):
    b = QPushButton(emoji)
    b.setFixedSize(28, 28)     # UI-SPEC: 28×28 minimum touch target
    b.setAccessibleName(name)  # D-19 / UI-SPEC Dim 2
    b.clicked.connect(lambda _, v=val: self.pane.wb.mark(self.sid, v))
    trow.addWidget(b)
cmp = QPushButton("⧂")    # ⤢
cmp.setFixedSize(28, 28)
cmp.setAccessibleName(tr("Compare"))
cmp.setToolTip(tr("Compare side-by-side with anchor"))
cmp.clicked.connect(lambda: self.pane.open_compare(self.global_idx))
trow.addWidget(cmp)
lay.addLayout(trow)
```

**Dimension evidence line** (net-new — from pre-fetched `enrich` dict, NOT serial meas_for call):
```python
m = enrich.get(self.sid) or {}
dim_parts = []
if m.get("w") and m.get("h"):
    dim_parts.append(f"{m['w']:.0f}×{m['h']:.0f} cm")  # ×
elif m.get("cat"):
    dim_parts.append(str(m["cat"]))
if m.get("material"):
    dim_parts.append(str(m["material"]))
if m.get("lines"):
    dim_parts.append(f"~{m['lines']:.0f} ln")
dim_str = " · ".join(dim_parts)   # ·
if dim_str:
    dim_lbl = QLabel(dim_str)
    dim_lbl.setStyleSheet(f"font-size:10px;color:{DIM_COLOR};")
    if m.get("mismatch"):
        dim_lbl.setToolTip(tr("Size may not match anchor"))
    lay.addWidget(dim_lbl)
```

**set_pixmap pattern** (sketch L680-689 — transplant verbatim):
```python
def set_pixmap(self, pix):
    try:
        if pix and not pix.isNull():
            self.img.setText("")
            self.img.setPixmap(pix.scaled(
                220, 130,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            ))
        else:
            self.img.setText(tr("(no image)"))
    except RuntimeError:
        pass   # widget already deleted — standard Phase 107 guard
```

---

### `JoinCandidatePane` (QWidget) — new class in `desktop/join_workbench.py`

**Analog:** Spike `_build_results_pane()` (sketch L785-905) + `_on_results` / `apply_filters` / `render_page` / `toggle_view` (sketch L1098-1340)

**Transplant vs net-new:** Layout skeleton (builder section, other-side collapsible, refine bar, status + view toggle + pagination, grid + table) transplants. Net-new: source selector row (D-14 Text/VS-stub/Combined-stub); self-match inline in status label (D-15); `_maybe_assemble` delegates to `shared/joins_lab.merge_candidates()`; all strings `tr()`-wrapped; enrichment dict drives material filter population (not serial SQLite at filter time).

**Right-pane section tag pattern** (sketch L789 — transplant `_tag()` from Phase 107 shell):
```python
rv.addWidget(self._tag(
    tr("THIS SIDE — find pages matching, line by line "
       "(hunt the MISSING continuation):"),
    "#0f766e",
))
```

**Other-side collapsible section** (sketch L797-816 — transplant, wrap labels in `tr()`):
```python
os_top = QHBoxLayout()
self.other_enable = QCheckBox(
    tr("also constrain the OTHER side (adjacent image p±1):"))
self.other_enable.toggled.connect(lambda v: self.other_box.setVisible(v))
os_top.addWidget(self.other_enable)
self.other_combine = QComboBox()
self.other_combine.addItems([tr("AND (narrow)"), tr("OR (widen)")])
os_top.addWidget(self.other_combine)
os_top.addStretch()
rv.addLayout(os_top)
self.other_box = QWidget()
ob = QVBoxLayout(self.other_box)
ob.setContentsMargins(16, 0, 0, 0)   # UI-SPEC: md=16px indent for other-side
self.other_builder = JoinQueryBuilder(
    self.do_search, first_hint=tr("word(s) on the OTHER side…"))
ob.addWidget(self.other_builder)
self.other_box.setVisible(False)
rv.addWidget(self.other_box)
```

**Source selector scaffolding — JWB-12 seam** (D-14 — net-new, VS/Combined disabled):
```python
src_row = QHBoxLayout()
src_row.addWidget(QLabel(tr("Source:") + " "))
self.src_text = QPushButton(tr("✎ text"))    # ✎ text
self.src_text.setCheckable(True)
self.src_text.setChecked(True)
self.src_text.setAccessibleName(tr("Text source (active)"))
self.src_vs = QPushButton(tr("⊙ visual"))    # ⊙ visual
self.src_vs.setEnabled(False)
self.src_vs.setToolTip(tr("Visual similarity — arriving in Phase 109"))
self.src_vs.setAccessibleName(tr("Visual source (coming soon)"))
self.src_combined = QPushButton(tr("★ combined"))  # ★ combined
self.src_combined.setEnabled(False)
self.src_combined.setToolTip(tr("Combined — arriving in Phase 109"))
self.src_combined.setAccessibleName(tr("Combined source (coming soon)"))
for b in (self.src_text, self.src_vs, self.src_combined):
    src_row.addWidget(b)
src_row.addStretch()
rv.addLayout(src_row)
```

**Refine / filter bar** (sketch L839-857 — transplant, wrap labels in `tr()`):
```python
refine = QHBoxLayout()
refine.addWidget(QLabel(tr("Refine:")))
self.filter_in = QLineEdit()
self.filter_in.setPlaceholderText(tr("filter by text, shelfmark, title…"))
self.filter_in.textChanged.connect(self.apply_filters)
refine.addWidget(self.filter_in, 1)
self.mat_filter = QComboBox()
self.mat_filter.addItem(tr("any material"))
self.mat_filter.currentIndexChanged.connect(self.apply_filters)
refine.addWidget(self.mat_filter)
self.dim_chk = QCheckBox(tr("has dimensions"))
self.dim_chk.stateChanged.connect(self.apply_filters)
refine.addWidget(self.dim_chk)
self.tri_filter = QComboBox()
self.tri_filter.addItems([tr("all"), tr("yes"), tr("maybe"), tr("no"), tr("untriaged")])
self.tri_filter.currentIndexChanged.connect(self.apply_filters)
refine.addWidget(self.tri_filter)
rv.addLayout(refine)
```

**Self-match inline in status label** (sketch L1231-1235 — transplant pattern; D-15 placement):
```python
# Inside apply_filters(), build prefix from self-match state:
if self._anchor_matched is True:
    prefix = tr("⚡ anchor matches this query ✓  ·  ")   # ⚡ ✓
elif self._anchor_matched is False:
    prefix = tr("⚡ anchor does NOT match this query ✗  ·  ")  # ⚡ ✗
else:
    prefix = ""
self.status.setText(f"{prefix}{len(self.filtered)}/{len(self.results)} " + tr("shown"))
```

**Triage counts in status** (sketch L1303-1308):
```python
def _update_status_counts(self):
    y = sum(1 for v in self.wb.triage.values() if v == "yes")
    m = sum(1 for v in self.wb.triage.values() if v == "maybe")
    n = sum(1 for v in self.wb.triage.values() if v == "no")
    base = self.status.text().split("  [")[0]
    self.status.setText(f"{base}  [Y {y}  ? {m}  N {n}]")
```

**_maybe_assemble delegation to `merge_candidates()`** (sketch L1149-1175 — adapt to call `shared/joins_lab.merge_candidates()`):
```python
def _maybe_assemble(self):
    from shared.joins_lab import merge_candidates
    if "text" in self._sources and self._text_cands is None:
        return
    if "vs" in self._sources and self._vs_cands is None:
        return
    text = self._text_cands or []
    vs   = self._vs_cands   or []    # always [] in Phase 108 (VS disabled)
    result = merge_candidates(text, vs)  # pure function from shared/joins_lab
    self.results = list(result.candidates)
    self._set_busy(False)
    self._finish_results()
```

**Grid / table toggle** (sketch L1238-1252 — transplant verbatim):
```python
def toggle_view(self):
    self.view_mode = "table" if self.view_mode == "grid" else "grid"
    self.view_btn.setText(tr("Grid view") if self.view_mode == "table" else tr("Table view"))
    self.render_results()
```

**render_page bounded 5-slot image pool** (sketch L1315-1338 — transplant; ThumbResolver emits URL → `_enqueue_image`):
```python
def render_page(self):
    self._cancel_images()
    # clear grid ...
    data = self.filtered
    start = self.page * PER_PAGE
    page_rs = data[start:start + PER_PAGE]
    items_for_thumbs = []
    for i, res in enumerate(page_rs):
        gidx = start + i
        enrich = self._enrich.get(r_sid(res)) or {}
        card = CandidateCard(self, res, gidx, enrich)
        self.cards[gidx] = card
        self.grid.addWidget(card, i // GRID_COLS, i % GRID_COLS)
        items_for_thumbs.append((gidx, r_sid(res)))
    if items_for_thumbs and self.wb.meta_mgr is not None:
        self._resolver = ThumbResolver(self.wb.meta_mgr, items_for_thumbs)
        self._resolver.resolved.connect(self._on_thumb_url)
        self._resolver.start()
```

**Attach seam** — insert `JoinCandidatePane` at `desktop/join_workbench.py:838` where `layout.addStretch()` is the placeholder:
```python
# Before (Phase 107 placeholder at line 838):
layout.addStretch()

# After (Phase 108 attach):
self._candidate_pane = JoinCandidatePane(self)
layout.addWidget(self._candidate_pane, 1)
```

---

### `CompareDialog` (QDialog) — new class in `desktop/join_workbench.py`

**Analog:** Spike `CompareDialog` (sketch L1472-1563)

**Transplant vs net-new:** Top bar (prev/next/triage), action row, two-pane body, `_fill()` using `res["display"]["img"]` all transplant. Net-new: `wb.act_*` public methods replace sketch's `self.wb.act_*` (already public in Phase 107); `setAccessibleName()` on all buttons; `"other side matched"` meta label when `_via_other_side`; `apply_line_numbered_text` for RTL numbered text (not raw QTextBrowser.setHtml); `setWindowFlags(Qt.WindowType.Dialog)` for modeless.

**Init and top bar** (sketch L1473-1511 — transplant; add `setAccessibleName`, `tr()`):
```python
class CompareDialog(QDialog):
    def __init__(self, wb, start_idx: int):
        super().__init__(wb)
        self.wb = wb
        self.idx = max(0, min(start_idx, len(wb.filtered) - 1))
        self.setWindowTitle(tr("Compare"))
        self.resize(1320, 870)
        self.setWindowFlags(
            self.windowFlags() | Qt.WindowType.WindowMaximizeButtonHint
        )
        v = QVBoxLayout(self)
        topbar = QHBoxLayout()
        self.prev_btn = QPushButton(tr("< prev"))
        self.prev_btn.setFixedSize(34, 28)    # UI-SPEC: 34×28 nav button
        self.prev_btn.setAccessibleName(tr("Previous candidate"))
        self.prev_btn.clicked.connect(lambda: self.step(-1))
        self.nxt_btn = QPushButton(tr("next >"))
        self.nxt_btn.setFixedSize(34, 28)
        self.nxt_btn.setAccessibleName(tr("Next candidate"))
        self.nxt_btn.clicked.connect(lambda: self.step(1))
        self.pos_lbl = QLabel("")
        topbar.addWidget(self.prev_btn)
        topbar.addWidget(self.pos_lbl, 1)
        topbar.addWidget(self.nxt_btn)
        for emoji, val, name in (
            (tr("Y yes"), "yes", tr("Mark yes")),
            (tr("? maybe"), "maybe", tr("Mark maybe")),
            (tr("N no"), "no", tr("Mark no")),
        ):
            b = QPushButton(emoji)
            b.setAccessibleName(name)
            b.clicked.connect(lambda _, x=val: self._mark(x))
            topbar.addWidget(b)
        v.addLayout(topbar)
```

**Action row** (sketch L1493-1505 — transplant; use Phase 107 public methods per D-20):
```python
arow = QHBoxLayout()
for label, fn in (
    (tr("\U0001f4d6 Browse"), lambda: self.wb.open_result_in_browse(self._cur())),
    (tr("\U0001f9e9 Puzzle"), lambda: self.wb.open_result_in_puzzle(self._cur())),
    (tr("\U0001f4cb List"),   lambda: self.wb.open_result_in_list(self._cur(), None)),
    (tr("\U0001f517 Join"),   lambda: self.wb.open_result_as_join(self._cur())),
    (tr("⚓ Re-anchor"),  self._reanchor),
):
    b = QPushButton(label)
    b.setAccessibleName(label)
    b.clicked.connect(lambda _, f=fn: f())
    arow.addWidget(b)
arow.addStretch()
v.addLayout(arow)
```

**Two-pane body `_pane()` factory** (sketch L1514-1523):
```python
def _pane(self):
    box = QVBoxLayout()
    shelf = QLabel()
    shelf.setStyleSheet("font-weight:bold;font-size:13px;")
    shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
    meta = QLabel()
    meta.setWordWrap(True)
    meta.setStyleSheet(f"font-size:11px;color:{META_COLOR};")
    img = QLabel(tr("…"))       # …
    img.setMinimumHeight(360)        # UI-SPEC: 360px minimum image height
    img.setAlignment(Qt.AlignmentFlag.AlignCenter)
    img.setStyleSheet("background:#e2e8f0;color:#64748b;")
    txt = QTextBrowser()
    box.addWidget(shelf); box.addWidget(meta)
    box.addWidget(img); box.addWidget(txt, 1)
    return {"box": box, "shelf": shelf, "meta": meta, "img": img, "txt": txt}
```

**`_fill()` pattern** (sketch L1525-1540 — transplant; use `apply_line_numbered_text` + `wb._load_image` + "other side matched" label):
```python
def _fill(self, pane, res: dict, is_anchor: bool):
    pane["shelf"].setText(r_shelf(res))
    bits = [meta_brief(res)]
    enrich = self.wb._candidate_pane._enrich.get(r_sid(res)) or {}
    m = enrich
    if m.get("w") and m.get("h"):
        bits.append(f"▧ {m['w']:.0f}×{m['h']:.0f} cm")
    elif m.get("material"):
        bits.append("▧ " + str(m["material"]))
    if not is_anchor and res.get("_via_other_side"):
        bits.append(tr("other side matched"))   # D-18 / R-06 label
    pane["meta"].setText("   ·   ".join(b for b in bits if b))
    apply_line_numbered_text(
        pane["txt"],
        htmlify(r_text(res), res.get("highlight_pattern")),
        source_text=r_text(res),
        is_html=True,
    )
    p = page_of(res)  # reads res["display"]["img"] — already set to neighbor page for _via_other_side (R-06)
    self.wb._enqueue_image_for_pane(pane["img"], r_sid(res), p, width=1400)
```

**`step()` / `paint()` / `_mark()` / `_cur()` pattern** (sketch L1542-1563 — transplant verbatim):
```python
def paint(self):
    cand = self.wb.filtered[self.idx]
    tri  = self.wb.triage.get(r_sid(cand))
    self.pos_lbl.setText(
        tr("candidate") + f" {self.idx + 1}/{len(self.wb.filtered)}"
        f"   {r_shelf(self.wb._anchor_res)}  vs  {r_shelf(cand)}"
        f"   [{tri or '-'}]"
    )
    self._fill(self.left, self.wb._anchor_res, is_anchor=True)
    self._fill(self.right, cand, is_anchor=False)

def step(self, d: int):
    self.idx = max(0, min(self.idx + d, len(self.wb.filtered) - 1))
    self.paint()

def _cur(self) -> dict:
    return self.wb.filtered[self.idx]

def _mark(self, val: str):
    self.wb.mark(r_sid(self._cur()), val)
    self.paint()

def _reanchor(self):
    self.wb.set_anchor(self._cur())
    self.accept()
```

---

### Desktop `SearchExecutor` adapter — new inner class in `desktop/join_workbench.py`

**Analog:** `shared/joins_lab.py::SearchExecutor` Protocol (`:149-193`)

**Transplant vs net-new:** Protocol already defined (Phase 106). The concrete adapter is a thin class wrapping `self.searcher` + `self.meta_mgr` on `JoinWorkbenchWindow`. Net-new: instantiation at window init, passed to `JoinCandidatePane`.

**Adapter pattern** (derived from Protocol definition at `shared/joins_lab.py:149-193`):
```python
class _DesktopSearchExecutor:
    """Concrete SearchExecutor adapter for the desktop app (D-22).

    Wraps JoinWorkbenchWindow's searcher + meta_mgr.
    No per-app normalizer — thin passthrough per Phase 106 D-01.
    """
    def __init__(self, searcher, meta_mgr):
        self._searcher = searcher
        self._meta_mgr = meta_mgr

    def execute_search(self, query_str, mode, gap, progress_callback=None,
                       exclude_words=None, responsa_options=None,
                       restrict_sys_ids=None, text_position=None,
                       corpus_scope="all") -> list:
        return self._searcher.execute_search(
            query_str, mode, gap,
            progress_callback=progress_callback,
            exclude_words=exclude_words,
            responsa_options=responsa_options,
            restrict_sys_ids=restrict_sys_ids,
            text_position=text_position,
            corpus_scope=corpus_scope,
        ) or []

    def get_browse_page(self, sys_id, p_num=None, next_prev=0,
                        absolute_index=None, allow_cross=False,
                        volume_ie=None) -> dict | None:
        return self._searcher.get_browse_page(
            sys_id, p_num=p_num, next_prev=next_prev,
            absolute_index=absolute_index, allow_cross=allow_cross,
            volume_ie=volume_ie,
        )

    def get_meta_for_id(self, sys_id: str) -> tuple:
        return self._meta_mgr.get_meta_for_id(sys_id)

    def get_library_for_id(self, sys_id: str) -> str:
        return self._meta_mgr.get_library_for_id(sys_id) or ""
```

**Instantiation in `JoinWorkbenchWindow.__init__`** (or at `set_anchor` time):
```python
self._executor = _DesktopSearchExecutor(self.searcher, self.meta_mgr)
# passed into JoinCandidatePane:
self._candidate_pane = JoinCandidatePane(self, executor=self._executor)
```

---

## Shared Patterns

### QThread lifecycle + deleted-widget safety
**Source:** Phase 107 `ThumbBatchWorker` (`desktop/join_workbench.py:522-566`)
**Apply to:** All new QThread workers (`ThumbResolver`, `_CrossSideWorker`, `_EnrichWorker`)

```python
# Cancel flag:
def cancel(self):
    self._cancel = True

# Guard all GUI-thread QLabel writes in the connected slot:
def _on_thumb_url(self, gidx: int, url: str):
    card = self.cards.get(gidx)
    if not card:
        return
    try:
        if not url:
            card.set_pixmap(None)
            return
        self._enqueue_image(card.img, url)
    except RuntimeError:
        pass  # widget deleted — standard Phase 107 guard
```

### Image loading bounded pool (5 slots)
**Source:** Spike `_enqueue_image` / `_pump_images` (sketch L1370-1412) + `desktop/image_loader.ImageLoaderThread`
**Apply to:** Candidate card images, CompareDialog pane images

```python
MAX_CONCURRENT_IMG = 5

def _enqueue_image(self, label, url, target=None):
    if not url:
        label.setText(tr("(no image)"))
        return
    self._img_queue.append((label, url, target))
    self._pump_images()

def _pump_images(self):
    while self._img_active < MAX_CONCURRENT_IMG and self._img_queue:
        label, url, target = self._img_queue.pop(0)
        self._img_active += 1
        loader = ImageLoaderThread(url)
        def _done(img, lbl=label, tg=target):
            self._img_active -= 1
            try:
                pix = QPixmap.fromImage(img)   # QPixmap on GUI thread only
                # scale + set ...
            except RuntimeError:
                pass
            self._pump_images()
        loader.image_loaded.connect(_done)
        loader.load_failed.connect(lambda lbl=label: (
            setattr(self, '_img_active', self._img_active - 1) or
            self._pump_images()
        ))
        self._img_threads.append(loader)
        loader.start()
```

### i18n: every new string `tr()`-wrapped
**Source:** `genizah_core.tr` (imported at top of `desktop/join_workbench.py` line 7)
**Apply to:** All new strings in `JoinQueryBuilder`, `CandidateCard`, `JoinCandidatePane`, `CompareDialog`

```python
from genizah_core import tr   # already imported in desktop/join_workbench.py:7
# Usage:
b = QPushButton(tr("Add to List"))
b.setAccessibleName(tr("Add to List"))
b.setToolTip(tr("Add this candidate to a saved list"))
```

### No `_vs_*` private app method calls (D-20)
**Source:** Phase 107 public action method names (`desktop/join_workbench.py:1529-1561`)
**Apply to:** All candidate/compare action buttons in `CandidateCard` and `CompareDialog`

```python
# CORRECT (D-20 — public Phase 107 methods):
btn_browse.clicked.connect(lambda: self.wb.open_result_in_browse_from_table(res))
btn_puzzle.clicked.connect(lambda: self.wb.open_anchor_in_puzzle(r_sid(res)))
btn_list.clicked.connect(lambda: self.wb.show_add_to_list_menu(
    [{"sys_id": r_sid(res), "fl_id": "", "img": page_of(res)}],
    source="join_workbench", anchor_widget=btn_list,
))
btn_join.clicked.connect(lambda: self.wb.open_anchor_as_join(
    r_sid(res), r_shelf(res), partner_sid=r_sid(self.wb._anchor_res),
    partner_shelf=r_shelf(self.wb._anchor_res),
))

# WRONG — do NOT use:
# self.wb.app._vs_add_to_puzzle(...)
# self.wb.app._vs_open_joins_with_partner(...)
```

### Triage key: `sys_id` (not `(sys_id, page)`)
**Source:** `JoinWorkbenchWindow.triage` dict; UI-SPEC; spike L1100-1113
**Apply to:** All `triage.get(...)` / `wb.mark(...)` calls

```python
# Dedup key (one entry per IMAGE):
Candidate.key = (sys_id, page)    # used by dedup_candidates() only

# Triage key (one mark per FRAGMENT regardless of page):
self.triage = {}              # {sys_id: "yes"/"maybe"/"no"/None}
self.triage.get(r_sid(res))   # always index by sys_id, not Candidate.key
```

### `_image_cancel` on page change / view toggle / dialog close
**Source:** Spike `_cancel_images` (sketch L1414-1425) + Phase 107 `cancel()` pattern
**Apply to:** `JoinCandidatePane.toggle_view`, `JoinCandidatePane.set_page`, `CompareDialog.closeEvent`

```python
def _cancel_images(self):
    if self._resolver:
        self._resolver.cancel()
        self._resolver = None
    self._img_queue.clear()
    for t in self._img_threads:
        try:
            if hasattr(t, "cancel"):
                t.cancel()
        except Exception:
            pass
    self._img_threads = [t for t in self._img_threads if t.isRunning()]
```

---

## No Analog Found

All 8 surfaces have analogs. The following items are **net-new logic** within otherwise-analoged classes (planner should note these as requiring fresh implementation):

| Item | Inside class | Reason |
|------|-------------|--------|
| Multi-box OR row (`boxes` strip + `[+ or]` / `add_or_box`) (D-04/D-05) | `JoinQueryBuilder` | Spike uses ONE `term` box per row; the locked design is N clickable OR word-boxes per row joined with `\|` into the single term — additive extension of the spike row |
| JWB-12 source-selector row (D-14) | `JoinCandidatePane` | No prior source-selector widget in the codebase; VS/Combined disabled in 108 |
| `_EnrichWorker` batch logic | `_EnrichWorker.run()` | Spike did serial per-card calls; the batch pattern is the anti-pattern fix |
| Size-mismatch hint computation | `_EnrichWorker.run()` | Novel logic; ratio > 1.4 threshold from D-13 |
| `page_position` page-start/end combobox or checkbox pair | `JoinQueryBuilder` | UI-SPEC defers placement to "Claude's discretion" (RESEARCH open question #2) |
| `"other side matched"` candidate-pane label | `CompareDialog._fill()` | D-18 / R-06; no equivalent label exists anywhere in the app |
| `DesktopSearchExecutor` concrete adapter class | `JoinWorkbenchWindow` inner class | Protocol is new (Phase 106); concrete adapter is Phase 108 net-new |

---

## Metadata

**Analog search scope:** `desktop/join_workbench.py`, `genizah_app.py`, `shared/joins_lab.py`, `shared/fjms_service.py`, `desktop/image_loader.py`, `desktop/widgets/line_number_text_edit.py`, `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt`
**Files read:** 9 source files (complete or targeted reads)
**Pattern extraction date:** 2026-06-05

---

## PATTERN MAPPING COMPLETE
</content>
