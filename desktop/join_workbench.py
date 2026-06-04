# -*- coding: utf-8 -*-
"""Desktop Join Workbench — anchor-pinned join-hunting shell (Phase 107).

This module holds the pure helpers (Plan 01) and the JoinWorkbenchWindow shell (Plan 02).
Pure helpers are import-safe without a QApplication so they can be unit-tested headlessly.
"""
from genizah_core import CURRENT_LANG, get_logger, tr  # noqa: F401
from shared.joins_lab import htmlify, page_of, snippet_html, snippet_plain  # D-18: reuse Phase 106

logger = get_logger(__name__)

__all__ = [
    # Phase 106 re-exports (D-18)
    "htmlify",
    "page_of",
    "snippet_html",
    "snippet_plain",
    # Result-dict accessors
    "r_sid",
    "r_shelf",
    "r_title",
    "r_text",
    "r_lib",
    # Pure helpers
    "iiif_full",
    "meta_brief",
    "badge_for_source",
    "dedup_join_rows",
    # Plan 02 pure helpers (testable headlessly)
    "_clamp_zoom",
    "_image_url_for_idx",
    "normalize_join_source",
    "_other_member_of",
    "build_known_join_rows",
    # Plan 02 window class
    "JoinWorkbenchWindow",
]

# ---------------------------------------------------------------------------
# Result-dict accessors — transplanted verbatim from the spike sketch.
# These are part of the workbench's PUBLIC test surface (the private _r_*
# twins in shared.joins_lab are NOT imported here — D-18 only applies to the
# shared pure functions; the workbench accessors are new public API).
# ---------------------------------------------------------------------------


def r_sid(res):
    """Return the system-id string from a search result dict."""
    return (res.get("display") or {}).get("id") or res.get("sys_id") or ""


def r_shelf(res):
    """Return the shelfmark string from a search result dict."""
    d = res.get("display") or {}
    return d.get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?"


def r_title(res):
    """Return the title string from a search result dict."""
    return (res.get("display") or {}).get("title") or ""


def r_text(res):
    """Return the full text from a search result dict."""
    return res.get("full_text") or res.get("text") or ""


def r_lib(res):
    """Return the library code from a search result dict."""
    d = res.get("display") or {}
    return d.get("library_code") or d.get("library") or ""


# ---------------------------------------------------------------------------
# iiif_full — build a full-resolution IIIF image URL from a base URL.
# Source: spike sketch join_workbench.py.txt:151-157 (D-05 / RESEARCH verified).
# ---------------------------------------------------------------------------


def iiif_full(base_url, width=2000):
    """Build a full-resolution IIIF image URL.

    If base_url already ends with .jpg it is already a direct URL and is
    returned unchanged.  An empty / None base_url returns "".
    """
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url
    return f"{base_url}/full/{width},/0/default.jpg"


# ---------------------------------------------------------------------------
# meta_brief — one-line summary for the anchor panel (library · N img · title).
# tr('img') is bootstrapped in Plan-01 Task-1 (the closed i18n set).
# ---------------------------------------------------------------------------


def meta_brief(meta: dict, shelfmark: str = "") -> str:
    """Return a short summary string: library · N img · title (title truncated to 60)."""
    lib = meta.get("library_code", "") or ""
    n_img = len(meta.get("images_nli") or meta.get("images_ext") or [])
    title = (meta.get("title") or "")[:60]
    parts = [p for p in [lib, f"{n_img} {tr('img')}" if n_img else "", title] if p]
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# badge_for_source — source-provenance badge (D-09).
#
# CODEX-VERIFIED (107-REVIEWS.md must-fix #9): do NOT store tr("User") /
# tr("Community") / tr("Known join") at IMPORT time — that freezes the label
# in whatever language was active at import, so a later CURRENT_LANG switch
# (en <-> he) would not be reflected.  Instead store SOURCE KEYS in the config
# and call tr() AT CALL TIME inside badge_for_source / _label_for_source.
# ---------------------------------------------------------------------------

# Config stores (label_kind, light_bg, dark_bg).
#   label_kind: "literal:PGP" / "literal:FJMS"  => use the literal (proper noun, no tr())
#               "tr:User" / "tr:Community"       => resolve via tr() at call time
_BADGE_CONFIG = {
    "PGP":       ("literal:PGP",   "#0ea5e9", "#38bdf8"),
    "FJMS":      ("literal:FJMS",  "#8b5cf6", "#a78bfa"),
    "user":      ("tr:User",       "#10b981", "#34d399"),
    "community": ("tr:Community",  "#10b981", "#34d399"),
}


def _label_for_source(label_kind: str) -> str:
    """Resolve a badge label at call time (must-fix #9: no frozen import-time tr())."""
    if label_kind.startswith("literal:"):
        return label_kind.split(":", 1)[1]
    if label_kind.startswith("tr:"):
        return tr(label_kind.split(":", 1)[1])
    return label_kind


def badge_for_source(source: str, is_dark: bool) -> tuple:
    """Return (label, hex_color) for a join source string.

    Known sources: 'PGP', 'FJMS', 'user', 'community'.
    Any other / unknown source falls back to tr("Known join") + gray color.
    Labels for 'user' and 'community' are resolved via tr() at call time so
    a CURRENT_LANG switch is immediately reflected (must-fix #9).
    """
    cfg = _BADGE_CONFIG.get(source or "")
    if cfg is None:
        # Generic fallback (D-09); tr("Known join") resolved at call time.
        return tr("Known join"), ("#9ca3af" if is_dark else "#6b7280")
    label_kind, light_bg, dark_bg = cfg
    return _label_for_source(label_kind), (dark_bg if is_dark else light_bg)


# ---------------------------------------------------------------------------
# dedup_join_rows — merge join dicts from multiple sources, dedup by
# order-insensitive (a, b) pair.  Mirrors corrections_ui.py:3607
# _merge_fjms_joins_into_display (PATTERNS.md § "Known-joins dedup pattern").
# ---------------------------------------------------------------------------


def dedup_join_rows(join_lists: list) -> list:
    """Merge join dicts from multiple sources; dedup by order-insensitive (a,b) pair.

    Call order = priority: the FIRST source that supplies a pair wins its
    badge/source field.  Subsequent occurrences of the same pair are ignored.
    """
    seen = {}
    for joins in join_lists:
        for j in joins:
            a = (j.get("fragment_a") or "").upper().strip()
            b = (j.get("fragment_b") or "").upper().strip()
            key = (min(a, b), max(a, b))
            if key not in seen:
                seen[key] = j
    return list(seen.values())


# ---------------------------------------------------------------------------
# Plan 02 — pure module-level helpers (testable headlessly without QApplication)
# ---------------------------------------------------------------------------


def _clamp_zoom(z):
    """Clamp zoom factor to [0.25, 4.0] bounds."""
    return max(0.25, min(4.0, z))


def _image_url_for_idx(images, idx, width=2000):
    """Return the IIIF full URL for images[idx], or '' if out of range.

    Uses iiif_full() so NLI base URLs get the IIIF suffix and direct .jpg
    URLs are returned unchanged.  Empty list or out-of-range index returns ''.
    """
    if not images or idx < 0 or idx >= len(images):
        return ""
    return iiif_full((images[idx] or {}).get("url"), width)


# ---------------------------------------------------------------------------
# Plan 02 — four-source known-joins data layer (pure, testable)
# ---------------------------------------------------------------------------


def normalize_join_source(j: dict) -> str:
    """Return the canonical source string for a join dict.

    'source' key wins if non-empty.  Falls back to 'user' if is_local=True.
    Returns '' for unrecognised/missing source.
    """
    s = (j.get("source") or "").strip()
    if s:
        return s
    return "user" if j.get("is_local") else ""


def _other_member_of(j: dict, anchor_sid: str, anchor_shelf: str):
    """Return (other_sid, other_shelf) — the member of a join dict that is NOT the anchor.

    CODEX-VERIFIED must-fix #5: resolve by sys_id FIRST (the join dict carries
    document_id_a/_b), falling back to shelfmark when ids are absent (PGP carries
    document_id_b; FJMS we add it; the transitive-closure user joins carry
    document_id_a/_b; community has neither -> shelfmark only).
    """
    a_sid = j.get("document_id_a") or ""
    b_sid = j.get("document_id_b") or ""
    fa = j.get("fragment_a") or ""
    fb = j.get("fragment_b") or ""
    anc_sid = (anchor_sid or "").strip()
    anc_shelf = (anchor_shelf or "").upper().strip()
    # sys_id-first
    if anc_sid and (a_sid or b_sid):
        if a_sid == anc_sid:
            return b_sid, fb
        if b_sid == anc_sid:
            return a_sid, fa
    # shelfmark fallback
    if fa.upper().strip() == anc_shelf:
        return b_sid, fb
    if fb.upper().strip() == anc_shelf:
        return a_sid, fa
    # neither side matches the anchor (transitive edge not touching the anchor):
    # surface fragment_b's member by default (caller dedups by pair, so both ends
    # still appear via their own incident edges).
    return b_sid, fb


def build_known_join_rows(user_joins, pgp_joins, fjms_joins, community_joins,
                           anchor_sid, anchor_shelf, meta_mgr=None):
    """Merge the four join lists (dedup by pair) and return ONE ROW PER CONNECTED MEMBER
    (the non-anchor fragment of each unique pair).

    Each row: {fragment_a, fragment_b, source, other_sid, other_shelf}.
    Call order = badge priority: PGP > FJMS > user > community (first source wins on dedup).
    must-fix #5: member rows (not per-edge), sys_id-first 'other' resolution.
    """
    merged = dedup_join_rows([
        pgp_joins or [],
        fjms_joins or [],
        user_joins or [],
        community_joins or [],
    ])
    rows = []
    seen_members = set()
    for j in merged:
        other_sid, other_shelf = _other_member_of(j, anchor_sid, anchor_shelf)
        # resolve a missing sys_id from shelfmark (community rows lack ids)
        if not other_sid and other_shelf and meta_mgr is not None:
            try:
                other_sid = (meta_mgr.resolve_system_by_shelfmark(other_shelf) or {}).get("sys_id") or ""
            except Exception:
                other_sid = ""
        # one row per unique connected MEMBER (by sys_id when known, else by shelfmark)
        member_key = (other_sid or "", (other_shelf or "").upper().strip())
        if member_key in seen_members:
            continue
        seen_members.add(member_key)
        rows.append({
            "fragment_a": j.get("fragment_a") or "",
            "fragment_b": j.get("fragment_b") or "",
            "source": normalize_join_source(j),
            "other_sid": other_sid or "",
            "other_shelf": other_shelf or "",
        })
    return rows


# ---------------------------------------------------------------------------
# Plan 02 — Qt imports and QThread workers.
# These are placed BELOW the pure helpers so the module can still be imported
# headlessly (the Qt imports themselves are safe, but worker subclasses require
# QApplication to be running only when instantiated, not at import time).
# ---------------------------------------------------------------------------

try:
    from PyQt6.QtWidgets import (
        QDialog, QHBoxLayout, QVBoxLayout, QLabel, QPushButton, QScrollArea,
        QSplitter, QTextBrowser, QWidget, QLineEdit, QInputDialog, QMessageBox,
    )
    from PyQt6.QtCore import Qt, QThread, pyqtSignal
    from PyQt6.QtGui import QPalette, QPixmap, QImage
    from desktop.image_loader import ImageLoaderThread
    from desktop.widgets.line_number_text_edit import apply_line_numbered_text
    _QT_AVAILABLE = True
except ImportError:
    _QT_AVAILABLE = False


# ---------------------------------------------------------------------------
# _AnchorLoadWorker — loads anchor metadata + folio text off the UI thread.
# Carries a generation token (must-fix #7) to allow latest-wins semantics.
# ---------------------------------------------------------------------------

if _QT_AVAILABLE:
    class _AnchorLoadWorker(QThread):
        """Load anchor image list (enrich_metadata route) + folio text.

        Signal: done(int gen, dict out)
        out keys: images, text, total, meta, page, initial
        """
        done = pyqtSignal(int, dict)

        def __init__(self, wb, gen: int, sys_id: str, page, initial: bool = False):
            super().__init__()
            self.wb = wb
            self._gen = gen
            self.sys_id = sys_id
            self.page = page
            self.initial = initial

        def cancel(self):
            """Best-effort cancel (gen token is the real correctness guard)."""
            pass

        def run(self):
            out = {
                "page": self.page,
                "initial": self.initial,
                "images": [],
                "text": "",
                "total": None,
                "meta": {},
            }
            try:
                meta = self.wb.meta_mgr.enrich_metadata(self.sys_id) or {}
                # must-fix #4: use meta.get("images") — the ALREADY-PRIORITIZED list
                # (ext-FIRST: enrich_metadata sets images = images_ext if images_ext else images_nli).
                # Do NOT pick the sub-lists (images_nli / images_ext) yourself — use "images" directly.
                out["images"] = meta.get("images") or []
                out["meta"] = meta
            except Exception:
                out["images"] = []
                out["meta"] = {}
            try:
                bp = self.wb.searcher.get_browse_page(self.sys_id, self.page) or {}
                out["text"] = bp.get("text", "") or ""
                out["total"] = bp.get("total_pages")
            except Exception:
                pass
            self.done.emit(self._gen, out)

    class _PageTextWorker(QThread):
        """Fetch one folio's transcription text for the anchor (off-thread).

        Signal: done(int gen, str text)
        """
        done = pyqtSignal(int, str)

        def __init__(self, wb, gen: int, sid: str, p):
            super().__init__()
            self.wb = wb
            self._gen = gen
            self.sid = sid
            self.p = p

        def cancel(self):
            """Best-effort cancel (gen token is the real correctness guard)."""
            pass

        def run(self):
            txt = ""
            try:
                txt = (self.wb.searcher.get_browse_page(self.sid, self.p) or {}).get("text", "") or ""
            except Exception:
                txt = ""
            self.done.emit(self._gen, txt)

    class _KnownJoinsLoadWorker(QThread):
        """Off-UI-thread FOUR-source known-joins loader.

        Loads: user joins (JoinsManager), PGP joins (document_service),
               FJMS joins (fjms_service), community joins (corrections_client).
        Signal: done(int gen, list rows)
        Each row: {fragment_a, fragment_b, source, other_sid, other_shelf}
        must-fix #6: community fetch is hasattr-guarded (REST client lacks the method).
        must-fix #7: carries and emits a generation token.
        """
        done = pyqtSignal(int, list)

        def __init__(self, wb, gen: int, anchor_sid: str, anchor_shelf: str):
            super().__init__()
            self.wb = wb
            self._gen = gen
            self.anchor_sid = anchor_sid
            self.anchor_shelf = anchor_shelf

        def cancel(self):
            """Best-effort cancel."""
            pass

        def run(self):
            anchor_sid = self.anchor_sid
            anchor_shelf = self.anchor_shelf

            # 1. User / transitive joins via JoinsManager
            # CODEX-VERIFIED: get_connected_fragments_by_id returns a DICT
            user_joins = []
            try:
                cached = (self.wb.joins_mgr.get_connected_fragments_by_id(anchor_sid) or {})
                user_joins = cached.get("joins", []) or []
            except Exception:
                user_joins = []

            # 2. PGP joins via shared.document_service (mirror corrections_ui _get_pgp_joins)
            pgp_joins = []
            try:
                from shared.document_service import get_document_for_fragment, get_fragments_for_document
                pgp_doc = get_document_for_fragment(anchor_sid) or {}
                pgpid = pgp_doc.get("pgpid")
                if pgpid:
                    frags = get_fragments_for_document(pgpid) or []
                    unique = {f.get("sys_id") for f in frags if f.get("sys_id")}
                    if len(unique) > 1:
                        for f in frags:
                            f_sid = f.get("sys_id") or ""
                            f_shelf = f.get("shelfmark") or ""
                            if not f_shelf or f_sid == anchor_sid:
                                continue
                            pgp_joins.append({
                                "fragment_a": anchor_shelf, "fragment_b": f_shelf,
                                "source": "PGP",
                                "document_id_a": anchor_sid, "document_id_b": f_sid,
                                "relationship_type": "same_composition",
                                "notes": f"PGP Document #{pgpid}",
                            })
            except Exception:
                pgp_joins = []

            # 3. FJMS joins via shared.fjms_service (mirror _get_fjms_joins; ADD document_id_b=alma_id)
            fjms_joins = []
            try:
                from shared.fjms_service import get_fjms_service
                svc = get_fjms_service()
                if svc and svc.is_available():
                    members = svc.get_join_group(anchor_sid) or []
                    for member in members:
                        alma = member.get("alma_id") or ""
                        if not alma or alma == anchor_sid:
                            continue
                        shelf = alma
                        try:
                            s, _ = self.wb.meta_mgr.get_meta_for_id(alma)
                            if s and s != "Unknown":
                                shelf = s
                        except Exception:
                            pass
                        fjms_joins.append({
                            "fragment_a": anchor_shelf, "fragment_b": shelf,
                            "source": "FJMS",
                            "document_id_a": anchor_sid, "document_id_b": alma,
                            "relationship_type": ", ".join(member.get("join_types", []) or []),
                        })
            except Exception:
                fjms_joins = []

            # 4. COMMUNITY puzzle joins via corrections_client (SC#3)
            # must-fix #6: ONLY SupabaseCorrectionsClient has this method; guard with hasattr.
            community_joins = []
            try:
                client = getattr(self.wb, "corrections_client", None)
                if client is not None and hasattr(client, "get_published_joins_for_fragment"):
                    published = client.get_published_joins_for_fragment(anchor_sid) or []
                    anchor_up = (anchor_shelf or "").upper().strip()
                    for pj in published:
                        for sm in (pj.get("shelfmarks") or []):
                            if (sm or "").upper().strip() == anchor_up:
                                continue  # skip the anchor itself
                            community_joins.append({
                                "fragment_a": anchor_shelf, "fragment_b": sm,
                                "source": "community",
                                "document_id_a": anchor_sid, "document_id_b": "",
                            })
            except Exception:
                community_joins = []

            rows = build_known_join_rows(
                user_joins, pgp_joins, fjms_joins, community_joins,
                anchor_sid, anchor_shelf, self.wb.meta_mgr,
            )
            self.done.emit(self._gen, rows)

    class ThumbBatchWorker(QThread):
        """Batched thumbnail fetch — ONE worker thread for all known-join rows (D-10).

        Emits QImage (NOT QPixmap — must-fix #8: QPixmap must be constructed on GUI thread).
        Signal: resolved(int gen, int row_index, object qimage_or_none)
        """
        resolved = pyqtSignal(int, int, object)

        def __init__(self, wb, gen: int, sids: list):
            super().__init__()
            self.wb = wb
            self._gen = gen
            self.sids = list(sids)
            self._cancel = False

        def cancel(self):
            self._cancel = True

        def run(self):
            import requests
            import genizah_core

            for i, sid in enumerate(self.sids):
                if self._cancel:
                    return
                qimg = None
                try:
                    if sid:
                        url = self.wb.meta_mgr.get_thumbnail(sid, size=320)
                        if url:
                            resp = requests.get(
                                url,
                                headers=genizah_core.Config.HTTP_HEADERS,
                                timeout=5,
                                verify=False,
                            )
                            if resp.status_code == 200:
                                # QImage on a worker thread is OK (must-fix #8: NOT QPixmap)
                                candidate = QImage()
                                candidate.loadFromData(resp.content)
                                if not candidate.isNull():
                                    qimg = candidate
                except Exception:
                    qimg = None
                self.resolved.emit(self._gen, i, qimg)

    # -------------------------------------------------------------------------
    # JoinWorkbenchWindow — the main modeless QDialog shell (Plan 02, JWB-01).
    # -------------------------------------------------------------------------

    class JoinWorkbenchWindow(QDialog):
        """Modeless anchor-pinned join-hunting shell.

        D-01: modeless (setModal(False)); opened with show(), not exec().
        D-02: single reusable instance; set_anchor(res) re-anchors without spawning a second window.
        JWB-01: window exposes set_anchor(res) and _reload_known_joins() as public API.
        """

        def __init__(self, parent, app):
            super().__init__(parent)
            self.setModal(False)
            self._app = app
            self.meta_mgr = app.meta_mgr
            self.searcher = app.searcher
            self.joins_mgr = app.joins_mgr
            self.corrections_client = getattr(app, "corrections_client", None)

            # Generation token — monotonically increasing, bumped on every set_anchor.
            # must-fix #7: every worker carries a copy and every slot drops stale results.
            self._gen = 0

            # Anchor state
            self._anchor_sid = None
            self._anchor_res = None
            self._anchor_images = []
            self._anchor_idx = 0
            self._zoom = 1.0
            self._anchor_full_pix = None

            # Worker refs (best-effort cancel on re-anchor / close)
            self._anchor_worker = None
            self._page_text_worker = None
            self._img_loader = None
            self._thumb_worker = None
            self._known_joins_worker = None

            # Known-join row thumbnail labels (list, indexed by row; set in _build_join_row)
            self._join_thumb_labels = []

            # Dark-mode detection — set once in __init__ (pattern from result_dialog.py:562)
            palette = self.palette()
            self.is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128

            self._init_ui()
            self.setWindowTitle(tr("Join Workbench"))
            self.setMinimumSize(900, 680)
            self.resize(1000, 720)

        # ------------------------------------------------------------------
        # UI construction
        # ------------------------------------------------------------------

        def _init_ui(self):
            root = QHBoxLayout(self)
            root.setContentsMargins(0, 0, 0, 0)
            splitter = QSplitter(Qt.Orientation.Horizontal)
            splitter.addWidget(self._build_anchor_pane())
            splitter.addWidget(self._build_right_pane())
            splitter.setSizes([420, 540])
            root.addWidget(splitter)

        def _build_anchor_pane(self) -> QWidget:
            """Build the left anchor pane (top-to-bottom per UI-SPEC Layout Contract)."""
            pane = QWidget()
            layout = QVBoxLayout(pane)
            layout.setContentsMargins(8, 8, 8, 8)
            layout.setSpacing(6)

            # 1. ANCHOR tag strip
            anchor_tag = QLabel(tr("ANCHOR"))
            anchor_tag.setStyleSheet("font-weight:bold;font-size:11px;color:#14b8a6;")
            layout.addWidget(anchor_tag)

            # 2. Shelfmark label
            self.anchor_shelf = QLabel()
            self.anchor_shelf.setStyleSheet("font-weight:bold;font-size:15px;")
            self.anchor_shelf.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse
            )
            layout.addWidget(self.anchor_shelf)

            # 3. Meta-brief line
            self.anchor_meta = QLabel()
            self.anchor_meta.setStyleSheet("font-size:11px;color:#94a3b8;")
            self.anchor_meta.setWordWrap(True)
            layout.addWidget(self.anchor_meta)

            # 4. Toolbar row (zoom + folio nav)
            toolbar = QHBoxLayout()
            toolbar.setSpacing(4)

            btn_zoom_out = QPushButton("-")
            btn_zoom_out.setFixedWidth(30)
            btn_zoom_out.setToolTip("Zoom out")
            btn_zoom_out.setAccessibleName("Zoom out")
            btn_zoom_out.clicked.connect(self._zoom_out)
            toolbar.addWidget(btn_zoom_out)

            btn_zoom_in = QPushButton("+")
            btn_zoom_in.setFixedWidth(30)
            btn_zoom_in.setToolTip("Zoom in")
            btn_zoom_in.setAccessibleName("Zoom in")
            btn_zoom_in.clicked.connect(self._zoom_in)
            toolbar.addWidget(btn_zoom_in)

            toolbar.addStretch()

            self.btn_folio_prev = QPushButton("◄")
            self.btn_folio_prev.setFixedWidth(30)
            self.btn_folio_prev.setAccessibleName("Previous folio")
            self.btn_folio_prev.clicked.connect(self._folio_prev)
            toolbar.addWidget(self.btn_folio_prev)

            self.folio_counter = QLabel("")
            self.folio_counter.setStyleSheet("font-size:11px;color:#94a3b8;")
            toolbar.addWidget(self.folio_counter)

            self.btn_folio_next = QPushButton("►")
            self.btn_folio_next.setFixedWidth(30)
            self.btn_folio_next.setAccessibleName("Next folio")
            self.btn_folio_next.clicked.connect(self._folio_next)
            toolbar.addWidget(self.btn_folio_next)

            layout.addLayout(toolbar)

            # 5. Image area (scroll + label)
            self.anchor_img_scroll = QScrollArea()
            self.anchor_img_scroll.setWidgetResizable(True)
            img_bg = "#374151" if self.is_dark else "#e2e8f0"
            self.anchor_img_label = QLabel()
            self.anchor_img_label.setMinimumSize(360, 280)
            self.anchor_img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.anchor_img_label.setStyleSheet(f"background:{img_bg};")
            self.anchor_img_scroll.setWidget(self.anchor_img_label)
            layout.addWidget(self.anchor_img_scroll, 1)

            # 6. Transcription text browser
            self.anchor_text_browser = QTextBrowser()
            layout.addWidget(self.anchor_text_browser, 1)

            return pane

        def _build_right_pane(self) -> QWidget:
            """Build the right pane: cold-start row + anchor action-row + known-joins panel."""
            pane = QWidget()
            layout = QVBoxLayout(pane)
            layout.setContentsMargins(8, 8, 8, 8)
            layout.setSpacing(6)

            # 1. Cold-start row (Entry point #3, D-03)
            coldstart_row = QHBoxLayout()
            self.coldstart_input = QLineEdit()
            self.coldstart_input.setPlaceholderText(tr("Enter shelfmark…"))
            btn_open = QPushButton(tr("Open fragment"))
            btn_open.clicked.connect(self._cold_start_open)
            coldstart_row.addWidget(self.coldstart_input, 1)
            coldstart_row.addWidget(btn_open)
            layout.addLayout(coldstart_row)

            # 2. Anchor action-row (always visible, D-13)
            anchor_action_row = QHBoxLayout()
            anchor_action_row.setSpacing(4)

            self.btn_anchor_browse = QPushButton("\U0001f4d6")
            self.btn_anchor_browse.setFixedWidth(28)
            self.btn_anchor_browse.setToolTip(tr("Browse manuscript"))
            self.btn_anchor_browse.setAccessibleName(tr("Browse manuscript"))
            self.btn_anchor_browse.clicked.connect(self._anchor_browse)
            anchor_action_row.addWidget(self.btn_anchor_browse)

            self.btn_anchor_puzzle = QPushButton("\U0001f9e9")
            self.btn_anchor_puzzle.setFixedWidth(28)
            self.btn_anchor_puzzle.setToolTip(tr("Add to Puzzle"))
            self.btn_anchor_puzzle.setAccessibleName(tr("Add to Puzzle"))
            self.btn_anchor_puzzle.clicked.connect(self._anchor_puzzle)
            anchor_action_row.addWidget(self.btn_anchor_puzzle)

            self.btn_anchor_list = QPushButton("\U0001f4cb")
            self.btn_anchor_list.setFixedWidth(28)
            self.btn_anchor_list.setToolTip(tr("Add to List"))
            self.btn_anchor_list.setAccessibleName(tr("Add to List"))
            self.btn_anchor_list.clicked.connect(self._anchor_add_to_list)
            anchor_action_row.addWidget(self.btn_anchor_list)

            self.btn_anchor_add_join = QPushButton("\U0001f517")
            self.btn_anchor_add_join.setFixedWidth(28)
            self.btn_anchor_add_join.setToolTip(tr("Add as Join"))
            self.btn_anchor_add_join.setAccessibleName(tr("Add as Join"))
            self.btn_anchor_add_join.clicked.connect(self._on_add_as_join)
            anchor_action_row.addWidget(self.btn_anchor_add_join)

            anchor_action_row.addStretch()
            layout.addLayout(anchor_action_row)

            # 3. Known-joins panel (hidden when empty, D-11)
            self.joins_panel = QWidget()
            joins_panel_layout = QVBoxLayout(self.joins_panel)
            joins_panel_layout.setContentsMargins(0, 0, 0, 0)
            joins_panel_layout.setSpacing(4)

            header_color = "#14b8a6" if self.is_dark else "#0f766e"
            self.joins_header = QLabel()
            self.joins_header.setStyleSheet(
                f"font-weight:bold;font-size:11px;color:{header_color};"
            )
            joins_panel_layout.addWidget(self.joins_header)

            # Scrollable rows container
            rows_scroll = QScrollArea()
            rows_scroll.setWidgetResizable(True)
            rows_scroll.setMaximumHeight(320)
            rows_container = QWidget()
            self.joins_rows_layout = QVBoxLayout(rows_container)
            self.joins_rows_layout.setContentsMargins(0, 0, 0, 0)
            self.joins_rows_layout.setSpacing(2)
            self.joins_rows_layout.addStretch()
            rows_scroll.setWidget(rows_container)
            joins_panel_layout.addWidget(rows_scroll)

            self.joins_panel.setVisible(False)
            layout.addWidget(self.joins_panel)

            layout.addStretch()
            return pane

        # ------------------------------------------------------------------
        # Anchor loading / set_anchor
        # ------------------------------------------------------------------

        def set_anchor(self, res: dict):
            """Set or replace the anchor fragment.

            D-02: single reusable instance — re-anchors without spawning a second window.
            must-fix #7: bumps generation token FIRST so in-flight workers become stale.
            """
            # Bump generation BEFORE cancelling so any in-flight worker is stale immediately
            self._gen += 1
            gen = self._gen

            self._cancel_workers()
            self._anchor_res = dict(res)
            self._anchor_sid = r_sid(res)

            try:
                self.anchor_shelf.setText(r_shelf(res))
            except RuntimeError:
                pass

            page = (res.get("display") or {}).get("img") or 1
            try:
                page = max(1, int(page))
            except (TypeError, ValueError):
                page = 1

            self._anchor_idx = max(0, page - 1)
            self._zoom = 1.0
            self._anchor_full_pix = None
            self._anchor_images = []

            try:
                self.anchor_img_label.setText("...")
            except RuntimeError:
                pass

            self._start_anchor_load(gen, page=page, initial=True)
            self._reload_known_joins(gen)

        def _start_anchor_load(self, gen: int, page, initial: bool = False):
            """Start the _AnchorLoadWorker for the current anchor."""
            self._anchor_worker = _AnchorLoadWorker(
                self, gen, self._anchor_sid, page, initial
            )
            self._anchor_worker.done.connect(self._on_anchor_loaded)
            self._anchor_worker.start()

        def _on_anchor_loaded(self, gen: int, out: dict):
            """Handle anchor metadata + text result. Drop if generation is stale."""
            if gen != self._gen:
                return  # must-fix #7: drop stale result
            self._anchor_images = out.get("images") or []
            self._update_folio_controls()

            meta = out.get("meta") or {}
            try:
                self.anchor_meta.setText(
                    meta_brief(meta, r_shelf(self._anchor_res) if self._anchor_res else "")
                )
            except RuntimeError:
                pass

            text = out.get("text") or ""
            try:
                apply_line_numbered_text(
                    self.anchor_text_browser,
                    htmlify(text, None),
                    source_text=text,
                    is_html=True,
                )
            except RuntimeError:
                pass

            self._load_current_image()

        def _load_current_image(self):
            """Start an ImageLoaderThread for the current anchor image index."""
            url = _image_url_for_idx(self._anchor_images, self._anchor_idx, 2000)
            if not url:
                try:
                    self.anchor_img_label.setText(tr("No image"))
                except RuntimeError:
                    pass
                return

            # Cancel old loader
            if self._img_loader is not None:
                try:
                    self._img_loader.cancel()
                    self._img_loader.quit()
                except Exception:
                    pass

            gen = self._gen
            self._img_loader = ImageLoaderThread(url)
            # Capture gen in closure so the lambda drops stale results (must-fix #7)
            self._img_loader.image_loaded.connect(
                lambda qi, g=gen: self._on_img(g, qi)
            )
            self._img_loader.load_failed.connect(
                lambda g=gen: self._on_img_failed(g)
            )
            self._img_loader.start()
            self._update_folio_controls()

        def _update_folio_controls(self):
            """Update folio counter label and prev/next button enabled state."""
            total = len(self._anchor_images)
            counter_text = f"{self._anchor_idx + 1}/{total}" if total else ""
            try:
                self.folio_counter.setText(counter_text)
                self.btn_folio_prev.setEnabled(self._anchor_idx > 0)
                self.btn_folio_next.setEnabled(self._anchor_idx < total - 1)
            except RuntimeError:
                pass

        def _on_img(self, gen: int, qimage: QImage):
            """Handle image_loaded signal. Drop if generation is stale."""
            if gen != self._gen:
                return  # must-fix #7
            self._anchor_full_pix = QPixmap.fromImage(qimage)
            self._apply_zoom()

        def _on_img_failed(self, gen: int):
            """Handle load_failed signal. Drop if generation is stale."""
            if gen != self._gen:
                return  # must-fix #7
            try:
                self.anchor_img_label.setText(tr("No image"))
            except RuntimeError:
                pass

        def _apply_zoom(self):
            """Rescale the cached full pixmap by the current zoom factor."""
            if not self._anchor_full_pix:
                return
            scaled = self._anchor_full_pix.scaled(
                int(self._anchor_full_pix.width() * self._zoom),
                int(self._anchor_full_pix.height() * self._zoom),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            try:
                self.anchor_img_label.setPixmap(scaled)
            except RuntimeError:
                pass

        def _zoom_in(self):
            self._zoom = _clamp_zoom(self._zoom * 1.25)
            self._apply_zoom()

        def _zoom_out(self):
            self._zoom = _clamp_zoom(self._zoom / 1.25)
            self._apply_zoom()

        def _folio_prev(self):
            """Navigate to the previous folio image (same anchor, D-07)."""
            if self._anchor_idx <= 0:
                return
            self._anchor_idx -= 1
            # Folio nav does NOT bump self._gen (stays within same anchor)
            gen = self._gen
            self._page_text_worker = _PageTextWorker(
                self, gen, self._anchor_sid, self._anchor_idx + 1
            )
            self._page_text_worker.done.connect(self._on_page_text)
            self._page_text_worker.start()
            self._load_current_image()

        def _folio_next(self):
            """Navigate to the next folio image (same anchor, D-07)."""
            if self._anchor_idx >= len(self._anchor_images) - 1:
                return
            self._anchor_idx += 1
            gen = self._gen
            self._page_text_worker = _PageTextWorker(
                self, gen, self._anchor_sid, self._anchor_idx + 1
            )
            self._page_text_worker.done.connect(self._on_page_text)
            self._page_text_worker.start()
            self._load_current_image()

        def _on_page_text(self, gen: int, text: str):
            """Handle folio page text result. Drop if stale."""
            if gen != self._gen:
                return  # must-fix #7
            try:
                apply_line_numbered_text(
                    self.anchor_text_browser,
                    htmlify(text, None),
                    source_text=text,
                    is_html=True,
                )
            except RuntimeError:
                pass

        def _cancel_workers(self):
            """Best-effort cancel all in-flight workers.

            cancel()/quit() cannot stop a blocking run(), so the gen token is
            the real correctness guard. This is cleanup / resource management.
            """
            for w in (
                self._anchor_worker,
                self._page_text_worker,
                self._img_loader,
                self._thumb_worker,
                self._known_joins_worker,
            ):
                if w is None:
                    continue
                try:
                    w.cancel()
                except Exception:
                    pass
                try:
                    if w.isRunning():
                        w.quit()
                except Exception:
                    pass

        def closeEvent(self, event):
            """Invalidate generation + cancel workers on close."""
            self._gen += 1  # must-fix #7: invalidate any in-flight workers
            self._cancel_workers()
            super().closeEvent(event)

        # ------------------------------------------------------------------
        # Known-joins panel
        # ------------------------------------------------------------------

        def _reload_known_joins(self, gen=None):
            """Start a _KnownJoinsLoadWorker for the current anchor.

            Called by set_anchor (with the anchor's gen) and by _on_add_as_join
            (with self._gen after dialog closes).
            """
            if not self._anchor_sid:
                return
            if gen is None:
                gen = self._gen

            # Cancel existing known-joins worker
            if self._known_joins_worker is not None:
                try:
                    self._known_joins_worker.cancel()
                    if self._known_joins_worker.isRunning():
                        self._known_joins_worker.quit()
                except Exception:
                    pass

            shelf = r_shelf(self._anchor_res) if self._anchor_res else ""
            self._known_joins_worker = _KnownJoinsLoadWorker(
                self, gen, self._anchor_sid, shelf
            )
            self._known_joins_worker.done.connect(self._on_known_joins_loaded)
            self._known_joins_worker.start()

        def _on_known_joins_loaded(self, gen: int, rows: list):
            """Handle known-joins result. Drop if stale. Render rows + fire ThumbBatchWorker."""
            if gen != self._gen:
                return  # must-fix #7

            # Clear existing rows (remove all but the trailing stretch)
            while self.joins_rows_layout.count() > 1:
                item = self.joins_rows_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

            count = len(rows)
            # D-11: panel hidden entirely when empty
            try:
                self.joins_panel.setVisible(count > 0)
            except RuntimeError:
                return

            try:
                self.joins_header.setText(f"{tr('Known Joins')} ({count})")
            except RuntimeError:
                return

            self._join_thumb_labels = []
            sids = []
            for row in rows:
                row_widget = self._build_join_row(row)
                self.joins_rows_layout.insertWidget(
                    self.joins_rows_layout.count() - 1, row_widget
                )
                sids.append(row.get("other_sid") or "")

            # Fire ONE ThumbBatchWorker for all rows (D-10 batch constraint)
            if sids:
                if self._thumb_worker is not None:
                    try:
                        self._thumb_worker.cancel()
                        if self._thumb_worker.isRunning():
                            self._thumb_worker.quit()
                    except Exception:
                        pass
                self._thumb_worker = ThumbBatchWorker(self, gen, sids)
                self._thumb_worker.resolved.connect(self._on_thumb_resolved)
                self._thumb_worker.start()

        def _on_thumb_resolved(self, gen: int, idx: int, qimg):
            """Handle a batched thumbnail result (from ThumbBatchWorker).

            Drop if stale. Convert QImage -> QPixmap ON THE UI THREAD (must-fix #8).
            """
            if gen != self._gen:
                return  # must-fix #7
            if idx < 0 or idx >= len(self._join_thumb_labels):
                return
            label = self._join_thumb_labels[idx]
            if qimg is not None:
                try:
                    # must-fix #8: QPixmap.fromImage on the UI thread
                    pixmap = QPixmap.fromImage(qimg)
                    scaled = pixmap.scaled(
                        48, 48,
                        Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation,
                    )
                    label.setPixmap(scaled)
                except RuntimeError:
                    pass

        def _build_join_row(self, row: dict) -> QWidget:
            """Build one per-member known-join row widget."""
            thumb_bg = "#374151" if self.is_dark else "#e2e8f0"
            other_sid = row.get("other_sid") or ""
            other_shelf = row.get("other_shelf") or ""
            source = row.get("source") or ""

            widget = QWidget()
            h = QHBoxLayout(widget)
            h.setSpacing(4)
            h.setContentsMargins(4, 2, 4, 2)

            # Thumbnail label (48x48, filled by ThumbBatchWorker slot)
            thumb_label = QLabel("(no img)")
            thumb_label.setFixedSize(48, 48)
            thumb_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            thumb_label.setStyleSheet(f"background:{thumb_bg};font-size:9px;")
            h.addWidget(thumb_label)
            self._join_thumb_labels.append(thumb_label)

            # Source badge
            badge_label, badge_color = badge_for_source(source, self.is_dark)
            badge = QLabel(badge_label)
            badge.setFixedWidth(60)
            badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
            badge.setStyleSheet(
                f"background:{badge_color};color:white;border-radius:3px;"
                f"font-size:10px;padding:1px 4px;"
            )
            h.addWidget(badge)

            # Shelfmark + title stack
            text_col = QVBoxLayout()
            text_col.setSpacing(1)

            shelf_label = QLabel(other_shelf or "?")
            shelf_label.setStyleSheet("font-size:12px;")
            shelf_label.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse
            )
            text_col.addWidget(shelf_label)

            title_text = (row.get("fragment_b") or "")[:60]
            if len(row.get("fragment_b") or "") > 60:
                title_text += "…"
            title_label = QLabel(title_text)
            title_label.setStyleSheet("font-size:10px;color:#94a3b8;")
            title_label.setWordWrap(True)
            text_col.addWidget(title_label)

            h.addLayout(text_col, 1)

            # Per-row action buttons
            actions = QHBoxLayout()
            actions.setSpacing(2)

            btn_browse = QPushButton("\U0001f4d6")
            btn_browse.setFixedWidth(28)
            btn_browse.setToolTip(tr("Browse manuscript"))
            btn_browse.setAccessibleName(tr("Browse manuscript"))
            member_res = {"display": {"id": other_sid, "shelfmark": other_shelf}}
            btn_browse.clicked.connect(
                lambda checked=False, r=member_res: self._app.open_result_in_browse_from_table(r)
            )
            actions.addWidget(btn_browse)

            btn_puzzle = QPushButton("\U0001f9e9")
            btn_puzzle.setFixedWidth(28)
            btn_puzzle.setToolTip(tr("Add to Puzzle"))
            btn_puzzle.setAccessibleName(tr("Add to Puzzle"))
            btn_puzzle.clicked.connect(
                lambda checked=False, sid=other_sid: self._app.open_anchor_in_puzzle(sid)
            )
            actions.addWidget(btn_puzzle)

            btn_list = QPushButton("\U0001f4cb")
            btn_list.setFixedWidth(28)
            btn_list.setToolTip(tr("Add to List"))
            btn_list.setAccessibleName(tr("Add to List"))
            btn_list.clicked.connect(
                lambda checked=False, sid=other_sid, btn=btn_list: self._app.show_add_to_list_menu(
                    [{"sys_id": sid, "fl_id": "", "img": 1}],
                    source="join_workbench",
                    anchor_widget=btn,
                )
            )
            actions.addWidget(btn_list)

            btn_reanchor = QPushButton("⚓")
            btn_reanchor.setFixedWidth(28)
            btn_reanchor.setToolTip(tr("Make anchor"))
            btn_reanchor.setAccessibleName(tr("Make anchor"))
            btn_reanchor.setStyleSheet(
                "border:1px solid #14b8a6;background:transparent;border-radius:3px;"
            )
            btn_reanchor.clicked.connect(
                lambda checked=False, sid=other_sid, shelf=other_shelf: self.set_anchor(
                    {"display": {"id": sid, "shelfmark": shelf, "img": 1},
                     "uid": f"{sid}_P001"}
                )
            )
            actions.addWidget(btn_reanchor)

            h.addLayout(actions)
            return widget

        # ------------------------------------------------------------------
        # Anchor action-row callbacks
        # ------------------------------------------------------------------

        def _anchor_browse(self):
            """Open the current anchor in Browse via public host method (SC#5)."""
            if self._anchor_res:
                self._app.open_result_in_browse_from_table(self._anchor_res)

        def _anchor_puzzle(self):
            """Open the current anchor in Puzzle via public host method (SC#5)."""
            if self._anchor_sid:
                self._app.open_anchor_in_puzzle(self._anchor_sid)

        def _anchor_add_to_list(self):
            """Add the current anchor to a List via public host method (SC#5)."""
            if not self._anchor_sid:
                return
            fl_from_idx = ""
            self._app.show_add_to_list_menu(
                [{"sys_id": self._anchor_sid, "fl_id": fl_from_idx, "img": self._anchor_idx + 1}],
                source="join_workbench",
                anchor_widget=self.btn_anchor_list,
            )

        def _on_add_as_join(self):
            """Open JoinsDialog pre-filled with the anchor as Fragment A.

            exec() blocks until dialog closes; refresh known joins after (SC#4, Pitfall 3).
            """
            if not self._anchor_sid or not self._anchor_res:
                return
            self._app.open_anchor_as_join(
                self._anchor_sid, r_shelf(self._anchor_res)
            )
            # SC#4: after dialog closes (exec() is synchronous), refresh the group
            self._reload_known_joins(self._gen)

        # ------------------------------------------------------------------
        # Cold-start entry point (Entry point #3, D-03)
        # ------------------------------------------------------------------

        def _cold_start_open(self):
            """Resolve a shelfmark string and set it as the anchor.

            CODEX-VERIFIED must-fix #12: handle options length 1 / no top-level sys_id.
            """
            q = self.coldstart_input.text().strip()
            if not q:
                return
            res = self.meta_mgr.resolve_system_by_shelfmark(q) or {}
            opts = res.get("options") or []
            sid = res.get("sys_id")

            # must-fix #12: single exact match -> top-level sys_id (options == [])
            # options length 1 (lone partial) -> top-level sys_id is None; take opts[0]
            # >1 options -> show picker
            if not sid and len(opts) == 1:
                sid = (opts[0] or {}).get("sys_id")
            elif len(opts) > 1:
                items = [
                    f"{o.get('shelfmark', '')} - {o.get('title', '')}" for o in opts
                ]
                choice, ok = QInputDialog.getItem(
                    self, tr("Join Workbench"), tr("Enter shelfmark…"), items, 0, False
                )
                if not ok:
                    return
                sid = (opts[items.index(choice)] or {}).get("sys_id")

            if not sid:
                QMessageBox.warning(
                    self, tr("Join Workbench"),
                    tr("No manuscript found for '{}'").format(q),
                )
                return

            shelf = q
            try:
                s, _ = self.meta_mgr.get_meta_for_id(sid) or (q, "")
                if s:
                    shelf = s
            except Exception:
                pass

            self.set_anchor({
                "display": {"id": sid, "shelfmark": shelf, "img": 1},
                "uid": f"{sid}_P001",
            })
