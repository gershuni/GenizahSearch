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
    "puzzle_add_targets",
    # Plan 02 window class
    "JoinWorkbenchWindow",
    # Plan 03 (108-02) query builder + executor adapter
    "JoinQueryBuilder",
    "_DesktopSearchExecutor",
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
# Plan 01 — VS→Candidate adapter shim (pure, testable; no Qt)
# ---------------------------------------------------------------------------


def _normalize_vs_row(row: dict, shelfmark: str = "", title: str = "",
                      library_code: str = "") -> dict:
    """Map a get_suggestions() row to a normalize_candidate()-compatible dict.

    VS service output: {'alma_id': str, 'svm_score': float, 'rank': int}
    Key renames: alma_id -> display.id, rank -> vs_rank (normalize_candidate reads 'vs_rank').
    display.img = None -> page_of() returns None -> Candidate.page = None (VS is
    manuscript-level; RR-12 None-page guard relies on this).
    _via_vs = True -> Candidate.via_vs = True.

    Review #5: shelfmark MUST never be empty. _EnrichWorker does NOT populate
    shelfmark/title (verified join_workbench.py:1530-1598), and CandidateCard reads
    c.shelfmark with NO fallback (verified :1668) -> a blank shim shelfmark renders a
    blank bold card label. Plan 02 passes a readable shelfmark from meta_mgr.csv_bank;
    when none is available, fall back to str(alma_id) so the card is never blank.

    Named without a _vs_ prefix so the no_private AST guard (D-18) does not flag callers.
    """
    alma_id = row["alma_id"]
    return {
        "display": {
            "id": alma_id,
            "shelfmark": shelfmark or str(alma_id),   # review #5: never blank
            "title": title,
            "library_code": library_code,
            "img": None,
        },
        "uid": f"{alma_id}|vs",
        "vs_rank": row["rank"],
        "svm_score": row["svm_score"],
        "_via_vs": True,
        "full_text": "",
        "scope": "",
    }


# ---------------------------------------------------------------------------
# Plan 03 — candidate_to_result_dict adapter (pure, testable; no Qt)
# ---------------------------------------------------------------------------


def candidate_to_result_dict(c) -> dict:
    """Thin adapter: Candidate dataclass -> raw result-dict shape for Phase-107 host methods.

    Used ONLY at host-method boundaries (browse / add-to-list / image pump).
    Do NOT use this on Candidate attributes inside the pane/workers — read the
    dataclass fields directly (RR-2).
    """
    return {
        "display": {
            "id": c.sys_id,
            "shelfmark": c.shelfmark,
            "title": c.title,
            "library_code": c.library_code,
            "img": c.page,
        },
        "full_text": c.full_text,
        "snippet": c.snippet,
        "uid": c.uid,
        "highlight_pattern": c.highlight_pattern,
        "score": c.score,
        "scope": c.scope,
        "_via_other_side": c.via_other_side,
    }


# ---------------------------------------------------------------------------
# Plan 06 — pick_callback helper (pure, testable, no Qt)
# ---------------------------------------------------------------------------


def _invoke_pick(callback, c) -> bool:
    # MARKED REMOVABLE (Phase 109 G-08, D-11 one-cycle soft-retire): with the JoinsDialog pick-back
    # retired (G-08 reverses G-05), this pick-callback machinery has no live caller. RETAINED one
    # cycle as a safety net; tests (test_invoke_pick_forwards_sysid_shelfmark,
    # test_set_pick_callback_rerenders) keep it green. Removable once the parity UAT signs off.
    """Pick-mode: forward a chosen Candidate's (sys_id, shelfmark) to the JoinsDialog callback.

    Returns True and calls callback(c.sys_id, c.shelfmark) when a callback is set.
    Returns False without calling callback when callback is None.
    Named without a _vs_ prefix so the no_private AST guard (D-18) does not flag callers.
    """
    if callback is None:
        return False
    callback(c.sys_id, c.shelfmark)
    return True


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


def puzzle_add_targets(anchor_sid, member_sids):
    """Return the ordered, de-duplicated list of sys_ids to add to the puzzle.

    UAT contract: "adding to puzzle automatically adds the anchor; if the anchor
    is already there it is not added again."  The anchor is ALWAYS first and
    appears EXACTLY once, even if it also shows up among ``member_sids``.  Empty
    / falsy sids are dropped.  The puzzle canvas itself further dedups by
    (sys_id, folio_label), so re-adding across separate clicks is also safe.
    """
    out = []
    seen = set()
    for sid in [anchor_sid, *(member_sids or [])]:
        sid = (sid or "").strip()
        if sid and sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


# ---------------------------------------------------------------------------
# Feature 9 — material_display: Hebrew material terms in HE UI
# Pure helper (no Qt) so cards/table/compare/filter can all call it.
# ---------------------------------------------------------------------------

# Triage glyphs — language-neutral (avoids leaking English yes/maybe/no into HE UI)
# Round-4 UAT: ✓ / ? / ✗ (was Y / ? / N) to match the triage buttons.
_TRIAGE_GLYPH = {"yes": "✓", "maybe": "?", "no": "✗"}


def _candidate_shelf_badge(c):
    """Shared provenance-badge text + eye tooltip for a Candidate.

    Used by the candidate TABLE row and the COMPARE candidate pane so the 👁 visual-similarity
    badge (G-06) and the ⚓ self / ⇄ other-side badges render IDENTICALLY to the grid card across
    all three surfaces (round-4 UAT: eye must appear in compare + table too).

    Precedence (G-06.4): anchor-self > other-side > visual look-alike. Text-only candidates are
    unbadged. Returns (shelf_text, eye_tooltip) where eye_tooltip is the tr() tooltip for a visual
    look-alike or None.

    NB: CandidateCard keeps its own inline copy of this precedence (pinned by
    test_eye_badge_precedence_after_self_otherside) — keep the two in sync.
    """
    text = c.shelfmark or ""
    if getattr(c, "is_anchor_self", False):
        return text + tr("  ⚓ self"), None
    if getattr(c, "via_other_side", False):
        return text + tr("  ⇄ other side"), None
    if getattr(c, "via_vs", False):
        return text + "  👁", tr("visual similarity")
    return text, None

_MATERIAL_HE = {
    "paper":     "נייר",
    "parchment": "קלף",
    "papyrus":   "פפירוס",
    "vellum":    "קלף",
    "leather":   "עור",
    "cloth":     "בד",
    "mixed":     "מעורב",
}


def material_display(material: str) -> str:
    """Return the Hebrew material term in HE UI, English otherwise.

    Gated on CURRENT_LANG=='he'; unknown values are returned as-is.
    Filter VALUE stays English (only the displayed label is Hebrew).
    """
    if not material:
        return material or ""
    if CURRENT_LANG == "he":
        return _MATERIAL_HE.get(material.lower(), material)
    return material


# ---------------------------------------------------------------------------
# Plan 02 — Qt imports and QThread workers.
# These are placed BELOW the pure helpers so the module can still be imported
# headlessly (the Qt imports themselves are safe, but worker subclasses require
# QApplication to be running only when instantiated, not at import time).
# ---------------------------------------------------------------------------

try:
    from PyQt6.QtWidgets import (
        QDialog, QFrame, QGridLayout, QHBoxLayout, QVBoxLayout, QLabel, QPushButton,
        QScrollArea, QSpinBox, QSplitter, QTableWidget, QTableWidgetItem, QTextBrowser,
        QWidget, QLineEdit, QInputDialog, QMessageBox,
        QCheckBox, QMenu, QComboBox, QListWidget, QListWidgetItem,
    )
    from PyQt6.QtCore import Qt, QThread, pyqtSignal
    from PyQt6.QtGui import QPalette, QPixmap, QImage, QTextCursor, QTextBlockFormat
    from desktop.image_loader import ImageLoaderThread
    from desktop.widgets.line_number_text_edit import apply_line_numbered_text
    from gui_threads import SearchThread
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

    class _PannableScrollArea(QScrollArea):
        """QScrollArea whose content can be dragged with the mouse (hand-pan).

        UAT: when the anchor image is zoomed larger than the viewport the scholar
        drags it into view instead of fishing for scrollbars. Mouse events that
        the inner QLabel ignores propagate up to this widget.
        """

        def __init__(self, parent=None):
            super().__init__(parent)
            self._panning = False
            self._pan_anchor = None
            self.viewport().setCursor(Qt.CursorShape.OpenHandCursor)

        def mousePressEvent(self, e):
            if e.button() == Qt.MouseButton.LeftButton:
                self._panning = True
                self._pan_anchor = e.position().toPoint()
                self.viewport().setCursor(Qt.CursorShape.ClosedHandCursor)
                e.accept()
                return
            super().mousePressEvent(e)

        def mouseMoveEvent(self, e):
            if self._panning and self._pan_anchor is not None:
                pos = e.position().toPoint()
                delta = pos - self._pan_anchor
                self._pan_anchor = pos
                hbar = self.horizontalScrollBar()
                vbar = self.verticalScrollBar()
                hbar.setValue(hbar.value() - delta.x())
                vbar.setValue(vbar.value() - delta.y())
                e.accept()
                return
            super().mouseMoveEvent(e)

        def mouseReleaseEvent(self, e):
            if e.button() == Qt.MouseButton.LeftButton and self._panning:
                self._panning = False
                self.viewport().setCursor(Qt.CursorShape.OpenHandCursor)
                e.accept()
                return
            super().mouseReleaseEvent(e)

    # -------------------------------------------------------------------------
    # JoinQueryBuilder — reusable multi-row query builder widget (Plan 03/108-02).
    # Each row = a horizontal strip of single-token OR word-boxes (RR-1) with
    # PER-ROW modifiers HOISTED outside the slash-group (RR-13).
    # -------------------------------------------------------------------------

    class JoinQueryBuilder(QWidget):
        """Multi-row query builder: rows of single-token OR word-boxes.

        Each row has:
        - An ⓘ info tooltip button (typed-sign legend)
        - A horizontal strip of single-token word-boxes ("boxes_strip")
        - A [+ or] button to add OR alternatives
        - A per-row modifier indicator label (badge)
        - A compact gap QSpinBox (↓N lines to skip)
        - A per-row ⚙ gear button (opens options dialog for that row)
        - A remove button (×)

        Modifiers are PER-ROW (one mods dict per row, hoisted outside the slash-group).
        Global search options (variants/ja/flex/bidir) live in self._global_opts dict
        and are edited via the "Search options ▾" button dialog.
        build_side_query() composes a SideQuery → compose() 3-tuple.

        RR-5: allow_page_position=True (anchor side) / False (other side).
        RR-13: wildcard-prefix disabled on multi-box rows (enforced in ⚙ dialog).
        RR-14: _responsa_opts() exposes ja/flex_spacing/bidirectional for Plan 03 merge.
        """

        def __init__(self, on_search, first_hint: str,
                     allow_page_position: bool = True, parent=None):
            super().__init__(parent)
            self._on_search_cb = on_search
            self._first_hint = first_hint
            self._allow_page_position = allow_page_position

            # Global search options dict (persists even if dialog is never opened).
            # _responsa_opts() reads from here; the "Search options ▾" dialog writes here.
            self._global_opts = {
                "variants": False,
                "ja": False,
                "flex_spacing": False,
                "bidirectional": False,
            }

            # Row list: each entry is a dict with keys:
            #   end, boxes, mods, ind, start, gap, rm, widget, boxes_strip_layout
            self.rows = []

            self._init_ui()

        # ------------------------------------------------------------------
        # UI construction
        # ------------------------------------------------------------------

        def _init_ui(self):
            outer = QVBoxLayout(self)
            outer.setSpacing(2)
            outer.setContentsMargins(0, 0, 0, 0)

            # Row container — rows are inserted here by add_row()
            self._rows_box = QVBoxLayout()
            self._rows_box.setSpacing(2)
            outer.addLayout(self._rows_box)

            # NOTE: the first row is added at the END of this method (see below),
            # AFTER every widget add_row()/_update_preview() touches (page_pos and
            # especially self._preview_edit) has been constructed.

            # Controls row: [+ Add Line] [stretch] [Search options ▾]
            ctrl_row = QHBoxLayout()
            btn_add_line = QPushButton(tr("+ Add Line"))
            btn_add_line.setToolTip(tr("Add another manuscript line to the query"))
            btn_add_line.clicked.connect(self._on_add_line)
            ctrl_row.addWidget(btn_add_line)
            ctrl_row.addStretch()

            # "Search options ▾" button — opens dialog for variants/ja/flex/bidir
            self._btn_search_opts = QPushButton(tr("Search options ▾"))
            self._btn_search_opts.setToolTip(tr("Global search options (variants, Judeo-Arabic, flex spacing, bidirectional)"))
            self._btn_search_opts.clicked.connect(self._open_search_options_dialog)
            ctrl_row.addWidget(self._btn_search_opts)

            outer.addLayout(ctrl_row)

            # Page-position control (anchor side only — RR-5)
            if self._allow_page_position:
                pp_row = QHBoxLayout()
                pp_row.addWidget(QLabel(tr("Position:") + " "))
                self.page_pos = QComboBox()
                self.page_pos.setToolTip(
                    tr("Match must fall at the START or END of the page text "
                       "(like the main search). Realizes the page-anchored first/last line.")
                )
                self.page_pos.addItem(tr("page: anywhere"), None)
                self.page_pos.addItem(tr("page: start of text"), "start")
                self.page_pos.addItem(tr("page: end of text"), "end")
                self.page_pos.currentIndexChanged.connect(self._update_preview)
                pp_row.addWidget(self.page_pos)
                pp_row.addStretch()
                outer.addLayout(pp_row)
            else:
                self.page_pos = None

            # Read-only Preview row
            preview_row = QHBoxLayout()
            preview_row.addWidget(QLabel(tr("Preview:")))
            self._preview_edit = QLineEdit()
            self._preview_edit.setReadOnly(True)
            self._preview_edit.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            self._preview_edit.setPlaceholderText(
                tr("(query preview — fill in at least one word box)")
            )
            self._preview_edit.setStyleSheet(
                "font-family: 'Consolas', 'Courier New', monospace; font-size: 13px; "
                "padding: 4px 8px; background: #2d2d2d; border: 1px solid #555; "
                "border-radius: 4px; min-height: 22px; color: #94a3b8;"
            )
            preview_row.addWidget(self._preview_edit, 1)
            outer.addLayout(preview_row)

            # Add the first row LAST — now that _preview_edit and page_pos all exist,
            # add_row()'s trailing _update_preview() can run without an AttributeError.
            # The row inserts into the top-positioned _rows_box.
            self.add_row(placeholder=self._first_hint)

        def _on_add_line(self):
            self.add_row()

        # ------------------------------------------------------------------
        # Row management
        # ------------------------------------------------------------------

        def add_row(self, placeholder: str = ""):
            """Add a new row with one initial word-box."""
            if not placeholder:
                placeholder = tr("word(s) on this line…")

            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setSpacing(2)
            row_layout.setContentsMargins(0, 0, 0, 0)

            # ⓘ typed-sign legend tooltip button (D-04: tooltip only, no parsing)
            info_btn = QPushButton("ⓘ")
            info_btn.setFixedWidth(22)
            info_btn.setFlat(True)
            info_btn.setToolTip(tr(
                "Type signs directly in a word box:\n"
                "  #word — grammatical prefixes\n"
                "  word# — grammatical suffixes\n"
                "  %word — plene/defective spelling\n"
                "  *word / word* — wildcard\n"
                "  −word — exclude\n"
                "(the same options are available via the line's ⚙)"
            ))
            row_layout.addWidget(info_btn)

            # Boxes strip (horizontal layout for OR word-boxes)
            boxes_strip_widget = QWidget()
            boxes_strip_layout = QHBoxLayout(boxes_strip_widget)
            boxes_strip_layout.setSpacing(2)
            boxes_strip_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.addWidget(boxes_strip_widget, 1)

            # [+ or] button
            add_or_btn = QPushButton(tr("+ or"))
            add_or_btn.setAccessibleName(tr("Add an OR alternative to this line"))
            add_or_btn.setToolTip(
                tr("Add another single word that may appear INSTEAD (OR) on this same line")
            )
            row_layout.addWidget(add_or_btn)

            # Per-row modifier indicator label (badge — shown when mods are set)
            ind_lbl = QLabel("")
            ind_lbl.setStyleSheet("color: #14b8a6; font-size: 10px;")
            ind_lbl.setVisible(False)
            row_layout.addWidget(ind_lbl)

            # Gap spinbox (compact inline ↓N)
            gap_spin = QSpinBox()
            gap_spin.setRange(0, 40)
            gap_spin.setPrefix(tr("↓ "))
            gap_spin.setSuffix(tr(" ln"))
            gap_spin.setToolTip(
                tr("Lines to skip before the next line (0 = the very next line)")
            )
            gap_spin.setFixedWidth(72)
            row_layout.addWidget(gap_spin)

            # Per-row ⚙ gear button (opens line options dialog — step 2)
            gear_btn = QPushButton("⚙")
            gear_btn.setFixedWidth(26)
            gear_btn.setToolTip(tr("Line options (modifiers, starts/ends line)"))
            row_layout.addWidget(gear_btn)

            # Remove button
            rm_btn = QPushButton("×")
            rm_btn.setFixedWidth(24)
            rm_btn.setAccessibleName(tr("Remove row"))
            row_layout.addWidget(rm_btn)

            # Build the entry dict (start/end checkboxes are now in the ⚙ dialog but
            # we keep them as hidden QCheckBoxes on the entry so build_side_query can
            # read them — wired in the gear dialog via copy-on-OK in step 2).
            start_chk = QCheckBox()
            start_chk.setVisible(False)
            end_chk = QCheckBox()
            end_chk.setVisible(False)

            entry = {
                "end": end_chk,
                "boxes": [],
                "mods": {},
                "ind": ind_lbl,
                "start": start_chk,
                "gap": gap_spin,
                "rm": rm_btn,
                "gear": gear_btn,
                "widget": row_widget,
                "boxes_strip_layout": boxes_strip_layout,
            }

            # Wire [+ or] and remove
            add_or_btn.clicked.connect(lambda checked=False, e=entry: self.add_or_box(e))
            rm_btn.clicked.connect(lambda checked=False, e=entry: self._remove_row(e))
            gear_btn.clicked.connect(lambda checked=False, e=entry: self._open_row_options_dialog(e))

            # Add first box
            self._make_box(entry, placeholder)

            # Insert row into the UI
            self._rows_box.addWidget(row_widget)
            self.rows.append(entry)

            self._sync()
            self._update_preview()
            return entry

        def _make_box(self, entry: dict, placeholder_text: str = "") -> dict:
            """Create one single-token OR word-box and append to entry["boxes"]."""
            edit = QLineEdit()
            edit.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            edit.setPlaceholderText(placeholder_text or tr("word…"))
            edit.returnPressed.connect(self.on_enter)
            edit.textChanged.connect(self._update_preview)

            box = {"edit": edit}
            entry["boxes"].append(box)
            entry["boxes_strip_layout"].addWidget(edit)
            return box

        def add_or_box(self, entry: dict):
            """Append a new OR-alternative word-box to an existing row.

            When a second box is added, wildcard_prefix is cleared (RR-13) because
            the parser can't hoist a leading '*' onto an OR group.
            """
            new_box = self._make_box(entry, tr("or…"))
            # Add a small × button to remove this extra box
            rm_box_btn = QPushButton("×")
            rm_box_btn.setFixedWidth(18)
            rm_box_btn.setToolTip(tr("Remove this OR alternative"))
            rm_box_btn.clicked.connect(
                lambda checked=False, b=new_box, rb=rm_box_btn, e=entry:
                self._remove_box(e, b, rb)
            )
            entry["boxes_strip_layout"].addWidget(rm_box_btn)
            new_box["rm_btn"] = rm_box_btn
            # Clear stale wildcard_prefix on multi-box row (RR-13)
            if len(entry["boxes"]) > 1:
                entry["mods"]["wildcard_prefix"] = False
                self._update_row_indicator(entry)
            self._update_preview()

        def _remove_box(self, entry: dict, box: dict, rm_btn):
            """Remove an OR-alternative box (keep >= 1 box per row)."""
            if len(entry["boxes"]) <= 1:
                return
            box["edit"].setParent(None)
            box["edit"].deleteLater()
            rm_btn.setParent(None)
            rm_btn.deleteLater()
            entry["boxes"].remove(box)
            # Clear stale wildcard_prefix when row now has 1 box (RR-13)
            if len(entry["boxes"]) == 1:
                entry["mods"]["wildcard_prefix"] = False
            self._update_row_indicator(entry)
            self._update_preview()

        def _remove_row(self, entry: dict):
            """Remove a row (keep >= 1 row)."""
            if len(self.rows) <= 1:
                return
            entry["widget"].setParent(None)
            entry["widget"].deleteLater()
            self.rows.remove(entry)
            self._sync()
            self._update_preview()

        def _sync(self):
            """Hide the gap spinbox on the LAST row only (it connects to the next row)."""
            for i, e in enumerate(self.rows):
                e["gap"].setVisible(i < len(self.rows) - 1)

        # ------------------------------------------------------------------
        # "Search options ▾" dialog (global options: variants/ja/flex/bidir)
        # ------------------------------------------------------------------

        def _open_search_options_dialog(self):
            """Open a modal dialog to edit global search options (_global_opts dict)."""
            dlg = QDialog(self)
            dlg.setWindowTitle(tr("Search options"))
            lay = QVBoxLayout(dlg)
            lay.setSpacing(6)

            chk_variants = QCheckBox(tr("Expand spelling variants"))
            chk_variants.setChecked(self._global_opts["variants"])
            chk_ja = QCheckBox(tr("Judeo-Arabic"))
            chk_ja.setChecked(self._global_opts["ja"])
            chk_flex = QCheckBox(tr("Flex Spacing"))
            chk_flex.setChecked(self._global_opts["flex_spacing"])
            chk_bidir = QCheckBox(tr("Bidirectional"))
            chk_bidir.setChecked(self._global_opts["bidirectional"])

            for chk in (chk_variants, chk_ja, chk_flex, chk_bidir):
                lay.addWidget(chk)

            btn_row = QHBoxLayout()
            btn_done = QPushButton(tr("Done"))
            btn_done.setDefault(True)
            btn_row.addStretch()
            btn_row.addWidget(btn_done)
            lay.addLayout(btn_row)

            def _on_done():
                self._global_opts["variants"] = chk_variants.isChecked()
                self._global_opts["ja"] = chk_ja.isChecked()
                self._global_opts["flex_spacing"] = chk_flex.isChecked()
                self._global_opts["bidirectional"] = chk_bidir.isChecked()
                self._update_preview()
                dlg.accept()

            btn_done.clicked.connect(_on_done)
            dlg.exec()

        # ------------------------------------------------------------------
        # Per-row ⚙ line options dialog
        # ------------------------------------------------------------------

        def _open_row_options_dialog(self, entry: dict):
            """Open a modal dialog to edit per-row modifiers (copy-on-OK)."""
            multi_box = len(entry["boxes"]) > 1

            dlg = QDialog(self)
            dlg.setWindowTitle(tr("Line options"))
            lay = QVBoxLayout(dlg)
            lay.setSpacing(6)

            mods = entry["mods"]

            chk_neg = QCheckBox(tr("Negation −"))
            chk_neg.setChecked(mods.get("negation", False))
            chk_plene = QCheckBox(tr("Plene/Defective %"))
            chk_plene.setChecked(mods.get("plene", False))
            chk_prefix = QCheckBox(tr("Prefixes #_"))
            chk_prefix.setChecked(mods.get("prefix", False))
            chk_suffix = QCheckBox(tr("Suffixes _#"))
            chk_suffix.setChecked(mods.get("suffix", False))

            # Wildcard prefix: disabled + cleared when row has >1 OR box (RR-13)
            chk_wp = QCheckBox(tr("Wildcard *_"))
            if multi_box:
                chk_wp.setEnabled(False)
                chk_wp.setChecked(False)  # cleared — no stale True (RR-13)
                chk_wp.setToolTip(tr("Wildcard-prefix disabled for multi-box OR lines"))
            else:
                chk_wp.setChecked(mods.get("wildcard_prefix", False))

            chk_ws = QCheckBox(tr("Wildcard _*"))
            chk_ws.setChecked(mods.get("wildcard_suffix", False))

            sep = QFrame()
            sep.setFrameShape(QFrame.Shape.HLine)
            sep.setFrameShadow(QFrame.Shadow.Sunken)

            chk_start = QCheckBox(tr("⊢ starts line"))
            chk_start.setToolTip(tr("The FIRST word must be at the START of the line (right edge in Hebrew)"))
            chk_start.setChecked(entry["start"].isChecked())
            chk_end = QCheckBox(tr("⊣ ends line"))
            chk_end.setToolTip(tr("The LAST word must be at the END of the line (left edge in Hebrew)"))
            chk_end.setChecked(entry["end"].isChecked())

            for w in (chk_neg, chk_plene, chk_prefix, chk_suffix, chk_wp, chk_ws, sep, chk_start, chk_end):
                lay.addWidget(w)

            btn_row = QHBoxLayout()
            btn_cancel = QPushButton(tr("Cancel"))
            btn_ok = QPushButton(tr("Apply"))
            btn_ok.setDefault(True)
            btn_row.addStretch()
            btn_row.addWidget(btn_cancel)
            btn_row.addWidget(btn_ok)
            lay.addLayout(btn_row)

            btn_cancel.clicked.connect(dlg.reject)

            def _on_ok():
                # Copy-on-OK — write back to entry only on confirmation
                new_mods = {
                    "negation": chk_neg.isChecked(),
                    "plene": chk_plene.isChecked(),
                    "prefix": chk_prefix.isChecked(),
                    "suffix": chk_suffix.isChecked(),
                    "wildcard_prefix": chk_wp.isChecked() if not multi_box else False,
                    "wildcard_suffix": chk_ws.isChecked(),
                }
                entry["mods"] = new_mods
                entry["start"].setChecked(chk_start.isChecked())
                entry["end"].setChecked(chk_end.isChecked())
                self._update_row_indicator(entry)
                self._sync()
                self._update_preview()
                dlg.accept()

            btn_ok.clicked.connect(_on_ok)
            dlg.exec()

        _MOD_DISPLAY = {
            "negation": "−",
            "plene": "%",
            "prefix": "#_",
            "suffix": "_#",
            "wildcard_prefix": "*_",
            "wildcard_suffix": "_*",
        }

        def _update_row_indicator(self, entry: dict):
            """Update the per-row modifier indicator label."""
            mods = entry["mods"]
            parts = [v for k, v in self._MOD_DISPLAY.items() if mods.get(k)]
            text = " ".join(parts)
            entry["ind"].setText(text)
            entry["ind"].setVisible(bool(text))

        # ------------------------------------------------------------------
        # Build / query methods
        # ------------------------------------------------------------------

        def is_empty(self) -> bool:
            """Return True iff no box across ALL rows has non-whitespace text."""
            return not any(
                b["edit"].text().strip()
                for e in self.rows
                for b in e["boxes"]
            )

        def _page_position(self):
            """Return the selected page-position value, or None if control not present."""
            if self.page_pos is None:
                return None
            return self.page_pos.currentData()

        def _responsa_opts(self) -> dict:
            """Return the GLOBAL search options dict for Plan 03 to merge (RR-14).

            Reads from self._global_opts dict (persists across dialog open/close).
            NOTE: PER-ROW token modifiers (negation/plene/prefix/suffix/wildcards) are
            NOT in here — they are baked into the term by build_side_query's hoist (RR-13).
            ja/flex_spacing/bidirectional are exposed here because compose() hardcodes them
            False and SideQuery can't carry them — Plan 03's do_search merges them into ro.
            """
            v = self._global_opts.get("variants", False)
            return {
                "responsa_mode": True,
                "variants": v,
                "ja": self._global_opts.get("ja", False),
                "flex_spacing": self._global_opts.get("flex_spacing", False),
                "bidirectional": self._global_opts.get("bidirectional", False),
                "variant_mode": "variants" if v else "exact",
            }

        def build_side_query(self):
            """Build a SideQuery from all rows using the PER-ROW HOIST rule (RR-13).

            Returns a SideQuery, or None if is_empty().

            Each row's term:
              - Single non-empty box → decorate the lone token with the row's mods
                (genizah_core.py:6014-6027 order):
                  negation → '-' + t (overrides all)
                  else: plene → prefix → suffix-append → wildcard_prefix → wildcard_suffix
              - Multiple non-empty boxes → build the slash-group THEN HOIST row mods outside:
                  negation → '-(group)'
                  else: plene → prefix → suffix-append → wildcard_suffix
                  wildcard-PREFIX is NOT hoisted on multi-box (parser limitation, RR-13).
            """
            from shared.joins_lab import BuilderRow, SideQuery
            if self.is_empty():
                return None

            builder_rows = []
            for entry in self.rows:
                tokens = [
                    b["edit"].text().strip()
                    for b in entry["boxes"]
                    if b["edit"].text().strip()
                ]
                mods = entry["mods"]

                if not tokens:
                    term = ""
                elif len(tokens) == 1:
                    t = tokens[0]
                    if mods.get("negation"):
                        term = "-" + t
                    else:
                        if mods.get("plene"):
                            t = "%" + t
                        if mods.get("prefix"):
                            t = "#" + t
                        if mods.get("suffix"):
                            t = t + "#"
                        if mods.get("wildcard_prefix"):
                            t = "*" + t
                        if mods.get("wildcard_suffix"):
                            t = t + "*"
                        term = t
                else:
                    # Multi-box: build the group then HOIST row mods outside (RR-13)
                    group = "(" + "/".join(tokens) + ")"
                    if mods.get("negation"):
                        term = "-" + group
                    else:
                        t = group
                        if mods.get("plene"):
                            t = "%" + t
                        if mods.get("prefix"):
                            t = "#" + t
                        if mods.get("suffix"):
                            t = t + "#"
                        # wildcard-PREFIX not hoistable on group — skip it (RR-13)
                        if mods.get("wildcard_suffix"):
                            t = t + "*"
                        term = t

                builder_rows.append(BuilderRow(
                    term=term,
                    line_start=entry["start"].isChecked(),
                    line_end=entry["end"].isChecked(),
                    gap_to_next=entry["gap"].value(),
                ))

            return SideQuery(
                rows=tuple(builder_rows),
                variants=self._global_opts.get("variants", False),
                page_position=self._page_position(),
            )

        def _update_preview(self):
            """Update the read-only Preview QLineEdit with the composed query string."""
            from shared.joins_lab import compose
            if self.is_empty():
                self._preview_edit.setText("")
                return
            sq = self.build_side_query()
            if sq is None:
                self._preview_edit.setText("")
                return
            try:
                query_str, _ro, _pp = compose(sq)
                self._preview_edit.setText(query_str or "")
            except ValueError:
                self._preview_edit.setText(
                    tr("(page anchor needs a non-empty anchored line)")
                )

        def on_enter(self):
            """Called when Enter/Return is pressed in any word-box."""
            if self._on_search_cb is not None:
                self._on_search_cb()

        # ------------------------------------------------------------------
        # Session persistence (Feature 7) — INPUT state only; no results.
        # ------------------------------------------------------------------

        def to_state(self) -> dict:
            """Serialize the builder INPUT state to a plain dict (for session persistence).

            Captures: per-row boxes (text), mods, start/end checkbox states, gap value;
            _global_opts; and page_position index.
            Never persists candidates or results.
            """
            rows_state = []
            for entry in self.rows:
                rows_state.append({
                    "boxes": [b["edit"].text() for b in entry["boxes"]],
                    "mods": dict(entry["mods"]),
                    "start": entry["start"].isChecked(),
                    "end": entry["end"].isChecked(),
                    "gap": entry["gap"].value(),
                })
            page_pos_idx = 0
            if self.page_pos is not None:
                page_pos_idx = self.page_pos.currentIndex()
            return {
                "rows": rows_state,
                "global_opts": dict(self._global_opts),
                "page_pos_idx": page_pos_idx,
            }

        def from_state(self, state: dict):
            """Restore builder INPUT state from a plain dict (inverse of to_state).

            Clears existing rows, then rebuilds from state["rows"].
            Safe to call on a freshly constructed builder.
            """
            rows_data = state.get("rows") or []
            global_opts = state.get("global_opts") or {}
            page_pos_idx = state.get("page_pos_idx", 0)

            # Restore global opts FIRST (add_row calls _update_preview which reads these)
            self._global_opts.update({
                k: bool(global_opts.get(k, False))
                for k in ("variants", "ja", "flex_spacing", "bidirectional")
            })

            # Remove all existing rows
            for entry in list(self.rows):
                try:
                    entry["widget"].setParent(None)
                    entry["widget"].deleteLater()
                except RuntimeError:
                    pass
            self.rows.clear()

            # Rebuild rows from state
            for row_data in rows_data:
                boxes_texts = row_data.get("boxes") or [""]
                mods = row_data.get("mods") or {}
                start_chk = row_data.get("start", False)
                end_chk = row_data.get("end", False)
                gap_val = row_data.get("gap", 0)

                # add_row adds the first box with placeholder text
                entry = self.add_row(placeholder=boxes_texts[0] if boxes_texts else "")
                # Set text on the first box
                if boxes_texts:
                    try:
                        entry["boxes"][0]["edit"].setText(boxes_texts[0])
                    except (IndexError, KeyError):
                        pass
                # Add extra OR boxes
                for extra_text in boxes_texts[1:]:
                    self.add_or_box(entry)
                    try:
                        entry["boxes"][-1]["edit"].setText(extra_text)
                    except (IndexError, KeyError):
                        pass
                # Restore mods, start/end, gap
                entry["mods"] = dict(mods)
                entry["start"].setChecked(bool(start_chk))
                entry["end"].setChecked(bool(end_chk))
                entry["gap"].setValue(int(gap_val))
                self._update_row_indicator(entry)

            # If no rows were restored, add one blank row
            if not self.rows:
                self.add_row(placeholder=self._first_hint)

            # Restore page-position selection
            if self.page_pos is not None:
                idx = int(page_pos_idx)
                if 0 <= idx < self.page_pos.count():
                    self.page_pos.setCurrentIndex(idx)

            self._update_preview()

    # -------------------------------------------------------------------------
    # _DesktopSearchExecutor — thin SearchExecutor Protocol adapter (D-22, Plan 03).
    # Wraps self.searcher (SearchEngine) + self.meta_mgr (MetadataManager).
    # -------------------------------------------------------------------------

    class _DesktopSearchExecutor:
        """Concrete adapter satisfying the Phase-106 SearchExecutor Protocol.

        Thin passthrough — no per-app normalizer (Phase-106 D-01).
        Instantiated as self._executor in JoinWorkbenchWindow.__init__.
        Plan 03 passes self._executor into the candidate pane.
        """

        def __init__(self, searcher, meta_mgr):
            self._searcher = searcher
            self._meta_mgr = meta_mgr

        def execute_search(
            self,
            query_str,
            mode,
            gap,
            progress_callback=None,
            exclude_words=None,
            responsa_options=None,
            restrict_sys_ids=None,
            text_position=None,
            corpus_scope="all",
        ):
            """Forward to searcher.execute_search; return [] on any failure."""
            try:
                return self._searcher.execute_search(
                    query_str,
                    mode,
                    gap,
                    progress_callback=progress_callback,
                    exclude_words=exclude_words,
                    responsa_options=responsa_options,
                    restrict_sys_ids=restrict_sys_ids,
                    text_position=text_position,
                    corpus_scope=corpus_scope,
                ) or []
            except Exception:
                return []

        def get_browse_page(
            self,
            sys_id,
            p_num=None,
            next_prev=0,
            absolute_index=None,
            allow_cross=False,
            volume_ie=None,
        ):
            """Forward to searcher.get_browse_page."""
            return self._searcher.get_browse_page(
                sys_id,
                p_num=p_num,
                next_prev=next_prev,
                absolute_index=absolute_index,
                allow_cross=allow_cross,
                volume_ie=volume_ie,
            )

        def get_meta_for_id(self, sys_id: str) -> tuple:
            """Forward to meta_mgr.get_meta_for_id."""
            return self._meta_mgr.get_meta_for_id(sys_id)

        def get_library_for_id(self, sys_id: str) -> str:
            """Forward to meta_mgr.get_library_for_id."""
            return self._meta_mgr.get_library_for_id(sys_id) or ""

    # -------------------------------------------------------------------------
    # Plan 03 — QThread workers (Candidate-typed; RR-2)
    # -------------------------------------------------------------------------

    # Colour constants shared by CandidateCard and JoinCandidatePane.
    # All cards use these palette values regardless of dark-mode.
    _META_COLOR = "#64748b"
    _DIM_COLOR = "#94a3b8"
    # Feature 5 (Polish 3): muted/desaturated tones at 2px — easier on the eye.
    _TRI_COLOR = {
        None: "#94a3b8",
        "yes": "#4d9e6a",    # softer green  (was #16a34a)
        "maybe": "#c4853a",  # softer amber  (was #d97706)
        "no": "#c05050",     # softer red    (was #dc2626)
    }
    _MAX_CONCURRENT_IMG = 5   # bounded image-loader pool (PATTERNS "5 slots")

    class ThumbResolver(QThread):
        """Resolve NLI thumbnail URLs for candidate cards (manuscript-level, RR-7 note).

        Grid card THUMBNAILS are manuscript-level (get_thumbnail is fine here);
        the per-page matched image is resolved via _image_url_for_idx in the
        _enqueue_image_for_pane helper on the window.

        Signal: resolved(card_index, url_or_empty)
        """

        resolved = pyqtSignal(int, str)

        def __init__(self, meta_mgr, items: list):
            super().__init__()
            self.meta_mgr = meta_mgr
            self.items = list(items)   # list of (idx, sys_id)
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

    class _CrossSideWorker(QThread):
        """Run cross-side AND/OR membership check off the UI thread.

        Receives an ALREADY-MERGED b_ro (the pane merges the other_builder's
        ja/flex/bidir into it before starting this worker — RR-14).

        Signal: done(MergeResult)
        """

        done = pyqtSignal(object)

        def __init__(self, executor, base_candidates: list, b_query,
                     b_ro: dict, combine: str, a_pattern):
            super().__init__()
            self.executor = executor
            self.base = list(base_candidates)   # list of Candidate
            self.b_query = b_query              # SideQuery (other side)
            self.b_ro = b_ro                    # MERGED responsa_options (RR-14)
            self.combine = combine              # 'AND' | 'OR'
            self.a_pattern = a_pattern
            self._cancel = False

        def cancel(self):
            self._cancel = True

        def run(self):
            from shared.joins_lab import apply_cross_side, compose, MergeResult
            try:
                b_str, _b_ro_ignored, _b_pos = compose(self.b_query)
            except ValueError:
                self.done.emit(MergeResult(candidates=tuple(self.base), note=""))
                return
            if not b_str:
                self.done.emit(MergeResult(candidates=tuple(self.base), note=""))
                return
            try:
                # Pass the MERGED b_ro as b_responsa_options — do NOT re-compose here.
                result = apply_cross_side(
                    self.executor,
                    self.base,
                    self.b_query,
                    self.b_ro,
                    self.combine,
                    self.a_pattern,
                )
            except Exception as exc:
                logger.warning("_CrossSideWorker error: %s", exc)
                result = MergeResult(candidates=tuple(self.base), note="")
            self.done.emit(result)

    class _EnrichWorker(QThread):
        """Batch-enrich all candidates: measurements + snippets + size-mismatch hints.

        Emits a dict keyed by (sys_id, page) — c.key — so the same sys_id at
        different pages does not overwrite each other's snippet (RR-2 per-page key).
        Triage is SEPARATELY keyed by sys_id (R-05 deliberate split — a physical
        fragment shares triage regardless of which page it was found at).

        Signal: enriched({(sys_id, page): {...}})
        """

        enriched = pyqtSignal(dict)

        def __init__(self, fjms_svc, candidates: list, anchor_meas):
            super().__init__()
            self.fjms_svc = fjms_svc
            self.candidates = list(candidates)  # list of Candidate (RR-2)
            self.anchor_meas = anchor_meas or {}
            self._cancel = False

        def cancel(self):
            self._cancel = True

        def run(self):
            from shared.joins_lab import snippet_html, snippet_plain
            # Batch measurement fetch — ONE IN-query, never per-card (D-21, Pitfall 3)
            sys_ids = [c.sys_id for c in self.candidates]   # Candidate attribute (RR-2)
            meas = {}
            try:
                meas = self.fjms_svc.get_measurement_summaries_batch(sys_ids)  # RR-6
            except Exception as exc:
                logger.warning("_EnrichWorker.get_measurement_summaries_batch: %s", exc)

            out = {}   # keyed by (sys_id, page) == c.key (RR-2)
            for c in self.candidates:
                if self._cancel:
                    return
                # Measurements are per-manuscript (one size per sys_id)
                m = meas.get(c.sys_id) or {}
                # Read EXISTING key names from get_measurement_summaries_batch (RR-6)
                width_cm = m.get("width_cm")
                height_cm = m.get("height_cm")
                material = m.get("material")
                avg_num_lines = m.get("avg_num_lines")
                size_category = m.get("size_category")
                # Size-mismatch flag: ratio > 1.4 when both anchor and candidate width known (D-13)
                mismatch = False
                a_w = self.anchor_meas.get("width_cm") if self.anchor_meas else None
                if width_cm and a_w:
                    try:
                        ratio = max(width_cm, a_w) / max(min(width_cm, a_w), 0.01)
                        mismatch = ratio > 1.4
                    except Exception:
                        mismatch = False
                # Snippet generation — pure, safe off-thread
                snip_h = snippet_html(c.full_text, c.highlight_pattern, max_lines=6)
                snip_p = snippet_plain(c.full_text, c.highlight_pattern, max_chars=220)
                # Key by c.key = (sys_id, page) — per-page (RR-2)
                out[c.key] = {
                    "width_cm": width_cm,
                    "height_cm": height_cm,
                    "material": material,
                    "avg_num_lines": avg_num_lines,
                    "size_category": size_category,
                    "snippet_html": snip_h,
                    "snippet_plain": snip_p,
                    "mismatch": mismatch,
                }
            self.enriched.emit(out)

    # -------------------------------------------------------------------------
    # Plan 03 — _tag helper (section label) used by JoinCandidatePane
    # -------------------------------------------------------------------------

    def _tag(text: str, color: str) -> QLabel:
        """Return a styled section-tag QLabel (teal/green section header)."""
        lbl = QLabel(text)
        lbl.setWordWrap(True)
        lbl.setStyleSheet(
            f"font-size:11px;font-weight:bold;color:{color};"
            f"padding:2px 0;"
        )
        return lbl

    # -------------------------------------------------------------------------
    # Plan 03 — CandidateCard (QFrame) — one card per Candidate in the grid view
    # -------------------------------------------------------------------------

    class CandidateCard(QFrame):
        """A fixed-width card for one Candidate in the grid view (UI-SPEC Surface 2).

        Reads Candidate attributes directly (RR-2).  Triage border colour is driven
        by wb.triage[sys_id] (sys_id-keyed per R-05).
        Action buttons are icon-only with tr() tooltips (adapted_decision 9).
        Per-card checkbox reads from pane._selected_keys on render (adapted_decision 6).
        Right-click context menu: same actions + Y/?/N triage (adapted_decision 9).
        """

        def __init__(self, pane, c, global_idx: int, enrich: dict):
            super().__init__()
            self.pane = pane            # JoinCandidatePane
            self.c = c                  # Candidate dataclass (RR-2)
            self.global_idx = global_idx
            self.sid = c.sys_id         # read Candidate attribute directly (RR-2)
            self._ckey = pane._candidate_key(c)  # stable key for selection set
            self.setFixedWidth(232)
            self.setFrameShape(QFrame.Shape.Box)
            self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.customContextMenuRequested.connect(self._ctx_menu)
            self._restyle()

            lay = QVBoxLayout(self)
            lay.setContentsMargins(4, 4, 4, 4)
            lay.setSpacing(2)

            # 0. Per-card selection checkbox (top-right)
            chk_row = QHBoxLayout()
            chk_row.addStretch()
            self._sel_chk = QCheckBox()
            self._sel_chk.setToolTip(tr("Select this candidate"))
            # Block signals while setting initial state (adapted_decision 6)
            self._sel_chk.blockSignals(True)
            self._sel_chk.setChecked(self._ckey in pane._selected_keys)
            self._sel_chk.blockSignals(False)
            self._sel_chk.stateChanged.connect(
                lambda state: pane._set_selected(self._ckey, bool(state))
            )
            chk_row.addWidget(self._sel_chk)
            lay.addLayout(chk_row)

            # 1. Thumbnail image label
            self.img = QLabel(tr("loading…"))
            self.img.setFixedSize(220, 130)
            self.img.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.img.setStyleSheet("background:#e2e8f0;color:#64748b;")
            lay.addWidget(self.img)

            # 2. Shelfmark + provenance badge
            shelf_text = c.shelfmark
            eye_badge = False
            if c.is_anchor_self:
                shelf_text += tr("  ⚓ self")
            elif c.via_other_side:
                shelf_text += tr("  ⇄ other side")
            elif c.via_vs:
                # G-06: ONE eye badge for any visual look-alike (intersection OR pure-VS).
                # G-09: no rank. ★both / ⊙VS#rank are gone.
                shelf_text += "  👁"
                eye_badge = True
            # G-06.4 / review #6: text-only candidates render UNBADGED.
            shelf_lbl = QLabel(shelf_text)
            shelf_lbl.setStyleSheet("font-weight:bold;font-size:12px;")
            shelf_lbl.setWordWrap(True)
            if eye_badge:
                shelf_lbl.setToolTip(tr("visual similarity"))   # G-06.2 (HE: דמיון חזותי, pre-seeded)
            lay.addWidget(shelf_lbl)

            # 3. Dimension / material evidence line (from pre-fetched enrich dict, RR-6)
            m = enrich.get(c.key) or {}   # c.key = (sys_id, page) — RR-2 per-page key
            width_cm = m.get("width_cm")
            height_cm = m.get("height_cm")
            material = m.get("material")
            avg_num_lines = m.get("avg_num_lines")
            size_category = m.get("size_category")
            dim_parts = []
            if width_cm and height_cm:
                dim_parts.append(f"{width_cm:.0f}x{height_cm:.0f} cm")
            elif size_category:
                dim_parts.append(str(size_category))
            if material:
                dim_parts.append(material_display(str(material)))  # Feature 9: HE material
            if avg_num_lines:
                dim_parts.append(f"~{avg_num_lines:.0f} ln")
            if dim_parts:
                dim_str = "  ·  ".join(dim_parts)
                if m.get("mismatch"):
                    dim_str += f'  <span style="color:#d97706">{tr("⚠ size mismatch")}</span>'
                dim_lbl = QLabel(dim_str)
                dim_lbl.setStyleSheet(f"font-size:10px;color:{_DIM_COLOR};")
                if m.get("mismatch"):
                    dim_lbl.setToolTip(tr("Size may not match anchor"))
                lay.addWidget(dim_lbl)

            # 4. Snippet (pre-fetched HTML)
            # Feature 1: save as self.snip so folio-flip can update the text.
            self.snip = QTextBrowser()
            self.snip.setFixedHeight(72)
            self.snip.setReadOnly(True)
            self.snip.setHtml(m.get("snippet_html") or "")
            lay.addWidget(self.snip)
            # Feature 1: page-text worker ref (None until folio flip is requested)
            self._card_text_worker = None
            self._card_text_gen = 0

            # 5. Combined folio-nav + triage row (G-11): [▶ p.N ◀]  …(stretch)…  [Y][?][N]
            # RR-12: None-page guard — default to 1 if c.page is None.
            self._card_page = max(1, c.page or 1)
            row = QHBoxLayout()
            row.setSpacing(2)

            # --- folio nav LEFT (Feature 7 RTL glyphs: PREV=▶, NEXT=◀) ---
            self._folio_prev_btn = QPushButton("▶")
            self._folio_prev_btn.setFixedSize(24, 22)
            self._folio_prev_btn.setAccessibleName(tr("Previous folio"))
            self._folio_prev_btn.setToolTip(tr("Previous folio"))
            self._folio_prev_btn.clicked.connect(self._card_folio_prev)
            row.addWidget(self._folio_prev_btn)

            self._folio_lbl = QLabel(f"p.{self._card_page}")
            self._folio_lbl.setStyleSheet("font-size:10px;color:#94a3b8;")
            row.addWidget(self._folio_lbl)

            self._folio_next_btn = QPushButton("◀")
            self._folio_next_btn.setFixedSize(24, 22)
            self._folio_next_btn.setAccessibleName(tr("Next folio"))
            self._folio_next_btn.setToolTip(tr("Next folio"))
            self._folio_next_btn.clicked.connect(self._card_folio_next)
            row.addWidget(self._folio_next_btn)

            # --- stretch pushes triage to the RIGHT ---
            row.addStretch()

            # --- triage RIGHT (Y / ? / N) ---
            for emoji, val, aname in (
                ("✓", "yes", tr("Mark yes")),
                ("?", "maybe", tr("Mark maybe")),
                ("✗", "no", tr("Mark no")),
            ):
                btn = QPushButton(emoji)
                btn.setFixedSize(28, 28)
                btn.setAccessibleName(aname)
                btn.clicked.connect(
                    lambda _checked=False, v=val, s=self.sid: self.pane.wb.mark(s, v)
                )
                row.addWidget(btn)

            lay.addLayout(row)

            # 6. Action row: ICON-ONLY buttons (adapted_decision 9)
            arow = QHBoxLayout()
            arow.setSpacing(2)

            browse_btn = QPushButton("📖")
            browse_btn.setFixedSize(28, 28)
            browse_btn.setToolTip(tr("Browse"))
            browse_btn.setAccessibleName(tr("Browse"))
            browse_btn.clicked.connect(
                lambda _checked=False, c_=c: self.pane.wb.open_result_in_browse(c_)
            )
            arow.addWidget(browse_btn)

            puzzle_btn = QPushButton("🧩")
            puzzle_btn.setFixedSize(28, 28)
            puzzle_btn.setToolTip(tr("Add to Puzzle (with anchor)"))
            puzzle_btn.setAccessibleName(tr("Puzzle"))
            puzzle_btn.clicked.connect(
                lambda _checked=False, c_=c: self.pane.wb.open_result_in_puzzle(c_)
            )
            arow.addWidget(puzzle_btn)

            list_btn = QPushButton("☰")
            list_btn.setFixedSize(28, 28)
            list_btn.setToolTip(tr("Add to list"))
            list_btn.setAccessibleName(tr("List"))
            list_btn.clicked.connect(
                lambda _checked=False, c_=c, b=None: self.pane.wb.open_result_in_list(c_, b)
            )
            arow.addWidget(list_btn)

            join_btn = QPushButton("🔗")
            join_btn.setFixedSize(28, 28)
            join_btn.setToolTip(tr("Add as Join"))
            join_btn.setAccessibleName(tr("Add as Join"))
            join_btn.clicked.connect(
                lambda _checked=False, c_=c: self.pane.wb.open_result_as_join(c_)
            )
            arow.addWidget(join_btn)

            cmp_btn = QPushButton("⇄")
            cmp_btn.setFixedSize(28, 28)
            cmp_btn.setToolTip(tr("Compare side-by-side with anchor"))
            cmp_btn.setAccessibleName(tr("Compare"))
            cmp_btn.clicked.connect(
                lambda _checked=False, gi=self.global_idx: self.pane.open_compare(gi)
            )
            arow.addWidget(cmp_btn)

            reanchor_btn = QPushButton("⚓")
            reanchor_btn.setFixedSize(28, 28)
            reanchor_btn.setToolTip(tr("Set this candidate as the new anchor"))
            reanchor_btn.setAccessibleName(tr("Re-anchor"))
            reanchor_btn.clicked.connect(
                lambda _checked=False, c_=c: self.pane.wb.set_anchor(
                    candidate_to_result_dict(c_)
                )
            )
            arow.addWidget(reanchor_btn)

            # Plan 06 — pick mode: "Select as partner" button (G-05 / D-18).
            # Only created when a pick_callback is active on the Workbench window.
            # Because set_pick_callback/clear_pick_callback call _rerender_candidate_cards(),
            # a callback set AFTER the first render still produces this button (HIGH-4).
            # Named _on_pick_partner — no _vs_ prefix to keep D-18 no-private guard green.
            # MARKED REMOVABLE (Phase 109 G-08, D-11 one-cycle soft-retire): with the JoinsDialog pick-back
            # retired (G-08 reverses G-05), this pick-callback machinery has no live caller. RETAINED one
            # cycle as a safety net; tests (test_invoke_pick_forwards_sysid_shelfmark,
            # test_set_pick_callback_rerenders) keep it green. Removable once the parity UAT signs off.
            if self.pane.wb._pick_callback is not None:
                pick_btn = QPushButton(tr("Select as partner"))
                pick_btn.setToolTip(tr("Select as partner"))
                pick_btn.setAccessibleName(tr("Select as partner"))

                def _on_pick_partner(_checked=False, c_=c):
                    try:
                        if _invoke_pick(self.pane.wb._pick_callback, c_):
                            self.pane.wb.close()
                    except RuntimeError:
                        pass

                pick_btn.clicked.connect(_on_pick_partner)
                lay.addWidget(pick_btn)

            lay.addLayout(arow)

        def mouseDoubleClickEvent(self, event):
            """Feature 2: double-click opens Compare (skip if on checkbox/buttons)."""
            child = self.childAt(event.position().toPoint())
            # Don't open compare when clicking a checkbox or push-button
            if child is not None and isinstance(child, (QCheckBox, QPushButton)):
                super().mouseDoubleClickEvent(event)
                return
            try:
                self.pane.open_compare(self.global_idx)
            except Exception:
                pass
            event.accept()

        def _ctx_menu(self, pos):
            """Right-click context menu: same actions + Y/?/N triage (adapted_decision 9)."""
            menu = QMenu(self)
            c = self.c

            a_browse = menu.addAction("📖 " + tr("Browse"))
            a_browse.triggered.connect(
                lambda: self.pane.wb.open_result_in_browse(c)
            )
            a_puzzle = menu.addAction("🧩 " + tr("Add to Puzzle (with anchor)"))
            a_puzzle.triggered.connect(
                lambda: self.pane.wb.open_result_in_puzzle(c)
            )
            a_list = menu.addAction("☰ " + tr("Add to list"))
            a_list.triggered.connect(
                lambda: self.pane.wb.open_result_in_list(c, None)
            )
            a_join = menu.addAction("🔗 " + tr("Add as Join"))
            a_join.triggered.connect(
                lambda: self.pane.wb.open_result_as_join(c)
            )
            a_cmp = menu.addAction("⇄ " + tr("Compare side-by-side with anchor"))
            a_cmp.triggered.connect(
                lambda gi=self.global_idx: self.pane.open_compare(gi)
            )

            menu.addSeparator()

            for label, val in (
                ("Y — " + tr("Mark yes"), "yes"),
                ("? — " + tr("Mark maybe"), "maybe"),
                ("N — " + tr("Mark no"), "no"),
            ):
                act = menu.addAction(label)
                act.triggered.connect(
                    lambda _checked=False, v=val, s=self.sid: self.pane.wb.mark(s, v)
                )

            menu.exec(self.mapToGlobal(pos))

        def _restyle(self):
            """Update card border based on triage/selection state."""
            if self.c.is_anchor_self:
                # Feature 5: 2px softer teal for anchor-self (was 3px)
                self.setStyleSheet("QFrame{border:2px solid #4db8a6;border-radius:4px;}")
                return
            # Show selection outline when in _selected_keys
            if hasattr(self, "_ckey") and self._ckey in self.pane._selected_keys:
                self.setStyleSheet(
                    "QFrame{border:2px solid #4db8a6;border-radius:4px;"
                    "box-shadow:inset 0 0 0 1px #4db8a6;}"
                )
                return
            tri = self.pane.wb.triage.get(self.sid)
            color = _TRI_COLOR.get(tri, _TRI_COLOR[None])
            # Feature 5: 2px border (was 3px)
            self.setStyleSheet(f"QFrame{{border:2px solid {color};border-radius:4px;}}")

        def set_pixmap(self, pix):
            """Set the thumbnail pixmap (called from GUI thread — Pitfall 4 safe)."""
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
                pass   # widget deleted — standard Phase 107 guard

        def _card_folio_prev(self):
            """Step to the previous folio image and text for this card (Features 3+1)."""
            if self._card_page <= 1:
                return
            self._card_page -= 1
            self._refresh_card_image()
            self._refresh_card_text()

        def _card_folio_next(self):
            """Step to the next folio image and text for this card (Features 3+1).

            Upper bound: clamp at the image-count once known (graceful overshoot → no image).
            """
            self._card_page += 1
            self._refresh_card_image()
            self._refresh_card_text()

        def _refresh_card_image(self):
            """Re-enqueue the image for the current _card_page via the window's resolver.

            RR-12: _enqueue_image_for_pane guards None internally; page is always int here.
            """
            try:
                self._folio_lbl.setText(f"p.{self._card_page}")
                self.img.setText(tr("loading…"))
            except RuntimeError:
                return
            try:
                self.pane.wb._enqueue_image_for_pane(
                    self.img, self.sid, self._card_page, width=400
                )
            except Exception:
                pass

        def _refresh_card_text(self):
            """Fetch the page text for the current _card_page on a background worker.

            Feature 1: folio flip also updates the snippet.
            RR-12: page is always int here (guard was applied in _card_folio_prev/next).
            A local _card_text_gen token ensures stale results are dropped.
            """
            try:
                self.snip.setPlainText(tr("loading…"))
            except RuntimeError:
                return
            # Cancel any in-flight text worker for this card
            if self._card_text_worker is not None:
                try:
                    self._card_text_worker.cancel()
                except Exception:
                    pass
                self._card_text_worker = None
            self._card_text_gen += 1
            my_gen = self._card_text_gen
            page = self._card_page
            sid = self.sid
            try:
                # _PageTextWorker uses the wb gen as its own token; we add a local check.
                worker = _PageTextWorker(self.pane.wb, self.pane.wb._gen, sid, page)

                def _on_done(wgen: int, txt: str, _my_gen: int = my_gen) -> None:
                    if _my_gen != self._card_text_gen:
                        return  # stale: a newer folio flip is in-flight
                    try:
                        from shared.joins_lab import snippet_html
                        hi = snippet_html(txt, self.c.highlight_pattern, max_lines=6) if txt else ""
                        self.snip.setHtml(hi or "")
                    except RuntimeError:
                        pass

                worker.done.connect(_on_done)
                worker.start()
                self._card_text_worker = worker
            except Exception:
                pass

        def load_vs_text(self):
            """G-02: page-lazy fetch of a VS candidate's transcription (manuscript first page).

            Mirrors _refresh_card_text but for via_vs rows with no full_text. D-09 page-lazy.
            Called from _render_grid_page for each visible page card — never for the full ≤200 set.
            For non-VS or already-texted cards this is a no-op (the guard below).
            The done handler ALWAYS sets the snippet (even to "") so it never stays "loading…".
            """
            if not (getattr(self.c, "via_vs", False) and not (self.c.full_text or "")):
                return
            self._card_text_gen += 1
            my_gen = self._card_text_gen
            try:
                self.snip.setPlainText(tr("loading…"))
            except RuntimeError:
                return
            try:
                worker = _PageTextWorker(self.pane.wb, self.pane.wb._gen, self.sid, 1)

                def _on_done(_wgen, txt, _my_gen=my_gen):
                    if _my_gen != self._card_text_gen:
                        return
                    try:
                        from shared.joins_lab import snippet_html
                        hi = snippet_html(txt, self.c.highlight_pattern, max_lines=6) if txt else ""
                        self.snip.setHtml(hi or "")   # ALWAYS set — empty resolves to "", never stuck on "loading…"
                    except RuntimeError:
                        pass

                worker.done.connect(_on_done)
                worker.start()
                self._card_text_worker = worker
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Plan 03 — JoinCandidatePane (QWidget) — right-pane candidate hunt surface
    # -------------------------------------------------------------------------

    _GRID_COLS = 4
    _PER_PAGE = 20

    class JoinCandidatePane(QWidget):
        """Candidate-hunt surface in the Workbench right pane (Plan 03, JWB-07/10/11/12).

        Owns:
        - anchor-side JoinQueryBuilder (this side)
        - other-side collapsible JoinQueryBuilder (other-side builder, allow_page_position=False)
        - source selector (Text/Visual-disabled/Combined-disabled, D-14)
        - refine bar (text/material/dimensions/triage/size filters)
        - status label + self-match readout + view-toggle + pagination
        - grid (QScrollArea + QGridLayout, default) and table (QTableWidget, toggle)
        - _CrossSideWorker / _EnrichWorker coordination
        """

        def __init__(self, wb, executor):
            super().__init__()
            self.wb = wb            # JoinWorkbenchWindow (back-ref for triage/actions)
            self.executor = executor  # _DesktopSearchExecutor

            # Internal state
            self._text_cands = None   # list[Candidate] after dedup
            self._enrich: dict = {}   # {(sys_id, page): {...}} — per-page key (RR-2)
            self._anchor_matched = None  # bool or None
            self.results: list = []   # list[Candidate] — post-merge
            self.cards: dict = {}     # {global_idx: CandidateCard}
            self._page = 0            # current grid/table page (0-based)
            self.view_mode = "grid"   # 'grid' | 'table'
            self._resolver = None     # ThumbResolver (current page)
            self._cross_worker = None
            self._enrich_worker = None
            self._retired_workers = []  # crash-safety: running _EnrichWorkers awaiting finished() (0xC0000409)
            self._search_thread = None
            # Phase 109 VS source state (G-04 toggle model)
            self._vs_cands = None         # list[Candidate] from VS load, or None
            self._vs_on = False           # G-04: the single toggle's checked state (bool)
            self._vs_loaded_sid = None    # HIGH-1: anchor sys_id _vs_cands was loaded FOR (staleness key)
            self._pending_vs = None       # bool|None — toggle request deferred until new anchor's VS is known (BLOCKER A)

            self._build_ui()

        def _build_ui(self):
            rv = QVBoxLayout(self)
            rv.setContentsMargins(0, 0, 0, 0)
            rv.setSpacing(4)

            # --- Anchor (this-side) builder ---
            self.builder = JoinQueryBuilder(
                self.do_search,
                first_hint=tr("word(s) on this line…"),
            )
            rv.addWidget(self.builder)

            # --- Other-side collapsible ---
            os_row = QHBoxLayout()
            self.other_enable = QCheckBox(
                tr("search also on the other side of the leaf (p ±1)")
            )
            # Feature 6: tooltip without Latin AND/OR
            self.other_enable.setToolTip(
                tr("Narrow: keep only candidates whose adjacent page also matches. Widen: include adjacent pages as extra candidates.")
            )
            self.other_enable.toggled.connect(
                lambda v: self.other_box.setVisible(v)
            )
            os_row.addWidget(self.other_enable)
            # Feature 6: combo items without Latin AND/OR
            self.combine_combo = QComboBox()
            self.combine_combo.addItem(tr("Narrow"))
            self.combine_combo.addItem(tr("Widen"))
            os_row.addWidget(self.combine_combo)
            os_row.addStretch()
            rv.addLayout(os_row)

            self.other_box = QWidget()
            ob = QVBoxLayout(self.other_box)
            ob.setContentsMargins(16, 0, 0, 0)
            ob.setSpacing(2)
            # RR-5: allow_page_position=False on the other-side builder
            self.other_builder = JoinQueryBuilder(
                self.do_search,
                first_hint=tr("word(s) required on the OTHER side…"),
                allow_page_position=False,
            )
            ob.addWidget(self.other_builder)
            self.other_box.setVisible(False)  # collapsed by default (D-01)
            rv.addWidget(self.other_box)

            # --- Find Candidates action row (Phase 109: source selector + btn_find) ---
            src_row = QHBoxLayout()
            src_row.setSpacing(4)

            # Phase 109 G-04: single checkable "Visual Similarity" toggle (replaces 3 radios)
            # G-06.3: eye glyph prefix makes the toggle share the same visual vocabulary as the badge
            self.btn_vs_toggle = QPushButton("👁 " + tr("Visual Similarity"))
            self.btn_vs_toggle.setCheckable(True)
            # G-12.1: explicit :checked style so ON is unmistakable vs OFF (heavier border, faint shade,
            # no full accent fill — Hillel's explicit choice; bare native sunken state is NOT sufficient)
            self.btn_vs_toggle.setStyleSheet(
                "QPushButton:checked {"
                "  border: 2px solid #475569;"
                "  background-color: #e2e8f0;"
                "  font-weight: bold;"
                "}"
            )
            self.btn_vs_toggle.setToolTip(
                tr("Show only visual look-alikes; with a search term, only look-alikes that also match")
            )
            self.btn_vs_toggle.toggled.connect(self._on_vs_toggle)
            src_row.addWidget(self.btn_vs_toggle)

            src_row.addStretch()

            # Find Candidates button (always visible; with toggle ON + term, user presses Find
            # to run the text search; toggle ON + empty box auto-assembles on toggle)
            self.btn_find = QPushButton(tr("Find Candidates"))
            self.btn_find.clicked.connect(self.do_search)
            src_row.addWidget(self.btn_find)

            rv.addLayout(src_row)

            # --- Filter controls (persistent hidden widgets; apply_filters reads them) ---
            # These are NOT shown in the main layout; they live in the "Filter ▾" dialog.
            self.filter_in = QLineEdit()
            self.filter_in.setPlaceholderText(
                tr("Filter by shelfmark, text, or title…")
            )

            self.mat_filter = QComboBox()
            self.mat_filter.addItem(tr("any material"))

            self.dim_chk = QCheckBox(tr("Has dimensions"))

            self.tri_filter = QComboBox()
            self.tri_filter.addItems([
                tr("all triage"),
                tr("Y — kept"),
                tr("? — maybe"),
                tr("N — dismissed"),
                tr("untriaged"),
            ])

            # Size filter spinboxes — opt-in (D-13: off by default)
            self.size_btn = QPushButton(tr("Size filter"))
            self.size_btn.setCheckable(True)
            self.size_min = QSpinBox()
            self.size_min.setRange(0, 200)
            self.size_max = QSpinBox()
            self.size_max.setRange(0, 200)
            self.size_max.setValue(200)

            # --- Results toolbar: [Grid][Table] + Browse results ▶ + Clear + Filter ▾ + count ---
            res_toolbar = QHBoxLayout()
            res_toolbar.setSpacing(4)

            self.view_btn = QPushButton(tr("Table view"))
            self.view_btn.clicked.connect(self.toggle_view)
            res_toolbar.addWidget(self.view_btn)

            self.btn_browse_results = QPushButton(tr("Browse results ▶"))
            self.btn_browse_results.setToolTip(tr("Open Browse results compare window"))
            self.btn_browse_results.clicked.connect(self._browse_results)
            res_toolbar.addWidget(self.btn_browse_results)

            # "Clear" button — resets lab + clears persisted join_lab session state (Feature 5)
            self.btn_clear_lab = QPushButton(tr("Clear"))
            self.btn_clear_lab.setToolTip(
                tr("Clear anchor, builders, candidates, triage and session state")
            )
            self.btn_clear_lab.clicked.connect(self._clear_lab)
            res_toolbar.addWidget(self.btn_clear_lab)

            res_toolbar.addStretch()

            # "Filter ▾" button — opens filter dialog
            self.btn_filter = QPushButton(tr("Filter ▾"))
            self.btn_filter.setToolTip(tr("Open filter dialog"))
            self.btn_filter.clicked.connect(self._open_filter_dialog)
            res_toolbar.addWidget(self.btn_filter)

            self.status = QLabel(
                tr("Build a line-by-line query, then Find Candidates.")
            )
            self.status.setStyleSheet("font-size:10px;color:#94a3b8;")
            res_toolbar.addWidget(self.status)

            rv.addLayout(res_toolbar)

            # --- Shared bulk-action bar (grid + table) — hidden until selection non-empty ---
            self._selected_keys: set = set()
            bulk_bar = QHBoxLayout()
            self._bulk_bar_widget = QWidget()
            self._bulk_bar_widget.setVisible(False)
            bulk_inner = QHBoxLayout(self._bulk_bar_widget)
            bulk_inner.setSpacing(4)
            self._bulk_count_lbl = QLabel("")
            bulk_inner.addWidget(self._bulk_count_lbl)

            self._bulk_browse_btn = QPushButton("📖")
            self._bulk_browse_btn.setFixedWidth(32)
            self._bulk_browse_btn.setToolTip(tr("Browse — select exactly one"))
            self._bulk_browse_btn.setEnabled(False)
            self._bulk_browse_btn.clicked.connect(self._bulk_browse)
            bulk_inner.addWidget(self._bulk_browse_btn)

            bulk_puzzle_btn = QPushButton("🧩")
            bulk_puzzle_btn.setFixedWidth(32)
            bulk_puzzle_btn.setToolTip(tr("Add all to Puzzle (with anchor)"))
            bulk_puzzle_btn.clicked.connect(self._bulk_puzzle)
            bulk_inner.addWidget(bulk_puzzle_btn)

            bulk_list_btn = QPushButton("☰")
            bulk_list_btn.setFixedWidth(32)
            bulk_list_btn.setToolTip(tr("Add all to list"))
            bulk_list_btn.clicked.connect(self._bulk_list)
            bulk_inner.addWidget(bulk_list_btn)

            self._bulk_join_btn = QPushButton("🔗")
            self._bulk_join_btn.setFixedWidth(32)
            self._bulk_join_btn.setToolTip(tr("Add as join — select exactly one"))
            self._bulk_join_btn.setEnabled(False)
            self._bulk_join_btn.clicked.connect(self._bulk_join)
            bulk_inner.addWidget(self._bulk_join_btn)

            bulk_inner.addStretch()

            bulk_clear_btn = QPushButton(tr("✕ clear"))
            bulk_clear_btn.clicked.connect(self._bulk_clear)
            bulk_inner.addWidget(bulk_clear_btn)

            bulk_bar.addWidget(self._bulk_bar_widget)
            rv.addLayout(bulk_bar)

            # --- Pagination row (hidden until filtered > _PER_PAGE) ---
            pag_row = QHBoxLayout()
            pag_row.setSpacing(4)
            # Feature 7: RTL-correct glyphs — Prev points right (→), Next points left (←)
            self.btn_prev = QPushButton(tr("Prev →"))
            self.btn_prev.setFixedWidth(60)
            self.btn_prev.setVisible(False)  # hidden until needed
            self.btn_prev.clicked.connect(self._prev_page)
            pag_row.addWidget(self.btn_prev)

            self.page_lbl = QLabel("")
            self.page_lbl.setVisible(False)  # hidden until needed
            pag_row.addWidget(self.page_lbl)

            self.btn_next = QPushButton(tr("← Next"))
            self.btn_next.setFixedWidth(60)
            self.btn_next.setVisible(False)  # hidden until needed
            self.btn_next.clicked.connect(self._next_page)
            pag_row.addWidget(self.btn_next)

            pag_row.addStretch()
            rv.addLayout(pag_row)

            # G-13.1: distinct subtly-styled hint line near the grid (separate from the {n}/{m} counter).
            # Shown only when toggle ON + results; hidden when toggle OFF or empty intersection.
            self.vs_hint = QLabel("👁 " + tr("Turn off Visual Similarity to see more results"))
            self.vs_hint.setStyleSheet("font-size:10px;color:#64748b;font-style:italic;")
            self.vs_hint.setVisible(False)   # driven by apply_filters based on _vs_on + filtered
            rv.addWidget(self.vs_hint)

            # --- Grid view (default) ---
            self.grid_scroll = QScrollArea()
            self.grid_scroll.setWidgetResizable(True)
            self._grid_container = QWidget()
            self.grid_layout = QGridLayout(self._grid_container)
            self.grid_layout.setSpacing(4)
            self.grid_scroll.setWidget(self._grid_container)
            rv.addWidget(self.grid_scroll, 1)

            # --- Table view (hidden by default) ---
            # Feature 10: Source column removed (always "text" in Phase 108).
            # Column layout: 0 checkbox, 1 Shelfmark, 2 Score, 3 Snippet,
            #                4 Material, 5 Dimensions, 6 Page, 7 Triage  (8 cols total)
            _headers = [
                "",  # col 0: checkbox (master select-all on header click)
                tr("Shelfmark"), tr("Score"), tr("Snippet"),
                tr("Material"), tr("Dimensions"),
                tr("Page"), tr("Triage"),
            ]
            self.table = QTableWidget(0, len(_headers))
            self.table.setHorizontalHeaderLabels(_headers)
            self.table.setColumnWidth(0, 30)
            self.table.setEditTriggers(
                QTableWidget.EditTrigger.NoEditTriggers
            )
            self.table.setSelectionBehavior(
                QTableWidget.SelectionBehavior.SelectRows
            )
            self.table.setSortingEnabled(False)
            self.table.cellDoubleClicked.connect(self._table_double_clicked)
            # Master select-all: clicking the checkbox column header toggles all rows
            self.table.horizontalHeader().sectionClicked.connect(
                self._on_table_header_clicked
            )
            self.table.setVisible(False)
            rv.addWidget(self.table, 1)

        # --- RR-14: global ja/flex/bidir merge helper ---

        def _merge_globals(self, builder, ro: dict) -> dict:
            """Merge builder's ja/flex_spacing/bidirectional into ro.

            compose() hardcodes ja/flex/bidir=False in the returned ro; this step
            pulls the actual UI-toggle values back in so the global Search-Options
            actually reach the engine (RR-14). variants flows correctly via
            SideQuery.variants -> compose -> ro so it is NOT re-merged here.
            """
            overrides = {
                k: v
                for k, v in builder._responsa_opts().items()
                if k in ("ja", "flex_spacing", "bidirectional")
            }
            ro.update(overrides)
            return ro

        # --- Search flow ---

        def do_search(self):
            """Build and launch the main search (R-01 — ONE engine call per find)."""
            from shared.joins_lab import compose
            if self.builder.is_empty():
                try:
                    self.status.setText(
                        tr("Build a line-by-line query, then Find Candidates.")
                    )
                except RuntimeError:
                    pass
                return

            side = self.builder.build_side_query()
            if side is None:
                return
            try:
                query_str, ro, page_pos = compose(side)
            except ValueError as exc:
                try:
                    self.status.setText(str(exc))
                except RuntimeError:
                    pass
                return

            # RR-14: merge the builder's real ja/flex/bidir into the composed ro
            # (compose hardcodes them False at :745-747 in joins_lab.py)
            self._merge_globals(self.builder, ro)

            self._text_cands = None
            # Clear selection on new search (adapted_decision 6)
            self._selected_keys.clear()
            self._update_bulk_bar()
            try:
                self.status.setText(tr("working…"))
                self.btn_find.setEnabled(False)
            except RuntimeError:
                pass

            # Cancel any previous search thread
            if self._search_thread is not None:
                try:
                    self._search_thread.quit()
                except Exception:
                    pass
                self._search_thread = None

            # R-01: page_position forwarded as text_position; genizah scope only
            self._search_thread = SearchThread(
                self.wb.searcher,
                query_str,
                "exact",
                0,
                responsa_options=ro,
                text_position=page_pos,
                corpus_scope="genizah",
            )
            self._search_thread.results_signal.connect(self._on_results)
            self._search_thread.start()

        def _on_results(self, raw: list):
            """Handle raw engine results; dedup and optionally start cross-side worker."""
            from shared.joins_lab import compose, detect_self_match, dedup_candidates
            try:
                self.btn_find.setEnabled(True)
            except RuntimeError:
                pass

            # Anchor is excluded by default (adapted_decision 11: hardcoded)
            include_self = False
            self._anchor_matched = detect_self_match(raw, self.wb._anchor_sid)
            deduped, _ = dedup_candidates(raw, self.wb._anchor_sid, include_self)
            self._text_cands = list(deduped)

            # Other-side cross-filter?
            use_other = (
                self.other_enable.isChecked()
                and not self.other_builder.is_empty()
            )
            if use_other:
                b_side = self.other_builder.build_side_query()
                if b_side is not None:
                    try:
                        _b_str, b_ro, _b_pos = compose(b_side)
                    except ValueError:
                        self._maybe_assemble()
                        return
                    # RR-14: merge the OTHER builder's ja/flex/bidir into b_ro
                    self._merge_globals(self.other_builder, b_ro)

                    combine = (
                        "AND" if self.combine_combo.currentIndex() == 0 else "OR"
                    )
                    a_pattern = (
                        self._text_cands[0].highlight_pattern
                        if self._text_cands else None
                    )
                    # Cancel old cross-side worker
                    if self._cross_worker is not None:
                        try:
                            self._cross_worker.cancel()
                        except Exception:
                            pass
                    self._cross_worker = _CrossSideWorker(
                        self.executor,
                        self._text_cands,
                        b_side,
                        b_ro,        # MERGED b_ro (RR-14)
                        combine,
                        a_pattern,
                    )
                    self._cross_worker.done.connect(self._on_cross_done)
                    self._cross_worker.start()
                    return
            self._maybe_assemble()

        def _on_cross_done(self, merge_result):
            """Handle cross-side worker result (MergeResult — .candidates is correct here)."""
            self._text_cands = list(merge_result.candidates)  # MergeResult.candidates
            self._maybe_assemble()

        # ------------------------------------------------------------------ #
        # Phase 109 G-04 — VS toggle helpers (replaces 3-radio model)      #
        # ------------------------------------------------------------------ #

        def _on_vs_toggle(self, checked: bool):
            """Handle the VS toggle button toggled signal (G-04 — boolean state machine).

            ON:  load/refresh the CURRENT anchor's VS set (D-01 auto-load), then assemble.
                 assemble handles both the empty-box pure-VS case and the with-term
                 intersection case from self._vs_on + has_term.
            OFF: keep self._vs_cands (HIGH-1 — OFF-mode ★both badges still need the set);
                 reassemble text-only-with-badges.
            """
            self._vs_on = bool(checked)
            if checked:
                self._ensure_vs_loaded_for_anchor(silent=False)
                self._maybe_assemble()
            else:
                # Do NOT null _vs_cands — keep the CURRENT anchor's set for OFF-mode badges
                # (HIGH-1 / G-04 bullet 4). Re-render text-only-with-badges.
                self._maybe_assemble()

        def _ensure_vs_loaded_for_anchor(self, silent: bool = False):
            """HIGH-1: load + memoize the CURRENT anchor's VS set so OFF-mode badges are
            computed from the current anchor (not a stale prior anchor). Idempotent per
            wb._anchor_sid."""
            sid = self.wb._anchor_sid
            if not sid:
                self._vs_cands = None
                self._vs_loaded_sid = None
                return
            if self._vs_loaded_sid == sid and self._vs_cands is not None:
                return  # already loaded for this exact anchor
            self._vs_cands = self._load_visual_candidates(sid)  # [] when no VS (D-08) — fine
            self._vs_loaded_sid = sid
            if not silent:
                try:
                    if self._vs_cands:
                        self.status.setText(tr("Visual look-alikes loaded"))
                    else:
                        self.status.setText(tr("No visual similarity data for this manuscript"))
                except RuntimeError:
                    pass

        def _load_visual_candidates(self, anchor_sid, service=None):
            """Fetch + adapt the anchor's VS look-alikes into list[Candidate] (D-01/D-05).

            Review #3: the D-14a parity invariant drives THIS helper (inject `service` in tests).
            Review #5: shelfmark/title/library_code are batch-enriched from meta_mgr.csv_bank
            (O(1) dict lookups, no network/SQL per candidate) and fall back to str(alma_id) in
            the shim when csv_bank lacks the row -> cards never render blank.
            """
            from shared.joins_lab import normalize_candidate
            if not anchor_sid:
                return []
            if service is None:
                from shared.visual_similarity_service import get_vs_service
                service = get_vs_service()  # thread_safe=True default — safe; cheap local SQL
            if not service.is_available() or not service.has_suggestions(anchor_sid):
                return []                           # D-08 — no VS data
            raw = service.get_suggestions(anchor_sid, 200)   # D-05: full set, no cap, no floor
            csv_bank = {}
            try:
                if getattr(self.wb, "meta_mgr", None) is not None:
                    csv_bank = self.wb.meta_mgr.csv_bank or {}  # review #5 batch source
            except Exception:
                csv_bank = {}
            out = []
            for row in raw:
                meta = csv_bank.get(row["alma_id"]) or {}
                out.append(normalize_candidate(_normalize_vs_row(
                    row,
                    shelfmark=meta.get("shelfmark", ""),   # shim falls back to str(alma_id) if ""
                    title=meta.get("title", ""),
                    library_code=meta.get("library_code", ""),
                )))
            return out

        def _load_vs(self):
            """D-01: store the anchor's VS look-alikes on self._vs_cands + update status.

            Now delegates to _ensure_vs_loaded_for_anchor (centralises load + staleness key).
            """
            self._ensure_vs_loaded_for_anchor(silent=False)

        def _on_anchor_set(self):
            """Enable/disable the VS toggle based on whether the anchor has VS data (D-08),
            apply any pending source request (HIGH-3 / BLOCKER A), and reassemble for the
            new anchor (HIGH-2)."""
            from shared.visual_similarity_service import get_vs_service
            svc = get_vs_service()
            has_vs = (
                bool(self.wb._anchor_sid)
                and svc.is_available()
                and svc.has_suggestions(self.wb._anchor_sid)
            )
            try:
                self.btn_vs_toggle.setEnabled(has_vs)
                if not has_vs and self.btn_vs_toggle.isChecked():
                    self.btn_vs_toggle.setChecked(False)
                    self._vs_on = False
            except RuntimeError:
                pass
            # HIGH-1: load the CURRENT anchor's VS whenever available, even toggle OFF, for badges.
            if has_vs:
                self._ensure_vs_loaded_for_anchor(silent=True)
            # HIGH-3 / BLOCKER A: apply any pending source request now that VS availability is
            # known for THIS anchor, and clear the pending flag ONLY if it was actually applied.
            pending = self._pending_vs
            if pending is not None:
                applied = self.apply_source("visual" if pending else "text")
                if applied:
                    self._pending_vs = None   # clear ONLY after the request was actually applied
            # HIGH-2: reassemble for the NEW anchor under the current toggle (re-anchor reload).
            self._maybe_assemble()

        def apply_source(self, source: str) -> bool:
            """Switch the candidate source to 'source'. Returns True iff actually applied.

            HIGH-3: queries has_suggestions() directly for 'anchor HAS VS' — NOT btn_vs_toggle.isEnabled().
            set_source uses the return value to guard the pending-clear (BLOCKER A).
            'combined' is a synonym for 'visual' (maps to toggle ON) for forward-caller compatibility.
            """
            want_on = source in ("visual", "combined")
            if want_on:
                from shared.visual_similarity_service import get_vs_service
                svc = get_vs_service()
                has_vs = (
                    bool(self.wb._anchor_sid)
                    and svc.is_available()
                    and svc.has_suggestions(self.wb._anchor_sid)
                )
                if not has_vs:
                    # D-08: new anchor has no VS — stay OFF (toggle greyed by _on_anchor_set)
                    try:
                        if self.btn_vs_toggle.isChecked():
                            self.btn_vs_toggle.setChecked(False)
                        self._vs_on = False
                    except RuntimeError:
                        pass
                    return False   # NOT applied -> set_source keeps _pending_vs for _on_anchor_set
                try:
                    if self.btn_vs_toggle.isChecked():
                        self._on_vs_toggle(True)        # already checked -> no toggled signal; call directly
                    else:
                        self.btn_vs_toggle.setChecked(True)  # fires toggled -> _on_vs_toggle
                except RuntimeError:
                    return False
                return True
            else:  # "text"
                try:
                    self.btn_vs_toggle.setChecked(False)
                    self._vs_on = False
                except RuntimeError:
                    return False
                return True

        # ------------------------------------------------------------------ #
        # End Phase 109 VS helpers                                           #
        # ------------------------------------------------------------------ #

        def _maybe_assemble(self):
            """Merge sources based on the boolean VS toggle and assemble results.

            G-04 toggle model (Task 1/2):
              - toggle ON  + empty box -> pure VS (merge_candidates([], vs))
              - toggle ON  + term      -> INTERSECTION only (👁 eye: via_text AND via_vs)
              - toggle OFF             -> text-with-VS-badges; VS-only rows excluded
            Tracks _empty_intersection flag for MEDIUM-1 empty-state message in apply_filters.
            """
            from shared.joins_lab import merge_candidates
            text = self._text_cands or []
            vs = self._vs_cands or []
            has_term = (not self.builder.is_empty()) or bool(text)
            self._empty_intersection = False  # MEDIUM-1: drives the empty-state message in apply_filters
            if self._vs_on and not has_term:
                # toggle ON + empty box -> pure VS look-alikes (G-04 bullet 1)
                merged = list(merge_candidates([], vs))
            elif self._vs_on and has_term:
                # toggle ON + term -> INTERSECTION only (★both); NOT the old union (G-04 bullets 2,3)
                merged_all = merge_candidates(text, vs)
                merged = [c for c in merged_all if c.via_text and c.via_vs]
                if not merged:
                    self._empty_intersection = True   # MEDIUM-1
            else:
                # toggle OFF -> text-only rows, but text candidates that are ALSO VS look-alikes keep
                # the VS/👁 eye badge (G-04 bullet 4). merge_candidates annotates via_vs on text rows
                # that appear in vs; we then DROP VS-only rows.
                merged_all = merge_candidates(text, vs)
                merged = [c for c in merged_all if c.via_text]  # text-only + ★both, never VS-only
            self.results = list(merged)
            self._page = 0
            self._start_enrich()

        def _retire_enrich_worker(self):
            """Crash-safely tear down the current _EnrichWorker before starting a new one.

            A QThread must NOT be destroyed while still running — doing so triggers Qt's
            "QThread: Destroyed while thread is still running" abort, which surfaces on Windows
            as exit code 0xC0000409 (STATUS_STACK_BUFFER_OVERRUN / MSVC fastfail). cancel() only
            sets a flag; run()'s in-flight measurement SQL batch keeps executing until it returns,
            so simply doing `self._enrich_worker = None` here drops the only Python reference and
            CPython refcounting deletes the C++ QThread mid-run -> hard crash. This reproduced as
            "toggle Visual Similarity OFF right after a search" (the search's enrich worker is
            still running when the toggle re-enters _start_enrich).

            Fix: cancel + disconnect the stale result, then RETAIN a still-running worker in
            self._retired_workers (reaped on its finished() signal) instead of dropping it. An
            already-finished worker is safe to release immediately.
            """
            old = self._enrich_worker
            self._enrich_worker = None
            if old is None:
                return
            try:
                old.cancel()
            except Exception:
                pass
            # Drop the stale result so a late emit from the cancelled worker cannot clobber the
            # new worker's enrichment dict.
            try:
                old.enriched.disconnect(self._on_enriched)
            except (TypeError, RuntimeError):
                pass
            try:
                running = old.isRunning()
            except RuntimeError:
                running = False
            if not running:
                return  # finished -> safe to let it be garbage-collected now
            # Still running: keep a reference until it actually finishes (no mid-run destruction).
            self._retired_workers.append(old)
            try:
                old.finished.connect(lambda w=old: self._reap_enrich_worker(w))
            except (TypeError, RuntimeError):
                pass
            # Guard the race where it finished between the isRunning() check and the connect.
            try:
                if not old.isRunning():
                    self._reap_enrich_worker(old)
            except RuntimeError:
                self._reap_enrich_worker(old)

        def _reap_enrich_worker(self, w):
            """Release a retired _EnrichWorker once its QThread has actually finished.

            Runs on the UI thread (finished() is delivered to the main thread), so mutating
            self._retired_workers here is safe. Retaining the reference until finished() is what
            prevents destroying a running QThread (Windows 0xC0000409)."""
            try:
                self._retired_workers.remove(w)
            except (ValueError, RuntimeError):
                pass

        def _start_enrich(self):
            """Start the batched enrichment worker."""
            from shared.fjms_service import get_fjms_service
            # Cancel old enrich worker (crash-safe teardown — see _retire_enrich_worker).
            self._retire_enrich_worker()

            fjms_svc = None
            try:
                fjms_svc = get_fjms_service()
            except Exception:
                pass

            # Anchor measurements for size-mismatch hint
            anchor_meas = {}
            if self.wb._anchor_sid:
                try:
                    from shared.fjms_service import get_fjms_service as _gsvc
                    svc = _gsvc()
                    batch = svc.get_measurement_summaries_batch([self.wb._anchor_sid])
                    anchor_meas = batch.get(self.wb._anchor_sid) or {}
                except Exception:
                    anchor_meas = {}
            # Cache for CompareDialog._fill_anchor (anchor material/dims on BOTH panes)
            self.wb._anchor_meas = anchor_meas

            if fjms_svc is not None and self.results:
                self._enrich_worker = _EnrichWorker(fjms_svc, self.results, anchor_meas)
                self._enrich_worker.enriched.connect(self._on_enriched)
                self._enrich_worker.start()
            else:
                # G-03: empty intersection renders an empty state here (apply_filters sets the
                # 'no matches' label) — never a perpetual spinner. No fjms service OR empty results.
                self._enrich = {}
                self.apply_filters()

        def _on_enriched(self, enrich: dict):
            """Handle enrichment result (keyed by (sys_id, page) — RR-2)."""
            self._enrich = enrich
            # Populate material filter from unique materials in enrich
            materials = sorted(set(
                v.get("material") or ""
                for v in enrich.values()
                if v.get("material")
            ))
            try:
                # Feature 9: store English value as userData; display HE label when HE UI.
                # apply_filters reads userData (English) for comparison so filter VALUE
                # stays English regardless of display language.
                current_mat_val = self.mat_filter.currentData() or self.mat_filter.currentText()
                self.mat_filter.blockSignals(True)
                self.mat_filter.clear()
                self.mat_filter.addItem(tr("any material"), "")  # userData="" = any
                for mat in materials:
                    self.mat_filter.addItem(material_display(mat), mat)  # display HE, value EN
                # Restore previous selection by userData
                restored = False
                for i in range(self.mat_filter.count()):
                    if self.mat_filter.itemData(i) == current_mat_val:
                        self.mat_filter.setCurrentIndex(i)
                        restored = True
                        break
                if not restored:
                    self.mat_filter.setCurrentIndex(0)
                self.mat_filter.blockSignals(False)
            except RuntimeError:
                pass
            self.apply_filters()

        def apply_filters(self):
            """Apply refine-bar filters and update the display."""
            text_q = (self.filter_in.text() if self.filter_in else "").strip().lower()
            # Feature 9: read English value from userData (display label may be Hebrew)
            mat_q = ""
            if self.mat_filter:
                mat_q = self.mat_filter.currentData()
                if mat_q is None:
                    mat_q = ""   # userData not set (legacy path)
            mat_any = (not mat_q)  # userData=="" means "any material"
            need_dims = self.dim_chk.isChecked()
            tri_q_idx = self.tri_filter.currentIndex()
            # tri_q_idx: 0=all, 1=Y, 2=?, 3=N, 4=untriaged
            _tri_map = {1: "yes", 2: "maybe", 3: "no"}

            size_active = self.size_btn.isChecked() if hasattr(self, "size_btn") else False
            size_min = self.size_min.value() if size_active else 0
            size_max = self.size_max.value() if size_active else 200

            filtered = []
            for c in self.results:
                # Text filter
                if text_q and not (
                    text_q in (c.shelfmark or "").lower()
                    or text_q in (c.full_text or "").lower()
                    or text_q in (c.title or "").lower()
                ):
                    continue
                # Material filter
                m = self._enrich.get(c.key) or {}
                if not mat_any:
                    if (m.get("material") or "") != mat_q:
                        continue
                # Has-dimensions filter
                if need_dims and not (m.get("width_cm") and m.get("height_cm")):
                    continue
                # Triage filter
                if tri_q_idx == 4:
                    # untriaged
                    if self.wb.triage.get(c.sys_id):
                        continue
                elif tri_q_idx in _tri_map:
                    if self.wb.triage.get(c.sys_id) != _tri_map[tri_q_idx]:
                        continue
                # Size filter (opt-in)
                if size_active:
                    w = m.get("width_cm")
                    if w is None or not (size_min <= w <= size_max):
                        continue
                filtered.append(c)

            self.wb.filtered = filtered

            # Prune selection to current filtered universe (adapted_decision 6)
            if hasattr(self, "_selected_keys"):
                filtered_keys = {self._candidate_key(c) for c in filtered}
                self._selected_keys &= filtered_keys

            try:
                if getattr(self, "_empty_intersection", False) and not filtered:
                    # G-13.3: combined empty-intersection message takes precedence (no bare "0/0 shown",
                    # no never-resolving spinner — MEDIUM-1). The bare key is retained as a fallback
                    # reference so test_empty_intersection_status_message stays green.
                    _ = tr("No look-alikes match this search")  # MEDIUM-1 fallback key retained  # noqa: F841
                    self.status.setText(
                        tr("No look-alikes match this search — turn off Visual Similarity to see all results")
                    )
                    self.vs_hint.setVisible(False)   # combined message already carries "turn off" advice
                else:
                    self.status.setText(
                        f"{len(filtered)}/{len(self.results)} " + tr("shown")
                    )
                    # G-13.2: hint whenever toggle is ON and results are shown (pure-VS OR intersection).
                    self.vs_hint.setVisible(bool(getattr(self, "_vs_on", False)) and bool(filtered))
            except RuntimeError:
                pass

            self.render_results()
            self._update_status_counts()

        def render_results(self):
            """Dispatch to the current view mode."""
            if self.view_mode == "grid":
                self._render_grid_page()
            else:
                self._render_table()

        def _render_grid_page(self):
            """Render the current page of candidates into the grid (QGridLayout)."""
            # Clear old cards
            self.wb._cancel_images()
            self.cards.clear()
            # Remove all widgets from grid
            while self.grid_layout.count():
                item = self.grid_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

            start = self._page * _PER_PAGE
            page_cands = self.wb.filtered[start:start + _PER_PAGE]
            items_for_thumbs = []
            for i, c in enumerate(page_cands):
                gidx = start + i
                enrich = self._enrich
                card = CandidateCard(self, c, gidx, enrich)
                self.cards[gidx] = card
                card.load_vs_text()   # G-02: page-lazy VS card text fetch (no-op for non-VS / already-texted)
                self.grid_layout.addWidget(card, i // _GRID_COLS, i % _GRID_COLS)
                items_for_thumbs.append((gidx, c.sys_id))

            # Start ThumbResolver for this page
            if items_for_thumbs and self.wb.meta_mgr is not None:
                if self._resolver is not None:
                    try:
                        self._resolver.cancel()
                    except Exception:
                        pass
                self._resolver = ThumbResolver(self.wb.meta_mgr, items_for_thumbs)
                self._resolver.resolved.connect(self._on_thumb_url)
                self._resolver.start()

            self._update_pagination()

        def _on_thumb_url(self, card_idx: int, url: str):
            """Handle a ThumbResolver URL — enqueue image load on GUI thread."""
            card = self.cards.get(card_idx)
            if card is None:
                return
            if not url:
                try:
                    card.img.setText(tr("(no image)"))
                except RuntimeError:
                    pass
                return
            # Load via the bounded pool; use "card" target for scaled display
            loader = ImageLoaderThread(url)
            def _on_loaded(qi, c=card):
                try:
                    if qi is not None and not qi.isNull():
                        pix = QPixmap.fromImage(qi)
                        c.set_pixmap(pix)
                    else:
                        c.set_pixmap(None)
                except RuntimeError:
                    pass
            loader.image_loaded.connect(_on_loaded)
            loader.load_failed.connect(lambda c=card: c.set_pixmap(None))
            loader.start()
            self.wb._img_threads.append(loader)

        def _render_table(self):
            """Render all filtered candidates into the table view.

            Feature 10: Source column removed (was col 6; always "text" in Phase 108).
            Column layout: 0 checkbox, 1 Shelfmark, 2 Score, 3 Snippet,
                           4 Material, 5 Dimensions, 6 Page, 7 Triage  (8 cols total)
            Feature 12: Snippet column uses QLabel with highlight markup (col 3).
            """
            self.table.setRowCount(0)
            for c in self.wb.filtered:
                m = self._enrich.get(c.key) or {}
                row = self.table.rowCount()
                self.table.insertRow(row)

                # Col 0: selection checkbox (block signals while initializing)
                chk_item = QTableWidgetItem()
                ckey = self._candidate_key(c)
                chk_item.setData(Qt.ItemDataRole.UserRole, ckey)
                chk_item.setCheckState(
                    Qt.CheckState.Checked if ckey in self._selected_keys
                    else Qt.CheckState.Unchecked
                )
                self.table.setItem(row, 0, chk_item)

                # Data columns (offset +1); Source column removed (Feature 10)
                # Round-4: shelfmark carries the same 👁 / ⚓ / ⇄ badge as the grid card.
                shelf_text, eye_tip = _candidate_shelf_badge(c)
                shelf_item = QTableWidgetItem(shelf_text)
                if eye_tip:
                    shelf_item.setToolTip(eye_tip)   # G-06.2 "visual similarity"
                self.table.setItem(row, 1, shelf_item)
                score_str = f"{c.score:.3f}" if c.score is not None else ""
                self.table.setItem(row, 2, QTableWidgetItem(score_str))
                # Feature 12: snippet with highlight markup via QLabel
                snip_html = m.get("snippet_html") or ""
                snip_plain = m.get("snippet_plain") or ""
                if snip_html:
                    snip_lbl = QLabel()
                    snip_lbl.setTextFormat(Qt.TextFormat.RichText)
                    snip_lbl.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
                    snip_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                    snip_lbl.setWordWrap(False)
                    snip_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                    snip_lbl.setText(f'<span dir="rtl">{snip_html}</span>')
                    self.table.setCellWidget(row, 3, snip_lbl)
                else:
                    self.table.setItem(row, 3, QTableWidgetItem(snip_plain))
                mat_val = m.get("material") or ""
                self.table.setItem(row, 4, QTableWidgetItem(material_display(mat_val)))
                w = m.get("width_cm")
                h = m.get("height_cm")
                dims = f"{w:.0f}x{h:.0f}" if w and h else ""
                self.table.setItem(row, 5, QTableWidgetItem(dims))
                self.table.setItem(row, 6, QTableWidgetItem(str(c.page) if c.page else ""))
                self.table.setItem(
                    row, 7, QTableWidgetItem(_TRIAGE_GLYPH.get(self.wb.triage.get(c.sys_id), ""))
                )

            # Wire cellChanged to update selection set (must connect AFTER setRowCount(0))
            try:
                self.table.cellChanged.disconnect(self._on_table_cell_changed)
            except Exception:
                pass
            self.table.cellChanged.connect(self._on_table_cell_changed)

            self._update_pagination()

        def _on_table_cell_changed(self, row: int, col: int):
            """Handle checkbox column toggle (col 0) to update _selected_keys."""
            if col != 0:
                return
            item = self.table.item(row, 0)
            if item is None:
                return
            ckey = item.data(Qt.ItemDataRole.UserRole)
            if not ckey:
                return
            checked = item.checkState() == Qt.CheckState.Checked
            if checked:
                self._selected_keys.add(ckey)
            else:
                self._selected_keys.discard(ckey)
            self._update_bulk_bar()

        def _on_table_header_clicked(self, section: int):
            """Master select-all/none on checkbox column header click (adapted_decision 8)."""
            if section != 0:
                return
            # Determine whether to select all or deselect all
            all_selected = all(
                self.table.item(r, 0) is not None and
                self.table.item(r, 0).checkState() == Qt.CheckState.Checked
                for r in range(self.table.rowCount())
            ) if self.table.rowCount() > 0 else False

            # Block cellChanged to batch-update efficiently
            try:
                self.table.cellChanged.disconnect(self._on_table_cell_changed)
            except Exception:
                pass

            new_state = Qt.CheckState.Unchecked if all_selected else Qt.CheckState.Checked
            for r in range(self.table.rowCount()):
                item = self.table.item(r, 0)
                if item is None:
                    continue
                item.setCheckState(new_state)
                ckey = item.data(Qt.ItemDataRole.UserRole)
                if ckey:
                    if new_state == Qt.CheckState.Checked:
                        self._selected_keys.add(ckey)
                    else:
                        self._selected_keys.discard(ckey)

            self.table.cellChanged.connect(self._on_table_cell_changed)
            self._update_bulk_bar()

        def _table_double_clicked(self, row: int, col: int):
            """Map table row to global index and open compare.

            Clicking column 0 (checkbox) must NOT open compare (adapted_decision 8).
            """
            if col == 0:
                return  # checkbox column — don't open compare
            # Table shows all filtered (no pagination), so row == global_filtered_idx
            self.open_compare(row)

        def _update_pagination_visibility(self):
            """Show prev/next row ONLY when filtered results span more than one page."""
            visible = len(self.wb.filtered) > _PER_PAGE
            try:
                self.btn_prev.setVisible(visible)
                self.page_lbl.setVisible(visible)
                self.btn_next.setVisible(visible)
            except RuntimeError:
                pass

        def _update_pagination(self):
            """Update prev/next buttons and page label, and hide row when not needed."""
            total = len(self.wb.filtered)
            total_pages = max(1, (total + _PER_PAGE - 1) // _PER_PAGE)
            self._update_pagination_visibility()
            try:
                self.btn_prev.setEnabled(self._page > 0)
                self.btn_next.setEnabled(self._page < total_pages - 1)
                self.page_lbl.setText(f"{self._page + 1}/{total_pages}")
            except RuntimeError:
                pass

        def _prev_page(self):
            if self._page > 0:
                self._page -= 1
                self.render_results()

        def _next_page(self):
            total = len(self.wb.filtered)
            total_pages = max(1, (total + _PER_PAGE - 1) // _PER_PAGE)
            if self._page < total_pages - 1:
                self._page += 1
                self.render_results()

        def toggle_view(self):
            """Toggle between grid and table views."""
            self.view_mode = "table" if self.view_mode == "grid" else "grid"
            try:
                self.view_btn.setText(
                    tr("Grid view") if self.view_mode == "table" else tr("Table view")
                )
                self.grid_scroll.setVisible(self.view_mode == "grid")
                self.table.setVisible(self.view_mode == "table")
            except RuntimeError:
                pass
            self.wb._cancel_images()
            self.render_results()

        def _clear_lab(self):
            """Clear anchor, builders, candidates, triage, selection, filter (Feature 5).

            Also wipes the persisted join_lab session state so a subsequent restore is empty.
            """
            # Reset anchor on the window
            self.wb._anchor_sid = None
            self.wb._anchor_res = None
            self.wb._anchor_images = []
            self.wb._anchor_idx = 0
            self.wb.triage = {}
            self.wb.filtered = []
            try:
                self.wb.anchor_shelf.setText("")
                self.wb.anchor_meta.setText("")
                self.wb.anchor_img_label.clear()
            except RuntimeError:
                pass

            # Reset builders — clear all rows, add one blank row back
            for builder in (self.builder, self.other_builder):
                # Remove all rows
                for entry in list(builder.rows):
                    try:
                        entry["widget"].setParent(None)
                        entry["widget"].deleteLater()
                    except RuntimeError:
                        pass
                builder.rows.clear()
                builder._global_opts = {
                    "variants": False,
                    "ja": False,
                    "flex_spacing": False,
                    "bidirectional": False,
                }
                builder.add_row(placeholder=builder._first_hint)

            # Reset other-side enable
            try:
                self.other_enable.setChecked(False)
                self.other_box.setVisible(False)
            except RuntimeError:
                pass

            # Reset candidates / selection
            self.results = []
            self._text_cands = None
            self._selected_keys.clear()
            self._enrich.clear()
            self._page = 0
            self._update_bulk_bar()

            # Reset filter controls to defaults
            try:
                self.filter_in.clear()
                self.mat_filter.setCurrentIndex(0)
                self.tri_filter.setCurrentIndex(0)
            except RuntimeError:
                pass

            # Re-render (empty)
            self.wb._cancel_images()
            self.render_results()
            self._update_pagination_visibility()
            try:
                self.status.setText(
                    tr("Build a line-by-line query, then Find Candidates.")
                )
            except RuntimeError:
                pass

            # Wipe persisted join_lab state (write empty state on next _save_session)
            try:
                from shared.session_persistence import load_session_state, save_session_state
                state = load_session_state() or {}
                state["join_lab"] = {"open": False}
                save_session_state(state)
            except Exception as exc:
                logger.debug("_clear_lab: could not wipe join_lab state: %s", exc)

        def _restyle_card(self, sys_id: str):
            """Restyle any visible card whose sys_id matches (triage-state change)."""
            for card in self.cards.values():
                if card.sid == sys_id:
                    try:
                        card._restyle()
                    except RuntimeError:
                        pass

        def _update_status_counts(self):
            """Append [Y x  ? y  N z] triage counts to the status label."""
            y = sum(1 for v in self.wb.triage.values() if v == "yes")
            m = sum(1 for v in self.wb.triage.values() if v == "maybe")
            n = sum(1 for v in self.wb.triage.values() if v == "no")
            try:
                base = self.status.text().split("  [")[0]
                self.status.setText(f"{base}  [Y {y}  ? {m}  N {n}]")
            except RuntimeError:
                pass

        def _toggle_size_filter(self, on: bool):
            """Kept for apply_filters compatibility — no visible widget to show/hide now."""
            if not on:
                self.apply_filters()

        # ------------------------------------------------------------------
        # Filter ▾ dialog
        # ------------------------------------------------------------------

        def _open_filter_dialog(self):
            """Open the filter dialog (adapted_decision 12).

            Builds a fresh dialog each time, pre-populating controls from the
            persistent hidden filter widgets (filter_in, mat_filter, etc.).
            On Apply the hidden widgets are updated + apply_filters is called.
            Anchor info panel shows known fields from wb anchor state.
            """
            dlg = QDialog(self)
            dlg.setWindowTitle(tr("Filter candidates"))
            lay = QVBoxLayout(dlg)
            lay.setSpacing(6)

            # --- Current fragment info panel ---
            anchor_meta = {}
            if self.wb._anchor_sid:
                try:
                    anchor_meta = self.wb.meta_mgr.enrich_metadata(self.wb._anchor_sid) or {}
                except Exception:
                    anchor_meta = {}
            anchor_shelf = ""
            if self.wb._anchor_res:
                from desktop.join_workbench import r_shelf
                anchor_shelf = r_shelf(self.wb._anchor_res)

            # Gather known fields from enrich dict for the anchor
            anchor_enrich = self.wb._candidate_pane._enrich.get(
                (self.wb._anchor_sid, None)
            ) if hasattr(self.wb, "_candidate_pane") else {}
            # Try measurement data from the pane's enrich cache
            anchor_meas_w = None
            anchor_meas_mat = None
            for key, ev in (self._enrich or {}).items():
                if isinstance(key, tuple) and len(key) >= 1 and key[0] == self.wb._anchor_sid:
                    anchor_meas_w = ev.get("width_cm")
                    anchor_meas_mat = ev.get("material")
                    break

            if anchor_shelf or self.wb._anchor_sid:
                info_grp = QWidget()
                info_lay = QVBoxLayout(info_grp)
                info_lay.setContentsMargins(6, 4, 6, 4)
                info_lay.setSpacing(3)
                info_title = QLabel(
                    f"{tr('Current fragment')} — {anchor_shelf or self.wb._anchor_sid}"
                )
                info_title.setStyleSheet("font-weight:bold;font-size:11px;")
                info_lay.addWidget(info_title)

                # Show only known fields
                known_parts = []
                lib = anchor_meta.get("library_code") or ""
                if lib:
                    known_parts.append(lib)
                if anchor_meas_mat:
                    known_parts.append(str(anchor_meas_mat))
                if anchor_meas_w:
                    known_parts.append(f"{anchor_meas_w:.0f} cm")
                if known_parts:
                    chips_lbl = QLabel("  ·  ".join(known_parts))
                    chips_lbl.setStyleSheet("font-size:11px;color:#94a3b8;")
                    info_lay.addWidget(chips_lbl)

                # "from anchor" shortcuts
                if anchor_meas_mat or anchor_meas_w:
                    shortcuts_row = QHBoxLayout()
                    if anchor_meas_mat:
                        btn_mat = QPushButton(
                            tr("match material") + f" ({anchor_meas_mat})"
                        )
                        btn_mat.setFlat(True)
                        btn_mat.clicked.connect(
                            lambda _checked=False, m=anchor_meas_mat: _set_mat(m)
                        )
                        shortcuts_row.addWidget(btn_mat)
                    if anchor_meas_w:
                        btn_w = QPushButton(tr("width ±2 cm of anchor"))
                        btn_w.setFlat(True)
                        btn_w.clicked.connect(
                            lambda _checked=False, w=anchor_meas_w: _set_width_range(w)
                        )
                        shortcuts_row.addWidget(btn_w)
                    shortcuts_row.addStretch()
                    info_lay.addLayout(shortcuts_row)

                info_grp.setStyleSheet(
                    "QWidget{border:1px solid #3c3c3c;border-radius:6px;background:#181818;}"
                )
                lay.addWidget(info_grp)

            # --- Filter controls ---
            dlg_filter_in = QLineEdit()
            dlg_filter_in.setPlaceholderText(tr("Filter by shelfmark, text, or title…"))
            dlg_filter_in.setText(self.filter_in.text())
            lay.addWidget(dlg_filter_in)

            mat_row = QHBoxLayout()
            mat_row.addWidget(QLabel(tr("Material") + ":"))
            # Feature 9: mirror the mat_filter items including userData (English) + display (HE)
            dlg_mat = QComboBox()
            dlg_mat.addItem(tr("any material"), "")
            current_mat_items = [
                (self.mat_filter.itemText(i), self.mat_filter.itemData(i) or self.mat_filter.itemText(i))
                for i in range(1, self.mat_filter.count())
            ]
            for label, val in current_mat_items:
                dlg_mat.addItem(label, val)
            # Restore selection by userData
            current_val = self.mat_filter.currentData() or ""
            for i in range(dlg_mat.count()):
                if dlg_mat.itemData(i) == current_val:
                    dlg_mat.setCurrentIndex(i)
                    break
            mat_row.addWidget(dlg_mat, 1)
            lay.addLayout(mat_row)

            dlg_dim = QCheckBox(tr("Has dimensions"))
            dlg_dim.setChecked(self.dim_chk.isChecked())
            lay.addWidget(dlg_dim)

            tri_row = QHBoxLayout()
            tri_row.addWidget(QLabel(tr("Triage") + ":"))
            dlg_tri = QComboBox()
            dlg_tri.addItems([
                tr("all triage"), tr("Y — kept"), tr("? — maybe"),
                tr("N — dismissed"), tr("untriaged"),
            ])
            dlg_tri.setCurrentIndex(self.tri_filter.currentIndex())
            tri_row.addWidget(dlg_tri, 1)
            lay.addLayout(tri_row)

            sep2 = QFrame()
            sep2.setFrameShape(QFrame.Shape.HLine)
            sep2.setFrameShadow(QFrame.Shadow.Sunken)
            lay.addWidget(sep2)

            dlg_size_chk = QCheckBox(tr("Filter by width (cm)"))
            dlg_size_chk.setChecked(self.size_btn.isChecked())
            lay.addWidget(dlg_size_chk)

            size_row = QHBoxLayout()
            dlg_size_min = QSpinBox()
            dlg_size_min.setRange(0, 200)
            dlg_size_min.setValue(self.size_min.value())
            dlg_size_min.setPrefix(tr("min") + " ")
            size_row.addWidget(dlg_size_min)
            dlg_size_max = QSpinBox()
            dlg_size_max.setRange(0, 200)
            dlg_size_max.setValue(self.size_max.value())
            dlg_size_max.setPrefix(tr("max") + " ")
            size_row.addWidget(dlg_size_max)
            size_row.addStretch()
            lay.addLayout(size_row)

            # Note text
            note = QLabel(tr("Size filter note"))
            note.setStyleSheet("font-size:10px;color:#6b7280;")
            note.setWordWrap(True)
            lay.addWidget(note)

            # "from anchor" shortcut helpers for the dialog's spinboxes
            def _set_mat(m):
                # Feature 9: find by userData (English value), not display text
                for i in range(dlg_mat.count()):
                    if (dlg_mat.itemData(i) or "") == m:
                        dlg_mat.setCurrentIndex(i)
                        return
                # Fallback: match by text (handles legacy / unknown materials)
                idx2 = dlg_mat.findText(m)
                if idx2 >= 0:
                    dlg_mat.setCurrentIndex(idx2)

            def _set_width_range(w):
                dlg_size_chk.setChecked(True)
                dlg_size_min.setValue(max(0, int(w) - 2))
                dlg_size_max.setValue(min(200, int(w) + 2))

            btn_row = QHBoxLayout()
            btn_reset = QPushButton(tr("Reset"))
            btn_apply = QPushButton(tr("Apply"))
            btn_apply.setDefault(True)
            btn_row.addWidget(btn_reset)
            btn_row.addStretch()
            btn_row.addWidget(btn_apply)
            lay.addLayout(btn_row)

            def _on_reset():
                dlg_filter_in.setText("")
                dlg_mat.setCurrentIndex(0)
                dlg_dim.setChecked(False)
                dlg_tri.setCurrentIndex(0)
                dlg_size_chk.setChecked(False)
                dlg_size_min.setValue(0)
                dlg_size_max.setValue(200)

            def _on_apply():
                # Write back to persistent hidden filter widgets
                self.filter_in.setText(dlg_filter_in.text())
                # Sync material combo by userData (Feature 9: English value)
                self.mat_filter.blockSignals(True)
                chosen_val = dlg_mat.currentData() or ""
                synced = False
                for i in range(self.mat_filter.count()):
                    if (self.mat_filter.itemData(i) or "") == chosen_val:
                        self.mat_filter.setCurrentIndex(i)
                        synced = True
                        break
                if not synced:
                    self.mat_filter.setCurrentIndex(0)
                self.mat_filter.blockSignals(False)
                self.dim_chk.setChecked(dlg_dim.isChecked())
                self.tri_filter.setCurrentIndex(dlg_tri.currentIndex())
                self.size_btn.setChecked(dlg_size_chk.isChecked())
                self.size_min.setValue(dlg_size_min.value())
                self.size_max.setValue(dlg_size_max.value())
                self.apply_filters()
                dlg.accept()

            btn_reset.clicked.connect(_on_reset)
            btn_apply.clicked.connect(_on_apply)
            dlg.exec()

        # ------------------------------------------------------------------
        # "Browse results ▶" + bulk actions + selection management
        # ------------------------------------------------------------------

        def _candidate_key(self, c) -> str:
            """Return a stable string key for a Candidate (sys_id + page)."""
            return f"{c.sys_id}:{c.page}"

        def _set_selected(self, key: str, checked: bool):
            """Toggle a candidate key in _selected_keys and update bulk bar."""
            if checked:
                self._selected_keys.add(key)
            else:
                self._selected_keys.discard(key)
            self._update_bulk_bar()

        def _update_bulk_bar(self):
            """Show/hide the bulk bar; update count label + enabled state."""
            n = len(self._selected_keys)
            try:
                self._bulk_bar_widget.setVisible(n > 0)
                self._bulk_count_lbl.setText(f"{n} {tr('selected')}")
                # Browse and Join enabled only for exactly ONE selection
                self._bulk_browse_btn.setEnabled(n == 1)
                self._bulk_join_btn.setEnabled(n == 1)
            except RuntimeError:
                pass

        def _bulk_browse(self):
            """Bulk Browse — single selection only; opens browse for that candidate."""
            if len(self._selected_keys) != 1:
                return
            key = next(iter(self._selected_keys))
            for c in self.wb.filtered:
                if self._candidate_key(c) == key:
                    self.wb.open_result_in_browse(c)
                    return

        def _bulk_puzzle(self):
            """Bulk puzzle — adds anchor + all selected candidates (adapted_decision 7)."""
            sids = []
            for c in self.wb.filtered:
                if self._candidate_key(c) in self._selected_keys:
                    sids.append(c.sys_id)
            if sids and self.wb._anchor_sid:
                from desktop.join_workbench import puzzle_add_targets
                for sid in puzzle_add_targets(self.wb._anchor_sid, sids):
                    self.wb._app.open_anchor_in_puzzle(sid)

        def _bulk_list(self):
            """Bulk add-to-list — all selected candidates."""
            items = []
            for c in self.wb.filtered:
                if self._candidate_key(c) in self._selected_keys:
                    items.append({"sys_id": c.sys_id, "fl_id": "", "img": c.page or 1})
            if items:
                self.wb._app.show_add_to_list_menu(items, source="join_workbench")

        def _bulk_join(self):
            """Bulk join — single selection only; opens join dialog."""
            if len(self._selected_keys) != 1:
                return
            key = next(iter(self._selected_keys))
            for c in self.wb.filtered:
                if self._candidate_key(c) == key:
                    self.wb.open_result_as_join(c)
                    return

        def _bulk_clear(self):
            """Clear all selections and re-render."""
            self._selected_keys.clear()
            self._update_bulk_bar()
            self.render_results()

        def _browse_results(self):
            """Open CompareDialog over filtered list (adapted_decision 13).

            If exactly one candidate is selected, start at that candidate;
            otherwise start at the first result.
            """
            if not self.wb.filtered:
                return
            start_idx = 0
            if len(self._selected_keys) == 1:
                key = next(iter(self._selected_keys))
                for i, c in enumerate(self.wb.filtered):
                    if self._candidate_key(c) == key:
                        start_idx = i
                        break
            self.open_compare(start_idx)

        def open_compare(self, global_idx: int):
            """Open the side-by-side CompareDialog for a candidate (JWB-08).

            Creates a modeless CompareDialog and shows it; keeps a reference so it
            is not garbage-collected while open.
            """
            self._compare = CompareDialog(self.wb, global_idx)
            self._compare.show()

    # -------------------------------------------------------------------------
    # Plan 04 — CompareDialog (QDialog) — two-pane side-by-side compare (JWB-08).
    # -------------------------------------------------------------------------

    class CompareDialog(QDialog):
        """Modeless two-pane compare dialog: anchor (left) vs candidate (right).

        D-16: carries its OWN anchor pane — the Phase-107 pinned pane is NOT reused,
        so the workbench stays usable behind the dialog.
        RR-2: the filtered list holds Candidate dataclasses; reads candidate.* attributes
        directly.  The anchor (wb._anchor_res) is the raw Phase-107 DICT — read with r_*.
        RR-12: c.page (Optional[int]) is passed STRAIGHT to _enqueue_image_for_pane, which
        guards None internally — no page-1 arithmetic here.
        D-20: all actions route through Phase-107 public methods (no _vs_* calls).
        """

        def __init__(self, wb, start_idx: int):
            super().__init__(wb)
            self.wb = wb
            self.idx = max(0, min(start_idx, max(0, len(wb.filtered) - 1)))
            self.setWindowTitle(tr("Compare"))
            self.resize(1320, 870)
            # Modeless child; add maximize button hint (UI-SPEC Surface 9)
            self.setWindowFlags(
                self.windowFlags() | Qt.WindowType.WindowMaximizeButtonHint
            )
            self.setModal(False)

            v = QVBoxLayout(self)
            v.setSpacing(4)

            # ── Top bar: prev/next nav + position label + Y/?/N triage ──────
            topbar = QHBoxLayout()
            # Feature 7: RTL-correct glyphs — prev points right (>), next points left (<)
            self.prev_btn = QPushButton(tr("prev >"))
            self.prev_btn.setFixedHeight(28)
            self.prev_btn.setMinimumWidth(84)   # round-4: 34px clipped the label text
            self.prev_btn.setAccessibleName(tr("Previous candidate"))
            self.prev_btn.clicked.connect(lambda: self.step(-1))
            topbar.addWidget(self.prev_btn)

            self.pos_lbl = QLabel("")
            topbar.addWidget(self.pos_lbl, 1)

            self.nxt_btn = QPushButton(tr("< next"))
            self.nxt_btn.setFixedHeight(28)
            self.nxt_btn.setMinimumWidth(84)   # round-4: 34px clipped the label text
            self.nxt_btn.setAccessibleName(tr("Next candidate"))
            self.nxt_btn.clicked.connect(lambda: self.step(1))
            topbar.addWidget(self.nxt_btn)

            for emoji, val, aname in (
                ("✓", "yes", tr("Mark yes")),
                ("?", "maybe", tr("Mark maybe")),
                ("✗", "no", tr("Mark no")),
            ):
                btn = QPushButton(emoji)
                btn.setAccessibleName(aname)
                btn.clicked.connect(lambda _, x=val: self._mark(x))
                topbar.addWidget(btn)
            v.addLayout(topbar)

            # ── Action row: Browse / Puzzle / List / Join / Re-anchor ────────
            arow = QHBoxLayout()
            browse_btn = QPushButton(tr("📖 Browse"))
            browse_btn.setAccessibleName(tr("📖 Browse"))
            browse_btn.clicked.connect(lambda: self.wb.open_result_in_browse(self._cur()))
            arow.addWidget(browse_btn)

            puzzle_btn = QPushButton(tr("🧩 Puzzle"))
            puzzle_btn.setAccessibleName(tr("🧩 Puzzle"))
            # Compare puzzle: anchor + candidate (adapted_decision 10)
            puzzle_btn.clicked.connect(lambda: self.wb.open_result_in_puzzle(self._cur()))
            arow.addWidget(puzzle_btn)
            # Note: open_result_in_puzzle already adds anchor+candidate via open_anchors_in_puzzle

            list_btn = QPushButton(tr("📋 Add to List"))
            list_btn.setAccessibleName(tr("📋 Add to List"))
            list_btn.clicked.connect(lambda: self.wb.open_result_in_list(self._cur(), None))
            arow.addWidget(list_btn)

            join_btn = QPushButton(tr("🔗 Add as Join"))
            join_btn.setAccessibleName(tr("🔗 Add as Join"))
            join_btn.clicked.connect(lambda: self.wb.open_result_as_join(self._cur()))
            arow.addWidget(join_btn)

            reanchor_btn = QPushButton(tr("⚓ Re-anchor"))
            reanchor_btn.setAccessibleName(tr("⚓ Re-anchor"))
            reanchor_btn.clicked.connect(self._reanchor)
            arow.addWidget(reanchor_btn)

            arow.addStretch()
            v.addLayout(arow)

            # ── Two-pane body ────────────────────────────────────────────────
            body = QHBoxLayout()
            self.left = self._pane()    # anchor pane
            self.right = self._pane()   # candidate pane
            body.addLayout(self.left["box"], 1)
            body.addLayout(self.right["box"], 1)
            v.addLayout(body, 1)

            # Paint initial state
            if wb.filtered:
                self.paint()

        def _pane(self) -> dict:
            """Factory: build one compare pane (VBoxLayout + widgets + folio + zoom).

            Feature 3: adds dims_lbl (material/dimensions line) and zoom − / + controls.
            Feature 7: folio glyphs RTL-corrected.
            Returns dict with keys: box, shelf, meta, dims_lbl,
            folio_prev, folio_lbl, folio_next, img, txt,
            sys_id (str), page (int), zoom (float), full_pix (QPixmap or None).
            """
            box = QVBoxLayout()
            shelf = QLabel()
            shelf.setStyleSheet("font-weight:bold;font-size:13px;")
            shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            shelf.setWordWrap(True)

            meta = QLabel()
            meta.setWordWrap(True)
            meta.setStyleSheet(f"font-size:11px;color:{_META_COLOR};")

            # Feature 3: material/dimensions line
            dims_lbl = QLabel()
            dims_lbl.setWordWrap(True)
            dims_lbl.setStyleSheet(f"font-size:10px;color:{_DIM_COLOR};")
            dims_lbl.setVisible(False)

            # Per-pane folio browse + zoom row (Features 4+3)
            # Feature 7: RTL glyphs — PREV points right (▶), NEXT points left (◀)
            ctrl_row = QHBoxLayout()
            ctrl_row.setSpacing(2)
            folio_prev = QPushButton("▶")
            folio_prev.setFixedSize(28, 22)
            folio_prev.setToolTip(tr("Previous folio"))
            folio_prev.setAccessibleName(tr("Previous folio"))
            folio_lbl = QLabel("p.1")
            folio_lbl.setStyleSheet(f"font-size:10px;color:{_META_COLOR};")
            folio_next = QPushButton("◀")
            folio_next.setFixedSize(28, 22)
            folio_next.setToolTip(tr("Next folio"))
            folio_next.setAccessibleName(tr("Next folio"))
            ctrl_row.addWidget(folio_prev)
            ctrl_row.addWidget(folio_lbl)
            ctrl_row.addWidget(folio_next)
            ctrl_row.addStretch()
            # Feature 3: zoom controls
            btn_zoom_out = QPushButton("-")
            btn_zoom_out.setFixedSize(26, 22)
            btn_zoom_out.setToolTip(tr("Zoom out"))
            btn_zoom_out.setAccessibleName(tr("Zoom out"))
            btn_zoom_in = QPushButton("+")
            btn_zoom_in.setFixedSize(26, 22)
            btn_zoom_in.setToolTip(tr("Zoom in"))
            btn_zoom_in.setAccessibleName(tr("Zoom in"))
            ctrl_row.addWidget(btn_zoom_out)
            ctrl_row.addWidget(btn_zoom_in)

            img = QLabel(tr("…"))
            img.setAlignment(Qt.AlignmentFlag.AlignCenter)
            img.setStyleSheet("background:#e2e8f0;color:#64748b;")
            # Round-4: a zoomed image overflows its pane — host it in a pannable scroll area
            # (drag to pan) so zoom in/out is actually visible (was downscaled-to-label before).
            img_scroll = _PannableScrollArea()
            img_scroll.setWidgetResizable(False)
            img_scroll.setMinimumHeight(360)
            img_scroll.setAlignment(Qt.AlignmentFlag.AlignCenter)
            img_scroll.setWidget(img)

            txt = QTextBrowser()
            txt.setReadOnly(True)

            box.addWidget(shelf)
            box.addWidget(meta)
            box.addWidget(dims_lbl)
            box.addLayout(ctrl_row)
            box.addWidget(img_scroll, 1)
            box.addWidget(txt, 1)

            pane_dict = {
                "box": box,
                "shelf": shelf,
                "meta": meta,
                "dims_lbl": dims_lbl,
                "folio_prev": folio_prev,
                "folio_lbl": folio_lbl,
                "folio_next": folio_next,
                "img": img,
                "img_scroll": img_scroll,
                "txt": txt,
                "sys_id": "",
                "page": 1,
                "zoom": 1.0,
                "full_pix": None,
            }
            # Wire folio buttons — capture pane_dict by reference
            folio_prev.clicked.connect(lambda _=False, pd=pane_dict: self._pane_folio_step(pd, -1))
            folio_next.clicked.connect(lambda _=False, pd=pane_dict: self._pane_folio_step(pd, +1))
            # Wire zoom — capture pane_dict by reference
            btn_zoom_out.clicked.connect(lambda _=False, pd=pane_dict: self._pane_zoom(pd, 1/1.25))
            btn_zoom_in.clicked.connect(lambda _=False, pd=pane_dict: self._pane_zoom(pd, 1.25))
            return pane_dict

        def _pane_zoom(self, pane: dict, factor: float):
            """Feature 3 / round-4: apply a zoom factor to a compare pane image.

            Client-side scale of the cached full-resolution pixmap (mirrors the main anchor
            image's _apply_zoom), clamped to [0.25, 4.0]. The previous implementation re-fetched
            at a larger width but then _pump_images downscaled it back to the label size, so zoom
            had NO visible effect (round-4 UAT). No network on a zoom click now.
            """
            pane["zoom"] = _clamp_zoom(pane.get("zoom", 1.0) * factor)
            self._render_pane_image(pane)

        def _render_pane_image(self, pane: dict):
            """Scale the pane's cached full pixmap by its zoom and size the label so the
            pannable scroll area can pan a zoomed image (mirrors window._apply_zoom)."""
            pix = pane.get("full_pix")
            lbl = pane.get("img")
            if pix is None or lbl is None:
                return
            z = pane.get("zoom", 1.0) or 1.0
            try:
                scaled = pix.scaled(
                    max(1, int(pix.width() * z)),
                    max(1, int(pix.height() * z)),
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
                lbl.setPixmap(scaled)
                lbl.resize(scaled.size())
            except RuntimeError:
                pass

        def _set_pane_pix(self, pane: dict, pix):
            """Store a freshly-loaded full pixmap and fit it to the viewport on (re)load.

            Fit-to-view never upscales past native (min(ratio, 1.0)) — same rule as the main
            anchor image — so a fresh image fills the pane and the user zooms IN from there."""
            pane["full_pix"] = pix
            vw = vh = 0
            try:
                scroll = pane.get("img_scroll")
                if scroll is not None:
                    vw = scroll.viewport().width()
                    vh = scroll.viewport().height()
            except RuntimeError:
                vw = vh = 0
            pw = max(1, pix.width())
            ph = max(1, pix.height())
            if vw > 10 and vh > 10:
                ratio = min(vw / pw, vh / ph)
                pane["zoom"] = min(ratio, 1.0) if ratio > 0 else 1.0
            else:
                pane["zoom"] = 1.0
            self._render_pane_image(pane)

        def _load_pane_image(self, pane: dict, sys_id: str, page):
            """Fetch the pane's (sys_id, page) image at good resolution and render it with
            client-side zoom (via the on_pixmap hook). Replaces the old direct enqueue that
            relied on label-fit scaling (which defeated zoom)."""
            if not sys_id:
                return
            try:
                self.wb._enqueue_image_for_pane(
                    pane["img"], sys_id, page, width=1400,
                    on_pixmap=lambda p, pd=pane: self._set_pane_pix(pd, p),
                )
            except Exception:
                pass

        def _load_pane_page_text(self, pane: dict, highlight):
            """Fetch the (sys_id, page) folio transcription for a pane on a background worker
            and render it line-numbered. Round-4: the text below each image must be the TEXT OF
            THAT PAGE — the anchor pane had none, and the candidate pane showed the WHOLE
            manuscript (c.full_text) instead of the matched page."""
            sid = pane.get("sys_id")
            if not sid:
                return
            try:
                pane["txt"].setPlainText(tr("loading…"))
            except (RuntimeError, KeyError):
                return
            try:
                gen = self.wb._gen
                page = pane.get("page", 1)
                txt_widget = pane["txt"]

                def _on_text(wgen: int, txt: str, w=txt_widget, h=highlight) -> None:
                    if wgen != self.wb._gen:
                        return
                    try:
                        apply_line_numbered_text(
                            w, htmlify(txt, h), source_text=txt, is_html=True,
                        )
                    except RuntimeError:
                        pass

                worker = _PageTextWorker(self.wb, gen, sid, page)
                worker.done.connect(_on_text)
                worker.start()
                if not hasattr(self, "_pane_text_workers"):
                    self._pane_text_workers = []
                self._pane_text_workers.append(worker)
            except Exception:
                pass

        def _fill_anchor(self, pane: dict, res_dict: dict):
            """Fill the anchor pane from the raw result DICT (read with r_* helpers, RR-2).

            The anchor is the Phase-107 pinned result dict — never a Candidate.
            """
            try:
                pane["shelf"].setText(r_shelf(res_dict))
            except RuntimeError:
                return
            # Meta line: meta_brief
            enrich = self.wb._candidate_pane._enrich if hasattr(
                self.wb, "_candidate_pane"
            ) else {}
            anchor_key = (r_sid(res_dict), page_of(res_dict))
            m = enrich.get(anchor_key) or {}
            # The anchor is never enriched into the candidate _enrich dict — fall back to
            # the cached anchor measurements so the anchor pane shows material/dims too.
            am = getattr(self.wb, "_anchor_meas", {}) or {}
            width_cm = m.get("width_cm") or am.get("width_cm")
            height_cm = m.get("height_cm") or am.get("height_cm")
            material = m.get("material") or am.get("material")
            try:
                pane["meta"].setText(meta_brief(res_dict))
            except RuntimeError:
                return
            # Feature 3: material/dimensions line (separate label below meta)
            dim_parts = []
            if width_cm and height_cm:
                dim_parts.append(f"{width_cm:.0f}×{height_cm:.0f} cm")
            if material:
                dim_parts.append(material_display(str(material)))  # Feature 9
            try:
                if dim_parts:
                    pane["dims_lbl"].setText("  ·  ".join(dim_parts))
                    pane["dims_lbl"].setVisible(True)
                else:
                    pane["dims_lbl"].setVisible(False)
            except (RuntimeError, KeyError):
                pass
            # Track pane identity for per-pane folio browse (Feature 4) BEFORE loading text/image.
            p = page_of(res_dict)
            pane["sys_id"] = r_sid(res_dict)
            pane["page"] = max(1, p or 1)
            try:
                pane["folio_lbl"].setText(f"p.{pane['page']}")
            except (RuntimeError, KeyError):
                pass
            # Round-4 #2: the anchor pane had no text. Fetch the matched PAGE's transcription
            # (the same page shown in the image) on a background worker, like folio nav does.
            self._load_pane_page_text(pane, res_dict.get("highlight_pattern"))
            # Image: per-page (RR-7); page may be None for synthetic anchor — pump guards None.
            self._load_pane_image(pane, r_sid(res_dict), p)

        def _fill_candidate(self, pane: dict, c):
            """Fill the candidate pane from a Candidate dataclass (RR-2).

            Reads c.* attributes directly — never r_sid(c)/r_text(c)/page_of(c).
            RR-12: passes c.page (Optional[int]) straight to _enqueue_image_for_pane;
            the pump's None-page guard handles VS-only / None-page rows — no page-1
            arithmetic here.
            """
            # Round-4: the compare candidate pane carries the same 👁 / ⚓ / ⇄ badge as the card.
            shelf_text, eye_tip = _candidate_shelf_badge(c)
            try:
                pane["shelf"].setText(shelf_text)
                pane["shelf"].setToolTip(eye_tip or "")   # G-06.2 "visual similarity" (cleared if none)
            except RuntimeError:
                return
            # Meta line: library · title + "other side matched"
            lib_title = " · ".join(p for p in [c.library_code, c.title[:60]] if p)
            meta_parts = [lib_title] if lib_title else []
            if c.via_other_side:
                meta_parts.append(tr("other side matched"))   # D-18 / R-06 label
            enrich = self.wb._candidate_pane._enrich if hasattr(
                self.wb, "_candidate_pane"
            ) else {}
            m = enrich.get(c.key) or {}   # c.key = (sys_id, page) per RR-2
            width_cm = m.get("width_cm")
            height_cm = m.get("height_cm")
            material = m.get("material")
            try:
                pane["meta"].setText("   ·   ".join(b for b in meta_parts if b))
            except RuntimeError:
                return
            # Feature 3: material/dimensions line (separate label)
            dim_parts = []
            if width_cm and height_cm:
                dim_parts.append(f"{width_cm:.0f}×{height_cm:.0f} cm")
            if material:
                dim_parts.append(material_display(str(material)))  # Feature 9
            try:
                if dim_parts:
                    pane["dims_lbl"].setText("  ·  ".join(dim_parts))
                    pane["dims_lbl"].setVisible(True)
                else:
                    pane["dims_lbl"].setVisible(False)
            except (RuntimeError, KeyError):
                pass
            # Track pane identity for per-pane folio browse (Feature 4) BEFORE loading text/image.
            # RR-12: c.page is Optional[int]; pane["page"] is clamped to ≥1.
            pane["sys_id"] = c.sys_id
            pane["page"] = max(1, c.page or 1)
            try:
                pane["folio_lbl"].setText(f"p.{pane['page']}")
            except (RuntimeError, KeyError):
                pass
            # Round-4 #3: show the matched PAGE's transcription (the page shown in the image),
            # NOT c.full_text (the whole manuscript). Fetch page text on a background worker.
            self._load_pane_page_text(pane, c.highlight_pattern)
            # Image: matched page (RR-7). c.page may be None — pump/loader guard None.
            self._load_pane_image(pane, c.sys_id, c.page)

        def paint(self):
            """Refresh both panes, position label, and compare border (Feature 4)."""
            if not self.wb.filtered:
                return
            cand = self.wb.filtered[self.idx]
            tri = self.wb.triage.get(cand.sys_id)
            try:
                self.pos_lbl.setText(
                    tr("candidate") + f" {self.idx + 1}/{len(self.wb.filtered)}"
                    f"   {r_shelf(self.wb._anchor_res)}  vs  {cand.shelfmark}"
                    f"   [{tri or '-'}]"
                )
            except RuntimeError:
                return
            # Feature 4: update compare border color to reflect current candidate's triage
            self._restyle_compare(tri)
            # Anchor pane stays static (D-18) — re-filled on every paint
            self._fill_anchor(self.left, self.wb._anchor_res)
            # Candidate pane reflects the current candidate
            self._fill_candidate(self.right, cand)

        def step(self, d: int):
            """Move prev/next through the filtered candidate list (clamp, no wrap)."""
            if not self.wb.filtered:
                return
            self.idx = max(0, min(self.idx + d, len(self.wb.filtered) - 1))
            self.paint()

        def _cur(self):
            """Return the current Candidate (Candidate dataclass, RR-2)."""
            return self.wb.filtered[self.idx]

        def _mark(self, val: str):
            """Mark current candidate, refresh label, and color the compare border (Feature 4).

            G-10 / round-4: wb.mark() TOGGLES (a second click on the same value clears the triage).
            paint() re-reads the ACTUAL post-toggle triage and restyles the border accordingly, so
            we must NOT re-color with the clicked `val` afterwards — doing that left the border stuck
            on the clicked colour after a toggle-off (the "triage doesn't work in compare" report)."""
            self.wb.mark(self._cur().sys_id, val)
            self.paint()

        def _restyle_compare(self, triage_val):
            """Feature 4: apply triage-colored border to the compare window (gentle 2px)."""
            color = _TRI_COLOR.get(triage_val, _TRI_COLOR[None])
            try:
                self.setStyleSheet(
                    f"QDialog{{border:2px solid {color};border-radius:4px;}}"
                )
            except RuntimeError:
                pass

        def _reanchor(self):
            """Set current candidate as the new anchor, then close dialog."""
            self.wb.set_anchor(candidate_to_result_dict(self._cur()))
            self.accept()

        def _pane_folio_step(self, pane: dict, delta: int):
            """Step pane image AND text to the adjacent folio (Features 4+1).

            Independent of the candidate-list prev/next (which steps candidates).
            RR-12: pane["page"] is always int (≥1); guard against going below 1.
            Feature 1: also fetches page text via _PageTextWorker (background, never UI-thread).
            """
            new_page = max(1, pane["page"] + delta)
            pane["page"] = new_page
            try:
                pane["folio_lbl"].setText(f"p.{new_page}")
                pane["img"].setText(tr("loading…"))
            except (RuntimeError, KeyError):
                return
            if not pane.get("sys_id"):
                return
            # Highlight pattern from the current candidate (if any) for the page text.
            highlight = None
            if self.wb.filtered and 0 <= self.idx < len(self.wb.filtered):
                highlight = getattr(self.wb.filtered[self.idx], "highlight_pattern", None)
            # Round-4: reuse the shared page-text + zoomable-image helpers so folio nav matches
            # the initial fill (page-scoped text + client-side zoom).
            self._load_pane_page_text(pane, highlight)
            self._load_pane_image(pane, pane["sys_id"], new_page)

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

            # Phase-106 SearchExecutor adapter — Plan 03 passes this to the candidate pane.
            self._executor = _DesktopSearchExecutor(self.searcher, self.meta_mgr)

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
            # UAT follow-up: fit-the-whole-fragment-to-view on each fresh anchor image
            self._fit_pending = False
            # Known-join rows (cached for the joins-context dropdown) + per-row checkboxes
            self._known_join_rows = []
            self._join_row_checks = []
            # Joins section is collapsed by default; clicking the header expands it.
            self._joins_expanded = False

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

            # Plan 03 — triage state (sys_id-keyed per R-05; reset on re-anchor per D-10).
            # Deliberate split from the (sys_id, page) enrichment key: a physical fragment
            # is triaged once regardless of which page image was found at.
            self.triage: dict = {}
            # Plan 03 — post-filter candidate list (populated by JoinCandidatePane)
            self.filtered: list = []

            # Plan 03 — bounded image-loader pool state (5-slot, PATTERNS block)
            self._img_queue: list = []
            self._img_active: list = []
            self._img_threads: list = []
            self._thumb_resolver = None

            # Plan 06 — pick-mode callback (None = normal Workbench; set via set_pick_callback).
            # MARKED REMOVABLE (Phase 109 G-08, D-11 one-cycle soft-retire): with the JoinsDialog pick-back
            # retired (G-08 reverses G-05), this pick-callback machinery has no live caller. RETAINED one
            # cycle as a safety net; tests (test_invoke_pick_forwards_sysid_shelfmark,
            # test_set_pick_callback_rerenders) keep it green. Removable once the parity UAT signs off.
            self._pick_callback = None

            self._init_ui()
            self.setWindowTitle(tr("Joins Lab"))
            self.setMinimumSize(900, 680)
            self.resize(1000, 720)

        # ------------------------------------------------------------------
        # UI construction
        # ------------------------------------------------------------------

        def _init_ui(self):
            root = QHBoxLayout(self)
            root.setContentsMargins(0, 0, 0, 0)
            splitter = QSplitter(Qt.Orientation.Horizontal)
            # Anchor pane ~30%; right (candidate) pane ~70% (adapted_decision 14).
            # Both panes are resizable via the splitter handle.
            splitter.addWidget(self._build_anchor_pane())
            splitter.addWidget(self._build_right_pane())
            splitter.setStretchFactor(0, 0)   # anchor fixed-ish
            splitter.setStretchFactor(1, 1)   # right pane takes remaining space
            splitter.setSizes([300, 700])
            splitter.setMinimumWidth(900)
            root.addWidget(splitter)

        def _build_anchor_pane(self) -> QWidget:
            """Build the left anchor pane (top-to-bottom per UI-SPEC Layout Contract)."""
            pane = QWidget()
            layout = QVBoxLayout(pane)
            layout.setContentsMargins(8, 8, 8, 8)
            layout.setSpacing(6)

            # 1. Shelfmark label
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
            btn_zoom_out.setToolTip(tr("Zoom out"))
            btn_zoom_out.setAccessibleName(tr("Zoom out"))
            btn_zoom_out.clicked.connect(self._zoom_out)
            toolbar.addWidget(btn_zoom_out)

            btn_zoom_in = QPushButton("+")
            btn_zoom_in.setFixedWidth(30)
            btn_zoom_in.setToolTip(tr("Zoom in"))
            btn_zoom_in.setAccessibleName(tr("Zoom in"))
            btn_zoom_in.clicked.connect(self._zoom_in)
            toolbar.addWidget(btn_zoom_in)

            toolbar.addStretch()

            # Feature 7: RTL glyphs — PREV points right (►), NEXT points left (◄)
            self.btn_folio_prev = QPushButton("►")
            self.btn_folio_prev.setFixedWidth(30)
            self.btn_folio_prev.setAccessibleName(tr("Previous folio"))
            self.btn_folio_prev.clicked.connect(self._folio_prev)
            toolbar.addWidget(self.btn_folio_prev)

            self.folio_counter = QLabel("")
            self.folio_counter.setStyleSheet("font-size:11px;color:#94a3b8;")
            toolbar.addWidget(self.folio_counter)

            self.btn_folio_next = QPushButton("◄")
            self.btn_folio_next.setFixedWidth(30)
            self.btn_folio_next.setAccessibleName(tr("Next folio"))
            self.btn_folio_next.clicked.connect(self._folio_next)
            toolbar.addWidget(self.btn_folio_next)

            layout.addLayout(toolbar)

            # 5. Image area — pannable scroll (UAT: drag to move a zoomed image).
            # widgetResizable=False so a zoomed pixmap overflows the viewport and
            # becomes scrollable/pannable; the scroll area centers small images.
            self.anchor_img_scroll = _PannableScrollArea()
            self.anchor_img_scroll.setWidgetResizable(False)
            self.anchor_img_scroll.setAlignment(Qt.AlignmentFlag.AlignCenter)
            img_bg = "#374151" if self.is_dark else "#e2e8f0"
            self.anchor_img_label = QLabel()
            self.anchor_img_label.setMinimumSize(360, 280)
            self.anchor_img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.anchor_img_label.setStyleSheet(f"background:{img_bg};")
            self.anchor_img_scroll.setWidget(self.anchor_img_label)

            # 6. Transcription text browser — RTL: right-aligned text AND the
            # line-number gutter on the right edge (apply_line_numbered_text's
            # _reposition_gutter places the gutter on the leading side under RTL).
            self.anchor_text_browser = QTextBrowser()
            self.anchor_text_browser.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

            # 7. Image / text / joins in a vertical splitter so each is resizable
            # (UAT). The known-joins section lives UNDER the text on the LEFT;
            # the right pane is reserved for the Phase-108 candidate hunt.
            self.left_split = QSplitter(Qt.Orientation.Vertical)
            self.left_split.addWidget(self.anchor_img_scroll)
            self.left_split.addWidget(self.anchor_text_browser)
            self.left_split.addWidget(self._build_joins_panel())
            self.left_split.setStretchFactor(0, 1)
            self.left_split.setStretchFactor(1, 1)
            self.left_split.setStretchFactor(2, 0)
            self.left_split.setSizes([360, 300, 1])  # joins collapsed (header only)
            layout.addWidget(self.left_split, 1)

            # 8. Anchor action buttons — ICON-ONLY at the BOTTOM of the anchor pane
            # (adapted_decision 14). These act on the ANCHOR, not candidates.
            anchor_actions = QHBoxLayout()
            anchor_actions.setSpacing(8)
            anchor_actions.addStretch()

            self.btn_anchor_browse = QPushButton("📖")
            self.btn_anchor_browse.setFixedSize(34, 32)
            self.btn_anchor_browse.setToolTip(tr("Browse this fragment"))
            self.btn_anchor_browse.setAccessibleName(tr("Browse manuscript"))
            self.btn_anchor_browse.clicked.connect(self._anchor_browse)
            anchor_actions.addWidget(self.btn_anchor_browse)

            self.btn_anchor_puzzle = QPushButton("🧩")
            self.btn_anchor_puzzle.setFixedSize(34, 32)
            self.btn_anchor_puzzle.setToolTip(tr("Add anchor to a Puzzle"))
            self.btn_anchor_puzzle.setAccessibleName(tr("Add to Puzzle"))
            self.btn_anchor_puzzle.clicked.connect(self._anchor_puzzle)
            anchor_actions.addWidget(self.btn_anchor_puzzle)

            self.btn_anchor_list = QPushButton("☰")
            self.btn_anchor_list.setFixedSize(34, 32)
            self.btn_anchor_list.setToolTip(tr("Add anchor to a list"))
            self.btn_anchor_list.setAccessibleName(tr("Add to List"))
            self.btn_anchor_list.clicked.connect(self._anchor_add_to_list)
            anchor_actions.addWidget(self.btn_anchor_list)

            self.btn_anchor_add_join = QPushButton("🔗")
            self.btn_anchor_add_join.setFixedSize(34, 32)
            self.btn_anchor_add_join.setToolTip(tr("Start a join from this anchor"))
            self.btn_anchor_add_join.setAccessibleName(tr("Add as Join"))
            self.btn_anchor_add_join.clicked.connect(self._on_add_as_join)
            anchor_actions.addWidget(self.btn_anchor_add_join)

            anchor_actions.addStretch()
            layout.addLayout(anchor_actions)

            return pane

        def _build_right_pane(self) -> QWidget:
            """Build the right pane: cold-start row + candidate pane.

            Anchor actions moved to bottom of anchor pane (adapted_decision 14).
            """
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
            # Pick-from-personal-list button (📋) — mirrors the JoinsDialog affordance.
            self.btn_coldstart_pick = QPushButton("\U0001f4cb")
            self.btn_coldstart_pick.setFixedWidth(30)
            self.btn_coldstart_pick.setToolTip(tr("Pick from personal list"))
            self.btn_coldstart_pick.setAccessibleName(tr("Pick from personal list"))
            self.btn_coldstart_pick.clicked.connect(self._pick_from_list)
            coldstart_row.addWidget(self.btn_coldstart_pick)
            layout.addLayout(coldstart_row)

            # 2. (Phase 108) candidate-hunt surface — JoinCandidatePane (Plan 03).
            self._candidate_pane = JoinCandidatePane(self, self._executor)
            layout.addWidget(self._candidate_pane, 1)
            return pane

        def _build_joins_panel(self) -> QWidget:
            """Build the known-joins panel (UAT round 2).

            Collapsed by default: only the clickable "Known Joins (N)" header
            shows; clicking it expands a resizable body (controls + scrollable
            rows). A chain-icon dropdown button gives the joins-context menu.
            """
            header_color = "#14b8a6" if self.is_dark else "#0f766e"

            self.joins_panel = QWidget()
            joins_panel_layout = QVBoxLayout(self.joins_panel)
            joins_panel_layout.setContentsMargins(0, 0, 0, 0)
            joins_panel_layout.setSpacing(4)

            # Header row: clickable collapse/expand toggle + chain-icon dropdown.
            header_row = QHBoxLayout()
            header_row.setSpacing(6)
            self.btn_joins_toggle = QPushButton(f"▸ {tr('Known Joins')}")
            self.btn_joins_toggle.setFlat(True)
            self.btn_joins_toggle.setCursor(Qt.CursorShape.PointingHandCursor)
            self.btn_joins_toggle.setStyleSheet(
                f"QPushButton{{font-weight:bold;font-size:11px;color:{header_color};"
                f"text-align:left;border:none;padding:0;}}"
            )
            self.btn_joins_toggle.clicked.connect(self._toggle_joins_body)
            header_row.addWidget(self.btn_joins_toggle)
            header_row.addStretch()

            # Joins-context dropdown — chain icon + ▾ triangle to signal a menu.
            self.btn_joins_context = QPushButton("\U0001f517 ▾")
            self.btn_joins_context.setFixedWidth(42)
            self.btn_joins_context.setToolTip(tr("Joins context"))
            self.btn_joins_context.setAccessibleName(tr("Joins context"))
            self.btn_joins_context.clicked.connect(self._show_joins_context_menu)
            header_row.addWidget(self.btn_joins_context)
            joins_panel_layout.addLayout(header_row)

            # Collapsible body (controls + scrollable rows) — hidden by default.
            self.joins_body = QWidget()
            body_layout = QVBoxLayout(self.joins_body)
            body_layout.setContentsMargins(0, 0, 0, 0)
            body_layout.setSpacing(4)

            controls_row = QHBoxLayout()
            controls_row.setSpacing(6)
            self.chk_join_select_all = QCheckBox(tr("Select All"))
            self.chk_join_select_all.stateChanged.connect(self._on_join_select_all)
            controls_row.addWidget(self.chk_join_select_all)
            controls_row.addStretch()
            self.btn_add_selected_puzzle = QPushButton(tr("Add selected to puzzle"))
            self.btn_add_selected_puzzle.setToolTip(tr("Add selected to puzzle"))
            self.btn_add_selected_puzzle.setAccessibleName(tr("Add selected to puzzle"))
            self.btn_add_selected_puzzle.clicked.connect(self._add_selected_to_puzzle)
            controls_row.addWidget(self.btn_add_selected_puzzle)
            body_layout.addLayout(controls_row)

            rows_scroll = QScrollArea()
            rows_scroll.setWidgetResizable(True)
            rows_container = QWidget()
            self.joins_rows_layout = QVBoxLayout(rows_container)
            self.joins_rows_layout.setContentsMargins(0, 0, 0, 0)
            self.joins_rows_layout.setSpacing(2)
            self.joins_rows_layout.addStretch()
            rows_scroll.setWidget(rows_container)
            body_layout.addWidget(rows_scroll)

            self.joins_body.setVisible(False)  # collapsed by default
            joins_panel_layout.addWidget(self.joins_body)

            self.joins_panel.setVisible(False)
            return self.joins_panel

        def _joins_header_text(self) -> str:
            """Header label: arrow reflects expanded state + current join count."""
            arrow = "▾" if self._joins_expanded else "▸"
            n = len(self._known_join_rows)
            return f"{arrow} {tr('Known Joins')} ({n})"

        def _set_joins_expanded(self, expanded: bool):
            """Show/hide the joins body and resize the splitter (default ≈ half)."""
            self._joins_expanded = bool(expanded)
            try:
                self.joins_body.setVisible(self._joins_expanded)
                self.btn_joins_toggle.setText(self._joins_header_text())
            except RuntimeError:
                return
            split = getattr(self, "left_split", None)
            if split is None:
                return
            total = sum(split.sizes()) or split.height() or 720
            if self._joins_expanded:
                # joins ≈ half the height; image + text share the other half.
                split.setSizes([int(total * 0.25), int(total * 0.25), int(total * 0.5)])
            else:
                # joins collapsed to its header; image + text share the space.
                split.setSizes([int(total * 0.5), int(total * 0.5), 1])

        def _toggle_joins_body(self):
            """Header click — flip the joins section open/closed (UAT)."""
            self._set_joins_expanded(not self._joins_expanded)

        # ------------------------------------------------------------------
        # Phase 109 — public source selector
        # ------------------------------------------------------------------

        def set_source(self, source: str):
            """Public: switch the candidate source (e.g. 'visual') after open. Used by reroutes
            (Plan 03 / Plan 06).

            BLOCKER A (HIGH-3): stash the request as pane._pending_vs and clear it ONLY when
            apply_source() returns True (actually applied). On a reused window whose previous anchor
            had no VS, apply_source returns False -> we MUST keep _pending_vs so _on_anchor_set
            re-applies it for the NEW anchor once its VS availability is known. (The old code swallowed
            the request by clearing the pending flag unconditionally before checking the return value.)
            """
            pane = getattr(self, "_candidate_pane", None)
            if pane is None:
                return
            # map the source string to the boolean pending flag ('visual'/'combined' -> True, else False)
            pane._pending_vs = source in ("visual", "combined")
            try:
                applied = pane.apply_source(source)
            except (RuntimeError, AttributeError):
                # pane not fully ready yet — _on_anchor_set will apply pending after grey-out (review #2)
                return
            if applied:
                pane._pending_vs = None   # clear ONLY after the request was actually applied
            # else: leave pane._pending_vs set; _on_anchor_set applies it once the new anchor's VS is known

        # ------------------------------------------------------------------
        # Plan 06 — pick-mode callback (G-05 / HIGH-4)
        # MARKED REMOVABLE (Phase 109 G-08, D-11 one-cycle soft-retire): with the JoinsDialog pick-back
        # retired (G-08 reverses G-05), this pick-callback machinery has no live caller. RETAINED one
        # cycle as a safety net; tests (test_invoke_pick_forwards_sysid_shelfmark,
        # test_set_pick_callback_rerenders) keep it green. Removable once the parity UAT signs off.
        # ------------------------------------------------------------------

        def set_pick_callback(self, cb):
            """Enter pick mode: store the callback and re-render visible cards so the
            'Select as partner' button appears on current-page cards immediately.

            HIGH-4: call BEFORE set_anchor/set_source so the FIRST rendered card page already
            reflects pick mode. The pre-anchor re-render paints the OLD anchor's cards — that is
            safe because the immediately-following set_anchor (Plan 05 BLOCKER B) clears the grid
            before the NEW anchor repaints, so no stale-anchor pick card survives.
            """
            self._pick_callback = cb
            self._rerender_candidate_cards()

        def clear_pick_callback(self):
            """Leave pick mode: clear the callback and re-render visible cards so any stale
            'Select as partner' buttons are removed immediately (normal-open safety net).

            HIGH-4: call BEFORE set_anchor on a normal (non-pick) open so the first rendered
            card page already has no pick button.
            """
            self._pick_callback = None
            self._rerender_candidate_cards()

        def _rerender_candidate_cards(self):
            """Belt-and-braces re-render: rebuild the current page's CandidateCards so each
            card re-evaluates the pick-button condition against the current _pick_callback.
            No-ops when the pane is not yet constructed or is already deleted.
            """
            pane = getattr(self, "_candidate_pane", None)
            if pane is None:
                return
            try:
                pane.render_results()   # re-runs _render_grid_page -> rebuilds cards
            except (RuntimeError, AttributeError):
                pass

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
            self._fit_pending = True  # fit the whole fragment to view on first image
            self._anchor_full_pix = None
            self._anchor_images = []
            self._set_joins_expanded(False)  # joins collapse on each fresh anchor
            # D-10: triage reset on re-anchor (sys_id-keyed; cleared so old marks
            # from a previous anchor don't bleed into the new session).
            self.triage = {}

            # HIGH-2 + NEW-HIGH (BLOCKER B): invalidate per-anchor candidate DATA and clear the
            # stale rendered CARD WIDGETS so the NEW anchor reloads cleanly and no previous-anchor
            # cards (incl. any "Select as partner" pick buttons in the Plan-06 pick flow) linger.
            pane = getattr(self, "_candidate_pane", None)
            if pane is not None:
                pane._text_cands = None
                pane._vs_cands = None
                pane._vs_loaded_sid = None     # force _ensure_vs_loaded_for_anchor to reload for the new sid
                pane.results = []
                self.filtered = []             # wb.filtered is the window attr read by _render_grid_page
                try:
                    pane.render_results()      # existing card-widget clear: empties self.cards + the grid
                except (RuntimeError, AttributeError):
                    pass

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
                self._right_align_anchor_text()
            except RuntimeError:
                pass

            self._load_current_image()

            # Phase 109: update grey-out + apply pending source after anchor is known (D-08/review #2)
            try:
                self._candidate_pane._on_anchor_set()
            except (RuntimeError, AttributeError):
                pass

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
            # UAT: show the ENTIRE fragment by default. Fit once per fresh anchor
            # image; folio nav / manual zoom keep the user's current zoom.
            if self._fit_pending:
                self._fit_to_view()
                self._fit_pending = False
            self._apply_zoom()

        def _fit_to_view(self):
            """Set the zoom factor so the whole fragment image fits the image area."""
            if not self._anchor_full_pix:
                return
            try:
                vp = self.anchor_img_scroll.viewport().size()
            except RuntimeError:
                return
            pw, ph = self._anchor_full_pix.width(), self._anchor_full_pix.height()
            vw, vh = vp.width(), vp.height()
            if pw <= 0 or ph <= 0 or vw <= 0 or vh <= 0:
                return
            ratio = min(vw / pw, vh / ph)
            if ratio > 0:
                # Never upscale a small image past native; allow shrink-to-fit below
                # the manual-zoom clamp floor so the whole fragment is visible.
                self._zoom = min(ratio, 1.0)

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
                # Size the label to the pixmap so the scroll area can pan/scroll a
                # zoomed image (minimumSize keeps small images centered).
                self.anchor_img_label.resize(scaled.size())
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
                self._right_align_anchor_text()
            except RuntimeError:
                pass

        def _right_align_anchor_text(self):
            """Force every block of the anchor transcription to right-align (RTL UAT).

            htmlify already emits a right-aligned RTL div, but applying the block
            format directly guarantees right alignment regardless of how Qt renders
            the wrapper.
            """
            try:
                cursor = self.anchor_text_browser.textCursor()
                cursor.select(QTextCursor.SelectionType.Document)
                fmt = QTextBlockFormat()
                fmt.setAlignment(Qt.AlignmentFlag.AlignRight)
                cursor.mergeBlockFormat(fmt)
                cursor.clearSelection()
                self.anchor_text_browser.setTextCursor(cursor)
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
            self._cancel_images()
            super().closeEvent(event)

        # ------------------------------------------------------------------
        # Plan 03 — triage state (D-10 / R-05)
        # ------------------------------------------------------------------

        def mark(self, sys_id: str, val: str):
            """Record a triage decision for a fragment (sys_id-keyed, R-05).

            val: 'yes' | 'maybe' | 'no'.
            Triage is keyed by sys_id (physical fragment), not (sys_id, page) — the
            deliberate split from the enrichment-dict key means a fragment triaged via
            page 3 is immediately visible when the scholar looks at page 4 of the same
            manuscript. See test_join_workbench_triage.py for the contract.
            """
            # G-10.1: idempotent per-state toggle. If the current triage already equals val,
            # clear it; otherwise set it. (Clicking Y then Y clears; clicking Y then N sets N.)
            if self.triage.get(sys_id) == val:
                self.triage.pop(sys_id, None)
            else:
                self.triage[sys_id] = val
            # Propagate visual update to the candidate pane (may not exist yet).
            try:
                self._candidate_pane._restyle_card(sys_id)
                self._candidate_pane._update_status_counts()
            except (RuntimeError, AttributeError):
                pass

        # ------------------------------------------------------------------
        # Plan 03 — public action delegators (D-20: zero _vs_ calls)
        # One Candidate -> one host-method call via the PUBLIC API.
        # ------------------------------------------------------------------

        def open_result_in_browse(self, c):
            """Open a candidate Candidate in Browse (Phase-107 host method)."""
            self._app.open_result_in_browse_from_table(candidate_to_result_dict(c))

        def open_result_in_puzzle(self, c):
            """Open anchor + candidate in Fragment Puzzle (adapted_decision 10)."""
            from desktop.join_workbench import puzzle_add_targets
            sids = puzzle_add_targets(self._anchor_sid, [c.sys_id])
            self._app.open_anchors_in_puzzle(sids)

        def open_result_in_list(self, c, anchor_widget=None):
            """Add a candidate to a personal list (Phase-107 host method)."""
            self._app.show_add_to_list_menu(
                [{"sys_id": c.sys_id, "fl_id": "", "img": c.page or 1}],
                source="join_workbench",
                anchor_widget=anchor_widget,
            )

        def open_result_as_join(self, c):
            """Open JoinsDialog pre-filled with anchor as A and candidate as B.

            Calls the EXTENDED public open_anchor_as_join (RR-3, D-17, D-20).
            No _vs_ private calls.
            """
            if not self._anchor_res:
                return
            self._app.open_anchor_as_join(
                r_sid(self._anchor_res),
                r_shelf(self._anchor_res),
                partner_sys_id=c.sys_id,
                partner_shelfmark=c.shelfmark,
            )

        # ------------------------------------------------------------------
        # Plan 03 — bounded image-loader pool (5 slots, PATTERNS block)
        # _enqueue_image_for_pane resolves per-page IIIF URL (RR-7/RR-12).
        # ------------------------------------------------------------------

        def _enqueue_image(self, label, url, target=None, on_pixmap=None):
            """Enqueue a URL for loading into a QLabel (bounded 5-slot pool).

            on_pixmap (optional): callback(QPixmap) invoked on the GUI thread with the FULL
            loaded pixmap. When set, the default label-fit scaling is skipped so the caller can
            render the pixmap itself (used by CompareDialog for client-side zoom — round-4)."""
            if not url:
                try:
                    label.setText(tr("(no image)"))
                except RuntimeError:
                    pass
                return
            self._img_queue.append((label, url, target, on_pixmap))
            self._pump_images()

        def _pump_images(self):
            """Start image loaders up to the 5-slot ceiling (Pitfall 4: QPixmap on GUI thread)."""
            # Clean up finished threads
            self._img_threads = [t for t in self._img_threads if t.isRunning()]
            while self._img_queue and len(self._img_threads) < _MAX_CONCURRENT_IMG:
                label, url, target, on_pix = self._img_queue.pop(0)
                loader = ImageLoaderThread(url)
                # Closure captures label + target; QPixmap.fromImage MUST run on GUI thread.
                def _on_loaded(qi, lbl=label, tgt=target, op=on_pix):
                    try:
                        if qi is not None and not qi.isNull():
                            pix = QPixmap.fromImage(qi)
                            if op is not None:
                                # Caller renders the full pixmap itself (CompareDialog zoom).
                                lbl.setText("")
                                op(pix)
                            elif tgt == "card":
                                # Card set_pixmap scales to 220x130
                                lbl.setText("")
                                lbl.setPixmap(pix.scaled(
                                    220, 130,
                                    Qt.AspectRatioMode.KeepAspectRatio,
                                    Qt.TransformationMode.SmoothTransformation,
                                ))
                            else:
                                lbl.setPixmap(pix.scaled(
                                    lbl.width() or 400, lbl.height() or 300,
                                    Qt.AspectRatioMode.KeepAspectRatio,
                                    Qt.TransformationMode.SmoothTransformation,
                                ))
                        else:
                            lbl.setText(tr("(no image)"))
                    except RuntimeError:
                        pass   # widget deleted — standard Phase 107 guard
                loader.image_loaded.connect(_on_loaded)
                loader.load_failed.connect(
                    lambda lbl=label: self._try_setText(lbl, tr("(no image)"))
                )
                loader.finished.connect(self._pump_images)
                loader.start()
                self._img_threads.append(loader)

        def _try_setText(self, label, text: str):
            """Set label text, ignoring RuntimeError for deleted widgets."""
            try:
                label.setText(text)
            except RuntimeError:
                pass

        def _enqueue_image_for_pane(self, label, sys_id: str, page, width: int = 1400, on_pixmap=None):
            """Resolve and enqueue the MATCHED PAGE image for a candidate (RR-7, RR-12).

            Uses the per-page _image_url_for_idx path (NOT meta_mgr.get_thumbnail()).
            RR-12 GUARD: Candidate.page is Optional[int]; a None page is treated as
            page 1 (first page) before the page-1 arithmetic.
            on_pixmap (optional): see _enqueue_image — lets CompareDialog do client-side zoom.
            """
            # RR-12: guard None page BEFORE any page-1 arithmetic
            if page is None:
                page = 1
            images = None
            try:
                meta = self.meta_mgr.enrich_metadata(sys_id)
                images = (meta or {}).get("images") or []
            except Exception:
                images = []
            # page is 1-based; _image_url_for_idx is 0-based; returns '' for out-of-range
            url = _image_url_for_idx(images, page - 1, width)
            self._enqueue_image(label, url, on_pixmap=on_pixmap)

        def _cancel_images(self):
            """Cancel all pending image loads (called on re-anchor / page change / close)."""
            self._img_queue.clear()
            for t in self._img_threads:
                try:
                    t.cancel()
                except Exception:
                    pass
            self._img_threads.clear()
            if self._thumb_resolver is not None:
                try:
                    self._thumb_resolver.cancel()
                except Exception:
                    pass
                self._thumb_resolver = None

        # ------------------------------------------------------------------
        # Session persistence (Feature 7) — INPUT only; never saves results.
        # ------------------------------------------------------------------

        def to_state(self) -> dict:
            """Serialize the Join Lab INPUT state to a plain dict.

            Captured: anchor identity, both builder states, other-side enable + mode,
            triage dict, filter text + material + triage filter + view_mode, open flag.
            Candidate result lists are NEVER persisted (search_history.json lesson).
            """
            pane = getattr(self, "_candidate_pane", None)
            anchor_state = {
                "sys_id": self._anchor_sid or "",
                "shelfmark": r_shelf(self._anchor_res) if self._anchor_res else "",
                "img": (self._anchor_res.get("display") or {}).get("img", 1)
                       if self._anchor_res else 1,
                "uid": (self._anchor_res or {}).get("uid", ""),
            }
            builder_state = pane.builder.to_state() if pane else {}
            other_builder_state = pane.other_builder.to_state() if pane else {}
            other_enabled = False
            other_mode_idx = 0
            filter_text = ""
            mat_filter_idx = 0
            tri_filter_idx = 0
            view_mode = "grid"
            if pane is not None:
                try:
                    other_enabled = pane.other_enable.isChecked()
                    other_mode_idx = pane.combine_combo.currentIndex()
                    filter_text = pane.filter_in.text()
                    mat_filter_idx = pane.mat_filter.currentIndex()
                    tri_filter_idx = pane.tri_filter.currentIndex()
                    view_mode = pane.view_mode
                except RuntimeError:
                    pass

            return {
                "open": self.isVisible(),
                "anchor": anchor_state,
                "builder": builder_state,
                "other_builder": other_builder_state,
                "other_enabled": other_enabled,
                "other_mode_idx": other_mode_idx,
                "triage": dict(self.triage),
                "filter_text": filter_text,
                "mat_filter_idx": mat_filter_idx,
                "tri_filter_idx": tri_filter_idx,
                "view_mode": view_mode,
            }

        def restore_state(self, state: dict):
            """Restore the Join Lab INPUT state from a dict (see to_state).

            Sets the anchor, rebuilds builders/filters/triage, then DEFERS the
            candidate search to the background SearchThread via do_search().
            NEVER blocks the UI thread — no synchronous search here (hard constraint).
            """
            if not state:
                return

            # Restore anchor
            anchor = state.get("anchor") or {}
            sid = anchor.get("sys_id") or ""
            if sid:
                shelfmark = anchor.get("shelfmark") or sid
                img = anchor.get("img") or 1
                uid = anchor.get("uid") or f"{sid}_P001"
                res_dict = {
                    "display": {"id": sid, "shelfmark": shelfmark, "img": img},
                    "uid": uid,
                }
                self.set_anchor(res_dict)

            pane = getattr(self, "_candidate_pane", None)
            if pane is None:
                return

            # Restore builders
            builder_state = state.get("builder") or {}
            if builder_state:
                try:
                    pane.builder.from_state(builder_state)
                except Exception as exc:
                    logger.warning("restore_state: builder from_state failed: %s", exc)

            other_builder_state = state.get("other_builder") or {}
            if other_builder_state:
                try:
                    pane.other_builder.from_state(other_builder_state)
                except Exception as exc:
                    logger.warning("restore_state: other_builder from_state failed: %s", exc)

            # Restore other-side enable + mode
            try:
                other_enabled = bool(state.get("other_enabled", False))
                pane.other_enable.setChecked(other_enabled)
                pane.other_box.setVisible(other_enabled)
                other_mode_idx = int(state.get("other_mode_idx", 0))
                if 0 <= other_mode_idx < pane.combine_combo.count():
                    pane.combine_combo.setCurrentIndex(other_mode_idx)
            except RuntimeError:
                pass

            # Restore triage
            self.triage = dict(state.get("triage") or {})

            # Restore filter controls
            try:
                filter_text = state.get("filter_text") or ""
                pane.filter_in.setText(filter_text)
                mat_filter_idx = int(state.get("mat_filter_idx", 0))
                if 0 <= mat_filter_idx < pane.mat_filter.count():
                    pane.mat_filter.setCurrentIndex(mat_filter_idx)
                tri_filter_idx = int(state.get("tri_filter_idx", 0))
                if 0 <= tri_filter_idx < pane.tri_filter.count():
                    pane.tri_filter.setCurrentIndex(tri_filter_idx)
                view_mode = state.get("view_mode") or "grid"
                if view_mode != pane.view_mode:
                    pane.toggle_view()
            except RuntimeError:
                pass

            # DEFERRED search — runs on the background SearchThread, never on the UI thread
            # (hard constraint: no synchronous search / heavy work in restore).
            if sid and not pane.builder.is_empty():
                from PyQt6.QtCore import QTimer
                QTimer.singleShot(0, pane.do_search)

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
            # Cache rows for the joins-context dropdown; reset bulk-select state.
            self._known_join_rows = list(rows)
            self._join_row_checks = []
            try:
                self.chk_join_select_all.blockSignals(True)
                self.chk_join_select_all.setChecked(False)
                self.chk_join_select_all.blockSignals(False)
            except RuntimeError:
                pass

            # D-11: panel hidden entirely when empty
            try:
                self.joins_panel.setVisible(count > 0)
            except RuntimeError:
                return

            try:
                self.btn_joins_toggle.setText(self._joins_header_text())
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

            # Per-row select checkbox (UAT: "add selected to puzzle"). Carries the
            # member sys_id/shelfmark so _add_selected_to_puzzle can collect picks.
            chk = QCheckBox()
            chk.setProperty("member_sid", other_sid)
            chk.setProperty("member_shelf", other_shelf)
            chk.stateChanged.connect(self._on_join_row_check_changed)
            h.addWidget(chk)
            self._join_row_checks.append(chk)

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
            # UAT: adding a single member also pins the anchor (dedup-safe).
            btn_puzzle.clicked.connect(
                lambda checked=False, sid=other_sid: self._add_member_to_puzzle_with_anchor(sid)
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
        # Known-joins puzzle / selection / context callbacks (UAT follow-up)
        # ------------------------------------------------------------------

        def _add_to_puzzle_targets(self, member_sids):
            """Add the anchor + the given members to the puzzle via the public
            host method (SC#5: open_anchor_in_puzzle, NOT _vs_*).

            puzzle_add_targets guarantees the anchor is included exactly once and
            is never duplicated; the puzzle canvas dedups across separate clicks.
            """
            for sid in puzzle_add_targets(self._anchor_sid, member_sids):
                self._app.open_anchor_in_puzzle(sid)

        def _add_member_to_puzzle_with_anchor(self, member_sid):
            """Single-member add — pins the anchor too (UAT)."""
            self._add_to_puzzle_targets([member_sid])

        def _add_selected_to_puzzle(self):
            """Add every checked known-join row (plus the anchor) to the puzzle (UAT)."""
            picks = []
            for chk in self._join_row_checks:
                try:
                    if chk.isChecked():
                        picks.append(chk.property("member_sid") or "")
                except RuntimeError:
                    continue
            self._add_to_puzzle_targets(picks)

        def _on_join_row_check_changed(self, _state=None):
            """Keep the Select-All tri-state in sync with the per-row checkboxes."""
            checks = [c for c in self._join_row_checks]
            if not checks:
                return
            try:
                checked = sum(1 for c in checks if c.isChecked())
                self.chk_join_select_all.blockSignals(True)
                self.chk_join_select_all.setChecked(checked == len(checks) and checked > 0)
                self.chk_join_select_all.blockSignals(False)
            except RuntimeError:
                pass

        def _on_join_select_all(self, state):
            """Select / clear every per-row checkbox."""
            want = bool(state)
            for chk in self._join_row_checks:
                try:
                    chk.blockSignals(True)
                    chk.setChecked(want)
                    chk.blockSignals(False)
                except RuntimeError:
                    continue

        def _show_joins_context_menu(self):
            """Pop a dropdown of the connected members (mirrors the ResultDialog
            joins button). Selecting a member re-anchors the workbench on it."""
            menu = QMenu(self)
            rows = self._known_join_rows or []
            if not rows:
                act = menu.addAction(tr("No joined fragments"))
                act.setEnabled(False)
            else:
                header = menu.addAction(f"{tr('Known Joins')} ({len(rows)})")
                header.setEnabled(False)
                menu.addSeparator()
                for row in rows:
                    sid = row.get("other_sid") or ""
                    shelf = row.get("other_shelf") or "?"
                    label, _ = badge_for_source(row.get("source") or "", self.is_dark)
                    act = menu.addAction(f"[{label}] {shelf}")
                    act.triggered.connect(
                        lambda checked=False, s=sid, sh=shelf: self.set_anchor(
                            {"display": {"id": s, "shelfmark": sh, "img": 1},
                             "uid": f"{s}_P001"}
                        )
                    )
            menu.exec(self.btn_joins_context.mapToGlobal(
                self.btn_joins_context.rect().bottomLeft()
            ))

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

        def _pick_from_list(self):
            """Pick a fragment from a personal list and set it as the anchor (UAT).

            Mirrors the JoinsDialog list-picker (corrections_ui._show_list_picker).
            """
            lists_mgr = getattr(self._app, "lists_mgr", None)
            if not lists_mgr:
                QMessageBox.information(
                    self, tr("Joins Lab"), tr("No lists available")
                )
                return

            dialog = QDialog(self)
            dialog.setWindowTitle(tr("Pick from List"))
            dialog.resize(400, 300)
            layout = QVBoxLayout(dialog)

            list_layout = QHBoxLayout()
            list_layout.addWidget(QLabel(tr("List:")))
            list_combo = QComboBox()
            list_combo.addItem(tr("-- Select list --"), None)
            try:
                all_lists = lists_mgr.get_all_lists(include_recent=True)
            except Exception:
                all_lists = []
            for lst in all_lists:
                display_name = lst.get("name", tr("Unnamed"))
                if lst.get("is_recent"):
                    display_name = tr("Recent")
                list_combo.addItem(display_name, lst.get("id"))
            list_layout.addWidget(list_combo, 1)
            layout.addLayout(list_layout)

            items_list = QListWidget()
            layout.addWidget(items_list, 1)

            def load_list_items():
                items_list.clear()
                list_id = list_combo.currentData()
                if not list_id:
                    return
                try:
                    entries = lists_mgr.get_items_in_list(list_id)
                except Exception:
                    entries = []
                for entry in entries:
                    sid = entry.get("sys_id", "")
                    shelf = sid
                    title = ""
                    if self.meta_mgr and sid:
                        try:
                            s, _ = self.meta_mgr.get_meta_for_id(sid)
                            if s:
                                shelf = s
                            cached = self.meta_mgr.nli_cache.get(sid, {})
                            title = cached.get("title", "")
                            if title and len(title) > 40:
                                title = title[:40] + "..."
                        except (KeyError, AttributeError, IndexError):
                            pass
                    display = f"{shelf} - {title}" if title else shelf
                    li = QListWidgetItem(display)
                    li.setData(Qt.ItemDataRole.UserRole, {"sys_id": sid, "shelfmark": shelf})
                    items_list.addItem(li)

            list_combo.currentIndexChanged.connect(load_list_items)

            btn_row = QHBoxLayout()
            btn_row.addStretch()
            btn_cancel = QPushButton(tr("Cancel"))
            btn_cancel.clicked.connect(dialog.reject)
            btn_row.addWidget(btn_cancel)
            btn_select = QPushButton(tr("Select"))
            btn_select.setEnabled(False)
            btn_row.addWidget(btn_select)
            layout.addLayout(btn_row)

            items_list.itemSelectionChanged.connect(
                lambda: btn_select.setEnabled(bool(items_list.selectedItems()))
            )
            items_list.itemDoubleClicked.connect(lambda: btn_select.click())

            picked = {}

            def do_select():
                sel = items_list.selectedItems()
                if sel:
                    picked.update(sel[0].data(Qt.ItemDataRole.UserRole) or {})
                    dialog.accept()

            btn_select.clicked.connect(do_select)

            if dialog.exec() and picked.get("sys_id"):
                self.set_anchor({
                    "display": {
                        "id": picked["sys_id"],
                        "shelfmark": picked.get("shelfmark", ""),
                        "img": 1,
                    },
                    "uid": f"{picked['sys_id']}_P001",
                })

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
                    self, tr("Joins Lab"), tr("Enter shelfmark…"), items, 0, False
                )
                if not ok:
                    return
                sid = (opts[items.index(choice)] or {}).get("sys_id")

            if not sid:
                QMessageBox.warning(
                    self, tr("Joins Lab"),
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
