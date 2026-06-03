# -*- coding: utf-8 -*-
"""
Join Workbench — a triage SKETCH embedded in the desktop app.

Opened from the 🔗 "Find joins" button on a search-result row, in ResultDialog, or in the
Browse tab. The fragment in view becomes the ANCHOR (pinned left: brief metadata, zoomable
image with folio prev/next, line-numbered text). The scholar builds a responsa-style,
line-by-line query for THIS side, and optionally an identical line-builder for the OTHER side
of the leaf (the recto/verso pair = adjacent image p±1), combined AND (narrow) or OR (widen).
Candidates come back as a paginated thumbnail grid or a responsa-style table, deduped to one
row per ms image, with material + visual-similarity helpers, a highlighted snippet, an in-place
refine/filter bar, and a YES / MAYBE / NO triage toggle; any candidate can be enlarged
side-by-side vs the anchor.

Reuses the app's own pipeline: SearchThread + searcher.execute_search (search),
meta_mgr.enrich_metadata → images_nli/ext + ImageLoaderThread (the proven, Referer-bearing,
Rosetta-fallback image route used by Browse/ResultDialog), searcher.get_browse_page (folio
text), apply_line_numbered_text (RTL gutter). Throwaway sketch — every production-file hook is
tagged JOINS-SKETCH; see .planning/spikes/002-assisted-join-workbench/REVERT.md.
"""
import html
import os
import re
import sqlite3

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDialog, QFrame, QGridLayout,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QPushButton, QScrollArea,
    QSpinBox, QSplitter, QTableWidget, QTableWidgetItem, QTextBrowser,
    QVBoxLayout, QWidget,
)

from desktop.image_loader import ImageLoaderThread
from desktop.widgets.line_number_text_edit import apply_line_numbered_text

try:
    from gui_threads import SearchThread
except Exception:  # pragma: no cover
    SearchThread = None

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PER_PAGE = 20
GRID_COLS = 4
MAX_CONCURRENT_IMG = 5
TRI_COLOR = {"yes": "#16a34a", "maybe": "#d97706", "no": "#dc2626", None: "#94a3b8"}
META_COLOR = "#8b5cf6"   # readable on both light and dark themes
DIM_COLOR = "#0ea5e9"
MARK_A = "\x01"
MARK_B = "\x02"

VS_TIP = ("Visual Similarity (SVM) — a precomputed image-pair score from "
          "visual_similarity.db. Blank means there is NO precomputed pair for these two "
          "fragments (the table covers ~50% of the corpus). A hint, not a join guarantee.")
SCORE_TIP = ("Relevance (Tantivy) — how strongly the TEXT matched your query. Higher = "
             "stronger textual hit. It says nothing about whether the fragments physically join.")


# ----------------------------------------------------------------- result accessors
def r_sid(res):
    return (res.get("display") or {}).get("id") or res.get("sys_id") or ""

def r_shelf(res):
    return (res.get("display") or {}).get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?"

def r_title(res):
    return (res.get("display") or {}).get("title") or ""

def r_text(res):
    return res.get("full_text") or res.get("text") or ""

def r_lib(res):
    d = res.get("display") or {}
    return d.get("library_code") or d.get("library") or ""

def _to_int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError, AttributeError):
        return None


def page_of(res):
    """Best-effort 1-based page/image number for a result."""
    p = _to_int((res.get("display") or {}).get("img"))
    if p:
        return p
    m = re.search(r"_P0*(\d+)", res.get("uid") or "")
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def htmlify(text, pattern=None):
    """Escape + newlines to <br> + optional regex highlight to red bold. RTL."""
    text = text or ""
    if pattern:
        try:
            rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            text = rx.sub(lambda m: MARK_A + m.group(0) + MARK_B, text)
        except re.error:
            pass
    t = html.escape(text)
    t = t.replace("\n", "<br>")
    t = t.replace(MARK_A, "<b style='color:#dc2626'>").replace(MARK_B, "</b>")
    return f"<div dir='rtl' style='text-align:right'>{t}</div>"


def _match_line(lines, pattern):
    if not pattern:
        return -1
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error:
        return -1
    for i, ln in enumerate(lines):
        if rx.search(ln):
            return i
    return -1


def snippet_html(text, pattern, max_lines=8):
    """HTML snippet centered on the first match (so the hit is always visible)."""
    lines = (text or "").split("\n")
    hit = _match_line(lines, pattern)
    if hit < 0:
        chosen = [ln for ln in lines if ln.strip()][:max_lines]
    else:
        lo = max(0, hit - 2)
        chosen = lines[lo:lo + max_lines]
    return htmlify("\n".join(chosen), pattern)


def snippet_plain(text, pattern, max_chars=220):
    """Plain-text snippet centered on the match, for table cells."""
    lines = (text or "").split("\n")
    hit = _match_line(lines, pattern)
    if hit < 0:
        parts = [ln.strip() for ln in lines if ln.strip()][:3]
    else:
        lo = max(0, hit - 1)
        parts = [ln.strip() for ln in lines[lo:lo + 3] if ln.strip()]
    s = "  /  ".join(parts)
    return (s[:max_chars] + "…") if len(s) > max_chars else s


def iiif_full(base_url, width=2000):
    """Turn an images_nli/ext base URL into a loadable image URL (Browse's _resolve_url)."""
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url
    return f"{base_url}/full/{width},/0/default.jpg"


# ----------------------------------------------------------------- material + VS (cached)
def _find_db(name):
    for cand in (
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "GenizahSearchPro", "data", "fist_data", name),
        os.path.join(_ROOT, "fist_data", name),
    ):
        if cand and os.path.isfile(cand):
            return cand
    return None

_FJMS_DB = _find_db("fjms_enrichment.db")
_VS_DB = _find_db("visual_similarity.db")
_MEAS_CACHE = {}
_VS_CACHE = {}


def meas_for(sys_id):
    if not _FJMS_DB or not sys_id:
        return None
    key = str(sys_id)
    if key in _MEAS_CACHE:
        return _MEAS_CACHE[key]
    out = None
    try:
        con = sqlite3.connect(f"file:{_FJMS_DB}?mode=ro", uri=True)
        r = con.execute(
            "select catalog_width_cm,catalog_height_cm,material,avg_num_lines,size_category "
            "from manuscript_measurements where AlmaId=?", (key,)).fetchone()
        con.close()
        if r:
            out = {"w": r[0], "h": r[1], "material": r[2], "lines": r[3], "cat": r[4]}
    except Exception:
        out = None
    _MEAS_CACHE[key] = out
    return out


def material_str(m):
    if not m:
        return None
    bits = []
    if m.get("w") and m.get("h"):
        bits.append(f"{m['w']:.0f}×{m['h']:.0f} cm")
    elif m.get("cat"):
        bits.append(str(m["cat"]))
    if m.get("material"):
        bits.append(str(m["material"]))
    if m.get("lines"):
        bits.append(f"~{m['lines']:.0f} ln")
    return " · ".join(bits) or None


def material_for(sys_id):
    return material_str(meas_for(sys_id))


def vs_score(anchor_sys, cand_sys):
    if not _VS_DB or not anchor_sys or not cand_sys or str(anchor_sys) == str(cand_sys):
        return None
    try:
        a, b = int(anchor_sys), int(cand_sys)
    except (ValueError, TypeError):
        return None
    key = (min(a, b), max(a, b))
    if key in _VS_CACHE:
        return _VS_CACHE[key]
    val = None
    try:
        con = sqlite3.connect(f"file:{_VS_DB}?mode=ro", uri=True)
        r = con.execute(
            "select max(svm_score) from visual_suggestions "
            "where (alma_id_a=? and alma_id_b=?) or (alma_id_a=? and alma_id_b=?)",
            (a, b, b, a)).fetchone()
        con.close()
        if r and r[0] is not None:
            val = r[0]
    except Exception:
        val = None
    _VS_CACHE[key] = val
    return val


def meta_brief(res):
    disp = res.get("display") or {}
    bits = []
    lib = r_lib(res)
    if lib:
        bits.append(str(lib))
    img = disp.get("img")
    if img:
        bits.append(f"img {img}")
    t = r_title(res)
    if t:
        bits.append((t[:70] + "…") if len(t) > 70 else t)
    return "  ·  ".join(bits)


# ----------------------------------------------------------------- workers
class ThumbResolver(QThread):
    """Resolve NLI thumbnail URLs for candidate cards (get_thumbnail does a MARC fetch)."""
    resolved = pyqtSignal(int, str)  # (global index, url or '')

    def __init__(self, meta_mgr, items):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.items = items  # list of (idx, sys_id)
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


class _AnchorLoadWorker(QThread):
    """Load the anchor's image list (the proven enrich_metadata route) + folio text."""
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


class _PageTextWorker(QThread):
    """Fetch one folio's transcription text for the anchor (off-thread)."""
    done = pyqtSignal(str)

    def __init__(self, wb, sid, p):
        super().__init__()
        self.wb = wb
        self.sid = sid
        self.p = p

    def run(self):
        txt = ""
        try:
            txt = (self.wb.searcher.get_browse_page(self.sid, self.p) or {}).get("text", "") or ""
        except Exception:
            txt = ""
        self.done.emit(txt)


class _CrossSideWorker(QThread):
    """Constrain/expand candidates by the OTHER side of the leaf — the adjacent image p±1.
    Query B runs through the real engine (so line-gaps/positions/variants work); matching is
    pure (sys_id, page±1) set membership. AND narrows; OR widens."""
    progress = pyqtSignal(int, int)
    done = pyqtSignal(dict)
    CAP = 4000

    def __init__(self, wb, base, b_query, b_ro, combine, a_pattern):
        super().__init__()
        self.wb = wb
        self.base = base
        self.b_query = b_query
        self.b_ro = b_ro
        self.combine = combine
        self.a_pattern = a_pattern
        self._total = {}
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def _page_total(self, sid):
        if sid not in self._total:
            t = None
            try:
                t = (self.wb.searcher.get_browse_page(sid, 1) or {}).get("total_pages")
            except Exception:
                t = None
            self._total[sid] = t
        return self._total[sid]

    def _make_neighbor_result(self, sid, n):
        txt = ""
        try:
            txt = (self.wb.searcher.get_browse_page(sid, n) or {}).get("text", "") or ""
        except Exception:
            txt = ""
        shelf = title = ""
        lib = ""
        try:
            shelf, title = self.wb.meta_mgr.get_meta_for_id(sid)
        except Exception:
            pass
        try:
            lib = self.wb.meta_mgr.get_library_for_id(sid) or ""
        except Exception:
            pass
        return {
            "display": {"id": sid, "shelfmark": shelf, "title": title,
                        "library_code": lib, "img": n},
            "full_text": txt, "uid": f"{sid}|{n}",
            "highlight_pattern": self.a_pattern, "_via_other_side": True,
        }

    def run(self):
        # 1) run query B through the engine → set of (sid, page) where the OTHER side matches
        try:
            bres = self.wb.searcher.execute_search(
                self.b_query, "exact", 0, responsa_options=self.b_ro, corpus_scope="genizah") or []
        except Exception:
            bres = []
        bset = set()
        for r in bres:
            sid, p = r_sid(r), page_of(r)
            if sid and p is not None:
                bset.add((sid, p))
        out, seen, note = [], set(), ""
        if self.combine == "OR":
            for r in self.base:                       # OR keeps the whole this-side set
                if self._cancel:
                    return
                seen.add((r_sid(r), page_of(r)))
                out.append(r)
            added = 0
            for (sid, q) in list(bset):               # + pages whose OTHER side matched B
                if self._cancel:
                    return
                t = self._page_total(sid)
                for n in (q - 1, q + 1):
                    if n < 1 or (t is not None and n > t):
                        continue
                    if (sid, n) in seen:
                        continue
                    seen.add((sid, n))
                    out.append(self._make_neighbor_result(sid, n))
                    added += 1
            note = f"B matched {len(bset)} pages · +{added} via other side"
        else:                                          # AND: keep only if a neighbor matched B
            for i, r in enumerate(self.base):
                if self._cancel:
                    return
                self.progress.emit(i + 1, len(self.base))
                sid, p = r_sid(r), page_of(r)
                if sid and p is not None and ((sid, p - 1) in bset or (sid, p + 1) in bset):
                    out.append(r)
            note = f"B matched {len(bset)} pages"
        self.done.emit({"results": out, "note": note})


class _VsLoadWorker(QThread):
    """Load the precomputed visual-similarity look-alikes for the anchor as candidates."""
    progress = pyqtSignal(int, int)
    done = pyqtSignal(list)
    LIMIT = 80

    def __init__(self, wb, sys_id):
        super().__init__()
        self.wb = wb
        self.sys_id = sys_id
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        data = []
        try:
            from shared.visual_similarity_service import get_vs_service
            data = get_vs_service().get_suggestions(self.sys_id, limit=self.LIMIT) or []
        except Exception:
            data = []
        csv_bank = getattr(self.wb.meta_mgr, "csv_bank", {}) or {}
        out = []
        n = len(data)
        for i, s in enumerate(data):
            if self._cancel:
                return
            self.progress.emit(i + 1, n)
            sid = s.get("alma_id") or ""
            meta = csv_bank.get(sid) or {}
            txt = ""
            try:
                txt = (self.wb.searcher.get_browse_page(sid, 1) or {}).get("text", "") or ""
            except Exception:
                txt = ""
            out.append({
                "display": {"id": sid, "shelfmark": meta.get("shelfmark") or sid,
                            "title": meta.get("title", "") or "",
                            "library_code": meta.get("library_code", "") or "", "img": 1},
                "full_text": txt, "uid": f"{sid}|vs",
                "_via_vs": True, "svm_score": s.get("svm_score"), "vs_rank": s.get("rank"),
            })
        self.done.emit(out)


# ----------------------------------------------------------------- reusable query builder
class QueryBuilder(QWidget):
    """A responsa-style, line-by-line query builder used for BOTH sides of the leaf.
    Each row = one manuscript line: [⌞start] [word(s)] [end⌝] [↓ N ln gap]. Rows compose into a
    responsa line-break query ( | groups + [|N] line gaps + line_start/line_end anchors )."""

    def __init__(self, on_enter, first_hint="word(s) on this line…"):
        super().__init__()
        self.on_enter = on_enter
        self.first_hint = first_hint
        self.rows = []
        v = QVBoxLayout(self)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(2)
        self.rows_box = QVBoxLayout()
        self.rows_box.setSpacing(2)
        v.addLayout(self.rows_box)
        ctl = QHBoxLayout()
        add = QPushButton("+ line")
        add.setToolTip("add another manuscript line")
        add.clicked.connect(self.add_row)
        ctl.addWidget(add)
        self.variants_chk = QCheckBox("variants")
        self.variants_chk.setToolTip("expand spelling variants (responsa variant expansion)")
        ctl.addWidget(self.variants_chk)
        ctl.addStretch()
        v.addLayout(ctl)
        self.add_row()

    def add_row(self):
        rw = QWidget()
        row = QHBoxLayout(rw)
        row.setContentsMargins(0, 0, 0, 0)
        # RTL layout: in Hebrew a line STARTS on the right and ENDS on the left, so the
        # "ends line" toggle sits left of the field and "starts line" sits right of it.
        end = QCheckBox("ends line ⊣")
        end.setToolTip("the LAST word must be at the END of the line (left edge in Hebrew) — a torn line ending")
        term = QLineEdit()
        term.setPlaceholderText(self.first_hint if not self.rows else "word(s) on this line…")
        term.returnPressed.connect(self.on_enter)
        start = QCheckBox("⊢ starts line")
        start.setToolTip("the FIRST word must be at the START of the line (right edge in Hebrew) — a torn line beginning")
        gap = QSpinBox()
        gap.setRange(0, 40)
        gap.setPrefix("↓ ")
        gap.setSuffix(" ln")
        gap.setToolTip("lines to skip before the next line (0 = the very next line)")
        rm = QPushButton("×")
        rm.setFixedWidth(24)
        entry = {"start": start, "term": term, "end": end, "gap": gap, "rm": rm, "widget": rw}
        rm.clicked.connect(lambda: self.remove_row(entry))
        row.addWidget(end)
        row.addWidget(term, 1)
        row.addWidget(start)
        row.addWidget(gap)
        row.addWidget(rm)
        self.rows.append(entry)
        self.rows_box.addWidget(rw)
        self._sync()

    def remove_row(self, entry):
        if len(self.rows) <= 1:
            return
        self.rows.remove(entry)
        entry["widget"].setParent(None)
        entry["widget"].deleteLater()
        self._sync()

    def _sync(self):
        n = len(self.rows)
        for i, e in enumerate(self.rows):
            e["gap"].setVisible(i < n - 1)   # last row has no "next" line
            e["rm"].setEnabled(n > 1)

    def is_empty(self):
        return not any(e["term"].text().strip() for e in self.rows)

    def _responsa_opts(self):
        v = self.variants_chk.isChecked()
        return {"responsa_mode": True, "variants": v, "ja": False, "flex_spacing": False,
                "bidirectional": False, "variant_mode": "variants" if v else "exact"}

    def compose(self):
        """Return (query_str, responsa_options) or (None, None)."""
        rows = [e for e in self.rows if e["term"].text().strip()]
        if not rows:
            return None, None
        ro = self._responsa_opts()
        multiline = len(rows) > 1 or any(e["start"].isChecked() or e["end"].isChecked() for e in rows)
        if not multiline:
            return rows[0]["term"].text().strip(), ro
        parts = []
        for i, e in enumerate(rows):
            toks = e["term"].text().strip().split()
            if not toks:
                continue
            if e["start"].isChecked():
                toks[0] = "|" + toks[0]
            if e["end"].isChecked():
                toks[-1] = toks[-1] + "|"
            parts.append(" ".join(toks))
            if i < len(rows) - 1:
                parts.append(f"[|{e['gap'].value()}]")
        return " ".join(parts), ro


# ----------------------------------------------------------------- a single result card
class CandidateCard(QFrame):
    def __init__(self, dialog, res, global_idx):
        super().__init__()
        self.dialog = dialog
        self.res = res
        self.global_idx = global_idx
        self.sid = r_sid(res)
        self.setFixedWidth(232)
        self.setFrameShape(QFrame.Shape.Box)
        self._restyle()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(2)

        self.img = QLabel("loading…")
        self.img.setFixedSize(220, 130)
        self.img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.img.setStyleSheet("background:#e2e8f0;color:#64748b;")
        lay.addWidget(self.img)

        if res.get("_is_anchor_self"):
            suffix, tip = "  ⚓ self", "this IS the anchor (shown because 'include anchor itself' is on)"
        elif res.get("_via_text") and res.get("_via_vs"):
            suffix, tip = "  ★ both", "matches BOTH your text query AND visual similarity — strongest signal"
        elif res.get("_via_vs"):
            rk = res.get("vs_rank")
            suffix = f"  ⊙ VS#{rk}" if rk else "  ⊙ VS"
            tip = "from the visual-similarity look-alikes (text-blind)"
        elif res.get("_via_other_side"):
            suffix, tip = "  ⇄ other", "pulled in via the OTHER side (OR)"
        else:
            suffix, tip = "", ""
        shelf = QLabel(r_shelf(res) + suffix)
        shelf.setStyleSheet("font-weight:bold;font-size:12px;")
        shelf.setToolTip(tip)
        lay.addWidget(shelf)

        helpers = []
        mat = material_for(self.sid)
        if mat:
            helpers.append(mat)
        vs = vs_score(dialog.anchor_sid, self.sid)
        if vs is not None:
            helpers.append(f"VS {vs:.2f}")
        if helpers:
            h = QLabel("  ·  ".join(helpers))
            h.setWordWrap(True)
            h.setStyleSheet(f"font-size:10px;color:{META_COLOR};")
            h.setToolTip(VS_TIP if vs is not None else "")
            lay.addWidget(h)

        snip = QTextBrowser()
        snip.setHtml(snippet_html(r_text(res), res.get("highlight_pattern"), max_lines=6))
        snip.setFixedHeight(72)
        snip.setStyleSheet("background:#f8fafc;color:#0f172a;font-size:10px;border:none;")
        lay.addWidget(snip)

        trow = QHBoxLayout()
        for emoji, val in (("Y", "yes"), ("?", "maybe"), ("N", "no")):
            b = QPushButton(emoji)
            b.setFixedWidth(28)
            b.clicked.connect(lambda _, v=val: self.dialog.mark(self.sid, v))
            trow.addWidget(b)
        cmp = QPushButton("⤢")
        cmp.setFixedWidth(28)
        cmp.setToolTip("enlarge / compare beside the anchor")
        cmp.clicked.connect(lambda: self.dialog.open_compare(self.global_idx))
        trow.addWidget(cmp)
        rea = QPushButton("⚓")
        rea.setFixedWidth(28)
        rea.setToolTip("re-anchor on this fragment")
        rea.clicked.connect(lambda: self.dialog.set_anchor(self.res))
        trow.addWidget(rea)
        lay.addLayout(trow)

        arow = QHBoxLayout()
        b_browse = QPushButton("📖"); b_browse.setFixedWidth(28); b_browse.setToolTip("Browse manuscript")
        b_browse.clicked.connect(lambda: self.dialog.act_browse(self.res))
        b_puz = QPushButton("🧩"); b_puz.setFixedWidth(28); b_puz.setToolTip("Add to Puzzle")
        b_puz.clicked.connect(lambda: self.dialog.act_puzzle(self.res))
        b_list = QPushButton("📋"); b_list.setFixedWidth(28); b_list.setToolTip("Add to List")
        b_list.clicked.connect(lambda: self.dialog.act_list(self.res, b_list))
        b_join = QPushButton("🔗"); b_join.setFixedWidth(28); b_join.setToolTip("Add as Join")
        b_join.clicked.connect(lambda: self.dialog.act_join(self.res))
        for b in (b_browse, b_puz, b_list, b_join):
            arow.addWidget(b)
        lay.addLayout(arow)

    def _restyle(self):
        if self.res.get("_is_anchor_self"):
            self.setStyleSheet("QFrame{border:3px solid #14b8a6;border-radius:4px;}")
            return
        tri = self.dialog.triage.get(self.sid)
        self.setStyleSheet(f"QFrame{{border:3px solid {TRI_COLOR[tri]};border-radius:4px;}}")

    def set_pixmap(self, pix):
        try:
            if pix and not pix.isNull():
                self.img.setText("")
                self.img.setPixmap(pix.scaled(220, 130, Qt.AspectRatioMode.KeepAspectRatio,
                                              Qt.TransformationMode.SmoothTransformation))
            else:
                self.img.setText("(no image)")
        except RuntimeError:
            pass


# ----------------------------------------------------------------- the workbench dialog
class JoinWorkbenchDialog(QDialog):
    def __init__(self, parent, app, anchor_result):
        super().__init__(parent)
        self.app = app
        self.searcher = getattr(app, "searcher", None)
        self.meta_mgr = getattr(app, "meta_mgr", None)
        self.setWindowTitle("Join Workbench (sketch)")
        self.resize(1340, 860)

        self.anchor = dict(anchor_result)
        self.anchor_sid = r_sid(self.anchor)
        self.results = []
        self.filtered = []
        self.page = 0
        self.view_mode = "grid"
        self.triage = {}
        self.cards = {}
        self._img_threads = []
        self._img_queue = []
        self._img_active = 0
        self._resolver = None
        self._search_thread = None
        self._cross = None
        self._vs_loader = None
        self._a_pattern = None
        self._anchor_matched = None     # did the anchor itself match the last query?
        self._sources = set()           # {'text','vs'} for the current run
        self._text_cands = None
        self._vs_cands = None
        self._cross_note = ""
        # anchor image / folio state
        self.zoom = 1.0
        self._anchor_full_pix = None
        self._anchor_images = []
        self._anchor_idx = 0
        self._anchor_total = 1

        root = QHBoxLayout(self)
        split = QSplitter(Qt.Orientation.Horizontal)
        root.addWidget(split)
        split.addWidget(self._build_anchor_pane())
        split.addWidget(self._build_results_pane())
        split.setSizes([460, 880])

        self.render_anchor()

    # -------------------------------------------------- pane builders
    def _build_anchor_pane(self):
        left = QWidget()
        lv = QVBoxLayout(left)
        lv.addWidget(self._tag("ANCHOR", "#14b8a6"))
        self.anchor_shelf = QLabel()
        self.anchor_shelf.setStyleSheet("font-weight:bold;font-size:15px;")
        self.anchor_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lv.addWidget(self.anchor_shelf)
        self.anchor_meta = QLabel()
        self.anchor_meta.setWordWrap(True)
        self.anchor_meta.setStyleSheet("font-size:11px;color:#94a3b8;")
        lv.addWidget(self.anchor_meta)
        self.anchor_mat = QLabel()
        self.anchor_mat.setWordWrap(True)
        self.anchor_mat.setStyleSheet(f"font-size:11px;color:{DIM_COLOR};")
        lv.addWidget(self.anchor_mat)

        tb = QHBoxLayout()
        zo = QPushButton("−"); zo.setFixedWidth(30); zo.setToolTip("zoom out")
        zo.clicked.connect(lambda: self.zoom_anchor(1 / 1.25))
        zi = QPushButton("+"); zi.setFixedWidth(30); zi.setToolTip("zoom in")
        zi.clicked.connect(lambda: self.zoom_anchor(1.25))
        self.folio_prev = QPushButton("◀ img"); self.folio_prev.setToolTip("previous image in this shelfmark")
        self.folio_prev.clicked.connect(lambda: self.nav_folio(-1))
        self.folio_lbl = QLabel("")
        self.folio_next = QPushButton("img ▶"); self.folio_next.setToolTip("next image in this shelfmark")
        self.folio_next.clicked.connect(lambda: self.nav_folio(1))
        tb.addWidget(zo); tb.addWidget(zi); tb.addStretch()
        tb.addWidget(self.folio_prev); tb.addWidget(self.folio_lbl); tb.addWidget(self.folio_next)
        lv.addLayout(tb)

        self.anchor_img = QLabel("…")
        self.anchor_img.setMinimumSize(360, 280)
        self.anchor_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.anchor_img.setStyleSheet("background:#e2e8f0;color:#64748b;")
        self.anchor_img_scroll = QScrollArea()
        self.anchor_img_scroll.setWidgetResizable(False)
        self.anchor_img_scroll.setWidget(self.anchor_img)
        self.anchor_img_scroll.setMinimumHeight(300)
        lv.addWidget(self.anchor_img_scroll, 1)

        self.anchor_text = QTextBrowser()
        lv.addWidget(self.anchor_text, 1)
        return left

    def _build_results_pane(self):
        right = QWidget()
        rv = QVBoxLayout(right)

        rv.addWidget(self._tag("THIS SIDE — find candidate pages matching, line by line "
                               "(hunt the MISSING continuation, not what's already in the anchor):",
                               "#0f766e"))
        self.builder = QueryBuilder(self.do_search,
                                    first_hint="word(s) on this line…")
        rv.addWidget(self.builder)

        # --- OTHER SIDE (recto/verso pair) ---
        os_top = QHBoxLayout()
        self.other_enable = QCheckBox("also constrain the OTHER side (the recto/verso pair = adjacent image p±1):")
        self.other_enable.setToolTip("first image → only +1, last → only −1, middle → both neighbours tested")
        self.other_enable.toggled.connect(lambda v: self.other_box.setVisible(v))
        os_top.addWidget(self.other_enable)
        self.other_combine = QComboBox()
        self.other_combine.addItems(["AND (narrow)", "OR (widen)"])
        self.other_combine.setToolTip("AND: keep only candidates whose OTHER side also matches (narrow a flood).\n"
                                      "OR: also pull in pages whose other side matches (widen a poor yield).")
        os_top.addWidget(self.other_combine)
        os_top.addStretch()
        rv.addLayout(os_top)
        self.other_box = QWidget()
        ob = QVBoxLayout(self.other_box)
        ob.setContentsMargins(18, 0, 0, 0)
        self.other_builder = QueryBuilder(self.do_search,
                                          first_hint="word(s) required on the OTHER side…")
        ob.addWidget(self.other_builder)
        self.other_box.setVisible(False)
        rv.addWidget(self.other_box)

        sb = QHBoxLayout()
        self.include_anchor_chk = QCheckBox("include anchor itself")
        self.include_anchor_chk.setToolTip("show the anchor fragment in the results if it matches — "
                                           "useful to verify your query works (applies on next Search)")
        sb.addWidget(self.include_anchor_chk)
        sb.addStretch()
        self.visual_btn = QPushButton("Visual similarities")
        self.visual_btn.setToolTip("load the precomputed visual-similarity look-alikes for the anchor")
        self.visual_btn.clicked.connect(self.load_visual)
        sb.addWidget(self.visual_btn)
        self.combine_btn = QPushButton("Search + visual")
        self.combine_btn.setToolTip("run the text query AND load visual look-alikes, merged "
                                    "(★ = a fragment that hits BOTH — the strongest signal)")
        self.combine_btn.clicked.connect(self.do_combine)
        sb.addWidget(self.combine_btn)
        self.search_btn = QPushButton("Search")
        self.search_btn.setMinimumWidth(110)
        self.search_btn.clicked.connect(self.do_search)
        sb.addWidget(self.search_btn)
        rv.addLayout(sb)

        # --- refine / filter bar (items 8, 9, 12) ---
        refine = QHBoxLayout()
        refine.addWidget(QLabel("Refine:"))
        self.filter_in = QLineEdit()
        self.filter_in.setPlaceholderText("filter / search within results (text, shelfmark, title)…")
        self.filter_in.textChanged.connect(self.apply_filters)
        refine.addWidget(self.filter_in, 1)
        self.mat_filter = QComboBox()
        self.mat_filter.addItem("any material")
        self.mat_filter.currentIndexChanged.connect(self.apply_filters)
        refine.addWidget(self.mat_filter)
        self.dim_chk = QCheckBox("has dimensions")
        self.dim_chk.stateChanged.connect(self.apply_filters)
        refine.addWidget(self.dim_chk)
        self.tri_filter = QComboBox()
        self.tri_filter.addItems(["all", "yes", "maybe", "no", "untriaged"])
        self.tri_filter.currentIndexChanged.connect(self.apply_filters)
        refine.addWidget(self.tri_filter)
        rv.addLayout(refine)

        # --- status + view toggle + paging ---
        info = QHBoxLayout()
        self.status = QLabel("Build a line-by-line query, then Search.")
        self.status.setStyleSheet("font-size:11px;color:#94a3b8;")
        info.addWidget(self.status, 1)
        self.view_btn = QPushButton("Table view")
        self.view_btn.setToolTip("toggle thumbnail grid <-> responsa-style table (resizable, text columns)")
        self.view_btn.clicked.connect(self.toggle_view)
        info.addWidget(self.view_btn)
        self.prev_btn = QPushButton("<"); self.prev_btn.setFixedWidth(34)
        self.prev_btn.clicked.connect(lambda: self.set_page(self.page - 1))
        self.page_lbl = QLabel("")
        self.next_btn = QPushButton(">"); self.next_btn.setFixedWidth(34)
        self.next_btn.clicked.connect(lambda: self.set_page(self.page + 1))
        info.addWidget(self.prev_btn); info.addWidget(self.page_lbl); info.addWidget(self.next_btn)
        rv.addLayout(info)

        # --- grid view ---
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.grid_host = QWidget()
        self.grid = QGridLayout(self.grid_host)
        self.grid.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.scroll.setWidget(self.grid_host)
        rv.addWidget(self.scroll, 1)

        # --- table view (hidden until toggled) ---
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["", "Shelfmark", "Library", "Title", "Text", "Material", "VS", "Relevance"])
        self.table.setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setWordWrap(True)
        self.table.setSortingEnabled(False)
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.table.setColumnWidth(0, 24)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.verticalHeader().setDefaultSectionSize(46)
        for col, tip in ((6, VS_TIP), (7, SCORE_TIP)):
            it = self.table.horizontalHeaderItem(col)
            if it:
                it.setToolTip(tip)
        self.table.cellDoubleClicked.connect(self._on_table_double)
        rv.addWidget(self.table, 1)
        return right

    @staticmethod
    def _tag(text, color):
        lbl = QLabel(text)
        lbl.setWordWrap(True)
        lbl.setStyleSheet(f"font-weight:bold;font-size:11px;color:{color};")
        return lbl

    # -------------------------------------------------- anchor
    def set_anchor(self, res):
        self.anchor = dict(res)
        self.anchor_sid = r_sid(self.anchor)
        self._anchor_images = []
        self._anchor_idx = 0
        self._anchor_total = 1
        self.render_anchor()
        self.apply_filters()  # VS scores recompute against the new anchor

    def render_anchor(self):
        self.anchor_shelf.setText(r_shelf(self.anchor))
        self.anchor_meta.setText(meta_brief(self.anchor))
        ms = material_str(meas_for(self.anchor_sid))
        self.anchor_mat.setText(("◧  " + ms) if ms else "")
        apply_line_numbered_text(
            self.anchor_text,
            htmlify(r_text(self.anchor), self.anchor.get("highlight_pattern")),
            source_text=r_text(self.anchor), is_html=True,
        )
        self.zoom = 1.0
        self._anchor_full_pix = None
        self.anchor_img.setText("…")
        page = _to_int((self.anchor.get("display") or {}).get("img")) or 1
        self.folio_lbl.setText(f"{page}/?")
        w = _AnchorLoadWorker(self, self.anchor_sid, page, initial=True)
        w.done.connect(self._on_anchor_loaded)
        self._img_threads.append(w)
        w.start()

    def _on_anchor_loaded(self, out):
        self._anchor_images = out.get("images") or []
        total = len(self._anchor_images) or out.get("total") or 1
        self._anchor_total = total
        page = out.get("page") or 1
        self._anchor_idx = max(0, min(page - 1, total - 1))
        self.folio_lbl.setText(f"{self._anchor_idx + 1}/{total}")
        if not out.get("initial"):
            txt = out.get("text") or ""
            if txt:
                apply_line_numbered_text(
                    self.anchor_text, htmlify(txt, self.anchor.get("highlight_pattern")),
                    source_text=txt, is_html=True)
        self.zoom = 1.0
        self._load_anchor_image_at(self._anchor_idx)

    def _image_url_at(self, idx):
        if 0 <= idx < len(self._anchor_images):
            return iiif_full((self._anchor_images[idx] or {}).get("url", ""), 2000)
        return ""

    def _load_anchor_image_at(self, idx):
        url = self._image_url_at(idx)
        if not url:
            try:
                self.anchor_img.setText("(no image)")
            except RuntimeError:
                pass
            return
        self._load_anchor_image(url)

    def nav_folio(self, d):
        if self._anchor_total <= 1:
            return
        new = max(0, min(self._anchor_idx + d, self._anchor_total - 1))
        if new == self._anchor_idx:
            return
        self._anchor_idx = new
        self.folio_lbl.setText(f"{new + 1}/{self._anchor_total}")
        self.zoom = 1.0
        self._anchor_full_pix = None
        self.anchor_img.setText("…")
        self._load_anchor_image_at(new)
        tw = _PageTextWorker(self, self.anchor_sid, new + 1)
        tw.done.connect(self._on_page_text)
        self._img_threads.append(tw)
        tw.start()

    def _on_page_text(self, txt):
        if txt:
            apply_line_numbered_text(
                self.anchor_text, htmlify(txt, self.anchor.get("highlight_pattern")),
                source_text=txt, is_html=True)

    def zoom_anchor(self, factor):
        self.zoom = max(0.3, min(4.0, self.zoom * factor))
        self._render_anchor_image()

    def _render_anchor_image(self):
        pix = self._anchor_full_pix
        if not pix or pix.isNull():
            return
        w = max(140, int(360 * self.zoom))
        scaled = pix.scaledToWidth(w, Qt.TransformationMode.SmoothTransformation)
        try:
            self.anchor_img.setText("")
            self.anchor_img.setPixmap(scaled)
            self.anchor_img.resize(scaled.size())
        except RuntimeError:
            pass

    def _load_anchor_image(self, url):
        if not url:
            try:
                self.anchor_img.setText("(no image)")
            except RuntimeError:
                pass
            return
        loader = ImageLoaderThread(url)

        def _done(img):
            try:
                self._anchor_full_pix = QPixmap.fromImage(img)
                self._render_anchor_image()
            except RuntimeError:
                pass

        def _fail():
            try:
                self.anchor_img.setText("(no image)")
            except RuntimeError:
                pass

        loader.image_loaded.connect(_done)
        loader.load_failed.connect(_fail)
        self._img_threads.append(loader)
        loader.start()

    # -------------------------------------------------- search / candidate sources
    def do_search(self):      # text only
        self._run_sources({"text"})

    def load_visual(self):    # visual similarity only
        self._run_sources({"vs"})

    def do_combine(self):     # text + visual, merged
        self._run_sources({"text", "vs"})

    def _run_sources(self, sources):
        sources = set(sources)
        q = ro = None
        if "text" in sources:
            q, ro = self.builder.compose()
            if not q:
                if sources == {"text"}:
                    self.status.setText("type a query for the text side"); return
                sources.discard("text")   # combine with empty text → visual only
        if not sources:
            self.status.setText("nothing to search"); return
        if not self.searcher or SearchThread is None:
            self.status.setText("search engine unavailable"); return
        self._sources = sources
        self._text_cands = None
        self._vs_cands = None
        self._cross_note = ""
        if "text" not in sources:
            self._anchor_matched = None   # no text query → no self-match readout
        if self._cross and self._cross.isRunning():
            self._cross.cancel()
        if self._vs_loader and self._vs_loader.isRunning():
            self._vs_loader.cancel()
        self._set_busy(True)
        self.status.setText("working…")
        if "text" in sources:
            self._search_thread = SearchThread(self.searcher, q, "exact", 0,
                                               responsa_options=ro, corpus_scope="genizah")
            self._search_thread.results_signal.connect(self._on_results)
            self._search_thread.error_signal.connect(self._on_search_error)
            self._search_thread.start()
        if "vs" in sources:
            self._vs_loader = _VsLoadWorker(self, self.anchor_sid)
            self._vs_loader.progress.connect(self._on_vs_progress)
            self._vs_loader.done.connect(self._on_vs_loaded)
            self._vs_loader.start()

    def _set_busy(self, busy):
        for b in (self.search_btn, self.visual_btn, self.combine_btn):
            b.setEnabled(not busy)

    def _on_search_error(self, msg):
        self._set_busy(False)
        self.status.setText("error: " + str(msg)[:140])

    def _on_results(self, results):
        # did the anchor itself match? (self-verification signal)
        self._anchor_matched = any(r_sid(r) == self.anchor_sid for r in results)
        include_self = self.include_anchor_chk.isChecked()
        seen, deduped = set(), []           # compact to one row per ms image (item 7)
        for r in results:
            is_self = (r_sid(r) == self.anchor_sid)
            if is_self and not include_self:
                continue
            key = r.get("uid") or f"{r_sid(r)}|{(r.get('display') or {}).get('img')}"
            if key in seen:
                continue
            seen.add(key)
            r = dict(r); r["_via_text"] = True
            if is_self:
                r["_is_anchor_self"] = True
            deduped.append(r)
        self._a_pattern = next((r.get("highlight_pattern") for r in deduped
                                if not r.get("_is_anchor_self")), None)
        if self.other_enable.isChecked() and not self.other_builder.is_empty():
            bq, bro = self.other_builder.compose()
            cs = "OR" if self.other_combine.currentIndex() == 1 else "AND"
            self.status.setText(f"this side: {len(deduped)} · checking the other side ({cs})…")
            self._cross = _CrossSideWorker(self, deduped, bq, bro, cs, self._a_pattern)
            self._cross.progress.connect(self._on_cross_progress)
            self._cross.done.connect(self._on_cross_done)
            self._cross.start()
        else:
            self._set_text_cands(deduped, "")

    def _on_cross_progress(self, i, n):
        self.status.setText(f"checking the other side… {i}/{n}")

    def _on_cross_done(self, payload):
        self._set_text_cands(payload.get("results", []), payload.get("note", ""))

    def _on_vs_progress(self, i, n):
        self.status.setText(f"loading visual similarities… {i}/{n}")

    def _on_vs_loaded(self, vs_list):
        self._set_vs_cands([v for v in vs_list if r_sid(v) != self.anchor_sid])

    def _set_text_cands(self, cands, note=""):
        self._text_cands = cands
        self._cross_note = note
        self._maybe_assemble()

    def _set_vs_cands(self, cands):
        self._vs_cands = cands
        self._maybe_assemble()

    def _maybe_assemble(self):
        if "text" in self._sources and self._text_cands is None:
            return
        if "vs" in self._sources and self._vs_cands is None:
            return
        text = self._text_cands or []
        vs = self._vs_cands or []
        if "vs" not in self._sources:
            merged = text
        elif "text" not in self._sources:
            merged = vs
        else:                                          # combine: annotate overlap, append VS-only
            vs_by_sid = {r_sid(v): v for v in vs}
            for r in text:
                v = vs_by_sid.get(r_sid(r))
                if v is not None:
                    r["_via_vs"] = True
                    r["vs_rank"] = v.get("vs_rank")
            text_sids = {r_sid(r) for r in text}
            merged = text + [v for v in vs if r_sid(v) not in text_sids]

            def _k(r):
                both = r.get("_via_text") and r.get("_via_vs")
                tier = 0 if both else (1 if r.get("_via_text") else 2)
                return (tier, r.get("vs_rank") or 99999)
            merged.sort(key=_k)
        self.results = merged
        self._set_busy(False)
        self._finish_results()

    def _finish_results(self):
        self._populate_material_filter()
        self.apply_filters()
        extra = []
        if "vs" in self._sources and "text" in self._sources:
            extra.append("text + visual")
        elif self._sources == {"vs"}:
            extra.append("visual only")
        if self._cross_note:
            extra.append(self._cross_note)
        if extra:
            self.status.setText(self.status.text() + "  ·  " + " · ".join(extra))

    def _populate_material_filter(self):
        mats = sorted({(meas_for(r_sid(r)) or {}).get("material")
                       for r in self.results if (meas_for(r_sid(r)) or {}).get("material")})
        self.mat_filter.blockSignals(True)
        self.mat_filter.clear()
        self.mat_filter.addItem("any material")
        for m in mats:
            self.mat_filter.addItem(str(m))
        self.mat_filter.blockSignals(False)

    # -------------------------------------------------- refine / filter
    def apply_filters(self, *_):
        text = self.filter_in.text().strip().lower()
        mat = self.mat_filter.currentText()
        need_dim = self.dim_chk.isChecked()
        tri_want = self.tri_filter.currentText()
        out = []
        for r in self.results:
            sid = r_sid(r)
            if text:
                hay = f"{r_shelf(r)} {r_title(r)} {r_text(r)}".lower()
                if text not in hay:
                    continue
            m = meas_for(sid)
            if mat and mat != "any material" and (not m or str(m.get("material") or "") != mat):
                continue
            if need_dim and (not m or not (m.get("w") and m.get("h"))):
                continue
            if tri_want != "all":
                tri = self.triage.get(sid)
                if tri_want == "untriaged":
                    if tri is not None:
                        continue
                elif tri != tri_want:
                    continue
            out.append(r)
        self.filtered = out
        self.page = 0
        prefix = ""
        if self._anchor_matched is True:
            prefix = "⚓ anchor matches this query ✓  ·  "
        elif self._anchor_matched is False:
            prefix = "⚓ anchor does NOT match this query ✗  ·  "
        self.status.setText(f"{prefix}{len(self.filtered)}/{len(self.results)} shown")
        self.render_results()

    # -------------------------------------------------- view dispatch (grid <-> table)
    def render_results(self):
        if self.view_mode == "table":
            self.scroll.setVisible(False)
            self.table.setVisible(True)
            self.render_table()
        else:
            self.table.setVisible(False)
            self.scroll.setVisible(True)
            self.render_page()

    def toggle_view(self):
        self.view_mode = "table" if self.view_mode == "grid" else "grid"
        self.view_btn.setText("Grid view" if self.view_mode == "table" else "Table view")
        self.render_results()

    def render_table(self):
        self._cancel_images()
        self.table.setRowCount(0)
        self.table.setRowCount(len(self.filtered))
        for i, res in enumerate(self.filtered):
            sid = r_sid(res)
            tri = self.triage.get(sid)
            vs = vs_score(self.anchor_sid, sid)
            score = ""
            try:
                if res.get("score") is not None:
                    score = f"{float(res['score']):.0f}"
            except (TypeError, ValueError):
                pass
            vals = [
                {"yes": "Y", "maybe": "?", "no": "N"}.get(tri, ""),
                r_shelf(res), r_lib(res), r_title(res),
                snippet_plain(r_text(res), res.get("highlight_pattern")),
                material_for(sid) or "",
                (f"{vs:.2f}" if vs is not None else ""),
                score,
            ]
            for c, val in enumerate(vals):
                it = QTableWidgetItem(str(val))
                it.setData(Qt.ItemDataRole.UserRole, i)
                if c == 6 and vs is not None:
                    it.setToolTip(VS_TIP)
                elif c == 7 and score:
                    it.setToolTip(SCORE_TIP)
                self.table.setItem(i, c, it)
        self.table.resizeRowsToContents()
        self.page_lbl.setText(f"{len(self.filtered)} rows")
        self._update_status_counts()

    def _on_table_double(self, row, _col):
        it = self.table.item(row, 0)
        if it is not None:
            self.open_compare(it.data(Qt.ItemDataRole.UserRole))

    # -------------------------------------------------- triage / paging
    def mark(self, sid, val):
        self.triage[sid] = None if self.triage.get(sid) == val else val
        for card in self.cards.values():
            if card.sid == sid:
                card._restyle()
        if self.view_mode == "table" and self.table.isVisible():
            self.render_table()
        self._update_status_counts()

    def _update_status_counts(self):
        y = sum(1 for v in self.triage.values() if v == "yes")
        m = sum(1 for v in self.triage.values() if v == "maybe")
        n = sum(1 for v in self.triage.values() if v == "no")
        base = self.status.text().split("  [")[0]
        self.status.setText(f"{base}  [Y {y}  ? {m}  N {n}]")

    def set_page(self, p):
        total = max(1, (len(self.filtered) + PER_PAGE - 1) // PER_PAGE)
        self.page = max(0, min(p, total - 1))
        self.render_page()

    def render_page(self):
        self._cancel_images()
        while self.grid.count():
            w = self.grid.takeAt(0).widget()
            if w:
                w.deleteLater()
        self.cards.clear()
        data = self.filtered
        total = max(1, (len(data) + PER_PAGE - 1) // PER_PAGE)
        start = self.page * PER_PAGE
        page_rs = data[start:start + PER_PAGE]
        self.page_lbl.setText(f"{self.page + 1}/{total}")
        items_for_thumbs = []
        for i, res in enumerate(page_rs):
            gidx = start + i
            card = CandidateCard(self, res, gidx)
            self.cards[gidx] = card
            self.grid.addWidget(card, i // GRID_COLS, i % GRID_COLS)
            items_for_thumbs.append((gidx, r_sid(res)))
        self._update_status_counts()
        if items_for_thumbs and self.meta_mgr is not None:
            self._resolver = ThumbResolver(self.meta_mgr, items_for_thumbs)
            self._resolver.resolved.connect(self._on_thumb_url)
            self._resolver.start()

    def _on_thumb_url(self, gidx, url):
        card = self.cards.get(gidx)
        if not card:
            return
        if not url:
            card.set_pixmap(None)
            return
        self._enqueue_image(card.img, url, target=None)

    # -------------------------------------------------- image loading (bounded; compare panes)
    def _load_image(self, label, sys_id, width, target=None):
        if not self.meta_mgr or not sys_id:
            label.setText("(no image)"); return
        outer = self

        class _One(QThread):
            got = pyqtSignal(str)

            def run(self_thr):
                try:
                    self_thr.got.emit(outer.meta_mgr.get_thumbnail(sys_id) or "")
                except Exception:
                    self_thr.got.emit("")

        t = _One()
        t.got.connect(lambda u: self._enqueue_image(label, re.sub(r"/full/\d+,/", f"/full/{width},/", u), target=target)
                      if u else label.setText("(no image)"))
        self._img_threads.append(t)
        t.start()

    def _enqueue_image(self, label, url, target=None):
        if not url:
            label.setText("(no image)"); return
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
                    pix = QPixmap.fromImage(img)
                    parent_card = lbl.parent()
                    if tg:
                        lbl.setText("")
                        lbl.setPixmap(pix.scaled(tg[0], tg[1], Qt.AspectRatioMode.KeepAspectRatio,
                                                 Qt.TransformationMode.SmoothTransformation))
                    elif isinstance(parent_card, CandidateCard):
                        parent_card.set_pixmap(pix)
                    else:
                        lbl.setText("")
                        lbl.setPixmap(pix.scaled(220, 130, Qt.AspectRatioMode.KeepAspectRatio,
                                                 Qt.TransformationMode.SmoothTransformation))
                except RuntimeError:
                    pass
                self._pump_images()

            def _fail(lbl=label):
                self._img_active -= 1
                try:
                    lbl.setText("(no image)")
                except RuntimeError:
                    pass
                self._pump_images()

            loader.image_loaded.connect(_done)
            loader.load_failed.connect(_fail)
            self._img_threads.append(loader)
            loader.start()

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

    # -------------------------------------------------- compare (one-by-one)
    def open_compare(self, global_idx):
        if not self.filtered:
            return
        CompareDialog(self, global_idx).exec()

    # -------------------------------------------------- candidate actions (reuse app methods)
    def act_browse(self, res):
        try:
            self.app.open_result_in_browse_from_table(res)
        except Exception as e:
            self.status.setText("browse failed: " + str(e)[:90])

    def act_puzzle(self, res):
        try:
            self.app._vs_add_to_puzzle(r_sid(res))
        except Exception as e:
            self.status.setText("puzzle failed: " + str(e)[:90])

    def act_list(self, res, btn):
        try:
            items = [{"sys_id": r_sid(res), "fl_id": "", "img": ""}]
            self.app.show_add_to_list_menu(items, source="from Join Workbench", anchor_widget=btn)
        except Exception as e:
            self.status.setText("add-to-list failed: " + str(e)[:90])

    def act_join(self, res):
        try:
            self.app._vs_open_joins_with_partner(self.anchor_sid, r_shelf(self.anchor),
                                                 r_sid(res), r_shelf(res))
        except Exception as e:
            self.status.setText("add-as-join failed: " + str(e)[:90])

    def closeEvent(self, ev):
        self._cancel_images()
        if self._search_thread and self._search_thread.isRunning():
            try:
                self._search_thread.cancel_flag = True
            except Exception:
                pass
        if self._cross and self._cross.isRunning():
            self._cross.cancel()
        super().closeEvent(ev)


# ----------------------------------------------------------------- enlarge / compare
class CompareDialog(QDialog):
    def __init__(self, wb, start_idx):
        super().__init__(wb)
        self.wb = wb
        self.idx = max(0, min(start_idx, len(wb.filtered) - 1))
        self.setWindowTitle("Compare")
        self.resize(1320, 870)
        v = QVBoxLayout(self)

        topbar = QHBoxLayout()
        prev = QPushButton("< prev"); prev.clicked.connect(lambda: self.step(-1))
        nxt = QPushButton("next >"); nxt.clicked.connect(lambda: self.step(1))
        self.pos_lbl = QLabel("")
        topbar.addWidget(prev); topbar.addWidget(self.pos_lbl, 1); topbar.addWidget(nxt)
        for emoji, val in (("Y yes", "yes"), ("? maybe", "maybe"), ("N no", "no")):
            b = QPushButton(emoji)
            b.clicked.connect(lambda _, x=val: self._mark(x))
            topbar.addWidget(b)
        v.addLayout(topbar)

        arow = QHBoxLayout()
        for label, fn in (
            (f"📖 {''}Browse", lambda: self.wb.act_browse(self._cur())),
            ("🧩 Puzzle", lambda: self.wb.act_puzzle(self._cur())),
            ("📋 Add to List", lambda: self.wb.act_list(self._cur(), None)),
            ("🔗 Add as Join", lambda: self.wb.act_join(self._cur())),
            ("⚓ Re-anchor", self._reanchor),
        ):
            b = QPushButton(label)
            b.clicked.connect(lambda _, f=fn: f())
            arow.addWidget(b)
        arow.addStretch()
        v.addLayout(arow)

        body = QHBoxLayout()
        self.left = self._pane()
        self.right = self._pane()
        body.addLayout(self.left["box"]); body.addLayout(self.right["box"])
        v.addLayout(body, 1)
        self.paint()

    def _pane(self):
        box = QVBoxLayout()
        shelf = QLabel(); shelf.setStyleSheet("font-weight:bold;font-size:13px;")
        meta = QLabel(); meta.setWordWrap(True); meta.setStyleSheet(f"font-size:11px;color:{META_COLOR};")
        img = QLabel("…"); img.setMinimumHeight(360)
        img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        img.setStyleSheet("background:#e2e8f0;color:#64748b;")
        txt = QTextBrowser()
        box.addWidget(shelf); box.addWidget(meta); box.addWidget(img); box.addWidget(txt, 1)
        return {"box": box, "shelf": shelf, "meta": meta, "img": img, "txt": txt}

    def _fill(self, pane, res, is_candidate):
        pane["shelf"].setText(r_shelf(res))
        bits = [meta_brief(res)]
        ms = material_str(meas_for(r_sid(res)))
        if ms:
            bits.append("◧ " + ms)
        if is_candidate:
            vs = vs_score(self.wb.anchor_sid, r_sid(res))
            if vs is not None:
                bits.append(f"VS {vs:.2f}")
                pane["meta"].setToolTip(VS_TIP)
        pane["meta"].setText("   ·   ".join([b for b in bits if b]))
        apply_line_numbered_text(pane["txt"], htmlify(r_text(res), res.get("highlight_pattern")),
                                 source_text=r_text(res), is_html=True)
        pane["img"].setText("…")
        self.wb._load_image(pane["img"], r_sid(res), width=1400, target=(620, 460))

    def paint(self):
        cand = self.wb.filtered[self.idx]
        self.pos_lbl.setText(f"candidate {self.idx + 1}/{len(self.wb.filtered)}   "
                             f"anchor {r_shelf(self.wb.anchor)}  vs  {r_shelf(cand)}   "
                             f"[{self.wb.triage.get(r_sid(cand)) or '-'}]")
        self._fill(self.left, self.wb.anchor, is_candidate=False)
        self._fill(self.right, cand, is_candidate=True)

    def step(self, d):
        self.idx = max(0, min(self.idx + d, len(self.wb.filtered) - 1))
        self.paint()

    def _cur(self):
        return self.wb.filtered[self.idx]

    def _reanchor(self):
        self.wb.set_anchor(self._cur())
        self.accept()

    def _mark(self, val):
        self.wb.mark(r_sid(self.wb.filtered[self.idx]), val)
        self.paint()
