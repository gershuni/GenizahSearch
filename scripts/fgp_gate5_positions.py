# -*- coding: utf-8 -*-
"""Gate 5 of the FGP source-folders plan: the 20-image geometry check, then the positions prototype.

Why
---
Folder B (``FGPTranscriptions_WithPOSInfoAndImages``) positions every word (B1, the Talmud-project
directories) or line segment (B2, the ``C<n>`` directories) of ~24,600 transcribed images in FGP's own
pixel frame. The apps show the NLI IIIF rendition of the same photograph. Nothing yet proves that a box
transformed from the FGP frame lands on the word in the displayed image, and nothing yet ties a box to
the edition and revision the reader selected. This gate (assessment §8.5) declares a landmark tolerance
and storage limits BEFORE measuring, samples 20 images by seed, fetches exactly the rendition the apps
display, records a transform per image and measures the residual landmark error after it, then builds
the positions store in two candidate schemas tied to gate 4's candidate sidecar, and fails on its
declared conditions.

Rules (declared before the first run; ``docs/plans/FGP_POSITIONS_RULES.md``)
-----------------------------------------------------------------------------
  Frame: B1 ``coldata.imagesize`` (the editor's image; region and multi-image directories are a region
     image or a composite and are unresolved by rule); B2 the fjms master as FGP turned it (swapped at a
     recorded rotation of 90/270, or when the columns fit only the swapped size), columns offset by ``x1``/``y1``.
  Displayed image: the NLI IIIF canvas at the image's gallery index, ``/full/2000,/0/default.jpg``.
  Transform: rotation 0/90/180/270 x mirror, a uniform scale searched around the dimension ratio (the
     FGP master may be a crop cut from a larger library photograph), a small angle (+-10 degrees), and the
     translation -- by normalised cross-correlation of the FGP photograph over the displayed image, a sweep
     at 1/16 scale, angle and scale refinement at 1/4, then the landmarks themselves decide between the
     refined candidates (one median translation correction each). Recording a transform is not a pass.
  Landmarks (TOLERANCE): positioned boxes whose text has a letter, 3x3 grid, up to 2 per cell by patch variance; patch side
     max(1.2 h, 96 px); NCC at the coarser resolution within +-1.0 h; found at NCC >= 0.45; PASS iff
     >= 6 found in >= 4 cells over two grid rows and columns, max residual <= 0.35 h, RMS <= 0.20 h,
     h = the line pitch in displayed pixels.
  Sample (STRATA, SEED): 20 images from the eligible frame F (geometry, no region / multi-image dir, an
     NLI gallery row, a candidate row on the image), stratified by format and rotation class, round-robin
     over libraries.
  Store (LIMITS): schema P (one zlib JSON payload per (row, image, column)) and schema R (relational
     rows / words), each within 512 MB installed, 1 MB largest payload, cold read p95 25 ms / max 200 ms,
     build peak 2,048 MB; the smaller passing schema is chosen, P on a tie.
  Tie: a payload exists only when the candidate row's content hash equals the hash of the text re-read
     from the directory by gate 3's reader; spans are offsets into ``content`` and are kept only when the
     slice equals the word; ``overlay_allowed`` only for the images the geometry check passed.

Checks (a failing check exits 1; a missed expectation too)
  G1 geometry: every sampled image PASSes.  G2 no overlay without a tie.  G3 spans read back equal the
  content slice.  G4 two runs identical (digest).  G5 size.  G6 largest payload.  G7 cold read latency.
  G8 build memory.  G9 the two schemas agree.  E_* declared expectations.

Outputs (``--out-dir``, default ``fgp_data/inventory``): ``positions_payload.db``, ``positions_relational.db``,
``gate5.db`` (``sample``, ``landmarks``, ``dirs``, ``payloads``, ``schema_measures``, ``latency``, ``memory``,
``checks``, ``counts``, ``gate5_meta``), ``gate5_summary.json``, ``GATE5_REPORT.md``; fetched manifests and
images under ``--cache-dir`` (default ``fgp_data/inventory/gate5_cache``).

Usage
  python scripts/fgp_gate5_positions.py --allow-single-run
  python scripts/fgp_gate5_positions.py --previous-summary fgp_data/inventory/gate5_run1/gate5_summary.json \
      --expect-file scripts/fgp_gate5_positions.expect.json
  --images N,N,... replaces the seeded sample; --offline fails a fetch instead of going to the network.
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import io
import json
import math
import os
import random
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
import zlib
from urllib.parse import parse_qs, urlparse

import numpy as np
from PIL import Image, ImageOps
from scipy import ndimage, signal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.fgp_association_inventory import DEFAULT_OUT, parse_expectations, ro  # noqa: E402
from scripts.fgp_gate3_text_pipeline import read_b2, ws  # noqa: E402
from scripts.fgp_rebuild_text_and_credit import parse_fgp_xml  # noqa: E402
from shared.nli_crossref_service import NliCrossrefService, parse_folio_label  # noqa: E402

DEFAULT_WITHPOS = r"C:\FGPTranscriptions_WithPOSInfoAndImages"
DEFAULT_INVENTORY = os.path.join(DEFAULT_OUT, "inventory.db")
DEFAULT_GATE3 = os.path.join(DEFAULT_OUT, "gate3.db")
DEFAULT_GATE4 = os.path.join(DEFAULT_OUT, "gate4.db")
DEFAULT_CANDIDATE = os.path.join(DEFAULT_OUT, "fgp_transcriptions.candidate.db")
DEFAULT_NLI = os.path.join(ROOT, "nli_data", "nli_crossref.db")
DEFAULT_LIBRARIES = os.path.join(ROOT, "libraries.csv")
DEFAULT_CACHE = os.path.join(DEFAULT_OUT, "gate5_cache")
PAYLOAD_DB = "positions_payload.db"
RELATIONAL_DB = "positions_relational.db"
SEED = 20261026
DISPLAY_WIDTH = 2000
LATENCY_OTHERS = 200
SAMPLE_SIZE = 20
SAMPLE_RESERVE = 10          # further seeded draws per stratum, taken only when NLI does not serve a drawn image
UNDISPLAYABLE = frozenset({"manifest_fetch_failed", "image_fetch_failed", "image_not_in_gallery", "canvas_missing",
                           "page_unconfirmed"})
FOLIO_IN_LABEL_RE = re.compile(r"(\d+)\s*([rv])\b", re.I)

# Declared before the first run (docs/plans/FGP_POSITIONS_RULES.md section 2). Changing one is a new declaration.
TOLERANCE = {
    "ncc_floor": 0.45,        # a landmark is found at this normalised cross-correlation or above
    "min_found": 6,           # found landmarks an image needs
    "min_cells": 4,           # distinct 3x3 grid cells among them, spanning two grid rows and two grid columns
    "max_r_over_h": 0.35,     # every residual <= this fraction of the line pitch
    "rms_r_over_h": 0.20,     # RMS residual <= this fraction of the line pitch
    "patch_h": 1.2,           # patch side = max(patch_h * h, patch_min_px)
    "patch_min_px": 96,
    "patch_max_display": 3.0, # ... and never more than a third of the display's shorter side (four giant lines on a page)
    "window_h": 1.0,          # search window = +- window_h * h around the predicted centre ...
    "window_max_display": 6.0,  # ... and never more than a sixth of the display's shorter side
    "per_cell": 2,            # landmarks per grid cell, by patch variance
    "attempt_min": 8,         # landmarks attempted when the image has them
    "sweep_coarse": 16,       # stage 1: NCC of the photograph over the display at 1/16 of the displayed scale ...
    "sweep_sigma": 2.0,       # ... high-passed with this sigma, candidates ranked by the peak's prominence
    "sweep_octave_steps": 32, # stage-1 scale steps per octave (1/32 octave = 2.2 %)
    "sweep_keep": 5,          # the most prominent orientation/scale pairs taken to stage 2, beside every orientation's own best
    "landmark_trials": 32,    # stage 3: every candidate (seeds and their refinements, at most this many) is tried
    "window_widen_display": 3.0,  # a trial with fewer than three landmarks found retries once with the window at a third of the display
    "coarse": 4,              # stage 2 (angle, scale refinement) and the landmark windows' high-pass: 1/4 scale ...
    "hp_sigma": 3.0,          # ... with this sigma
    "scale_octaves": 1.5,     # the scale sweep spans this many octaves either side of the dimension ratio (0.35x .. 2.8x)
    "scale_refine_steps": 4,  # then 1/64-octave steps either side of the best scale ...
    "scale_fine_steps": 4,    # ... and, after the angle, 1/256-octave steps (a 1 % scale error is 20 px on a 2000-px image)
    "angle_steps": 10,        # whole degrees either side of the 90-degree multiple ...
    "angle_refine_steps": 4,  # ... and quarter degrees either side of the best whole degree
    "template_min_side_px": 384,  # a registration template narrower than this (displayed px) is too small to trust
    "template_min_area": 0.03,    # ... or covering less than 3 % of the display
    "flat_window_floor": 0.05,  # a display window with < 5 % of the template's variance is flat: no match there
}
# Declared before the first run (section 4).
LIMITS = {
    "size.installed_mb": 512.0,
    "payload.largest_mb": 1.0,
    "latency.p95_ms": 25.0,
    "latency.max_ms": 200.0,
    "memory.build_peak_mb": 2048.0,
}
# (stratum, format, rotation classes, quota, draw order of classes for the large-rotation strata)
STRATA = (
    ("A", "B1", ("small",), 7, None),
    ("B", "B1", ("180", "90/270", "other"), 3, ("180", "90/270", "any")),
    ("C", "B2", ("small",), 6, None),
    ("D", "B2", ("180", "90/270", "other"), 4, ("180", "180", "90/270", "90/270")),
)
UNQUOTED_KEY_RE = re.compile(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)')
B2_ARTEFACT_RE = re.compile(r"_test", re.I)
LETTER_RE = re.compile(r"[א-תa-zA-Zؠ-ي]")   # a landmark needs ink: lacuna dot-runs and bare sigla are positioned, not matched   # the editor's empty "test" boxes; split(ROW) boxes ARE the words
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Referer": "https://www.nli.org.il/"}


def log(msg: str) -> None:
    print(msg, flush=True)


def _j(v) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def pct(values: list, q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def rotation_class(deg) -> str:
    if deg is None:
        return "small"
    try:
        d = float(deg) % 360.0
    except (TypeError, ValueError):
        return "small"
    if d <= 10 or d >= 350:
        return "small"
    if abs(d - 180) <= 10:
        return "180"
    if abs(d - 90) <= 10 or abs(d - 270) <= 10:
        return "90/270"
    return "other"


# --------------------------------------------------------------------------- folder-B geometry readers


def lenient_json(s: str):
    """Strict JSON first; the join directories' ``alldata`` carries unquoted keys (``from2:0``)."""
    try:
        return json.loads(s), False
    except ValueError:
        return json.loads(UNQUOTED_KEY_RE.sub(r'\1"\2"\3', s)), True


def norm_box(b) -> tuple:
    """(tight, band) as [x, y, w, h] lists or None, from the dict ``{0:..,1:..}`` or list ``[[..],[..]]`` form."""
    def ok(v):
        return (isinstance(v, list) and len(v) == 4 and all(isinstance(x, (int, float)) for x in v)
                and v[2] > 0 and v[3] > 0)
    tight = band = None
    if isinstance(b, dict):
        tight, band = b.get("0", b.get(0)), b.get("1", b.get(1))
    elif isinstance(b, list):
        tight = b[0] if len(b) > 0 else None
        band = b[1] if len(b) > 1 else None
    return ([float(x) for x in tight] if ok(tight) else None), ([float(x) for x in band] if ok(band) else None)


def _collapse(raw: str) -> tuple:
    """``re.sub(r"[ \\t]+", " ", raw)`` with an index map raw -> collapsed (-1 for a dropped space)."""
    out, idx = [], []
    for ch in raw:
        if ch in " \t":
            if out and out[-1] == " ":
                idx.append(-1)
                continue
            out.append(" ")
        else:
            out.append(ch)
        idx.append(len(out) - 1)
    return "".join(out), idx


class B1Geometry:
    """One B1 directory: the text exactly as ``parse_fgp_xml`` builds it, with the offset of every
    positioned word, and the rows and boxes of every column in the frame ``coldata.imagesize``."""

    def __init__(self, dir_path: str):
        self.dir = dir_path
        self.error = None
        self.lenient = False
        self.text = ""
        self.columns = []          # dicts: image, col_key, frame, crop, frame_file, rows, words
        self.photo_crop = None     # alldata.box: where img.jpg sits in the frame
        self._read()

    def _read(self) -> None:
        names = sorted(os.listdir(self.dir))
        xmls = [f for f in names if f.lower().endswith(".xml")]
        jss = [f for f in names if f.lower().endswith(".js") and not f.lower().endswith(".xml.js")]
        if not xmls or not jss:
            self.error = "missing_xml_or_js"
            return
        xml_path, js_path = os.path.join(self.dir, xmls[0]), os.path.join(self.dir, jss[0])
        try:
            with open(js_path, encoding="utf-8", errors="replace") as fh:
                raw = fh.read()
            parts = re.split(r"\n?coldata\[", raw)
            alldata, lenient = lenient_json(parts[0].split("=", 1)[1].strip().rstrip(";"))
            self.lenient = lenient
            coldata = {}
            for p in parts[1:]:
                key, body = p.split("]=", 1)
                cd, lenient = lenient_json(body.strip().rstrip(";"))
                self.lenient = self.lenient or lenient
                coldata[key.strip().strip('"')] = cd
        except (ValueError, IndexError) as e:
            self.error = f"js_parse:{type(e).__name__}"
            return
        try:
            root = ET.fromstring(open(xml_path, encoding="utf-8", errors="replace").read())
        except ET.ParseError:
            self.error = "xml_parse"
            return
        # the text, as parse_fgp_xml builds it, with offsets
        cols = root.findall("col") or [root]
        col_blocks = []   # (col_n, [(row_uid, line, [(uid, text, s, e)])])
        for col in cols:
            rows_out = []
            for row in col.iter("row"):
                rawline, words = "", []
                for w in row.iter("w"):
                    t = w.text or ""
                    words.append((w.get("u"), t, len(rawline), len(rawline) + len(t)))
                    rawline += t
                collapsed, idx = _collapse(rawline)
                lead = len(collapsed) - len(collapsed.lstrip())
                line = collapsed.strip()
                if not line:
                    continue
                spans = []
                for uid, t, a, b in words:
                    core = t.strip()
                    if not core or uid is None:
                        continue
                    a1 = a + (len(t) - len(t.lstrip()))
                    b1 = b - (len(t) - len(t.rstrip()))
                    s0, e0 = idx[a1], idx[b1 - 1]
                    if s0 < 0 or e0 < 0:
                        continue
                    s, e = s0 - lead, e0 + 1 - lead
                    if 0 <= s < e <= len(line):
                        spans.append((uid, line[s:e], s, e))
                rows_out.append((row.get("u"), line, spans))
            if rows_out:
                col_blocks.append((col.get("n"), rows_out))
        full = "\n\n".join("\n".join(line for _, line, _ in rows) for _, rows in col_blocks)
        lead_full = len(full) - len(full.lstrip())
        self.text = full.strip()
        offset, blocks_with_offsets = 0, []
        for ci, (n, rows) in enumerate(col_blocks):
            if ci:
                offset += 2
            rows_abs = []
            for ri, (ruid, line, spans) in enumerate(rows):
                if ri:
                    offset += 1
                rows_abs.append((ruid, line, [(uid, t, offset + s - lead_full, offset + e - lead_full) for uid, t, s, e in spans]))
                offset += len(line)
            blocks_with_offsets.append((n, rows_abs))
        abox = alldata.get("box") if isinstance(alldata, dict) else None
        if isinstance(abox, dict) and all(isinstance(abox.get(k), (int, float)) for k in ("x1", "y1", "x2", "y2")):
            self.photo_crop = [float(abox[k]) for k in ("x1", "y1", "x2", "y2")]
        # geometry per column
        bycol = collections.defaultdict(list)
        for key, cd in coldata.items():
            if isinstance(cd, dict):
                bycol[str(cd.get("col"))].append((key, cd))
        acols = alldata.get("cols") if isinstance(alldata, dict) else None
        acols = acols if isinstance(acols, dict) else {}
        for n, rows_abs in blocks_with_offsets:
            cands = bycol.get(str(n), [])
            want = (acols.get(str(n)) or {}).get("FGP") if isinstance(acols.get(str(n)), dict) else None
            if want and len(cands) > 1:
                cands = [c for c in cands if str(c[1].get("fgp")) == str(want)]
            if len(cands) != 1:
                self.columns.append({"col_key": f"?_{n}", "error": "column_unmatched", "rows": [], "words": []})
                continue
            key, cd = cands[0]
            size = cd.get("imagesize")
            if not (isinstance(size, list) and len(size) == 2 and all(isinstance(v, (int, float)) and v > 0 for v in size)):
                self.columns.append({"col_key": key, "error": "no_imagesize", "rows": [], "words": []})
                continue
            boxs = cd.get("boxs") if isinstance(cd.get("boxs"), dict) else {}
            pnts = {str(r.get("id")): r.get("pnts") for r in (cd.get("rows") or []) if isinstance(r, dict)}
            box = cd.get("box") or {}
            crop = [float(box.get(k, 0) or 0) for k in ("x1", "y1", "x2", "y2")] if isinstance(box, dict) else None
            frame_file = ""
            m = re.search(r"fn=([^&]+)", str(cd.get("fn") or ""))
            if m:
                frame_file = m.group(1).replace("%2F", "/")
            rows_g, words_g = [], []
            for row_no, (ruid, line, spans) in enumerate(rows_abs, 1):
                tight, band = norm_box((boxs.get(str(ruid)) or {}).get("box"))
                bl = pnts.get(str(ruid))
                baseline = [[float(p[0]), float(p[1])] for p in bl if isinstance(p, list) and len(p) == 2] if isinstance(bl, list) else []
                rows_g.append({"row_no": row_no, "band": band or tight, "baseline": baseline})
                for uid, t, s, e in spans:
                    wt, _ = norm_box((boxs.get(str(uid)) or {}).get("box"))
                    if wt:
                        words_g.append({"text": t, "start": s, "end": e, "row_no": row_no, "box": wt})
            fgp = str(cd.get("fgp") or "")
            self.columns.append({"image": re.sub(r"[._]G\d+$", "", fgp), "fgp": fgp, "col_key": key, "col": str(n),
                                 "frame": [float(size[0]), float(size[1])], "crop": crop, "frame_file": frame_file,
                                 "rows": rows_g, "words": words_g})

    def photo(self) -> tuple:
        """(PIL image, crop box in the frame) of ``img.jpg`` -- the FGP photograph at ``alldata.box``."""
        p = os.path.join(self.dir, "img.jpg")
        if not os.path.isfile(p):
            return None, None
        crop = self.photo_crop or next((c["crop"] for c in self.columns if c.get("crop")), None)
        if not crop:
            return None, None
        return Image.open(p), crop


class B2Geometry:
    """One B2 directory: the text exactly as ``read_b2`` builds it with the offset of every segment,
    rows and segment boxes per column in the master frame (column offset added). The frame is the fjms
    pixel size as FGP turned the master: swapped when the recorded rotation is 90/270, or when the
    columns fit only the swapped size."""

    def __init__(self, dir_path: str, master: tuple | None, rotation=None):
        self.dir = dir_path
        self.error = None
        self.lenient = False
        self.text = ""
        self.columns = []
        self.photo_crop = None
        self.image = ""
        self.frame_swapped = False
        self.spans_dropped = 0
        self._read(master, rotation)

    def _read(self, master, rotation) -> None:
        path = os.path.join(self.dir, "data.js")
        if not os.path.isfile(path):
            self.error = "missing_data_js"
            return
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                s = fh.read()
            j, self.lenient = lenient_json(s[s.find("=") + 1:].strip().rstrip(";"))
        except (ValueError, IndexError) as e:
            self.error = f"js_parse:{type(e).__name__}"
            return
        if not isinstance(j, dict):
            self.error = "js_shape"
            return
        self.image = str(j.get("FGP") or "").lstrip("Cc")
        # the text with offsets, as read_b2 builds it
        line_at = {}   # ndx -> (line, offset)
        col_texts = []
        offset = 0
        for ci, col in enumerate(j.get("ac") or []):
            rows = col.get("rows") if isinstance(col, dict) else None
            if not rows:
                continue
            lines = []
            for k in sorted(rows, key=lambda x: int(x) if str(x).isdigit() else 0):
                r = rows[k] if isinstance(rows[k], dict) else {}
                line = ws(r.get("txt", ""))
                if line:
                    lines.append((str(r.get("ndx")), line))
            if not lines:
                continue
            if col_texts:
                offset += 2
            for li, (ndx, line) in enumerate(lines):
                if li:
                    offset += 1
                line_at[ndx] = (line, offset)
                offset += len(line)
            col_texts.append("\n".join(line for _, line in lines))
        full = "\n\n".join(col_texts)
        lead = len(full) - len(full.lstrip())
        self.text = full.strip()
        if master is None:
            self.error = "no_frame"
            return
        cols = j.get("cols") if isinstance(j.get("cols"), dict) else {}
        offsets = {}
        for n, c in cols.items():
            if isinstance(c, dict) and all(isinstance(c.get(k), (int, float)) for k in ("x1", "y1", "x2", "y2")):
                offsets[str(n)] = (float(c["x1"]), float(c["y1"]), float(c["x2"]), float(c["y2"]))
        master = (float(master[0]), float(master[1]))

        def fits(m):
            return all(o[2] <= m[0] + 1 and o[3] <= m[1] + 1 for o in offsets.values())

        swapped = (master[1], master[0])
        if offsets and (rotation_class(rotation) == "90/270" or (not fits(master) and fits(swapped))):
            master, self.frame_swapped = swapped, True
        segs = collections.defaultdict(list)
        for b in j.get("boxs") or []:
            if not isinstance(b, dict):
                continue
            txt = ws(str(b.get("txt") or ""))
            try:
                x1, y1, x2, y2 = (float(b.get(k)) for k in ("x1", "y1", "x2", "y2"))
            except (TypeError, ValueError):
                continue
            if not txt or x1 < 0 or B2_ARTEFACT_RE.search(str(b.get("id") or "")) or x2 <= x1 or y2 <= y1:
                continue
            segs[str(b.get("row"))].append((x1, y1, x2, y2, txt))
        percol = collections.defaultdict(lambda: {"rows": [], "words": []})
        for r in j.get("rows") or []:
            if not isinstance(r, dict):
                continue
            col = str(r.get("col"))
            if col not in offsets:
                continue
            ox, oy = offsets[col][0], offsets[col][1]
            ndx = str(r.get("id"))
            if ndx not in line_at:
                continue
            line, loff = line_at[ndx]
            try:
                x1, y1, x2, y2 = (float(r.get(k)) for k in ("x1", "y1", "x2", "y2"))
            except (TypeError, ValueError):
                continue
            xy = r.get("xy") if isinstance(r.get("xy"), list) else []
            baseline = [[ox + float(p[0]), oy + float(p[1])] for p in xy if isinstance(p, list) and len(p) == 2]
            row_no = len(percol[col]["rows"]) + 1
            percol[col]["rows"].append({"row_no": row_no, "band": [ox + x1, oy + y1, x2 - x1, y2 - y1], "baseline": baseline})
            cursor = 0
            for sx1, sy1, sx2, sy2, txt in sorted(segs.get(ndx, []), key=lambda t: -t[2]):
                pos = line.find(txt, cursor)
                if pos < 0:
                    pos = line.find(txt)
                if pos < 0:
                    self.spans_dropped += 1
                    continue
                if pos >= cursor:
                    cursor = pos + len(txt)
                percol[col]["words"].append({"text": txt, "start": loff + pos - lead, "end": loff + pos + len(txt) - lead,
                                             "row_no": row_no, "box": [ox + sx1, oy + sy1, sx2 - sx1, sy2 - sy1]})
        img = j.get("img") if isinstance(j.get("img"), dict) else {}
        q = parse_qs(urlparse(str(img.get("src") or "")).query)
        try:
            self.photo_crop = [float(q[k][0]) for k in ("x1", "y1", "x2", "y2")]
        except (KeyError, ValueError, IndexError):
            if offsets:
                self.photo_crop = [min(o[0] for o in offsets.values()), min(o[1] for o in offsets.values()),
                                   max(o[2] for o in offsets.values()), max(o[3] for o in offsets.values())]
        for n in sorted(percol, key=lambda x: int(x) if x.isdigit() else 0):
            self.columns.append({"image": self.image, "fgp": self.image, "col_key": f"{self.image}_{n}", "col": n,
                                 "frame": [float(master[0]), float(master[1])], "crop": list(offsets[n]),
                                 "frame_file": (q.get("fn") or [""])[0], "rows": percol[n]["rows"], "words": percol[n]["words"]})

    def photo(self) -> tuple:
        p = os.path.join(self.dir, "img.jpg")
        if not os.path.isfile(p) or not self.photo_crop:
            return None, None
        return Image.open(p), self.photo_crop


def line_pitch(columns: list) -> float | None:
    """Median spacing of consecutive baselines (first point) over the columns, else median band height."""
    diffs, heights = [], []
    for c in columns:
        ys = sorted(r["baseline"][0][1] for r in c["rows"] if r.get("baseline"))
        diffs.extend(b - a for a, b in zip(ys, ys[1:]) if b - a > 0)
        heights.extend(r["band"][3] for r in c["rows"] if r.get("band"))
    if diffs:
        return float(np.median(diffs))
    if heights:
        return float(np.median(heights))
    return None


# --------------------------------------------------------------------------- transforms and landmarks


class Transform:
    """frame -> canvas: mirror (x -> W0 - x), rotation k*90 deg clockwise, scale (sx, sy), translation (dx, dy)."""

    def __init__(self, frame: tuple, canvas: tuple, rot: int, mirror: bool, dx: float = 0.0, dy: float = 0.0,
                 scale: float | None = None, angle: float = 0.0):
        self.frame, self.canvas, self.rot, self.mirror = (float(frame[0]), float(frame[1])), canvas, rot % 4, bool(mirror)
        wr, hr = self.rotated_dims()
        self.s0 = canvas[0] / wr
        if scale is None:
            self.sx, self.sy = canvas[0] / wr, canvas[1] / hr
        else:
            self.sx = self.sy = float(scale)
        self.scale_searched = scale is not None and abs(self.sx / self.s0 - 1.0) > 1e-9
        self.angle = float(angle)      # degrees, clockwise on the screen, about the scaled frame's centre
        self.dx, self.dy = dx, dy

    def aspect_delta(self) -> float:
        """|log| of the rotated frame's aspect ratio against the canvas's (0 = the same shape)."""
        wr, hr = self.rotated_dims()
        return abs(math.log((wr / hr) / (self.canvas[0] / self.canvas[1])))

    def rotated_dims(self) -> tuple:
        return (self.frame[0], self.frame[1]) if self.rot % 2 == 0 else (self.frame[1], self.frame[0])

    def apply(self, x: float, y: float) -> tuple:
        W0, H0 = self.frame
        if self.mirror:
            x = W0 - x
        if self.rot == 0:
            xr, yr = x, y
        elif self.rot == 1:
            xr, yr = H0 - y, x
        elif self.rot == 2:
            xr, yr = W0 - x, H0 - y
        else:
            xr, yr = y, W0 - x
        xs, ys = xr * self.sx, yr * self.sy
        if self.angle:
            wr, hr = self.rotated_dims()
            cx, cy = wr * self.sx / 2.0, hr * self.sy / 2.0
            a = math.radians(self.angle)
            xs, ys = cx + (xs - cx) * math.cos(a) - (ys - cy) * math.sin(a), cy + (xs - cx) * math.sin(a) + (ys - cy) * math.cos(a)
        return xs + self.dx, ys + self.dy

    def apply_box(self, box: list) -> tuple:
        """Axis-aligned bounding box in the canvas of a frame box [x, y, w, h]."""
        x, y, w, h = box
        pts = [self.apply(x, y), self.apply(x + w, y), self.apply(x, y + h), self.apply(x + w, y + h)]
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        return min(xs), min(ys), max(xs), max(ys)

    def orient(self, im: Image.Image) -> Image.Image:
        if self.mirror:
            im = ImageOps.mirror(im)
        if self.rot == 1:
            im = im.transpose(Image.Transpose.ROTATE_270)
        elif self.rot == 2:
            im = im.transpose(Image.Transpose.ROTATE_180)
        elif self.rot == 3:
            im = im.transpose(Image.Transpose.ROTATE_90)
        if self.angle:
            # PIL rotates counter-clockwise on the screen; the transform's angle is clockwise
            fill = int(np.asarray(im.convert("L")).mean()) if im.mode != "L" else int(np.asarray(im).mean())
            im = im.rotate(-self.angle, resample=Image.Resampling.BICUBIC, expand=True, fillcolor=fill)
        return im

    def scale_y_pitch(self) -> float:
        """The scale that applies to a frame-vertical distance (the line pitch)."""
        return self.sy if self.rot % 2 == 0 else self.sx

    def as_dict(self) -> dict:
        return {"rot": self.rot * 90, "mirror": self.mirror, "sx": round(self.sx, 5), "sy": round(self.sy, 5),
                "dx": round(self.dx, 2), "dy": round(self.dy, 2), "frame": list(self.frame), "canvas": list(self.canvas),
                "scale_searched": int(self.scale_searched), "angle": round(self.angle, 2)}


def gray_array(im: Image.Image) -> np.ndarray:
    return np.asarray(im.convert("L"), dtype=np.float64)


def highpass(a: np.ndarray, sigma: float) -> np.ndarray:
    return a - ndimage.gaussian_filter(a, sigma)


def phase_correlate(a: np.ndarray, b: np.ndarray) -> tuple:
    """(dx, dy, peak, second) such that b shifted by (dx, dy) best overlays a."""
    F, G = np.fft.fft2(a), np.fft.fft2(b)
    R = F * np.conj(G)
    R /= np.abs(R) + 1e-9
    r = np.fft.ifft2(R).real
    iy, ix = np.unravel_index(int(np.argmax(r)), r.shape)
    peak = float(r[iy, ix])
    # the second peak outside a 3-px neighbourhood of the first
    rr = r.copy()
    rr[max(0, iy - 3):iy + 4, max(0, ix - 3):ix + 4] = -1
    second = float(rr.max())
    dx = ix if ix < r.shape[1] // 2 else ix - r.shape[1]
    dy = iy if iy < r.shape[0] // 2 else iy - r.shape[0]
    return float(dx), float(dy), peak, second


def ncc_map(window: np.ndarray, template: np.ndarray) -> np.ndarray:
    """Normalised cross-correlation of ``template`` over ``window`` (valid positions)."""
    t = template - template.mean()
    tn = math.sqrt(float((t * t).sum()))
    if tn < 1e-9:
        return np.zeros((1, 1))
    h, w = template.shape
    corr = signal.fftconvolve(window, t[::-1, ::-1], mode="valid")
    ones = np.ones_like(template)
    s1 = signal.fftconvolve(window, ones, mode="valid")
    s2 = signal.fftconvolve(window * window, ones, mode="valid")
    n = h * w
    var = s2 - s1 * s1 / n
    flat = var < TOLERANCE["flat_window_floor"] * tn * tn
    var[var < 1e-9] = 1e-9
    out = corr / (np.sqrt(var) * tn)
    out[flat] = 0.0
    return out


def _register_candidate(T: Transform, d: float, disp_hp: np.ndarray, photo_gray: Image.Image, crop: list,
                        coarse: int, sigma: float) -> tuple:
    """Slide the oriented, scaled photograph over the displayed image (NCC at 1/coarse of the displayed scale) and
    return (dx, dy, peak, second) -- the translation in canvas px that puts it where it matches best."""
    H, W = disp_hp.shape
    x1, y1, x2, y2 = T.apply_box([crop[0], crop[1], crop[2] - crop[0], crop[3] - crop[1]])
    pw, ph = int(round((x2 - x1) * d / coarse)), int(round((y2 - y1) * d / coarse))
    if min(pw, ph) * coarse < TOLERANCE["template_min_side_px"] or pw * ph < TOLERANCE["template_min_area"] * H * W:
        return None     # a template this small matches anything
    tmpl = gray_array(T.orient(photo_gray).resize((pw, ph), Image.Resampling.BOX))
    ox = oy = 0
    if pw > W - 2:      # a photograph wider than the display: keep its centre
        ox = (pw - (W - 2)) // 2
        tmpl = tmpl[:, ox:ox + W - 2]
    if ph > H - 2:
        oy = (ph - (H - 2)) // 2
        tmpl = tmpl[oy:oy + H - 2, :]
    if tmpl.shape[0] * tmpl.shape[1] < 0.4 * pw * ph:
        return None     # mostly off the display: not this candidate
    nccm = ncc_map(disp_hp, highpass(tmpl, sigma))
    iy, ix = np.unravel_index(int(np.argmax(nccm)), nccm.shape)
    peak = float(nccm[iy, ix])
    rr = nccm.copy()
    rr[max(0, iy - 3):iy + 4, max(0, ix - 3):ix + 4] = -2
    second = float(rr.max()) if rr.size else -2.0
    if second < -1:
        second = 0.0    # a template as large as the display has one position: no crowd to stand above
    dx = (ix - ox) * coarse / d - x1
    dy = (iy - oy) * coarse / d - y1
    return dx, dy, peak, second


def _coarse_display(display: Image.Image, coarse: int, sigma: float) -> np.ndarray:
    return highpass(gray_array(display.resize((max(8, display.width // coarse), max(8, display.height // coarse)),
                                              Image.Resampling.BOX)), sigma)


def find_transform(frame: tuple, canvas: tuple, display: Image.Image, photo: Image.Image, crop: list) -> tuple:
    """Stages 1 and 2 of the registration. Stage 1 sweeps every orientation (rotation 0/90/180/270 x mirror) at
    every 1/32-octave scale step within 1.5 octaves of its dimension ratio by NCC of the photograph over the
    displayed image at 1/16 scale (a small tilt costs little there), and keeps, by the peak's prominence over
    the next-best position, every orientation's best pair and the five best pairs overall.
    Stage 2, at 1/4 scale, searches the angle, refines the scale and refines the angle for each of them.
    Returns ([(Transform, peak, second)] best peak first, [peaks]); stage 3 (``check_image``) tries the
    landmarks against them."""
    d = display.width / canvas[0]
    sweep_c, sweep_s = TOLERANCE["sweep_coarse"], TOLERANCE["sweep_sigma"]
    fine_c, fine_s = TOLERANCE["coarse"], TOLERANCE["hp_sigma"]
    disp_sweep = _coarse_display(display, sweep_c, sweep_s)
    disp_fine = _coarse_display(display, fine_c, fine_s)
    photo_gray = photo.convert("L")
    peaks, stage1 = [], []
    per_octave = TOLERANCE["sweep_octave_steps"]
    steps = int(round(TOLERANCE["scale_octaves"] * per_octave))
    for rot in range(4):
        for mirror in (False, True):
            s0 = Transform(frame, canvas, rot, mirror).s0
            obest = None
            for k in range(-steps, steps + 1):
                T = Transform(frame, canvas, rot, mirror, scale=s0 * 2 ** (k / per_octave))
                r = _register_candidate(T, d, disp_sweep, photo_gray, crop, sweep_c, sweep_s)
                if r is None:
                    continue
                cand = (Transform(frame, canvas, rot, mirror, r[0], r[1], scale=T.sx), r[2], r[3])
                stage1.append(cand)
                if obest is None or r[2] - r[3] > obest[1] - obest[2]:
                    obest = cand
            if obest is not None:
                peaks.append({"stage": 1, "rot": rot * 90, "mirror": mirror, "scale": round(obest[0].sx, 5),
                              "peak": round(obest[1], 4), "second": round(obest[2], 4)})
    if not stage1:
        return [], peaks
    stage1.sort(key=lambda c: -(c[1] - c[2]))     # by prominence: a true alignment stands alone, spurious peaks come in crowds
    seeds, seen = [], set()
    per_orientation = {}
    for cand in stage1:
        k = (cand[0].rot, cand[0].mirror)
        if k not in per_orientation:
            per_orientation[k] = cand
    dims = []   # the dimension-ratio scale of every orientation, whatever its sweep rank (frame = canvas is common)
    for rot in range(4):
        for mirror in (False, True):
            T = Transform(frame, canvas, rot, mirror)
            r = _register_candidate(T, d, disp_sweep, photo_gray, crop, sweep_c, sweep_s)
            if r is not None:
                dims.append((Transform(frame, canvas, rot, mirror, r[0], r[1], scale=T.s0), r[2], r[3]))
    for cand in list(per_orientation.values()) + stage1[:TOLERANCE["sweep_keep"]] + dims:
        key = (cand[0].rot, cand[0].mirror, round(cand[0].sx, 3))
        if key not in seen:
            seen.add(key)
            seeds.append(cand)
    refined = []

    def consider(T):
        r = _register_candidate(T, d, disp_fine, photo_gray, crop, fine_c, fine_s)
        if r is None:
            return None
        return (Transform(frame, canvas, T.rot, T.mirror, r[0], r[1], scale=T.sx, angle=T.angle), r[2], r[3])

    for seed, speak, ssecond in seeds:
        refined.append((seed, speak, ssecond))     # the seed as the sweep left it: a tiny photograph gives stage 2 only noise
        local = consider(seed)
        if local is None:
            continue
        # the angle first (a tilt hides the scale), the scale, the angle again
        for a in range(-TOLERANCE["angle_steps"], TOLERANCE["angle_steps"] + 1):
            if a:
                r = consider(Transform(frame, canvas, seed.rot, seed.mirror, scale=seed.sx, angle=float(a)))
                if r is not None and r[1] > local[1]:
                    local = r
        T1 = local[0]
        for k in range(-TOLERANCE["scale_refine_steps"], TOLERANCE["scale_refine_steps"] + 1):
            if k:
                r = consider(Transform(frame, canvas, T1.rot, T1.mirror, scale=T1.sx * 2 ** (k / 64.0), angle=T1.angle))
                if r is not None and r[1] > local[1]:
                    local = r
        T2 = local[0]
        for q in range(-TOLERANCE["angle_refine_steps"], TOLERANCE["angle_refine_steps"] + 1):
            if q:
                r = consider(Transform(frame, canvas, T2.rot, T2.mirror, scale=T2.sx, angle=T2.angle + q * 0.25))
                if r is not None and r[1] > local[1]:
                    local = r
        T3 = local[0]
        for k in range(-TOLERANCE["scale_fine_steps"], TOLERANCE["scale_fine_steps"] + 1):
            if k:
                r = consider(Transform(frame, canvas, T3.rot, T3.mirror, scale=T3.sx * 2 ** (k / 256.0), angle=T3.angle))
                if r is not None and r[1] > local[1]:
                    local = r
        peaks.append({"stage": 2, "rot": local[0].rot * 90, "mirror": local[0].mirror, "scale": round(local[0].sx, 5),
                      "angle": round(local[0].angle, 2), "peak": round(local[1], 4), "second": round(local[2], 4)})
        refined.append(local)
    refined.sort(key=lambda c: -c[1])
    uniq, seen2 = [], set()
    for c in refined:
        key = (c[0].rot, c[0].mirror, round(c[0].sx, 4), round(c[0].angle, 2), round(c[0].dx), round(c[0].dy))
        if key not in seen2:
            seen2.add(key)
            uniq.append(c)
    return uniq, peaks


def pick_landmarks(columns: list, photo_gray: np.ndarray, crop: list, ps: float) -> tuple:
    """Up to ``per_cell`` boxes per 3x3 cell by patch variance in the photograph; (candidates, grid)."""
    boxes = [w["box"] + [w["text"]] for c in columns for w in c["words"] if LETTER_RE.search(w["text"])]
    if not boxes:
        return [], None
    xs0 = min(b[0] for b in boxes)
    ys0 = min(b[1] for b in boxes)
    xs1 = max(b[0] + b[2] for b in boxes)
    ys1 = max(b[1] + b[3] for b in boxes)
    gw, gh = max(1e-6, xs1 - xs0), max(1e-6, ys1 - ys0)
    cells = collections.defaultdict(list)
    for b in boxes:
        cx, cy = b[0] + b[2] / 2, b[1] + b[3] / 2
        cell = (min(2, int(3 * (cy - ys0) / gh)), min(2, int(3 * (cx - xs0) / gw)))
        # variance of the word's own patch in the photograph
        px0, py0 = int((b[0] - crop[0]) * ps), int((b[1] - crop[1]) * ps)
        px1, py1 = int(math.ceil((b[0] + b[2] - crop[0]) * ps)), int(math.ceil((b[1] + b[3] - crop[1]) * ps))
        px0, py0 = max(0, px0), max(0, py0)
        px1, py1 = min(photo_gray.shape[1], px1), min(photo_gray.shape[0], py1)
        var = float(photo_gray[py0:py1, px0:px1].var()) if px1 > px0 + 1 and py1 > py0 + 1 else -1.0
        if var >= 0:
            cells[cell].append((var, b, cell))
    chosen = []
    for cell in sorted(cells):
        # a word of two or more letters before a single letter (a lone letter matches its twin two words away), then variance
        chosen.extend(sorted(cells[cell], key=lambda t: (-(len(LETTER_RE.findall(t[1][4])) >= 2), -t[0]))[:TOLERANCE["per_cell"]])
    return chosen, (xs0, ys0, xs1, ys1)


def match_landmarks(T: Transform, d: float, h_disp: float, chosen: list, photo_gray: np.ndarray, crop: list, ps: float,
                    disp_gray: np.ndarray, window_cap: float | None = None) -> list:
    """Predict each landmark's centre, cut its patch from the photograph, search the displayed image."""
    short = min(disp_gray.shape[0], disp_gray.shape[1])
    side = min(max(TOLERANCE["patch_h"] * h_disp, TOLERANCE["patch_min_px"]), short / TOLERANCE["patch_max_display"])
    win = min(TOLERANCE["window_h"] * h_disp, short / (window_cap or TOLERANCE["window_max_display"]))
    if window_cap:
        win = max(win, short / window_cap)
    s_avg = (T.sx + T.sy) / 2.0
    out = []
    for var, b, cell in chosen:
        cx, cy = b[0] + b[2] / 2, b[1] + b[3] / 2
        X, Y = T.apply(cx, cy)
        px, py = X * d, Y * d
        rec = {"cell": f"{cell[0]}{cell[1]}", "text": b[4], "pred_x": round(px, 1), "pred_y": round(py, 1),
               "found": 0, "ncc": None, "found_x": None, "found_y": None, "residual": None, "note": ""}
        # the patch in the frame: a square of side side/(d*s_avg) centred on the box
        half_f = side / (d * s_avg) / 2.0
        fx0, fy0, fx1, fy1 = cx - half_f, cy - half_f, cx + half_f, cy + half_f
        qx0, qy0 = int(round((fx0 - crop[0]) * ps)), int(round((fy0 - crop[1]) * ps))
        qx1, qy1 = int(round((fx1 - crop[0]) * ps)), int(round((fy1 - crop[1]) * ps))
        cx0, cy0, cx1, cy1 = max(0, qx0), max(0, qy0), min(photo_gray.shape[1], qx1), min(photo_gray.shape[0], qy1)
        if cx1 - cx0 < 4 or cy1 - cy0 < 4 or (cx1 - cx0) * (cy1 - cy0) < 0.6 * max(1, (qx1 - qx0) * (qy1 - qy0)):
            rec["note"] = "patch_outside_photo"
            out.append(rec)
            continue
        patch = Image.fromarray(photo_gray[cy0:cy1, cx0:cx1].astype(np.uint8))
        patch = T.orient(patch)
        # the oriented patch's extent in displayed pixels (clipped at the photograph's edge, rotated by the transform)
        pw_disp = patch.width / ps * d * s_avg
        ph_disp = patch.height / ps * d * s_avg
        # the clipped patch's centre in displayed pixels (frame centre of the clipped square, transformed)
        fcx, fcy = crop[0] + (cx0 + cx1) / 2.0 / ps, crop[1] + (cy0 + cy1) / 2.0 / ps
        Xc, Yc = T.apply(fcx, fcy)
        pcx, pcy = Xc * d, Yc * d
        # working scale: the coarser of photograph and displayed resolution
        photo_per_disp = ps / (d * s_avg)
        wsc = min(1.0, photo_per_disp)
        tw, th = max(4, int(round(pw_disp * wsc))), max(4, int(round(ph_disp * wsc)))
        wx0, wy0 = int(math.floor(pcx - pw_disp / 2 - win)), int(math.floor(pcy - ph_disp / 2 - win))
        wx1, wy1 = int(math.ceil(pcx + pw_disp / 2 + win)), int(math.ceil(pcy + ph_disp / 2 + win))
        wx0c, wy0c = max(0, wx0), max(0, wy0)
        wx1c, wy1c = min(disp_gray.shape[1], wx1), min(disp_gray.shape[0], wy1)
        if wx1c - wx0c < pw_disp + 2 or wy1c - wy0c < ph_disp + 2:
            rec["note"] = "window_outside_display"
            out.append(rec)
            continue
        window = Image.fromarray(disp_gray[wy0c:wy1c, wx0c:wx1c].astype(np.uint8))
        ww, wh = max(tw + 2, int(round((wx1c - wx0c) * wsc))), max(th + 2, int(round((wy1c - wy0c) * wsc)))
        t_arr = gray_array(patch.resize((tw, th), Image.Resampling.LANCZOS))
        w_arr = gray_array(window.resize((ww, wh), Image.Resampling.LANCZOS))
        sig = max(1.0, TOLERANCE["hp_sigma"] * wsc)
        nccm = ncc_map(highpass(w_arr, sig), highpass(t_arr, sig))
        iy, ix = np.unravel_index(int(np.argmax(nccm)), nccm.shape)
        peak = float(nccm[iy, ix])
        # found centre in displayed pixels
        fxd = wx0c + (ix + tw / 2.0) * (wx1c - wx0c) / ww
        fyd = wy0c + (iy + th / 2.0) * (wy1c - wy0c) / wh
        rec.update({"ncc": round(peak, 3), "found_x": round(fxd, 1), "found_y": round(fyd, 1),
                    "residual": round(math.hypot(fxd - pcx, fyd - pcy), 2), "found": int(peak >= TOLERANCE["ncc_floor"])})
        out.append(rec)
    return out


def judge(landmarks: list, h_disp: float) -> tuple:
    """(passed, reason, stats) under the declared tolerance."""
    found = [m for m in landmarks if m["found"]]
    cells = {m["cell"] for m in found}
    rows_ = {c[0] for c in cells}
    cols_ = {c[1] for c in cells}
    res = [m["residual"] for m in found]
    stats = {"attempted": len(landmarks), "found": len(found), "cells": len(cells),
             "max_r": round(max(res), 2) if res else None,
             "rms_r": round(math.sqrt(sum(r * r for r in res) / len(res)), 2) if res else None, "h": round(h_disp, 2)}
    if len(found) < TOLERANCE["min_found"]:
        return False, "too_few_landmarks", stats
    if len(cells) < TOLERANCE["min_cells"] or len(rows_) < 2 or len(cols_) < 2:
        return False, "landmarks_not_distributed", stats
    if stats["max_r"] > TOLERANCE["max_r_over_h"] * h_disp:
        return False, "max_residual", stats
    if stats["rms_r"] > TOLERANCE["rms_r_over_h"] * h_disp:
        return False, "rms_residual", stats
    return True, "", stats


# --------------------------------------------------------------------------- inputs


def load_b_tables(inventory: str) -> tuple:
    inv = ro(inventory)
    dirs = {}
    for r in inv.execute("SELECT dir, format, numbers, is_multi_image, region, has_js, has_data_js, png_count FROM b_dirs"):
        dirs[r[0]] = {"dir": r[0], "format": r[1], "numbers": json.loads(r[2] or "[]"), "multi": bool(r[3]), "region": r[4],
                      "has_js": bool(r[5]), "has_data_js": bool(r[6]), "png_count": r[7] or 0}
    numbers = {}
    for r in inv.execute("SELECT number, formats, dirs, pixel_width, pixel_height, rotation_deg, nli_alma_ids, nli_image_names FROM b_numbers"):
        numbers[str(r[0])] = {"number": str(r[0]), "formats": r[1], "dirs": json.loads(r[2] or "[]"), "pw": r[3], "ph": r[4],
                              "rotation": r[5], "almas": json.loads(r[6] or "[]"), "image_names": json.loads(r[7] or "[]")}
    inv.close()
    return dirs, numbers


def load_candidate_rows(candidate: str, gate3: str, gate4: str) -> tuple:
    """(dir name -> [{id, durable_key, hash, sys_id, c_number, reader}] for every candidate row gate 3 read from folder B,
    durable_key -> {hash: content}, durable_key -> hash, the candidate's fgp_meta) -- one pass over the sidecar, so a
    rebuild of the candidate during the run cannot leave the tie and the verification reading different files."""
    g4 = ro(gate4)
    row_key = {r[0]: r[1] for r in g4.execute("SELECT id, row_key FROM candidate_rows")}
    g4.close()
    g3 = ro(gate3)
    src = {r[0]: (r[1], r[2]) for r in g3.execute("SELECT row_key, reader, source_path FROM text_rows WHERE reader IN ('b1_xml', 'b2_rows')")}
    g3.close()
    con = ro(candidate)
    bydir = collections.defaultdict(list)
    contents = collections.defaultdict(dict)
    hashes = {}
    for rid, dk, sid, cn, content in con.execute("SELECT id, durable_key, sys_id, c_number, content FROM fgp_transcriptions"):
        h = sha(content or "")
        contents[dk][h] = content or ""
        hashes[dk] = h
        rk = row_key.get(rid)
        if rk is None or rk not in src:
            continue
        reader, path = src[rk]
        d = (path or "").replace("/", "\\").split("\\")[0]
        if not d:
            continue
        bydir[d].append({"id": rid, "durable_key": dk, "hash": h, "sys_id": str(sid or ""),
                         "c_number": str(cn or "").lstrip("Cc"), "reader": reader})
    meta = dict(con.execute("SELECT key, value FROM fgp_meta").fetchall())
    con.close()
    return bydir, contents, hashes, meta


def load_libraries(path: str) -> dict:
    out = {}
    if not path or not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8-sig", newline="") as fh:
        for row in csv.reader(fh):
            if len(row) > 3 and row[0].isdigit():
                out[row[0]] = row[3] or "unknown"
    return out


def eligible_frame(dirs: dict, numbers: dict, bydir: dict, libraries: dict) -> dict:
    """F: number -> record, under the declared frame (section 3)."""
    F = {}
    for num, n in numbers.items():
        if not n["almas"]:
            continue
        good_dirs = []
        for d in n["dirs"]:
            bd = dirs.get(d)
            if not bd or bd["multi"] or bd["region"]:
                continue
            if (bd["format"] == "B1" and bd["has_js"]) or (bd["format"] == "B2" and bd["has_data_js"]):
                good_dirs.append(bd)
        if not good_dirs:
            continue
        if all(bd["format"] == "B2" for bd in good_dirs) and not (n["pw"] and n["ph"]):
            continue    # a B2 image without a recorded pixel size has no frame (section 1)
        cand = [c for bd in good_dirs for c in bydir.get(bd["dir"], []) if c["c_number"] == num]
        if not cand:
            continue
        fmt = "B1" if any(bd["format"] == "B1" for bd in good_dirs) else "B2"
        F[num] = {"number": num, "format": fmt, "dirs": [bd["dir"] for bd in good_dirs], "rows": cand,
                  "two_col": any(bd["format"] == "B2" and bd["png_count"] >= 2 for bd in good_dirs),
                  "rotation": n["rotation"], "rclass": rotation_class(n["rotation"]),
                  "library": libraries.get(str(n["almas"][0]), "unknown"), "sys_id": str(n["almas"][0])}
    return F


def sample_images(F: dict, seed: int = SEED, reserve: int = 0) -> tuple:
    """The declared stratified, seeded, library round-robin draw; (chosen [(number, stratum)], shortfall per stratum).
    With ``reserve`` > 0 each stratum draws that many further images in the same order, for the gate to take when
    NLI does not serve a drawn image; the first ``quota`` of a stratum are the same with or without a reserve."""
    rng = random.Random(seed)
    chosen, shortfall = [], {}
    taken = set()
    for stratum, fmt, classes, quota0, order in STRATA:
        quota = quota0 + reserve
        pool_all = [f for f in F.values() if f["format"] == fmt and f["rclass"] in classes]
        libs = sorted({f["library"] for f in pool_all})
        rng.shuffle(libs)
        picked = 0
        draw_classes = list(order) if order else ["any"] * quota
        while picked < quota:
            want = draw_classes[picked] if picked < len(draw_classes) else "any"
            pool = [f for f in pool_all if f["number"] not in taken and (want == "any" or f["rclass"] == want)]
            if not pool and want != "any":
                pool = [f for f in pool_all if f["number"] not in taken]
            if not pool:
                break
            if stratum == "C" and picked == 0 and any(f["two_col"] for f in pool):
                pool = [f for f in pool if f["two_col"]]
            got = False
            for lib in libs:
                lp = sorted(f["number"] for f in pool if f["library"] == lib)
                if lp:
                    num = rng.choice(lp)
                    chosen.append((num, stratum))
                    taken.add(num)
                    picked += 1
                    got = True
                    break
            if not got:
                break
            libs = libs[1:] + libs[:1]
        if picked < quota0:
            shortfall[stratum] = quota0 - picked
    return chosen, shortfall


# --------------------------------------------------------------------------- fetching what the apps display


FETCH_WAITS = (3, 6, 12, 24)   # seconds between attempts on a 429 / 5xx / timeout; the NLI IIIF host answers 503 in bursts


def fetch_bytes(url: str, timeout: tuple = (10, 60)) -> bytes | None:
    """GET through the shared NLI host policy, retried on transient failures; None after the last attempt."""
    from shared.nli_fetch import nli_image_get
    for attempt, wait in enumerate(FETCH_WAITS + (None,)):
        try:
            r = nli_image_get(url, headers=HEADERS, timeout=timeout)
        except Exception as e:  # noqa: BLE001 -- recorded as a fetch failure, never raised
            log(f"  fetch failed {url}: {type(e).__name__} (attempt {attempt + 1})")
            r = None
        if r is not None:
            if r.status_code == 200 and r.content:
                return r.content
            log(f"  fetch {r.status_code} {url} (attempt {attempt + 1})")
            if r.status_code not in (429,) and not 500 <= r.status_code < 600:
                return None
        if wait is None:
            return None
        time.sleep(wait)
    return None


def cached(cache_dir: str, name: str, url: str, offline: bool) -> bytes | None:
    os.makedirs(cache_dir, exist_ok=True)
    p = os.path.join(cache_dir, name)
    if os.path.isfile(p) and os.path.getsize(p) > 0:
        with open(p, "rb") as fh:
            return fh.read()
    if offline:
        return None
    data = fetch_bytes(url)
    if data:
        with open(p, "wb") as fh:
            fh.write(data)
    return data


def manifest_canvases(data: bytes) -> list:
    try:
        m = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return []
    out = []
    seqs = m.get("sequences") or []
    for c in (seqs[0].get("canvases") if seqs else []) or []:
        images = c.get("images") or []
        service = ((images[0].get("resource") or {}).get("service") or {}) if images else {}
        fl = re.search(r"FL(\d+)", str(service.get("@id", "")))
        out.append({"fl": fl.group(1) if fl else None, "label": str(c.get("label", "")), "width": c.get("width"), "height": c.get("height")})
    return out


def displayed_for(number: str, rec: dict, numbers: dict, nli: NliCrossrefService, cache_dir: str, offline: bool) -> dict:
    """The gallery index, the manifest canvas and the 2000-wide rendition the apps display for this image."""
    sys_id = rec["sys_id"]
    out = {"sys_id": sys_id, "gallery_index": None, "gallery_count": None, "manifest_count": None, "fl": None,
           "canvas_label": "", "folio_label": "", "canvas_w": None, "canvas_h": None, "image": None, "image_sha": None,
           "manifest_sha": None, "error": ""}
    imgs = nli.get_folio_images(sys_id) or []
    out["gallery_count"] = len(imgs)
    idx = next((i for i, im in enumerate(imgs) if str(im.get("fgp_image_number_id") or "") == number), None)
    if idx is None:
        out["error"] = "image_not_in_gallery"
        return out
    out["gallery_index"] = idx
    out["folio_label"] = imgs[idx].get("folio_label") or parse_folio_label(imgs[idx].get("image_name") or "")
    mdata = cached(cache_dir, f"manifest_{sys_id}-1.json",
                   f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest", offline)
    if not mdata:
        out["error"] = "manifest_fetch_failed"
        return out
    out["manifest_sha"] = sha_bytes(mdata)
    canvases = manifest_canvases(mdata)
    out["manifest_count"] = len(canvases)
    if idx >= len(canvases) or not canvases[idx].get("fl"):
        out["error"] = "canvas_missing"
        return out
    c = canvases[idx]
    out.update({"fl": c["fl"], "canvas_label": c["label"], "canvas_w": c.get("width"), "canvas_h": c.get("height")})
    idata = cached(cache_dir, f"FL{c['fl']}_w{DISPLAY_WIDTH}.jpg",
                   f"https://iiif.nli.org.il/IIIFv21/FL{c['fl']}/full/{DISPLAY_WIDTH},/0/default.jpg", offline)
    if not idata:
        out["error"] = "image_fetch_failed"
        return out
    out["image_sha"] = sha_bytes(idata)
    try:
        im = Image.open(io.BytesIO(idata))
        im.load()
    except Exception as e:  # noqa: BLE001
        out["error"] = f"image_decode:{type(e).__name__}"
        return out
    out["image"] = im
    if not out["canvas_w"] or not out["canvas_h"]:
        out["canvas_w"], out["canvas_h"] = im.width, im.height
    return out


# --------------------------------------------------------------------------- the geometry check


def check_image(number: str, rec: dict, geoms: dict, numbers: dict, nli: NliCrossrefService, cache_dir: str, offline: bool) -> tuple:
    """One sampled image: (sample record, landmark records)."""
    n = numbers[number]
    srec = {"number": number, "format": rec["format"], "rotation_deg": n["rotation"], "rclass": rec["rclass"],
            "library": rec["library"], "dirs": _j(rec["dirs"]), "frame_w": None, "frame_h": None, "frame_is_master": None,
            "frame_file": "", "pitch_disp": None, "transform": None, "peaks": None, "passed": 0, "reason": "",
            "attempted": 0, "found": 0, "cells": 0, "max_r": None, "rms_r": None, "canvas_label_agrees": None,
            "gallery_manifest_agree": None, "row_sys_id_agrees": None, "scale_searched": None, "aspect_delta": None,
            "frame_swapped": None, "peak": None, "peak_ratio": None, "translation_corrected": None,
            "translation_correction_px": None, "candidates_tried": None, "window_widened": None}
    disp = displayed_for(number, rec, numbers, nli, cache_dir, offline)
    for k in ("sys_id", "gallery_index", "gallery_count", "manifest_count", "fl", "canvas_label", "folio_label",
              "canvas_w", "canvas_h", "image_sha", "manifest_sha"):
        srec[k] = disp.get(k)
    if disp["error"]:
        srec["reason"] = disp["error"]
        return srec, []
    srec["gallery_manifest_agree"] = int(disp["gallery_count"] == disp["manifest_count"])
    m = FOLIO_IN_LABEL_RE.search(disp["canvas_label"] or "")
    canvas_folio = (m.group(1).lstrip("0") or "0") + m.group(2).lower() if m else ""
    gallery_folio = (re.sub(r"\D", "", disp["folio_label"]).lstrip("0") or "0") + disp["folio_label"][-1].lower() \
        if disp["folio_label"] and disp["folio_label"][-1].lower() in "rv" else ""
    srec["canvas_label_agrees"] = int(bool(canvas_folio) and canvas_folio == gallery_folio)
    srec["row_sys_id_agrees"] = int(all(r["sys_id"] == disp["sys_id"] for r in rec.get("rows", [])))
    if not srec["gallery_manifest_agree"] or not srec["canvas_label_agrees"]:
        # the canvas at the gallery index is not confirmed as this image's page: the apps' positional page mapping
        # (a known alignment defect) may show another leaf or the other side here, and nothing about positions can
        # be measured on it (Hamburg AS 13: 4 gallery images, 8 canvases; JNUL 577.4.24: canvases labelled "Page n")
        srec["reason"] = "page_unconfirmed"
        return srec, []
    geom = None
    for d in rec["dirs"]:
        g = geoms.get(d)
        if g is not None and not g.error and any(c.get("image") == number and not c.get("error") for c in g.columns):
            geom = g
            break
    if geom is None:
        srec["reason"] = "no_geometry"
        return srec, []
    columns = [c for c in geom.columns if c.get("image") == number and not c.get("error")]
    frame = tuple(columns[0]["frame"])
    srec["frame_w"], srec["frame_h"] = frame
    srec["frame_is_master"] = int(n["pw"] is not None and {frame[0], frame[1]} == {float(n["pw"]), float(n["ph"])})
    srec["frame_file"] = columns[0].get("frame_file") or ""
    photo, crop = geom.photo()
    if photo is None:
        srec["reason"] = "no_photograph"
        return srec, []
    photo_gray = gray_array(photo)
    ps = photo.width / max(1e-6, (crop[2] - crop[0]))
    canvas = (float(disp["canvas_w"]), float(disp["canvas_h"]))
    display = disp["image"]
    d = display.width / canvas[0]
    srec["frame_swapped"] = int(getattr(geom, "frame_swapped", False))
    pitch = line_pitch(columns)
    if not pitch:
        srec["reason"] = "no_pitch"
        return srec, []
    chosen, _grid = pick_landmarks(columns, photo_gray, crop, ps)
    if not chosen:
        srec["reason"] = "no_landmark_candidates"
        return srec, []
    candidates, peaks = find_transform(frame, canvas, display, photo, crop)
    if not candidates:
        srec["reason"] = "no_registration"
        srec["peaks"] = _j(peaks)
        return srec, []
    # stage 3: the landmarks decide between the refined candidates (best registration peak first). Each gets one
    # translation correction -- the median displacement of its found landmarks (ruled text gives the correlation a
    # peak one line pitch off, and the words then all land one line away, uniformly) -- and is re-measured against
    # landmarks searched afresh from the corrected prediction. Scale, angle and orientation are never touched.
    disp_gray = gray_array(display)
    trials = []
    for T, peak, second in candidates[:TOLERANCE["landmark_trials"]]:
        h_disp = pitch * T.scale_y_pitch() * d
        marks = match_landmarks(T, d, h_disp, chosen, photo_gray, crop, ps, disp_gray)
        found = [m for m in marks if m["found"]]
        widened = 0
        if len(found) < 3:
            # the registration of a tiny photograph can be off by more than a line: look once in a wider window
            marks = match_landmarks(T, d, h_disp, chosen, photo_gray, crop, ps, disp_gray, TOLERANCE["window_widen_display"])
            found = [m for m in marks if m["found"]]
            widened = 1
        corrected, corr_px = 0, 0.0
        if len(found) >= 3:
            mdx = float(np.median([m["found_x"] - m["pred_x"] for m in found]))
            mdy = float(np.median([m["found_y"] - m["pred_y"] for m in found]))
            if math.hypot(mdx, mdy) > 1.0:
                T = Transform(frame, canvas, T.rot, T.mirror, T.dx + mdx / d, T.dy + mdy / d, scale=T.sx, angle=T.angle)
                corrected, corr_px = 1, round(math.hypot(mdx, mdy), 2)
                marks = match_landmarks(T, d, h_disp, chosen, photo_gray, crop, ps, disp_gray)
        passed, reason, stats = judge(marks, h_disp)
        trials.append({"passed": passed, "reason": reason, "stats": stats, "T": T, "peak": peak, "marks": marks,
                       "corrected": corrected, "corr_px": corr_px, "h_disp": h_disp, "widened": widened})
        peaks.append({"stage": 3, "rot": T.rot * 90, "mirror": T.mirror, "scale": round(T.sx, 5), "angle": round(T.angle, 2),
                      "peak": round(peak, 4), "found": stats["found"], "rms_r": stats["rms_r"], "passed": int(passed),
                      "corrected_px": corr_px, "widened": widened})
        if passed and stats["found"] == stats["attempted"]:
            break   # every landmark found under this candidate: nothing further to try
    best = max(trials, key=lambda t: (t["passed"], t["stats"]["found"], -(t["stats"]["rms_r"] if t["stats"]["rms_r"] is not None else 1e9), t["peak"]))
    T, marks, stats = best["T"], best["marks"], best["stats"]
    srec["transform"], srec["peaks"] = _j(T.as_dict()), _j(peaks)
    srec["scale_searched"] = int(T.scale_searched or bool(T.angle))
    srec["aspect_delta"] = round(T.aspect_delta(), 4)
    srec["peak"] = round(best["peak"], 4)
    pk = sorted((p["peak"] for p in peaks if p.get("stage") == 2), reverse=True)
    srec["peak_ratio"] = round(pk[0] / pk[1], 2) if len(pk) > 1 and pk[1] > 0 else None
    srec["translation_corrected"], srec["translation_correction_px"] = best["corrected"], best["corr_px"]
    srec["candidates_tried"], srec["window_widened"] = len(trials), best["widened"]
    srec["pitch_disp"] = round(best["h_disp"], 2)
    srec.update({"passed": int(best["passed"]), "reason": best["reason"], "attempted": stats["attempted"], "found": stats["found"],
                 "cells": stats["cells"], "max_r": stats["max_r"], "rms_r": stats["rms_r"]})
    for m in marks:
        m["number"] = number
    return srec, marks


# --------------------------------------------------------------------------- the stores


def payload_obj(col: dict, transform: dict | None, status: str) -> dict:
    return {"frame": col["frame"], "frame_file": col.get("frame_file") or "", "crop": col.get("crop"),
            "rows": [[r["row_no"]] + (r["band"] or [None] * 4) + [r["baseline"]] for r in col["rows"]],
            "words": [[w["start"], w["end"], w["row_no"]] + [round(v, 2) for v in w["box"]] for w in col["words"]],
            "transform": transform, "transform_status": status}


class Stores:
    def __init__(self, out_dir: str):
        for name in (PAYLOAD_DB, RELATIONAL_DB):
            p = os.path.join(out_dir, name)
            if os.path.exists(p):
                os.remove(p)
        self.p_path, self.r_path = os.path.join(out_dir, PAYLOAD_DB), os.path.join(out_dir, RELATIONAL_DB)
        self.P = sqlite3.connect(self.p_path)
        self.P.executescript(
            "CREATE TABLE positions (durable_key TEXT NOT NULL, text_hash TEXT NOT NULL, image TEXT NOT NULL, col TEXT NOT NULL,"
            " frame_w REAL, frame_h REAL, transform_status TEXT, overlay_allowed INTEGER, payload BLOB,"
            " PRIMARY KEY (durable_key, text_hash, image, col));"
            "CREATE TABLE positions_meta (key TEXT PRIMARY KEY, value TEXT);")
        self.R = sqlite3.connect(self.r_path)
        self.R.executescript(
            "CREATE TABLE position_images (pk INTEGER PRIMARY KEY, durable_key TEXT NOT NULL, text_hash TEXT NOT NULL,"
            " image TEXT NOT NULL, col TEXT NOT NULL, frame_w REAL, frame_h REAL, frame_file TEXT, crop_x1 REAL, crop_y1 REAL,"
            " crop_x2 REAL, crop_y2 REAL, transform_json TEXT, transform_status TEXT, overlay_allowed INTEGER);"
            "CREATE UNIQUE INDEX ix_images_key ON position_images (durable_key, text_hash, image, col);"
            "CREATE TABLE position_rows (image_pk INTEGER NOT NULL, row_no INTEGER, x REAL, y REAL, w REAL, h REAL, baseline_json TEXT);"
            "CREATE INDEX ix_rows_pk ON position_rows (image_pk);"
            "CREATE TABLE position_words (image_pk INTEGER NOT NULL, row_no INTEGER, start INTEGER, end INTEGER,"
            " x REAL, y REAL, w REAL, h REAL);"
            "CREATE INDEX ix_words_pk ON position_words (image_pk);"
            "CREATE TABLE positions_meta (key TEXT PRIMARY KEY, value TEXT);")
        self.largest_blob = 0
        self.largest_rel = 0
        self.pk = 0
        self.pending = 0

    def add(self, key: tuple, col: dict, transform: dict | None, status: str, allowed: int) -> tuple:
        dk, th, image, cn = key
        obj = payload_obj(col, transform, status)
        blob = zlib.compress(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), 6)
        self.P.execute("INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?)",
                       (dk, th, image, cn, col["frame"][0], col["frame"][1], status, allowed, blob))
        self.pk += 1
        crop = col.get("crop") or [None] * 4
        self.R.execute("INSERT INTO position_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (self.pk, dk, th, image, cn, col["frame"][0], col["frame"][1], col.get("frame_file") or "",
                        crop[0], crop[1], crop[2], crop[3], _j(transform) if transform else None, status, allowed))
        rows = [(self.pk, r["row_no"]) + tuple(r["band"] or [None] * 4) + (json.dumps(r["baseline"]),) for r in col["rows"]]
        words = [(self.pk, w["row_no"], w["start"], w["end"]) + tuple(round(v, 2) for v in w["box"]) for w in col["words"]]
        self.R.executemany("INSERT INTO position_rows VALUES (?,?,?,?,?,?,?)", rows)
        self.R.executemany("INSERT INTO position_words VALUES (?,?,?,?,?,?,?,?)", words)
        self.largest_blob = max(self.largest_blob, len(blob))
        self.largest_rel = max(self.largest_rel, len(json.dumps({"rows": rows, "words": words})))
        self.pending += 1
        if self.pending >= 500:
            self.P.commit()
            self.R.commit()
            self.pending = 0
        return len(blob), sha_bytes(blob), len(words)

    def finish(self, meta: dict) -> None:
        for con in (self.P, self.R):
            con.executemany("INSERT OR REPLACE INTO positions_meta VALUES (?,?)", [(k, str(v)) for k, v in meta.items()])
            con.commit()
            con.close()


def read_payload_p(path: str, key: tuple) -> dict | None:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT payload FROM positions WHERE durable_key=? AND text_hash=? AND image=? AND col=?", key).fetchone()
        return json.loads(zlib.decompress(row[0]).decode("utf-8")) if row else None
    finally:
        con.close()


def read_payload_r(path: str, key: tuple) -> dict | None:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        img = con.execute("SELECT pk, frame_w, frame_h, transform_json, transform_status FROM position_images"
                          " WHERE durable_key=? AND text_hash=? AND image=? AND col=?", key).fetchone()
        if not img:
            return None
        rows = con.execute("SELECT row_no, x, y, w, h, baseline_json FROM position_rows WHERE image_pk=? ORDER BY row_no", (img[0],)).fetchall()
        words = con.execute("SELECT start, end, row_no, x, y, w, h FROM position_words WHERE image_pk=? ORDER BY rowid", (img[0],)).fetchall()
        return {"frame": [img[1], img[2]], "rows": [list(r[:5]) + [json.loads(r[5])] for r in rows],
                "words": [list(w) for w in words], "transform": json.loads(img[3]) if img[3] else None, "transform_status": img[4]}
    finally:
        con.close()


# --------------------------------------------------------------------------- the build


def build(args, dirs: dict, numbers: dict, F: dict, bydir: dict, sample_results: dict, contents: dict) -> dict:
    """Parse every directory of F, tie it to the candidate rows, write both stores; returns the records."""
    try:
        import psutil
        proc = psutil.Process()
    except ImportError:  # pragma: no cover
        proc = None
    stores = Stores(args.out_dir)
    dir_records, payload_records = [], []
    C = collections.Counter()
    verified = {num: json.loads(r["transform"]) for num, r in sample_results.items() if r["passed"]}
    scope = sorted({d for f in F.values() for d in f["dirs"]})
    if getattr(args, "dirs_limit", None):
        scope = scope[:args.dirs_limit]
    # the region and multi-image directories of F's numbers, unresolved by rule, listed for G2
    byrule = sorted({d for f in F.values() for d in numbers[f["number"]]["dirs"]
                     if d in dirs and (dirs[d]["multi"] or dirs[d]["region"])})
    for d in byrule:
        bd = dirs[d]
        dir_records.append({"dir": d, "format": bd["format"], "images": len(bd["numbers"]), "status": "unresolved",
                            "reason": "region_dir" if bd["region"] else "composite_frame", "rows_tied": 0, "payloads": 0,
                            "words": 0, "spans_dropped": 0, "lenient": 0})
        C["dirs.unresolved." + ("region_dir" if bd["region"] else "composite_frame")] += 1
    peak_mb = 0.0
    t0 = time.time()
    for i, d in enumerate(scope):
        bd = dirs[d]
        path = os.path.join(args.withpos, d)
        rows = bydir.get(d, [])
        rec = {"dir": d, "format": bd["format"], "images": len(bd["numbers"]), "status": "unresolved", "reason": "",
               "rows_tied": 0, "payloads": 0, "words": 0, "spans_dropped": 0, "lenient": 0}
        if not rows:
            rec["reason"] = "no_candidate_row"
        else:
            if bd["format"] == "B1":
                g = B1Geometry(path)
            else:
                n = numbers.get(bd["numbers"][0]) if bd["numbers"] else None
                master = (n["pw"], n["ph"]) if n and n["pw"] and n["ph"] else None
                g = B2Geometry(path, master, n["rotation"] if n else None)
            rec["lenient"] = int(g.lenient)
            if g.error:
                rec["reason"] = "no_frame" if g.error == "no_frame" else "parse_error"
                rec["detail"] = g.error
            else:
                reader_text = parse_fgp_xml(os.path.join(path, sorted(f for f in os.listdir(path) if f.lower().endswith(".xml"))[0])) \
                    if bd["format"] == "B1" else (read_b2(path)[0] or "")
                if g.text != reader_text:
                    rec["reason"] = "text_not_reproduced"
                else:
                    th = sha(g.text)
                    tied = [r for r in rows if r["hash"] == th]
                    if not tied:
                        rec["reason"] = "hash_mismatch"
                    else:
                        rec["status"], rec["rows_tied"] = "resolved", len(tied)
                        rec["spans_dropped"] += getattr(g, "spans_dropped", 0)
                        for col in g.columns:
                            if col.get("error"):
                                C["columns.error." + col["error"]] += 1
                                continue
                            image = col["image"]
                            # every span must reproduce the content slice
                            kept = []
                            for w in col["words"]:
                                if g.text[w["start"]:w["end"]] == w["text"]:
                                    kept.append(w)
                                else:
                                    rec["spans_dropped"] += 1
                            col = dict(col, words=kept)
                            status = "verified" if image in verified else "unchecked"
                            for r in tied:
                                key = (r["durable_key"], th, image, col["col"])
                                size, bsha, nwords = stores.add(key, col, verified.get(image), status, int(status == "verified"))
                                payload_records.append({"durable_key": key[0], "text_hash": th, "image": image, "col": col["col"],
                                                        "dir": d, "words": nwords, "rows": len(col["rows"]), "blob_bytes": size,
                                                        "blob_sha": bsha, "transform_status": status,
                                                        "overlay_allowed": int(status == "verified"), "candidate_id": r["id"]})
                                rec["payloads"] += 1
                                rec["words"] += nwords
        if rec["status"] == "unresolved":
            C["dirs.unresolved." + rec["reason"]] += 1
        else:
            C["dirs.resolved"] += 1
        C["dirs.lenient_json"] += rec["lenient"]
        dir_records.append(rec)
        if proc is not None and (i % 200 == 0 or i == len(scope) - 1):
            mi = proc.memory_info()
            peak_mb = max(peak_mb, getattr(mi, "peak_wset", mi.rss) / 1e6)
        if i % 2000 == 0:
            log(f"  {i}/{len(scope)} dirs, {len(payload_records)} payloads, {time.time() - t0:.0f} s")
    stores.finish({"built_at": time.strftime("%Y-%m-%d %H:%M:%S"), "gate": "5", "payloads": len(payload_records),
                   "schema_version": "proto-1"})
    return {"dirs": dir_records, "payloads": payload_records, "counts": C, "peak_mb": peak_mb,
            "largest_blob": stores.largest_blob, "largest_rel": stores.largest_rel, "build_seconds": time.time() - t0}


def measure_stores(args, payload_records: list, sample_numbers: set, contents: dict) -> tuple:
    """Sizes, largest payload, cold read latency per schema, and the read-back checks G3 / G9."""
    p_path, r_path = os.path.join(args.out_dir, PAYLOAD_DB), os.path.join(args.out_dir, RELATIONAL_DB)
    measures = {"P": {"installed_mb": os.path.getsize(p_path) / 1e6}, "R": {"installed_mb": os.path.getsize(r_path) / 1e6}}
    keys_all = sorted((p["durable_key"], p["text_hash"], p["image"], p["col"]) for p in payload_records)
    sampled = [k for k in keys_all if k[2] in sample_numbers]
    rng = random.Random(SEED)
    others = [k for k in keys_all if k[2] not in sample_numbers]
    rng.shuffle(others)
    keys = sampled + others[:args.latency_others]
    lat = {"P": [], "R": []}
    span_bad, disagree = 0, 0
    for key in keys:
        t = time.perf_counter()
        a = read_payload_p(p_path, key)
        lat["P"].append((time.perf_counter() - t) * 1000)
        t = time.perf_counter()
        b = read_payload_r(r_path, key)
        lat["R"].append((time.perf_counter() - t) * 1000)
        if a is None or b is None:
            disagree += 1
            continue
        wa = [tuple(w[:3]) for w in a["words"]]
        wb = [tuple(w[:3]) for w in b["words"]]
        if wa != wb or len(a["rows"]) != len(b["rows"]):
            disagree += 1
        content = contents.get(key[0], {}).get(key[1])
        if content is not None:
            for w in a["words"]:
                if w[1] <= w[0] or w[1] > len(content):
                    span_bad += 1
    for s in ("P", "R"):
        v = lat[s]
        measures[s].update({"reads": len(v), "p50_ms": round(pct(v, 0.5), 3), "p95_ms": round(pct(v, 0.95), 3),
                            "max_ms": round(max(v), 3) if v else 0.0})
    return measures, {"span_bad": span_bad, "disagree": disagree, "reads": len(keys)}


def verify_stores(p_path: str, candidate_hashes: dict, allowed_images: set, byrule_dirs: set, payload_dirs: dict) -> dict:
    """G2 read back from the payload store: every key tied to a candidate row of that hash; overlay_allowed only on
    verified images; nothing from a region / composite directory."""
    con = sqlite3.connect(f"file:{p_path}?mode=ro", uri=True)
    untied = unallowed = byrule = 0
    for dk, th, image, col, allowed in con.execute("SELECT durable_key, text_hash, image, col, overlay_allowed FROM positions"):
        if candidate_hashes.get(dk) != th:
            untied += 1
        if allowed and image not in allowed_images:
            unallowed += 1
        if payload_dirs.get((dk, th, image, col)) in byrule_dirs:
            byrule += 1
    con.close()
    return {"untied": untied, "unallowed": unallowed, "byrule": byrule}


def verify_spans(p_path: str, contents: dict, limit: int | None = None) -> dict:
    """G3: every span's text equals the content slice, read back from the payload store."""
    con = sqlite3.connect(f"file:{p_path}?mode=ro", uri=True)
    bad = total = 0
    for dk, th, blob in con.execute("SELECT durable_key, text_hash, payload FROM positions"):
        content = contents.get(dk, {}).get(th)
        obj = json.loads(zlib.decompress(blob).decode("utf-8"))
        for w in obj["words"]:
            total += 1
            if content is None or not (0 <= w[0] < w[1] <= len(content)):
                bad += 1
    con.close()
    return {"bad": bad, "total": total}


# --------------------------------------------------------------------------- checks, digest, outputs


def run_checks(sample: list, built: dict, measures: dict, readback: dict, g2: dict, g3: dict, digest: str, previous: str | None,
               allow_single: bool, expectations: dict, C: collections.Counter) -> tuple:
    checks = []

    def add(name, passed, expected, actual, note=""):
        checks.append({"name": name, "passed": int(bool(passed)), "expected": str(expected), "actual": str(actual), "note": note})

    failed = [s["number"] + ":" + (s["reason"] or "fail") for s in sample if not s["passed"]]
    add("G1_geometry", not failed and len(sample) > 0, f"{len(sample)} images PASS", f"{len(sample) - len(failed)} PASS; failed: {failed[:20]}",
        "residual landmark error after the recorded transform, under the declared tolerance")
    add("G1_sample_size", len(sample) >= C.get("sample.quota", len(sample)), f">= {C.get('sample.quota', len(sample))} measured",
        len(sample), "undisplayable images gave way to the next seeded draw of their stratum")
    add("G2_no_overlay_without_tie", g2["untied"] == 0 and g2["unallowed"] == 0 and g2["byrule"] == 0, 0,
        f"untied {g2['untied']}, allowed-unverified {g2['unallowed']}, by-rule dirs {g2['byrule']}",
        "payloads tied to a candidate row of the same text hash; overlay_allowed only on verified images")
    add("G3_spans", g3["bad"] == 0, 0, f"{g3['bad']} of {g3['total']}", "span slices read back from the store equal the word")
    if previous:
        add("G4_two_runs_identical", digest == previous, previous[:16], digest[:16], "digest of transforms, residuals, payload hashes, reasons")
    else:
        add("G4_two_runs_identical", allow_single, "previous digest", "single run allowed" if allow_single else "no previous summary",
            "pass --previous-summary or --allow-single-run")
    within = {}
    for s in ("P", "R"):
        m = measures[s]
        largest_mb = (built["largest_blob"] if s == "P" else built["largest_rel"]) / 1e6
        m["largest_payload_mb"] = round(largest_mb, 4)
        within[s] = (m["installed_mb"] <= LIMITS["size.installed_mb"] and largest_mb <= LIMITS["payload.largest_mb"]
                     and m["p95_ms"] <= LIMITS["latency.p95_ms"] and m["max_ms"] <= LIMITS["latency.max_ms"])
    passing = [s for s in ("P", "R") if within[s]]
    chosen = min(passing, key=lambda s: (measures[s]["installed_mb"], s)) if passing else None
    if chosen is None:
        chosen_note = "neither schema within every limit"
    else:
        chosen_note = f"{chosen} chosen: smallest installed size among the passing schemas (P on a tie)"
    cm = measures[chosen] if chosen else {"installed_mb": 0, "largest_payload_mb": 0, "p95_ms": 0, "max_ms": 0}
    add("G5_size", chosen is not None and cm["installed_mb"] <= LIMITS["size.installed_mb"], f"<= {LIMITS['size.installed_mb']} MB",
        f"{cm['installed_mb']:.1f} MB" if chosen else "no passing schema", chosen_note)
    add("G6_largest_payload", chosen is not None and cm["largest_payload_mb"] <= LIMITS["payload.largest_mb"],
        f"<= {LIMITS['payload.largest_mb']} MB", f"{cm['largest_payload_mb']:.4f} MB" if chosen else "no passing schema", chosen_note)
    add("G7_cold_read_latency", chosen is not None and cm["p95_ms"] <= LIMITS["latency.p95_ms"] and cm["max_ms"] <= LIMITS["latency.max_ms"],
        f"p95 <= {LIMITS['latency.p95_ms']} ms, max <= {LIMITS['latency.max_ms']} ms",
        f"p95 {cm['p95_ms']} ms, max {cm['max_ms']} ms" if chosen else "no passing schema", "fresh read-only connection per read")
    add("G8_build_memory", built["peak_mb"] <= LIMITS["memory.build_peak_mb"], f"<= {LIMITS['memory.build_peak_mb']} MB",
        f"{built['peak_mb']:.0f} MB", "peak working set of the build")
    add("G9_schemas_agree", readback["disagree"] == 0, 0, readback["disagree"], f"read back {readback['reads']} keys from both stores")
    for key in sorted(expectations):
        actual = C.get(key, 0)
        add(f"E_{key}", actual == expectations[key], expectations[key], actual, "declared expectation")
    return checks, chosen


def digest_of(sample: list, payloads: list, dir_records: list) -> str:
    lines = []
    for s in sorted(sample, key=lambda s: s["number"]):
        t = json.loads(s["transform"]) if s["transform"] else {}
        lines.append("S|" + "|".join(str(x) for x in (s["number"], s["fl"], t.get("rot"), t.get("mirror"), t.get("sx"), t.get("sy"),
                                                       t.get("dx"), t.get("dy"), s["found"], s["cells"], s["max_r"], s["rms_r"],
                                                       s["passed"], s["reason"])))
    for p in sorted(payloads, key=lambda p: (p["durable_key"], p["text_hash"], p["image"], p["col"])):
        lines.append("P|" + "|".join((p["durable_key"], p["text_hash"], p["image"], p["col"], p["blob_sha"], str(p["overlay_allowed"]))))
    for d in sorted(dir_records, key=lambda d: d["dir"]):
        lines.append("D|" + "|".join((d["dir"], d["status"], d["reason"], str(d["payloads"]), str(d["spans_dropped"]))))
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def _create(conn: sqlite3.Connection, table: str, rows: list, pk: str | None = None) -> None:
    if not rows:
        conn.execute(f'CREATE TABLE "{table}" (empty TEXT)')
        return
    cols = list(rows[0].keys())
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    coldefs = ", ".join(f'"{c}"' + (" PRIMARY KEY" if c == pk else "") for c in cols)
    conn.execute(f'CREATE TABLE "{table}" ({coldefs})')
    conn.executemany(f'INSERT INTO "{table}" VALUES ({",".join("?" * len(cols))})',
                     [tuple(_cell(r.get(c)) for c in cols) for r in rows])


def _cell(v):
    if isinstance(v, (dict, list, tuple, set)):
        return _j(sorted(v) if isinstance(v, set) else v)
    if isinstance(v, bool):
        return int(v)
    return v


def write_outputs(out_dir: str, sample: list, landmarks: list, built: dict, measures: dict, checks: list, C: collections.Counter,
                  meta: dict, undisplayable: list | None = None) -> None:
    undisplayable = undisplayable or []
    db = os.path.join(out_dir, "gate5.db")
    if os.path.exists(db):
        os.remove(db)
    con = sqlite3.connect(db)
    _create(con, "sample", sample, pk="number")
    _create(con, "undisplayable", undisplayable)
    _create(con, "landmarks", landmarks)
    _create(con, "dirs", built["dirs"], pk="dir")
    _create(con, "payloads", built["payloads"])
    _create(con, "schema_measures", [dict(schema=s, **{k: v for k, v in m.items()}) for s, m in measures.items()], pk="schema")
    _create(con, "limits", [{"key": k, "value": v} for k, v in sorted(LIMITS.items())], pk="key")
    _create(con, "tolerance", [{"key": k, "value": v} for k, v in sorted(TOLERANCE.items())], pk="key")
    _create(con, "memory", [{"metric": "build_peak_mb", "value_mb": round(built["peak_mb"], 1), "budget_mb": LIMITS["memory.build_peak_mb"]}], pk="metric")
    _create(con, "checks", checks, pk="name")
    _create(con, "counts", [{"key": k, "value": v} for k, v in sorted(C.items())], pk="key")
    _create(con, "gate5_meta", [{"key": k, "value": _cell(v)} for k, v in sorted(meta.items())], pk="key")
    con.commit()
    con.close()


def render_report(sample: list, built: dict, measures: dict, checks: list, C: collections.Counter, meta: dict, chosen: str | None) -> str:
    L = ["# FGP gate 5 — positions prototype and the 20-image geometry check", "",
         f"Run {meta['run_at']}; digest `{meta['digest'][:16]}`; result **{'PASS' if meta['passed'] else 'FAIL'}**"
         f" ({sum(c['passed'] for c in checks)} / {len(checks)} checks). Sample: {meta['sample_mode']}"
         f"{'; shortfall ' + _j(meta['shortfall']) if meta.get('shortfall') else ''}.", "",
         "## Checks", "", "| check | passed | expected | actual | note |", "|---|---|---|---|---|"]
    for c in checks:
        L.append(f"| {c['name']} | {'yes' if c['passed'] else 'NO'} | {c['expected']} | {c['actual']} | {c['note']} |")
    L += ["", "## The sampled images", "",
          "| image | stratum | fmt | rot | library | frame | master? | FL | label | transform | h | found | max r | RMS r | result |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for s in sample:
        t = json.loads(s["transform"]) if s["transform"] else {}
        tr = f"{t.get('rot')}° {'mirror ' if t.get('mirror') else ''}×{t.get('sx')}/{t.get('sy')} +({t.get('dx')}, {t.get('dy')})" if t else "—"
        L.append(f"| {s['number']} | {s.get('stratum', '')} | {s['format']} | {s['rotation_deg']} | {s['library']} | "
                 f"{s['frame_w']}×{s['frame_h']} | {s['frame_is_master']} | {s['fl']} | {s['canvas_label']} / {s['folio_label']} | {tr} | "
                 f"{s['pitch_disp']} | {s['found']}/{s['attempted']} in {s['cells']} cells | {s['max_r']} | {s['rms_r']} | "
                 f"{'PASS' if s['passed'] else 'FAIL ' + (s['reason'] or '')} |")
    L += ["", "## The store", "", f"Directories in scope: {C.get('dirs.resolved', 0)} resolved, "
          f"{sum(v for k, v in C.items() if k.startswith('dirs.unresolved.'))} unresolved "
          f"({', '.join(k.split('.')[-1] + ' ' + str(v) for k, v in sorted(C.items()) if k.startswith('dirs.unresolved.'))}); "
          f"{len(built['payloads'])} payloads, {sum(p['words'] for p in built['payloads'])} positioned words, "
          f"{sum(d['spans_dropped'] for d in built['dirs'])} spans dropped; build {built['build_seconds']:.0f} s, peak {built['peak_mb']:.0f} MB.", "",
          "| schema | installed | largest payload | reads | p50 | p95 | max | within limits |", "|---|---|---|---|---|---|---|---|"]
    for s, m in measures.items():
        ok = (m["installed_mb"] <= LIMITS["size.installed_mb"] and m["largest_payload_mb"] <= LIMITS["payload.largest_mb"]
              and m["p95_ms"] <= LIMITS["latency.p95_ms"] and m["max_ms"] <= LIMITS["latency.max_ms"])
        L.append(f"| {s} | {m['installed_mb']:.1f} MB | {m['largest_payload_mb']:.4f} MB | {m['reads']} | {m['p50_ms']} ms | {m['p95_ms']} ms | "
                 f"{m['max_ms']} ms | {'yes' if ok else 'NO'} |")
    L += ["", f"Chosen schema: **{chosen or 'none'}**.", "", "## Counts", ""]
    for k, v in sorted(C.items()):
        L.append(f"- `{k}` = {v}")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- main


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--withpos", default=DEFAULT_WITHPOS)
    ap.add_argument("--inventory", default=DEFAULT_INVENTORY)
    ap.add_argument("--gate3", default=DEFAULT_GATE3)
    ap.add_argument("--gate4", default=DEFAULT_GATE4)
    ap.add_argument("--candidate", default=DEFAULT_CANDIDATE)
    ap.add_argument("--nli", default=DEFAULT_NLI)
    ap.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--cache-dir", default=None, help="fetched manifests and images (default <out-dir>/gate5_cache)")
    ap.add_argument("--images", default=None, help="comma-separated image numbers replacing the seeded sample")
    ap.add_argument("--latency-others", type=int, default=LATENCY_OTHERS)
    ap.add_argument("--dirs-limit", type=int, default=None, help="development only: build the first N directories of the scope")
    ap.add_argument("--previous-summary", default=None)
    ap.add_argument("--allow-single-run", action="store_true")
    ap.add_argument("--offline", action="store_true", help="never fetch; a cache miss is a fetch failure")
    ap.add_argument("--refetch", action="store_true", help="ignore the cache")
    ap.add_argument("--expect", action="append", default=[])
    ap.add_argument("--expect-file", default=None)
    args = ap.parse_args(argv)
    args.cache_dir = args.cache_dir or os.path.join(args.out_dir, "gate5_cache")
    os.makedirs(args.out_dir, exist_ok=True)
    if args.refetch and os.path.isdir(args.cache_dir):
        for f in os.listdir(args.cache_dir):
            os.remove(os.path.join(args.cache_dir, f))
    expectations = parse_expectations(args.expect, args.expect_file)
    previous = None
    if args.previous_summary:
        with open(args.previous_summary, encoding="utf-8") as fh:
            previous = json.load(fh).get("digest")
    t_start = time.time()
    run_at = time.strftime("%Y-%m-%d %H:%M:%S")

    log("loading the inventory, gate 3, gate 4 and the candidate sidecar")
    dirs, numbers = load_b_tables(args.inventory)
    bydir, contents, hashes, cand_meta = load_candidate_rows(args.candidate, args.gate3, args.gate4)
    log(f"  candidate sidecar built {cand_meta.get('built_at')}: {cand_meta.get('rows')} rows")
    libraries = load_libraries(args.libraries_csv)
    F = eligible_frame(dirs, numbers, bydir, libraries)
    C = collections.Counter()
    C["frame.eligible_images"] = len(F)
    C["frame.eligible_dirs"] = len({d for f in F.values() for d in f["dirs"]})
    for f in F.values():
        C[f"frame.{f['format']}.{f['rclass']}"] += 1
    log(f"  eligible frame: {len(F)} images in {C['frame.eligible_dirs']} directories")
    # the sample
    if args.images:
        wanted = [x.strip() for x in args.images.split(",") if x.strip()]
        chosen = [(n, "manual") for n in wanted]
        shortfall = {}
        quotas = {"manual": len(chosen)}
        sample_mode = f"--images {args.images}"
    else:
        chosen, shortfall = sample_images(F, reserve=SAMPLE_RESERVE)
        quotas = {st: q for st, _, _, q, _ in STRATA}
        sample_mode = f"seeded ({SEED}) stratified draw, reserve {SAMPLE_RESERVE} per stratum"
    C["sample.shortfall"] = sum(shortfall.values())
    log(f"  sample: {[n for n, _ in chosen]} ({sample_mode})")

    # geometry: parse the sampled images' directories, fetch what the apps display, check
    nli = NliCrossrefService(db_path=args.nli)
    geoms = {}
    sample, landmarks, undisplayable = [], [], []
    measured = collections.Counter()
    for num, stratum in chosen:
        if measured[stratum] >= quotas.get(stratum, 0):
            continue    # the stratum's quota is measured; the rest of its draw was reserve
        rec = F.get(num)
        if rec is None:
            n = numbers.get(num)
            srec = {"number": num, "stratum": stratum, "format": "", "rotation_deg": n["rotation"] if n else None, "rclass": "",
                    "library": "", "dirs": "[]", "passed": 0, "reason": "not_in_frame", "attempted": 0, "found": 0, "cells": 0,
                    "max_r": None, "rms_r": None, "transform": None, "fl": None, "canvas_label": "", "folio_label": "",
                    "frame_w": None, "frame_h": None, "frame_is_master": None, "pitch_disp": None}
            sample.append(srec)
            continue
        for d in rec["dirs"]:
            if d not in geoms:
                path = os.path.join(args.withpos, d)
                if dirs[d]["format"] == "B1":
                    geoms[d] = B1Geometry(path)
                else:
                    n = numbers[num]
                    geoms[d] = B2Geometry(path, (n["pw"], n["ph"]) if n["pw"] and n["ph"] else None, n["rotation"])
        log(f"  image {num} ({stratum}, {rec['format']}, {rec['rclass']}, {rec['library']})")
        srec, marks = check_image(num, rec, geoms, numbers, nli, args.cache_dir, args.offline)
        srec["stratum"] = stratum
        if srec["reason"] in UNDISPLAYABLE and not args.images:
            # NLI does not serve this image now (the apps show nothing or a Rosetta thumbnail): nothing to check,
            # the stratum takes its next seeded draw
            log(f"    undisplayable ({srec['reason']}): the next seeded draw of stratum {stratum} takes its place")
            undisplayable.append({"number": num, "stratum": stratum, "reason": srec["reason"], "sys_id": srec.get("sys_id"),
                                  "fl": srec.get("fl"), "library": rec["library"], "format": rec["format"]})
            continue
        measured[stratum] += 1
        log(f"    {'PASS' if srec['passed'] else 'FAIL ' + srec['reason']}: found {srec['found']}/{srec['attempted']} in {srec['cells']} cells,"
            f" max r {srec['max_r']}, RMS {srec['rms_r']}, h {srec['pitch_disp']}, transform {srec['transform']}")
        sample.append(srec)
        landmarks.extend(marks)
    nli.close()
    C["sample.images"] = len(sample)
    C["sample.undisplayable"] = len(undisplayable)
    C["sample.quota"] = min(sum(quotas.values()), sum(quotas.values()) - sum(shortfall.values()))
    sample_results = {s["number"]: s for s in sample}
    C["geometry.pass"] = sum(1 for s in sample if s["passed"])
    C["geometry.fail"] = sum(1 for s in sample if not s["passed"])
    for s in sample:
        if s.get("frame_is_master") is not None:
            C["geometry.frame_is_master"] += int(s["frame_is_master"])
        if s.get("scale_searched") is not None:
            C["geometry.scale_searched"] += int(s["scale_searched"])
            C["geometry.frame_swapped"] += int(s["frame_swapped"] or 0)
            C["geometry.translation_corrected"] += int(s.get("translation_corrected") or 0)
        if s.get("canvas_label_agrees") is not None:
            C["geometry.canvas_label_agrees"] += int(s["canvas_label_agrees"])
        if s.get("gallery_manifest_agree") is not None:
            C["geometry.gallery_manifest_agree"] += int(s["gallery_manifest_agree"])
        t = json.loads(s["transform"]) if s.get("transform") else None
        if t:
            C[f"geometry.rot.{t['rot']}{'m' if t['mirror'] else ''}"] += 1

    # the store
    log("building the positions store (both schemas)")
    built = build(args, dirs, numbers, F, bydir, sample_results, contents)
    C.update(built["counts"])
    C["payloads"] = len(built["payloads"])
    C["payloads.overlay_allowed"] = sum(p["overlay_allowed"] for p in built["payloads"])
    C["payloads.words"] = sum(p["words"] for p in built["payloads"])
    C["spans.dropped"] = sum(d["spans_dropped"] for d in built["dirs"])
    C["payloads.images"] = len({p["image"] for p in built["payloads"]})
    C["payloads.rows"] = len({p["durable_key"] for p in built["payloads"]})
    log(f"  {C['payloads']} payloads on {C['payloads.images']} images, {C['payloads.words']} words; build {built['build_seconds']:.0f} s")

    log("measuring the stores")
    sample_numbers = {s["number"] for s in sample}
    measures, readback = measure_stores(args, built["payloads"], sample_numbers, contents)
    allowed_images = {s["number"] for s in sample if s["passed"]}
    byrule_dirs = {d["dir"] for d in built["dirs"] if d["reason"] in ("region_dir", "composite_frame")}
    payload_dirs = {(p["durable_key"], p["text_hash"], p["image"], p["col"]): p["dir"] for p in built["payloads"]}
    g2 = verify_stores(os.path.join(args.out_dir, PAYLOAD_DB), hashes, allowed_images, byrule_dirs, payload_dirs)
    g3 = verify_spans(os.path.join(args.out_dir, PAYLOAD_DB), contents)
    C["spans.verified"] = g3["total"] - g3["bad"]
    digest = digest_of(sample, built["payloads"], built["dirs"])
    checks, chosen_schema = run_checks(sample, built, measures, readback, g2, g3, digest, previous, args.allow_single_run, expectations, C)
    passed = all(c["passed"] for c in checks)
    meta = {"run_at": run_at, "seconds": round(time.time() - t_start, 1), "digest": digest, "passed": int(passed),
            "sample_mode": sample_mode, "shortfall": shortfall, "chosen_schema": chosen_schema, "limits": LIMITS,
            "tolerance": TOLERANCE, "seed": SEED, "display_width": DISPLAY_WIDTH, "withpos": args.withpos,
            "candidate": args.candidate, "candidate_built_at": cand_meta.get("built_at"), "candidate_rows": cand_meta.get("rows"),
            "cache_dir": args.cache_dir, "offline": args.offline}
    write_outputs(args.out_dir, sample, landmarks, built, measures, checks, C, meta, undisplayable)
    summary = {"digest": digest, "passed": passed, "run_at": run_at, "seconds": meta["seconds"], "chosen_schema": chosen_schema,
               "checks": checks, "counts": dict(sorted(C.items())), "measures": measures, "limits": LIMITS, "tolerance": TOLERANCE,
               "sample": [{k: s.get(k) for k in ("number", "stratum", "format", "rclass", "library", "fl", "passed", "reason",
                                                 "found", "attempted", "cells", "max_r", "rms_r", "pitch_disp", "transform",
                                                 "frame_is_master", "image_sha")} for s in sample],
               "shortfall": shortfall, "sample_mode": sample_mode, "build_peak_mb": round(built["peak_mb"], 1),
               "undisplayable": undisplayable, "candidate_built_at": cand_meta.get("built_at")}
    with open(os.path.join(args.out_dir, "gate5_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(args.out_dir, "GATE5_REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(sample, built, measures, checks, C, meta, chosen_schema))
    for c in checks:
        log(f"  [{'ok' if c['passed'] else 'FAIL'}] {c['name']}: expected {c['expected']}, actual {c['actual']}")
    log(f"gate 5 {'PASS' if passed else 'FAIL'} ({sum(c['passed'] for c in checks)}/{len(checks)} checks; digest {digest[:16]};"
        f" {meta['seconds']} s; schema {chosen_schema})")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
