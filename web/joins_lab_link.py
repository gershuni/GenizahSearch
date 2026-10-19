# -*- coding: utf-8 -*-
"""The Joins Lab screen as a link: encode it into the URL, read it back safely.

Owner decision 2026-10-19 (docs/plans/JOINS_LAB_VS_IMPROVEMENTS_PLAN.md, PR 2): a
"Copy link" button, and an address bar that follows the screen, reopen the same
Joins Lab screen for anyone. The anchor stays in the readable parameters the page
already takes (``sys_id``, ``page``, ``fl_id``, ``volume_ie``), so a link whose
state cannot be read still opens the right fragment. Everything else rides in one
``state`` parameter: compact JSON, zlib-compressed, base64url.

The link carries the INPUTS, never results: the search is run again on open.
What it carries:

* ``b`` -- the anchor-side builder: ``rows`` (the word-model lines), ``type``
  (responsa | exact | variants | fuzzy | regex), ``var`` (variants on),
  ``text`` (the single-line query), ``pos`` (text position)
* ``g`` -- global options: ``flex``, ``bidir``
* ``o`` -- the other side: ``on``, ``rows``, ``combine`` (AND | OR)
* ``vs`` -- Visual Similarity on
* ``f`` -- the candidate filter (the filter dialog's keys)
* ``view`` -- grid | table; ``gp`` -- the 0-based grid/table page
* ``cmp`` -- the Compare pair's candidate as ``[sys_id, page]``
* ``m`` -- the sender's marks ({sys_id: yes|maybe|no}), only when the sender
  ticks "Include my marks". The recipient sees them for that visit only; they
  are never saved into the recipient's state.

A link is untrusted input. ``decode_share_state`` never raises. It returns None
for anything it cannot read, caps the token and the decompressed size (no zlib
bomb), drops unknown keys, and coerces or drops every value. Builder rows go
through the same caps as the persisted state (``joins_lab_storage._cap_rows``).
"""
from __future__ import annotations

import base64
import binascii
import json
import zlib
from typing import Any, Optional
from urllib.parse import urlencode

from shared.sys_id_patterns import CORPUS_SYS_ID_RE
from web.joins_lab_storage import _cap_rows

SHARE_PARAM = "state"
SHARE_VERSION = 1

MAX_TOKEN_CHARS = 12_000           # longer -> refused (a URL stays well inside limits)
MAX_DECODED_BYTES = 200_000        # decompression cap
MAX_LINK_MARKS = 300               # marks carried by one link
MAX_PAGE_INDEX = 10_000

SEARCH_TYPES = ("responsa", "exact", "variants", "fuzzy", "regex")
TEXT_POSITIONS = ("anywhere", "start", "end", "line_start", "line_end")
VERDICTS = ("yes", "maybe", "no")
VIEWS = ("grid", "table")
COMBINES = ("AND", "OR")
_FILTER_TRIAGE_STATES = ("All", "Not triaged", "Yes", "Maybe", "No")
_MAX_TEXT_CHARS = 2_000
_MAX_FILTER_LIST = 50
_MAX_FILTER_TEXT = 200


# ---------------------------------------------------------------------------
# Value coercion (shared by encode and decode, so both sides agree)
# ---------------------------------------------------------------------------

def _is_sys_id(value: Any) -> bool:
    return isinstance(value, str) and bool(CORPUS_SYS_ID_RE.fullmatch(value))


def _int_in(value: Any, low: int, high: int) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if low <= n <= high else None


def _clean_rows(rows: Any) -> list:
    """Word-model lines through the persisted-state caps; mods kept as name -> bool."""
    cleaned = []
    for row in _cap_rows(rows):
        if "words" not in row:
            continue                      # legacy flat rows are never produced by a link
        for w in row["words"]:
            w["mods"] = {str(k)[:40]: bool(v) for k, v in w["mods"].items()
                         if isinstance(v, bool)}
            w["gap_to_next_word"] = _int_in(w.get("gap_to_next_word"), 0, 50) or 0
        row["gap_to_next_line"] = _int_in(row.get("gap_to_next_line"), 0, 50) or 0
        cleaned.append(row)
    return cleaned


def _clean_builder(b: Any) -> dict:
    if not isinstance(b, dict):
        return {}
    out: dict = {"rows": _clean_rows(b.get("rows"))}
    if b.get("type") in SEARCH_TYPES:
        out["type"] = b["type"]
    out["var"] = bool(b.get("var", False))
    text = b.get("text", "")
    out["text"] = text[:_MAX_TEXT_CHARS] if isinstance(text, str) else ""
    if b.get("pos") in TEXT_POSITIONS:
        out["pos"] = b["pos"]
    return out


def _clean_filter(f: Any) -> dict:
    if not isinstance(f, dict):
        return {}
    out: dict = {}
    mats = f.get("materials")
    if isinstance(mats, list):
        out["materials"] = [m[:_MAX_FILTER_TEXT] for m in mats[:_MAX_FILTER_LIST]
                            if isinstance(m, str)]
    out["has_dims"] = bool(f.get("has_dims", False))
    out["exclude_mismatch"] = bool(f.get("exclude_mismatch", False))
    tri = f.get("triage_states")
    if isinstance(tri, list):
        out["triage_states"] = [t for t in tri if t in _FILTER_TRIAGE_STATES]
    q = f.get("text_q", "")
    out["text_q"] = q[:_MAX_FILTER_TEXT] if isinstance(q, str) else ""
    return out


def _clean_marks(m: Any) -> dict:
    if not isinstance(m, dict):
        return {}
    out: dict = {}
    for sid, verdict in m.items():
        if len(out) >= MAX_LINK_MARKS:
            break
        if _is_sys_id(sid) and verdict in VERDICTS:
            out[sid] = verdict
    return out


def clean_share_state(state: Any) -> dict:
    """Keep only the known keys, each coerced to its type or dropped."""
    if not isinstance(state, dict):
        return {}
    out: dict = {}
    if "b" in state:
        out["b"] = _clean_builder(state["b"])
    g = state.get("g")
    if isinstance(g, dict):
        out["g"] = {"flex": bool(g.get("flex", False)), "bidir": bool(g.get("bidir", False))}
    o = state.get("o")
    if isinstance(o, dict) and o.get("on"):
        out["o"] = {
            "on": True,
            "rows": _clean_rows(o.get("rows")),
            "combine": o["combine"] if o.get("combine") in COMBINES else "AND",
        }
    if state.get("vs"):
        out["vs"] = True
    if "f" in state:
        out["f"] = _clean_filter(state["f"])
    if state.get("view") in VIEWS:
        out["view"] = state["view"]
    gp = _int_in(state.get("gp"), 0, MAX_PAGE_INDEX)
    if gp:
        out["gp"] = gp
    cmp_ = state.get("cmp")
    if isinstance(cmp_, (list, tuple)) and len(cmp_) == 2 and _is_sys_id(cmp_[0]):
        out["cmp"] = [cmp_[0], _int_in(cmp_[1], 1, 100_000)]
    marks = _clean_marks(state.get("m"))
    if marks:
        out["m"] = marks
    return out


def share_inputs_from_full_state(full_state: Any) -> dict:
    """The stored per-visitor blob (``write_full_state``) in the link's shape.

    Lets one routine apply the inputs whether they come from storage or a link.
    Legacy blobs without ``variants_on`` infer it from the search type, as the
    restore always has.
    """
    if not isinstance(full_state, dict):
        return {}
    variants = full_state.get("variants_on")
    if variants is None:
        variants = full_state.get("builder_mode") == "variants"
    state: dict = {
        "b": {
            "rows": full_state.get("builder_rows", []),
            "type": full_state.get("builder_mode", "responsa"),
            "var": bool(variants),
            "text": full_state.get("single_text", ""),
            "pos": full_state.get("text_position", "anywhere"),
        },
        "g": {
            "flex": bool(full_state.get("flex_spacing", False)),
            "bidir": bool(full_state.get("bidirectional", False)),
        },
        "f": full_state.get("active_filter") or {},
        "view": full_state.get("view_mode", "grid"),
    }
    if full_state.get("other_side_enabled"):
        state["o"] = {
            "on": True,
            "rows": full_state.get("other_side_rows", []),
            "combine": full_state.get("other_side_combine", "AND"),
        }
    return clean_share_state(state)


def same_inputs(a: dict, b: dict) -> bool:
    """True when two cleaned states ask for the same search (builders, options, VS)."""
    keys = ("b", "g", "o", "vs")
    return all(a.get(k) == b.get(k) for k in keys)


# ---------------------------------------------------------------------------
# Token <-> state
# ---------------------------------------------------------------------------

def encode_share_state(state: dict) -> str:
    """The ``state`` parameter for *state* (cleaned first, so it decodes to the same)."""
    payload = {"v": SHARE_VERSION, **clean_share_state(state)}
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(zlib.compress(raw, 9)).decode("ascii").rstrip("=")


def decode_share_state(token: Any) -> Optional[dict]:
    """The cleaned state a ``state`` parameter carries, or None if it cannot be read."""
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_CHARS:
        return None
    try:
        packed = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        inflater = zlib.decompressobj()
        raw = inflater.decompress(packed, MAX_DECODED_BYTES)
        if inflater.unconsumed_tail:
            return None                   # would decompress past the cap
        data = json.loads(raw.decode("utf-8"))
    except (binascii.Error, ValueError, zlib.error, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or data.get("v") != SHARE_VERSION:
        return None
    return clean_share_state(data)


def build_share_url(
    sys_id: str,
    *,
    page: Optional[int] = None,
    fl_id: Optional[str] = None,
    volume_ie: Optional[str] = None,
    state: Optional[dict] = None,
    base: str = "/joins-lab",
) -> str:
    """``/joins-lab?sys_id=...&page=...&state=...`` (relative unless *base* is absolute)."""
    params: dict = {"sys_id": sys_id}
    if page:
        params["page"] = int(page)
    if fl_id:
        params["fl_id"] = fl_id
    if volume_ie:
        params["volume_ie"] = volume_ie
    cleaned = clean_share_state(state or {})
    if cleaned:
        params[SHARE_PARAM] = encode_share_state(cleaned)
    return f"{base}?{urlencode(params)}"
