# -*- coding: utf-8 -*-
"""Searching the scholarly editions beside the automatic transcriptions.

Editions search plan (private repository), §3.3-§3.7, PR 2. The editions index
(``shared/editions_indexer.py``) is opened as a **leaf** ``SearchEngine`` -- the main path on a
second index, no child and no My Library of its own -- and attached to the main engine, whose
``execute_search`` runs the legs the **source choice** asks for and groups the hits **by page**.
This module holds what is not the engine itself:

* the switch (``EDITIONS_SEARCH_ENABLED``: on / off / ``editors``) and the generation's
  readiness (one build id, the schema's fields, the document count);
* the source choice (``all`` / ``editions`` / ``automatic``);
* edition identities (``ED:fgp:<id>``, ``ED:pgp:<id>:<sys_id>``, ``<sys_id>_ED_<kind>_<id>``) and
  the ``edition`` record every edition row carries;
* the grouping of main, edition and My Library rows into one result per page (§3.4);
* the edition resolver (the sidecar's original text and credit for a version);
* promotion of a new generation under the readers' lease (§3.7).

Nothing here is served unless a caller passes ``sources`` to ``execute_search``: with no source
choice the search is today's, row for row.
"""
from __future__ import annotations

import bisect
import itertools
import logging
import operator
import os
import re
import sqlite3
import unicodedata
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from shared.dictionary_switch import OFF, read_switch, serves
from shared.edition_search_text import CLEANER_VERSION, SOURCE_FGP, SOURCE_PGP, search_text
from shared.editions_indexer import (DB_DIRNAME, REQUIRED_FIELDS, UID_PREFIX, default_generation_dir,
                                     edition_header, read_generation)

LOGGER = logging.getLogger("genizah." + __name__)

#: The switch: off (the default), on, or ``editors`` (signed-in editors and admins only; the web
#: caller decides who is one and passes the source choice to the search worker).
SWITCH_ENV = "EDITIONS_SEARCH_ENABLED"

SOURCES_ALL = "all"            # automatic transcriptions and editions (the default once served)
SOURCES_EDITIONS = "editions"  # editions only
SOURCES_AUTOMATIC = "automatic"  # automatic transcriptions only: today's search exactly
SOURCES = (SOURCES_ALL, SOURCES_EDITIONS, SOURCES_AUTOMATIC)

#: The labels a result names the texts that matched by (plan §3.5).
SOURCE_AUTOMATIC_LABEL = "automatic"
SOURCE_LOCAL_LABEL = "local"
EDITION_SOURCE_LABELS = {SOURCE_FGP: "fgp_edition", SOURCE_PGP: "pgp_edition"}
INDEX_SOURCE_KINDS = {"FGP": SOURCE_FGP, "PGP": SOURCE_PGP}

#: RRF constant, as for My Library (``shared.search_engine.RRF_K``).
RRF_K = 60

_UID_RE = re.compile(r"^ED:(fgp|pgp):([^:]+)(?::(\d+))?$")
_HEADER_RE = re.compile(r"^(\d+)_ED_(fgp|pgp)_([^_]+)(?:_(.+))?$")


# --- switch and readiness ------------------------------------------------------------------

def editions_switch() -> str:
    """``on``, ``off`` or ``editors``; off while the variable is unset."""
    return read_switch(SWITCH_ENV, OFF)


def editions_served_to(editor: bool) -> bool:
    """Is the switch on for this reader (on: everyone; editors: an editor or admin)? Readiness is
    the engine's (``SearchEngine.editions_ready``)."""
    return serves(editions_switch(), editor)


def check_generation(gen_dir: str) -> Tuple[bool, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """``(ready, reason, meta, browse map payload)`` of the generation at *gen_dir* (plan §3.6):
    its index, browse map, format marker and meta share one build id; the schema has every field
    the search reads; the index holds as many documents as the meta says. Never raises."""
    from shared.index_format import read_index_format
    try:
        db_path = os.path.join(gen_dir, DB_DIRNAME)
        if not os.path.isdir(db_path):
            return False, "no editions index", None, None
        meta, payload = read_generation(gen_dir)
        if meta is None:
            return False, "no editions meta", None, None
        marker = read_index_format(db_path) or {}
        ids = {marker.get("build_id"), meta.get("build_id"), (payload or {}).get("build_id")}
        if len(ids) != 1 or None in ids:
            return False, "the index, its browse map and its meta are of different builds", meta, payload
        if not isinstance((payload or {}).get("map"), dict):
            return False, "the editions browse map is unreadable", meta, payload
        import tantivy

        from shared.search_tokenizer import register_search_tokenizers
        index = tantivy.Index.open(db_path)
        register_search_tokenizers(index)
        for name in REQUIRED_FIELDS:
            try:
                index.parse_query(f'{name}:"x"', [name])
            except Exception:  # the parser refuses a field the schema does not have
                return False, f"the editions index has no field {name}", meta, payload
        n = index.searcher().num_docs
        expected = (meta.get("counts") or {}).get("documents")
        if n != expected:
            return False, f"{n:,} documents in the editions index, its meta says {expected}", meta, payload
        return True, "", meta, payload
    except MemoryError:
        raise
    except Exception as e:
        LOGGER.warning("Editions generation %s not readable: %r", gen_dir, e)
        return False, f"unreadable: {e!r}", None, None


def open_editions_engine(meta_mgr, var_mgr, gen_dir: Optional[str] = None):
    """The leaf engine on the generation at *gen_dir* (default ``<INDEX_DIR>/Editions``), or None
    when it is not ready (logged once with the reason)."""
    gen_dir = gen_dir or default_generation_dir()
    ready, reason, meta, payload = check_generation(gen_dir)
    if not ready:
        if os.path.exists(gen_dir):
            LOGGER.warning("Editions index at %s not opened: %s", gen_dir, reason)
        return None
    from shared.search_engine import SearchEngine
    engine = SearchEngine(meta_mgr, var_mgr, worker_mode=True, open_local=False,
                          index_path=os.path.join(gen_dir, DB_DIRNAME), edition_leaf=True)
    if engine.searcher is None:
        return None
    engine.set_edition_generation(gen_dir, meta, payload["map"])
    return engine


# --- the source choice ---------------------------------------------------------------------

def normalize_sources(value: Optional[str]) -> str:
    """The source choice a search runs with: ``automatic`` when none is given (today's search),
    else one of ``SOURCES``; anything else is refused."""
    if value is None or value == "":
        return SOURCES_AUTOMATIC
    value = str(value).strip().lower()
    if value not in SOURCES:
        raise ValueError(f"sources must be one of {', '.join(SOURCES)}")
    return value


# --- identities ----------------------------------------------------------------------------

def is_edition_uid(uid: Any) -> bool:
    return isinstance(uid, str) and uid.startswith(UID_PREFIX + ":")


def is_edition_header(header: Any) -> bool:
    return isinstance(header, str) and _HEADER_RE.match(header) is not None


def parse_edition_uid(uid: str) -> Optional[Tuple[str, str, Optional[str]]]:
    """``(kind, id, sys_id or None)`` of ``ED:fgp:<id>`` / ``ED:pgp:<id>:<sys_id>``."""
    m = _UID_RE.match(uid or "")
    return (m.group(1), m.group(2), m.group(3)) if m else None


def parse_edition_header(header: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
    """``(sys_id, kind, id, page uid or None)`` of ``<sys_id>_ED_<kind>_<id>[_<page uid>]``."""
    m = _HEADER_RE.match(header or "")
    return (m.group(1), m.group(2), m.group(3), m.group(4)) if m else None


def edition_version(kind: str, edition_id: Any) -> str:
    """The version id the chooser and the browse link use: ``fgp:<id>`` / ``pgp:<id>``."""
    return f"{kind}:{edition_id}"


def edition_record(uid: str, sys_id: str, *, page_uid: Optional[str], span: Tuple[int, int],
                   cross_page: bool, entry: Optional[Dict[str, Any]] = None,
                   generation: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The ``edition`` record of an edition row (plan §3.4): consumers branch on it, never on uid
    syntax. *page_uid*: the main index's page the match starts on, None when unlocated. *span*:
    the match in the indexed search text (the viewer maps it to the original through the
    versioned cleaner). *entry*: the edition's browse-map entry (credit, also_ids)."""
    kind, eid, _sys = parse_edition_uid(uid) or ("", "", None)
    entry = entry or {}
    generation = generation or {}
    return {
        "kind": kind,
        "id": eid,
        "uid": uid,
        "version": edition_version(kind, eid),
        "sys_id": str(sys_id),
        "located": page_uid is not None,
        "page_uid": page_uid,
        "span": [int(span[0]), int(span[1])],
        "cross_page": bool(cross_page),
        "credit": dict(entry.get("credit") or {}) or None,
        "also_ids": list(entry.get("also_ids") or []),
        "cleaner_version": generation.get("cleaner_version"),
        "build_id": generation.get("build_id"),
    }


def member_header(uid: str, sys_id: str, page_uid: Optional[str]) -> str:
    """A member's header: the edition's, with the page it is shown on when that is known (never
    a page zero)."""
    kind, eid, _sys = parse_edition_uid(uid) or ("", "", None)
    return edition_header(kind, eid, sys_id, page_uid)


def part_index_at(boundaries: Sequence[Dict[str, Any]], offset: int) -> Optional[int]:
    """The part of an edition holding character *offset* (strict: the part that contains it, never
    a fallback to the first part), or None."""
    for i, b in enumerate(boundaries):
        if b.get("start", 0) <= offset < b.get("end", 0):
            return i
    return None


def key_text(content: str, boundaries: Sequence[Dict[str, Any]], key: str, uid: str) -> str:
    """The text of one member: the parts of the edition on page *key*, or its unlocated parts
    when *key* is the edition's own uid -- what an excluded word is looked for in (§3.3 item 14)."""
    parts = [content[b.get("start", 0):b.get("end", 0)] for b in boundaries
             if (b.get("uid") or uid) == key]
    return "\n".join(parts) if parts else content


# --- matching on the folded text -----------------------------------------------------------

# Dropped from the text the check runs on: points and cantillation marks (the maqaf, U+05BE,
# becomes a space first); with *strip_brackets*, the square brackets too.
_DROP_RE = re.compile("([\u0591-\u05BD\u05BF-\u05CF]+)")
_DROP_BRACKETS_RE = re.compile("([\u0591-\u05BD\u05BF-\u05CF\\[\\]]+)")


class MatchText:
    """An edition's text as the check runs on it, and the way back.

    ``text``: the points folded as the index folds them (``search_fold``: the maqaf a space,
    every other point or cantillation mark dropped) and, for a bracket-free query, the brackets
    out -- so a pointed edition that the index retrieves by its plain spelling is also accepted
    (1,764 editions are pointed, plan §3.9). Built with one regex pass, its kept runs recorded;
    a span maps back by binary search (an edition can be tens of thousands of characters, and
    every occurrence is mapped)."""

    __slots__ = ("original", "text", "_match_starts", "_orig_starts", "_brackets")

    def __init__(self, original: str, strip_brackets: bool):
        self.original = original
        self._brackets = strip_brackets
        work = original.replace("\u05BE", " ") if "\u05BE" in original else original
        rx = _DROP_BRACKETS_RE if strip_brackets else _DROP_RE
        self._match_starts: Optional[List[int]] = None
        self._orig_starts: Optional[List[int]] = None
        if rx.search(work) is None:
            self.text = work
            return
        # Kept and dropped runs alternate (split on a captured group); their starts are running
        # sums of their lengths -- all of it in C, not a loop over characters or runs.
        parts = rx.split(work)
        kept, dropped = parts[0::2], parts[1::2]
        kept_lens = list(map(len, kept))
        self.text = "".join(kept)
        self._match_starts = list(itertools.accumulate(kept_lens[:-1], initial=0))
        self._orig_starts = list(itertools.accumulate(
            map(operator.add, kept_lens[:-1], map(len, dropped)), initial=0))

    def to_original(self, i: int) -> int:
        """The original index of character *i* of ``text``."""
        if self._match_starts is None:
            return i
        if i >= len(self.text):
            return len(self.original)
        k = bisect.bisect_right(self._match_starts, i) - 1
        return self._orig_starts[k] + (i - self._match_starts[k])

    def to_match(self, o: int) -> int:
        """Where original index *o* falls in ``text`` (a dropped character: the next kept one)."""
        if self._match_starts is None:
            return o
        k = bisect.bisect_right(self._orig_starts, o) - 1
        if k < 0:
            return 0
        run_len = (self._match_starts[k + 1] if k + 1 < len(self._match_starts) else len(self.text)) \
            - self._match_starts[k]
        offset = o - self._orig_starts[k]
        return self._match_starts[k] + min(offset, run_len)

    def span(self, start: int, end: int) -> Tuple[int, int]:
        """The original span that ``text``'s [start, end) stands for: the same characters, with
        the points and brackets among them and the points of its last letter; a bracket pair
        closed or opened right at an edge stays whole (as ``_unstripped_span``)."""
        if self._match_starts is None:
            return start, end
        text = self.original
        s = self.to_original(start)
        if end <= start:
            return s, s
        e = self.to_original(end - 1) + 1
        while e < len(text) and unicodedata.combining(text[e]):
            e += 1
        if self._brackets:
            seg = text[s:e]
            if seg.count("[") > seg.count("]") and text[e:e + 1] == "]":
                e += 1
            if seg.count("]") > seg.count("[") and s and text[s - 1] == "[":
                s -= 1
        return s, e


# --- text positions (plan §3.3 item 11) ----------------------------------------------------

_WORD_CHAR_RE = re.compile(r"[^\W_]")
_analyzer = None


def _edition_words(line: str) -> List[str]:
    """A line's words as the editions index's position fields hold them: folded, split by the
    hebword tokenizer (``shared/editions_indexer.py::position_fields``)."""
    global _analyzer
    from shared.editions_indexer import search_fold
    if _analyzer is None:
        from shared.index_seams import hebword_analyzer
        _analyzer = hebword_analyzer()
    return list(_analyzer.analyze(search_fold(line)))


def edition_position_ok(content: str, match, text_position: Optional[str],
                        line_constraints: Optional[Dict[int, str]] = None, strip_brackets: bool = True) -> bool:
    """``Indexer._validate_position_match`` for an edition's search text, which the position
    fields hold folded and tokenizer-split: a match is at the start (end) of the text or line
    when nothing but signs, points and spaces stands before (after) it, and ``L<n>:word``
    compares the line's first (last) folded word -- so a pointed or punctuated boundary word that
    the index retrieves is accepted, never only the raw word."""
    if not text_position:
        return True

    def blank(s: str) -> bool:
        if strip_brackets:
            s = s.replace("[", "").replace("]", "")
        return _WORD_CHAR_RE.search(s) is None

    if line_constraints:
        lines = [ln for ln in content.split("\n") if ln.strip()]
        for line_num, word in line_constraints.items():
            if line_num < 1 or line_num > len(lines):
                return False
            words = _edition_words(lines[line_num - 1])
            wanted = _edition_words(word)
            if not words or not wanted:
                return False
            if text_position == "line_start" and words[0] != wanted[0]:
                return False
            if text_position == "line_end" and words[-1] != wanted[-1]:
                return False
        return True
    start, end = match.start(), match.end()
    if text_position == "start":
        return blank(content[:start])
    if text_position == "end":
        return blank(content[end:])
    if text_position == "line_start":
        before = content[:start]
        return blank(before[before.rfind("\n") + 1:])
    if text_position == "line_end":
        after = content[end:]
        nl = after.find("\n")
        return blank(after if nl < 0 else after[:nl])
    return True


# --- one result per page (plan §3.4) -------------------------------------------------------

def source_label(row: Dict[str, Any]) -> str:
    """A row's source as results name it: ``automatic``, ``local``, ``fgp_edition``, ``pgp_edition``."""
    edition = row.get("edition")
    if edition:
        return EDITION_SOURCE_LABELS.get(edition.get("kind"), "edition")
    if (row.get("display") or {}).get("source") == "LOCAL":
        return SOURCE_LOCAL_LABEL
    return SOURCE_AUTOMATIC_LABEL


def group_key(row: Dict[str, Any]) -> str:
    """The page a row is grouped under: an edition member's ``group_key`` (its page, or its own
    uid when unlocated), any other row's uid."""
    return row.get("group_key") or row.get("uid")


def group_results(main_rows: Sequence[Dict[str, Any]], edition_rows: Sequence[Dict[str, Any]],
                  local_rows: Sequence[Dict[str, Any]] = (), *, ids_only: bool = False,
                  match_count: Optional[Callable[[Dict[str, Any]], int]] = None,
                  k: int = RRF_K) -> List[Dict[str, Any]]:
    """One result per page from the two legs' rows, ordered with My Library's rows.

    *main_rows* (the main index's, after ``_deduplicate``: one per page) and *edition_rows* (the
    editions index's members, one per edition and page) are each in rank order; a row's group is
    ``group_key``. A group's members are ordered by their leg rank (``1/(k + rank)``, the automatic
    text first on a tie), and its first member is its best: the group row is that member's row
    with ``uid`` = the group key, ``members``, ``sources`` and ``edition``. Groups and *local_rows*
    (never grouped) are ordered by reciprocal rank fusion of each group's best rank in each leg.

    *match_count*: an all-star search orders by matches instead (decision 2): a group counts its
    best-counting member (``_matches``), and the rows go most matches first, then by shelfmark, by
    ``SearchEngine._order_all_star`` -- the caller sorts; here each group row gets its
    ``_matches``.

    *ids_only*: light rows, one per group key (``uid``, ``display``, ``scope``)."""
    groups: Dict[str, Dict[str, Any]] = {}
    order: List[Dict[str, Any]] = []

    def add(row: Dict[str, Any], leg: str, rank: int) -> None:
        key = group_key(row)
        g = groups.get(key)
        if g is None:
            g = groups[key] = {"key": key, "members": [], "best": {}, "local": False}
            order.append(g)
        g["members"].append((1.0 / (k + rank), 0 if leg == "main" else 1, len(g["members"]), row))
        g["best"].setdefault(leg, rank)

    for rank, row in enumerate(main_rows, start=1):
        add(row, "main", rank)
    for rank, row in enumerate(edition_rows, start=1):
        add(row, "editions", rank)
    for rank, row in enumerate(local_rows, start=1):
        order.append({"key": None, "members": [(0.0, 2, 0, row)], "best": {"local": rank}, "local": True})

    def score(g: Dict[str, Any]) -> float:
        return sum(1.0 / (k + rank) for rank in g["best"].values())

    fused = sorted(order, key=lambda g: (score(g), not g["local"]), reverse=True)
    out: List[Dict[str, Any]] = []
    for g in fused:
        if g["local"]:
            out.append(g["members"][0][3])
            continue
        members = [m[3] for m in sorted(g["members"], key=lambda m: (-m[0], m[1], m[2]))]
        best = members[0]
        if ids_only:
            out.append({"uid": g["key"], "display": dict(best.get("display") or {}),
                        "scope": best.get("scope", "page")})
            continue
        row = dict(best)
        row["uid"] = g["key"]
        row.pop("group_key", None)
        row["members"] = members
        row["sources"] = list(dict.fromkeys(source_label(m) for m in members))
        row["edition"] = best.get("edition")
        row["edition_match"] = any(m.get("edition") for m in members)
        # A group keyed by an edition's own uid: no page could be named for that hit.
        row["page_identified"] = not (best.get("edition") and g["key"] == best["edition"]["uid"])
        if match_count is not None:
            row["_matches"] = max(match_count(m) for m in members)
            for m in members:
                m.pop("_matches", None)
        out.append(row)
    return out


def results_summary(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """``{'results': N, 'with_edition': M}`` for the count line (plan §3.4): it never says
    "edition-only", which a capped automatic search cannot establish."""
    n = m = 0
    for r in rows:
        n += 1
        m += bool(r.get("edition_match"))
    return {"results": n, "with_edition": m}


# --- the edition resolver ------------------------------------------------------------------

class EditionResolver:
    """An edition's original text and credit, read from the sidecars by version (``fgp:<id>``,
    ``pgp:<id>``) or uid. ``search_text`` re-runs the versioned cleaner, so a span in the indexed
    text maps to the original's characters (``SearchText.span_to_original``); a cleaner version or
    text other than the build's means the hit is stale (plan §3.4)."""

    def __init__(self, fgp_db: Optional[str] = None, pgp_db: Optional[str] = None):
        # The sidecars the services read (their own lookup: the user's data folder first).
        if fgp_db is None:
            from shared.fgp_service import FgpService
            svc = FgpService()
            fgp_db = getattr(svc, "_db_path", None)
            svc.close()
        if pgp_db is None:
            from shared.document_service import PgpService
            svc = PgpService()
            pgp_db = getattr(svc, "_db_path", None) or getattr(svc, "db_path", None)
            close = getattr(svc, "close", None)
            if close is not None:
                close()
        self._fgp_db = fgp_db
        self._pgp_db = pgp_db

    def resolve(self, version_or_uid: str) -> Optional[Dict[str, Any]]:
        parsed = parse_edition_uid(version_or_uid) if is_edition_uid(version_or_uid) else None
        if parsed is None:
            m = re.match(r"^(fgp|pgp):(\d+)$", version_or_uid or "")
            if not m:
                return None
            parsed = (m.group(1), m.group(2), None)
        kind, eid = parsed[0], parsed[1]
        try:
            row = self._fgp(eid) if kind == SOURCE_FGP else self._pgp(eid)
        except sqlite3.Error as e:
            LOGGER.warning("Edition %s:%s not readable: %r", kind, eid, e)
            return None
        if row is None:
            return None
        st = search_text(row["original"], kind, genuzot_marks=row.pop("genuzot", False))
        row.update(kind=kind, id=eid, version=edition_version(kind, eid), search=st,
                   search_text=st.text, cleaner_version=CLEANER_VERSION)
        return row

    def _fgp(self, eid: str) -> Optional[Dict[str, Any]]:
        if not self._fgp_db or not os.path.exists(self._fgp_db):
            return None
        from shared.fgp_marks_legend import uses_genuzot_marks
        from shared.fgp_service import pick_fgp_credit
        con = sqlite3.connect(f"file:{self._fgp_db}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        try:
            r = con.execute("SELECT * FROM fgp_transcriptions WHERE id = ?", (int(eid),)).fetchone()
        finally:
            con.close()
        if r is None:
            return None
        src = dict(r)
        src["is_fgp"] = True
        return {"original": src.get("content") or "", "sys_id": str(src.get("sys_id") or ""),
                "credit": {"en": pick_fgp_credit(src, "en"), "he": pick_fgp_credit(src, "he")},
                "genuzot": uses_genuzot_marks(src)}

    def _pgp(self, eid: str) -> Optional[Dict[str, Any]]:
        if not self._pgp_db or not os.path.exists(self._pgp_db):
            return None
        con = sqlite3.connect(f"file:{self._pgp_db}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        try:
            r = con.execute("SELECT content, source_scholar, pgpid FROM document_sources WHERE id = ?",
                            (int(eid),)).fetchone()
        finally:
            con.close()
        if r is None:
            return None
        scholar = str(r["source_scholar"] or "").strip() or None
        return {"original": r["content"] or "", "pgpid": r["pgpid"],
                "credit": {"en": scholar, "he": scholar}}


# --- promotion -----------------------------------------------------------------------------

def promote_with_lease(new_dir: str, live_dir: Optional[str] = None) -> None:
    """Promote *new_dir* to the live generation under the exclusive lease the research workers'
    read leases block (plan §3.7). A process that holds the index for its lifetime closes it
    first (``SearchEngine.close_editions``)."""
    from shared.editions_indexer import promote_generation
    from shared.local_index_leases import index_leases
    live_dir = live_dir or default_generation_dir()
    with index_leases([live_dir], exclusive=True):
        promote_generation(new_dir, live_dir)
