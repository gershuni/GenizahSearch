# -*- coding: utf-8 -*-
"""The editions index: FGP and PGP scholarly editions as a second Tantivy index.

Editions search plan (private repository), §3.1 and §3.7, PR 1: this module builds one
**generation** of the index from the two sidecars and nothing serves it yet. A generation is
a directory holding

* ``tantivy_db/`` -- the main index's schema (``build_main_schema``), one document per
  searchable edition text, with ``index_format.json`` inside it (reversed words, no seams);
* ``editions_browse_map.pkl`` -- sys_id -> that manuscript's edition documents;
* ``editions_meta.json`` -- written last: the build id shared by all three, the inputs'
  versions and fingerprints, the counts and the cleaner's rule counts.

**The canonical set** is a written rule that ``scripts/verify_editions_index.py`` recomputes
on its own:

* FGP: the ``Digital Edition`` sources the version chooser keeps for the manuscript
  (``FgpService.chooser_sources``: its manuscript-wide deduplication, no page filter); of two
  whose search text is identical, the lower id is indexed and the other listed in its
  ``also_ids``.
* PGP: every ``Edition`` source that is not a translation, once per manuscript it is linked
  to (``document_fragments``), so a joined document is found on each fragment.

**Locating.** Every document carries ``boundaries`` in the format of the main index's
aggregate documents, covering its whole search text: each part names the main index's page
(``uid`` copied verbatim from the main browse map) or ``None`` when no page can be named.
Only the chooser's confident cases are located, and only in a manuscript with one image set
(IE):

* an FGP per-image row whose C-number is the FGP image number of exactly one NLI image, in a
  manuscript with as many pages as images;
* an FGP whole-document row's sections, each on the page its number names;
* a PGP text whose fragment link says recto or verso, in a two-page manuscript, when the
  text has no recto/verso markers of its own (the chooser's rule);
* a PGP text on one fragment whose sections carry canvas numbers, each section on that page;
  or, in a two-page manuscript, sections labelled recto and verso;
* a text with no image, side or located section of its own, in a manuscript of one page.

Everything else is unlocated, and its hits stay results of their own (plan §3.4).
"""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import pickle
import re
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from shared.edition_search_text import CLEANER_VERSION, SOURCE_FGP, SOURCE_PGP, SearchText, search_text

LOGGER = logging.getLogger("genizah." + __name__)

BUILDER_VERSION = 2   # 2: each edition's credit in the browse map
EDITIONS_DIRNAME = "Editions"
DB_DIRNAME = "tantivy_db"
BROWSE_MAP_FILENAME = "editions_browse_map.pkl"
META_FILENAME = "editions_meta.json"
INDEX_SOURCES = {SOURCE_FGP: "FGP", SOURCE_PGP: "PGP"}
UID_PREFIX = "ED"
#: Fields every editions index must have (the readiness check reads the real schema).
REQUIRED_FIELDS = ("unique_id", "content", "content_search", "content_head", "content_tail",
                   "line_starts", "line_ends", "source", "full_header", "shelfmark", "scope",
                   "boundaries")


class BuildCancelled(Exception):
    """A build stopped by its caller's *should_stop* (the desktop's background build, at exit
    or when the main index is rebuilt). What it wrote is left for the caller to remove."""


def _check_stop(should_stop: Optional[Callable[[], bool]]) -> None:
    if should_stop is not None and should_stop():
        raise BuildCancelled("the editions build was stopped")


def default_generation_dir() -> str:
    """``<INDEX_DIR>/Editions``, beside My Library's ``LocalIndex``. ``GENIZAH_EDITIONS_DIR``
    (dev and gates only: scripts/verify_editions_search.py --web-timing) names another; read at
    each call, so research workers started with it read the same generation."""
    override = os.environ.get("GENIZAH_EDITIONS_DIR")
    if override:
        return override
    from shared.config import Config
    return os.path.join(Config.INDEX_DIR, EDITIONS_DIRNAME)


def edition_uid(kind: str, edition_id: Any, sys_id: Optional[str] = None) -> str:
    """``ED:fgp:<id>`` / ``ED:pgp:<id>:<sys_id>`` (a PGP text is indexed once per fragment)."""
    if kind == SOURCE_PGP:
        return f"{UID_PREFIX}:{kind}:{edition_id}:{sys_id}"
    return f"{UID_PREFIX}:{kind}:{edition_id}"


def edition_header(kind: str, edition_id: Any, sys_id: str, page_uid: Optional[str] = None) -> str:
    """``<sys_id>_ED_<kind>_<id>``, plus ``_<IE>_P<n>_FL<k>`` when the whole text lies on one
    known page. An unknown page is never written as page zero."""
    header = f"{sys_id}_{UID_PREFIX}_{kind}_{edition_id}"
    return f"{header}_{page_uid}" if page_uid else header


# --- documents ---------------------------------------------------------------------------

@dataclass
class EditionDoc:
    kind: str
    edition_id: Any
    sys_id: str
    search: SearchText
    shelfmark: str = ""
    also_ids: List[Any] = field(default_factory=list)
    boundaries: List[Dict[str, Any]] = field(default_factory=list)
    #: Who made the edition, as the version chooser credits it: ``{'en': ..., 'he': ...}``.
    credit: Dict[str, Optional[str]] = field(default_factory=dict)

    @property
    def uid(self) -> str:
        return edition_uid(self.kind, self.edition_id, self.sys_id)

    @property
    def page_uid(self) -> Optional[str]:
        """The page when every located part is on one page and nothing is unlocated."""
        uids = {b.get("uid") for b in self.boundaries}
        return next(iter(uids)) if len(uids) == 1 and None not in uids else None

    @property
    def header(self) -> str:
        return edition_header(self.kind, self.edition_id, self.sys_id, self.page_uid)


class PageLocator:
    """Names the main index's page for a part of an edition, from the main browse map and
    the NLI image list; None whenever the answer is not certain."""

    def __init__(self, browse_map: Dict[Any, List[Dict[str, Any]]],
                 folio_images_for: Optional[Callable[[str], List[Dict[str, Any]]]] = None):
        self._map = {str(k): v for k, v in (browse_map or {}).items()}
        self._folio_images_for = folio_images_for

    def pages(self, sys_id: str) -> List[Dict[str, Any]]:
        """The manuscript's pages, when they form one image set (one IE); else []."""
        pages = self._map.get(str(sys_id)) or []
        ies = {p.get("ie_id") or _ie_of(p.get("uid")) for p in pages}
        return list(pages) if len(ies) == 1 else []

    def only_page(self, sys_id: str) -> Optional[Dict[str, Any]]:
        """The manuscript's page when it has exactly one: a text with no side or image of
        its own can only be on it."""
        pages = self.pages(sys_id)
        return pages[0] if len(pages) == 1 else None

    def page_at(self, sys_id: str, number: Any) -> Optional[Dict[str, Any]]:
        pages = self.pages(sys_id)
        try:
            n = int(number)
        except (TypeError, ValueError):
            return None
        return pages[n - 1] if 1 <= n <= len(pages) else None

    def page_of_fgp_image(self, sys_id: str, image_number: str, folio_label: str) -> Optional[Dict[str, Any]]:
        """The page shown for an FGP image: the NLI image whose FGP image number (else folio
        label) is the row's, in a manuscript with exactly one page per image."""
        if self._folio_images_for is None:
            return None
        pages = self.pages(sys_id)
        try:
            images = self._folio_images_for(str(sys_id)) or []
        except Exception:  # an unreadable crossref: unlocated, never an error
            LOGGER.warning("NLI folio images unavailable for %s", sys_id, exc_info=True)
            return None
        if not pages or len(pages) != len(images):
            return None
        hits: List[int] = []
        if image_number:
            hits = [i for i, img in enumerate(images)
                    if str(img.get("fgp_image_number_id") or "").strip() == image_number]
        elif folio_label:
            hits = [i for i, img in enumerate(images)
                    if str(img.get("folio_label") or "").strip().lower() == folio_label]
        return pages[hits[0]] if len(hits) == 1 else None


def _ie_of(uid: Optional[str]) -> Optional[str]:
    m = re.match(r"(IE\d+)_", uid or "")
    return m.group(1) if m else None


def _boundary(start: int, end: int, page: Optional[Dict[str, Any]], kind: str, sys_id: str) -> Dict[str, Any]:
    return {
        "start": start, "end": end,
        "uid": page.get("uid") if page else None,
        "p_num": page.get("p_num") if page else None,
        "full_header": page.get("full_header", "") if page else "",
        "source": INDEX_SOURCES[kind],
        "sys_id": str(sys_id),
    }


def _cover(parts: List[Tuple[int, int, Optional[Dict[str, Any]]]], length: int, kind: str,
           sys_id: str) -> List[Dict[str, Any]]:
    """Boundaries covering ``[0, length)``: the located *parts* (non-overlapping, in order),
    the gaps between them as unlocated parts."""
    out: List[Dict[str, Any]] = []
    pos = 0
    for start, end, page in sorted(parts, key=lambda p: p[0]):
        if start < pos or end <= start:
            continue
        if start > pos:
            out.append(_boundary(pos, start, None, kind, sys_id))
        out.append(_boundary(start, end, page, kind, sys_id))
        pos = end
    if pos < length or not out:
        out.append(_boundary(pos, length, None, kind, sys_id))
    return out


def _section_parts(st: SearchText, sections: Any, page_for: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]):
    """Search-text parts for each section whose text is found in the edition."""
    parts = []
    original = st.original
    cursor = 0
    for sec in sections or []:
        if not isinstance(sec, dict):
            continue
        text = sec.get("text") or sec.get("content")
        if not isinstance(text, str) or not text.strip():
            continue
        at = original.find(text, cursor)
        if at < 0:
            at = original.find(text)
        if at < 0:
            continue
        cursor = at + len(text)
        page = page_for(sec)
        span = st.span_from_original(at, at + len(text))
        if page is not None and span is not None:
            parts.append((span[0], span[1], page))
    return parts


# --- FGP ---------------------------------------------------------------------------------

def _connect_ro(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def fgp_documents(fgp_db: str, locator: PageLocator, rules: Optional[Counter] = None,
                  should_stop: Optional[Callable[[], bool]] = None) -> Iterator[EditionDoc]:
    """Every FGP edition document, by the canonical rule (module docstring). *should_stop*:
    asked before each manuscript; True raises BuildCancelled."""
    from shared.fgp_marks_legend import uses_genuzot_marks
    from shared.fgp_service import (FgpService, _fgp_match_folio, _fgp_match_image_number, pick_fgp_credit,
                                    source_relation_kind)

    service = FgpService(db_path=fgp_db, thread_safe=False)
    if not service.is_available():
        raise RuntimeError(f"FGP sidecar not readable: {fgp_db}")
    try:
        con = _connect_ro(fgp_db)
        try:
            rows = con.execute("SELECT DISTINCT sys_id FROM fgp_transcriptions "
                               "WHERE sys_id IS NOT NULL AND sys_id <> '' ORDER BY sys_id").fetchall()
            sys_ids = [str(r[0]) for r in rows]
        finally:
            con.close()
        for sys_id in sys_ids:
            _check_stop(should_stop)
            editions = [s for s in service.chooser_sources(sys_id)
                        if source_relation_kind(s) == "edition" and (s.get("content") or "").strip()]
            by_text: Dict[str, EditionDoc] = {}
            for src in sorted(editions, key=lambda s: int(s.get("id") or 0)):
                st = search_text(src.get("content"), SOURCE_FGP, genuzot_marks=uses_genuzot_marks(src))
                if not st.text.strip():
                    continue
                if rules is not None:
                    for name in st.rules:
                        rules[name] += 1
                twin = by_text.get(st.text)
                if twin is not None:
                    twin.also_ids.append(src.get("id"))
                    continue
                doc = EditionDoc(kind=SOURCE_FGP, edition_id=src.get("id"), sys_id=sys_id, search=st,
                                 shelfmark=str(src.get("full_shelfmark") or ""),
                                 credit={"en": pick_fgp_credit(src, "en"), "he": pick_fgp_credit(src, "he")})
                image_number = _fgp_match_image_number(src)
                folio = _fgp_match_folio(src)
                if image_number or folio:
                    page = locator.page_of_fgp_image(sys_id, image_number, folio)
                    parts = [(0, len(st.text), page)] if page else []
                else:
                    parts = _section_parts(
                        st, src.get("sections"),
                        lambda sec: locator.page_at(sys_id, sec.get("canvas_num", sec.get("page_num"))))
                    only = locator.only_page(sys_id)
                    if not parts and only is not None:
                        parts = [(0, len(st.text), only)]
                doc.boundaries = _cover(parts, len(st.text), SOURCE_FGP, sys_id)
                by_text[st.text] = doc
            yield from by_text.values()
    finally:
        service.close()


# --- PGP ---------------------------------------------------------------------------------

def pgp_canonical_pairs(con: sqlite3.Connection) -> List[sqlite3.Row]:
    """The PGP (source, fragment) pairs to index: the sources the chooser counts as editions
    (``"Edition"`` in the relation, so "Edition ; Translation" too: ``source_relation_kind``),
    with content, once per linked sys_id."""
    from shared.fgp_service import source_relation_kind
    rows = con.execute(
        "SELECT s.id AS id, s.pgpid AS pgpid, s.doc_relation AS doc_relation, s.content AS content, "
        "s.source_scholar AS source_scholar, "
        "s.sections AS sections, f.sys_id AS sys_id, f.page_info AS page_info, f.shelfmark AS shelfmark "
        "FROM document_sources s JOIN document_fragments f ON f.document_id = s.pgpid "
        "WHERE f.sys_id IS NOT NULL AND f.sys_id <> '' AND s.content IS NOT NULL AND s.content <> '' "
        "ORDER BY s.id, f.sys_id").fetchall()
    out = []
    seen = set()
    for r in rows:
        if source_relation_kind({"doc_relation": r["doc_relation"]}) != "edition":
            continue
        key = (r["id"], str(r["sys_id"]))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def pgp_documents(pgp_db: str, locator: PageLocator, rules: Optional[Counter] = None,
                  should_stop: Optional[Callable[[], bool]] = None) -> Iterator[EditionDoc]:
    """Every PGP edition document, by the canonical rule (module docstring). *should_stop*:
    asked before each text; True raises BuildCancelled."""
    from shared.document_service import parse_transcription_sections

    con = _connect_ro(pgp_db)
    try:
        pairs = pgp_canonical_pairs(con)
        fragments_of: Dict[Any, set] = defaultdict(set)
        for r in con.execute("SELECT document_id, sys_id FROM document_fragments "
                             "WHERE sys_id IS NOT NULL AND sys_id <> ''"):
            fragments_of[r[0]].add(str(r[1]))
    finally:
        con.close()
    cache: Dict[Any, SearchText] = {}
    for r in pairs:
        _check_stop(should_stop)
        sys_id = str(r["sys_id"])
        st = cache.get(r["id"])
        if st is None:
            st = search_text(r["content"], SOURCE_PGP)
            cache[r["id"]] = st
            if rules is not None:
                for name in st.rules:
                    rules[name] += 1
        if not st.text.strip():
            continue
        scholar = str(r["source_scholar"] or "").strip() or None
        doc = EditionDoc(kind=SOURCE_PGP, edition_id=r["id"], sys_id=sys_id, search=st,
                         shelfmark=str(r["shelfmark"] or ""), credit={"en": scholar, "he": scholar})
        try:
            sections = json.loads(r["sections"]) if r["sections"] else None
        except (TypeError, ValueError):
            sections = None
        single_fragment = len(fragments_of.get(r["pgpid"], ())) == 1
        pages = locator.pages(sys_id)
        side = (r["page_info"] or "").strip().lower()
        parts: List[Tuple[int, int, Optional[Dict[str, Any]]]] = []
        parsed = parse_transcription_sections(r["content"] or "")
        has_both = bool(parsed.get("recto")) and bool(parsed.get("verso"))
        if side in ("recto", "verso") and not has_both:
            if len(pages) == 2:
                parts = [(0, len(st.text), pages[0] if side == "recto" else pages[1])]
        elif single_fragment and isinstance(sections, list) and sections:
            if any(isinstance(s, dict) and s.get("canvas_num") for s in sections):
                parts = _section_parts(st, sections, lambda sec: locator.page_at(sys_id, sec.get("canvas_num")))
            elif len(pages) == 2:
                parts = _section_parts(st, sections, lambda sec: _side_page(sec, pages))
        if not parts and single_fragment and side in ("", "recto and verso") and len(pages) == 1:
            parts = [(0, len(st.text), pages[0])]
        doc.boundaries = _cover(parts, len(st.text), SOURCE_PGP, sys_id)
        yield doc


def _side_page(section: Dict[str, Any], pages: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    label = str(section.get("label") or "").strip().lower().strip("():")
    if label == "recto":
        return pages[0]
    if label == "verso":
        return pages[1]
    return None


# --- the generation ----------------------------------------------------------------------

def file_fingerprint(path: Optional[str]) -> Optional[Dict[str, Any]]:
    """sha256 and size of an input file, or None when there is none."""
    if not path or not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "bytes": os.path.getsize(path)}


def _sidecar_meta(path: str, table: str, keys: Sequence[str]) -> Dict[str, Any]:
    con = _connect_ro(path)
    try:
        rows = dict(con.execute(f"SELECT key, value FROM {table}").fetchall())
    finally:
        con.close()
    return {k: rows.get(k) for k in keys}


def input_stamp(fgp_db: str, pgp_db: str, browse_map_path: str, nli_db: Optional[str],
                fingerprint: Callable[[Optional[str]], Optional[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """The inputs a generation is built from, as recorded in its meta: a change in any of
    them is a reason to rebuild (plan §3.7). *fingerprint*: file_fingerprint, or one that
    gives the same answer from a cache (the desktop's check at each start)."""
    fingerprint = fingerprint or file_fingerprint
    return {
        "fgp": dict(_sidecar_meta(fgp_db, "fgp_meta", ("version", "built_at", "rows", "built_from_gate3_digest")),
                    file=fingerprint(fgp_db)),
        "pgp": dict(_sidecar_meta(pgp_db, "meta", ("version", "upstream_commit", "imported_at")),
                    file=fingerprint(pgp_db)),
        "browse_map": fingerprint(browse_map_path),
        "nli_crossref": fingerprint(nli_db),
        "cleaner_version": CLEANER_VERSION,
        "builder_version": BUILDER_VERSION,
    }


#: The inputs a generation records (input_stamp), in the order a drift report names them.
INPUT_KEYS = ("fgp", "pgp", "browse_map", "nli_crossref", "cleaner_version", "builder_version")


def changed_inputs(recorded: Optional[Dict[str, Any]], installed: Dict[str, Any]) -> List[str]:
    """The keys of INPUT_KEYS whose installed value is not the one a generation was built
    from (*recorded*: its meta's ``inputs``): a sidecar by its file (sha256 and size; its
    version rows describe the file), anything else as it is. Empty: the generation is
    current (plan section 3.7)."""
    recorded = recorded or {}

    def fingerprint(value):
        return value.get("file") if isinstance(value, dict) and "file" in value else value
    return [key for key in INPUT_KEYS if fingerprint(recorded.get(key)) != fingerprint(installed.get(key))]


def search_fold(text: str) -> str:
    """The folded form the editions index retrieves by: ``strip_search_diacritics`` (as the
    main index's ``content_search``) and, unlike the main index, the points too, since 1,764
    editions are pointed (2026-11-15) and a query is not. The maqaf becomes a space first so
    ``על־פי`` stays two words. Only derived, unstored fields use it: ``content`` keeps every
    mark, and the engine's check looks past marks."""
    from shared.text_normalize import strip_nikud, strip_search_diacritics
    return strip_nikud(strip_search_diacritics(text.replace("־", " ")))


#: What the build reads from the NLI crossref (``NliCrossrefService.get_images``).
NLI_IMAGE_COLUMNS = ("NLI_AlmaId", "FGPImageNumberId", "FGPNumber", "ImageName", "ImageSourceName", "Shelfmark")


def check_nli_crossref(path: str) -> None:
    """Raise ValueError unless *path* is a readable NLI crossref with its image table and
    rows: the service swallows query errors and answers ``[]``, which would leave every FGP
    per-image text unplaced without a word (Codex review of PR 1)."""
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nli_images)")}
            missing = [c for c in NLI_IMAGE_COLUMNS if c not in cols]
            if missing:
                raise ValueError(f"NLI crossref {path}: table nli_images lacks {', '.join(missing)}")
            if con.execute("SELECT 1 FROM nli_images LIMIT 1").fetchone() is None:
                raise ValueError(f"NLI crossref {path}: table nli_images is empty")
        finally:
            con.close()
    except sqlite3.Error as e:
        raise ValueError(f"NLI crossref {path} is not a readable SQLite database: {e}") from e


def position_fields(text: str, analyzer, fold: Callable[[str], str], head_words: int = 10) -> Dict[str, str]:
    """The head, tail and line fields of a search text, from its folded, tokenizer-split
    words (plan §3.1): a folded query then finds a pointed or punctuated first or last word.
    Lines are numbered as the text's non-empty lines are, so ``L<n>:`` agrees with the
    content even where a line holds no word."""
    words_by_line = []
    for line in text.split("\n"):
        if line.strip():
            words_by_line.append(list(analyzer.analyze(fold(line))))
    words = [w for line in words_by_line for w in line]
    ls_parts: List[str] = []
    le_parts: List[str] = []
    for n, line_words in enumerate(words_by_line, 1):
        if line_words:
            ls_parts += [line_words[0], f"L{n}:{line_words[0]}"]
            le_parts += [line_words[-1], f"L{n}:{line_words[-1]}"]
    return {
        "content_head": " ".join(words[:head_words]),
        "content_tail": " ".join(words[-head_words:]),
        "line_starts": " ".join(ls_parts),
        "line_ends": " ".join(le_parts),
    }


def build_generation(out_dir: str, *, fgp_db: str, pgp_db: str, browse_map_path: str,
                     nli_db: Optional[str] = None,
                     folio_images_for: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
                     documents: Optional[Iterable[EditionDoc]] = None,
                     force: bool = False, progress: Optional[Callable[[str], None]] = None,
                     should_stop: Optional[Callable[[], bool]] = None) -> Dict[str, Any]:
    """Build one generation into *out_dir* and return its meta.

    *folio_images_for* defaults to the NLI crossref at *nli_db*. *documents* replaces the
    canonical documents (the gate's fault injections and tests); the meta then still
    records the real inputs, so a build from other documents fails the membership check.
    *should_stop* (the desktop's background build): asked as the documents are read and
    written; True raises BuildCancelled, before the meta is written, so what is left in
    *out_dir* never reads as a generation.

    The meta records the inputs as they were **before** anything was read from them: an
    input replaced while the build reads (the desktop's sidecar update) then leaves the
    generation stale against the new file, never stamped with it (Codex review of PR #25).
    """
    import tantivy

    from shared.index_format import write_index_format
    from shared.index_seams import REVERSED_WORDS_FIELD, hebword_analyzer, reversed_words
    from shared.indexer import build_main_schema
    from shared.search_tokenizer import register_search_tokenizers

    say = progress or (lambda msg: LOGGER.info(msg))
    if folio_images_for is None:
        if not nli_db:
            raise ValueError("an NLI crossref (nli_db) is required to place FGP texts on their pages")
        check_nli_crossref(nli_db)
    stamp = input_stamp(fgp_db, pgp_db, browse_map_path, nli_db)
    if os.path.exists(out_dir):
        if not force:
            raise FileExistsError(f"{out_dir} exists; pass force=True to replace it")
        shutil.rmtree(out_dir)
    db_path = os.path.join(out_dir, DB_DIRNAME)
    os.makedirs(db_path)

    with open(browse_map_path, "rb") as f:
        browse_map = pickle.load(f)
    if folio_images_for is None:
        from shared.nli_crossref_service import NliCrossrefService
        nli = NliCrossrefService(db_path=nli_db)
        folio_images_for = nli.get_folio_images
    locator = PageLocator(browse_map, folio_images_for)

    rules = {SOURCE_FGP: Counter(), SOURCE_PGP: Counter()}
    if documents is None:
        say("reading FGP editions")
        docs = list(fgp_documents(fgp_db, locator, rules[SOURCE_FGP], should_stop))
        say("reading PGP editions")
        docs += list(pgp_documents(pgp_db, locator, rules[SOURCE_PGP], should_stop))
    else:
        docs = list(documents)

    schema = build_main_schema(reversed_words=True)
    index = tantivy.Index(schema, path=db_path)
    register_search_tokenizers(index)
    analyzer = hebword_analyzer()
    writer = index.writer(heap_size=512 * 1024 * 1024)
    edition_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    counts = Counter()
    located = {SOURCE_FGP: Counter(), SOURCE_PGP: Counter()}
    say(f"indexing {len(docs):,} documents")
    for n, doc in enumerate(docs):
        if n % 500 == 0:
            _check_stop(should_stop)
        text = doc.search.text
        folded = search_fold(text)
        fields = {
            "unique_id": doc.uid,
            "content": text,
            "content_search": folded,
            REVERSED_WORDS_FIELD: reversed_words(folded, analyzer),
            "source": INDEX_SOURCES[doc.kind],
            "full_header": doc.header,
            "shelfmark": doc.shelfmark,
            "scope": "page",
            "boundaries": json.dumps(doc.boundaries, ensure_ascii=False),
        }
        fields.update(position_fields(text, analyzer, search_fold))
        writer.add_document(tantivy.Document(**fields))
        counts[doc.kind] += 1
        state = {u is not None for u in (b["uid"] for b in doc.boundaries)}
        located[doc.kind]["all" if state == {True} else "part" if True in state else "none"] += 1
        edition_map[doc.sys_id].append({
            "uid": doc.uid, "full_header": doc.header, "source": INDEX_SOURCES[doc.kind],
            "edition_id": doc.edition_id, "also_ids": list(doc.also_ids), "page_uid": doc.page_uid,
            "credit": dict(doc.credit),
        })
    writer.commit()
    writer.wait_merging_threads()
    _check_stop(should_stop)

    marker = write_index_format(db_path, reversed_field=True, seams=None)
    build_id = marker["build_id"]
    with open(os.path.join(out_dir, BROWSE_MAP_FILENAME), "wb") as f:
        pickle.dump({"build_id": build_id, "map": dict(edition_map)}, f)
    meta = {
        "build_id": build_id,
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "inputs": stamp,
        "counts": {
            "documents": sum(counts.values()),
            "documents_by_source": {k: counts[k] for k in (SOURCE_FGP, SOURCE_PGP)},
            "manuscripts": len(edition_map),
            "also_ids": sum(len(d.also_ids) for d in docs),
            "located": {k: dict(v) for k, v in located.items()},
        },
        "rules_texts": {k: dict(v) for k, v in rules.items()},
    }
    tmp = os.path.join(out_dir, META_FILENAME + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    os.replace(tmp, os.path.join(out_dir, META_FILENAME))
    say(f"built {meta['counts']['documents']:,} documents for {meta['counts']['manuscripts']:,} manuscripts")
    return meta


def read_generation(gen_dir: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """``(meta, browse map payload)`` of a generation, or None for what is missing."""
    meta = None
    try:
        with open(os.path.join(gen_dir, META_FILENAME), "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (OSError, ValueError):
        pass
    payload = None
    try:
        with open(os.path.join(gen_dir, BROWSE_MAP_FILENAME), "rb") as f:
            payload = pickle.load(f)
    except (OSError, pickle.UnpicklingError, EOFError):
        pass
    return meta, payload


def restore_previous_generation(live_dir: str) -> bool:
    """Put ``<live>.prev`` back in place of *live_dir* (a deploy's rollback, plan section 3.7).
    The generation it replaces is kept as ``<live>.failed`` (an older one is deleted first), for
    a look at what went wrong. False when there is no ``.prev`` to restore. Readers must have
    closed the live generation (the deploy stops the service first)."""
    prev, failed = live_dir + ".prev", live_dir + ".failed"
    if not os.path.isdir(prev):
        return False
    if os.path.exists(live_dir):
        if os.path.exists(failed):
            shutil.rmtree(failed)
        os.replace(live_dir, failed)
    os.replace(prev, live_dir)
    return True


def set_aside_generation(live_dir: str) -> bool:
    """Take *live_dir* out of service, kept as ``<live>.failed`` (an older one is deleted first):
    the rollback of a FIRST install, which has no ``.prev`` to put back -- the site then serves
    without editions, as before the install. False when there is nothing live."""
    failed = live_dir + ".failed"
    if not os.path.isdir(live_dir):
        return False
    if os.path.exists(failed):
        shutil.rmtree(failed)
    os.replace(live_dir, failed)
    return True


def promote_generation(new_dir: str, live_dir: str) -> None:
    """Replace *live_dir* with *new_dir*, keeping the old one as ``<live>.prev`` (an older
    ``.prev`` is deleted first). Readers must have closed the live generation (plan §3.7);
    PR 1 uses this for local builds only."""
    prev = live_dir + ".prev"
    if os.path.exists(prev):
        shutil.rmtree(prev)
    if os.path.exists(live_dir):
        os.replace(live_dir, prev)
    try:
        os.replace(new_dir, live_dir)
    except OSError:
        if os.path.exists(prev) and not os.path.exists(live_dir):
            os.replace(prev, live_dir)
        raise
