# -*- coding: utf-8 -*-
"""Read-only lookups in the Friedman dictionary sidecar (``friedman_data/friedman.db``).

M. A. Friedman's *Dictionary of Medieval Judeo-Arabic for the Geniza Documents of the India
Book and Other Texts* (Jerusalem 2016), recovered from the author's PDFs by
``scripts/friedman_units.py`` (entries, sub-entries, binyan paragraphs), ``scripts/friedman_index.py``
(the book's own index of forms) and ``scripts/build_friedman_sidecar.py`` (the file). This
module only reads that file, decides nothing about its content and never writes.

Fail-closed like ``shared/blau_service.py``: the service is READY only when the file is where a
sidecar lives, opens read-only, passes ``PRAGMA quick_check``, carries the schema version this
reader understands, was keyed by the normalizer this process runs, holds every table a lookup
reads, and carries the credit line and a status. ``is_available()`` is readiness AND the
``FRIEDMAN_DICTIONARY_ENABLED`` switch, which defaults to OFF: the sidecar's status says
"candidate_not_reviewed_not_for_production", and every lookup repeats that status so a surface
can show it. Nothing here raises into a page.

The lookup: ``normalize_ja`` the word, then ``candidate_stems`` in order (the word first, then one
proclitic layer peeled, then one pronoun suffix), each against the units' head keys; then each
candidate against the book's index of forms, whose roots reach the entries by their DOTTED root
(ג̇לב is not גלב) and only by the plain key when the dotted root reaches nothing. Every match says
how it was reached (``via``: head, stem, index) and what matched; an entry brings its sub-entries
and binyan paragraphs, a sub-entry its entry. Everything is bounded (``MAX_MATCHES``,
``MAX_CHILDREN``, ``MAX_QUOTATIONS``) so one click never renders a page. No morphology beyond
the stems: an inflected form the index does not list is not found, and the lookup says nothing
about which sense fits.

The expression search (plan step 4): ``expression(text, gap)`` takes two to ``MAX_EXPRESSION_WORDS``
words and answers with every unit holding them IN ORDER, with at most ``gap`` printed words
between each consecutive pair, inside ONE quotation or ONE head (the sidecar's ``tokens`` table,
keyed by ``shared.ja_expression.scope_keys``, which the builder, this reader and Blau's share). A hit never borrows a word from another
quotation or another unit. Each typed word matches a printed word when they share a key: the
word itself, or one of its clitic alternatives (``candidate_stems``: a conjunction, a
preposition, the article, a pronoun suffix peeled, on either side). No morphology beyond that;
no unordered matching yet. Every place says which printed word carried which typed word, and by
what (``via``: word, stem). Bounded by ``MAX_EXPRESSION_HITS`` and ``MAX_PLACES``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

from shared.blau_normalize import NORMALIZER_VERSION, candidate_stems, normalize_ja  # noqa: F401 - normalize_ja: the index builder and the tests key through this module
from shared.ja_expression import (  # noqa: F401 - the names the builders, the evaluation and the tests take from here
    DEFAULT_GAP, FUNCTION_WORDS, MAX_EXPRESSION_WORDS, MAX_GAP, MIN_EXPRESSION_STEM, NO_ARTICLE_PEEL,
    OPTIONAL_LETTERS_RE, PREPOSITIONS, PROCLITIC_RE, ExpressionComponent, ExpressionPlace, clamp_gap, components_for,
    is_function_word, lookup_key, ordered_match, preposition_stems, scope_keys, short_remainder, tiered, word_keys,
)
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "friedman-sidecar-2"
#: The versions of the pipelines whose output this reader serves; a sidecar built by another
#: unit recovery, index reading or sidecar writer is refused at open, like another schema. The
#: builders pin the same numbers (tests/test_verify_friedman_sidecar.py keeps them equal).
EXPECTED_SIDECAR_VERSION = "2"
EXPECTED_RECOVERY_VERSION = "4"
EXPECTED_INDEX_VERSION = "1"
SIDECAR_DIR = "friedman_data"
SIDECAR_FILENAME = "friedman.db"
ENV_DB_PATH = "GENIZAH_FRIEDMAN_DB_PATH"
ENV_ENABLED = "FRIEDMAN_DICTIONARY_ENABLED"
REQUIRED_TABLES = ("meta", "units", "unit_keys", "unit_roots", "quotations", "index_forms", "tokens")
MAX_MATCHES = 12
MAX_CHILDREN = 30
MAX_QUOTATIONS = 8
#: The expression search (its words, gaps and keys are shared.ja_expression's): units answered,
#: places shown per unit.
MAX_EXPRESSION_HITS = 20
MAX_PLACES = 4
SCOPE_HEAD = "head"
SCOPE_QUOTATION = "quotation"
#: The keying rules live in shared.ja_expression; these names stay for the evaluation and the tests.
_short_remainder = short_remainder
_preposition_stems = preposition_stems
_is_function_word = is_function_word


def friedman_enabled() -> bool:
    """The switch, read live; OFF unless set, because the sidecar is a candidate."""
    return os.environ.get(ENV_ENABLED, "0").strip().lower() in ("1", "true", "yes", "on")


def default_sidecar_path() -> Optional[str]:
    """Where the sidecar lives: the env var, the user's data folder, then the repository."""
    override = os.environ.get(ENV_DB_PATH, "").strip()
    if override:
        return override
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


#: A note the book sets in a head's bracket, "(ולא רק פי)", "(=לא בד)", "(ריבוי של סנג̇ה̈)": its
#: words are searchable (a note may carry a key: the variant it gives) but they must not widen the
#: gap between the head's own words, so they come AFTER them. The optional letters of (א)קצר
#: are not a note; ``word_keys`` reads them.
NOTE_RE = re.compile(r"\((?![א-ת]{1,2}\))[^()]*\)")


def _head_scope_order(head_text: str) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """The head's printed words, and the order the expression search reads them in: the words
    outside the notes first, then the notes' words (a word is a note's when it lies inside a
    bracket NOTE_RE matches)."""
    text = str(head_text or "")
    spans = [m.span() for m in NOTE_RE.finditer(text)]
    found = list(re.finditer(r"\S+", text))
    in_note = [any(a <= m.start() and m.end() <= b for a, b in spans) for m in found]
    order = [i for i, inside in enumerate(in_note) if not inside] + [i for i, inside in enumerate(in_note) if inside]
    return tuple(m.group(0) for m in found), tuple(order)


def head_scope_text(head_text: str) -> str:
    """The words of a head the expression search runs over, in this order: the head without its
    notes, then each note. The builder, the search and the evaluation all split this text."""
    words, order = _head_scope_order(head_text)
    return " ".join(words[i] for i in order)


def head_scope_map(head_text: str) -> tuple[int, ...]:
    """For each position in ``head_scope_text(head_text).split()``, the index of that word in
    ``head_text.split()``: a match found in the search order shown in the printed order."""
    return _head_scope_order(head_text)[1]


@dataclass(frozen=True)
class FriedmanQuotation:
    n: int
    quote: str
    translation: str
    locator: str
    foreign: str


@dataclass(frozen=True)
class FriedmanUnit:
    id: str
    kind: str
    letter: str
    parent: Optional[str]
    head_text: str
    head_plain: str
    root: str
    lemma: str
    binyan: str
    definition: str
    references: str
    printed_page: str
    pdf_url: str
    flags: tuple[str, ...]
    quotations: tuple[FriedmanQuotation, ...]
    quotations_total: int


@dataclass(frozen=True)
class FriedmanMatch:
    unit: FriedmanUnit
    via: str            # "head" (the word itself), "stem" (a peeled candidate), "index" (the book's index of forms)
    matched: str        # the key or index form that reached the unit
    parent: Optional[FriedmanUnit]
    children: tuple[FriedmanUnit, ...]
    children_total: int


@dataclass(frozen=True)
class FriedmanLookup:
    word: str
    key: str
    candidates: tuple[str, ...]
    matches: tuple[FriedmanMatch, ...]
    truncated: bool
    status: str
    credit_he: str

    @property
    def found(self) -> bool:
        return bool(self.matches)


def empty_lookup(word: str = "", key: str = "", candidates: tuple[str, ...] = (), status: str = "",
                 credit_he: str = "") -> FriedmanLookup:
    return FriedmanLookup(word, key, candidates, (), False, status, credit_he)


#: One typed word and the printed word that carried it; one quotation or head holding the
#: expression: shared.ja_expression's records (a head's ``quotation`` is None).
FriedmanComponent = ExpressionComponent
FriedmanPlace = ExpressionPlace


@dataclass(frozen=True)
class FriedmanExpressionHit:
    unit: FriedmanUnit
    parent: Optional[FriedmanUnit]
    places: tuple[FriedmanPlace, ...]
    places_total: int


@dataclass(frozen=True)
class FriedmanExpression:
    text: str
    words: tuple[str, ...]
    candidates: tuple[tuple[str, ...], ...]   # the keys each word answers to, in its order
    gap: int
    hits: tuple[FriedmanExpressionHit, ...]
    truncated: bool
    status: str
    credit_he: str

    @property
    def word(self) -> str:
        """The text, under the name the panel's head reads for every dictionary's answer."""
        return self.text

    @property
    def found(self) -> bool:
        return bool(self.hits)


#: What the dictionary panel receives from this service: one word's lookup, or several words' expression.
FriedmanAnswer = Union[FriedmanLookup, FriedmanExpression]


def empty_expression(text: str = "", gap: int = DEFAULT_GAP, status: str = "", credit_he: str = "",
                     words: tuple[str, ...] = (), candidates: tuple[tuple[str, ...], ...] = ()) -> FriedmanExpression:
    return FriedmanExpression(text, words, candidates, gap, (), False, status, credit_he)


def _validate(conn: Any) -> tuple[str, dict[str, str]]:
    """'' and the meta when the open file is one this reader serves; else the reason."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        return f"missing table(s): {', '.join(missing)}", {}
    meta = {str(k): ("" if v is None else str(v)) for k, v in conn.execute("SELECT key, value FROM meta")}
    if meta.get("schema") != SCHEMA_VERSION:
        return f"schema {meta.get('schema')!r}; this reader understands {SCHEMA_VERSION}", {}
    if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
        return (f"keyed by normalizer {meta.get('normalizer_version')!r}; this process runs "
                f"{NORMALIZER_VERSION}: rebuild the sidecar"), {}
    for key, expected in (("sidecar_version", EXPECTED_SIDECAR_VERSION), ("recovery_version", EXPECTED_RECOVERY_VERSION),
                          ("index_version", EXPECTED_INDEX_VERSION)):
        if meta.get(key) != expected:
            return f"{key} {meta.get(key)!r}; this reader serves {expected}: rebuild the sidecar", {}
    if not meta.get("credit_he", "").strip():
        return "no credit line (meta.credit_he)", {}
    if not meta.get("status", "").strip():
        return "no status (meta.status)", {}
    expected = meta.get("units", "")
    actual = conn.execute("SELECT COUNT(*) FROM units").fetchone()[0]
    if not expected.isdigit() or int(expected) != int(actual):
        return f"meta.units = {expected!r} but the table holds {actual} rows", {}
    check = conn.execute("PRAGMA quick_check").fetchone()
    if check is None or check[0] != "ok":
        return f"quick_check: {check[0] if check else 'no answer'}", {}
    return "", meta


def check_sidecar(path: str) -> tuple[str, dict[str, str]]:
    """'' and the meta when the file at ``path`` is one this reader serves, else the reason: the
    deploy gate (scripts/verify_friedman_sidecar.py) asks this before and after an upload."""
    if not path or not os.path.isfile(path):
        return f"no sidecar at {path!r}", {}
    try:
        conn = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return f"cannot open: {exc}", {}
    try:
        return _validate(conn)
    except sqlite3.Error as exc:
        return f"cannot read: {exc}", {}
    finally:
        conn.close()


class FriedmanService:
    """Read-only access to the Friedman sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not self._db_path:
            self.unavailable_reason = "no sidecar path"
            return
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("FriedmanService: %s", self.unavailable_reason)
            return
        conn = None
        try:
            uri = f"file:{Path(self._db_path).resolve().as_posix()}?mode=ro"
            if thread_safe:
                conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row, timeout=10.0)
            else:
                conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
                conn.row_factory = sqlite3.Row
            problem, meta = _validate(conn)
        except Exception as exc:  # a file that is not a database, a locked file, ...
            problem, meta = f"cannot open: {exc}", {}
        if problem:
            self.unavailable_reason = problem
            logger.warning("FriedmanService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("FriedmanService: serving %s (%s units, status %s)", self._db_path, meta.get("units"),
                    meta.get("status"))

    # -- state -------------------------------------------------------------

    @property
    def db_path(self) -> Optional[str]:
        return self._db_path

    def is_ready(self) -> bool:
        return self._conn is not None

    def is_available(self) -> bool:
        """Ready AND switched on. The only question a surface asks."""
        return self.is_ready() and friedman_enabled()

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    @property
    def status(self) -> str:
        return self._meta.get("status", "")

    @property
    def credit_he(self) -> str:
        return self._meta.get("credit_he", "")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self.unavailable_reason = "closed"

    # -- the lookup ----------------------------------------------------------

    def lookup(self, word: str, limit: int = MAX_MATCHES) -> FriedmanLookup:
        """What the dictionary has for one clicked or typed word. Never raises."""
        word = str(word or "").strip()
        if not word or not self.is_available():
            return empty_lookup(word, status=self.status, credit_he=self.credit_he)
        try:
            key = lookup_key(word)
            # The stems come from the cleaned key: on the raw word the PDF's presentation-form
            # harakat would split it and no clitic would be peeled (Codex, round 4 of #487).
            candidates = tuple(dict.fromkeys([key, *candidate_stems(key)])) if key else ()
            if not candidates:
                return empty_lookup(word, key, (), self.status, self.credit_he)
            return self._lookup(word, key, candidates, max(1, int(limit)))
        except Exception:
            logger.exception("FriedmanService: lookup failed for %r", word)
            return empty_lookup(word, status=self.status, credit_he=self.credit_he)

    def unit(self, unit_id: str) -> Optional[FriedmanUnit]:
        """One unit by id, with its quotations; None when unknown or unavailable."""
        if not self.is_available():
            return None
        try:
            row = self._conn.execute("SELECT * FROM units WHERE id = ?", (str(unit_id),)).fetchone()
            return self._unit(row) if row is not None else None
        except Exception:
            logger.exception("FriedmanService: unit %r failed", unit_id)
            return None

    # -- the expression search -----------------------------------------------

    def expression(self, text: str, gap: int = DEFAULT_GAP, limit: int = MAX_EXPRESSION_HITS) -> FriedmanExpression:
        """The units holding the typed words in order inside one quotation or one head, with at
        most ``gap`` printed words between consecutive words: first the units whose own head is
        the expression, then those whose head (or whose entry's head) holds its words, then the
        rest, the words standing together before gapped; an expression of function words only
        (אללה עליה) is shown in the last tier only where its words stand together. Empty (never
        raising) for fewer than two or more than ``MAX_EXPRESSION_WORDS`` words, for a word
        without Hebrew letters, or when the dictionary is not available."""
        text = " ".join(str(text or "").split())
        gap = clamp_gap(gap)
        if not text or not self.is_available():
            return empty_expression(text, gap, self.status, self.credit_he)
        words = tuple(text.split())
        candidates = tuple(word_keys(w) for w in words)
        if not 2 <= len(words) <= MAX_EXPRESSION_WORDS or not all(candidates):
            return empty_expression(text, gap, self.status, self.credit_he, words, candidates)
        try:
            return self._expression(text, words, candidates, gap, max(1, int(limit)))
        except Exception:
            logger.exception("FriedmanService: expression search failed for %r", text)
            return empty_expression(text, gap, self.status, self.credit_he, words, candidates)

    def _expression(self, text: str, words: tuple[str, ...], candidates: tuple[tuple[str, ...], ...], gap: int,
                    limit: int) -> FriedmanExpression:
        accepts = [frozenset(c) for c in candidates]
        every = sorted(set().union(*accepts))
        marks = ",".join("?" for _ in every)
        # The scopes (one quotation, one head) that hold some key of every typed word at all.
        held: dict[tuple[str, str, int], set[str]] = {}
        for unit_id, scope, n, key in self._conn.execute(
                f"SELECT DISTINCT unit_id, scope, n, key FROM tokens WHERE key IN ({marks})", every):
            held.setdefault((unit_id, scope, n), set()).add(key)
        scopes = [s for s, keys in held.items() if all(keys & a for a in accepts)]
        # Then the order and the gaps, scope by scope, over its printed words.
        places: dict[str, list[FriedmanPlace]] = {}
        for unit_id, scope, n in scopes:
            rows = self._conn.execute(
                "SELECT pos, key FROM tokens WHERE unit_id = ? AND scope = ? AND n = ? ORDER BY pos, rowid",
                (unit_id, scope, n)).fetchall()
            items = [(int(r["pos"]), str(r["key"])) for r in rows]
            match = ordered_match(items, accepts, gap)
            if match is None:
                continue
            place = self._place(unit_id, scope, n, items, match, words, candidates)
            if place is not None:
                places.setdefault(unit_id, []).append(place)
        order: dict[str, int] = {}
        parents: dict[str, Optional[str]] = {}
        if places:
            for row in self._conn.execute(f"SELECT id, ord, parent FROM units WHERE id IN ({','.join('?' for _ in places)})",
                                          list(places)):
                order[row["id"]] = int(row["ord"])
                parents[row["id"]] = row["parent"]
        # Three tiers: the expression is the unit's own head; the unit's head or its entry's head
        # holds every typed word (its quotations illustrate the expression); any other place.
        # An expression of function words only is shown in the third tier only where its words
        # stand together: anything else there was mostly noise in the review (FUNCTION_WORDS).
        keys_of: dict[str, set[str]] = {}
        if places:
            for row in self._conn.execute(f"SELECT unit_id, key FROM unit_keys WHERE unit_id IN ({','.join('?' for _ in places)})",
                                          list(places)):
                keys_of.setdefault(row["unit_id"], set()).add(row["key"])
        # The second tier: the unit's own head, or its entry's head, holds the typed words IN
        # ORDER within the widest gap, whatever gap the reader chose. A head with the words
        # reversed or scattered says nothing about the expression (Codex, round 3 of #487).
        head_units = list(set(places) | {p for p in parents.values() if p})
        head_items: dict[str, list[tuple[int, str]]] = {}
        if head_units:
            for row in self._conn.execute(
                    f"SELECT unit_id, pos, key FROM tokens WHERE scope = ? AND unit_id IN ({','.join('?' for _ in head_units)}) "
                    "ORDER BY unit_id, pos, rowid", [SCOPE_HEAD, *head_units]):
                head_items.setdefault(row["unit_id"], []).append((int(row["pos"]), str(row["key"])))
        heads_in_order = {uid for uid, items in head_items.items() if ordered_match(items, accepts, MAX_GAP) is not None}
        ranked = tiered(places, keys_of, heads_in_order, parents, candidates, order, (SCOPE_HEAD,))
        hits = []
        for unit_id, found in ranked[:limit]:
            row = self._conn.execute("SELECT * FROM units WHERE id = ?", (unit_id,)).fetchone()
            unit = self._unit(row)
            parent = None
            if unit.parent:
                prow = self._conn.execute("SELECT * FROM units WHERE id = ?", (unit.parent,)).fetchone()
                parent = self._unit(prow) if prow is not None else None
            hits.append(FriedmanExpressionHit(unit, parent, tuple(found[:MAX_PLACES]), len(found)))
        return FriedmanExpression(text, words, candidates, gap, tuple(hits), len(ranked) > limit, self.status,
                                  self.credit_he)

    def _place(self, unit_id: str, scope: str, n: int, items: list[tuple[int, str]], match: tuple[int, ...],
               words: tuple[str, ...], candidates: tuple[tuple[str, ...], ...]) -> Optional[FriedmanPlace]:
        quotation = None
        if scope == SCOPE_QUOTATION:
            q = self._conn.execute(
                "SELECT n, quote, translation, locator, foreign_text FROM quotations WHERE unit_id = ? AND n = ?",
                (unit_id, n)).fetchone()
            if q is None:
                return None
            quotation = FriedmanQuotation(q["n"], q["quote"], q["translation"], q["locator"], q["foreign_text"])
            printed = tuple(quotation.quote.split())
        else:
            row = self._conn.execute("SELECT head_text FROM units WHERE id = ?", (unit_id,)).fetchone()
            original, order = _head_scope_order(row["head_text"]) if row is not None else ((), ())
            printed = tuple(original[i] for i in order)
        found = components_for(items, match, words, candidates, printed)
        if found is None:
            return None   # the table and the text disagree: not this reader's sidecar
        positions, components = found
        if scope == SCOPE_HEAD:   # shown in the printed order, notes where the book has them
            return FriedmanPlace(scope, n, original, tuple(order[p] for p in positions), components, None,
                                 search_positions=positions)
        return FriedmanPlace(scope, n, printed, positions, components, quotation)

    def _lookup(self, word: str, key: str, candidates: tuple[str, ...], limit: int) -> FriedmanLookup:
        hits: list[tuple[sqlite3.Row, str, str]] = []
        seen: set[str] = set()

        def add(row: sqlite3.Row, via: str, matched: str) -> None:
            if row["id"] not in seen:
                seen.add(row["id"])
                hits.append((row, via, matched))

        for candidate in candidates:
            for row in self._conn.execute(
                    "SELECT u.* FROM units u JOIN unit_keys k ON k.unit_id = u.id WHERE k.key = ? ORDER BY u.ord",
                    (candidate,)):
                add(row, "head" if candidate == key else "stem", candidate)
        for candidate in candidates:
            for form in self._conn.execute(
                    "SELECT form, root_text, root_dotted, root_key FROM index_forms WHERE form_key = ? ORDER BY id",
                    (candidate,)):
                rows = []
                if form["root_dotted"]:
                    rows = self._conn.execute(
                        "SELECT u.* FROM units u JOIN unit_roots r ON r.unit_id = u.id "
                        "WHERE r.dotted = ? AND u.kind = 'entry' ORDER BY u.ord", (form["root_dotted"],)).fetchall()
                    if not rows:
                        rows = self._conn.execute(
                            "SELECT u.* FROM units u JOIN unit_keys k ON k.unit_id = u.id "
                            "WHERE k.key = ? AND u.kind = 'entry' ORDER BY u.ord", (form["root_key"],)).fetchall()
                for row in rows:
                    add(row, "index", f"{form['form']} ({form['root_text']})")
        truncated = len(hits) > limit
        matches = tuple(self._match(row, via, matched) for row, via, matched in hits[:limit])
        return FriedmanLookup(word, key, candidates, matches, truncated, self.status, self.credit_he)

    def _match(self, row: sqlite3.Row, via: str, matched: str) -> FriedmanMatch:
        unit = self._unit(row)
        parent = None
        children: tuple[FriedmanUnit, ...] = ()
        total = 0
        if unit.kind == "entry":
            rows = self._conn.execute("SELECT * FROM units WHERE parent = ? ORDER BY ord", (unit.id,)).fetchall()
            total = len(rows)
            children = tuple(self._unit(r) for r in rows[:MAX_CHILDREN])
        elif unit.parent:
            prow = self._conn.execute("SELECT * FROM units WHERE id = ?", (unit.parent,)).fetchone()
            parent = self._unit(prow) if prow is not None else None
        return FriedmanMatch(unit, via, matched, parent, children, total)

    def _unit(self, row: sqlite3.Row) -> FriedmanUnit:
        quotes = self._conn.execute(
            "SELECT n, quote, translation, locator, foreign_text FROM quotations WHERE unit_id = ? ORDER BY n LIMIT ?",
            (row["id"], MAX_QUOTATIONS)).fetchall()
        try:
            flags = tuple(str(f) for f in json.loads(row["flags"] or "[]"))
        except ValueError:
            flags = ()
        return FriedmanUnit(
            id=row["id"], kind=row["kind"], letter=row["letter"], parent=row["parent"],
            head_text=row["head_text"] or "", head_plain=row["head_plain"] or "", root=row["root"] or "",
            lemma=row["lemma"] or "", binyan=row["binyan"] or "", definition=row["definition"] or "",
            references=row["refs"] or "", printed_page=row["printed_page"] or "", pdf_url=row["pdf_url"] or "",
            flags=flags,
            quotations=tuple(FriedmanQuotation(q["n"], q["quote"], q["translation"], q["locator"], q["foreign_text"])
                             for q in quotes),
            quotations_total=int(row["quotations"] or 0))


# ---------------------------------------------------------------------------
# The process-wide instance (one per app, like the other sidecar services)
# ---------------------------------------------------------------------------

_default_service: Optional[FriedmanService] = None
_lock = threading.Lock()


def get_friedman_service(thread_safe: bool = True) -> FriedmanService:
    """The default service, created on first use (per-thread connections by default)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = FriedmanService(thread_safe=thread_safe)
        return _default_service


def reset_friedman_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
