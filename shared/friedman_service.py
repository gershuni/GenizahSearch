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
``MAX_CHILDREN``, ``MAX_QUOTATIONS``) so one click never renders a page; the last two are far above
the book's largest entry, so a match carries all its sub-entries and quotations, and the panel shows
the first few and the rest on a click (owner, 2026-11-11). No morphology beyond
the stems. The word is also looked up in ``tokens``, every word of every quotation and head: the
units holding it, other than the matches and the units under a matched entry, are ``quotes``, each
with the places and the word marked (``MAX_QUOTE_HITS``, ``MAX_QUOTE_PLACES``; never for a function
word or a word more than ``MAX_QUOTE_UNITS`` units hold). An inflected form the index does not list is found only where a quotation prints it, and the
lookup says nothing about which sense fits.

A root's stems (owner, 2026-11-11; the private dictionary lookup plan, section 4.6). Every match
carries the stems its entry lists (``entry_stems``: the entry's own stem label and its paragraphs',
each read as every stem number it names) and the root the entry is filed under. A root typed with a
stem ("דלל X", "דלל 10", ``shared.ja_roots.root_and_stem``) answers with that stem's paragraph under
each entry of the root that lists it (``via`` "chosen"); when none does, it says so
(``FriedmanLookup.stem_missing``) and shows the root as a plain lookup does. ``stem_pairs`` is the
whole list of (root, stem) pairs, built once in memory. A word written with the dot (ד'לל, ד̇לל)
puts the units of the dotted reading first and marks the rest (``dot_mismatch``), the rule of
Blau's lookup (``shared.ja_roots.dot_pattern``); an undotted word gives no preference.

Matches found by verb form (the plan's PR 3, sections 4.4 and 6.1): when the word itself, its base
form and the index reach nothing, the entries whose listed stems have the word among their generated
forms (``verb_form_index``, ``shared.ja_verb_forms.verb_form_cards``), at most ``MAX_VERB_FORM_CARDS``,
``via`` "verb_form" with the form, the root and the stems as ``matched``. Beside matches the word
itself reaches, those roots are only suggested (``FriedmanLookup.suggestions``). Only the kinds the
switch names are shown (``FRIEDMAN_VERB_FORM_KINDS``, default A, B, D: the graded sample, owner
2026-11-11); a shown kind opens on the stem's paragraph only if ``FRIEDMAN_VERB_FORM_STEM_KINDS`` names
it too (default A, B, D).

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
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional, Union

from shared.blau_normalize import NORMALIZER_VERSION, candidate_stems, normalize_ja  # noqa: F401 - normalize_ja: the index builder and the tests key through this module
from shared.ja_expression import (  # noqa: F401 - the names the builders, the evaluation and the tests take from here
    DEFAULT_GAP, FUNCTION_WORDS, MAX_EXPRESSION_WORDS, MAX_GAP, MIN_EXPRESSION_STEM, NO_ARTICLE_PEEL,
    OPTIONAL_LETTERS_RE, PREPOSITIONS, PROCLITIC_RE, ExpressionComponent, ExpressionPlace, clamp_gap, components_for,
    is_function_word, lookup_key, ordered_match, preposition_stems, scope_keys, short_remainder, tiered, word_keys,
)
from shared.ja_roots import (  # head_begins_with: moved there (borrowed_roots uses it); importable from here
    BorrowedRoot, StemPair, dot_pattern, dots_agree, head_begins_with, root_and_stem, stem_numbers,
)
from shared.dictionary_switch import OFF, ON, read_switch, serves
from shared.ja_verb_forms import MAX_SUGGESTIONS  # noqa: F401 - importable from here, as before the move
from shared.ja_verb_forms import (
    MAX_VERB_FORM_CARDS,
    suggested_roots,
    switch_kinds,
    verb_form_cards,
    verb_form_index,
    word_stem,
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
#: The switches of the matches found by verb form (PR 3): the kinds shown ("A,B,D"), and those of
#: them that open on the stem's paragraph. Read live; unset is the default below, set and empty is none.
ENV_VERB_FORM_KINDS = "FRIEDMAN_VERB_FORM_KINDS"
ENV_VERB_FORM_STEM_KINDS = "FRIEDMAN_VERB_FORM_STEM_KINDS"
#: The graded sample (owner, 2026-11-11: recall counts more than precision): where Friedman finds
#: nothing for the word itself, a match found by verb form names a right root for 87-95% of words of
#: kinds A, B and D; its guessed stem is right for 98% (A), 88% (B) and 94% (D) of them.
DEFAULT_VERB_FORM_KINDS = "A,B,D"
DEFAULT_VERB_FORM_STEM_KINDS = "A,B,D"
REQUIRED_TABLES = ("meta", "units", "unit_keys", "unit_roots", "quotations", "index_forms", "tokens")
MAX_MATCHES = 12
#: Bounds, not cuts (owner, 2026-11-11: nothing only on the PDF page): the local sidecar's largest
#: entry has 44 sub-entries and stem paragraphs, its largest unit 16 quotations. The panel shows the
#: first few and the rest on a click; any beyond these are counted (``children_total``,
#: ``quotations_total``) and pointed to on the page.
MAX_CHILDREN = 100
MAX_QUOTATIONS = 50
#: The expression search (its words, gaps and keys are shared.ja_expression's): units answered,
#: places shown per unit.
MAX_EXPRESSION_HITS = 20
MAX_PLACES = 4
#: One word's lookup also reads ``tokens``: the units whose quotations or head hold the word
#: (``FriedmanLookup.quotes``), and the places shown under each.
MAX_QUOTE_HITS = 5
MAX_QUOTE_PLACES = 2
#: A word held by more units than this is a common word (קאל, כתאב: over a hundred), and five of
#: them say nothing about it: no quotes at all. Measured 2026-11-10: nine keys in ten are in six
#: units or fewer.
MAX_QUOTE_UNITS = 25
SCOPE_HEAD = "head"
SCOPE_QUOTATION = "quotation"
#: How a match was reached when the reader typed a root with a stem ("דלל X"): the stem's
#: paragraph itself, chosen, not found by the word.
VIA_CHOSEN = "chosen"
#: How a match was reached when another dictionary gave its root (``FriedmanService.borrow``; owner,
#: 2026-11-11): ``FriedmanMatch.borrowed`` names which, and with what.
VIA_BORROWED = "borrowed"
#: How a match was reached through the generated forms of a stem its entry lists (PR 3):
#: ``FriedmanMatch.matched`` is the form, the root and the stems ("אסתדלו (ד̇לל X)").
VIA_VERB_FORM = "verb_form"
#: The keying rules live in shared.ja_expression; these names stay for the evaluation and the tests.
_short_remainder = short_remainder
_preposition_stems = preposition_stems
_is_function_word = is_function_word


def friedman_switch() -> str:
    """The switch, read live: on, off, or ``editors`` (``shared.dictionary_switch``); OFF unless
    set, because the sidecar is a candidate."""
    return read_switch(ENV_ENABLED, OFF)


def friedman_enabled() -> bool:
    """The switch is on for everyone (not ``editors`` only)."""
    return friedman_switch() == ON


def verb_form_kinds() -> frozenset[str]:
    """The kinds of match found by verb form the lookup shows (``FRIEDMAN_VERB_FORM_KINDS``, read on
    every call); ``DEFAULT_VERB_FORM_KINDS`` unless set; set and empty, none."""
    return switch_kinds(ENV_VERB_FORM_KINDS, DEFAULT_VERB_FORM_KINDS)


def verb_form_stem_kinds() -> frozenset[str]:
    """The kinds whose matches open on the stem's paragraph (``FRIEDMAN_VERB_FORM_STEM_KINDS``);
    ``DEFAULT_VERB_FORM_STEM_KINDS`` unless set. Any other match is the whole entry."""
    return switch_kinds(ENV_VERB_FORM_STEM_KINDS, DEFAULT_VERB_FORM_STEM_KINDS)


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
    via: str            # "head" (the word itself), "stem" (a peeled candidate), "index" (the book's index of forms), "chosen" (a typed stem), "borrowed" (another dictionary's root), "verb_form" (a generated form of a listed stem)
    matched: str        # the key or index form that reached the unit
    parent: Optional[FriedmanUnit]
    children: tuple[FriedmanUnit, ...]
    children_total: int
    #: The stems the dictionary lists under the unit's entry (its own and its binyan paragraphs'),
    #: I to X; the entry's root as printed, which opens one of them ("ד̇לל X"); the stem chosen (typed,
    #: or the one the word's shape gives, ``shared.ja_verb_forms``).
    stems: tuple[int, ...] = ()
    root: str = ""
    stem: int = 0
    #: The looked-up word carries the dot and this unit's entry is not spelled with it there.
    dot_mismatch: bool = False
    #: A match for a root another dictionary gave (``VIA_BORROWED``): each (source, evidence),
    #: ``shared.ja_roots.BorrowedRoot``'s.
    borrowed: tuple[tuple[str, str], ...] = ()
    #: A match found by verb form (``VIA_VERB_FORM``): its kind, A to E (plan 6.1). '': not one.
    kind: str = ""


@dataclass(frozen=True)
class FriedmanLookup:
    word: str
    key: str
    candidates: tuple[str, ...]
    matches: tuple[FriedmanMatch, ...]
    truncated: bool
    status: str
    credit_he: str
    #: The units whose quotations or head hold the word (the ``tokens`` table), other than the
    #: matches and the units under a matched entry; and how many more there are.
    quotes: tuple["FriedmanExpressionHit", ...] = ()
    more_quotes: int = 0
    #: The stem typed after the root ("דלל X"), 0 for a plain lookup.
    stem: int = 0
    #: Beside the matches the word itself reached: the other roots its verb forms give, (root as
    #: printed, stem; 0 when several fit), at most ``MAX_SUGGESTIONS``; a press looks "root stem" up.
    suggestions: tuple[tuple[str, int], ...] = ()

    @property
    def found(self) -> bool:
        return bool(self.matches or self.quotes)

    @property
    def stem_missing(self) -> bool:
        """A stem was typed, the root was found, and no entry filed under it lists that stem: the
        matches are the root's, with the stems they have."""
        return bool(self.stem and self.matches and not any(m.stem == self.stem for m in self.matches))


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
        # Built on first use, then read-only (``_stems``): the (root, stem) pairs, each entry's
        # stems with the units holding them, each entry's roots, and the entries under each root.
        self._index_lock = threading.Lock()
        self._stem_pairs: Optional[tuple[StemPair, ...]] = None
        self._entry_stem_units: dict[str, dict[int, tuple[str, ...]]] = {}
        self._entry_roots: dict[str, tuple[str, ...]] = {}
        self._root_entries: dict[str, tuple[str, ...]] = {}
        self._verb_forms: Optional[dict[str, tuple[StemPair, ...]]] = None
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
        """Ready AND switched on for everyone: what a surface asks that does not know who is
        reading (an ``editors`` switch stays hidden there)."""
        return self.is_ready() and friedman_enabled()

    def served_to(self, editor: bool) -> bool:
        """Ready AND shown to this reader: on for everyone, or ``editors`` and the reader an editor
        or admin (``shared.dictionary_switch``; owner, 2026-11-11: a preview before release)."""
        return self.is_ready() and serves(friedman_switch(), editor)

    def _answering(self) -> bool:
        """The lookups answer while the switch is not off: the page decided who may ask."""
        return self.is_ready() and friedman_switch() != OFF

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

    def answer(self, text: str, gap: int = DEFAULT_GAP, stems: bool = True) -> FriedmanAnswer:
        """What the panel asks for what the reader clicked, selected or typed: several words are an
        expression search, one word a lookup, and a root with a typed stem ("דלל X") the lookup of
        that root with the stem chosen; ``stems`` False (words clicked or selected in the
        manuscript) reads no stem: "פרק 2" there is two words."""
        text = " ".join(str(text or "").split())
        if len(text.split()) > 1 and not (stems and root_and_stem(text)[1]):
            return self.expression(text, gap)
        return self.lookup(text)

    def lookup(self, word: str, limit: int = MAX_MATCHES, verb_forms: Optional[frozenset[str]] = None,
               verb_form_stems: Optional[frozenset[str]] = None) -> FriedmanLookup:
        """What the dictionary has for one clicked or typed word, or for a root with a typed stem
        ("דלל X", ``shared.ja_roots.root_and_stem``). ``verb_forms`` / ``verb_form_stems``: the kinds
        of match found by verb form shown, and those opened on the stem's paragraph; None reads the
        switches (``verb_form_kinds``; the graded sample passes every kind). Never raises."""
        word = str(word or "").strip()
        kinds = verb_form_kinds() if verb_forms is None else frozenset(verb_forms)
        stem_kinds = verb_form_stem_kinds() if verb_form_stems is None else frozenset(verb_form_stems)
        if not word or not self._answering():
            return empty_lookup(word, status=self.status, credit_he=self.credit_he)
        try:
            root, stem = root_and_stem(word)
            if stem:
                return self._lookup_stem(word, root, stem, max(1, int(limit)))
            key = lookup_key(word)
            # The stems come from the cleaned key: on the raw word the PDF's presentation-form
            # harakat would split it and no clitic would be peeled (Codex, round 4 of #487).
            candidates = tuple(dict.fromkeys([key, *candidate_stems(key)])) if key else ()
            if not candidates:
                return empty_lookup(word, key, (), self.status, self.credit_he)
            return self._lookup(word, key, candidates, max(1, int(limit)), verb_forms=kinds,
                                verb_form_stems=stem_kinds)
        except Exception:
            logger.exception("FriedmanService: lookup failed for %r", word)
            return empty_lookup(word, status=self.status, credit_he=self.credit_he)

    # -- the stems an entry lists ---------------------------------------------

    def _stems(self) -> tuple[StemPair, ...]:
        """Built once from the open file (plan section 4.1): a unit with a stem label (an entry's
        own, a ``binyan`` paragraph's, a sub-entry's) gives its entry every stem number the label
        names (``stem_numbers``: "IV/II" is II and IV, "(" none), and a (root, stem) pair for each
        root the entry is filed under (``unit_roots``, dotted). Nothing is stored."""
        if self._stem_pairs is None:
            with self._index_lock:
                if self._stem_pairs is None:
                    entry_roots: dict[str, list[str]] = {}
                    root_entries: dict[str, list[str]] = {}
                    for row in self._conn.execute(
                            "SELECT r.unit_id, r.dotted FROM unit_roots r JOIN units u ON u.id = r.unit_id "
                            "WHERE u.kind = 'entry' ORDER BY u.ord, r.rowid"):
                        dotted = str(row["dotted"] or "").strip()
                        key = lookup_key(dotted)
                        if not key:
                            continue
                        roots = entry_roots.setdefault(row["unit_id"], [])
                        if dotted not in roots:
                            roots.append(dotted)
                        entries = root_entries.setdefault(key, [])
                        if row["unit_id"] not in entries:
                            entries.append(row["unit_id"])
                    stem_units: dict[str, dict[int, list[str]]] = {}
                    pairs: list[StemPair] = []
                    for row in self._conn.execute(
                            "SELECT id, kind, parent, binyan FROM units WHERE binyan IS NOT NULL AND binyan <> '' "
                            "ORDER BY ord"):
                        entry_id = row["id"] if row["kind"] == "entry" else row["parent"]
                        numbers = stem_numbers(row["binyan"])
                        if not entry_id or not numbers:
                            continue
                        for number in numbers:
                            stem_units.setdefault(entry_id, {}).setdefault(number, []).append(row["id"])
                            for dotted in entry_roots.get(entry_id, ()):
                                pairs.append(StemPair(dotted, lookup_key(dotted), number, row["id"], entry_id))
                    self._entry_roots = {e: tuple(r) for e, r in entry_roots.items()}
                    self._root_entries = {k: tuple(e) for k, e in root_entries.items()}
                    self._entry_stem_units = {e: {n: tuple(u) for n, u in sorted(s.items())}
                                              for e, s in stem_units.items()}
                    self._stem_pairs = tuple(pairs)
        return self._stem_pairs

    def stem_pairs(self) -> tuple[StemPair, ...]:
        """Every (root, stem) pair the dictionary lists, in the book's order (plan section 4.1);
        () when the dictionary is not available or on any error. Never raises."""
        if not self._answering():
            return ()
        try:
            return self._stems()
        except Exception:
            logger.exception("FriedmanService: the stem pairs could not be read")
            return ()

    def verb_form_index(self) -> dict[str, tuple[StemPair, ...]]:
        """Every form key the (root, stem) pairs the book lists may be written as, and the pairs that
        give it (``shared.ja_verb_forms.verb_form_index``; plan section 4.3, PR 2): built once from
        the open file, held in memory. Nothing in the lookup calls it yet: the gate
        (``scripts/measure_dictionary_stems.py``) measures it first. {} when unavailable or on any
        error. Never raises."""
        if not self.is_ready():
            return {}
        try:
            if self._verb_forms is None:
                pairs = self._stems()
                with self._index_lock:
                    if self._verb_forms is None:
                        self._verb_forms = verb_form_index(pairs)
            return self._verb_forms
        except Exception:
            logger.exception("FriedmanService: the verb form index could not be built")
            return {}

    def entry_stems(self, entry_id: str) -> tuple[int, ...]:
        """The stem numbers the dictionary lists under one entry, I to X in order; () for an entry
        with none, when unavailable, or on any error. Never raises."""
        return tuple(self._stem_units(entry_id))

    def _stem_units(self, entry_id: Optional[str]) -> dict[int, tuple[str, ...]]:
        if not entry_id or not self.is_ready():
            return {}
        try:
            self._stems()
            return self._entry_stem_units.get(entry_id, {})
        except Exception:
            logger.exception("FriedmanService: the stems of %r could not be read", entry_id)
            return {}

    def _lookup_stem(self, word: str, root: str, stem: int, limit: int) -> FriedmanLookup:
        """A root with a typed stem: the paragraph of that stem (the ``binyan`` unit, or the entry
        when the stem is its own) under every entry filed under the root that lists it, the entries
        whose dotted root agrees with a dotted typed root first; when none lists it, the root as a
        plain lookup of it shows it (``stem_missing``), with each entry's stems."""
        key = lookup_key(root)
        self._stems()
        pattern = dot_pattern(root)
        found: list[tuple[str, bool]] = []
        for entry_id in self._root_entries.get(key, ()):
            units = self._entry_stem_units.get(entry_id, {}).get(stem, ())
            if not units:
                continue
            mismatch = "*" in pattern and not any(dots_agree(pattern, r) for r in self._entry_roots.get(entry_id, ()))
            found.extend((unit_id, mismatch) for unit_id in units if unit_id not in {u for u, _m in found})
        if not found:
            plain = self._lookup(root, key, (key,), limit, quotes=False)
            return replace(plain, word=word, stem=stem)
        found.sort(key=lambda hit: hit[1])
        matches = []
        for unit_id, mismatch in found[:limit]:
            row = self._conn.execute("SELECT * FROM units WHERE id = ?", (unit_id,)).fetchone()
            if row is not None:
                matches.append(self._match(row, VIA_CHOSEN, root, stem=stem, dot_mismatch=mismatch))
        return FriedmanLookup(word, key, (key,), tuple(matches), len(found) > limit, self.status, self.credit_he,
                              stem=stem)

    def unit(self, unit_id: str) -> Optional[FriedmanUnit]:
        """One unit by id, with its quotations; None when unknown or unavailable."""
        if not self._answering():
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
        if not text or not self._answering():
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

    def _lookup(self, word: str, key: str, candidates: tuple[str, ...], limit: int,
                quotes: bool = True, verb_forms: frozenset[str] = frozenset(),
                verb_form_stems: frozenset[str] = frozenset()) -> FriedmanLookup:
        # A hit: the unit, how it was reached, what reached it, and the index form when the book's
        # index did (a spelling whose dots count, ``_dot_mismatch``).
        hits: list[tuple[sqlite3.Row, str, str, str]] = []
        seen: set[str] = set()

        def add(row: sqlite3.Row, via: str, matched: str, form: str = "") -> None:
            if row["id"] not in seen:
                seen.add(row["id"])
                hits.append((row, via, matched, form))

        for candidate in candidates:
            rows = self._conn.execute(
                "SELECT u.* FROM units u JOIN unit_keys k ON k.unit_id = u.id WHERE k.key = ? ORDER BY u.ord",
                (candidate,)).fetchall()
            # The units whose own head begins with the word first, then those that list it as another
            # spelling or inside a longer head, each in the book's order (owner, 2026-11-11: וצל
            # reached the noun אَצْל/אُצْל/וَצْל before the verb וצל).
            rows.sort(key=lambda row: not head_begins_with(row["head_text"], candidate))
            for row in rows:
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
                    add(row, "index", f"{form['form']} ({form['root_text']})", str(form["form"] or ""))
        # The word written with the dot (an apostrophe or geresh after the letter, or U+0307): the
        # units of the dotted reading first, the rest marked, as Blau's lookup does (owner,
        # 2026-11-11). An undotted word gives no preference.
        pattern = dot_pattern(word)
        mismatch = {row["id"]: self._dot_mismatch(row, form, pattern) for row, _via, _matched, form in hits} \
            if "*" in pattern else {}
        if mismatch:
            hits.sort(key=lambda hit: mismatch[hit[0]["id"]])
        truncated = len(hits) > limit
        # An entry the book's index reached opens on the stem the word's shape gives (אסתקר: stem X
        # of קרר; owner, 2026-11-11); the word itself or its base form reached what it names.
        matches = tuple(self._word_stem_match(row, via, matched, candidates, mismatch.get(row["id"], False))
                        if via == "index" else
                        self._match(row, via, matched, dot_mismatch=mismatch.get(row["id"], False))
                        for row, via, matched, _form in hits[:limit])
        # The matches found by verb form, only when the word itself reached nothing (their entries
        # leave the quotes, as a matched entry's do); beside the word's own matches, the roots its verb
        # forms give are suggestions (owner, 2026-11-11: the graded sample).
        entries = {row["id"] if row["kind"] == "entry" else row["parent"] for row, *_rest in hits}
        verb_matches: list[FriedmanMatch] = []
        verb_more = 0
        suggestions: tuple[tuple[str, int], ...] = ()
        if hits:
            suggestions = suggested_roots(candidates, self.verb_form_index() if verb_forms else {}, verb_forms,
                                          entries)
        else:
            verb_matches, verb_more = self._verb_form_matches(candidates, entries, verb_forms, verb_form_stems)
        matched_entries = {row["id"] for row, *_rest in hits if row["kind"] == "entry"}
        for match in verb_matches:
            seen.add(match.unit.id)
            matched_entries.add(match.unit.id if match.unit.kind == "entry" else (match.unit.parent or match.unit.id))
        found_quotes, more_quotes = (self._quotes(word, seen, matched_entries) if quotes else ((), 0))
        return FriedmanLookup(word, key, candidates, matches + tuple(verb_matches), truncated or verb_more > 0,
                              self.status, self.credit_he, found_quotes, more_quotes, suggestions=suggestions)

    def _verb_form_matches(self, candidates: tuple[str, ...], shown: set[Optional[str]], kinds: frozenset[str],
                           stem_kinds: frozenset[str]) -> tuple[list[FriedmanMatch], int]:
        """The matches found by verb form (plan 4.4, PR 3): the entries the word's keys reach through
        the generated forms of a stem the entry lists (``shared.ja_verb_forms.verb_form_cards``, in the
        plan's order), other than the entries the word already reached (``shown``, which they join),
        of a kind switched on (``kinds``); at most ``MAX_VERB_FORM_CARDS``, the rest counted. A match
        is the stem's paragraph when its kind is in ``stem_kinds`` and the word fits one listed stem
        (as "דלל X" opens it), else the entry with its stems as buttons."""
        if not kinds:
            return [], 0
        index = self.verb_form_index()
        if not index:
            return [], 0
        self._stems()
        out: list[FriedmanMatch] = []
        more = 0
        for card in verb_form_cards(candidates, index):
            if card.kind not in kinds or card.entry_id in shown:
                continue
            if len(out) >= MAX_VERB_FORM_CARDS:
                more += 1
                continue
            stem = card.stem if card.kind in stem_kinds else 0
            units = self._entry_stem_units.get(card.entry_id, {}).get(stem, ()) if stem else ()
            row = self._conn.execute("SELECT * FROM units WHERE id = ?",
                                     (units[0] if units else card.entry_id,)).fetchone()
            if row is None:
                continue
            match = self._match(row, VIA_VERB_FORM, card.evidence(), stem=stem if units else 0)
            out.append(replace(match, kind=card.kind))
            shown.add(card.entry_id)
        return out, more

    def _word_stem_match(self, row: sqlite3.Row, via: str, matched: str, keys: tuple[str, ...],
                         dot_mismatch: bool = False) -> FriedmanMatch:
        """An entry reached through its root, opened on the stem the word's shape gives
        (``shared.ja_verb_forms.word_stem``): that stem's paragraph (the entry itself when the stem is
        its own) with the stem chosen, as "קרר X" opens it. The entry as it is when none of its
        listed stems, or several, hold the word."""
        if row["kind"] == "entry":
            self._stems()
            units = self._entry_stem_units.get(row["id"], {})
            stem = 0
            for dotted in self._entry_roots.get(row["id"], ()):
                stem = word_stem(keys, lookup_key(dotted), units) if units else 0
                if stem:
                    break
            if stem:
                unit_id = units[stem][0]
                unit_row = row if unit_id == row["id"] else self._conn.execute(
                    "SELECT * FROM units WHERE id = ?", (unit_id,)).fetchone()
                if unit_row is not None:
                    return self._match(unit_row, via, matched, stem=stem, dot_mismatch=dot_mismatch)
        return self._match(row, via, matched, dot_mismatch=dot_mismatch)

    # -- roots borrowed from the other dictionaries -------------------------

    def borrow(self, result: FriedmanAnswer, borrowed: tuple[BorrowedRoot, ...]) -> FriedmanAnswer:
        """Friedman's answer with a match for each entry filed under a root another dictionary gives
        for the word (``shared.ja_roots.borrowed_roots``: Blau's own entries, Lane's first root),
        after Friedman's own (owner, 2026-11-11: ויכון reached nothing here though Lane gave كون). The
        entries spelled with the same dots where several share the letters; an entry Friedman's own
        matches already show gets no second match, and two dictionaries giving one entry make one
        match naming both; each opens on the stem the word's shape gives. The quotes under a
        borrowed entry go (its match shows it). Not for an expression or a typed stem. Never
        raises: on any error, the answer as it was."""
        if not isinstance(result, FriedmanLookup) or result.stem or not borrowed or not self._answering():
            return result
        try:
            return self._borrow(result, borrowed)
        except Exception:
            logger.exception("FriedmanService: borrowing roots for %r failed", result.word)
            return result

    def _borrow(self, result: FriedmanLookup, borrowed: tuple[BorrowedRoot, ...]) -> FriedmanLookup:
        self._stems()

        def entry_of(unit: FriedmanUnit) -> str:
            return unit.id if unit.kind == "entry" else (unit.parent or unit.id)

        shown = {entry_of(match.unit) for match in result.matches}
        # A match found by verb form (PR 3) whose root another dictionary also gives keeps its place
        # and names that dictionary too.
        verb_entries = {entry_of(match.unit) for match in result.matches if match.via == VIA_VERB_FORM}
        verb_sources: dict[str, list[tuple[str, str]]] = {}
        sources: dict[str, list[tuple[str, str]]] = {}
        overflow = False
        for item in borrowed:
            key = lookup_key(item.root)
            if not key or " " in key:
                continue
            entries = self._root_entries.get(key, ())
            pattern = dot_pattern(item.root)
            same = [e for e in entries if any(dot_pattern(r) == pattern for r in self._entry_roots.get(e, ()))]
            for entry_id in same or entries:
                given = (item.source, item.evidence)
                if entry_id in verb_entries:
                    if given not in verb_sources.setdefault(entry_id, []):
                        verb_sources[entry_id].append(given)
                    continue
                if entry_id in shown:
                    continue
                if entry_id not in sources:
                    if len(result.matches) + len(sources) >= MAX_MATCHES:
                        overflow = True
                        continue
                    sources[entry_id] = []
                if given not in sources[entry_id]:
                    sources[entry_id].append(given)
        if verb_sources:
            result = replace(result, matches=tuple(
                replace(match, borrowed=match.borrowed + tuple(verb_sources[entry_of(match.unit)]))
                if match.via == VIA_VERB_FORM and verb_sources.get(entry_of(match.unit)) else match
                for match in result.matches))
        added = []
        # An entry two dictionaries agree on first (ויכון: כון, which Blau and Lane both give, before
        # כלו, which only Blau's phrase gives); otherwise in the order the roots came.
        for entry_id, given in sorted(sources.items(), key=lambda item: -len(item[1])):
            row = self._conn.execute("SELECT * FROM units WHERE id = ?", (entry_id,)).fetchone()
            if row is not None:
                match = self._word_stem_match(row, VIA_BORROWED, "", result.candidates)
                added.append(replace(match, borrowed=tuple(given)))
        if not added:
            return replace(result, truncated=True) if overflow else result
        quotes = tuple(hit for hit in result.quotes
                       if (hit.parent.id if hit.parent is not None else hit.unit.id) not in sources)
        # A suggested root another dictionary also gives is now a match: no chip for it.
        taken = {dot_pattern(match.root) for match in added}
        return replace(result, matches=result.matches + tuple(added), truncated=result.truncated or overflow,
                       quotes=quotes,
                       suggestions=tuple(s for s in result.suggestions if dot_pattern(s[0]) not in taken))

    def _dot_mismatch(self, row: sqlite3.Row, form: str, pattern: str) -> bool:
        """The looked-up word carries the dot (``pattern``) and none of the unit's spellings has it on
        the same letters: the roots its entry is filed under (``unit_roots``, dotted), the entry's
        head when the unit is the entry, and the index form that reached it. A sub-entry's or a
        stem paragraph's own head is not a spelling of the word (it is mostly its complement)."""
        self._stems()
        entry_id = row["id"] if row["kind"] == "entry" else row["parent"]
        spellings = list(self._entry_roots.get(entry_id, ()))
        if row["kind"] == "entry" and row["head_text"]:
            spellings.append(row["head_text"])
        if form:
            spellings.append(form)
        return not any(dots_agree(pattern, spelling) for spelling in spellings)

    def _quotes(self, word: str, matched: set[str], matched_entries: set[str]
                ) -> tuple[tuple[FriedmanExpressionHit, ...], int]:
        """The units whose quotations or head hold ``word``, and how many more there are: the word
        as the expression search keys it (``word_keys``) against ``tokens``. Not the matched units
        or the units under a matched entry (the reader has those), not a function word (פי is in
        two thousand units and none of them is about it). The units that print the word exactly,
        and most often, come first; then the book's order."""
        if len(lookup_key(word).split()) != 1:
            return (), 0   # several words are the expression search's, never any-of-them
        keys = word_keys(word)
        if not keys or is_function_word(keys):
            return (), 0
        marks = ",".join("?" for _ in keys)
        place = "t.scope || ' ' || t.n"
        units = [r for r in self._conn.execute(
            f"SELECT u.id, u.ord, u.parent, COUNT(DISTINCT {place}) AS places, "
            f"COUNT(DISTINCT CASE WHEN t.key = ? AND t.peeled = 0 THEN {place} END) AS exact "
            f"FROM tokens t JOIN units u ON u.id = t.unit_id WHERE t.key IN ({marks}) GROUP BY u.id", (keys[0], *keys))
            if r["id"] not in matched and r["parent"] not in matched_entries]
        if len(units) > MAX_QUOTE_UNITS:
            return (), 0
        units.sort(key=lambda r: (-int(r["exact"]), -int(r["places"]), int(r["ord"])))
        accepts = [frozenset(keys)]
        hits: list[FriedmanExpressionHit] = []
        for row in units[:MAX_QUOTE_HITS]:
            scopes = self._conn.execute(
                "SELECT scope, n, MAX(key = ? AND peeled = 0) AS exact FROM tokens "
                f"WHERE unit_id = ? AND key IN ({marks}) GROUP BY scope, n ORDER BY exact DESC, scope = ?, n LIMIT ?",
                (keys[0], row["id"], *keys, SCOPE_QUOTATION, MAX_QUOTE_PLACES)).fetchall()
            places: list[FriedmanPlace] = []
            for found in scopes:
                items = [(int(r["pos"]), str(r["key"])) for r in self._conn.execute(
                    "SELECT pos, key FROM tokens WHERE unit_id = ? AND scope = ? AND n = ? ORDER BY pos, rowid",
                    (row["id"], found["scope"], found["n"]))]
                match = ordered_match(items, accepts, 0)
                place_found = (self._place(row["id"], found["scope"], int(found["n"]), items, match, (word,), (keys,))
                               if match is not None else None)
                if place_found is not None:
                    places.append(place_found)
            if not places:
                continue
            unit = self._unit(self._conn.execute("SELECT * FROM units WHERE id = ?", (row["id"],)).fetchone())
            parent = None
            if unit.parent:
                prow = self._conn.execute("SELECT * FROM units WHERE id = ?", (unit.parent,)).fetchone()
                parent = self._unit(prow) if prow is not None else None
            hits.append(FriedmanExpressionHit(unit, parent, tuple(places), max(int(row["places"]), len(places))))
        return tuple(hits), max(len(units) - MAX_QUOTE_HITS, 0)

    def _match(self, row: sqlite3.Row, via: str, matched: str, stem: int = 0,
               dot_mismatch: bool = False) -> FriedmanMatch:
        """One unit as a match: an entry brings its sub-entries and stem paragraphs (when a stem
        was chosen and the entry is that stem's paragraph, the panel opens them only on a click:
        the entry's own text is the stem), a sub-entry or a stem paragraph its entry. Every match
        carries the stems its entry lists and the entry's root, which opens one of them."""
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
        entry = unit if unit.kind == "entry" else parent
        stems = tuple(self._stem_units(entry.id)) if entry is not None else ()
        roots = self._entry_roots.get(entry.id, ()) if entry is not None else ()
        root = roots[0] if roots else (entry.root if entry is not None else "")
        return FriedmanMatch(unit, via, matched, parent, children, total, stems=stems, root=root,
                             stem=stem if stem in stems else 0, dot_mismatch=dot_mismatch)

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
