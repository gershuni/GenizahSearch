# -*- coding: utf-8 -*-
"""Read-only lookups in the Blau dictionary sidecar (``blau_data/blau.db``).

Phase 2 of docs/plans/BLAU_DICTIONARY_PLAN.md: a reader clicks a word of a
transcription and this module answers what Joshua Blau's *Dictionary of
Mediaeval Judaeo-Arabic Texts* (2006; blau-dict.netlify.app, release 1.1) has
for it. scripts/build_blau_sidecar.py builds the file and
scripts/verify_blau_sidecar.py gates it; this module only reads it, decides
nothing about its content and never writes.

Fail-closed, the way web/discovery_assets.py treats its sidecar. The service
is READY only when the file is where a sidecar lives, opens read-only, passes
``PRAGMA quick_check``, carries the schema version this reader understands,
was keyed by the normalizer this process runs (``meta.normalizer_version`` ==
``shared.blau_normalize.NORMALIZER_VERSION``; otherwise every lookup would
silently miss), holds every table a lookup reads and the lookup index, and
carries the credit line of the owner's ruling (a) -- a panel without the
credit must not open. Any of those failing leaves ``is_ready()`` False with
the reason in ``unavailable_reason``, and ``lookup`` answers an empty result.
``is_available()`` is readiness AND the ``BLAU_DICTIONARY_ENABLED`` kill
switch (default on, read live like ``FGP_TRANSCRIPTIONS_ENABLED``). Nothing
here raises into a page.

The lookup (plan §6.2): ``normalize_ja`` the clicked word, then
``candidate_stems`` in order -- the word first, then one proclitic layer
peeled, then one pronoun suffix -- each against ``forms_norm``. A key of a
headword-like kind (root, sub-entry, headword, variant) makes one card per
scope it reaches -- a dictionary entry, or one sub-entry of it -- per
candidate, in candidate order, with the matched senses first and then the
senses under that scope. A key found only inside the dictionary's own example
phrases (kind ``phrase``) is listed apart, under "also in examples": the entry
it sits in defines some OTHER word. Ambiguity is the norm, not an error:
several candidates that hit are several cards, never one merged answer.
Everything is bounded (``MAX_CARDS``, ``MAX_SENSES``, ``MAX_EXAMPLES``) so one
click never renders a page.

A single word also reaches an entry whose NAME is several words and begins
with it (``NAME_PREFIX_KINDS``): ذلك is the entry "ד̇לך ו", and before this a
click on דלך reached only دلك "to polish" (owner, 2026-11-04).

The dot above. The lookup key drops it (the automatic text mostly omits it,
plan §3), so דלך and ד̇לך share a key and both readings are cards. When the
word itself carries the dot -- written with U+0307, or with an apostrophe or
geresh after the letter, which is how the automatic text and any keyboard
write it (``dot_pattern``) -- the cards whose spelling has the dot on the same
letters come first. Only the letters Judeo-Arabic dots count (``DOT_LETTERS``),
so an abbreviation mark (ר', מ') changes nothing.

A typed word may be Arabic script (the panel's box): it is looked up against
the Arabic spellings of the entries, sub-entries and forms, by a key that
also undoes the extraction's broken spacing (``arabic_key``).

No guessing at misread letters: a lookup answers what the word's own
spelling reaches. One-letter "close spellings" were built and measured
against the PGP Judeo-Arabic editions (scripts/measure_blau_close_spellings.py)
and dropped by the owner on 2026-11-04: about 1 offer in 20 was right, because
most words with no entry were read correctly and the dictionary lacks them.
The reader corrects a letter in the box instead.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import quote

from shared.blau_normalize import (
    NORMALIZER_VERSION,
    QUOTE_MARKS,
    candidate_stems,
    fold_finals,
    normalize_arabic,
    normalize_ja,
)
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader understands. scripts/build_blau_sidecar.py
#: writes its own ``SCHEMA_VERSION`` into ``meta.version``; the two are pinned
#: equal by tests/test_blau_service.py, and a file of another version is not
#: opened (reject-incompatible, never a silent read of changed columns).
SCHEMA_VERSION = "1.0.0"
SIDECAR_DIR = "blau_data"
SIDECAR_FILENAME = "blau.db"
#: The dictionary's site; an entry page is ``entry/<root id>`` (its
#: ``js/search.js::openInReader``).
SITE_URL = "https://blau-dict.netlify.app/"

HEADWORD_KINDS = ("root", "sub_entry", "headword", "variant")
#: Kinds whose multi-word name a single word may begin ("ד̇לך ו"). Not variants:
#: a multi-word variant is mostly a verb with its preposition, and would bury a
#: common word under cards.
NAME_PREFIX_KINDS = ("root", "sub_entry", "headword")
#: The letters Judeo-Arabic writes with a dot above for a second Arabic letter
#: (finals folded): ג̇ ד̇ ט̇ צ̇ ת̇ כ̇.
DOT_LETTERS = frozenset("גדטצתכ")
PHRASE_KIND = "phrase"
MAX_CARDS = 10
MAX_SENSES = 4
MAX_EXAMPLES = 8
REQUIRED_TABLES = ("meta", "roots", "sub_entries", "stems", "forms", "forms_norm")
LOOKUP_INDEX = "idx_forms_norm_norm"
_KIND_RANK = {kind: rank for rank, kind in enumerate(HEADWORD_KINDS)}
#: What a sense needs of a form; never ``segments_json``, the largest column.
_FORM_COLUMNS = ("id, ja_text, arabic_spelling, binyan, gloss_he, gloss_en, cross_reference, target_ids, "
                 "page, is_classical, is_reconstructed, is_erroneous")


def blau_enabled() -> bool:
    """The ``BLAU_DICTIONARY_ENABLED`` kill switch, read on every call (default on)."""
    value = os.environ.get("BLAU_DICTIONARY_ENABLED")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def entry_url(root_id: str) -> str:
    """The dictionary site's page for one entry."""
    return f"{SITE_URL}entry/{quote(str(root_id), safe='')}"


_DOT_ABOVE = "̇"


def dot_pattern(text: str) -> str:
    """The Hebrew letters of ``text`` (finals folded, other marks dropped, words
    split by one space) with ``*`` after each letter of ``DOT_LETTERS`` that
    carries the dot above: U+0307 as the dictionary writes it, or an apostrophe
    or geresh after the letter as the automatic text and a keyboard write it.
    ``ד'לך`` and ``ד̇לך`` are both ``ד*לכ``; ``ר'`` is ``ר``."""
    out: list[str] = []
    for ch in unicodedata.normalize("NFC", text or ""):
        if "א" <= ch <= "ת":
            out.append(fold_finals(ch))
        elif ch == _DOT_ABOVE or ch in QUOTE_MARKS:
            if out and out[-1] in DOT_LETTERS:
                out.append("*")
        elif ch.isspace() and out and out[-1] != " ":
            out.append(" ")
    return "".join(out).strip()


def _dots_agree(word_pattern: str, spelling: str) -> bool:
    """The dictionary spelling's first word, dots included, sits inside the
    looked-up word (whose proclitics the candidate may have peeled)."""
    first = dot_pattern(spelling).split(" ", 1)[0]
    return bool(first) and first in word_pattern


# The Arabic key: on top of normalize_arabic, the letters a typist writes either
# way (ta marbuta / ha, alef maqsura / ya, the hamza seats) fold together.
_ARABIC_EXTRA_FOLD = str.maketrans({"ة": "ه", "ى": "ي", "ؤ": "و", "ئ": "ي"})
_ARABIC_ALTERNATIVES = re.compile(r"[|,،/;؛]")
_ARABIC_PLURAL_MARK = "ج"   # the dictionary writes a plural as "ج أبايا"


def arabic_key(text: str) -> str:
    """One Arabic spelling as a lookup key: ``normalize_arabic``, the dictionary's
    plural mark dropped, the extra folds, and every space removed -- the
    extraction split words (``أخ ذ``, tatweel-broken forms), so the key is the
    letters alone, and a typed phrase meets a stored one the same way."""
    words = normalize_arabic(text).split()
    if len(words) > 1 and words[0] == _ARABIC_PLURAL_MARK:
        words = words[1:]
    return "".join(words).translate(_ARABIC_EXTRA_FOLD)


def arabic_keys(spelling: str) -> list[str]:
    """The keys of a stored Arabic spelling: one per alternative (the dictionary
    separates them with ``|`` and commas), each of at least two letters."""
    out: list[str] = []
    for part in _ARABIC_ALTERNATIVES.split(spelling or ""):
        key = arabic_key(part)
        if len(key) >= 2 and key not in out:
            out.append(key)
    return out


def arabic_candidates(text: str) -> list[str]:
    """A typed Arabic word's candidates: the word, then without a conjunction
    (و, ف), then without the article (ال); each at least two letters."""
    key = arabic_key(text)
    if len(key) < 2:
        return []
    out = [key]
    rest = key
    if rest[:1] in ("و", "ف") and len(rest) - 1 >= 3:
        rest = rest[1:]
        out.append(rest)
    if rest.startswith("ال") and len(rest) - 2 >= 2:
        rest = rest[2:]
        out.append(rest)
    return out


def default_sidecar_path() -> Optional[str]:
    """Where the sidecar lives: the user's updated copy first (the desktop
    sidecar updater's location, as for the FGP and PGP sidecars), then the
    repository's ``blau_data/blau.db``."""
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


# ---------------------------------------------------------------------------
# What a lookup answers: plain frozen records a renderer can draw without a
# single decision of its own.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BlauSense:
    """One form under an entry: the dictionary's own text, spelling, binyan,
    glosses and flags. ``matched`` says this very form carries the key that
    was looked up (a headword or variant hit); the other senses of a card are
    the first forms under the matched sub-entry or root."""
    form_id: str
    ja_text: str
    arabic: str
    binyan: str
    gloss_he: str
    gloss_en: str
    cross_reference: str
    see: tuple[tuple[str, str], ...]
    page: Optional[int]
    is_classical: bool
    is_reconstructed: bool
    is_erroneous: bool
    matched: bool


@dataclass(frozen=True)
class BlauCard:
    """One dictionary entry (a root) that a candidate key hit."""
    root_id: str
    name: str
    arabic: str
    page: Optional[int]
    url: str
    candidate: str
    matched_key: str
    matched_kind: str
    sub_entry: str
    via: str
    senses: tuple[BlauSense, ...]
    more_senses: int
    #: The looked-up word carries the dot (``dot_pattern``) and this entry's
    #: spelling does not have it on the same letters: another word, probably.
    dot_mismatch: bool = False


@dataclass(frozen=True)
class BlauExample:
    """An entry whose example phrases contain the key: it defines another word."""
    root_id: str
    name: str
    url: str
    phrase: str


@dataclass(frozen=True)
class BlauLookup:
    word: str
    key: str
    candidates: tuple[str, ...]
    cards: tuple[BlauCard, ...]
    more_cards: int
    examples: tuple[BlauExample, ...]
    more_examples: int
    #: "ja" (Hebrew letters) or "arabic" (a typed Arabic-script word).
    script: str = "ja"

    @property
    def found(self) -> bool:
        return bool(self.cards or self.examples)


def empty_lookup(word: str = "", key: str = "", candidates: tuple[str, ...] = (),
                 script: str = "ja") -> BlauLookup:
    return BlauLookup(word=word, key=key, candidates=candidates, cards=(), more_cards=0,
                      examples=(), more_examples=0, script=script)


class _CardBuild:
    """A card while its hits are collected: one scope (an entry, or one of its
    sub-entries) reached by one candidate."""

    __slots__ = ("root", "candidate", "matched_key", "matched_kind", "via", "sub_entry_id",
                 "matched_form_ids", "dot_mismatch")

    def __init__(self, root: sqlite3.Row, candidate: str, matched_key: str, matched_kind: str, via: str,
                 sub_entry_id: str):
        self.root = root
        self.candidate = candidate
        self.matched_key = matched_key
        self.matched_kind = matched_kind
        self.via = via
        self.sub_entry_id = sub_entry_id
        self.matched_form_ids: list[str] = []
        self.dot_mismatch = False


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _page(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def _has_content(form: sqlite3.Row) -> bool:
    return any(_text(form[c]) for c in ("ja_text", "gloss_he", "gloss_en", "cross_reference"))


class BlauService:
    """Read-only access to the Blau sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        # Built on first use, then read-only: the Arabic-spelling index (typed Arabic).
        self._index_lock = threading.Lock()
        self._arabic_index: Optional[dict[str, tuple[dict[str, str], ...]]] = None
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not self._db_path:
            self.unavailable_reason = "no sidecar path"
            return
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("BlauService: %s", self.unavailable_reason)
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
            logger.warning("BlauService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("BlauService: serving %s (schema %s, source release %s, %s keys)",
                    self._db_path, meta.get("version"), meta.get("source_release"),
                    meta.get("rows_forms_norm"))

    # -- state -------------------------------------------------------------

    @property
    def db_path(self) -> Optional[str]:
        return self._db_path

    def is_ready(self) -> bool:
        """The sidecar is open and passed every check."""
        return self._conn is not None

    def is_available(self) -> bool:
        """Ready AND the kill switch is on. The only question a surface asks."""
        return self.is_ready() and blau_enabled()

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    @property
    def credit_he(self) -> str:
        """The site's own credit line (ruling (a)); shown on every surface."""
        return self._meta.get("credit_he", "")

    @property
    def credit_en(self) -> str:
        return self._meta.get("credit_en", "")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self.unavailable_reason = "closed"

    # -- the lookup ----------------------------------------------------------

    def lookup(self, word: str) -> BlauLookup:
        """What the dictionary has for one clicked or typed word. A word with
        Hebrew letters is looked up as Judeo-Arabic; a word with Arabic letters
        only, against the Arabic spellings. Never raises; empty when
        unavailable, when the word has neither, or on any error."""
        word = _text(word)
        if not word or not self.is_available():
            return empty_lookup(word)
        key = ""
        candidates: tuple[str, ...] = ()
        script = "ja"
        try:
            key = normalize_ja(word)
            if not key:
                script = "arabic"
                candidates = tuple(arabic_candidates(word))
                if not candidates:
                    return empty_lookup(word)
                key = candidates[0]
                return self._lookup_arabic(word, key, candidates)
            candidates = tuple(candidate_stems(key))
            return self._lookup(word, key, candidates)
        except Exception:
            logger.exception("BlauService: lookup of %r failed", word)
            return empty_lookup(word, key, candidates, script)

    # -- typed Arabic ---------------------------------------------------------

    def _arabic(self) -> dict[str, tuple[dict[str, str], ...]]:
        """arabic_key -> the hits it reaches: entries and sub-entries by their
        Arabic spelling (kinds root / sub_entry), forms by theirs (a matched
        form, kind headword). Built once from the open file; nothing new is
        stored in the sidecar."""
        if self._arabic_index is None:
            with self._index_lock:
                if self._arabic_index is None:
                    index: dict[str, list[dict[str, str]]] = {}

                    def add(spelling: Any, hit: dict[str, str]) -> None:
                        for akey in arabic_keys(_text(spelling)):
                            index.setdefault(akey, []).append(hit)

                    for r in self._conn.execute("SELECT id, arabic_spelling FROM roots ORDER BY ordinal"):
                        add(r["arabic_spelling"], {"kind": "root", "root_id": _text(r["id"]),
                                                   "sub_entry_id": "", "form_id": "", "raw": ""})
                    for r in self._conn.execute(
                            "SELECT id, root_id, arabic_spelling FROM sub_entries ORDER BY root_id, ordinal"):
                        add(r["arabic_spelling"], {"kind": "sub_entry", "root_id": _text(r["root_id"]),
                                                   "sub_entry_id": _text(r["id"]), "form_id": "", "raw": ""})
                    for r in self._conn.execute(
                            "SELECT id, root_id, sub_entry_id, arabic_spelling FROM forms "
                            "WHERE arabic_spelling <> '' ORDER BY id"):
                        add(r["arabic_spelling"], {"kind": "headword", "root_id": _text(r["root_id"]),
                                                   "sub_entry_id": _text(r["sub_entry_id"]),
                                                   "form_id": _text(r["id"]), "raw": ""})
                    self._arabic_index = {k: tuple(v) for k, v in index.items()}
        return self._arabic_index

    def _lookup_arabic(self, word: str, key: str, candidates: tuple[str, ...]) -> BlauLookup:
        index = self._arabic()
        cards: dict[tuple[str, str, str], _CardBuild] = {}
        overflow: set[tuple[str, str, str]] = set()
        roots: dict[str, Optional[sqlite3.Row]] = {}
        for candidate in candidates:
            hits = sorted(index.get(candidate, ()), key=lambda h: _KIND_RANK.get(h["kind"], len(HEADWORD_KINDS)))
            for hit in hits:
                self._add_hit(cards, set(), overflow, roots, hit, candidate)
        return BlauLookup(
            word=word, key=key, candidates=candidates,
            cards=tuple(self._finish(build, roots) for build in cards.values()),
            more_cards=len(overflow), examples=(), more_examples=0, script="arabic",
        )

    def _lookup(self, word: str, key: str, candidates: tuple[str, ...]) -> BlauLookup:
        # A card is one scope reached by one candidate: keyed by (candidate,
        # root, sub-entry), so two candidates that reach the same entry (כתאב
        # and תאב) stay two cards in candidate order, and a root hit and a
        # sub-entry hit under it are not one card with mixed senses (plan §6.2;
        # Codex on #473).
        cards: dict[tuple[str, str, str], _CardBuild] = {}
        card_roots: set[str] = set()
        overflow: set[tuple[str, str, str]] = set()
        phrase_rows: list[sqlite3.Row] = []
        roots: dict[str, Optional[sqlite3.Row]] = {}
        prefix_marks = ",".join("?" * len(NAME_PREFIX_KINDS))
        for candidate in candidates:
            rows = list(self._conn.execute(
                "SELECT norm, raw, kind, root_id, sub_entry_id, form_id FROM forms_norm "
                "WHERE norm = ? ORDER BY rowid", (candidate,)))
            rows.sort(key=lambda r: _KIND_RANK.get(r["kind"], len(HEADWORD_KINDS)))
            if " " not in candidate:
                # Entry names of several words that begin with this one (a
                # range on the lookup index: every key "candidate <more>").
                named = list(self._conn.execute(
                    "SELECT norm, raw, kind, root_id, sub_entry_id, form_id FROM forms_norm "
                    f"WHERE norm > ? AND norm < ? AND kind IN ({prefix_marks}) ORDER BY rowid",
                    (candidate + " ", candidate + " \U0010ffff", *NAME_PREFIX_KINDS)))
                named.sort(key=lambda r: _KIND_RANK.get(r["kind"], len(HEADWORD_KINDS)))
                rows.extend(named)
            for row in rows:
                kind = row["kind"]
                if kind in _KIND_RANK:
                    self._add_hit(cards, card_roots, overflow, roots, row, candidate)
                elif kind == PHRASE_KIND:
                    phrase_rows.append(row)
        examples: dict[str, BlauExample] = {}
        example_overflow: set[str] = set()
        for row in phrase_rows:
            root_id = _text(row["root_id"])
            if not root_id or root_id in card_roots or root_id in examples or root_id in example_overflow:
                continue
            root = self._root(roots, root_id)
            if root is None:
                continue
            if len(examples) >= MAX_EXAMPLES:
                example_overflow.add(root_id)
                continue
            examples[root_id] = BlauExample(root_id=root_id, name=_text(root["name_ja"]),
                                            url=entry_url(root_id), phrase=_text(row["raw"]))
        builds = list(cards.values())
        word_pattern = dot_pattern(word)
        if "*" in word_pattern:
            # The word carries the dot: its reading first (stable: the rest
            # keep candidate order), the others marked.
            for build in builds:
                build.dot_mismatch = not _dots_agree(word_pattern, build.matched_key)
            builds.sort(key=lambda b: b.dot_mismatch)
        return BlauLookup(
            word=word, key=key, candidates=candidates,
            cards=tuple(self._finish(build, roots) for build in builds),
            more_cards=len(overflow),
            examples=tuple(examples.values()),
            more_examples=len(example_overflow),
        )

    def _add_hit(self, cards: dict[tuple[str, str, str], _CardBuild], card_roots: set[str],
                 overflow: set[tuple[str, str, str]], roots: dict[str, Optional[sqlite3.Row]],
                 row: Mapping[str, Any], candidate: str) -> None:
        """One hit into its card. ``row`` is a ``forms_norm`` row, or the same
        fields as a dict for a typed-Arabic hit (``_arabic``)."""
        root_id = _text(row["root_id"])
        root = self._root(roots, root_id)
        if root is None:  # the gate forbids a dangling root; never trust it anyway
            return
        via = ""
        if root["is_cross_reference"] and _text(root["target_id"]):
            target = self._root(roots, _text(root["target_id"]))
            if target is not None:
                via = _text(root["name_ja"])
                root, root_id = target, _text(target["id"])
        # The scope: the sub-entry the key sits under, else the whole entry.
        sub_entry_id = _text(row["sub_entry_id"]) if row["kind"] != "root" else ""
        key = (candidate, root_id, sub_entry_id)
        build = cards.get(key)
        if build is None:
            if len(cards) >= MAX_CARDS:
                overflow.add(key)
                return
            build = cards[key] = _CardBuild(root, candidate, _text(row["raw"]), row["kind"], via, sub_entry_id)
            card_roots.add(root_id)
        form_id = _text(row["form_id"])
        if form_id and form_id not in build.matched_form_ids:
            build.matched_form_ids.append(form_id)

    def _finish(self, build: _CardBuild, roots: dict[str, Optional[sqlite3.Row]]) -> BlauCard:
        root = build.root
        root_id = _text(root["id"])
        senses: list[BlauSense] = []
        seen: set[str] = set()
        for form_id in build.matched_form_ids:
            form = self._conn.execute(f"SELECT {_FORM_COLUMNS} FROM forms WHERE id = ?", (form_id,)).fetchone()
            if form is not None and form_id not in seen:
                seen.add(form_id)
                senses.append(self._sense(form, roots, matched=True))
        # The scope is the matched sub-entry, else the whole entry. Both go
        # through the root index (there is none on sub_entry_id).
        if build.sub_entry_id:
            scope_sql, scope_args = "root_id = ? AND sub_entry_id = ?", (root_id, build.sub_entry_id)
        else:
            scope_sql, scope_args = "root_id = ?", (root_id,)
        total = int(self._conn.execute(f"SELECT COUNT(*) FROM forms WHERE {scope_sql}",
                                       scope_args).fetchone()[0])
        # Form ids follow the dictionary's own order (the builder numbers the
        # forms as it reads the entries); `ordinal` is per stem and would
        # interleave the stems.
        if len(senses) < MAX_SENSES:
            for form in self._conn.execute(
                    f"SELECT {_FORM_COLUMNS} FROM forms WHERE {scope_sql} ORDER BY id", scope_args):
                if len(senses) >= MAX_SENSES:
                    break
                form_id = _text(form["id"])
                if form_id in seen or not _has_content(form):
                    continue
                seen.add(form_id)
                senses.append(self._sense(form, roots, matched=False))
        shown = tuple(senses[:MAX_SENSES])
        sub_entry = ""
        if build.sub_entry_id:
            row = self._conn.execute("SELECT name_ja FROM sub_entries WHERE id = ?",
                                     (build.sub_entry_id,)).fetchone()
            sub_entry = _text(row["name_ja"]) if row is not None else ""
        return BlauCard(
            root_id=root_id, name=_text(root["name_ja"]), arabic=_text(root["arabic_spelling"]),
            page=_page(root["start_page_printed"]), url=entry_url(root_id),
            candidate=build.candidate, matched_key=build.matched_key, matched_kind=build.matched_kind,
            sub_entry=sub_entry, via=build.via, senses=shown,
            more_senses=max(0, total - len(shown)), dot_mismatch=build.dot_mismatch,
        )

    def _sense(self, form: sqlite3.Row, roots: dict[str, Optional[sqlite3.Row]], matched: bool) -> BlauSense:
        see: list[tuple[str, str]] = []
        target_ids = _text(form["target_ids"])
        if target_ids:
            try:
                ids = json.loads(target_ids)
            except ValueError:
                ids = []
            for target in ids if isinstance(ids, list) else []:
                root = self._target_root(roots, _text(target))
                if root is not None:
                    see.append((_text(root["name_ja"]), entry_url(_text(root["id"]))))
        return BlauSense(
            form_id=_text(form["id"]), ja_text=_text(form["ja_text"]), arabic=_text(form["arabic_spelling"]),
            binyan=_text(form["binyan"]), gloss_he=_text(form["gloss_he"]), gloss_en=_text(form["gloss_en"]),
            cross_reference=_text(form["cross_reference"]), see=tuple(see), page=_page(form["page"]),
            is_classical=bool(form["is_classical"]), is_reconstructed=bool(form["is_reconstructed"]),
            is_erroneous=bool(form["is_erroneous"]), matched=matched,
        )

    def _root(self, cache: dict[str, Optional[sqlite3.Row]], root_id: str) -> Optional[sqlite3.Row]:
        if root_id not in cache:
            cache[root_id] = self._conn.execute(
                "SELECT id, name_ja, arabic_spelling, start_page_printed, is_cross_reference, target_id "
                "FROM roots WHERE id = ?", (root_id,)).fetchone() if root_id else None
        return cache[root_id]

    def _target_root(self, cache: dict[str, Optional[sqlite3.Row]], target: str) -> Optional[sqlite3.Row]:
        """A form's cross-reference target: a root, or a stem (then its root)."""
        root = self._root(cache, target)
        if root is None and target:
            stem = self._conn.execute("SELECT root_id FROM stems WHERE id = ?", (target,)).fetchone()
            if stem is not None:
                root = self._root(cache, _text(stem["root_id"]))
        return root


def _validate(conn: Any) -> tuple[str, dict[str, str]]:
    """'' and the meta when the open file is one this reader serves; else the reason."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        return f"missing table(s): {', '.join(missing)}", {}
    meta = {str(k): ("" if v is None else str(v)) for k, v in conn.execute("SELECT key, value FROM meta")}
    if meta.get("version") != SCHEMA_VERSION:
        return f"schema version {meta.get('version')!r}; this reader understands {SCHEMA_VERSION}", {}
    if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
        return (f"keyed by normalizer {meta.get('normalizer_version')!r}; this process runs "
                f"{NORMALIZER_VERSION}: rebuild the sidecar"), {}
    if not meta.get("credit_he", "").strip():
        return "no credit line (meta.credit_he)", {}
    index = conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                         (LOOKUP_INDEX,)).fetchone()
    if index is None:
        return f"lookup index {LOOKUP_INDEX} missing", {}
    expected = meta.get("rows_forms_norm", "")
    actual = conn.execute("SELECT COUNT(*) FROM forms_norm").fetchone()[0]
    if not expected.isdigit() or int(expected) != int(actual):
        return f"meta.rows_forms_norm = {expected!r} but the table holds {actual} rows", {}
    check = conn.execute("PRAGMA quick_check").fetchone()
    if check is None or check[0] != "ok":
        return f"quick_check: {check[0] if check else 'no answer'}", {}
    return "", meta


# ---------------------------------------------------------------------------
# The process-wide instance (one per app, like the other sidecar services)
# ---------------------------------------------------------------------------

_default_service: Optional[BlauService] = None
_lock = threading.Lock()


def get_blau_service(thread_safe: bool = True) -> BlauService:
    """The default service, created on first use (per-thread connections by
    default: safe under NiceGUI's ``run.io_bound`` pool)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = BlauService(thread_safe=thread_safe)
        return _default_service


def reset_blau_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
