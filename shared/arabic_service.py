# -*- coding: utf-8 -*-
"""Read-only lookups in the Arabic dictionary sidecar (``arabic_data/arabic.db``): Lane's
*Arabic-English Lexicon* for a Judeo-Arabic or an Arabic word.

Phase B of docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md. scripts/build_arabic_sidecar.py builds the
file and scripts/verify_arabic_sidecar.py gates it; this module only reads it, decides nothing
about its content and never writes.

Fail-closed, as shared/blau_service.py. The service is READY only when the file is where a sidecar
lives, opens read-only, passes ``PRAGMA quick_check``, carries the schema version this reader
understands, was keyed by the normalizer this process runs (``meta.normalizer_version`` ==
``shared.arabic_normalize.NORMALIZER_VERSION``; otherwise every lookup would silently miss), holds
every table and lookup index a lookup reads, and carries the Perseus credit line (D3: a Lane card
without it must not show). Any of those failing leaves ``is_ready()`` False with the reason in
``unavailable_reason``, and ``lookup`` answers an empty result. ``is_available()`` is readiness AND
the ``LANE_DICTIONARY_ENABLED`` kill switch (default on, read live). Nothing here raises into a
page.

The lookup is the plan's method v4 (``shared.arabic_method``), the one the gate measured on this
file: a word in Hebrew letters is read as every Arabic skeleton it may be, the readings attested in
the OpenITI word count are ranked commonest first (``MAX_READINGS`` kept), and the first reading's
roots are found through the closed-class table, Lane's headwords, then stripping. A word typed in
Arabic letters is its own reading. No guessing at misread letters, and no judgement of the word's
language (owner, D11): a reader who asks for Arabic gets the Arabic readings.

Beyond that method (2026-11-13; measured on Blau's citations and on unseen documents): a word no entry of Lane's is, is tried as a form of a verb stem Lane numbers
under a root (``_verb_form_roots``, the rules of ``shared.ja_verb_forms`` that Blau's and
Friedman's tabs run on their own stems); such a root is added after the root the reading gave,
never ahead of it. And ``borrow`` adds Lane's entries of the roots Blau's and Friedman's answers
give for the word, marked as theirs.

Each root card holds Lane's entries under that root: the entries whose headword is the reading, or
the reading with a proclitic and a pronoun peeled, or the closed-class table's headword, first;
then the rest in Lane's order. Everything is bounded (``MAX_ROOTS``, ``MAX_ENTRIES``, ``MAX_TEXT``)
so one click never renders a page; a root carries all its entries and their whole text (the panel
shows the first few and the rest on a click), and only what lies beyond the
bound is counted and linked on Ephraim Meiri's site, where the entries come from (D5).
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import quote

from shared.arabic_method import (
    BACKOFF_ENCLITICS,
    BACKOFF_PROCLITICS,
    MIN_COUNT,
    closed_class_roots,
    rank_readings,
    reading_roots,
    strip_roots,
)
from shared.arabic_normalize import (
    NORMALIZER_VERSION,
    arabic_tokens,
    is_abbreviation,
    ja_letters,
    readings,
    root_key,
    skeleton,
    strip_harakat,
    strong_letters,
)
from shared.arabic_normalize import DOTTED, LETTERS
from shared.blau_normalize import (
    BRACKETS, QUOTE_MARKS, article_candidates, candidate_stems, lexical_key, normalize_ja, query_analyses,
)
from shared.dictionary_switch import OFF, ON, read_switch, serves
from shared.ja_roots import BORROW_LANE, BorrowedRoot, StemPair, carries_root, ja_root_of_arabic
from shared.ja_verb_forms import finite_form, stem_forms, verb_form_cards
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader understands; scripts/build_arabic_sidecar.py writes its own
#: ``SCHEMA_VERSION`` into ``meta.version`` and tests/test_arabic_service.py pins the two equal.
SCHEMA_VERSION = "1.0.0"
SIDECAR_DIR = "arabic_data"
SIDECAR_FILENAME = "arabic.db"
#: Ephraim Meiri's site, where the Lane entries come from (D5): an entry is ``?id=<entry id>``.
SITE_URL = "https://semitic.lang.emeiri.org/"
REQUIRED_TABLES = ("meta", "lane_roots", "lane_entries", "lane_forms", "freq", "closed_class")
#: Every table whose row count the builder records in ``meta.rows_<table>``: a table that lost rows
#: (an empty closed-class table sends דלך to the wrong root) is not served.
COUNTED_TABLES = ("lane_roots", "lane_entries", "lane_forms", "freq", "closed_class")
#: The Perseus credit line, verbatim (D3); scripts/build_arabic_sidecar.py checks the source against
#: the same words and tests/test_arabic_service.py pins the two copies equal.
PERSEUS_CREDIT = ("Text provided by Perseus Digital Library, with funding from The U.S. Department of "
                  "Education and The Max Planck Society.")
LOOKUP_INDEXES = ("idx_lane_entries_root", "idx_lane_forms_skeleton")
#: The method the gate measured (plan section 7.1) and the floor it measured at (D11: 2).
METHOD_VERSION = "v4"
MAX_READINGS = 3
MAX_ROOTS = 1  # the gate measured the first root only (P1)
#: A root card carries every entry of its root, each with its whole text (nothing
#: only on the site); the panel shows the first few, each cut at ``MAX_TEXT``, and the rest on a
#: click. A bound, not a cut: the largest root has 69 entries (34,090 characters in all).
MAX_ENTRIES = 100
#: A word no headword and no counted reading explains is tried as a form of a verb stem Lane numbers
#: under a root (``shared.ja_verb_forms``, the rules Blau's and Friedman's tabs run on their own
#: stems): the kinds of hit that open a root (``hit_kind``: the word's own key or a prefixed stem; a
#: short or clitic-peeled form of stem I, II or IV only with a subject ending), and how many roots.
VERB_FORM_KINDS = frozenset("ABD")
MAX_VERB_FORM_ROOTS = 2
#: The roots Blau's and Friedman's answers give for the word (``ArabicService.borrow``): how many
#: of them Lane's tab adds, and how many Lane roots one of them may be when its letters do not say.
MAX_BORROWED_ROOTS = 2
MAX_BORROWED_KEYS = 2
#: The letters a root key in Hebrew letters may lose in a form (``_ja_strong``).
_JA_WEAK = "אוי"
MAX_TEXT = 600


ENV_ENABLED = "LANE_DICTIONARY_ENABLED"


def lane_switch() -> str:
    """The ``LANE_DICTIONARY_ENABLED`` switch, read on every call (default on): on, off, or
    ``editors`` (``shared.dictionary_switch``)."""
    return read_switch(ENV_ENABLED, ON)


def lane_enabled() -> bool:
    """The switch is on for everyone (not ``editors`` only)."""
    return lane_switch() == ON


def entry_url(entry_id: str) -> str:
    """One Lane entry on Ephraim Meiri's site."""
    return f"{SITE_URL}?id={quote(str(entry_id), safe='')}"


def root_url(root: str) -> str:
    """Lane's entries under a root on Ephraim Meiri's site (a headword search in Lane)."""
    return f"{SITE_URL}?q={quote(str(root), safe='')}&source=lane"


def default_sidecar_path() -> Optional[str]:
    """Where the sidecar lives: the user's updated copy first (as for the Blau, FGP and PGP
    sidecars), then the repository's ``arabic_data/arabic.db``."""
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


# ---------------------------------------------------------------------------
# What a lookup answers: plain frozen records a renderer draws without a decision of its own.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LaneReading:
    """One Arabic reading of the word: its skeleton, its commonest spelling in the word count
    (with the hamza and the ة the skeleton folds away) and its count."""
    skeleton: str
    spelling: str
    count: int


@dataclass(frozen=True)
class LaneEntry:
    """One Lane entry: the headword, the printed page, the text (cut at ``MAX_TEXT``)."""
    entry_id: str
    headword: str
    page: Optional[int]
    text: str
    cut: bool
    url: str
    #: Its headword is the reading (or the reading's stem, or the closed-class table's headword).
    matched: bool
    #: The whole text when ``text`` is cut ('' otherwise): the panel shows it on a click.
    full: str = ""
    #: The printed evidence behind a match; empty for a root-only inference.
    match_kind: str = ""
    matched_form: str = ""


@dataclass(frozen=True)
class LaneRoot:
    """One root the first reading may come from, with its entries."""
    root_key: str
    name: str
    page: Optional[int]
    url: str
    entries: tuple[LaneEntry, ...]
    more_entries: int
    inferred: bool = False
    #: A root another dictionary gave for the word (``ArabicService.borrow``): (the dictionary,
    #: what it gave it by), as ``BlauCard.borrowed``. Empty for a root Lane's own reading found.
    borrowed: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class LaneLookup:
    word: str
    #: "ja" (Hebrew letters) or "arabic" (a typed Arabic-script word).
    script: str
    readings: tuple[LaneReading, ...]
    roots: tuple[LaneRoot, ...]
    problem: str = ""

    @property
    def found(self) -> bool:
        return bool(self.roots)

    @property
    def reading(self) -> Optional[LaneReading]:
        """The reading the root cards are for: the first."""
        return self.readings[0] if self.readings else None


#: Marks written inside one Judeo-Arabic word: the quote marks ``ja_letters`` reads as the dot,
#: and the editorial brackets of a restored letter (ד[ל]ך). Nonspacing marks (the combining dot,
#: niqqud) are dropped too.
_IN_WORD_MARKS = QUOTE_MARKS | BRACKETS


def one_ja_word(word: str) -> bool:
    """``word`` is ONE Judeo-Arabic word: no space at all, and, NFC-normalized as ``ja_letters``
    reads it, with the in-word marks dropped, a single run of Hebrew letters. A space, a maqaf
    (כל־מה) or a hyphen between letters makes two, which ``ja_letters`` would read as one (كلمة)."""
    if len(word.split()) != 1:
        return False
    text = unicodedata.normalize("NFC", word)
    kept = "".join(ch for ch in text if ch not in _IN_WORD_MARKS and unicodedata.category(ch) != "Mn")
    if any(ch.isalpha() and not "א" <= ch <= "ת" for ch in kept):
        return False  # another script joined on (דלך-كتاب): two words
    return len(re.findall("[א-ת]+", kept)) == 1


def _ja_strong(key: str) -> str:
    """A three-letter root key in Hebrew letters without the letters a form may lose (א ו י), a
    doubled letter once: what every form of the root holds, in order."""
    out: list[str] = []
    for ch in key:
        if ch not in _JA_WEAK and (not out or out[-1] != ch):
            out.append(ch)
    return "".join(out)


def _in_order(letters: str, word: str) -> bool:
    """Does ``word`` hold ``letters`` in order?"""
    rest = iter(word)
    return all(ch in rest for ch in letters)


#: The Arabic letters a form adds to its root's strong letters: the weak letters, the letters of
#: the stems' prefixes and infixes (with ط and د, stem VIII's after an emphatic or ز), of the
#: endings and of the clitics. ث, خ, ذ, ض, ظ, غ and ج are never one: a dotted letter is the root's.
_AFFIX_LETTERS = frozenset("اويتسنمهبلكفطد")


def _reads_as(strong: str, reading: str) -> bool:
    """Is ``reading`` a form of a root with these strong letters: it holds them in order, and every
    other letter is one a form adds (``_AFFIX_LETTERS``) or the root letter before it again (a
    doubled root written out: يقرر)? يثبت is no form of بتت: its ث is no prefix."""
    def walk(i: int, j: int) -> bool:
        if i == len(reading):
            return j == len(strong)
        ch = reading[i]
        if j < len(strong) and ch == strong[j] and walk(i + 1, j + 1):
            return True
        return (ch in _AFFIX_LETTERS or (j > 0 and ch == strong[j - 1])) and walk(i + 1, j)
    return walk(0, 0)


def _letters_shown(root: str, form: str) -> int:
    """How many of a root key's three letters ``form`` writes, in order (the longest run of them it
    holds): נזל writes all of נזל and two of זול."""
    best = [0] * (len(form) + 1)
    for letter in root:
        row = [0]
        for n, ch in enumerate(form):
            row.append(best[n] + 1 if ch == letter else max(best[n + 1], row[n]))
        best = row
    return best[-1]


def empty_lane(word: str = "", script: str = "ja") -> LaneLookup:
    return LaneLookup(word=word, script=script, readings=(), roots=())


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _page(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


#: A perfect verb's subject endings, longest first, that a Lane headword never carries (it cites the
#: third person: طالبهُ, not طالبتهُ).
_SUBJECT_ENDINGS = ("تما", "تم", "تن", "نا", "ت")
_OBJECT_PRONOUNS = tuple(p for p in BACKOFF_ENCLITICS if p)
# A missing pronoun entry must not turn into a guessed verb by removing its first letter.
_PERSONAL_PRONOUNS = frozenset(('انا', 'نحن', 'انت', 'انتما', 'انتم', 'انتن', 'هو', 'هي', 'هما', 'هم', 'هن'))


def _head_shapes(root_key: str, reading: str) -> list[str]:
    """The reading as a Lane headword may cite it (Lane matched none of its entries
    for أيضا, طالبته, استقر): the reading; without the alif of an adverbial accusative (أيضا -> أيض);
    without a perfect's subject ending, keeping an object pronoun (طالبته -> طالبه, طالبت -> طالب);
    and, for a doubled root, with its contracted last letter written twice (استقر -> استقرر).
    Matching only: which entries are marked and come first, never which root is found."""
    shapes = [reading]
    if reading.endswith("ا") and len(reading) >= 4 and not reading.endswith(("نا", "وا", "تما")):
        shapes.append(reading[:-1])   # not a verb's ending: كتبنا, كتبوا
    for shape in list(shapes):
        for pronoun in ("", *_OBJECT_PRONOUNS):
            for ending in _SUBJECT_ENDINGS:
                if shape.endswith(ending + pronoun) and len(shape) - len(ending + pronoun) >= 2:
                    shapes.append(shape[:len(shape) - len(ending + pronoun)] + pronoun)
                    break
    if len(root_key) == 3 and root_key[1] == root_key[2] and root_key[2] not in "Wء":
        last = root_key[2]
        shapes += [s + last for s in list(shapes) if s.endswith(last) and not s.endswith(last + last)]
    return list(dict.fromkeys(shapes))


def cut_text(text: str, limit: int = MAX_TEXT) -> tuple[str, bool]:
    """``text`` up to ``limit`` characters, ended at a space where one is near, and whether
    anything was cut."""
    text = " ".join(text.split())
    if len(text) <= limit:
        return text, False
    head = text[:limit]
    space = head.rfind(" ")
    if space > limit * 0.8:
        head = head[:space]
    return head.rstrip(" ,;:") + "…", True


def lane_heading(names: Sequence[str], entries: Sequence[tuple[str, str]], matched: set[str]) -> str:
    """Which of a root key's Lane headings titles the card, when the key has several (Lane files
    بوع and بيع, or سود and سيد, apart; one root key holds both): the heading of the first matched
    entry, in Lane's order (سيدي: سَيِّدٌ under سود); else the heading with the most entries, the
    commoner sense (يبيعه: بيع 'sell', not بوع 'fathom'; the method breaks a tie between roots the
    same way); else '' (a tie; the caller keeps the key's first name). Not the letters of the word:
    a ي in it does not make the root's (سيّد and جيّد are of سود and جود).
    ``entries`` is (entry id, the heading it is under) in Lane's order."""
    names = [n for n in names if n]
    if len(names) < 2:
        return ""
    for entry_id, heading in entries:
        if entry_id in matched and heading in names:
            return heading
    sizes = Counter(heading for _entry, heading in entries if heading in names).most_common(2)
    if sizes and (len(sizes) == 1 or sizes[0][1] > sizes[1][1]):
        return sizes[0][0]
    return ""


class _FreqView:
    """The word count for one lookup: ``.get`` like a Counter, read from ``freq`` by key and kept
    for the lookup's life. ``prefetch`` reads many keys in one query."""

    def __init__(self, conn: Any):
        self._conn = conn
        self._rows: dict[str, Optional[tuple[int, str]]] = {}

    def __bool__(self) -> bool:  # shared.arabic_method treats a falsy count as "no count"
        return True

    def prefetch(self, keys: Iterable[str]) -> None:
        wanted = [k for k in dict.fromkeys(keys) if k and k not in self._rows]
        for start in range(0, len(wanted), 500):
            chunk = wanted[start:start + 500]
            for k in chunk:
                self._rows[k] = None
            marks = ",".join("?" * len(chunk))
            for key, count, surface in self._conn.execute(
                    f"SELECT skeleton, count, surface FROM freq WHERE skeleton IN ({marks})", chunk):
                self._rows[key] = (int(count), _text(surface))

    def _row(self, key: str) -> Optional[tuple[int, str]]:
        if key not in self._rows:
            self.prefetch([key])
        return self._rows[key]

    def get(self, key: str, default: int = 0) -> int:
        row = self._row(key)
        return row[0] if row is not None else default

    def spelling(self, key: str) -> str:
        row = self._row(key)
        return row[1] if row is not None and row[1] else key


def _feminine_shapes(reading):
    """Construct -t and attached suffixes, validated against Lane's lexical forms."""
    out = []
    for prefix in BACKOFF_PROCLITICS:
        if not reading.startswith(prefix):
            continue
        stem = reading[len(prefix):]
        for suffix in ("", "ك", "ه", "ها", "هم", "نا", "ي"):
            base = stem[:-len(suffix)] if suffix and stem.endswith(suffix) else stem if not suffix else ""
            if len(base) >= 3 and base.endswith("ت"):
                out.append(base[:-1] + "ه")
    return tuple(dict.fromkeys(out))


def _printed_form_lists(text: str, marker: str) -> tuple[str, ...]:
    """Only the Arabic lists immediately following a grammatical label."""
    # Citations and parenthetical qualifications can interrupt the printed list.
    for _ in range(8):
        plain = re.sub(r'\([^()]*\)', '', text)
        if plain == text:
            break
        text = plain
    arabic = r'[\u0621-\u064a\u064b-\u065f\u0670\u0671]+'
    lists = re.finditer(marker + r'\s*(' + arabic
                        + r'(?:\s*(?:,|and|or)\s*' + arabic + r')*)', text)
    return tuple(dict.fromkeys(word for match in lists
                              for word in re.findall(arabic, match[1])))


def explicit_plural_forms(text: str, root: str = '') -> tuple[str, ...]:
    """Printed plurals, including 'pl. of X is Y' and a weak final yod under nunation."""
    arabic = r'[\u0621-\u064a\u064b-\u065f\u0670\u0671]+'
    marker = r'\bpl\.\s*(?:of\s+' + arabic + r'\s+(?:is|are)\s+|(?:(?:is|are)\s+)?)'
    forms = _printed_form_lists(text, marker)
    keys = [skeleton(form) for form in forms]
    if root.endswith('W'):
        keys.extend(skeleton(form) + 'ي' for form in forms
                    if '\u064d' in form and not skeleton(form).endswith(('ي', 'ه')))
    return tuple(dict.fromkeys(keys))


def explicit_verbal_nouns(text: str) -> tuple[str, ...]:
    """Verbal nouns printed after 'inf. n.', not arbitrary Arabic quotations."""
    return tuple(dict.fromkeys(skeleton(form) for form in
                              _printed_form_lists(text, r'\binf\.\s+ns?\.')))


def _reading_stems(reading: str):
    """Clitic alternatives with a cost; preserve an unpeeled final root letter first."""
    for pro in BACKOFF_PROCLITICS:
        if not reading.startswith(pro):
            continue
        for enc in BACKOFF_ENCLITICS:
            if not reading.endswith(enc):
                continue
            stem = reading[len(pro):len(reading) - len(enc)]
            if len(stem) >= 2:
                yield stem, .25 * len(pro) + .75 * bool(enc)


class ArabicService:
    """Read-only access to the Arabic sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        self._roots: set[str] = set()
        self._prior: Counter = Counter()
        self._forms: dict[str, set[str]] = {}
        self._plural_entries: dict[tuple[str, str], set[str]] = defaultdict(set)
        self._verbal_noun_entries: dict[tuple[str, str], set[str]] = defaultdict(set)
        self._head_forms: dict[str, set[str]] = {}
        self._verb_entries: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
        self._entry_stems: dict[str, int] = {}
        self._verb_by_strong: dict[str, list[StemPair]] = {}
        self._closed: dict[str, list[str]] = {}
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not self._db_path:
            self.unavailable_reason = "no sidecar path"
            return
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("ArabicService: %s", self.unavailable_reason)
            return
        conn = None
        try:
            uri = f"file:{Path(self._db_path).resolve().as_posix()}?mode=ro"
            if thread_safe:
                conn = ThreadLocalConnection(uri, timeout=10.0)
            else:
                conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
            problem, meta = _validate(conn)
            if not problem:
                self._load_tables(conn)
        except Exception as exc:  # a file that is not a database, a locked file, ...
            problem, meta = f"cannot open: {exc}", {}
        if problem:
            self.unavailable_reason = problem
            logger.warning("ArabicService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("ArabicService: serving %s (schema %s, %s Lane entries, %s frequency rows)",
                    self._db_path, meta.get("version"), meta.get("rows_lane_entries"), meta.get("rows_freq"))

    def _load_tables(self, conn: Any) -> None:
        """The small tables the method reads on every lookup, held in memory (as the gate's
        ``load_sidecar`` reads them): root keys, the article size per root (the tie-break prior),
        the headword table and the closed-class table. The word count stays on disk."""
        self._roots = {r[0] for r in conn.execute("SELECT root_key FROM lane_roots")}
        self._prior = Counter(dict(conn.execute(
            "SELECT root_key, SUM(LENGTH(text)) FROM lane_entries GROUP BY root_key").fetchall()))
        forms: dict[str, set[str]] = defaultdict(set)
        for form, key in conn.execute("SELECT skeleton, root_key FROM lane_forms"):
            forms[form].add(key)
        self._head_forms = {form: set(keys) for form, keys in forms.items()}
        for eid, key, text in conn.execute("SELECT id, root_key, text FROM lane_entries WHERE text LIKE '%pl.%' OR text LIKE '%inf.%'"):
            for form in explicit_plural_forms(text, key):
                forms[form].add(key)
                self._plural_entries[(key, form)].add(eid)
            for form in explicit_verbal_nouns(text):
                forms[form].add(key)
                self._verbal_noun_entries[(key, form)].add(eid)
        self._forms = dict(forms)
        for eid, key, root, text in conn.execute(
                "SELECT id, root_key, root, text FROM lane_entries WHERE text GLOB '[1-9][.:]*' OR text GLOB '10[.:]*'"):
            stem = re.match(r'^(10|[1-9])[.:]\s', text)
            ja = lexical_key(root)
            if len(ja) == 2:
                ja += ja[-1]
            if stem and len(ja) == 3:
                self._verb_entries[key].append((eid, ja, int(stem[1])))
                self._entry_stems[eid] = int(stem[1])
        # Every numbered verb entry by its root's strong letters: the roots a word's letters may hold.
        by_strong: dict[str, list[StemPair]] = defaultdict(list)
        for key, entries in self._verb_entries.items():
            for eid, ja, number in entries:
                if _ja_strong(ja):
                    by_strong[_ja_strong(ja)].append(StemPair(key, ja, number, eid, key))
        self._verb_by_strong = dict(by_strong)
        closed: dict[str, list[str]] = defaultdict(list)
        for key, rkey in conn.execute("SELECT skeleton, root_key FROM closed_class ORDER BY ordinal"):
            if rkey not in closed[key]:
                closed[key].append(rkey)
        self._closed = dict(closed)

    # -- state -------------------------------------------------------------

    @property
    def db_path(self) -> Optional[str]:
        return self._db_path

    def is_ready(self) -> bool:
        """The sidecar is open and passed every check."""
        return self._conn is not None

    def is_available(self) -> bool:
        """Ready AND the switch is on for everyone: what a surface asks that does not know who
        is reading (an ``editors`` switch stays hidden there)."""
        return self.is_ready() and lane_enabled()

    def served_to(self, editor: bool) -> bool:
        """Ready AND shown to this reader: on for everyone, or ``editors`` and the reader an editor
        or admin (``shared.dictionary_switch``; a preview before release)."""
        return self.is_ready() and serves(lane_switch(), editor)

    def _answering(self) -> bool:
        """The lookups answer while the switch is not off: the page decided who may ask."""
        return self.is_ready() and lane_switch() != OFF

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    @property
    def credit(self) -> str:
        """The Perseus credit line (D3), verbatim; shown on every Lane card."""
        return self._meta.get("lane_credit", "")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self.unavailable_reason = "closed"

    # -- the lookup ----------------------------------------------------------

    def lookup(self, word: str) -> LaneLookup:
        """What Lane has for one clicked or typed word. Hebrew letters are read as Judeo-Arabic;
        Arabic letters only are their own reading. Never raises; empty when unavailable, when the
        word is an abbreviation or has neither script, or on any error."""
        word = _text(word)
        if not word or not self._answering():
            return replace(empty_lane(word), problem="unavailable" if word else "")
        if len(word.split()) > 1:
            return replace(empty_lane(word), problem="word_only")
        script = "ja"
        try:
            counts = _FreqView(self._conn)
            all_readings = ()
            if ja_letters(word):
                # One word only: ja_letters reads past a space or a maqaf, so "כל מה" would be كلمة.
                if is_abbreviation(word) or not one_ja_word(word):
                    return replace(empty_lane(word), problem="unsupported")
                all_readings = readings(word, plural_alif=True)
                counts.prefetch(all_readings)
                ranked = rank_readings(word, counts, "frequency", METHOD_VERSION, MIN_COUNT)[:MAX_READINGS]
                found = tuple(LaneReading(r, counts.spelling(r), c) for r, c in ranked)
                for analysis in query_analyses(lexical_key(word)):
                    if 'spelling_alias' in analysis.steps:
                        aliases = rank_readings(analysis.key, counts, "frequency", METHOD_VERSION, MIN_COUNT)
                        alias_readings = tuple(LaneReading(r, counts.spelling(r), c) for r, c in aliases
                                               if r in self._closed)
                        if alias_readings:
                            found = alias_readings + tuple(r for r in found if r.skeleton not in {a.skeleton for a in alias_readings})
            else:
                script = "arabic"
                tokens = list(arabic_tokens(word))
                key = skeleton(tokens[0]) if len(tokens) == 1 else ""
                if not key:
                    return replace(empty_lane(word, script), problem="unsupported")
                found = (LaneReading(key, strip_harakat(tokens[0]), counts.get(key, 0)),)
            # The word a verb form is looked for in (``_verb_form_roots`` reads forms in Hebrew
            # letters): the Judeo-Arabic word itself, or an Arabic word letter for letter, read as
            # itself alone (يستعملون reached nothing, and יסתעמלון reached عمل).
            verb_word = word if script == "ja" else ja_root_of_arabic(key)
            verb_readings = all_readings if script == "ja" else (key,)
            inferred_readings = set()
            if found or all_readings:
                extra = []
                for reading_key in dict.fromkeys([*(r.skeleton for r in found), *all_readings]):
                    for candidate in _feminine_shapes(reading_key):
                        if candidate in self._forms and candidate not in inferred_readings and all(r.skeleton != candidate for r in found):
                            extra.append(LaneReading(candidate, counts.spelling(candidate), counts.get(candidate, 0)))
                            inferred_readings.add(candidate)
                found = found + tuple(extra)
            if not found:
                derived = self._verb_form_roots(verb_word, verb_readings, counts) if verb_word else []
                return LaneLookup(word=word, script=script,
                                  readings=tuple(LaneReading(r, counts.spelling(r), counts.get(r, 0))
                                                 for r in dict.fromkeys(r for _root, r in derived)),
                                  roots=tuple(root for root, _r in derived))
            first = found[0]
            inferred_first = first.skeleton in inferred_readings
            bare = first.skeleton[1:] if first.skeleton[:1] in 'وف' else first.skeleton
            if (bare in _PERSONAL_PRONOUNS and not closed_class_roots(first.skeleton, self._closed)
                    and first.skeleton not in self._head_forms):
                return LaneLookup(word=word, script=script, readings=found, roots=())
            keys = reading_roots(first.skeleton, surface={first.skeleton: first.spelling}, roots=self._roots,
                                 prior=self._prior, forms=self._forms, counts=counts, closed={} if inferred_first else self._closed,
                                 version=METHOD_VERSION)
            # Printed nominal forms retain their final consonants ahead of suffix guesses.
            if inferred_first or not closed_class_roots(first.skeleton, self._closed):
                attested = {}
                for form, cost in _reading_stems(first.skeleton):
                    for root in self._forms.get(form, ()):
                        score = cost + (0 if root in self._head_forms.get(form, ()) else .15)
                        attested[root] = min(attested.get(root, 99), score)
                if attested:
                    keys = sorted(attested, key=lambda root: (attested[root], keys.index(root) if root in keys else len(keys), root))
            finite = self._finite_entries(first.skeleton) if not inferred_first else {}
            bases = (first.skeleton, first.skeleton[1:]) if first.skeleton[:1] in 'وف' else (first.skeleton,)
            full_finite = any(first.skeleton in forms.values() for forms in finite.values())
            if finite and first.skeleton not in self._head_forms and (full_finite or not any(base in self._head_forms for base in bases)):
                keys = list(finite) + [key for key in keys if key not in finite]
            roots = [self._root(key, first.skeleton, inferred=inferred_first, finite=finite.get(key)) for key in keys[:MAX_ROOTS]]
            seen = set(keys[:MAX_ROOTS])
            for reading in found[1:]:
                if reading.skeleton not in _feminine_shapes(first.skeleton):
                    continue
                for key in sorted(self._forms.get(reading.skeleton, ())):
                    if key not in seen:
                        roots.append(self._root(key, reading.skeleton, inferred=True))
                        seen.add(key)
                    elif key not in finite:
                        # A constructed form and its noun may belong to the same root.
                        roots = [self._root(key, reading.skeleton, inferred=True) if r and r.root_key == key else r for r in roots]
            roots = [r for r in roots if r is not None]
            if verb_word and not any(e.matched for r in roots for e in r.entries):
                # No entry of Lane's is the word: the roots it is a verb form of are added. The root
                # the reading gave stays first (measured 2026-11-13 on unseen documents: a form's root
                # ahead of it replaced a right root more often than a wrong one), with its verb
                # entries marked when the word is a form of it too.
                own = roots[0].root_key if roots else ""
                derived = self._verb_form_roots(verb_word, verb_readings, counts, own=own)
                if derived:
                    forms = {root.root_key: root for root, _r in derived}
                    roots = ([forms.pop(own, None) or roots[0]] if roots else []) + list(forms.values()) + [
                        r for r in roots[1:] if r.root_key not in forms]
                    known = {r.skeleton for r in found}
                    found = found + tuple(LaneReading(r, counts.spelling(r), counts.get(r, 0))
                                          for r in dict.fromkeys(r for _root, r in derived) if r not in known)
            return LaneLookup(word=word, script=script, readings=found, roots=tuple(roots))
        except Exception:
            logger.exception("ArabicService: lookup of %r failed", word)
            return replace(empty_lane(word, script), problem="error")

    def _verb_form_roots(self, word: str, all_readings: Sequence[str], counts: Any,
                         own: str = "") -> list[tuple[LaneRoot, str]]:
        """The roots ``word`` is a verb form of, each with the reading it is one in: a form of a stem
        Lane numbers under the root (``shared.ja_verb_forms.stem_forms``), of a kind in
        ``VERB_FORM_KINDS`` or with a subject ending, one of the word's readings being a form of
        the root (``_reads_as``: a dot in the word decides between حضر and حصر). First ``own`` (the
        root Lane's reading already gave, by any form of its own), then the root the form writes most
        letters of (ينزل is نزل before stem VII of زول), then as ``verb_form_cards`` orders them,
        then by the reading's count; at most ``MAX_VERB_FORM_ROOTS`` beside ``own``."""
        key = lexical_key(word)
        if not key or " " in key or not self._verb_by_strong:
            return []
        candidates = candidate_stems(key)
        index: dict[str, list[StemPair]] = {}
        for cand in candidates:
            letters = [ch for ch in cand if ch not in _JA_WEAK]
            for signature in {"".join(c) for n in (1, 2, 3) for c in combinations(letters, n)}:
                for pair in self._verb_by_strong.get(signature, ()):
                    if cand in stem_forms(pair.root_key, pair.stem):
                        index.setdefault(cand, []).append(pair)
        if not index:
            return []
        ranked = sorted(all_readings, key=lambda r: -counts.get(r, 0))
        cards = []
        for card in verb_form_cards(candidates, index, article_candidates(key)):
            if card.kind not in VERB_FORM_KINDS and card.entry_id != own and not card.finite_for(key):
                continue   # the root Lane's reading gave stands among them by any form of its own
            strong = strong_letters(card.entry_id)
            reading = next((r for r in ranked if _reads_as(strong, r)), "")
            if reading:
                cards.append((card, reading))
        def shown(card: Any) -> int:
            return max(_letters_shown(card.root_key, k) for k in card.hit_keys)

        cards.sort(key=lambda found: (found[0].entry_id != own, -shown(found[0]), found[0].rank(),
                                      -counts.get(found[1], 0), -self._prior.get(found[0].entry_id, 0)))
        limit = MAX_VERB_FORM_ROOTS + bool(cards and cards[0][0].entry_id == own)
        out: list[tuple[LaneRoot, str]] = []
        for card, reading in cards[:limit]:
            entries = {pair.unit_id: reading for pairs in index.values() for pair in pairs
                       if pair.entry_id == card.entry_id and pair.stem in card.stems}
            root = self._root(card.entry_id, reading, inferred=True, finite=entries)
            if root is not None:
                out.append((root, reading))
        return out

    # -- roots borrowed from the other dictionaries -------------------------

    def borrow(self, result: LaneLookup, borrowed: Sequence[BorrowedRoot]) -> LaneLookup:
        """``result`` with Lane's entries of the roots Blau's and Friedman's answers give for the
        word (``shared.ja_roots.borrowed_roots``), each marked with the dictionary that gave it
        (``LaneRoot.borrowed``). A root Lane's own reading found is not repeated, and only a root
        the word holds the strong letters of is taken (``shared.ja_roots.carries_root``: the other
        dictionaries also list roots the word is only a clipped form of). Lane's own root stays
        first when one of its entries is the word; otherwise the borrowed roots come first (a
        root guessed from the letters gives way to one a dictionary lists the word under). Not for
        a personal pronoun, which Lane answers with no root on purpose. Never raises: on any error
        the result comes back as it was."""
        if not isinstance(result, LaneLookup) or not borrowed or result.problem or not self._answering():
            return result
        first = result.readings[0].skeleton if result.readings else ""
        if not result.roots and (first[1:] if first[:1] in "وف" else first) in _PERSONAL_PRONOUNS:
            return result
        try:
            return self._borrow(result, borrowed)
        except Exception:
            logger.exception("ArabicService: borrowing roots for %r failed", result.word)
            return result

    def _borrow(self, result: LaneLookup, borrowed: Sequence[BorrowedRoot]) -> LaneLookup:
        word_readings = [r.skeleton for r in result.readings]
        if result.script == "ja":
            counts = _FreqView(self._conn)
            every = [r for r in readings(result.word, plural_alif=True) if r not in word_readings]
            counts.prefetch(every)
            word_readings += sorted(every, key=lambda r: -counts.get(r, 0))
        own = {root.root_key for root in result.roots}
        word_key = lexical_key(result.word)
        added: dict[str, LaneRoot] = {}
        for item in borrowed:
            if item.source == BORROW_LANE or not carries_root(word_key, normalize_ja(item.root)):
                continue
            tag = (item.source, item.evidence)
            for key in self._lane_keys(item.root, word_readings):
                if key in own:
                    continue
                if key in added:
                    if tag not in added[key].borrowed:
                        added[key] = replace(added[key], borrowed=added[key].borrowed + (tag,))
                    continue
                if len(added) >= MAX_BORROWED_ROOTS:
                    continue
                strong = strong_letters(key)
                reading = next((r for r in word_readings if _in_order(strong, r)), "")
                root = self._root(key, reading)
                if root is not None:
                    added[key] = replace(root, borrowed=(tag,))
        if not added:
            return result
        own_first = bool(result.roots) and any(entry.matched for entry in result.roots[0].entries)
        roots = (*result.roots, *added.values()) if own_first else (*added.values(), *result.roots)
        return replace(result, roots=roots)

    def _lane_keys(self, root: str, word_readings: Sequence[str]) -> list[str]:
        """Lane's root keys for a root in Hebrew letters as Blau or Friedman prints it (dots kept):
        the root read as printed (a dotted letter its dotted reading, a plain one its plain); only
        when Lane has no such root, its other readings, those whose letters stand in one of the
        word's readings first. An א after the first letter is also a hamza (גזא: جزأ). At most
        ``MAX_BORROWED_KEYS``; [] for anything but a root of two to four letters."""
        letters = ja_letters(root)
        if not 2 <= len(letters) <= 4 or len(root.split()) != 1 or any(l not in LETTERS for l, _d in letters):
            return []

        def keys_of(arabic: str) -> list[str]:
            shapes = [arabic] + [arabic[:i] + "ء" + arabic[i + 1:] for i, ch in enumerate(arabic) if i and ch == "ا"]
            return [k for k in dict.fromkeys(root_key(shape) for shape in shapes) if k in self._roots]

        printed = "".join(DOTTED[letter] if dotted else LETTERS[letter][0] for letter, dotted in letters)
        found = keys_of(printed)
        if not found:
            others = list(dict.fromkeys(k for arabic in readings(root) if arabic != printed for k in keys_of(arabic)))
            fits = [k for k in others if any(_in_order(strong_letters(k), r) for r in word_readings)]
            found = fits or others
        return found[:MAX_BORROWED_KEYS]

    def _matched_entries(self, root_key: str, reading: str) -> set[str]:
        """The entries of ``root_key`` whose headword is the reading, or the reading with a
        proclitic and a pronoun peeled (as ``form_roots`` peels them), or whose closed-class row
        names them. Only when the reading itself names no entry, its other shapes a headword cites it
        by (``_head_shapes``: أيضا as أيض, طالبته as طالبه, استقر as استقرر): a reading Lane cites as
        it is (بنت) never marks another headword its shapes spell (بن)."""
        shapes = _head_shapes(root_key, reading)
        ids = self._forms_matching(root_key, shapes[:1])
        if not ids and len(shapes) > 1:
            ids = self._forms_matching(root_key, shapes[1:])
        closed = {reading} | {reading[1:] for pro in ("و", "ف") if reading.startswith(pro)}
        marks = ",".join("?" * len(closed))
        ids |= {r[0] for r in self._conn.execute(
            f"SELECT entry_id FROM closed_class WHERE root_key = ? AND entry_id IS NOT NULL "
            f"AND skeleton IN ({marks})", (root_key, *sorted(closed)))}
        return ids

    def _forms_matching(self, root_key: str, shapes: list[str]) -> set[str]:
        """The entries of ``root_key`` whose headword is one of ``shapes``, or one of them with a
        proclitic and a pronoun peeled (as ``form_roots`` peels them)."""
        stems = set()
        for shape in shapes:
            stems.add(shape)
            for pro in BACKOFF_PROCLITICS:
                if not shape.startswith(pro):
                    continue
                for enc in BACKOFF_ENCLITICS:
                    if shape.endswith(enc):
                        stem = shape[len(pro):len(shape) - len(enc)]
                        if len(stem) >= 2:
                            stems.add(stem)
        if not stems:
            return set()
        marks = ",".join("?" * len(stems))
        found = {r[0] for r in self._conn.execute(
            f"SELECT entry_id FROM lane_forms WHERE root_key = ? AND skeleton IN ({marks})",
            (root_key, *sorted(stems)))}
        for stem in stems:
            found.update(self._plural_entries.get((root_key, stem), ()))
            found.update(self._verbal_noun_entries.get((root_key, stem), ()))
        return found

    def _entry_evidence(self, root_key: str, reading: str) -> dict[str, tuple[float, str, str]]:
        evidence = {}
        for form, cost in _reading_stems(reading):
            for (eid,) in self._conn.execute(
                    'SELECT entry_id FROM lane_forms WHERE root_key=? AND skeleton=?', (root_key, form)):
                value = (cost, 'headword', form)
                evidence[eid] = min(evidence.get(eid, value), value)
            for kind, index in (('plural', self._plural_entries), ('verbal_noun', self._verbal_noun_entries)):
                for eid in index.get((root_key, form), ()):
                    value = (cost + .15, kind, form)
                    evidence[eid] = min(evidence.get(eid, value), value)
        return evidence

    def _finite_entries(self, reading: str) -> dict[str, dict[str, str]]:
        """Subject-marked verb forms, validated against Lane's numbered verb entries."""
        if closed_class_roots(reading, self._closed) or article_candidates(lexical_key(reading)):
            return {}
        bases = [reading]
        if reading[:1] in 'وف':
            bases.append(reading[1:])
        forms = list(dict.fromkeys([*bases, *(base[:-len(s)] for base in bases for s in _OBJECT_PRONOUNS
                                            if base.endswith(s) and len(base) - len(s) >= 4)]))
        roots = strip_roots(reading, self._roots, self._prior)
        for form in forms:
            # A final radical hamza is commonly written alif before a subject ending.
            for ending in ('ت', 'تم', 'تن', 'نا', 'تي', 'تو'):
                base = form[:-len(ending)] if form.endswith(ending) else ''
                if len(base) == 3 and base.endswith('ا'):
                    hamza = root_key(base[:-1] + 'ء')
                    if hamza in self._roots:
                        roots = [hamza, *(key for key in roots if key != hamza)]
            if len(form) == 3 and form[0] in 'يتنا':
                weak = root_key('و' + form[1:])
                if weak in self._roots and weak not in roots:
                    roots.append(weak)
                doubled = root_key(form[1:] + form[-1])
                if doubled in self._roots and doubled not in roots:
                    roots.append(doubled)
        found: dict[str, dict[str, str]] = {}
        for key in roots:
            for eid, root, stem in self._verb_entries.get(key, ()):
                for form in forms:
                    if finite_form(root, stem, lexical_key(form)):
                        found.setdefault(key, {})[eid] = form
                        break
        return found

    def _root(self, root_key: str, reading: str, *, inferred: bool = False,
              finite: Optional[dict[str, str]] = None) -> Optional[LaneRoot]:
        row = self._conn.execute("SELECT name, first_page, names_json FROM lane_roots WHERE root_key = ?",
                                 (root_key,)).fetchone()
        if row is None:
            return None
        listing = self._conn.execute("SELECT id, root, page FROM lane_entries WHERE root_key = ? ORDER BY ordinal",
                                     (root_key,)).fetchall()
        matched = self._matched_entries(root_key, reading)
        try:
            names = [n for n in json.loads(row[2] or "[]") if isinstance(n, str)]
        except ValueError:
            names = []
        name = lane_heading(names, [(r[0], r[1]) for r in listing], matched) or _text(row[0])
        page = row[1]
        if name != _text(row[0]):
            page = next((r[2] for r in listing if r[1] == name and r[2] is not None), row[1])
        under = {r[0] for r in listing if r[1] == name}
        # The matched entries first, then those under the heading shown; Lane's order within each.
        evidence = self._entry_evidence(root_key, reading)
        for eid, form in (finite or {}).items():
            evidence[eid] = min(evidence.get(eid, (99, '', '')), (1.1, 'verb_form', form))
        if article_candidates(lexical_key(reading)):
            evidence = {eid: (cost + (1 if eid in self._entry_stems and kind == 'headword' else 0), kind, form)
                        for eid, (cost, kind, form) in evidence.items()}
        # Closed-class links identify the reading, including after a conjunction. An
        # inferred feminine noun must not become a homographic preposition + pronoun.
        for form in (() if inferred else (reading, reading[1:] if reading[:1] in 'وف' else reading)):
            for (eid,) in self._conn.execute(
                    'SELECT entry_id FROM closed_class WHERE root_key=? AND skeleton=? AND entry_id IS NOT NULL',
                    (root_key, form)):
                evidence[eid] = (-1, 'closed_class', form)
        # Keep the same hierarchy for an accusative/inflected headword shape.
        if not evidence:
            for shape in _head_shapes(root_key, reading)[1:]:
                for eid, value in self._entry_evidence(root_key, shape).items():
                    if eid in matched:
                        evidence[eid] = min(evidence.get(eid, (99, '', '')), (value[0] + 1, 'inflected', value[2]))
        chosen = sorted((r[0] for r in listing), key=lambda i: (
            evidence.get(i, (99, '', ''))[0],
            i not in matched, i not in under))
        shown = chosen[:MAX_ENTRIES]
        matched.update(finite or {})
        entries = []
        for entry_id in shown:
            e = self._conn.execute("SELECT headword, page, text FROM lane_entries WHERE id = ?",
                                   (entry_id,)).fetchone()
            whole = " ".join(_text(e[2]).split())
            text, cut = cut_text(whole, MAX_TEXT)
            entries.append(LaneEntry(entry_id=entry_id, headword=_text(e[0]), page=_page(e[1]), text=text,
                                     cut=cut, url=entry_url(entry_id), matched=entry_id in matched,
                                     full=whole if cut else "", match_kind=evidence.get(entry_id, (0, '', ''))[1],
                                     matched_form=evidence.get(entry_id, (0, '', ''))[2]))
        return LaneRoot(root_key=root_key, name=name, page=_page(page), url=root_url(name),
                        entries=tuple(entries), more_entries=max(0, len(chosen) - len(shown)), inferred=inferred)


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
    if meta.get("lane_credit") != PERSEUS_CREDIT:
        return "meta.lane_credit is not the Perseus credit line", {}
    # The readings are offered from MIN_COUNT up (D11); a sidecar that stores only higher counts
    # would drop readings the gate measured.
    floor = meta.get("freq_min_stored", "")
    if not floor.isdigit() or not 1 <= int(floor) <= MIN_COUNT:
        return f"meta.freq_min_stored = {floor!r}; this reader offers readings counted {MIN_COUNT} or more", {}
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    absent = [i for i in LOOKUP_INDEXES if i not in indexes]
    if absent:
        return f"lookup index(es) missing: {', '.join(absent)}", {}
    for table in COUNTED_TABLES:
        expected = meta.get(f"rows_{table}", "")
        actual = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if not expected.isdigit() or int(expected) != int(actual):
            return f"meta.rows_{table} = {expected!r} but the table holds {actual} rows", {}
    check = conn.execute("PRAGMA quick_check").fetchone()
    if check is None or check[0] != "ok":
        return f"quick_check: {check[0] if check else 'no answer'}", {}
    return "", meta


# ---------------------------------------------------------------------------
# The process-wide instance (one per app, like the other sidecar services)
# ---------------------------------------------------------------------------

_default_service: Optional[ArabicService] = None
_lock = threading.Lock()


def get_arabic_service(thread_safe: bool = True) -> ArabicService:
    """The default service, created on first use (per-thread connections by default: safe under
    NiceGUI's ``run.io_bound`` pool)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = ArabicService(thread_safe=thread_safe)
        return _default_service


def reset_arabic_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
