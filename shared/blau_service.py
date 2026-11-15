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
``lookup_stems`` in order -- the word first, then one proclitic layer
peeled, then one pronoun suffix (a feminine ת before it also read as ה) --
each against ``forms_norm``. A key of a
headword-like kind (root, sub-entry, headword, variant) makes one card per
scope it reaches -- a dictionary entry, or one sub-entry of it -- per
candidate, in candidate order, with the matched senses first and then the
senses under that scope. A key found only inside the dictionary's own example
phrases (kind ``phrase``) is listed apart, under "also in examples": the entry
it sits in defines some OTHER word. On a 1.1.0 file the word is also looked up
in ``tokens``, every word of every citation and name: the entries holding it,
other than the cards', are ``quotes``, each with the places and the word marked
(``MAX_QUOTE_HITS``, ``MAX_QUOTE_PLACES``; never for a function word or a word
more than ``MAX_QUOTE_UNITS`` entries hold). An
inflected form the dictionary quotes under its root is reached this way and by
no other. Ambiguity is the norm, not an error:
several candidates that hit are several cards, never one merged answer.
Everything is bounded (``MAX_CARDS``, ``MAX_SENSES``, ``MAX_EXAMPLES``) so one
click never renders a page; a card's senses are all of its scope's (``MAX_SENSES`` is
far above the largest entry), and the panel shows the first few and the rest on a click.

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

A root's stems (the private dictionary lookup plan, section 4.6). Every card
carries the stems the dictionary lists under its entry (``root_stems``: a ``stems`` row's column,
shift and printed head together, ``stem_row_numbers``), and a root typed with a stem ("דלל X",
"דלל 10", ``shared.ja_roots.root_and_stem``) opens that stem's paragraph on each entry of the root
that lists it; when none does, the answer says so (``BlauLookup.stem_missing``) and shows the
root's cards. ``stem_pairs`` is the whole list of (root, stem) pairs, built once in memory.

Cards found by verb form (the plan's PR 3, sections 4.4 and 6.1): when the word itself reaches no
entry, the entries whose listed stems have the word among their generated forms
(``verb_form_index``, ``shared.ja_verb_forms.verb_form_cards``), at most ``MAX_VERB_FORM_CARDS``,
each saying how it was reached (``BlauCard.verb_form``). Beside cards the word itself reaches, those
roots are only suggested (``BlauLookup.suggestions``: a root and a stem a press looks up). Each card
has a kind (A to E), and only the kinds the switch names are shown (``BLAU_VERB_FORM_KINDS``,
default A, B, D: the graded sample); a shown kind opens on the word's stem only if
``BLAU_VERB_FORM_STEM_KINDS`` names it too (default A, D).

A typed word may be Arabic script (the panel's box): it is looked up against
the Arabic spellings of the entries, sub-entries and forms, by a key that
also undoes the extraction's broken spacing (``arabic_key``).

No guessing at misread letters: a lookup answers what the word's own
spelling reaches. One-letter "close spellings" were built and measured
against the PGP Judeo-Arabic editions (scripts/measure_blau_close_spellings.py)
and dropped by the owner on 2026-11-04: about 1 offer in 20 was right, because
most words with no entry were read correctly and the dictionary lacks them.
The reader corrects a letter in the box instead.

The expression search (2026-11-08, after Friedman's): ``expression(text, gap)`` takes two to
``MAX_EXPRESSION_WORDS`` words and answers with every entry (or sub-entry of one) holding them IN
ORDER, with at most ``gap`` printed words between each consecutive pair, inside ONE of its names
(a root's, a sub-entry's, a form's headword or variant) or ONE quoted citation: the sidecar's
``tokens`` table (schema 1.1.0), keyed by ``shared.ja_expression.scope_keys`` as Friedman's is, so
a typed word also meets a printed one through a clitic alternative on either side. The hits come
in the tiers ``shared.ja_expression.tiered`` defines: the entry whose own name IS the expression,
then the entries whose name holds the words in order, then the citations, and every place says
which printed word carried which typed word. ``answer(text, gap)`` is what the panel asks: one
word is a lookup, several an expression search. A 1.0.0 file has no ``tokens``: it is still served
(the code reaches the server before the rebuilt file), and ``answer`` falls back to the lookup,
which finds an entry NAME equal to the phrase and nothing else.
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
from typing import Any, Mapping, Optional, Union
from urllib.parse import quote

from shared.blau_normalize import matching_paths, evidence_cost, plural_analyses, query_stems
from shared.blau_normalize import ACCUSATIVE, BROKEN_PLURAL, DUAL, FEMININE_PLURAL, PLURAL, feminine_heading
from shared.blau_normalize import analysis_paths, article_candidates, bare_keys, preposition_candidates
from shared.blau_normalize import (
    NORMALIZER_VERSION,
    nominal_candidates,
    lexical_key,
    spelling_candidates,
    normalize_arabic,
    normalize_ja,
)
from shared.ja_expression import function_reading, matching_scopes
from shared.ja_expression import (
    clitic_governed, governed_runs, names_expression, verb_keys,
    governed_verb_run,
    is_function_word,
    component_runs, context_component_runs, context_quality,
    DEFAULT_GAP,
    expression_match, expression_words, ExpressionLimitError,
    KEYING_VERSION,
    MAX_CONTEXT_QUOTED,
    MAX_EXPRESSION_WORDS,
    context_windows,
    MAX_GAP,
    ExpressionPlace,
    clamp_gap,
    components_for,
    lookup_key,
    ordered_match,
    query_keys,
    quoted_gap,
    quoted_runs,
    tiered,
)
# The dot above (DOT_LETTERS, dot_pattern) lives in shared.ja_roots since 2026-11-11, so Friedman's
# lookup reads it the same way; the names stay importable from here.
from shared.ja_roots import supported_hollow_perfect
from shared.ja_roots import DOT_LETTERS, StemPair, dot_pattern, root_and_stem, stem_numbers  # noqa: F401 - DOT_LETTERS: importable from here, as before the move
from shared.ja_roots import BorrowedRoot, carries_root, ja_root_of_arabic
from shared.ja_roots import dots_agree as _dots_agree
from shared.dictionary_switch import OFF, ON, read_switch, serves
from shared.ja_verb_forms import listed_alef_perfect
from shared.ja_verb_forms import MAX_SUGGESTIONS  # noqa: F401 - importable from here, as before the move
from shared.ja_verb_forms import (
    MAX_VERB_FORM_CARDS,
    suggested_roots,
    switch_kinds,
    under_conjunction,
    verb_form_cards,
    verb_form_index,
    word_stems,
)
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader understands. scripts/build_blau_sidecar.py
#: writes its own ``SCHEMA_VERSION`` into ``meta.version``; the two are pinned
#: equal by tests/test_blau_service.py, and a file of another version is not
#: opened (reject-incompatible, never a silent read of changed columns) -- except
#: 1.0.0, the file before the expression search's ``tokens`` table, which is 1.1.0
#: without that table and is served without the expression search: the code goes
#: to the server first, the rebuilt file after it (``SERVED_SCHEMA_VERSIONS``).
SCHEMA_VERSION = "1.4.0"
SERVED_SCHEMA_VERSIONS = ("1.0.0", SCHEMA_VERSION)
SIDECAR_DIR = "blau_data"
SIDECAR_FILENAME = "blau.db"
ENV_DB_PATH = "GENIZAH_BLAU_DB_PATH"
ENV_ENABLED = "BLAU_DICTIONARY_ENABLED"
#: The switches of the cards found by verb form (PR 3): the kinds shown ("A,B,D"), and those of
#: them that open on the word's stem. Read live; unset is the default below, set and empty is none.
ENV_VERB_FORM_KINDS = "BLAU_VERB_FORM_KINDS"
ENV_VERB_FORM_STEM_KINDS = "BLAU_VERB_FORM_STEM_KINDS"
#: The graded sample (recall counts more than precision): where Blau finds nothing
#: for the word itself, a card found by verb form names a right root for 91-93% of words of kinds A,
#: B and D; its guessed stem is right for 94% (A) and 92% (D) of them, 80% (B) is under the 85% bar.
DEFAULT_VERB_FORM_KINDS = "A,B,D"
DEFAULT_VERB_FORM_STEM_KINDS = "A,D"
#: ``BlauCard.matched_kind`` of a card found by verb form.
MATCHED_VERB_FORM = "verb_form"
#: The dictionary's site; an entry page is ``entry/<root id>`` (its
#: ``js/search.js::openInReader``).
SITE_URL = "https://blau-dict.netlify.app/"

HEADWORD_KINDS = ("root", "sub_entry", "headword", "variant")
#: Kinds whose multi-word name a single word may begin ("ד̇לך ו"). Not variants:
#: a multi-word variant is mostly a verb with its preposition, and would bury a
#: common word under cards.
NAME_PREFIX_KINDS = ("root", "sub_entry", "headword")
PHRASE_KIND = "phrase"
#: The kinds that are a form written under an entry, not the entry's own name: a hit of one counts
#: only where the key carries the entry's root (``shared.ja_roots.carries_root``). The rest are the
#: words a verb takes, written after it ("אכ̇ד̇ לה": the form is "לה"; 293 of 680 one-word forms,
#: measured 2026-11-12), which reached the entry as if the clicked word were its own.
FORM_KINDS = ("headword", "variant")
#: The segments of a stem's printed head that name its stem number (``stems.head_json``).
STEM_HEAD_TYPES = ("binyan", "binyan_shift")
MAX_CARDS = 10
#: A card carries every sense of its scope that has content (the panel shows the first few and the
#: rest on a click; no sense only on the site). A bound, not a cut: the largest
#: entry of release 1.1 has 115 forms; any beyond it are counted in ``BlauCard.more_senses``.
MAX_SENSES = 200
MAX_EXAMPLES = 8
REQUIRED_TABLES = ("meta", "roots", "sub_entries", "stems", "forms", "forms_norm")
LOOKUP_INDEX = "idx_forms_norm_norm"
#: The expression search's table and index (schema 1.1.0), and its bounds: entries answered,
#: places shown per entry. The scopes: an entry's names, or a citation's quoted text.
TOKENS_TABLE = "tokens"
TOKENS_INDEX = "idx_tokens_key"
MAX_EXPRESSION_HITS = 20
MAX_PLACES = 4
#: One word's lookup also reads ``tokens``: the entries whose citations or names hold the word
#: (``BlauLookup.quotes``), and the places shown under each.
MAX_QUOTE_HITS = 5
MAX_QUOTE_PLACES = 2
#: A word held by more units than this is a common word (קאל, כתאב: over a hundred), and five of
#: them say nothing about it: no quotes at all. Measured 2026-11-10: nine keys in ten are in six
#: units or fewer.
MAX_QUOTE_UNITS = 25
NAME_SCOPES = ("root", "sub_entry", "form", "construction")
SCOPE_CITATION = "citation"
#: ``forms.ja_text`` joins a form's headword and its variants with this; each is one name.
NAME_SEPARATOR = " | "
_KIND_RANK = {kind: rank for rank, kind in enumerate(HEADWORD_KINDS)}
#: What a sense needs of a form; never ``segments_json``, the largest column.
_FORM_COLUMNS = ("id, ja_text, arabic_spelling, binyan, gloss_he, gloss_en, cross_reference, target_ids, "
                 "page, is_classical, is_reconstructed, is_erroneous, sub_entry_id")


def blau_switch() -> str:
    """The ``BLAU_DICTIONARY_ENABLED`` switch, read on every call (default on): on, off, or
    ``editors`` (``shared.dictionary_switch``)."""
    return read_switch(ENV_ENABLED, ON)


def blau_enabled() -> bool:
    """The switch is on for everyone (not ``editors`` only)."""
    return blau_switch() == ON


def verb_form_kinds() -> frozenset[str]:
    """The kinds of card found by verb form the lookup shows (``BLAU_VERB_FORM_KINDS``, read on
    every call); ``DEFAULT_VERB_FORM_KINDS`` unless set; set and empty, none."""
    return switch_kinds(ENV_VERB_FORM_KINDS, DEFAULT_VERB_FORM_KINDS)


def verb_form_stem_kinds() -> frozenset[str]:
    """The kinds whose cards open on the word's stem (``BLAU_VERB_FORM_STEM_KINDS``);
    ``DEFAULT_VERB_FORM_STEM_KINDS`` unless set. Any other card opens on the whole entry."""
    return switch_kinds(ENV_VERB_FORM_STEM_KINDS, DEFAULT_VERB_FORM_STEM_KINDS)


def entry_url(root_id: str) -> str:
    """The dictionary site's page for one entry."""
    return f"{SITE_URL}entry/{quote(str(root_id), safe='')}"


def name_alternatives(ja_text: Any) -> list[str]:
    """The names a stored name column holds (``forms.ja_text``: the headword and the variants,
    joined with ``NAME_SEPARATOR``); each is one scope of the expression search. The builder
    and the gate split the column the same way."""
    return [part for part in str(ja_text or "").split(NAME_SEPARATOR) if part.strip()]


def form_head_alternatives(ja_text: Any, arabic_spelling: Any) -> list[str]:
    """Printed form heads in either script; retain old JA positions and append new spellings."""
    names = name_alternatives(ja_text)
    seen = {lookup_key(name) for name in names}
    for name in name_alternatives(arabic_spelling):
        key = lookup_key(name)
        if key and key not in seen:
            names.append(name)
            seen.add(key)
    return names


def stem_row_numbers(binyan: Any, binyan_shift: Any, head_json: Any) -> tuple[int, ...]:
    """The stem numbers of one ``stems`` row, read from the three places the dictionary writes
    them, together (plan section 4.1): the ``binyan`` column, the ``binyan_shift`` ("IV > I" names
    both) and the printed head's binyan segments. 355 rows of release 1.1 have a column that
    disagrees with the printed head (דלל: the column says IV where the head prints V); such a
    row answers to both numbers until the builder is fixed."""
    found = set(stem_numbers(binyan)) | set(stem_numbers(binyan_shift))
    try:
        head = json.loads(head_json or "{}")
    except (TypeError, ValueError):
        head = {}
    for segment in (head.get("segments") or []) if isinstance(head, dict) else []:
        if isinstance(segment, dict) and segment.get("text_type") in STEM_HEAD_TYPES:
            found.update(stem_numbers(segment.get("text")))
    return tuple(sorted(found))


def _root_label(root: Mapping[str, Any]) -> str:
    """A root's first printed name, dots kept (what the box holds to open it again)."""
    names = _names_of(root["names_json"]) if "names_json" in root.keys() else []
    label = names[0] if names else _text(root["name_ja"]).split(NAME_SEPARATOR, 1)[0].strip()
    arabic = normalize_arabic(_text(root["arabic_spelling"])).replace(" ", "")
    # The digital source occasionally puts a quoted sentence in name_ja while retaining
    # its three-letter Arabic root. Use that root as a label; keep the source row untouched.
    if len(label.split()) >= 3 and len(arabic) == 3 and "|" not in _text(root["arabic_spelling"]):
        return ja_root_of_arabic(arabic)
    return label


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
    """Where the sidecar lives: ``GENIZAH_BLAU_DB_PATH`` when set (dev: a candidate file
    beside the served one), else the user's updated copy first (the desktop sidecar
    updater's location, as for the FGP and PGP sidecars), then the repository's
    ``blau_data/blau.db``."""
    override = os.environ.get(ENV_DB_PATH, "").strip()
    if override:
        return override
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
    #: The name of the sub-entry this sense begins, as the site heads it (קריץ under קרץ), when it is
    #: not the card's own scope; the entry's name where its own senses come back after one. '' on
    #: every other sense (קרצת's card gave קריץ's 'nettle' as the verb's).
    sub_entry: str = ""
    details_available: bool = False


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
    #: Every sense of the card's scope with content, the matched first; the panel shows the first
    #: few and the rest on a click. ``more_senses``: those beyond ``MAX_SENSES`` (on the site only).
    senses: tuple[BlauSense, ...]
    more_senses: int
    #: The looked-up word carries the dot (``dot_pattern``) and this entry's
    #: spelling does not have it on the same letters: another word, probably.
    dot_mismatch: bool = False
    #: The entry's first printed name (dots kept): with a stem, what opens that stem's paragraph.
    root_label: str = ""
    #: The stems the dictionary lists under this entry, I to X (``BlauService.root_stems``).
    stems: tuple[int, ...] = ()
    #: The stem chosen ("דלל X"): the senses are that stem's paragraph. 0: none chosen.
    stem: int = 0
    #: A card for a root another dictionary gives for the word (``BlauService.borrow``): each
    #: (source, evidence) that gave it, ``BorrowedRoot.source`` and ``.evidence``. (): Blau's own.
    borrowed: tuple[tuple[str, str], ...] = ()
    #: A card found by verb form (PR 3): what reached it, the word's form with the root and the
    #: stems it fits ("אסתדלו (דלל X)"), and the card's kind (A to E, plan 6.1). '': not one.
    verb_form: str = ""
    kind: str = ""

    analyses: tuple[tuple[str, ...], ...] = ()


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
    #: The entries whose citations or names hold the word (the ``tokens`` table), other than
    #: the cards' entries: each with the places, the word marked. Empty on a 1.0.0 file, for a
    #: function word and for a typed Arabic word.
    quotes: tuple["BlauExpressionHit", ...] = ()
    more_quotes: int = 0
    #: The stem typed after the root ("דלל X"), 0 for a plain lookup.
    stem: int = 0
    #: Beside the cards the word itself reached: the other roots its verb forms give, (root as
    #: printed, stem; 0 when several fit), at most ``MAX_SUGGESTIONS``; a press looks "root stem" up.
    suggestions: tuple[tuple[str, int], ...] = ()
    #: The expressions the word makes with the words beside it in the text, when the page sent them
    #: (``BlauService.context``): each a run of words with the units whose NAME holds
    #: it, or else that hold it with words between, shown first; the word's own cards and quotations
    #: leave out the units shown here.
    context: tuple["BlauExpression", ...] = ()
    problem: str = ""

    quote_exclusions: tuple[tuple[str, ...], ...] = ()
    quote_offset: int = 0

    @property
    def found(self) -> bool:
        return bool(self.cards or self.examples or self.quotes or self.context)

    @property
    def context_quoted(self) -> bool:
        """The words beside it reached only citations: no entry or sub-entry shown has the run in its name."""
        return bool(self.context) and not any(
            place.scope != SCOPE_CITATION for found in self.context for hit in found.hits for place in hit.places)

    @property
    def stem_missing(self) -> bool:
        """A stem was typed, the root was found, and no entry it reached lists that stem: the
        cards are the root's, with the stems they have."""
        return bool(self.stem and self.cards and not any(card.stem == self.stem for card in self.cards))


def function_word_first(cards: tuple[BlauCard, ...], key: str) -> tuple[BlauCard, ...]:
    """The cards with a function word's own entries first: the word itself, or what is left of it
    under a conjunction, a preposition or a pronoun (עליה is עלי with a pronoun before it is the
    noun עליה̈; וכל is כל before the verb וכל). The order is otherwise kept."""
    def other(card: BlauCard) -> bool:
        return not (function_reading(key, card.candidate) and normalize_ja(card.matched_key) == card.candidate
                    and not card.verb_form and not card.borrowed and not card.dot_mismatch
                    and all(set(path) <= {'prefix', 'suffix'} for path in card.analyses))
    return tuple(sorted(cards, key=other))


def empty_lookup(word: str = "", key: str = "", candidates: tuple[str, ...] = (),
                 script: str = "ja") -> BlauLookup:
    return BlauLookup(word=word, key=key, candidates=candidates, cards=(), more_cards=0,
                      examples=(), more_examples=0, script=script)


@dataclass(frozen=True)
class BlauCitation:
    """One quoted citation of a form: the Judeo-Arabic text, its Hebrew translation, the
    dictionary's locator (the citation as printed) and the source work it names."""
    n: int
    quote: str
    translation: str
    locator: str
    source: str
    foreign: str = ""   # Blau quotes no Arabic-script text; the shared renderer may ask
    id: str = ""
    commentary: str = ""
    bibliography: str = ""


@dataclass(frozen=True)
class BlauFormDetails:
    """Examples and notes belonging to one sense, loaded only when requested."""
    form_id: str
    notes: str
    classical_note: str
    citations: tuple[BlauCitation, ...]


@dataclass(frozen=True)
class BlauPlace(ExpressionPlace):
    """One name of an entry, or one citation under it, holding the whole expression; a
    citation's place also says which form it illustrates and what that form means."""
    form_id: str = ""
    form_text: str = ""
    gloss_he: str = ""
    gloss_en: str = ""
    binyan: str = ""
    requires_governing_verb: bool = False
    details_available: bool = False


@dataclass(frozen=True)
class BlauExpressionHit:
    """One entry, or one sub-entry of an entry, with the places that hold the expression."""
    root_id: str
    name: str
    arabic: str
    page: Optional[int]
    url: str
    sub_entry: str
    places: tuple[BlauPlace, ...]
    places_total: int


@dataclass(frozen=True)
class BlauExpression:
    text: str
    words: tuple[str, ...]
    candidates: tuple[tuple[str, ...], ...]   # the keys each word answers to, in its order
    gap: int
    hits: tuple[BlauExpressionHit, ...]
    truncated: bool

    ordered: bool = True
    problem: str = ""
    related: tuple["BlauExpression", ...] = ()

    @property
    def word(self) -> str:
        """The text, under the name the panel's head reads for every dictionary's answer."""
        return self.text

    @property
    def found(self) -> bool:
        return bool(self.hits or self.related)


#: What the dictionary panel receives from this service: one word's lookup, or several words' expression.
BlauAnswer = Union[BlauLookup, BlauExpression]


def empty_expression(text: str = "", gap: int = DEFAULT_GAP, words: tuple[str, ...] = (),
                     candidates: tuple[tuple[str, ...], ...] = ()) -> BlauExpression:
    return BlauExpression(text, words, candidates, gap, (), False)


class _CardBuild:
    """A card while its hits are collected: one scope (an entry, or one of its
    sub-entries) reached by one candidate."""

    __slots__ = ("root", "candidate", "matched_key", "matched_kind", "via", "sub_entry_id",
                 "matched_form_ids", "dot_mismatch", "stem", "verb_form", "kind", "nouns_first")

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
        self.stem = 0
        self.verb_form = ""
        self.kind = ""
        #: The senses outside the entry's stem paragraphs first (``_open_on_word_stem``).
        self.nouns_first = False


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _page(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def _has_content(form: sqlite3.Row) -> bool:
    return any(_text(form[c]) for c in ("ja_text", "gloss_he", "gloss_en", "cross_reference"))


def _names_of(names_json: Any) -> list[str]:
    """The names a ``names_json`` column lists, in the builder's order (one scope each)."""
    try:
        names = json.loads(names_json or "[]")
    except ValueError:
        return []
    return [str(n) for n in names if isinstance(n, str)] if isinstance(names, list) else []


class BlauService:
    """Read-only access to the Blau sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        # Built on first use, then read-only: the Arabic-spelling index (typed Arabic), and the
        # (root, stem) pairs with each root's stems (``_stems``).
        self._index_lock = threading.Lock()
        self._arabic_index: Optional[dict[str, tuple[dict[str, str], ...]]] = None
        self._stem_pairs: Optional[tuple[StemPair, ...]] = None
        self._root_stem_rows: dict[str, dict[int, tuple[str, ...]]] = {}
        self._verb_forms: Optional[dict[str, tuple[StemPair, ...]]] = None
        self._detail_forms: Optional[frozenset[str]] = None
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
        logger.info("BlauService: serving %s (schema %s, source release %s, %s keys, expression search %s)",
                    self._db_path, meta.get("version"), meta.get("source_release"),
                    meta.get("rows_forms_norm"), "on" if self.expressions_available() else "OFF until the sidecar is rebuilt")

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
        return self.is_ready() and blau_enabled()

    def served_to(self, editor: bool) -> bool:
        """Ready AND shown to this reader: on for everyone, or ``editors`` and the reader an editor
        or admin (``shared.dictionary_switch``; a preview before release)."""
        return self.is_ready() and serves(blau_switch(), editor)

    def _answering(self) -> bool:
        """The lookups answer while the switch is not off: the page decided who may ask."""
        return self.is_ready() and blau_switch() != OFF

    def expressions_available(self) -> bool:
        """The open file carries the expression search's ``tokens`` (schema 1.1.0); a 1.0.0
        file is served without it."""
        return self.is_ready() and self._meta.get("version") == SCHEMA_VERSION

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

    def _has_details(self, form_id: str) -> bool:
        if self._detail_forms is None:
            self._detail_forms = frozenset(r[0] for r in self._conn.execute(
                "SELECT id FROM forms WHERE COALESCE(notes, '') <> '' OR COALESCE(classical_note, '') <> '' "
                "UNION SELECT form_id FROM citations"))
        return form_id in self._detail_forms

    def form_details(self, form_id: str) -> Optional[BlauFormDetails]:
        """Read a sense's own examples, references and notes; never another sense's material."""
        if not form_id or not self._answering():
            return None
        try:
            form = self._conn.execute('SELECT notes, classical_note FROM forms WHERE id=?', (form_id,)).fetchone()
            if form is None:
                return None
            rows = self._conn.execute(
                "SELECT c.*, s.abbreviation, s.full_reference FROM citations c "
                "LEFT JOIN sources s ON s.id=c.source_id WHERE c.form_id=? ORDER BY c.ordinal, c.id",
                (form_id,)).fetchall()
            return BlauFormDetails(form_id, _text(form['notes']), _text(form['classical_note']), tuple(
                BlauCitation(int(r['ordinal']), _text(r['ja_text']), _text(r['translation']), _text(r['citation']),
                             _text(r['source_work']) or _text(r['abbreviation']), id=r['id'],
                             commentary=_text(r['commentary']), bibliography=_text(r['full_reference'])) for r in rows))
        except Exception:
            logger.exception('BlauService: sense details could not be read')
            return None

    # -- the lookup ----------------------------------------------------------

    def lookup(self, word: str, verb_forms: Optional[frozenset[str]] = None,
               verb_form_stems: Optional[frozenset[str]] = None, stems: bool = True,
               limit: Optional[int] = None) -> BlauLookup:
        """What the dictionary has for one clicked or typed word. A word with
        Hebrew letters is looked up as Judeo-Arabic; a word with Arabic letters
        only, against the Arabic spellings. ``verb_forms`` / ``verb_form_stems``: the kinds of
        card found by verb form shown, and those opened on the word's stem; None reads the switches
        (``verb_form_kinds``; the graded sample passes every kind). ``stems`` False: the text came
        from the manuscript, and a number after a word ("פרק 2") is no typed stem. Never raises;
        empty when unavailable, when the word has neither, or on any error."""
        word = _text(word)
        limit = MAX_CARDS if limit is None else max(1, int(limit))
        kinds = verb_form_kinds() if verb_forms is None else frozenset(verb_forms)
        stem_kinds = verb_form_stem_kinds() if verb_form_stems is None else frozenset(verb_form_stems)
        if not word or not self._answering():
            return replace(empty_lookup(word), problem="unavailable" if word else "")
        key = ""
        candidates: tuple[str, ...] = ()
        script = "ja"
        try:
            root, stem = root_and_stem(word) if stems else ("", 0)
            if stem:
                # "דלל X": the root as typed (no clitic peeled: the reader typed a root), with
                # that stem's paragraph opened on every entry of the root that lists it.
                key = normalize_ja(root)
                candidates = (key,)
                return self._lookup(word, key, candidates, stem=stem, root_text=root, limit=limit)
            key = normalize_ja(word)
            if not key:
                script = "arabic"
                candidates = tuple(arabic_candidates(word))
                if not candidates:
                    return replace(empty_lookup(word), problem="unsupported")
                key = candidates[0]
                native = self._lookup_arabic(word, key, candidates)
                ja = lexical_key(word)
                if not ja or self._meta.get("version") != SCHEMA_VERSION:
                    return native
                equivalents = spelling_candidates(self._conn, query_stems(ja))
                mapped = self._lookup(word, ja, equivalents, verb_forms=kinds, verb_form_stems=stem_kinds, limit=limit)
                seen = {(c.root_id, c.sub_entry) for c in native.cards}
                return replace(mapped, script="arabic", key=key, candidates=candidates, cards=native.cards + tuple(
                    c for c in mapped.cards if (c.root_id, c.sub_entry) not in seen))
            candidates = query_stems(key)
            if self._meta.get("version") == SCHEMA_VERSION:
                candidates = spelling_candidates(self._conn, candidates)
            result = self._lookup(word, key, candidates, verb_forms=kinds, verb_form_stems=stem_kinds, limit=limit)
            if self._meta.get('version') == SCHEMA_VERSION:
                expanded = spelling_candidates(self._conn, query_stems(key), matres=True)
                extra = tuple(k for k in expanded if k not in candidates)
                if extra:
                    additional = self._lookup(word, key, extra, limit=limit, plurals=False)
                    seen = {c.root_id for c in result.cards}
                    added = tuple(c for c in additional.cards if c.root_id not in seen)
                    combined = result.cards + added
                    result = replace(result, cards=combined[:limit],
                                     more_cards=result.more_cards + additional.more_cards + max(0, len(combined) - limit),
                                     candidates=tuple(dict.fromkeys((*result.candidates, *extra))))
            return replace(result, cards=function_word_first(result.cards, result.key))
        except Exception:
            logger.exception("BlauService: lookup of %r failed", word)
            return replace(empty_lookup(word, key, candidates, script), problem="error")

    # -- the expression search -----------------------------------------------

    def answer(self, text: str, gap: int = DEFAULT_GAP, stems: bool = True,
               before: tuple[str, ...] = (), after: tuple[str, ...] = (), ordered: bool = True) -> BlauAnswer:
        """What the panel asks for what the reader clicked, selected or typed: several words
        are an expression search where the sidecar supports it, else (one word, or a 1.0.0
        file) the lookup of the text as one key. A root with a typed stem ("דלל X") is the
        lookup of that root with the stem chosen, never an expression; ``stems`` False (words
        clicked or selected in the manuscript) reads no stem: "פרק 2" there is two words.
        ``before`` / ``after``: the words beside a clicked word in the text, for ``context``."""
        text = " ".join(_text(text).split())
        if len(text.split()) > 1 and self.expressions_available() and not (stems and root_and_stem(text)[1]):
            expression = self.expression(text, gap, ordered=ordered)
            if not expression.found and not expression.problem:
                related = []
                for components in component_runs(text):
                    partial = self.expression(components, gap, ordered=ordered)
                    if partial.found:
                        related.append(partial)
                        if len(related) == 3:
                            break
                if related:
                    expression = replace(expression, related=tuple(related))
            return expression
        result = self.lookup(text, stems=stems)
        if (before or after) and not result.stem and result.script == "ja":
            context = self.context(text, before, after, gap)
            if context:
                shown = {(hit.root_id, hit.sub_entry) for found in context for hit in found.hits}
                promoted = {(hit.root_id, hit.sub_entry) for found in context for hit in found.hits
                            if any(place.scope != SCOPE_CITATION for place in hit.places)}
                result = replace(result, context=context, cards=tuple(
                    card for card in result.cards if (card.root_id, card.sub_entry) not in promoted), quotes=tuple(
                    hit for hit in result.quotes if (hit.root_id, hit.sub_entry) not in shown))
        return result

    def context(self, word: str, before: tuple[str, ...] = (), after: tuple[str, ...] = (),
                gap: int = DEFAULT_GAP) -> tuple[BlauExpression, ...]:
        """The expressions a clicked word makes with the words beside it in the text
        (אללה in "אטאל אללה בקאך" reached the entries of אללה, and not the blessing): the
        runs ``shared.ja_expression.context_windows`` gives, longest first, searched with no word
        between; for the first size where an entry or a sub-entry has the run in its NAME (not only
        in a citation), each such run with those units and those places. When none has one in its
        name, the units that hold one with ``quoted_gap(gap)`` words between, in a name or in a
        citation: ``quoted_runs``, every size, longest first, at most
        ``MAX_CONTEXT_QUOTED`` units (``BlauLookup.context_quoted`` when only citations hold them). ()
        when none does, no word is beside it, on a 1.0.0 file, or on any error. Never raises."""
        if not (before or after) or not self.expressions_available():
            return ()
        try:
            windows = context_windows(word, before, after)
            index = self.verb_form_index()
            around = frozenset(key for value in (*before, word, *after) for key in query_keys(value, index))
            # Named constructions may have an intervening argument slot.
            for head_gap in dict.fromkeys((0, quoted_gap(gap))):
                for runs in windows:
                    found: list[BlauExpression] = []
                    for text in runs:
                        named = self._named(self._context_expression(text, head_gap, head_only=True), around)
                        if named is not None:
                            found.append(named)
                    if found:
                        return tuple(found)
            # No run of neighbours is a name: a verb with the preposition it governs across other
            # words (יפלח אללה בה), or with one written onto a later word (יטמע באלגאולה). The
            # governing word answers as a finite verb only, the other as the preposition.
            pairs = [(tuple(text.split()), None) for text in governed_runs(word, before, after)]
            pairs += [((governing, carrier), preposition) for governing, carrier, preposition in clitic_governed(word, before, after)]
            found = []
            for pair, preposition in pairs:
                keys = (verb_keys(pair[0], index), (preposition,) if preposition else query_keys(pair[1], index))
                if not all(keys):
                    continue
                result = self._expression(" ".join(pair), pair, keys, 0, MAX_EXPRESSION_HITS, True, True)
                named = self._named(self._eligible(result), around)
                if named is not None:
                    found.append(named)
            if found:
                return tuple(found)
            runs = list(dict.fromkeys([*quoted_runs(windows), *context_component_runs(word, before, after)]))
            governed = governed_verb_run(word, after, self.verb_form_index())
            if governed and governed not in runs:
                runs.append(governed)
            return self._quoted_context(runs, quoted_gap(gap), around)
        except Exception:
            logger.exception("BlauService: the words beside %r could not be looked up", word)
        return ()

    def _context_expression(self, text: str, gap: int, *, head_only: bool = False,
                            result: Optional[BlauExpression] = None) -> BlauExpression:
        """A complement alone can be searched explicitly, but cannot promote its absent verb."""
        from shared.ja_verb_forms import stem_forms

        def eligible(place):
            if place.requires_governing_verb:
                return False
            if place.scope == 'construction' and place.form_id:
                matched = dict(zip(place.positions, place.components))
                verb, prep = matched.get(0), matched.get(1)
                if verb is None or prep is None:
                    return False
                # Automatic neighbouring-word suggestions must fit the numbered stem.
                # An explicit root search can still inspect every numbered paragraph.
                forms = stem_forms(lookup_key(place.words[0]), stem_numbers(place.binyan)[0])
                if not any(key in forms for key in query_keys(verb.word)):
                    return False
                # A conjunction before the preposition can start another clause or member.
                # Do not automatically join it to the preceding verb/noun.
                key, printed = lookup_key(prep.word), lookup_key(prep.printed)
                if key[:1] in ('ו', 'פ') and key[:1] != printed[:1]:
                    return False
            return True

        result = self.expression(text, gap, head_only=head_only) if result is None else result
        hits = []
        for hit in result.hits:
            places = tuple(p for p in hit.places if eligible(p))
            if places:
                hits.append(replace(hit, places=places, places_total=len(places)))
        return replace(result, hits=tuple(hits))

    def _eligible(self, result: BlauExpression) -> BlauExpression:
        """``_context_expression``'s rules over a search already made."""
        return self._context_expression(result.text, result.gap, result=result)

    def _named(self, result: BlauExpression, around: frozenset[str]) -> Optional[BlauExpression]:
        """``result`` with the entries a name of which the run names, and those places: a name
        holding the run's words among other lexical words of its own, none of them in the text
        around the click (``around``, those words' keys), is not the expression read there
        (``shared.ja_expression.names_expression``). None when no entry is left."""
        index = self.verb_form_index()
        hits = []
        for hit in result.hits:
            names = tuple(place for place in hit.places if place.scope != SCOPE_CITATION
                          and names_expression(place, around, "", lambda w: query_keys(w, index)))
            if names:
                hits.append(replace(hit, places=names, places_total=len(names)))
        return replace(result, hits=tuple(hits), truncated=False) if hits else None

    def _quoted_context(self, runs: list[str], gap: int, around: frozenset[str] = frozenset()) -> tuple[BlauExpression, ...]:
        """``context`` with words between: each run with the entries and sub-entries, not shown for a
        longer run, that hold it, at most ``MAX_CONTEXT_QUOTED`` in all. A citation holding the run
        counts as it is; a name counts only when the run names it (``names_expression``, as in the
        stages before: a name passed over there must not come back here with words between)."""
        index = self.verb_form_index()
        found: list[BlauExpression] = []
        shown: set[tuple[str, str]] = set()
        candidates = []
        for text in runs:
            result = self._context_expression(text, gap)
            kept = []
            for hit in result.hits:
                places = tuple(p for p in hit.places if p.scope == SCOPE_CITATION
                               or names_expression(p, around, "", lambda w: query_keys(w, index)))
                if places:
                    kept.append(replace(hit, places=places, places_total=len(places)))
            candidates.append(replace(result, hits=tuple(kept)))
        candidates.sort(key=context_quality)
        for result in candidates:
            if len(shown) >= MAX_CONTEXT_QUOTED:
                break
            hits = []
            for hit in result.hits:
                unit = (hit.root_id, hit.sub_entry)
                if unit not in shown and len(shown) < MAX_CONTEXT_QUOTED:
                    shown.add(unit)
                    hits.append(hit)
            if hits:
                found.append(replace(result, hits=tuple(hits), truncated=False))
        return tuple(found)

    # -- roots borrowed from the other dictionaries -------------------------

    def borrow(self, result: BlauAnswer, borrowed: tuple[BorrowedRoot, ...], limit: Optional[int] = None) -> BlauAnswer:
        """Blau's answer with a card for each root another dictionary gives for the word
        (``shared.ja_roots.borrowed_roots``), after Blau's own cards (Blau has
        no list of inflected forms; Friedman's matches and Lane's first root reach קול for קאל).
        A root reaches the entries named by it, the one spelled with the same dots where several
        share the letters; an entry Blau's own cards already show gets no second card, and two
        dictionaries giving the same entry make one card that names both. The examples and quotes
        under a borrowed entry go (its card shows it). A borrowed card opens on the stem the word's
        shape gives (``_open_on_word_stem``), and among Blau's own cards an entry another dictionary
        names comes first, whichever key reached it. A card found by verb form keeps its place, after the word's
        own and before the borrowed, and names the dictionaries that also give its root. Not for an
        expression or a typed stem. Never raises: on any error, the answer as it was."""
        if not isinstance(result, BlauLookup) or result.stem or not borrowed or not self._answering():
            return result
        try:
            return self._borrow(result, borrowed, MAX_CARDS if limit is None else max(1, int(limit)))
        except Exception:
            logger.exception("BlauService: borrowing roots for %r failed", result.word)
            return result

    def _borrow(self, result: BlauLookup, borrowed: tuple[BorrowedRoot, ...], limit: int = MAX_CARDS) -> BlauLookup:
        shown = {card.root_id for card in result.cards}
        # A card found by verb form (PR 3) whose root another dictionary also gives keeps its place
        # and names that dictionary too; the word's own cards come first, the borrowed ones last.
        verb = tuple(card for card in result.cards if card.verb_form)
        verb_ids = {card.root_id for card in verb}
        verb_sources: dict[str, list[tuple[str, str]]] = {}
        roots: dict[str, Optional[sqlite3.Row]] = {}
        builds: dict[str, _CardBuild] = {}
        sources: dict[str, list[tuple[str, str]]] = {}
        overflow: set[str] = set()
        for item in borrowed:
            key = normalize_ja(item.root)
            if not key or " " in key:
                continue
            rows = list(self._conn.execute(
                "SELECT norm, raw, kind, root_id, sub_entry_id, form_id FROM forms_norm "
                "WHERE norm = ? AND kind = 'root' ORDER BY rowid", (key,)))
            pattern = dot_pattern(item.root)
            same_dots = [row for row in rows if dot_pattern(_text(row["raw"])) == pattern]
            for row in same_dots or rows:
                found: dict[tuple[str, str, str], _CardBuild] = {}
                self._add_hit(found, set(), set(), roots, row, key)
                for build in found.values():
                    root_id = _text(build.root["id"])
                    given = (item.source, item.evidence)
                    if root_id in verb_ids:
                        if given not in verb_sources.setdefault(root_id, []):
                            verb_sources[root_id].append(given)
                        continue
                    if root_id in shown:
                        continue
                    if root_id not in builds:
                        if len(result.cards) + len(builds) >= limit:
                            overflow.add(root_id)
                            continue
                        builds[root_id] = build
                    if given not in sources.setdefault(root_id, []):
                        sources[root_id].append(given)
        # Blau's own cards: the entry another dictionary names first, whichever key reached it
        # (ויכון: כון, which Lane names, before כ̇לו, which also prints יכון in a phrase; and
        # ואלקול: קול, reached with the article peeled, before טלע, which prints אלקול as a variant).
        # The dotted reading first; among the named, and among the rest, the key order the lookup made.
        named = {normalize_ja(item.root) for item in borrowed}
        key_order = {key: n for n, key in enumerate(result.candidates)}
        weak = tuple(card for card in result.cards if card.analyses and all('mater' in path for path in card.analyses))
        nominal = nominal_candidates(result.key)
        article = article_candidates(result.key) and not is_function_word((result.key,))
        finite_ids = {card.root_id for card in verb if card.verb_form and
                      any(c.entry_id == card.root_id and c.finite_for(result.key)
                          for c in verb_form_cards(result.candidates, self.verb_form_index(), nominal))}
        own = tuple(sorted((card for card in result.cards if not card.verb_form and card not in weak), key=lambda card: (
            card.dot_mismatch,
            all('suffix' in path for path in card.analyses) if article else 0,
            normalize_ja(card.root_label or card.name) not in named,
            evidence_cost(card.analyses), key_order.get(card.candidate, len(key_order)))))
        own += tuple(replace(card, borrowed=card.borrowed + tuple(verb_sources[card.root_id]))
                     if verb_sources.get(card.root_id) else card
                     for card in (sorted(verb, key=lambda c: -len(verb_sources.get(c.root_id, ()))) if finite_ids else verb))
        own += weak
        if finite_ids:
            own = tuple(sorted(own, key=lambda card: (
                card.dot_mismatch,
                0 if any(not path for path in card.analyses) else
                1 if card.verb_form and not card.sub_entry else 2)))
        if not builds and not overflow:
            own = function_word_first(own, result.key)
            return replace(result, cards=own) if own != result.cards else result
        if result.script == "ja":
            nominal = nominal_candidates(result.key)
            for build in builds.values():
                self._open_on_word_stem(build, result.candidates, nominal)
        # An entry two dictionaries agree on first; otherwise in the order the roots came.
        added = tuple(replace(self._finish(build, roots), borrowed=tuple(sources[root_id]), analyses=tuple(
                          (f"borrowed:{item.source}", *item.steps) for item in borrowed
                          if normalize_ja(item.root) == normalize_ja(_root_label(build.root))))
                      for root_id, build in sorted(builds.items(), key=lambda item: -len(sources[item[0]])))
        # A suggested root another dictionary also gives is now a card: no chip for it.
        taken = {dot_pattern(card.root_label or card.name) for card in added}
        return replace(
            result, cards=function_word_first(tuple(sorted(own + added, key=lambda c: not (
                any(source == "lane" for source, _ in c.borrowed)
                and supported_hollow_perfect(result.word, c.root_label, c.stems)))), result.key), more_cards=result.more_cards + len(overflow),
            examples=tuple(e for e in result.examples if e.root_id not in builds),
            quotes=tuple(q for q in result.quotes if q.root_id not in builds),
            suggestions=tuple(s for s in result.suggestions if dot_pattern(s[0]) not in taken),
        )

    # -- the stems a root lists ----------------------------------------------

    def _stems(self) -> tuple[tuple[StemPair, ...], dict[str, dict[int, tuple[str, ...]]]]:
        """Every (root, stem) pair the dictionary lists, and per root its stem numbers with the
        ``stems`` rows of each. Built once from the open file (plan section 4.1): a stems row
        under a root or under one of its sub-entries gives a pair for every number it names
        (``stem_row_numbers``); a row that names none gives none. Nothing is stored."""
        if self._stem_pairs is None:
            with self._index_lock:
                if self._stem_pairs is None:
                    pairs: list[StemPair] = []
                    by_root: dict[str, dict[int, list[str]]] = {}
                    for row in self._conn.execute(
                            "SELECT s.id, s.root_id, s.binyan, s.binyan_shift, s.head_json, r.name_ja, r.names_json, r.arabic_spelling "
                            "FROM stems s JOIN roots r ON r.id = s.root_id ORDER BY r.ordinal, s.rowid"):
                        numbers = stem_row_numbers(row["binyan"], row["binyan_shift"], row["head_json"])
                        if not numbers:
                            continue
                        root_id, stem_id = _text(row["root_id"]), _text(row["id"])
                        label = _root_label(row)
                        key = normalize_ja(label)
                        for number in numbers:
                            pairs.append(StemPair(label, key, number, stem_id, root_id))
                            by_root.setdefault(root_id, {}).setdefault(number, []).append(stem_id)
                    self._root_stem_rows = {root_id: {n: tuple(ids) for n, ids in sorted(stems.items())}
                                            for root_id, stems in by_root.items()}
                    self._stem_pairs = tuple(pairs)
        return self._stem_pairs, self._root_stem_rows

    def stem_pairs(self) -> tuple[StemPair, ...]:
        """Every (root, stem) pair the dictionary lists, in the dictionary's order (plan section
        4.1); () when the dictionary is not available or on any error. Never raises."""
        if not self._answering():
            return ()
        try:
            return self._stems()[0]
        except Exception:
            logger.exception("BlauService: the stem pairs could not be read")
            return ()

    def verb_form_index(self) -> dict[str, tuple[StemPair, ...]]:
        """Every form key the (root, stem) pairs the dictionary lists may be written as, and the
        pairs that give it (``shared.ja_verb_forms.verb_form_index``; plan section 4.3, PR 2): built
        once from the open file, held in memory. Nothing in the lookup calls it yet: the gate
        (``scripts/measure_dictionary_stems.py``) measures it first. {} when unavailable or on any
        error. Never raises."""
        if not self.is_ready():
            return {}
        try:
            if self._verb_forms is None:
                pairs = self._stems()[0]
                with self._index_lock:
                    if self._verb_forms is None:
                        self._verb_forms = verb_form_index(pairs)
            return self._verb_forms
        except Exception:
            logger.exception("BlauService: the verb form index could not be built")
            return {}

    def root_stems(self, root_id: str) -> tuple[int, ...]:
        """The stem numbers the dictionary lists under one entry, I to X in order; () for an entry
        with none, when unavailable, or on any error. Never raises."""
        return tuple(self._stem_rows(root_id))

    def _open_on_word_stem(self, build: _CardBuild, keys: tuple[str, ...],
                           nominal: frozenset[str] = frozenset()) -> None:
        """Open a borrowed card on the stem the word's shape gives (``shared.ja_verb_forms.word_stem``;
        אסתקר is stem X of קרר and its card opened on stem I): the one stem the
        entry lists whose generated forms hold one of the word's keys. Only for a root another
        dictionary gave: Blau's own card already shows the form the word matched first, and a
        typed root is the whole entry. Not for a card scoped to a sub-entry or with a stem already
        chosen; nothing when several listed stems hold the word. When none does, the word is no form
        of the entry's verbs, and the senses outside its stem paragraphs come first
        (אלחרוף, the plural of the noun חרף, opened on the verb's stem II sense). A key in
        ``nominal`` (a word with the article: ``article_candidates``) fits a stem only as its
        participle or verbal noun."""
        if build.stem or build.sub_entry_id or not keys:
            return
        root_id = _text(build.root["id"])
        listed = self._stem_rows(root_id)
        if not listed:
            return
        fits = word_stems(keys, normalize_ja(_root_label(build.root)), listed, nominal)
        if len(fits) == 1:
            build.stem = fits[0]
        elif not fits:
            build.nouns_first = True

    def _stem_rows(self, root_id: str) -> dict[int, tuple[str, ...]]:
        if not self.is_ready():
            return {}
        try:
            return self._stems()[1].get(_text(root_id), {})
        except Exception:
            logger.exception("BlauService: the stems of %r could not be read", root_id)
            return {}

    def expression(self, text: str, gap: int = DEFAULT_GAP, limit: int = MAX_EXPRESSION_HITS, ordered: bool = True, head_only: bool = False) -> BlauExpression:
        """Every entry holding the words of ``text`` in order with at most ``gap`` printed words
        between two of them, inside one name or one citation. Empty for one word, for more than
        ``MAX_EXPRESSION_WORDS``, for a word without Hebrew letters, when the dictionary is not
        available or has no ``tokens``, and on any error. Never raises."""
        text = " ".join(_text(text).split())
        gap = clamp_gap(gap)
        if not text or not self._answering() or not self.expressions_available():
            return replace(empty_expression(text, gap), ordered=ordered, problem="unavailable")
        words = expression_words(text)
        candidates = tuple(query_keys(w, self.verb_form_index()) for w in words)
        if not 2 <= len(text.split()) <= MAX_EXPRESSION_WORDS or not all(candidates):
            return replace(empty_expression(text, gap, words, candidates), ordered=ordered, problem="limit" if len(text.split()) > MAX_EXPRESSION_WORDS else "unsupported")
        try:
            result = self._expression(text, words, candidates, gap, max(1, int(limit)), ordered, head_only)
            original = tuple(text.split())
            if words != original:
                original_keys = tuple(query_keys(w, self.verb_form_index()) for w in original)
                intact = self._expression(text, original, original_keys, gap, max(1, int(limit)), ordered, head_only)
                combined = {}
                for hit in (*intact.hits, *result.hits):
                    combined.setdefault((hit.root_id, hit.sub_entry), hit)
                result = replace(result, hits=tuple(combined.values())[:limit],
                                 truncated=result.truncated or intact.truncated or len(combined) > limit)
            return replace(result, ordered=ordered)
        except ExpressionLimitError:
            return replace(empty_expression(text, gap), ordered=ordered, problem="work_limit")
        except Exception:
            logger.exception("BlauService: expression search failed for %r", text)
            return replace(empty_expression(text, gap, words, candidates), ordered=ordered, problem="error")

    def _expression(self, text: str, words: tuple[str, ...], candidates: tuple[tuple[str, ...], ...], gap: int,
                    limit: int, ordered: bool = True, head_only: bool = False) -> BlauExpression:
        accepts = [frozenset(c) for c in candidates]
        rows = matching_scopes(self._conn, candidates, 'scope, scope_id, n, root_id, sub_entry_id', head_only)
        unit_of = {(str(r['scope']), str(r['scope_id']), int(r['n'])):
                   (_text(r['root_id']), _text(r['sub_entry_id'])) for r in rows}
        scopes = list(unit_of)
        # Then the order and the gaps, scope by scope, over its printed words.
        places: dict[tuple[str, str], list[ExpressionPlace]] = {}
        for scope in scopes:
            rows = self._conn.execute(
                f"SELECT pos, key FROM {TOKENS_TABLE} WHERE scope = ? AND scope_id = ? AND n = ? ORDER BY pos, rowid",
                scope).fetchall()
            items = [(int(r["pos"]), str(r["key"])) for r in rows]
            match = expression_match(items, accepts, gap, ordered)
            if match is None:
                continue
            place = self._place(scope, items, match, words, candidates)
            if place is not None:
                places.setdefault(unit_of[scope], []).append(place)
        if not places:
            return empty_expression(text, gap, words, candidates)
        roots = sorted({unit[0] for unit in places})
        marks = ",".join("?" for _ in roots)
        # The first tier's keys: the unit's own names (the lookup's headword kinds), as keyed for
        # the lookup; a root's name belongs to the entry, the rest to their sub-entry.
        keys_of: dict[tuple[str, str], set[str]] = {}
        kinds = ",".join("?" for _ in HEADWORD_KINDS)
        for row in self._conn.execute(
                f"SELECT norm, kind, root_id, sub_entry_id FROM forms_norm WHERE root_id IN ({marks}) AND kind IN ({kinds})",
                [*roots, *HEADWORD_KINDS]):
            unit = (_text(row["root_id"]), "" if row["kind"] == "root" else _text(row["sub_entry_id"]))
            keys_of.setdefault(unit, set()).add(str(row["norm"]))
        # The second tier: a name of the unit, or of its entry, holds the typed words in order
        # within the widest gap, whatever gap the reader chose.
        name_items: dict[tuple[tuple[str, str], str, str, int], list[tuple[int, str]]] = {}
        names = ",".join("?" for _ in NAME_SCOPES)
        for row in self._conn.execute(
                f"SELECT scope, scope_id, n, root_id, sub_entry_id, pos, key FROM {TOKENS_TABLE} "
                f"WHERE scope IN ({names}) AND root_id IN ({marks}) ORDER BY scope, scope_id, n, pos, rowid",
                [*NAME_SCOPES, *roots]):
            unit = (_text(row["root_id"]), _text(row["sub_entry_id"]))
            name_items.setdefault((unit, str(row["scope"]), str(row["scope_id"]), int(row["n"])), []).append(
                (int(row["pos"]), str(row["key"])))
        heads_in_order = {key[0] for key, items in name_items.items() if ordered_match(items, accepts, MAX_GAP) is not None}
        parents = {unit: ((unit[0], "") if unit[1] else None) for unit in places}
        order: dict[tuple[str, str], tuple[int, int]] = {}
        root_ordinal = {str(r["id"]): int(r["ordinal"]) for r in self._conn.execute(
            f"SELECT id, ordinal FROM roots WHERE id IN ({marks})", roots)}
        sub_ordinal = {str(r["id"]): int(r["ordinal"]) for r in self._conn.execute(
            f"SELECT id, ordinal FROM sub_entries WHERE root_id IN ({marks})", roots)}
        for unit in places:
            order[unit] = (root_ordinal.get(unit[0], 0), sub_ordinal.get(unit[1], -1) + 1 if unit[1] else 0)
        ranked = tiered(places, keys_of, heads_in_order, parents, candidates, order, NAME_SCOPES)
        cache: dict[str, Optional[sqlite3.Row]] = {}
        hits = []
        for unit, found in ranked[:limit]:
            hit = self._hit(unit, found, cache)
            if hit is not None:
                hits.append(hit)
        return BlauExpression(text, words, candidates, gap, tuple(hits), len(ranked) > limit)

    def _hit(self, unit: tuple[str, str], found: list[ExpressionPlace],
             cache: dict[str, Optional[sqlite3.Row]]) -> Optional[BlauExpressionHit]:
        root_id, sub_id = unit
        root = self._root(cache, root_id)
        if root is None:
            return None
        sub_entry = ""
        if sub_id:
            row = self._conn.execute("SELECT name_ja FROM sub_entries WHERE id = ?", (sub_id,)).fetchone()
            sub_entry = _text(row["name_ja"]) if row is not None else ""
        # The same sense may print parallel Hebrew/Arabic heads or several spellings.
        # Keep their search keys, but do not spend the visible places on duplicate senses.
        seen = set()
        unique = []
        for place in found:
            if place.scope in ('form', 'construction') and place.form_id:
                identity = (place.scope, place.form_id, place.binyan)
                if identity in seen:
                    continue
                seen.add(identity)
            unique.append(place)
        return BlauExpressionHit(
            root_id=root_id, name=_text(root["name_ja"]), arabic=_text(root["arabic_spelling"]),
            page=_page(root["start_page_printed"]), url=entry_url(root_id), sub_entry=sub_entry,
            places=tuple(unique[:MAX_PLACES]), places_total=len(unique))

    def _place(self, scope: tuple[str, str, int], items: list[tuple[int, str]], match: tuple[int, ...],
               words: tuple[str, ...], candidates: tuple[tuple[str, ...], ...]) -> Optional[BlauPlace]:
        """The scope's printed words with the matched ones, or None when the table and the text
        disagree (not this reader's sidecar)."""
        kind, scope_id, n = scope
        quotation = None
        form_id = form_text = gloss_he = gloss_en = binyan = ""
        requires_governing_verb = False
        if kind == "construction":
            row = self._conn.execute("SELECT text, source_head, stem FROM constructions WHERE id = ?", (scope_id,)).fetchone()
            text = row["text"] if row is not None else ""
            form = self._conn.execute("SELECT id, gloss_he, gloss_en FROM forms WHERE id = ?",
                                      (scope_id.split(':', 1)[0],)).fetchone()
            if row is not None:
                from shared.ja_roots import stem_label
                binyan = stem_label(row["stem"])
            if form is not None and row is not None:
                form_id, form_text = form["id"], row["source_head"]
                gloss_he, gloss_en = _text(form["gloss_he"]), _text(form["gloss_en"])
        elif kind == "root":
            row = self._conn.execute("SELECT names_json FROM roots WHERE id = ?", (scope_id,)).fetchone()
            names = _names_of(row["names_json"]) if row is not None else []
            text = names[n] if n < len(names) else ""
        elif kind == "sub_entry":
            row = self._conn.execute("SELECT names_json FROM sub_entries WHERE id = ?", (scope_id,)).fetchone()
            names = _names_of(row["names_json"]) if row is not None else []
            text = names[n] if n < len(names) else ""
        elif kind == "form":
            row = self._conn.execute("SELECT id, ja_text, arabic_spelling, gloss_he, gloss_en FROM forms WHERE id = ?", (scope_id,)).fetchone()
            names = form_head_alternatives(row["ja_text"], row["arabic_spelling"]) if row is not None else []
            text = names[n] if n < len(names) else ""
            if row is not None:
                form_id, form_text = _text(row["id"]), text
                gloss_he, gloss_en = _text(row["gloss_he"]), _text(row["gloss_en"])
                if self._meta.get('version') == SCHEMA_VERSION and n >= len(name_alternatives(row['ja_text'])):
                    # Existing JA lexical heads retain their established phrase behavior.
                    # Newly exposed Arabic complements need their verb for auto-promotion.
                    prefix = form_id + ':'
                    requires_governing_verb = self._conn.execute(
                        'SELECT 1 FROM constructions WHERE id >= ? AND id < ? LIMIT 1',
                        (prefix, prefix + '\uffff')).fetchone() is not None
        elif kind == SCOPE_CITATION:
            row = self._conn.execute(
                "SELECT c.id, c.ja_text, c.translation, c.citation, c.source_work, f.id AS form_id, f.ja_text AS form_text, "
                "f.gloss_he, f.gloss_en FROM citations c JOIN forms f ON f.id = c.form_id WHERE c.id = ?",
                (scope_id,)).fetchone()
            if row is None:
                return None
            text = _text(row["ja_text"])
            quotation = BlauCitation(n, text, _text(row["translation"]), _text(row["citation"]), _text(row["source_work"]), id=row['id'])
            form_id, form_text = _text(row["form_id"]), (name_alternatives(row["form_text"]) or [""])[0]
            gloss_he, gloss_en = _text(row["gloss_he"]), _text(row["gloss_en"])
        else:
            return None
        printed = tuple(text.split())
        found = components_for(items, match, words, candidates, printed)
        if found is None:
            return None
        positions, components = found
        if kind == 'construction' and (form_id or scope_id.endswith(':ar')) and not {0, 1}.issubset(positions):
            # The verb and its first preposition are required, even when the reader allows
            # intervening words. A match only inside the complement belongs to its form scope.
            return None
        return BlauPlace(kind, n, printed, positions, components, quotation,
                         form_id=form_id, form_text=form_text, gloss_he=gloss_he, gloss_en=gloss_en, binyan=binyan,
                         requires_governing_verb=requires_governing_verb, details_available=self._has_details(form_id))

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

    def _radical_peeled(self, roots: dict[str, Optional[sqlite3.Row]], row: sqlite3.Row, candidate: str) -> bool:
        """Does ``candidate`` reach the name ``row`` only with the name's first letter taken for a
        conjunction or a preposition, where that letter is the root's first? מן is no כ̇מן without its
        כ̇, and דר no בדר. A name printed with the article (אלד̇י: די) or with a letter before its
        root's first (בכתאב under כתב), or that keeps the whole root without it (בברג under ברג), is
        still reached."""
        name = normalize_ja(_text(row["raw"]))
        if not name or name == candidate or name.startswith(candidate) or name.startswith("אל"):
            return False
        paths = analysis_paths(_text(row["raw"]), candidate)
        if not paths or not all("prefix" in path for path in paths):
            return False
        if row["kind"] == "root":
            return True
        root = self._root(roots, _text(row["root_id"]))
        label = normalize_ja(_root_label(root)).split(" ")[0] if root is not None else ""
        if not label or label[:1] != name[:1]:
            return False
        # A name really printed with a preposition that is also its root's first letter (בברג under
        # ברג) keeps every letter of the root after the preposition is taken off.
        letters = iter(candidate)
        return not all(letter in letters for letter in label)

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

    def _lookup(self, word: str, key: str, candidates: tuple[str, ...], stem: int = 0,
                root_text: str = "", verb_forms: frozenset[str] = frozenset(),
                verb_form_stems: frozenset[str] = frozenset(), limit: int = MAX_CARDS,
                plurals: bool = True) -> BlauLookup:
        # A card is one scope reached by one candidate: keyed by (candidate,
        # root, sub-entry), so two candidates that reach the same entry (כתאב
        # and תאב) stay two cards in candidate order, and a root hit and a
        # sub-entry hit under it are not one card with mixed senses (plan §6.2;
        # Codex on #473).
        #
        # With a typed stem (``stem``, the root in ``root_text``): the entries the root's name
        # reaches that list the stem are one card each, scoped to that stem's paragraph; when none
        # does, the cards are the root's as a plain lookup of it shows them (``stem_missing``).
        # No examples or quotes: the reader asked for a root.
        cards: dict[tuple[str, str, str], _CardBuild] = {}
        card_roots: set[str] = set()
        overflow: set[tuple[str, str, str]] = set()
        phrase_rows: list[sqlite3.Row] = []
        roots: dict[str, Optional[sqlite3.Row]] = {}
        prefix_marks = ",".join("?" * len(NAME_PREFIX_KINDS))
        names: dict[tuple[str, str], list[str]] = {}
        # ``plurals`` False: the second pass over the word's other spellings (``lookup``); the
        # first tried the word's noun forms already.
        written = candidates
        plural_kinds = ({a.key: a.steps[-1] for a in plural_analyses(key) if a.key not in candidates}
                        if plurals and not stem else {})
        plural_keys = tuple(plural_kinds)
        candidates = (*candidates, *plural_keys)
        as_written: Optional[bool] = None   # did the word reach an entry before its noun forms were tried
        noun_keys = {k for k, kind in plural_kinds.items() if kind == PLURAL}   # and those that reach one
        whole_entries: list[tuple[sqlite3.Row, str]] = []   # a noun form's root: after every sub-entry
        verbal: list[bool] = []   # asked once, and only for a word a feminine plural's reading reaches

        def alef_perfect() -> bool:
            if not verbal:   # a word with the article is no perfect: אלתאמאת is תאם's plural
                verbal.append(not article_candidates(key) and listed_alef_perfect(written, self.stem_pairs()))
            return verbal[0]

        def order(r: sqlite3.Row) -> tuple[int, int]:
            # The kind first (root, sub-entry, headword, variant); then an entry or sub-entry whose
            # FIRST name is the word before one that gives it as a later name
            # (אנג̇אץ is the second name of "אג̇אץ | אנג̇אץ" and the only name of another entry).
            return _KIND_RANK.get(r["kind"], len(HEADWORD_KINDS)), self._name_position(r, names)

        for candidate in candidates:
            # The entries reached before this key: a noun form does not open one of them again, but
            # opens every noun of its name under one entry (ברכאת: both sub-entries ברכה̈ of ברך).
            reached = frozenset(card_roots)
            rows = list(self._conn.execute(
                "SELECT norm, raw, kind, root_id, sub_entry_id, form_id FROM forms_norm "
                "WHERE norm = ? ORDER BY rowid", (candidate,)))
            if self._meta.get("version") == SCHEMA_VERSION:
                rows.extend(self._conn.execute(
                    "SELECT key AS norm, raw, kind, root_id, sub_entry_id, form_id FROM lookup_aliases "
                    "WHERE key = ?", (candidate,)).fetchall())
            rows.sort(key=order)
            if " " not in candidate:
                # Entry names of several words that begin with this one (a
                # range on the lookup index: every key "candidate <more>").
                named = list(self._conn.execute(
                    "SELECT norm, raw, kind, root_id, sub_entry_id, form_id FROM forms_norm "
                    f"WHERE norm > ? AND norm < ? AND kind IN ({prefix_marks}) ORDER BY rowid",
                    (candidate + " ", candidate + " \U0010ffff", *NAME_PREFIX_KINDS)))
                named.sort(key=order)
                rows.extend(named)
            for row in rows:
                kind = row["kind"]
                if self._radical_peeled(roots, row, candidate):
                    continue
                if candidate in plural_kinds:
                    # A noun the word may be a form of (``plural_analyses``): only a heading that IS
                    # that noun. A verb's root is no noun's heading, but for a noun's pattern
                    # (אבראג̇ is ברג̇), and a pattern is tried only for a word that reached nothing as
                    # written (measured 2026-11-14: beside a word's own entries it mostly named
                    # another word, עליה as a plural of עלה̈). Beyond the sound masculine plural, an
                    # entry the word reached already is not opened a second time.
                    if as_written is None:   # an entry, or a verb form the lookup will show
                        as_written = bool(cards) or any(
                            card.kind in verb_forms
                            for card in verb_form_cards(written, self.verb_form_index(), nominal_candidates(key)))
                    if (plural_kinds[candidate] != PLURAL and alef_perfect()
                            and not (plural_kinds[candidate] == FEMININE_PLURAL and candidate.endswith('ה'))):
                        continue   # אבראת is ברא IV with its subject ending, not אבר with a plural's
                    if plural_kinds[candidate] == ACCUSATIVE and feminine_heading(row['raw']):
                        continue
                    if kind not in {'root', 'sub_entry'} or lexical_key(row['raw']) != candidate:
                        continue
                    if (kind == 'root' and plural_kinds[candidate] != BROKEN_PLURAL
                            and self._stem_rows(_text(row['root_id']))):
                        continue
                    if plural_kinds[candidate] != PLURAL and _text(row['root_id']) in reached:
                        continue
                    if plural_kinds[candidate] == BROKEN_PLURAL and as_written:
                        continue
                    # A dual's noun does not begin with the article the word reached an entry
                    # without (אלזמאן is זמאן, not the dual of אלזם; אלפאן is the dual of אלף).
                    if (plural_kinds[candidate] == DUAL and candidate.startswith('אל')
                            and any(normalize_ja(build.matched_key) == candidate[2:] + 'אנ' for build in cards.values())):
                        continue
                    if plural_kinds[candidate] != PLURAL and kind == 'root':
                        # אלת̇מאר is ת̇מרה̈, a noun under ת̇מר, before it is the whole entry ת̇מר.
                        whole_entries.append((row, candidate))
                        continue
                    noun_keys.add(candidate)
                if (kind in FORM_KINDS and not self._carries_root(roots, _text(row["root_id"]), candidate)
                        and not self._carries_subentry(_text(row["sub_entry_id"]), candidate)):
                    continue   # a word the entry's verb takes, not a form of the entry (FORM_KINDS)
                if kind in _KIND_RANK:
                    self._add_hit(cards, card_roots, overflow, roots, row, candidate)
                elif kind == PHRASE_KIND and not stem:
                    phrase_rows.append(row)
        for row, candidate in whole_entries:
            if _text(row['root_id']) not in card_roots:
                noun_keys.add(candidate)
                self._add_hit(cards, card_roots, overflow, roots, row, candidate)
        if stem:
            return self._stem_answer(word, key, candidates, stem, root_text, cards, overflow, roots)
        # A word with the article is a noun (a verb's senses came above the noun's):
        # its keys with the article peeled reach a verb only as a participle or a verbal noun, and an
        # entry they reach by its name shows the senses outside its stem paragraphs first, unless the
        # word is the participle or verbal noun of one of its stems.
        nominal = nominal_candidates(key) | frozenset(noun_keys)
        candidates = (*written, *(k for k in plural_keys if k in noun_keys))
        finite = {card.entry_id for card in verb_form_cards(candidates, self.verb_form_index(), nominal)
                  if 'B' in verb_forms and (card.kind in verb_forms or card.kind in 'CE')
                  and not is_function_word((key,)) and card.finite_for(key)} if verb_forms else set()
        # What follows a preposition is a noun too (לראס): the same order of senses.
        noun_like = nominal | preposition_candidates(key)
        for build in cards.values():
            if build.candidate in noun_like and build.matched_kind == "root":
                listed = self._stem_rows(_text(build.root["id"]))
                if listed and not word_stems((build.candidate,), normalize_ja(_root_label(build.root)), listed,
                                             noun_like):
                    build.nouns_first = True
        # Distinctive subject marking may supply the verb even when a noun under
        # the same root already matched. Other generated alternatives remain
        # suggestions beside lexical matches.
        verb_builds: list[_CardBuild] = []
        verb_more = 0
        suggestions: tuple[tuple[str, int], ...] = ()
        if cards:
            suggestions = suggested_roots(candidates, self.verb_form_index() if verb_forms else {}, verb_forms,
                                          card_roots, nominal=nominal)
            main_roots = {_text(build.root['id']) for build in cards.values() if not build.sub_entry_id}
            if finite - main_roots:
                possible, verb_more = self._verb_form_builds(candidates, set(main_roots), roots,
                                                            verb_forms, verb_form_stems, nominal, eligible=finite)
                verb_builds = possible
        else:
            verb_builds, verb_more = self._verb_form_builds(candidates, card_roots, roots, verb_forms,
                                                            verb_form_stems, nominal, MAX_VERB_FORM_CARDS + max(0, limit - MAX_CARDS))
        card_roots.update(_text(build.root['id']) for build in verb_builds)
        quotes, more_quotes = self._quotes(word, card_roots, roots)
        quote_roots = {hit.root_id for hit in quotes}
        examples: dict[str, BlauExample] = {}
        example_overflow: set[str] = set()
        for row in phrase_rows:
            root_id = _text(row["root_id"])
            if (not root_id or root_id in card_roots or root_id in quote_roots or root_id in examples
                    or root_id in example_overflow):
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
        # An entry the word spells, with a conjunction or without (ונצף: the noun נצף before the
        # verb forms of וצף), is the word's own.
        bare = bare_keys(key)
        def rank(build):
            paths = matching_paths(word, build.matched_key, build.candidate)
            direct = any(not path for path in paths) or (
                build.candidate in bare and normalize_ja(build.matched_key) == build.candidate)
            finite_main = (_text(build.root['id']) in finite or build.verb_form) and not build.sub_entry_id
            suffix = bool(article_candidates(key)) and not is_function_word((key,)) and all('suffix' in path for path in paths)
            return (build.dot_mismatch, 0 if not finite or direct else 1 if finite_main else 2,
                    evidence_cost(paths) + suffix)
        builds.extend(verb_builds)
        builds.sort(key=rank)
        return BlauLookup(
            word=word, key=key, candidates=candidates,
            cards=tuple(replace(self._finish(build, roots), analyses=matching_paths(word, build.matched_key, build.candidate))
                        for build in builds[:limit]),
            more_cards=max(0, len(builds) - limit) + len(overflow) + verb_more,
            examples=tuple(examples.values()),
            more_examples=len(example_overflow),
            quotes=quotes, more_quotes=more_quotes, quote_exclusions=(tuple(card_roots),), quote_offset=MAX_QUOTE_HITS, suggestions=suggestions,
        )

    def _verb_form_builds(self, candidates: tuple[str, ...], card_roots: set[str],
                          roots: dict[str, Optional[sqlite3.Row]], kinds: frozenset[str],
                          stem_kinds: frozenset[str], nominal: frozenset[str] = frozenset(),
                          limit: int = MAX_VERB_FORM_CARDS, eligible: Optional[set[str]] = None) -> tuple[list[_CardBuild], int]:
        """The cards found by verb form (plan 4.4, PR 3): the entries the word's keys reach through the
        generated forms of a stem the entry lists (``shared.ja_verb_forms.verb_form_cards``, in the
        plan's order), other than the entries already cards (``card_roots``, which they join), of a
        kind switched on (``kinds``); at most ``MAX_VERB_FORM_CARDS``, the rest counted. A card opens
        on the word's stem when its kind is in ``stem_kinds`` and the word fits one listed stem, else
        on the whole entry with the stems as buttons. ``nominal``: the keys of a word with the article
        (``article_candidates``), which reach a stem only as its participle or verbal noun."""
        if not kinds:
            return [], 0
        index = self.verb_form_index()
        if not index:
            return [], 0
        builds: list[_CardBuild] = []
        more = 0
        for card in verb_form_cards(candidates, index, nominal):
            if card.entry_id in card_roots or (eligible is not None and card.entry_id not in eligible):
                continue
            inflected = card.kind == 'E' and (card.inflected_perfect or (
                'B' in kinds and under_conjunction(card.form, candidates[0])))
            finite = ('B' in kinds and (card.kind in kinds or card.kind in 'CE')
                      and card.finite_for(candidates[0]) and not is_function_word(candidates[:1]))
            nominal_form = card.form in nominal and card.form.endswith(('ינ', 'ונ')) and len(card.form) >= 5 and 'B' in kinds
            # A word that reached nothing else keeps the reading of its form under a clitic or a
            # pronoun (תכדמני, יעטוה): the only thing it reaches, said to be a possible analysis. Not a
            # form of three letters (תלת, אבן: mostly another word).
            alone = eligible is None and 'B' in kinds and card.kind == 'E' and len(card.form) >= 4
            if card.kind not in kinds and not inflected and not nominal_form and not finite and not alone:
                attested = card.kind in {'C', 'E'} and self._conn.execute(
                    "SELECT 1 FROM tokens WHERE root_id=? AND scope='citation' "
                    "AND key=? LIMIT 1", (card.entry_id, card.form)).fetchone()
                if not attested:
                    continue
            if len(builds) >= limit:
                more += 1
                continue
            root = self._root(roots, card.entry_id)
            if root is None:
                continue
            build = _CardBuild(root, card.form, "", MATCHED_VERB_FORM, "", "")
            build.verb_form, build.kind = card.evidence(), card.kind
            if card.kind in stem_kinds or ((inflected or nominal_form or finite) and stem_kinds):
                build.stem = card.stem
            builds.append(build)
            card_roots.add(card.entry_id)
        return builds, more

    def _stem_answer(self, word: str, key: str, candidates: tuple[str, ...], stem: int, root_text: str,
                     cards: dict[tuple[str, str, str], _CardBuild], overflow: set[tuple[str, str, str]],
                     roots: dict[str, Optional[sqlite3.Row]]) -> BlauLookup:
        """The cards of a root with a typed stem: one per entry whose NAME the root is (a root hit,
        not a form or sub-entry spelled the same) and which lists the stem, scoped to that stem's
        paragraph; else every card the root reaches, unscoped, so the reader sees the stems each
        entry has. The dotted reading first, as for a word (``dot_pattern`` of the typed root)."""
        builds = list(cards.values())
        pattern = dot_pattern(root_text)
        if "*" in pattern:
            for build in builds:
                build.dot_mismatch = not _dots_agree(pattern, build.matched_key)
            builds.sort(key=lambda b: b.dot_mismatch)
        chosen: list[_CardBuild] = []
        chosen_roots: set[str] = set()
        for build in builds:
            root_id = _text(build.root["id"])
            if build.matched_kind == "root" and root_id not in chosen_roots and stem in self._stem_rows(root_id):
                build.stem, build.sub_entry_id = stem, ""
                chosen.append(build)
                chosen_roots.add(root_id)
        shown = chosen or builds
        return BlauLookup(
            word=word, key=key, candidates=candidates,
            cards=tuple(self._finish(build, roots) for build in shown),
            more_cards=0 if chosen else len(overflow), examples=(), more_examples=0, stem=stem,
        )

    def quotation_page(self, result: BlauLookup, offset: int = 0):
        """Another bounded page, preserving the lookup's original lexical exclusions."""
        roots = {c.root_id for c in result.cards if not c.borrowed}
        if result.quote_exclusions:
            roots = set(result.quote_exclusions[0])
        return self._quotes(result.word, roots, {}, offset=offset)

    def _quotes(self, word: str, card_roots: set[str],
                roots: dict[str, Optional[sqlite3.Row]], offset: int = 0,
                limit: Optional[int] = None) -> tuple[tuple[BlauExpressionHit, ...], int]:
        """The entries (or sub-entries) whose citations or names hold ``word``, and how many more
        there are: the word as the expression search keys it (``word_keys``) against ``tokens``.
        Not the cards' entries (the reader has those), not a function word (פי is in a thousand
        entries and none of them is about it), not a 1.0.0 file. The entries that print the word
        exactly, and most often, come first; then the dictionary's order."""
        if not self.expressions_available() or len(lookup_key(word).split()) != 1:
            return (), 0   # several words are the expression search's, never any-of-them
        keys = query_keys(word)
        if not keys:
            return (), 0
        limit = MAX_QUOTE_HITS if limit is None else max(0, min(int(limit), 20))
        offset = max(0, int(offset))
        marks = ",".join("?" for _ in keys)
        place = "scope || ' ' || scope_id || ' ' || n"
        units = [r for r in self._conn.execute(
            f"SELECT root_id, COALESCE(sub_entry_id, '') AS sub_id, COUNT(DISTINCT {place}) AS places, "
            f"COUNT(DISTINCT CASE WHEN key = ? AND peeled = 0 THEN {place} END) AS exact "
            f"FROM {TOKENS_TABLE} WHERE key IN ({marks}) GROUP BY root_id, sub_id", (keys[0], *keys))
            if _text(r["root_id"]) not in card_roots]
        if not units:
            return (), 0
        ids = sorted({_text(r["root_id"]) for r in units})
        root_ordinal: dict[str, int] = {}
        sub_ordinal: dict[str, int] = {}
        for start in range(0, len(ids), 500):   # under SQLite's bound on parameters
            chunk = ids[start:start + 500]
            chunk_marks = ",".join("?" for _ in chunk)
            root_ordinal.update((str(r["id"]), int(r["ordinal"])) for r in self._conn.execute(
                f"SELECT id, ordinal FROM roots WHERE id IN ({chunk_marks})", chunk))
            sub_ordinal.update((str(r["id"]), int(r["ordinal"])) for r in self._conn.execute(
                f"SELECT id, ordinal FROM sub_entries WHERE root_id IN ({chunk_marks})", chunk))
        units.sort(key=lambda r: (-int(r["exact"]), -int(r["places"]), root_ordinal.get(_text(r["root_id"]), 0),
                                  sub_ordinal.get(r["sub_id"], -1) + 1 if r["sub_id"] else 0))
        accepts = [frozenset(keys)]
        hits: list[BlauExpressionHit] = []
        for row in units[offset:offset + limit]:
            unit = (_text(row["root_id"]), _text(row["sub_id"]))
            scopes = self._conn.execute(
                f"SELECT scope, scope_id, n, MAX(key = ? AND peeled = 0) AS exact FROM {TOKENS_TABLE} "
                f"WHERE root_id = ? AND COALESCE(sub_entry_id, '') = ? AND key IN ({marks}) "
                "GROUP BY scope, scope_id, n ORDER BY exact DESC, scope = ? , scope_id, n LIMIT ?",
                (keys[0], *unit, *keys, SCOPE_CITATION, MAX_QUOTE_PLACES)).fetchall()
            places: list[ExpressionPlace] = []
            for found in scopes:
                scope = (str(found["scope"]), str(found["scope_id"]), int(found["n"]))
                items = [(int(r["pos"]), str(r["key"])) for r in self._conn.execute(
                    f"SELECT pos, key FROM {TOKENS_TABLE} WHERE scope = ? AND scope_id = ? AND n = ? ORDER BY pos, rowid",
                    scope)]
                match = ordered_match(items, accepts, 0)
                place_found = self._place(scope, items, match, (word,), (keys,)) if match is not None else None
                if place_found is not None:
                    places.append(place_found)
            hit = self._hit(unit, places, roots) if places else None
            if hit is not None:
                hits.append(replace(hit, places_total=max(int(row["places"]), len(places))))
        return tuple(hits), max(len(units) - offset - limit, 0)

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
        key = ("", root_id, sub_entry_id)
        build = cards.get(key)
        if build is None:
            build = cards[key] = _CardBuild(root, candidate, _text(row["raw"]), row["kind"], via, sub_entry_id)
            card_roots.add(root_id)
        elif evidence_cost(analysis_paths(_text(row['raw']), candidate)) < evidence_cost(
                analysis_paths(build.matched_key, build.candidate)):
            build.candidate, build.matched_key, build.matched_kind = candidate, _text(row['raw']), row['kind']
        form_id = _text(row["form_id"])
        if form_id and form_id not in build.matched_form_ids:
            build.matched_form_ids.append(form_id)

    def _finish(self, build: _CardBuild, roots: dict[str, Optional[sqlite3.Row]]) -> BlauCard:
        root = build.root
        root_id = _text(root["id"])
        senses: list[BlauSense] = []
        under: list[str] = []   # the sub-entry each sense sits under, '' for the entry itself
        seen: set[str] = set()
        for form_id in build.matched_form_ids:
            form = self._conn.execute(f"SELECT {_FORM_COLUMNS} FROM forms WHERE id = ?", (form_id,)).fetchone()
            if form is not None and form_id not in seen:
                seen.add(form_id)
                senses.append(self._sense(form, roots, matched=True))
                under.append(_text(form["sub_entry_id"]))
        # The scope is the chosen stem's paragraph (its stems rows, wherever under the entry they
        # are), else the matched sub-entry, else the whole entry. All go through the root index
        # (there is none on sub_entry_id or stem_id).
        stem_rows = self._stem_rows(root_id)
        if build.stem and stem_rows.get(build.stem):
            ids = stem_rows[build.stem]
            scope_sql = f"root_id = ? AND stem_id IN ({','.join('?' for _ in ids)})"
            scope_args: tuple[str, ...] = (root_id, *ids)
        elif build.sub_entry_id:
            scope_sql, scope_args = "root_id = ? AND sub_entry_id = ?", (root_id, build.sub_entry_id)
        else:
            scope_sql, scope_args = "root_id = ?", (root_id,)
        # Every sense of the scope with content, the matched ones first (the panel shows the first
        # few and the rest on a click); beyond MAX_SENSES, counted only. Form ids follow the
        # dictionary's own order (the builder numbers the forms as it reads the entries);
        # `ordinal` is per stem and would interleave the stems.
        beyond = max(0, len(senses) - MAX_SENSES)
        forms = self._conn.execute(f"SELECT {_FORM_COLUMNS}, stem_id FROM forms WHERE {scope_sql} ORDER BY id",
                                   scope_args).fetchall()
        if build.nouns_first:
            # The word fits none of the entry's stems: the senses outside the stem paragraphs first,
            # each part in the dictionary's order.
            stem_ids = {stem_id for ids in stem_rows.values() for stem_id in ids}
            forms.sort(key=lambda form: _text(form["stem_id"]) in stem_ids)
        for form in forms:
            form_id = _text(form["id"])
            if form_id in seen or not _has_content(form):
                continue
            seen.add(form_id)
            if len(senses) >= MAX_SENSES:
                beyond += 1
                continue
            senses.append(self._sense(form, roots, matched=False))
            under.append(_text(form["sub_entry_id"]))
        sub_entry_id = build.sub_entry_id
        if build.stem and stem_rows.get(build.stem):
            # A chosen stem whose paragraph sits under one sub-entry (דלל VII is under דאלה̈):
            # the card names that sub-entry, as a sub-entry hit's card does.
            subs = {_text(r[0]) for r in self._conn.execute(
                f"SELECT DISTINCT sub_entry_id FROM forms WHERE {scope_sql}", scope_args)}
            sub_entry_id = subs.pop() if len(subs) == 1 else ""
        shown = self._head_sub_entries(senses[:MAX_SENSES], under, sub_entry_id, _text(root["name_ja"]))
        sub_entry = ""
        if sub_entry_id:
            row = self._conn.execute("SELECT name_ja FROM sub_entries WHERE id = ?",
                                     (sub_entry_id,)).fetchone()
            sub_entry = _text(row["name_ja"]) if row is not None else ""
        return BlauCard(
            root_id=root_id, name=_text(root["name_ja"]), arabic=_text(root["arabic_spelling"]),
            page=_page(root["start_page_printed"]), url=entry_url(root_id),
            candidate=build.candidate, matched_key=build.matched_key, matched_kind=build.matched_kind,
            sub_entry=sub_entry, via=build.via, senses=shown,
            more_senses=beyond, dot_mismatch=build.dot_mismatch,
            root_label=_root_label(root), stems=tuple(stem_rows),
            stem=build.stem if stem_rows.get(build.stem) else 0,
            verb_form=build.verb_form, kind=build.kind,
        )

    def _head_sub_entries(self, senses: list[BlauSense], under: list[str], scope: str,
                          entry_name: str) -> tuple[BlauSense, ...]:
        """The senses with a head where their sub-entry changes (``BlauSense.sub_entry``): the
        sub-entry's name where a run under one begins, unless it is the card's own ``scope``, and
        the entry's name where the entry's own senses come back after one."""
        names: dict[str, str] = {}
        out: list[BlauSense] = []
        before = scope
        for sense, sub_entry_id in zip(senses, under):
            if sub_entry_id != before:
                if sub_entry_id and sub_entry_id not in names:
                    row = self._conn.execute("SELECT name_ja FROM sub_entries WHERE id = ?",
                                             (sub_entry_id,)).fetchone()
                    names[sub_entry_id] = _text(row["name_ja"]) if row is not None else ""
                head = names[sub_entry_id] if sub_entry_id else entry_name
                if head:
                    sense = replace(sense, sub_entry=head)
                before = sub_entry_id
            out.append(sense)
        return tuple(out)

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
            is_erroneous=bool(form["is_erroneous"]), matched=matched, details_available=self._has_details(form['id']),
        )

    def _name_position(self, row: Mapping[str, Any], cache: dict[tuple[str, str], list[str]]) -> int:
        """Which of its scope's names a root or sub-entry hit matched (0: the first); 0 for any
        other kind. ``cache`` keeps each scope's names for one lookup."""
        kind = row["kind"]
        if kind == "root":
            scope = ("roots", _text(row["root_id"]))
        elif kind == "sub_entry":
            scope = ("sub_entries", _text(row["sub_entry_id"]))
        else:
            return 0
        if scope not in cache:
            found = self._conn.execute(f"SELECT names_json FROM {scope[0]} WHERE id = ?", (scope[1],)).fetchone()
            cache[scope] = _names_of(found["names_json"]) if found is not None else []
        raw = _text(row["raw"])
        return cache[scope].index(raw) if raw in cache[scope] else 0

    def _carries_root(self, cache: dict[str, Optional[sqlite3.Row]], root_id: str, key: str) -> bool:
        """Does ``key`` carry the root of the entry ``root_id`` under one of its printed names
        (``shared.ja_roots.carries_root``)? True when the entry cannot be read."""
        root = self._root(cache, root_id)
        if root is None:
            return True
        names = _names_of(root["names_json"]) or [_text(root["name_ja"])]
        if _root_label(root) not in names:
            names.append(_root_label(root))
        return any(carries_root(key, normalize_ja(name)) for name in names)

    def _root(self, cache: dict[str, Optional[sqlite3.Row]], root_id: str) -> Optional[sqlite3.Row]:
        if root_id not in cache:
            row = self._conn.execute(
                "SELECT id, name_ja, names_json, arabic_spelling, start_page_printed, is_cross_reference, target_id "
                "FROM roots WHERE id = ?", (root_id,)).fetchone() if root_id else None
            if row is not None and len(_text(row["name_ja"]).split()) >= 3 and " " not in _root_label(row):
                row = dict(row) | {"name_ja": _root_label(row)}
            cache[root_id] = row
        return cache[root_id]

    def _carries_subentry(self, sub_id: str, key: str) -> bool:
        """An explicit noun form belongs to its subentry, independently of its parent label."""
        if not sub_id:
            return False
        row = self._conn.execute("SELECT name_ja FROM sub_entries WHERE id = ?", (sub_id,)).fetchone()
        return row is not None and any(carries_root(key, normalize_ja(name))
                                       for name in name_alternatives(row[0]) if normalize_ja(name))

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
    if meta.get("version") not in SERVED_SCHEMA_VERSIONS:
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
    if meta.get("version") == SCHEMA_VERSION:
        if not {"spelling_keys", "lookup_aliases", "constructions"} <= tables:
            return "missing spelling_keys table: rebuild the sidecar", {}
        # The expression search's table, its index, its count and the keying it was written by.
        if TOKENS_TABLE not in tables:
            return f"schema {SCHEMA_VERSION} without the {TOKENS_TABLE} table", {}
        index = conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                             (TOKENS_INDEX,)).fetchone()
        if index is None:
            return f"expression index {TOKENS_INDEX} missing", {}
        expected = meta.get("rows_tokens", "")
        actual = conn.execute(f"SELECT COUNT(*) FROM {TOKENS_TABLE}").fetchone()[0]
        if not expected.isdigit() or int(expected) != int(actual):
            return f"meta.rows_tokens = {expected!r} but the table holds {actual} rows", {}
        if meta.get("keying_version") != str(KEYING_VERSION):
            return (f"tokens keyed by expression keying {meta.get('keying_version')!r}; this process runs "
                    f"{KEYING_VERSION}: rebuild the sidecar"), {}
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
