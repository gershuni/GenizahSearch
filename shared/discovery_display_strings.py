# -*- coding: utf-8 -*-
"""The ONE bilingual display vocabulary both discovery surfaces render
(Phase 136, plan 136-10, PANEL-01 / PANEL-02 / NOVEL-01).

The browse-page discovery panel and the corpus-wide findings page are built
by two separate plan tracks. Every claim-level word a reader sees on EITHER
of them is defined here, once, so the tracks can be built in parallel
without either one owning a shared file, and so every honesty-sensitive
string sits in one auditable place. That last point is not theoretical: the
findings sketch broke its own prohibited-wording rule in HAND-WRITTEN PROSE,
not in data (`.claude/skills/sketch-findings-genizahsearch/references/
findings-page.md`, "What to Avoid" -- the caveat's first draft used one of
the three relation words D-21 prohibits, inside a NEGATION, and the suite
caught it because a grep-based CI guard cannot see negation).

Note that this file's own prose obeys the same rule: the three prohibited
words are NEVER spelled out here, not even to explain the rule they belong
to, because a phrase sitting in a docstring is one careless copy away from a
surface. They are enumerated in exactly one place -- `136-CONTEXT.md` D-21 --
and enforced from exactly one place --
`tests/render_smoke/discovery_honesty_gate.py`.

WHERE A STRING LIVES
--------------------
`tr()` (`genizah_translations.py`) owns PAGE CHROME -- nav labels, page
titles and meta, mode-strip labels, the "Show as" control and its options,
sort options, the result-bar count phrasing, and the page-level
availability card.

THIS MODULE owns the CLAIM VOCABULARY -- relation chips and their tooltips,
row headlines, coverage labels, section headers, disclosure toggles, the
novelty candidacy wording, the panel's service-state copy, and the filter
short codes.

A string lives in EXACTLY ONE of the two. `tests/
test_discovery_display_strings.py::test_no_translation_key_duplicates_a_display_string`
fails if one is defined in both.

COMPOSITION, NOT A SECOND VOCABULARY
------------------------------------
Band labels come from `shared.discovery_band_labels.band_label` (TOOLTIP
use only -- no label string is redefined here). Bucket names come from
`shared.discovery_main_pool.bucket_label`. The two-bucket rule sentence
comes from `shared.discovery_main_pool.main_pool_sentence`. The "show more
possible matches" toggle and the recall disclaimer come from
`shared.discovery_band_labels`. None of those strings is retyped in this
file, and a test asserts so by scanning this module's source for each of
them.

THE FOUR HONESTY RULES EVERY STRING HERE OBEYS
----------------------------------------------
1. **No precision percentage and no confidence interval, ever** (D-06 /
   D-06a). The ONE permitted percentage is matched-letter page coverage on
   a DIRECT-family row (D-08a / D-21), and it renders only in its qualified
   form.
2. **None of the three relation words D-21 prohibits**, in any form,
   including negated. The owner's reason, verbatim: *"we are not sure that
   tier_a is same work and the next are parallels, just heuristics."* The
   words themselves are listed in `136-CONTEXT.md` D-21 and enforced by the
   shared honesty gate; they are not repeated here.
3. **No stored vocabulary key ever reaches markup.** `claim_type` values
   are this module's INPUT domain; they are never part of its OUTPUT. The
   relation filter travels as the short codes `dw` / `qw` / `st` precisely
   so a grep-based invariant over rendered HTML stays clean
   (`discovery-panel-layout.md`, "What to Avoid").
4. **No human-review badge string is defined here at all** (D-13f). The
   badge is dropped until the provenance of the 121 human-confirmed rows is
   established, and the safest way to keep it off a surface is for the
   string not to exist. The review-status helper in
   `shared.discovery_band_labels` is deliberately NOT imported, wrapped or
   re-exported here, and a test asserts this module names it nowhere.

RESTRICTED-CORPUS MASKING (D-25)
--------------------------------
No string here names a restricted corpus. Prior-record provenance is
either nameable ("recorded in the catalogue") or masked ("recorded in
another reference source") -- and that masked label is produced by the
build's own `masked_provenance_label`, never composed here.
"""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, Optional, Tuple

import scripts.discovery_ids as ids
from shared.discovery_band_labels import (
    RECALL_DISCLAIMER as _RECALL_DISCLAIMER,
)
from shared.discovery_band_labels import (
    SHOW_MORE_TOGGLE as _SHOW_MORE_TOGGLE,
)
from shared.discovery_band_labels import band_label as _band_label
from shared.discovery_main_pool import bucket_label as _bucket_label
from shared.discovery_main_pool import main_pool_sentence as _main_pool_sentence

# ---------------------------------------------------------------------------
# Language handling -- identical convention to
# `shared.discovery_band_labels.band_label`: an unknown `lang` resolves to
# EN rather than raising, because a surface must always render SOMETHING.
# ---------------------------------------------------------------------------


def _lang_key(lang: str) -> str:
    return "he" if lang == "he" else "en"


def _pick(table: Mapping[str, str], lang: str) -> str:
    return table[_lang_key(lang)]


# U+05BE HEBREW PUNCTUATION MAQAF. D-21 fixes the Hebrew section headers'
# hyphen at this codepoint specifically. Written as an escape and composed
# in, so a copy/paste through a tool that normalizes punctuation cannot
# silently substitute an ASCII hyphen or a Unicode dash.
_MAQAF = "־"

# U+00B7 MIDDLE DOT -- D-21's row separator ("Matches <work> - 68% of page").
_DOT = "·"


# ---------------------------------------------------------------------------
# Relation chips. The KEY set is the RENDERED-RELATION vocabulary
# (`ids.RENDERED_RELATIONS`), not the stored `claim_type` vocabulary; its
# display labels are the only thing that leaves.
#
# ⟨AMENDED 2026-08-12 -- C-track, owner ruling 2026-08-11, the "softer
# register"; `docs/specs/discovery-relation-matrix-v1.md` §1 is the frozen
# table these strings come from⟩
#
# Two changes, one of which is a correction rather than a rewording:
#
#   * "Direct match" / "התאמה ישירה" is RETIRED. It was the label on 28,462 of
#     28,464 main-pool rows -- a chip that says the same thing about almost
#     everything is not telling a reader anything, and "direct" overclaims what
#     a text match establishes. It becomes "Matches this work": a claim about
#     TEXT, not about identity.
#   * "Partial match" for `quotes_this_work` was a MISNOMER, not merely a
#     stiff phrase. The row means "this page includes a quotation OF the work",
#     which is not a partial version of matching it. "Includes a quotation".
#     (Note the noun: the honesty gate prohibits the bare word "quotes" as
#     relation wording -- `_PROHIBITED_PHRASES` -- so the frozen vocabulary
#     says "quotation", and this is one reason it does.)
#
# The keys are re-based on the rendered vocabulary because that is what callers
# will pass once every surface reads `rendered_relation`. It is safe during the
# transition: `direct_witness`, `quotes_this_work` and `shared_text` are
# spelled identically in both vocabularies, so a caller still passing a raw
# `claim_type` lands on the same row it always did.
#
# `uncertain` MUST be present before any surface reads the matrix column.
# `relation_chip` raises on an unknown key, and `findings_rows.py` wraps the
# call in `except ValueError` -- so a missing entry would not fail loudly
# there, it would silently drop the chip from every fail-closed row.
#
# `work_quotes_page` is DELIBERATELY ABSENT. Assigning its reader strings is an
# owner item deferred until a validated direction signal ships (§1), and the
# matrix provably never emits it in v1 (asserted in
# `tests/test_discovery_relation_matrix.py`). Absence therefore means "this
# cannot reach a reader", and the raise is the proof rather than a gap.
# ---------------------------------------------------------------------------

_RELATION_CHIP: Dict[str, Dict[str, str]] = {
    ids.RENDERED_RELATION_DIRECT_WITNESS: {
        "en": "Matches this work", "he": "מתאים לחיבור"},
    ids.RENDERED_RELATION_QUOTES_THIS_WORK: {
        "en": "Includes a quotation", "he": "כולל ציטוט"},
    ids.RENDERED_RELATION_SHARED_TEXT: {
        "en": "Shares text with this work", "he": "חולק טקסט"},
    ids.RENDERED_RELATION_UNCERTAIN: {
        "en": "Needs review", "he": "דורש בדיקה"},
}


def relation_chip(relation_kind: str, lang: str = "en") -> str:
    """The reader-facing chip for a rendered relation -- "Matches this work" /
    "Includes a quotation" / "Shares text with this work" / "Needs review" and
    their Hebrew equivalents.

    Raises `ValueError` on an unknown kind: a blank chip on a real row is a
    worse failure than a loud one, because it silently hides what kind of
    match the reader is looking at. The stored key is NEVER returned."""
    entry = _RELATION_CHIP.get(relation_kind)
    if entry is None:
        raise ValueError(
            "relation_chip: unknown relation kind {!r} (expected one of {})".format(
                relation_kind, sorted(_RELATION_CHIP)
            )
        )
    return _pick(entry, lang)


def relation_tooltip(evidence_source: str, confidence_band: str, lang: str = "en") -> str:
    """The frozen band label, UNMODIFIED, for use as the relation chip's
    `title` tooltip.

    Band labels are tooltip-only on both surfaces
    (`discovery-panel-layout.md`, "Amended twice since"), which is how the
    display-contract change stays reversible: the precise label is one hover
    away and never lost. This function adds nothing to, and removes nothing
    from, `shared.discovery_band_labels.band_label`."""
    return _band_label(evidence_source, confidence_band, lang)


# ---------------------------------------------------------------------------
# Row headline (D-21 match framing) + the matched-letter coverage qualifier.
# ---------------------------------------------------------------------------

# D-21's owner-selected row strings. The coverage clause is APPENDED to the
# direct-family form; `{work_title}` and `{pct}` are the only substitutions.
_ROW_DIRECT: Dict[str, str] = {
    "en": "Matches {work_title}",
    "he": "התאמה ל{work_title}",
}

# ⟨AMENDED 2026-08-12 -- C-track⟩ Was "Partial match with {work_title}" /
# "התאמה חלקית ל{work_title}". Retired for the same reason as the chip: the row
# means the page includes a quotation OF the work, which is not a partial
# version of matching it. Left unchanged, this headline would have contradicted
# the chip on the very same row.
_ROW_PARTIAL: Dict[str, str] = {
    "en": "Includes a quotation from {work_title}",
    "he": "כולל ציטוט מ{work_title}",
}

# ⟨ADDED 2026-08-12 -- C-track⟩ The fail-closed row. Deliberately asserts
# NOTHING: the work title alone, with no relation verb. The chip beside it
# already says "Needs review", so repeating that here would both duplicate the
# chip and dress an absence of knowledge up as a finding. A0b ratifies the
# wording (matrix spec 5a.1); the semantics -- no assertion -- are fixed by
# section 2's missing-input rule and are not a wording choice.
_ROW_UNCERTAIN: Dict[str, str] = {
    "en": "{work_title}",
    "he": "{work_title}",
}

_ROW_SHARED: Dict[str, str] = {
    "en": "Shares text with {work_title}",
    "he": "חולק טקסט עם {work_title}",
}

# D-21 verbatim ("68% of page" / "68% מהדף") with the matched-letter
# qualifier appended immediately after it. BOTH parts are load-bearing:
# "of page" / "מהדף" is the anchor the shared honesty gate recognises as the
# ONE legitimate percentage on a discovery surface, and the parenthetical
# is what makes the figure unambiguously matched-letter coverage rather
# than anything a reader could mistake for a precision estimate.
_COVERAGE_CLAUSE: Dict[str, str] = {
    "en": "{pct}% of page (matched letters)",
    "he": "{pct}% מהדף (אותיות תואמות)",
}

_COVERAGE_LABEL: Dict[str, str] = {
    "en": "matched-letter page coverage",
    "he": "כיסוי הדף באותיות תואמות",
}


def _coverage_percent(coverage_ppm: Optional[int]) -> Optional[int]:
    """Parts-per-million -> a whole percent, clamped to [0, 100]. `None`
    (the 11,941 shared-wording claims carry no matched letters at all)
    stays `None` so the caller omits the clause entirely rather than
    rendering a fabricated 0%."""
    if coverage_ppm is None:
        return None
    try:
        pct = round(float(coverage_ppm) / 10000.0)
    except (TypeError, ValueError):
        return None
    return max(0, min(100, int(pct)))


def row_headline(
    work_title: str,
    coverage_ppm: Optional[int],
    relation_kind: str,
    lang: str = "en",
    evidence_source: Optional[str] = None,
) -> str:
    """The match-framing headline for one row.

    A coverage percentage is included ONLY for the direct family, and only
    in its qualified matched-letter form:

    - `relation_kind` must be `direct_witness`; a partial match or a
      shared-text row never carries a percentage, because the figure has no
      honest meaning there.
    - `evidence_source`, when supplied, must not be `propagated`. Coverage
      is a DIRECT-family measurement (D-08a / Codex F-08: the metric was
      mis-described and is simply absent for propagated rows), and a
      propagated row can still carry `claim_type='direct_witness'` --
      5,604 `not_evaluated` claims do -- so the relation kind alone is not
      a sufficient gate. Omitting the argument gates on the relation kind
      alone, which is the conservative reading for a caller that does not
      know the family.
    - `coverage_ppm` of `None` omits the clause and its separator.
    """
    if relation_kind == ids.RENDERED_RELATION_DIRECT_WITNESS:
        template = _ROW_DIRECT
    elif relation_kind == ids.RENDERED_RELATION_QUOTES_THIS_WORK:
        template = _ROW_PARTIAL
    elif relation_kind == ids.RENDERED_RELATION_SHARED_TEXT:
        template = _ROW_SHARED
    elif relation_kind == ids.RENDERED_RELATION_UNCERTAIN:
        # C-track: an EXPLICIT branch, because the previous `else: raise` was
        # being load-bearing by accident. `findings_rows.py` catches the
        # ValueError and omits the coverage figure, which happens to be the
        # right outcome -- but reached through an exception, so nothing said so
        # and nothing tested it. `work_quotes_page` still raises: it has no
        # owner-assigned strings and the matrix provably never emits it.
        template = _ROW_UNCERTAIN
    else:
        raise ValueError(
            "row_headline: unknown relation kind {!r}".format(relation_kind)
        )

    headline = _pick(template, lang).format(work_title=work_title)

    if relation_kind != ids.RENDERED_RELATION_DIRECT_WITNESS:
        return headline
    if evidence_source == ids.EVIDENCE_SOURCE_PROPAGATED:
        return headline

    pct = _coverage_percent(coverage_ppm)
    if pct is None:
        return headline
    clause = _pick(_COVERAGE_CLAUSE, lang).format(pct=pct)
    return "{} {} {}".format(headline, _DOT, clause)


def coverage_label(lang: str = "en") -> str:
    """The standalone name of the one permitted figure, for a tooltip, an
    `aria-label` or a column header. It is page coverage measured in
    MATCHED LETTERS -- never a precision estimate, and never derived from
    the edit-distance field the build warns about."""
    return _pick(_COVERAGE_LABEL, lang)


_LOW_COVERAGE_NOTE: Dict[str, str] = {
    "en": "This match covers only a small part of the page.",
    "he": "ההתאמה מכסה רק חלק קטן מהדף.",
}


def low_coverage_note(lang: str = "en") -> str:
    """The optional note under a row whose match covers little of the page.
    It reports the EVIDENCE, never a verdict: a short liturgical passage may
    be exactly the correct identification for a prayer book (the honest
    counter-argument the owner accepted at gate 1, D-13c)."""
    return _pick(_LOW_COVERAGE_NOTE, lang)


_GRANULARITY_SUBLINE: Dict[str, str] = {
    "en": "↳ Same work at another granularity: {other_work_title}",
    "he": "↳ אותו חיבור ברמת פירוט אחרת: {other_work_title}",
}


def granularity_subline(other_work_title: str, lang: str = "en") -> str:
    """The optional sub-line under a row whose identical-span group
    collapsed two catalogued granularities of the SAME work (D-13d's
    author-gated rule -- e.g. a Torah commentary and its per-book variant,
    same author, prefix-shared title). A display-time collapse, not a data
    fix, so the collapsed sibling stays visible as context rather than
    disappearing."""
    return _pick(_GRANULARITY_SUBLINE, lang).format(other_work_title=other_work_title)


_MISSING_TITLE: Dict[str, str] = {
    "en": "Title unavailable",
    "he": "הכותרת אינה זמינה",
}


def missing_title(lang: str = "en") -> str:
    """What a row shows when the work's title could not be resolved. Four of
    thirteen sampled manuscripts had no titles for their elsewhere-claims;
    that is a service-layer gap, and saying so is honest where a silently
    blank row is not."""
    return _pick(_MISSING_TITLE, lang)


_NOT_AN_IDENTIFICATION_NOTE: Dict[str, str] = {
    "en": "Not identifications — these pages share wording with the works listed.",
    "he": "אינם זיהויים — דפים אלה חולקים ניסוח עם החיבורים המנויים.",
}


def not_an_identification_note(lang: str = "en") -> str:
    """The italic qualifier the middle disclosure bucket carries. D-13e
    keeps that bucket as a distinct third level and requires it be
    "explicitly never presented as an identification"."""
    return _pick(_NOT_AN_IDENTIFICATION_NOTE, lang)


# ---------------------------------------------------------------------------
# Section headers (D-21, with the Hebrew maqaf at U+05BE).
# ---------------------------------------------------------------------------

SECTION_ON_THIS_PAGE = "on_this_page"
SECTION_ELSEWHERE_IN_MANUSCRIPT = "elsewhere_in_manuscript"
SECTION_OTHER_MANUSCRIPTS = "other_manuscripts"
SECTION_PAGES_MATCHING_THIS_PAGE = "pages_matching_this_page"

_SECTION_HEADERS: Dict[str, Dict[str, str]] = {
    SECTION_ON_THIS_PAGE: {
        "en": "On this page",
        "he": "בדף זה",
    },
    SECTION_ELSEWHERE_IN_MANUSCRIPT: {
        "en": "Elsewhere in this manuscript",
        "he": "במקומות אחרים בכתב" + _MAQAF + "היד",
    },
    SECTION_OTHER_MANUSCRIPTS: {
        "en": "Other manuscripts matching {work_title}",
        "he": "כתבי" + _MAQAF + "יד נוספים התואמים ל{work_title}",
    },
    SECTION_PAGES_MATCHING_THIS_PAGE: {
        "en": "Pages matching this page in other manuscripts",
        "he": "דפים התואמים לדף זה בכתבי" + _MAQAF + "יד אחרים",
    },
}


def section_header(section_key: str, lang: str = "en", work_title: str = "") -> str:
    """One of the four panel/work-page section headers, in match framing.

    `work_title` is substituted into `SECTION_OTHER_MANUSCRIPTS` and ignored
    by the other three. Raises `ValueError` on an unknown key rather than
    rendering an empty heading."""
    entry = _SECTION_HEADERS.get(section_key)
    if entry is None:
        raise ValueError(
            "section_header: unknown section key {!r} (expected one of {})".format(
                section_key, sorted(_SECTION_HEADERS)
            )
        )
    return _pick(entry, lang).format(work_title=work_title)


# ---------------------------------------------------------------------------
# Disclosure toggles. D-13e ratified THREE levels for the panel:
# (1) identifications, default visible; (2) "Also shares text with",
# collapsed; (3) "Show more possible matches". Ruling F adds a FOURTH,
# orthogonal opt-in: catalogue-divergent rows, hidden by default behind an
# explicitly warned toggle.
# ---------------------------------------------------------------------------

TOGGLE_MORE_MATCHES = "more_matches"
TOGGLE_ALSO_SHARES_TEXT = "also_shares_text"
TOGGLE_DIVERGENCE = "divergence"

_TOGGLE_ALSO_SHARES_TEXT: Dict[str, str] = {
    "en": "Also shares text with",
    "he": "חולק טקסט גם עם",
}

# Ruling F (136-GATE1-DECISIONS.md section F). Divergent rows are neither
# auto-hidden by policy nor silently trusted: the SYSTEM never treats the
# catalogue's reading as a verdict, it only surfaces the non-correspondence
# and lets the reader decide. The wording below therefore states THAT the two
# do not correspond and says explicitly that neither side has been adjudicated
# -- it must never assert which side is right.
#
# "DO NOT CORRESPOND", NEVER "DISAGREE" / "CONFLICT" (owner ruling,
# 2026-08-06). A disagreement implies parties, and parties imply that one of
# them is mistaken -- which is precisely the adjudication ruling F says nobody
# has performed. The owner authored the Hebrew (`חוסר התאמה עם הקטלוג`, "lack
# of correspondence with the catalogue") for the findings-page selector and the
# ENGLISH follows it rather than the reverse.
#
# These strings had been left on the older "disagree"/"conflict" vocabulary
# after the selector was renamed, so one screen carried two vocabularies for
# one fact -- a reader filtered on "do not correspond" and got rows labelled
# "Disagrees" (found by external review, 2026-08-06). Both surfaces read this
# module, so the panel and the findings page change together.
_TOGGLE_DIVERGENCE: Dict[str, str] = {
    "en": "Show findings that do not correspond to the catalogue",
    "he": "הצג ממצאים שאינם מתאימים לקטלוג",
}

_DIVERGENCE_WARNING: Dict[str, str] = {
    "en": (
        "These findings do not correspond to an existing catalogue "
        "identification. Neither side has been adjudicated — read them with "
        "that in mind."
    ),
    "he": (
        "ממצאים אלה אינם מתאימים לזיהוי קטלוגי קיים. אף צד לא הוכרע — "
        "קראו אותם בהתאם."
    ),
}


def disclosure_toggle(toggle_key: str, lang: str = "en") -> str:
    """The label of one disclosure toggle.

    `TOGGLE_MORE_MATCHES` DELEGATES to
    `shared.discovery_band_labels.SHOW_MORE_TOGGLE` -- that string is
    already the D-11 shared constant and is not retyped here."""
    if toggle_key == TOGGLE_MORE_MATCHES:
        return _pick(_SHOW_MORE_TOGGLE, lang)
    if toggle_key == TOGGLE_ALSO_SHARES_TEXT:
        return _pick(_TOGGLE_ALSO_SHARES_TEXT, lang)
    if toggle_key == TOGGLE_DIVERGENCE:
        return _pick(_TOGGLE_DIVERGENCE, lang)
    raise ValueError(
        "disclosure_toggle: unknown toggle key {!r} (expected one of {})".format(
            toggle_key,
            [TOGGLE_MORE_MATCHES, TOGGLE_ALSO_SHARES_TEXT, TOGGLE_DIVERGENCE],
        )
    )


def divergence_warning(lang: str = "en") -> str:
    """The warning ruling F requires beside the divergence control. It records
    the non-correspondence and explicitly declines to adjudicate it."""
    return _pick(_DIVERGENCE_WARNING, lang)


# The per-ROW marker. The toggle above is a control and the warning is the
# prose beside it; neither travels with a row once the reader has opened the
# axis, and on the findings page a divergent row then sits in a flat list
# among ordinary ones. So the row carries its own marker, and the marker
# states BOTH facts on its face: that the two do not correspond, and that
# neither side has been adjudicated.
#
# The second half is not decoration. `divergence_correctness` -- ruling L's
# human-only column -- is NULL on every shipped row, so no adjudication
# exists; a marker naming only the non-correspondence would leave a reader free
# to supply the missing half themselves, and the owner's own reading of real
# cases is that BOTH directions occur. The wording therefore never says which
# side is right, in either language.
#
# "Does not correspond", never "Disagrees" -- see `_TOGGLE_DIVERGENCE` above
# for the full reasoning. This chip is the string a reader meets MOST often
# (~23.6% of rows carry it), so it is the one that most needs to match the
# control they filtered with.
_DIVERGENCE_CHIP: Dict[str, str] = {
    "en": "Does not correspond to the catalogue — not adjudicated",
    "he": "אינו מתאים לקטלוג — לא הוכרע",
}


def divergence_chip(lang: str = "en") -> str:
    """The neutral marker one catalogue-divergent row carries.

    NEUTRAL BY CONSTRUCTION: it is one string with no variants, so there is
    nothing here to colour-code by kind and no tier for a renderer to key a
    treatment on (D-24). It names a relationship between two records; it makes
    no claim about the match's quality and carries no figure of any kind."""
    return _pick(_DIVERGENCE_CHIP, lang)


# ---------------------------------------------------------------------------
# Related pages (D-11a). These are NOT work identifications: they are
# unevaluated candidate alignments between this page and pages of other
# manuscripts, and the label must say so on its face.
# ---------------------------------------------------------------------------

_RELATED_PAGES_LABEL: Dict[str, str] = {
    "en": "unevaluated candidate alignments",
    "he": "התאמות מועמדות שלא הוערכו",
}

_RELATED_PAGES_COUNT_ONE: Dict[str, str] = {
    "en": "1 unevaluated candidate alignment",
    "he": "התאמה מועמדת אחת שלא הוערכה",
}

_RELATED_PAGES_COUNT_MANY: Dict[str, str] = {
    "en": "{count} unevaluated candidate alignments",
    "he": "{count} התאמות מועמדות שלא הוערכו",
}


def related_pages_label(lang: str = "en") -> str:
    """The bare label for the related-pages count."""
    return _pick(_RELATED_PAGES_LABEL, lang)


def related_pages_count_line(count: int, lang: str = "en") -> str:
    """The related-pages count line, with singular agreement in both
    languages (Hebrew number-noun agreement is not optional)."""
    if count == 1:
        return _pick(_RELATED_PAGES_COUNT_ONE, lang)
    return _pick(_RELATED_PAGES_COUNT_MANY, lang).format(count=count)


# ---------------------------------------------------------------------------
# Delegated bucket vocabulary. `bucket_label` and `main_pool_sentence` live
# in `shared/discovery_main_pool.py` and are the SOLE definitions of those
# strings; the accessors below exist so a surface can take its whole
# vocabulary from one import without either string being redefined.
# ---------------------------------------------------------------------------


def bucket_name(in_main_pool: bool, lang: str = "en") -> str:
    """The bilingual bucket name, straight from
    `shared.discovery_main_pool.bucket_label`.

    The second bucket means there was NOT ENOUGH EVIDENCE for the main-pool
    rule -- it never means the identification is probably wrong, and its
    largest single group is same-work claims."""
    return _bucket_label(in_main_pool, lang)


def rule_sentence(lang: str = "en") -> str:
    """The ONE reader-facing sentence describing the two-bucket rule,
    straight from `shared.discovery_main_pool.main_pool_sentence` (itself
    pinned byte-for-byte against the methods page)."""
    return _main_pool_sentence(lang)


def recall_disclaimer(lang: str = "en") -> str:
    """The D-12 recall disclaimer, straight from
    `shared.discovery_band_labels.RECALL_DISCLAIMER`."""
    return _pick(_RECALL_DISCLAIMER, lang)


# ---------------------------------------------------------------------------
# Novelty candidacy wording (NOVEL-01, as amended; findings-page.md
# "Novelty is a prominent switch"). Reproduced verbatim from the ratified
# wording table.
#
# The line deliberately NOT crossed is "new discovery" / "likely new find":
# that stacks two unearned claims -- that the match is CORRECT and that it
# is NEW -- on a row with no human review until Phase 137. A wrong match
# that no catalogue records is not a discovery. "Candidates for new finds"
# asserts only candidacy, which is what the data supports.
#
# The checked-source list names no restricted corpus (D-25): the masked
# member appears only as "shelfmark attributions".
# ---------------------------------------------------------------------------

_NOVELTY_TOGGLE: Dict[str, str] = {
    "en": "Candidates for new finds",
    "he": "מועמדים לממצאים חדשים",
}

_NOVELTY_BADGE: Dict[str, str] = {
    "en": "Candidate new find",
    "he": "מועמד לממצא חדש",
}

_NOVELTY_SUBLINE: Dict[str, str] = {
    "en": "Findings you would not reach by searching the catalogues.",
    "he": "ממצאים שלא הייתם מגיעים אליהם בחיפוש בקטלוגים.",
}

_NOVELTY_HELP: Dict[str, str] = {
    "en": (
        "no finding aid we checked records this work on this fragment; the checked "
        "list is fixed and dated (FJMS + NLI catalogues and bibliography, titles, "
        "PGP, FGP, shelfmark attributions); the identification itself is an "
        "unreviewed algorithmic match, so this is a candidate, not a confirmed find"
    ),
    "he": (
        "שום כלי עזר שבדקנו אינו מתעד את החיבור הזה על הקטע הזה; רשימת המקורות "
        "שנבדקו קבועה ומתוארכת (קטלוגים וביבליוגרפיה של FJMS ושל הספרייה הלאומית, "
        "כותרות, PGP, FGP, ייחוסים לפי סימני מדף); הזיהוי עצמו הוא התאמה אלגוריתמית "
        "שלא נבדקה, ולכן זהו מועמד ולא ממצא מאושר"
    ),
}

_NOVELTY_UNKNOWN_BADGE: Dict[str, str] = {
    "en": "Novelty not checked",
    "he": "החידוש לא נבדק",
}


def novelty_strings(lang: str = "en") -> Dict[str, str]:
    """The four candidacy strings, keyed `toggle` / `badge` / `subline` /
    `help`, verbatim from the ratified wording table.

    A fresh dict is returned each call so a caller cannot mutate the
    module's own tables."""
    key = _lang_key(lang)
    return {
        "toggle": _NOVELTY_TOGGLE[key],
        "badge": _NOVELTY_BADGE[key],
        "subline": _NOVELTY_SUBLINE[key],
        "help": _NOVELTY_HELP[key],
    }


def novelty_unknown_badge(lang: str = "en") -> str:
    """The muted counterpart of the candidacy badge, for a row whose
    novelty is `not_checked` -- the fail-closed default. Nothing is ever
    novel by default: an unavailable source, an unnormalizable id, a stale
    cache or a model abstention all land here, and the reader is told the
    check did not run rather than being shown a verdict that was never
    reached."""
    return _pick(_NOVELTY_UNKNOWN_BADGE, lang)


# ---------------------------------------------------------------------------
# The relation filter's short codes. The stored `claim_type` key is the
# INPUT; the code is the only thing that reaches markup or a query string.
#
# There is deliberately NO code -> stored-key function. The reverse
# direction returns what a READER sees, and a filtered query uses
# `matches_filter_codes`, so no caller ever needs to turn a code back into
# an internal classification -- which is what keeps the grep-based
# "no stored vocabulary key in markup" invariant clean in BOTH directions.
# ---------------------------------------------------------------------------

_FILTER_CODE_BY_RELATION: Dict[str, str] = {
    ids.CLAIM_TYPE_DIRECT_WITNESS: "dw",
    ids.CLAIM_TYPE_QUOTES_THIS_WORK: "qw",
    ids.CLAIM_TYPE_SHARED_TEXT: "st",
}

_FILTER_CODES: Tuple[str, ...] = ("dw", "qw", "st")


def filter_codes() -> Tuple[str, ...]:
    """Every relation filter code, in display order."""
    return _FILTER_CODES


def filter_code(relation_kind: str) -> str:
    """The short code for a stored relation kind. Raises on an unknown kind
    rather than emitting a code that selects nothing."""
    code = _FILTER_CODE_BY_RELATION.get(relation_kind)
    if code is None:
        raise ValueError(
            "filter_code: unknown relation kind {!r} (expected one of {})".format(
                relation_kind, sorted(_FILTER_CODE_BY_RELATION)
            )
        )
    return code


def is_filter_code(code: str) -> bool:
    """Whether `code` is one of the three relation filter codes. A stored
    vocabulary key arriving here (e.g. from a hand-edited query string) is
    False, not silently accepted."""
    return code in _FILTER_CODES


def filter_label(code: str, lang: str = "en") -> str:
    """The reverse direction: a short code -> the label a reader sees for
    it. Never the stored key."""
    for relation_kind, candidate in _FILTER_CODE_BY_RELATION.items():
        if candidate == code:
            return relation_chip(relation_kind, lang)
    raise ValueError(
        "filter_label: unknown filter code {!r} (expected one of {})".format(
            code, list(_FILTER_CODES)
        )
    )


def matches_filter_codes(relation_kind: str, codes: Iterable[str]) -> bool:
    """Whether a row of `relation_kind` survives a relation filter holding
    `codes`. An EMPTY selection means ALL (the phase-wide convention:
    filters compose as AND, empty set = all), so a reader who has selected
    nothing is never shown nothing."""
    selected = tuple(codes or ())
    if not selected:
        return True
    return filter_code(relation_kind) in selected


# ---------------------------------------------------------------------------
# Text-vs-text excerpt disclosure (excerpt-v1, 2026-08-13). Claim vocabulary
# for the findings row's "Compare texts" panes. Match-framing register only
# (same ruling as the relation chips); the work pane's absence copy names an
# availability fact and never hints at WHY an edition is undisplayable.
# ---------------------------------------------------------------------------

_EXCERPT_STRINGS: Dict[str, Dict[str, str]] = {
    "toggle": {"en": "View text match", "he": "הצגת התאמת הטקסט"},
    "frag_label": {"en": "Manuscript passage", "he": "קטע כתב היד"},
    # Appended to frag_label for the automated layer only (text_layer='htr');
    # FGP/PGP transcriptions are human work and carry no such qualifier.
    "frag_htr_note": {"en": "automated transcription", "he": "תעתיק אוטומטי"},
    "work_label": {"en": "Edition passage", "he": "קטע המהדורה"},
    "work_unavailable": {
        "en": "The reference edition's text is not available for display.",
        "he": "טקסט מהדורת הייחוס אינו זמין להצגה.",
    },
    # The row exists but carries no excerpt (identifications whose evidence
    # is not excerpt-eligible). An honest empty, not an outage.
    "none": {
        "en": "No text excerpts are available for this identification.",
        "he": "אין קטעי טקסט זמינים לזיהוי זה.",
    },
    # work_source='reprojected': the passage was LOCATED in this (public)
    # edition by alignment, rather than read off stored offsets into it.
    "reprojected_note": {
        "en": "Passage located in this edition by automated alignment.",
        "he": "הקטע אותר במהדורה זו בהתאמה אוטומטית.",
    },
    "multi_span": {
        "en": "The strongest of {count} matching passages.",
        "he": "הקטע המובהק מתוך {count} קטעים תואמים.",
    },
    "failed": {
        "en": "The excerpt could not be loaded.",
        "he": "לא ניתן היה לטעון את הקטעים.",
    },
}


def excerpt_strings(lang: str = "en") -> Dict[str, str]:
    """Every excerpt-disclosure string, resolved for one language."""
    return {key: _pick(entry, lang) for key, entry in _EXCERPT_STRINGS.items()}


# ---------------------------------------------------------------------------
# Service-state copy (D-13/D-14 envelope). An outage is a visible temporary
# state with a retry -- never an empty section, which is indistinguishable
# from "this manuscript has no computed identifications" and would quietly
# under-report the corpus.
#
# NOTE: the PAGE-level availability card is page chrome and lives in
# `tr()`; these strings are the PANEL/section-level copy. They are
# deliberately different strings, and a test asserts no string is defined
# in both places.
# ---------------------------------------------------------------------------

_SERVICE_STATE_COPY: Dict[str, Dict[str, str]] = {
    "ok": {"en": "", "he": ""},
    "unavailable": {
        "en": "Computed identifications are temporarily unavailable.",
        "he": "הזיהויים המחושבים אינם זמינים באופן זמני.",
    },
    "timeout": {
        "en": "This took longer than expected.",
        "he": "הפעולה ארכה יותר מהצפוי.",
    },
    "busy": {
        "en": "The service is busy right now.",
        "he": "השירות עמוס כרגע.",
    },
}

_RETRY_LABEL: Dict[str, str] = {
    "en": "Try again",
    "he": "נסו שוב",
}


def service_state_message(state: str, lang: str = "en") -> str:
    """The reader-facing message for one envelope status. `ok` returns an
    empty string (there is nothing to say); every other status returns copy
    that names a TEMPORARY condition, so an outage never reads as an
    authoritative zero. Raises on an unknown status rather than rendering
    silence for a state nobody designed."""
    entry = _SERVICE_STATE_COPY.get(state)
    if entry is None:
        raise ValueError(
            "service_state_message: unknown service state {!r} (expected one of {})".format(
                state, sorted(_SERVICE_STATE_COPY)
            )
        )
    return _pick(entry, lang)


def retry_label(lang: str = "en") -> str:
    """The retry affordance that accompanies every non-ok service state."""
    return _pick(_RETRY_LABEL, lang)


# ---------------------------------------------------------------------------
# Curated display titles (owner ruling R, 2026-08-03)
#
# A DISPLAY-time override, deliberately not a data change. The reference text a
# row matched against really is the named work; what is wrong is the impression
# the bare title gives a reader. Rewriting `works.neutral_title` would falsify
# what the matcher actually compared and would silently change the novelty
# funnel's own name-matching if it were ever re-run, so the correction lives
# here, at the surface, where the misleading impression is.
#
# First entry, and the reason the mechanism exists: `משנה תורה, ספר אהבה` is the
# 10th most-claimed work in the corpus at 6,437 claim pairs (448 of them on the
# candidates surface), because Sefer Ahava CONTAINS the order of prayers -- so
# liturgy is drawn to it. The owner's ruling: most of these pages are liturgy,
# and the honest label names both possibilities rather than asserting the
# halakhic work or hiding the row (ruling N already settled that these rows
# SHIP: an imprecise claim that points a reader at "this is prayer" beats the
# silence these fragments otherwise have).
#
# This is the first instance of the deferred ~2,670-work title curation. Keep
# entries rare and owner-ruled; this is not a place to fix data quality.
# ---------------------------------------------------------------------------

CURATED_WORK_TITLES: Dict[str, Dict[str, str]] = {
    "w000176": {
        "he": "משנה תורה, ספר אהבה / סידור",
        "en": "Mishneh Torah, Sefer Ahava / Siddur",
    },
}


def display_work_title(work_id: str, neutral_title: str, lang: str = "en") -> str:
    """The title a READER sees for a work, applying any owner-ruled curated
    override and otherwise passing the recorded title through unchanged.

    Every surface that renders a work title must route through this -- a
    surface that formats `neutral_title` directly silently opts out of the
    curation and will show the misleading bare title.
    """
    entry = CURATED_WORK_TITLES.get(work_id)
    if entry is None:
        return neutral_title
    return _pick(entry, lang)
