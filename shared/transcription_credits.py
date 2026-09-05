# -*- coding: utf-8 -*-
"""Who made the transcription that is on screen — and how to credit them.

Written 2026-09-04 for the printed reading sheet (`/browse` → Print / Save as
PDF). The sheet shipped crediting MiDRASH unconditionally, which is wrong for
every page showing an FGP edition, a PGP edition, a translation, or a reader's
own correction: it credits the wrong people for someone else's scholarship.

WHY A SHARED MODULE AND NOT A HELPER IN `browse.py`
---------------------------------------------------
The same defect exists in the Word export (`web/export_service.export_browse_word`
always prints `CREDITS_TEXT`, i.e. MiDRASH, in English, whatever is on screen).
Putting the mapping here means the fix is one function both surfaces can call,
rather than the fifth hand-copy of a citation — which is exactly the failure
`shared/export_utils` was created to stop.

It deliberately imports only `shared.export_utils` (stdlib-only) and
`shared.fgp_service` constants. It must stay cheap enough for any surface to
import: no database connections, no NiceGUI, no web.translations.

WHY IT TAKES `lang` RATHER THAN CALLING tr()
--------------------------------------------
`shared/` modules do not reach into `web.translations` — `web/translations.py`
keeps the current language in a module global, and the export layer's
established pattern (`export_dossier.credits_lines`, `search_meta_labels`) is an
explicit `lang` argument with per-language dicts. This follows that.

WHAT "BILINGUAL" CAN AND CANNOT MEAN HERE
-----------------------------------------
The FRAME is bilingual. The CITATIONS are not, and must not be:

* The MiDRASH citation is documented in `shared/export_utils` as never
  translated on a Hebrew workbook or an English one — "a published citation is
  cited as published", and the DOI, dataset URL and authors' names must survive
  intact. The same rule applies on paper.
* PGP scholar attributions are English-only free text in `pgp.db`
  (`documents.transcription_source` / `document_sources.source_scholar`), e.g.
  "S. D. Goitein, unpublished editions. (T-S 8J)". There is no Hebrew column and
  no translation path anywhere in the repo. A Hebrew sheet therefore gets a
  Hebrew label around an English citation — which is correct, not a shortfall.
* FGP is the one provider with a genuine per-source bilingual credit
  (`source_credit_he` / `source_credit_en`), already picked by
  `shared.fgp_service.pick_fgp_credit`. We consume its result rather than
  re-implementing the choice.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from shared.export_utils import (
    GENIZAHSEARCH_URL,
    MIDRASH_CREDIT_LINES,
    MIDRASH_INLINE_CITATION,
    site_name,
)
from shared.fgp_service import FGP_ATTRIBUTION


# ---------------------------------------------------------------------------
# Source kinds
# ---------------------------------------------------------------------------
#
# `version_info['source']` takes SEVEN distinct literal values across
# `web/components/version_selector.py`. Two of them mean the same thing, which
# is the trap this constant exists to close:
#
#   'original'  — the plain V0.8 text, auto-selected when a `must_contain`
#                 search phrase forced it (version_selector.py, the must_contain
#                 branch).
#   'V0.8'      — the plain V0.8 text, chosen explicitly from the menu.
#   'V0.7'      — the older automatic transcription. Reachable through
#                 `handle_version_change`'s own branch in browse.py.
#
# All three are the automatic HTR transcription, so all three are MiDRASH. A
# `== 'V0.8'` test — the obvious thing to write — silently mis-credits the
# must_contain case, which is precisely the path a reader arriving from a search
# hit takes.
#
# `None` is included because the print sheet renders BEFORE any source decision
# exists, and because on a page with no alternative sources at all
# `on_version_change` is never called even once. The un-set state is the plain
# HTR text, so it must resolve to the MiDRASH credit rather than to nothing.
HTR_SOURCES = (None, '', 'original', 'V0.7', 'V0.8')

KIND_HTR = 'htr'
KIND_FGP = 'fgp'
KIND_PGP = 'pgp'
KIND_TRANSLATION = 'translation'
KIND_USER = 'user'
KIND_PENDING = 'pending'


# ---------------------------------------------------------------------------
# Localized frame
# ---------------------------------------------------------------------------

# Headings are whole localized TEMPLATES, not a shared label glued to a value
# with a colon. Composition read fine in English and produced nonsense in
# Hebrew: "תעתוק" + ": " + "תעתוק אוטומטי" is "transcription: transcription
# automatic", and "{language} {Translation}" inverts to "עברית תרגום" where
# Hebrew wants "תרגום לעברית". Each language owns its own word order.
_LABELS_EN: Dict[str, str] = {
    'htr_heading': "Transcription: automatic (MiDRASH)",
    'fgp_heading': "Transcription: Friedberg Genizah Project",
    'pgp_heading': "Transcription: Princeton Geniza Project",
    'translation_heading': "Translation",
    'translation_heading_lang': "Translation into {language}",
    'community_correction': "Community correction by {author}",
    'community_correction_anon': "Community correction",
    'pending_correction': "Unapproved community correction",
    'based_on': "Based on the automatic transcription:",
    'cite': "When publishing material from this site, please cite:",
    'how_to_cite': "How to cite this page",
    'how_to_cite_site': "How to cite this site",
    'using': "using",
    # SENTENCE forms of the provider names. The `*_heading` values above are
    # standalone LABELS for the print sheet and carry a colon ("Transcription:
    # Princeton Geniza Project"); dropped into prose that produced
    # "using Transcription: Princeton Geniza Project, S. D. Goitein..." --
    # a colon mid-sentence. Same facts, different grammar.
    'fgp_inline': "the Friedberg Genizah Project transcription",
    'pgp_inline': "the Princeton Geniza Project edition",
    'translation_inline': "the translation",
    'translation_inline_lang': "the {language} translation",
    'folio': "folio",
    'page_word': "page",
    'community_correction_of': "a community correction by {author}, based on",
    'anon_correction_of': "a community correction, based on",
    'pending_correction_of': "an unapproved community correction, based on",
    'retrieved_on': "Retrieved {date}",
    # Lowercase, for the parenthetical form inside the access line -- English
    # capitalises mid-sentence otherwise. Hebrew has no case, hence one key
    # there doing both jobs.
    'retrieved_on_inline': "retrieved {date}",
    'source_url': "Available at {url}",
    'site_line': "Retrieved from {site} — {url}",
}

_LABELS_HE: Dict[str, str] = {
    'htr_heading': "תעתוק אוטומטי (MiDRASH)",
    'fgp_heading': "תעתוק: פרויקט הגניזה של פרידברג",
    'pgp_heading': "תעתוק: פרויקט הגניזה של פרינסטון",
    'translation_heading': "תרגום",
    'translation_heading_lang': "תרגום ל{language}",
    'community_correction': "תיקון קהילתי מאת {author}",
    'community_correction_anon': "תיקון קהילתי",
    'pending_correction': "תיקון קהילתי שטרם אושר",
    'based_on': "מבוסס על התעתוק האוטומטי:",
    'cite': "בכל פרסום של החומר המוצג כאן, אנא צטטו את:",
    'how_to_cite': "כיצד לצטט דף זה",
    'how_to_cite_site': "כיצד לצטט את האתר",
    # "על בסיס" (owner, 2026-09-04), not "על פי". The clause names the
    # transcription a page RESTS ON, which is a statement about provenance;
    # "על פי" reads as conformity to an authority, which is a different claim.
    'using': "על בסיס",
    'fgp_inline': "תעתוק פרויקט הגניזה של פרידברג",
    'pgp_inline': "מהדורת פרויקט הגניזה של פרינסטון",
    'translation_inline': "התרגום",
    'translation_inline_lang': "התרגום ל{language}",
    'folio': "דף",
    'page_word': "עמוד",
    # "לתעתוק", not "המבוסס על": the connector above is now "על בסיס", and
    # the two together gave "על בסיס תיקון קהילתי ... המבוסס על ..." -- the
    # same root twice in one clause. "לתעתוק" says what a correction is a
    # correction OF, which is the fact that matters anyway.
    'community_correction_of': "תיקון קהילתי מאת {author} לתעתוק",
    'anon_correction_of': "תיקון קהילתי לתעתוק",
    'pending_correction_of': "תיקון קהילתי שטרם אושר לתעתוק",
    'retrieved_on': "נצפה בתאריך {date}",
    'retrieved_on_inline': "נצפה בתאריך {date}",
    'source_url': "זמין בכתובת {url}",
    # 'site_name(he)' is אתר..., so 'נצפה ב' + it reads 'נצפה באתר ...' correctly.
    'site_line': "נצפה ב{site} — {url}",
}

#: Translation-language names that `version_selector` passes through raw. It
#: builds them as hardcoded English grouping labels ('Hebrew', 'English') or the
#: source row's own language field, and never localizes them — the on-screen
#: notification has the same gap. Kept small and explicit rather than reaching
#: for the 5,000-entry web translations table, which `shared/` must not import.
_LANGUAGE_NAMES_HE: Dict[str, str] = {
    'Hebrew': "עברית",
    'English': "אנגלית",
    'Arabic': "ערבית",
    'Judeo-Arabic': "ערבית-יהודית",
    'Aramaic': "ארמית",
}


def _labels(lang: str) -> Dict[str, str]:
    return _LABELS_HE if str(lang or '').lower().startswith('he') else _LABELS_EN


def _language_name(name: str, lang: str) -> str:
    if not name:
        return ''
    if str(lang or '').lower().startswith('he'):
        return _LANGUAGE_NAMES_HE.get(name, name)
    return name


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TranscriptionCredit:
    """A rendered-surface-agnostic credit block.

    `heading` is one short localized line naming WHO made this text.
    `citation_lines` are the formal citation rows, if the source has any — for
    MiDRASH these are the canonical English rows and must not be reflowed or
    translated. `site_lines` credit this site and always appear.

    Surfaces render these; they do not decide their content. That is the whole
    point of putting the decision here.
    """

    kind: str
    heading: str
    citation_lines: List[str] = field(default_factory=list)
    site_lines: List[str] = field(default_factory=list)
    #: True when `citation_lines` carries the MiDRASH citation. Lets a caller
    #: assert the owner's rule ("MiDRASH only for HTR") without string-matching.
    credits_midrash: bool = False

    def all_lines(self) -> List[str]:
        """Every line, in print order — heading, citation, then the site."""
        return [self.heading] + list(self.citation_lines) + list(self.site_lines)


def _site_lines(lang: str) -> List[str]:
    """The site's own credit, on every sheet whatever the transcription source.

    A whole template per language rather than 'label: value': Hebrew's
    ``site_name`` already begins with אתר, so gluing a colon onto a
    generic label produced "נצפה באתר: אתר הגניזה..." — "viewed on site:
    site of the genizah".
    """
    labels = _labels(lang)
    return [labels['site_line'].format(site=site_name(lang), url=GENIZAHSEARCH_URL)]


def resolve_transcription_credit(
    version_info: Optional[Dict[str, Any]],
    *,
    lang: str = 'en',
) -> TranscriptionCredit:
    """Credit the transcription described by ``version_info``.

    ``version_info`` is the dict `create_version_selector` hands to its
    `on_version_change` callback, or ``None`` when no selection has been made —
    which is both the initial render and the (common) case of a manuscript with
    no alternative sources at all. ``None`` means the plain HTR text.

    Unknown `source` values fall back to the MiDRASH/HTR credit rather than to
    an empty block: the displayed text on an unrecognised path is still, in
    every code path that exists today, the automatic transcription, and a
    missing attribution is a licence problem where a redundant one is not.
    """
    info = version_info or {}
    source = info.get('source')
    labels = _labels(lang)

    if source not in HTR_SOURCES and source not in (
        KIND_FGP, KIND_PGP, KIND_TRANSLATION, KIND_USER, KIND_PENDING,
    ):
        source = None  # unknown → treat as the plain HTR text (see docstring)

    # --- The automatic transcription: the ONE case MiDRASH is owed. ---------
    if source in HTR_SOURCES:
        return TranscriptionCredit(
            kind=KIND_HTR,
            heading=labels['htr_heading'],
            citation_lines=list(MIDRASH_CREDIT_LINES),
            site_lines=_site_lines(lang),
            credits_midrash=True,
        )

    # --- FGP: the one provider with a real bilingual per-source credit. -----
    if source == KIND_FGP:
        # An FGP TRANSLATION is not a transcription. `version_selector` sends
        # `source='fgp'` with `is_translation=True` for those, and ignoring the
        # flag made the sheet, the chip and the .docx all call translated text
        # "Transcription: Friedberg Genizah Project" -- a false statement about
        # what the reader is looking at, separate from who made it.
        #
        # Credited to FGP either way; only the KIND and the heading change, so
        # the translation heading is used and the language named when known.
        if info.get('is_translation'):
            credit = (info.get('source_credit')
                      or info.get('attribution')
                      or FGP_ATTRIBUTION)
            language = _language_name(str(info.get('language') or ''), lang)
            heading = (labels['translation_heading_lang'].format(language=language)
                       if language else labels['translation_heading'])
            return TranscriptionCredit(
                kind=KIND_TRANSLATION,
                heading=heading,
                citation_lines=[str(credit)],
                site_lines=_site_lines(lang),
            )

        # `source_credit` is already language-picked by
        # `shared.fgp_service.pick_fgp_credit(src, get_language())` at the
        # version_selector call site. Re-picking here would need the raw source
        # row, which the callback does not carry.
        credit = (info.get('source_credit')
                  or info.get('attribution')
                  or FGP_ATTRIBUTION)
        return TranscriptionCredit(
            kind=KIND_FGP,
            heading=labels['fgp_heading'],
            citation_lines=[str(credit)],
            site_lines=_site_lines(lang),
        )

    # --- PGP: an English scholarly citation under a localized label. --------
    if source == KIND_PGP:
        attribution = str(info.get('attribution') or '').strip()
        lines = []
        if attribution and attribution.upper() != 'PGP':
            lines.append(attribution)
        url = info.get('pgp_url')
        if url:
            lines.append(labels['source_url'].format(url=url))
        return TranscriptionCredit(
            kind=KIND_PGP,
            heading=labels['pgp_heading'],
            citation_lines=lines,
            site_lines=_site_lines(lang),
        )

    # --- A translation: its own translator, never the transcriber. ----------
    if source == KIND_TRANSLATION:
        language = _language_name(str(info.get('language') or ''), lang)
        heading = (labels['translation_heading_lang'].format(language=language)
                   if language else labels['translation_heading'])
        attribution = str(info.get('attribution') or '').strip()
        lines = [attribution] if attribution else []
        url = info.get('pgp_url')
        if url:
            lines.append(labels['source_url'].format(url=url))
        return TranscriptionCredit(
            kind=KIND_TRANSLATION,
            heading=heading,
            citation_lines=lines,
            site_lines=_site_lines(lang),
        )

    # --- A reader's correction: THE CORRECTOR **AND** MiDRASH. --------------
    #
    # Deliberate, and the one place this module goes beyond "MiDRASH only for
    # HTR" as literally stated. A community correction is an edit OF the
    # automatic transcription — `web/supabase_client.py` stores the HTR as the
    # correction's `original_text` — so the printed text is a DERIVATIVE of a
    # CC-BY-4.0 work, and CC-BY attribution survives modification. Dropping
    # MiDRASH here would credit a proof-reader for a machine's reading and
    # breach the licence the /help page states.
    #
    # `author` is a display name (full_name or username), never an email or a
    # uuid: `web/supabase_client.get_corrections` selects only id/full_name/
    # username from `profiles`. It is the same name already shown on screen, so
    # printing it discloses nothing new.
    if source in (KIND_USER, KIND_PENDING):
        author = str(info.get('author') or '').strip()
        if source == KIND_PENDING:
            heading = labels['pending_correction']
        elif author:
            heading = labels['community_correction'].format(author=author)
        else:
            heading = labels['community_correction_anon']
        return TranscriptionCredit(
            kind=source,
            heading=heading,
            citation_lines=[labels['based_on']] + list(MIDRASH_CREDIT_LINES),
            site_lines=_site_lines(lang),
            credits_midrash=True,
        )

    raise AssertionError('unreachable: source %r escaped every branch' % (source,))


# ---------------------------------------------------------------------------
# "How to cite this page"
# ---------------------------------------------------------------------------
#
# Added 2026-09-04, when the owner replaced the sticky citation FOOTER with a
# small always-visible chip. The chip is the reason this needs its own function
# rather than reusing `TranscriptionCredit` directly: a citation a reader copies
# has to name the page they are on and WHEN they saw it, which a transcription
# credit does not.
#
# The Word export takes the same block, so an exported document and a copied
# citation say the same thing. That is the point of it living here.


# ---------------------------------------------------------------------------
# The retrieval date
# ---------------------------------------------------------------------------
#
# "Sept. 4, 2026" / "4 בספטמבר, 2026" (owner, 2026-09-04), replacing an ISO
# date. ISO was chosen because no Hebrew month table existed in this repo -- a
# codebase map confirmed that: the one hit anywhere was a single hand-typed
# `"February 2025": "פברואר 2025"` entry for one hardcoded string. So this
# creates the table, which is the actual cost the ISO choice was avoiding.
#
# The English side follows AP style, which is what the owner's own example
# ("Sept. 4, 2026") is: months of five letters or fewer are spelled out, the
# rest abbreviated with a period. Abbreviating all twelve would have given
# "Jun. 4" and "Jul. 4", which no American style guide writes.
#
# The Hebrew keeps the owner's comma before the year ("4 בספטמבר, 2026").
# Unpunctuated ("4 בספטמבר 2026") is the more usual Hebrew form; the comma is
# deliberate here so the two languages read the same way, and it was written
# out explicitly alongside the English.
_MONTHS_EN = (
    'Jan.', 'Feb.', 'March', 'April', 'May', 'June',
    'July', 'Aug.', 'Sept.', 'Oct.', 'Nov.', 'Dec.',
)

_MONTHS_HE = (
    'בינואר', 'בפברואר', 'במרץ', 'באפריל', 'במאי', 'ביוני',
    'ביולי', 'באוגוסט', 'בספטמבר', 'באוקטובר', 'בנובמבר', 'בדצמבר',
)


def _format_retrieved(retrieved_on, lang: str) -> str:
    """The retrieval date, written for a reader of ``lang``.

    Accepts a `date`, a `datetime`, or a string.

    An ISO ``YYYY-MM-DD`` string is PARSED and reformatted rather than passed
    through, because the browse Word export has to send this date through
    NiceGUI's session storage to a separate FastAPI route -- and that storage is
    JSON-backed, so a `date` object cannot survive the trip. ISO is how it
    travels; it is not how it is displayed. Any other string is handed back
    untouched, for a caller that already has a display date of its own.
    """
    if retrieved_on is None:
        return ''
    if isinstance(retrieved_on, str):
        try:
            retrieved_on = _dt.date.fromisoformat(retrieved_on.strip())
        except ValueError:
            return retrieved_on          # already a display string; leave it
    try:
        year, month, day = retrieved_on.year, retrieved_on.month, retrieved_on.day
    except AttributeError:
        return str(retrieved_on)
    if str(lang or '').lower().startswith('he'):
        return '%d %s, %d' % (day, _MONTHS_HE[month - 1], year)
    return '%s %d, %d' % (_MONTHS_EN[month - 1], day, year)


@dataclass(frozen=True)
class PageCitation:
    """One citation sentence a reader can paste into a bibliography.

    `heading` is the chip's own label; `text` is the citation. Deliberately a
    SENTENCE and not a list of lines: the first cut of this returned a block of
    six rows, and the owner's verdict was "too much duplicacy" -- the domain
    appeared twice, the word "retrieved" twice, and seventeen author names sat
    between the shelfmark and the URL. A citation is read and pasted as prose.

    The shape follows the owner's own model (2026-09-04):

        Cambridge University Library, T-S Ar.50.74, folio 1r. Dicta Genizah
        Search, https://genizahsearch.com/browse?... (retrieved 2026-09-04),
        using MiDRASH Automatic Transcriptions, Stoekl Ben Ezra, D. et al.,
        2025, Zenodo. https://doi.org/10.5281/zenodo.17734473.

    Three clauses: WHICH manuscript, WHERE it was read, WHOSE transcription.
    Any clause with nothing to say is dropped rather than left empty.
    """

    heading: str
    text: str = ''
    credits_midrash: bool = False
    kind: str = ''

    def as_text(self) -> str:
        """The citation, for a clipboard button."""
        return self.text


def _source_clause(version_info, lang: str) -> str:
    """The "using ..." clause: whose transcription this page is showing.

    Built from the SAME `resolve_transcription_credit` decision the printed
    sheet uses, so the chip and the print masthead can never disagree about who
    made the text. Only the rendering differs -- rows there, a clause here.
    """
    labels = _labels(lang)
    credit = resolve_transcription_credit(version_info, lang=lang)
    info = version_info or {}

    if credit.kind == KIND_HTR:
        return MIDRASH_INLINE_CITATION

    if credit.kind in (KIND_USER, KIND_PENDING):
        # A correction is an edit OF the automatic transcription, so the clause
        # names the corrector AND the work they corrected -- see the licence
        # reasoning on the `resolve_transcription_credit` user branch.
        author = str(info.get('author') or '').strip()
        if credit.kind == KIND_PENDING:
            lead = labels['pending_correction_of']
        elif author:
            lead = labels['community_correction_of'].format(author=author)
        else:
            lead = labels['anon_correction_of']
        return '%s %s' % (lead, MIDRASH_INLINE_CITATION)

    # FGP / PGP / a translation: the project or kind in SENTENCE form, then its
    # own credit. Not `credit.heading` -- that is the print sheet's standalone
    # label and carries a colon, which mid-sentence produced
    # "using Transcription: Princeton Geniza Project, S. D. Goitein...".
    if credit.kind == KIND_FGP:
        lead = labels['fgp_inline']
    elif credit.kind == KIND_PGP:
        lead = labels['pgp_inline']
    else:
        language = _language_name(str(info.get('language') or ''), lang)
        lead = (labels['translation_inline_lang'].format(language=language)
                if language else labels['translation_inline'])

    parts = [lead]
    for line in credit.citation_lines:
        if line and line not in lead:
            parts.append(line)
    return ', '.join(parts)


def _manuscript_clause(lang: str, library, shelfmark, folio) -> str:
    """"Cambridge University Library, T-S Ar.50.74, folio 1r" -- as much of it
    as the caller actually knows.

    Empty on a page with no manuscript (the homepage, /help), which is why the
    site clause below has to stand on its own.
    """
    labels = _labels(lang)
    parts = [str(p).strip() for p in (library, shelfmark) if p and str(p).strip()]
    if folio and str(folio).strip():
        parts.append('%s %s' % (labels['folio'], str(folio).strip()))
    return ', '.join(parts)


def page_citation(
    version_info: Optional[Dict[str, Any]] = None,
    *,
    lang: str = 'en',
    library: Optional[str] = None,
    shelfmark: Optional[str] = None,
    folio: Optional[str] = None,
    page_url: Optional[str] = None,
    retrieved_on=None,
) -> PageCitation:
    """The "how to cite this page" sentence.

    The owner's rule holds here as everywhere: MiDRASH is cited for the
    automatic transcription -- and by default, since `version_info=None` means
    the plain HTR text -- while someone else's edition, translation or
    correction is credited to them instead.

    This module never reads the clock: the surface passes `retrieved_on`. Not
    fastidiousness -- an accessed-date is a property of the READER's visit, so
    only the request that served them knows it, and a pure function is testable.
    """
    labels = _labels(lang)
    clauses = []

    ms = _manuscript_clause(lang, library, shelfmark, folio)
    if ms:
        clauses.append(ms)

    # WHERE it was read. The page URL already carries the domain, so the site
    # name is not repeated as a bare address beside it.
    site = site_name(lang)
    where = '%s, %s' % (site, page_url) if page_url else site
    stamp = _format_retrieved(retrieved_on, lang)
    if stamp:
        where = '%s (%s)' % (where, labels['retrieved_on_inline'].format(date=stamp))
    clauses.append(where)

    text = '. '.join(clauses)
    source = _source_clause(version_info, lang)
    if source:
        text = '%s, %s %s' % (text, labels['using'], source)
    if not text.endswith('.'):
        text += '.'

    credit = resolve_transcription_credit(version_info, lang=lang)
    return PageCitation(
        heading=labels['how_to_cite'],
        text=text,
        credits_midrash=credit.credits_midrash,
        kind=credit.kind,
    )


def site_citation(*, lang: str = 'en', retrieved_on=None) -> PageCitation:
    """How to cite the SITE as a whole -- the regular chip on every page.

    Owner, 2026-09-04: "most people will want to cite the website usage as a
    whole (they've found many things there), and the regular chip will just
    mention the dicta genizah search (and site address) and zenodo citation."

    So this is the DEFAULT the chip shows. A reader who used the site across
    many manuscripts has nothing to gain from a citation pinned to whichever
    folio happened to be on screen last, and would have to edit it back out.

    It is deliberately `page_citation` with no manuscript rather than a second
    string built by hand: one assembler means the site form and the folio form
    cannot drift in wording, punctuation or language. The only differences are
    that there is no manuscript clause, the URL is the site root, and the
    heading says "site" rather than "page".
    """
    citation = page_citation(
        None, lang=lang, page_url=GENIZAHSEARCH_URL, retrieved_on=retrieved_on)
    return PageCitation(
        heading=_labels(lang)['how_to_cite_site'],
        text=citation.text,
        credits_midrash=citation.credits_midrash,
        kind=citation.kind,
    )
