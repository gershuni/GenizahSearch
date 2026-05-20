# -*- coding: utf-8 -*-
"""Shared dossier helpers for research-grade xlsx export (Phase 94).

Both the web (`web/export_service.py`) and desktop (`genizah_app.py`) export
pipelines consume this module to emit the new `Manuscripts` and `Bibliography`
sub-sheets. Behavior is identical across both apps so the workbook structure
is uniform.

Public API
----------

Four lookup helpers (each exception-resilient: wraps the service call in
try/except, logs a warning via the module logger, returns ``None`` or ``[]``):

- :func:`pgp_subset_for_sys_id` — narrow PGP projection (6 keys; never emits
  the full transcription field — see the strict prohibition below).
- :func:`nli_subset_for_sys_id` — narrow NLI projection (2 keys: ``catalog_entry``
  + ``library_viewer_url``). Column header is **NLI Catalog Entry** (NOT
  "NLI Description"); the underlying ``get_catalog_entry()`` returns
  Neubauer–Cowley reference strings, not descriptions (Codex MUST-FIX 2).
- :func:`catalog_summary_for_sys_id` — narrow FJMS catalog projection (4 fields:
  ``title``, ``author_text``, ``copy_date``, ``copy_place``). Uses the narrow
  ``FjmsService.get_catalog_records()`` query; the detail variant (which loads
  ``full_texts``) is intentionally not invoked, since it would break the
  strict prohibition below — Codex MUST-FIX 3.
- :func:`bibliography_for_sys_id` — list of FJMS bib entries with the REAL
  service field names (``running_title``, ``title_year``, ``mention_page``,
  ``article_name``, ``article_author_eng``, ``catalog_acronym``); the
  superseded plan invented field names that don't exist (Codex MUST-FIX 1).

Two row-emitters (return Python primitives only — no openpyxl objects):

- :func:`build_manuscript_row` — assembles one Manuscripts sub-sheet row by
  calling the PGP + NLI + Catalog helpers (3 helpers, NOT 4 — the bibliography
  helper is intentionally NOT called from here; Codex MUST-FIX 4).
- :func:`build_bibliography_rows` — assembles 0..N Bibliography sub-sheet rows
  by calling only :func:`bibliography_for_sys_id`.

Two header constants (length-pinned by tests so row/header drift is caught):

- :data:`MANUSCRIPT_HEADERS` (14 columns).
- :data:`BIBLIOGRAPHY_HEADERS` (8 columns).

D-02 strict prohibition (no transcription text in NEW dossier surfaces)
----------------------------------------------------------------------

The Manuscripts sub-sheet, Bibliography sub-sheet, and JSON envelope
additions (``has_pgp`` / ``is_printed`` / ``domains``) MUST NOT contain
PGP transcription text or FJMS full-text content. The helpers in this
module project a small whitelist of fields per source; the upstream
service dicts may contain additional content fields that this module
deliberately drops. Specifically the prohibited fields by name are
``page_section_text``, ``transcription``, ``full_text``, and ``full_texts``
(naming them here for clarity does not violate the rule — what matters is
that the helper output dicts never contain those keys; see
``tests/test_export_dossier.py::TestPgpSubset::test_no_transcription_text_leak``
and ``TestBibliography::test_no_transcription_leak``).

The pre-existing main-sheet ``Full Text`` column is GRANDFATHERED per
CONTEXT D-02 amendment — that's downstream callers' concern, not this
module's.

D-04 contract (English-only metadata content)
---------------------------------------------

Row content is always English regardless of any ``lang`` parameter passed to
the row builders. ``lang`` on :func:`build_manuscript_row` exists ONLY for
the CALLER's downstream sheet-view direction decision (Hebrew UI → RTL on
the workbook sheet); this module never translates content. Library name
resolution comes from the caller-supplied ``meta_resolver`` callable, which
is expected to call ``genizah_core.get_library_display(code, short=False,
lang='en')`` to hard-pin English (Codex SHOULD-FIX 9).

MUST-FIX 94-01-A — module-scope factory imports
-----------------------------------------------

The 3 service factory functions are imported at MODULE SCOPE (top of file)
so test monkeypatches at ``'shared.export_dossier.<name>'`` actually
intercept the lookup. Lazy in-function imports would let calls bypass the
patched name. See ``tests/test_export_dossier.py`` for the canonical
fixture pattern.

Codex MUST-FIX disposition
--------------------------

1. **Real FJMS bib field names** — :func:`bibliography_for_sys_id` projects
   {running_title, title_year, mention_page, article_name, article_author_eng,
   catalog_acronym}. NEVER {Author, Publisher, Source Name}.
2. **NLI Catalog Entry, not Description** — :func:`nli_subset_for_sys_id`
   uses :meth:`NliCrossrefService.get_catalog_entry` (returns Neubauer-Cowley
   strings).
3. **Narrow catalog query only** — :func:`catalog_summary_for_sys_id` uses
   the narrow ``get_catalog_records`` query; the detail variant (which reads
   ``full_texts``) is intentionally not invoked, since the dossier surfaces
   would otherwise breach D-02.
4. **build_manuscript_row calls 3 helpers, NOT 4** — bibliography rows live
   on a separate sub-sheet built by :func:`build_bibliography_rows`.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

# MUST-FIX 94-01-A: hoist factory functions to module scope so test
# monkeypatches at 'shared.export_dossier.<name>' targets actually
# intercept. Lazy in-function imports break the patches.
from shared.document_service import get_document_for_fragment
from shared.fjms_service import get_fjms_service
from shared.nli_crossref_service import get_nli_crossref_service

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level header constants (Codex SHOULD-FIX 7)
# ---------------------------------------------------------------------------

MANUSCRIPT_HEADERS: List[str] = [
    "System ID",
    "Shelfmark",
    "Library",
    "Title",
    "PGP URL",
    "PGP Description",
    "PGP Type",
    "PGP Date",
    "PGP Languages",
    "PGP Tags",
    "NLI Catalog Entry",  # Codex MUST-FIX 2 — NOT 'NLI Description'.
    "Catalog Summary",
    "Library Viewer URL",
    "GenizahSearch URL",
]  # 14 columns; matches build_manuscript_row output order.


BIBLIOGRAPHY_HEADERS: List[str] = [
    "System ID",
    "Shelfmark",
    "Article Author",
    "Article Name",
    "Running Title",
    "Title Year",
    "Mention Page",
    "Catalog Acronym",
]  # 8 columns; matches build_bibliography_rows row output order.


# ---------------------------------------------------------------------------
# MetaResolver type alias
# ---------------------------------------------------------------------------

# Codex SHOULD-FIX 8 — callable returning a 4-key primitive dict,
# NOT an opaque meta_mgr object. Prevents silent web/desktop drift.
MetaResolver = Callable[[str], Optional[Dict[str, Any]]]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _split_pgp_languages(value: Any) -> List[str]:
    """Normalize a languages projection to a list of stripped non-empty strings.

    The SUPERSEDED-v2 plan had a latent bug: ``list(doc.get('languages_primary'))``
    iterates a comma-separated STRING as characters when the sidecar value is
    ``'Hebrew, Aramaic'`` instead of ``['Hebrew', 'Aramaic']``. The pgp.db
    sidecar's ``languages_primary`` / ``languages_secondary`` columns are
    declared TEXT (see ``scripts/export_pgp_sidecar.py:95-96``) so the
    comma-split branch is the production path; the list-input branch is
    defensive coverage for synthetic or future callers (Codex Q5).
    """
    if value is None or value == '':
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v and str(v).strip()]
    if isinstance(value, str):
        return [s.strip() for s in value.split(',') if s.strip()]
    return []


# ---------------------------------------------------------------------------
# Helper 1: PGP subset
# ---------------------------------------------------------------------------


def pgp_subset_for_sys_id(sys_id: str) -> Optional[Dict[str, Any]]:
    """Return a 6-key PGP projection for a manuscript or ``None``.

    Keys (always present when the helper returns a dict; values may be None):
    ``pgp_url``, ``description``, ``document_type``, ``date_display``,
    ``languages``, ``tags``.

    Date uses the fallback chain ``inferred_date_display`` →
    ``doc_date_standard`` → ``doc_date_original`` → ``None``.

    Languages are normalized via :func:`_split_pgp_languages` to handle the
    comma-separated TEXT projection bug; primary + secondary are merged with
    duplicates removed (preserving primary order).

    The helper NEVER emits ``page_section_text``, ``transcription``,
    ``full_text``, or any field outside the 6-key whitelist (D-02 boundary).
    """
    if not sys_id:
        return None
    try:
        doc = get_document_for_fragment(sys_id)
        if not doc:
            return None
        date_display = (
            doc.get('inferred_date_display')
            or doc.get('doc_date_standard')
            or doc.get('doc_date_original')
        )
        langs_primary = _split_pgp_languages(doc.get('languages_primary'))
        langs_secondary = _split_pgp_languages(doc.get('languages_secondary'))
        # Merge primary + secondary, dedupe while preserving order.
        languages = list(langs_primary)
        for lang in langs_secondary:
            if lang not in languages:
                languages.append(lang)
        return {
            'pgp_url': doc.get('pgp_url'),
            'description': doc.get('description'),
            'document_type': doc.get('document_type'),
            'date_display': date_display,
            'languages': languages,
            'tags': list(doc.get('tags') or []),
        }
    except Exception as e:
        logger.warning("pgp_subset_for_sys_id(%s) failed: %s", sys_id, e)
        return None


# ---------------------------------------------------------------------------
# Helper 2: NLI subset
# ---------------------------------------------------------------------------


def nli_subset_for_sys_id(sys_id: str) -> Optional[Dict[str, Any]]:
    """Return a 2-key NLI projection for a manuscript or ``None``.

    Returns ``{'catalog_entry': str|None, 'library_viewer_url': str|None}``
    when the NLI sidecar has at least one of the two values for the sys_id;
    otherwise returns ``None`` (no useful data).

    The column header in the workbook is **NLI Catalog Entry**
    (Codex MUST-FIX 2 — :meth:`NliCrossrefService.get_catalog_entry` returns
    Neubauer-Cowley reference strings such as ``'Neubauer - Cowley 2603.1'``,
    NOT descriptions).

    ``thread_safe=True`` is passed to the factory because exports run from
    background tasks on web (NiceGUI) and could run from any thread on
    desktop. The default is ``False`` (see
    ``shared/nli_crossref_service.py:1019``) — established web/shared
    consumers always pass ``True`` (e.g. ``shared/browse_service.py:215-216``)
    and the export pipeline follows the same convention.
    """
    if not sys_id:
        return None
    try:
        svc = get_nli_crossref_service(thread_safe=True)
        if not svc or not svc.is_available():
            return None
        catalog_entry = svc.get_catalog_entry(sys_id)
        viewer = svc.get_library_viewer_url(sys_id)
        viewer_url = viewer.get('url') if isinstance(viewer, dict) else None
        if not catalog_entry and not viewer_url:
            return None
        return {
            'catalog_entry': catalog_entry,
            'library_viewer_url': viewer_url,
        }
    except Exception as e:
        logger.warning("nli_subset_for_sys_id(%s) failed: %s", sys_id, e)
        return None


# ---------------------------------------------------------------------------
# Helper 3: Catalog summary
# ---------------------------------------------------------------------------


def catalog_summary_for_sys_id(sys_id: str) -> Optional[Dict[str, Any]]:
    """Return a narrow FJMS catalog projection for a manuscript or ``None``.

    Returns 4 fields: ``title`` (English first, Hebrew fallback per D-04),
    ``author_text``, ``copy_date`` (already sentinel-normalized to None by the
    service at :meth:`FjmsService.get_catalog_records`), ``copy_place``.

    **Aggregation strategy:** *first non-empty per field* across all records
    returned by :meth:`FjmsService.get_catalog_records`. Multiple records
    arise when a manuscript has been cataloged by several teams; we pick
    the first scholar-provided value for each field independently rather
    than picking a single record verbatim, because cataloging is often
    partial. Field choice rationale:

    - **title** + **author_text** + **copy_date** + **copy_place** cover the
      core scholarly metadata most likely to be cited.
    - ``textual_frame_eng`` and ``genizah_title_eng`` are intentionally NOT
      surfaced — they would duplicate the PGP ``description`` and
      ``document_type`` columns already present on the Manuscripts sheet.

    **Codex MUST-FIX 3 / D-02 boundary:** uses
    :meth:`FjmsService.get_catalog_records` only. The detail variant
    (which loads ``full_texts``) is intentionally not invoked from the
    dossier path.
    """
    if not sys_id:
        return None
    try:
        fjms = get_fjms_service(thread_safe=True)
        if not fjms or not fjms.is_available():
            return None
        records = fjms.get_catalog_records(sys_id)
        if not records:
            return None

        def _pick(field):
            for rec in records:
                v = rec.get(field)
                if v is not None and str(v).strip():
                    return v
            return None

        title = _pick('title') or _pick('title_heb')  # D-04: English first, Hebrew fallback.
        author_text = _pick('author_text')
        copy_date = _pick('copy_date')
        copy_place = _pick('copy_place')

        if not any((title, author_text, copy_date, copy_place)):
            return None

        return {
            'title': title,
            'author_text': author_text,
            'copy_date': copy_date,
            'copy_place': copy_place,
        }
    except Exception as e:
        logger.warning("catalog_summary_for_sys_id(%s) failed: %s", sys_id, e)
        return None


# ---------------------------------------------------------------------------
# Helper 4: Bibliography
# ---------------------------------------------------------------------------


def bibliography_for_sys_id(sys_id: str) -> List[Dict[str, Any]]:
    """Return 0..N FJMS bibliography entries projected to 6 whitelisted keys.

    Keys per entry: ``running_title``, ``title_year``, ``mention_page``,
    ``article_name``, ``article_author_eng``, ``catalog_acronym`` (Codex
    MUST-FIX 1 — these are the REAL FJMS field names; the SUPERSEDED-v2 plan
    invented Author / Publisher / Source Name which do not exist in
    :meth:`FjmsService.get_bibliography`'s return shape).

    The extended fields ``comment`` / ``note_for_display`` / ``catalog_entry``
    that the service may include are deliberately dropped — they don't fit
    the dossier sub-sheet structure and risk D-02 boundary creep.
    """
    if not sys_id:
        return []
    try:
        fjms = get_fjms_service(thread_safe=True)
        if not fjms or not fjms.is_available():
            return []
        entries = fjms.get_bibliography(sys_id) or []
        return [
            {
                'running_title': e.get('running_title'),
                'title_year': e.get('title_year'),
                'mention_page': e.get('mention_page'),
                'article_name': e.get('article_name'),
                'article_author_eng': e.get('article_author_eng'),
                'catalog_acronym': e.get('catalog_acronym'),
            }
            for e in entries
        ]
    except Exception as e:
        logger.warning("bibliography_for_sys_id(%s) failed: %s", sys_id, e)
        return []


# ---------------------------------------------------------------------------
# Row emitter 1: Manuscripts
# ---------------------------------------------------------------------------


def build_manuscript_row(
    sys_id: str,
    meta_resolver: Optional[MetaResolver],
    lang: str = 'en',
) -> List[Any]:
    """Build one Manuscripts sub-sheet row for a sys_id.

    Calls 3 helpers — :func:`pgp_subset_for_sys_id`,
    :func:`nli_subset_for_sys_id`, :func:`catalog_summary_for_sys_id`.
    Does NOT call :func:`bibliography_for_sys_id` (Codex MUST-FIX 4 —
    bibliography rows live on the separate Bibliography sub-sheet via
    :func:`build_bibliography_rows`).

    Returns a list of exactly 14 Python primitives matching
    :data:`MANUSCRIPT_HEADERS` order. Missing data renders as empty strings
    (NOT 'N/A' / placeholders — D-06).

    The ``lang`` parameter is reserved for the CALLER's downstream sheet-view
    direction decision; row content is always English regardless of ``lang``
    value per D-04 / Codex SHOULD-FIX 9.

    ``library_name`` comes from the caller-supplied ``meta_resolver``, which
    is expected to call ``genizah_core.get_library_display(code, short=False,
    lang='en')``. On unknown library codes that function returns the input
    code unchanged (``LIBRARY_CODES.get(code, code)`` semantics at
    ``genizah_core.py:1820-1838``) — this is acceptable graceful degradation
    per D-06 / MUST-FIX 94-01-B.
    """
    if meta_resolver is not None and sys_id:
        meta = meta_resolver(sys_id)
    else:
        meta = None

    if isinstance(meta, dict):
        shelfmark = meta.get('shelfmark') or ''
        title = meta.get('title') or ''
        library_name = meta.get('library_name') or ''
    else:
        shelfmark = ''
        title = ''
        library_name = ''

    pgp = pgp_subset_for_sys_id(sys_id) or {}
    nli = nli_subset_for_sys_id(sys_id) or {}
    catalog = catalog_summary_for_sys_id(sys_id) or {}

    # D-05: pipe-joined, NO surrounding spaces.
    languages_pipe = '|'.join(pgp.get('languages') or [])
    tags_pipe = '|'.join(pgp.get('tags') or [])

    # Catalog Summary cell: 'Title: X | Author: Y | Date: Z | Place: W'
    # with empty fields omitted entirely (CONTEXT D-08 helper 3 strategy).
    cat_parts: List[str] = []
    for key, label in (
        ('title', 'Title'),
        ('author_text', 'Author'),
        ('copy_date', 'Date'),
        ('copy_place', 'Place'),
    ):
        v = catalog.get(key)
        if v is not None and str(v).strip():
            cat_parts.append(f"{label}: {v}")
    catalog_summary_str = ' | '.join(cat_parts)

    genizah_url = (
        f'https://genizahsearch.com/browse?sys_id={sys_id}' if sys_id else ''
    )

    return [
        sys_id or '',
        shelfmark,
        library_name,
        title,
        pgp.get('pgp_url') or '',
        pgp.get('description') or '',
        pgp.get('document_type') or '',
        pgp.get('date_display') or '',
        languages_pipe,
        tags_pipe,
        nli.get('catalog_entry') or '',
        catalog_summary_str,
        nli.get('library_viewer_url') or '',
        genizah_url,
    ]


# ---------------------------------------------------------------------------
# Row emitter 2: Bibliography
# ---------------------------------------------------------------------------


def build_bibliography_rows(
    sys_id: str,
    meta_resolver: Optional[MetaResolver],
) -> List[List[Any]]:
    """Build 0..N Bibliography sub-sheet rows for a sys_id.

    Calls :func:`bibliography_for_sys_id` ONLY (Codex MUST-FIX 4).

    Returns a list of row-lists, each row exactly 8 cells matching
    :data:`BIBLIOGRAPHY_HEADERS` order. Empty list when the sys_id has no
    bib entries.

    Per D-06, missing string fields render as empty cells.
    """
    if not sys_id:
        return []
    entries = bibliography_for_sys_id(sys_id) or []
    if not entries:
        return []

    if meta_resolver is not None:
        meta = meta_resolver(sys_id)
    else:
        meta = None
    shelfmark = (
        meta.get('shelfmark') if isinstance(meta, dict) else None
    ) or ''

    rows: List[List[Any]] = []
    for entry in entries:
        # title_year may legitimately be an integer (e.g. 1967); preserve
        # numeric type when present.
        title_year_v = entry.get('title_year')
        if title_year_v in (None, ''):
            title_year_cell: Any = ''
        else:
            title_year_cell = title_year_v
        rows.append([
            sys_id or '',
            shelfmark,
            entry.get('article_author_eng') or '',
            entry.get('article_name') or '',
            entry.get('running_title') or '',
            title_year_cell,
            entry.get('mention_page') or '',
            entry.get('catalog_acronym') or '',
        ])
    return rows
