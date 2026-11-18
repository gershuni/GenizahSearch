# -*- coding: utf-8 -*-
"""Result rows of the catalogue text search (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md §4.3).

``SearchEngine.execute_search`` in mode ``CATALOG_MODE`` answers with these rows, so the search
page, the public API and the exports take a catalogue search like any other: one row per
manuscript, in the catalogue's rank order, with the shelfmark and title the other modes show
(``display``), a snippet of the catalogue text that matched (``snippet``, marked as the result
cards mark), and where that text comes from (``catalogue``). A catalogue row has no transcription
(``full_text`` is empty): the catalogue says what a manuscript is described as, never what it
reads, so these rows are never mixed into a text search's.

The search runs in the calling thread (SQLite, read-only; ``web/research_jobs.py`` keeps it out
of the research workers).
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

from shared.catalog_text_normalize import display_form

#: The engine mode (``SearchEngine.parse_query_syntax`` maps the ``%`` prefix to it).
CATALOG_MODE = "Catalog"
#: Rows a catalogue search returns at most (the count is exact; the rest are reported as cut).
CATALOG_MAX_ROWS = 5000

#: The source of a catalogue record, as the card names it (English; the apps translate).
SOURCE_LABELS = {
    "fjms": "FJMS Catalog",
    "pgp": "PGP description",
    "libraries": "Library catalogue title",
}
#: The field of an FJMS record, as the card names it after the source.
FIELD_LABELS = {
    "GenizahTitle": "Work / Title",
    "GenizahTitleEng": "Work / Title",
    "GenizahTitleEngTitle": "Work / Title",
    "Title": "Title",
    "TitleHeb": "Title",
    "RunningTitle": "Running Title",
    "FreeDesc": "Free Description",
    "TextualFrame": "Textual frame",
    "Comment": "Note",
    "Colophon": "Colophon",
    "AuthorText": "Author",
    "CopyPlace": "Place",
    "Mention": "Mentions",
    "FullText": "Full catalogue entry",
}
AUTOMATIC_LABEL = "automatic translation"


def catalog_options(options: Optional[dict]) -> dict:
    """The catalogue search's two switches from a request's options: ``include_translations``
    (default on, plan D2) and ``near_spellings`` (default off, D8)."""
    options = options or {}
    return {
        "include_translations": bool(options.get("include_translations", True)),
        "near_spellings": bool(options.get("near_spellings", False)),
    }


def source_label_parts(source: str, field: str, automatic: bool) -> list[str]:
    """The English pieces of a row's source line, in order: the source, the field (FJMS only),
    and "automatic translation" when it is one. The caller translates each and joins them."""
    parts = [SOURCE_LABELS.get(source, source)]
    if source == "fjms" and field in FIELD_LABELS:
        parts.append(FIELD_LABELS[field])
    if automatic:
        parts.append(AUTOMATIC_LABEL)
    return parts


def near_forms_for_display(forms: Iterable[str]) -> list[str]:
    """Near spellings as a reader writes them (Hebrew final letters restored)."""
    return [display_form(f) for f in forms]


def _known(meta_mgr: Any, sys_id: str) -> bool:
    """False when the catalogue the app has loaded does not know ``sys_id`` (a manuscript dropped
    since the file was built, plan §4.6); True while that catalogue is not loaded."""
    bank = getattr(meta_mgr, "csv_bank", None)
    return not bank or sys_id in bank


def _display(meta_mgr: Any, sys_id: str) -> dict:
    """The card's shelfmark, title and library for ``sys_id``."""
    shelfmark, title = meta_mgr.get_meta_for_id(sys_id)
    return {
        "shelfmark": shelfmark if shelfmark and shelfmark != "Unknown" else f"ID: {sys_id}",
        "title": title or "",
        "img": "",
        "source": "",
        "id": sys_id,
        "library_code": meta_mgr.get_library_for_id(sys_id) or "",
    }


def catalog_search_rows(query: str, options: Optional[dict], meta_mgr: Any,
                        restrict_sys_ids: Optional[set] = None, service: Any = None,
                        progress_callback=None, max_rows: int = CATALOG_MAX_ROWS) -> tuple[list[dict], dict]:
    """The rows of a catalogue search, and what the search says about them:
    ``{'total': every matching manuscript (after the restriction), 'capped': rows were cut at
    max_rows, 'near_forms': the near spellings searched (as written), 'available': the service is
    served}``. Never raises for a bad query; an unavailable service answers no rows."""
    from shared.catalog_text_service import get_catalog_text_service

    opts = catalog_options(options)
    service = service or get_catalog_text_service()
    info = {"total": 0, "capped": False, "near_forms": [], "available": bool(service.is_available())}
    if not info["available"]:
        return [], info
    result = service.search(query, include_translations=opts["include_translations"], limit=None,
                            near_spellings=opts["near_spellings"])
    info["near_forms"] = near_forms_for_display(result.near_forms)
    # Every hit is checked, not only the rows kept: the count is exact (the API publishes it), so a
    # manuscript the loaded catalogue no longer knows is left out of it wherever it ranks.
    hits = [h for h in result.hits
            if (restrict_sys_ids is None or h.sys_id in restrict_sys_ids) and _known(meta_mgr, h.sys_id)]
    total = len(hits)
    info["capped"] = total > max_rows
    kept: list[tuple[Any, dict]] = []
    for i, hit in enumerate(hits[:max_rows]):
        if progress_callback and i % 200 == 0:
            progress_callback(i, total)
        kept.append((hit, _display(meta_mgr, hit.sys_id)))
    snippets = service.snippets([hit for hit, _ in kept], result)
    rows: list[dict] = []
    for (hit, display), snippet in zip(kept, snippets):
        rows.append({
            "display": display,
            "snippet": snippet,
            "full_text": "",
            "uid": "",
            "raw_header": "",
            "raw_file_hl": "",
            "highlight_pattern": None,
            "metadata_only": True,
            "catalogue": {
                "source": hit.source, "field": hit.field, "lang": hit.lang,
                "automatic": hit.automatic, "near": hit.near, "exact": hit.exact,
            },
        })
    info["total"] = total
    return rows, info
