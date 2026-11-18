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


#: The catalogue switches, explained (English keys; shared/genizah_translations.py), on both apps.
CATALOG_NEAR_TOOLTIP = ("Fuzzy search: in Hebrew, a ו or י added or dropped (כתבה for כתובה); "
                        "in Latin letters, one letter different in words of five letters or more, two in "
                        "words of nine or more (Saadya for Saadia)")
CATALOG_TRANSLATIONS_TOOLTIP = ("Also search the automatic translations of the catalogue records "
                                "(English to Hebrew and Hebrew to English)")
#: The tag of a result found only through a near spelling: the text search's own word for it.
NEAR_LABEL = "Fuzzy"


def source_label(row: Any, translate=lambda s: s) -> str:
    """A catalogue row's source line in the reader's language, its pieces joined by " · ":
    "FJMS Catalog · Free Description · automatic translation", and "Fuzzy" when it was found
    only through a near spelling. '' for a row of any other search. *translate* is the app's tr."""
    info = row.get("catalogue") if isinstance(row, dict) else None
    if not info:
        return ""
    parts = [translate(p) for p in source_label_parts(info.get("source", ""), info.get("field", ""),
                                                      bool(info.get("automatic")))]
    if info.get("near"):
        parts.append(translate(NEAR_LABEL))
    return " · ".join(parts)


#: The panel a manuscript opened from a catalogue search shows: where the search found it.
FOUND_HEADING = "Found in the catalogue records"
#: Said under the panel when the manuscript has more matching records than it shows.
FOUND_MORE = "{shown} of {count} matching records"


def marked_segments(text: str, spans: Iterable[tuple[int, int]]) -> list[tuple[str, bool]]:
    """``text`` in pieces, each with whether it is a matched word: the order the apps render it."""
    pieces: list[tuple[str, bool]] = []
    cursor = 0
    for start, end in sorted(spans):
        if start < cursor or end > len(text):
            continue
        if start > cursor:
            pieces.append((text[cursor:start], False))
        pieces.append((text[start:end], True))
        cursor = end
    if cursor < len(text):
        pieces.append((text[cursor:], False))
    return pieces


def found_for_row(row: Any, service: Any = None, translate=lambda s: s) -> Optional[dict]:
    """``found_in_catalogue`` for a catalogue search's result row, by the search the row carries
    (``catalogue.searched``); None for a row of any other search, or one saved before rows carried
    their search."""
    info = row.get("catalogue") if isinstance(row, dict) else None
    searched = info.get("searched") if isinstance(info, dict) else None
    sys_id = (row.get("display") or {}).get("id") if isinstance(row, dict) else None
    if not isinstance(searched, dict) or not sys_id:
        return None
    return found_in_catalogue(sys_id, searched.get("query", ""), searched, service=service,
                              translate=translate)


def found_in_catalogue(sys_id: str, query: str, options: Optional[dict] = None, service: Any = None,
                       translate=lambda s: s) -> dict:
    """Where a catalogue search for *query*, with its switches (*options*), found *sys_id*, as a
    manuscript view opened from that search shows it: ``{'records': [...], 'count': n}``, each
    record ``{'label': its source line (source_label's), 'segments': marked_segments of its whole
    text, 'rtl': a Hebrew record}``, at most ``MATCHES_LIMIT`` of the *count* that match.

    The records come from the catalogue file itself, so every field the search reads is shown,
    those no catalogue panel lists too (FJMS notes and colophons, mention descriptions, records
    from general sources). No records when the file is not ready, or nothing matches; never
    raises. *translate* is the app's tr."""
    return found_records(*catalogue_matches(sys_id, query, options, service=service), translate=translate)


def catalogue_matches(sys_id: str, query: str, options: Optional[dict] = None,
                      service: Any = None) -> tuple[list, int]:
    """``CatalogTextService.matches`` for the panel, with the search's switches: the data only, no
    labels, so a web page can run it off its event loop (with near spellings it builds and looks
    up each word's candidates, up to a few tenths of a second a word). ``([], 0)`` without a
    query or a ready file."""
    from shared.catalog_text_service import get_catalog_text_service

    if not sys_id or not (query or "").strip():
        return [], 0
    opts = catalog_options(options)
    service = service or get_catalog_text_service()
    if not service.is_ready():
        return [], 0
    return service.matches(sys_id, query, include_translations=opts["include_translations"],
                           near_spellings=opts["near_spellings"])


def found_records(found: Iterable[Any], count: int, translate=lambda s: s) -> dict:
    """``found_in_catalogue``'s answer from ``catalogue_matches``' (the labels in the reader's
    language: *translate* is the app's tr)."""
    records = []
    for match in found:
        parts = [translate(p) for p in source_label_parts(match.source, match.field, match.automatic)]
        if match.near:
            parts.append(translate(NEAR_LABEL))
        records.append({
            "label": " · ".join(parts),
            "segments": marked_segments(match.text, match.spans),
            "rtl": match.lang == "he",
        })
    return {"records": records, "count": count}


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
                        progress_callback=None, max_rows: int = CATALOG_MAX_ROWS,
                        ids_only: bool = False) -> tuple[list[dict], dict]:
    """The rows of a catalogue search, and what the search says about them:
    ``{'total': every matching manuscript (after the restriction), 'capped': rows were cut at
    max_rows, 'near_forms': the near spellings searched (as written), 'available': the file is open
    and checked}``. Never raises for a bad query; a service that is not ready answers no rows. Whether
    the search is offered at all (the flag) is the surfaces' question: web/catalog_assets.py, the
    API's catalog_unavailable, desktop/catalog_search.py.

    *ids_only* (a search-within completing its step): every match, uncut, as a light row -- the
    manuscript's id, as the text modes' id rows carry it -- with no card, record text or snippet,
    so a broad word costs one row per manuscript and no more (Codex review of PR #47)."""
    from shared.catalog_text_service import get_catalog_text_service

    opts = catalog_options(options)
    service = service or get_catalog_text_service()
    info = {"total": 0, "capped": False, "near_forms": [], "available": bool(service.is_ready())}
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
    info["total"] = total
    if ids_only:
        return [{"uid": "", "display": {"id": hit.sys_id, "source": ""}, "metadata_only": True}
                for hit in hits], info
    info["capped"] = total > max_rows
    kept: list[tuple[Any, dict]] = []
    for i, hit in enumerate(hits[:max_rows]):
        if progress_callback and i % 200 == 0:
            progress_callback(i, total)
        kept.append((hit, _display(meta_mgr, hit.sys_id)))
    snippets = service.snippets([hit for hit, _ in kept], result)
    # The search itself, one object every row shares: a view opened from a row shows where the
    # search found its manuscript (found_in_catalogue), restored sessions and history included.
    searched = {"query": query, **opts}
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
                "searched": searched,
            },
        })
    return rows, info
