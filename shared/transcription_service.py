"""Source-agnostic "has manual transcription" presence (SEED-022).

A reader wants to know, on a result row, whether there is a *manual / scholarly*
transcription OR translation to READ for a manuscript -- regardless of which
project produced it. This module computes that union across sources:

    PGP readable text  ∪  FGP sources   (today)
    ∪  user-contributed transcriptions  (FUTURE -- no store exists yet)

It is deliberately ADDITIVE and independent of the existing PGP badge:

* The PGP badge keeps its own *link-presence* helper
  (``document_service.get_sys_ids_with_transcriptions`` -- ~34K sys_ids, "is this
  in PGP at all"). It is NOT touched here.
* This union uses the PGP *text-presence* predicate
  (``document_service.get_sys_ids_with_pgp_text`` -- has_transcription/has_translation,
  ~7.3K sys_ids) so the new tag means "there is genuine manual text to read".

Graceful degradation: FGP returns an empty set when its sidecar/flag is absent, so
the union then simply equals the PGP-text set (still a correct superset by
construction).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

logger = logging.getLogger(__name__)


def get_sys_ids_with_manual_transcriptions(
    sys_ids: List[str], *, include_user: bool = True
) -> Set[str]:
    """Union of manual-transcription presence across sources (translations included).

    PGP readable text ∪ FGP sources today; a user-source slot is reserved for the
    future (no ``transcriptions`` store exists yet -- ``corrections`` are edits and
    ``discoveries`` are discussion, so ``include_user`` is a no-op for now and kept
    only so a future store is a one-line add).

    Args:
        sys_ids: System IDs to check (cast to list once; matches the underlying
            helpers which require ``List[str]`` for ``len()``/slicing).
        include_user: Reserved. When a user-transcription store ships, this gates a
            third union term.

    Returns:
        Set of sys_ids that have at least one manual transcription OR translation.
    """
    sys_ids = list(sys_ids or [])
    if not sys_ids:
        return set()

    # Imported lazily so importing this module never forces the FGP/PGP service
    # singletons (and their SQLite connections) to construct.
    from shared.document_service import get_sys_ids_with_pgp_text
    from shared.fgp_service import get_sys_ids_with_fgp_sources

    out: Set[str] = set()
    out |= get_sys_ids_with_pgp_text(sys_ids)        # PGP readable text
    out |= get_sys_ids_with_fgp_sources(sys_ids)     # FGP (editions + translations); set() when absent
    # if include_user:
    #     out |= get_sys_ids_with_user_transcriptions(sys_ids)  # FUTURE -- no store yet
    return out


def union_manual_transcriptions(pgp_text_ids: Set[str], fgp_ids: Set[str]) -> Set[str]:
    """Combine already-fetched per-source sets into the manual-transcription union.

    Use this at call sites that ALREADY fetch the PGP-text and FGP sets (e.g. the
    web enrichment passes) to avoid re-querying PGP. Pure set algebra, no I/O.
    """
    return set(pgp_text_ids) | set(fgp_ids)


# A result row's page: (page number, its text, how many pages the manuscript or volume has).
PageRead = Tuple[int, str, int]


class ScholarlyCheck(NamedTuple):
    """What ``rows_with_scholarly_transcription`` found: the rows whose page has one, and
    whether every row was answered (False when a manuscript's sources or pages could not
    be read: its rows are then neither in *found* nor known to have none)."""
    found: Set[str]
    complete: bool


def _row_page_number(row: Dict[str, Any]) -> int:
    """The page a result opens on, read as Quick View reads it (``display['img']``, else 1)."""
    try:
        return int((row.get("display") or {}).get("img") or 1)
    except (TypeError, ValueError):
        return 1


def _found_in_shown_edition(row: Dict[str, Any], include_fgp: bool) -> bool:
    """The row was found in a scholarly transcription its reader is shown: a PGP edition, or an
    FGP one where the caller's FGP switch is on (the row's own edition or any member's)."""
    editions = [row.get("edition")] + [m.get("edition") for m in row.get("members") or []
                                       if isinstance(m, dict)]
    kinds = {e.get("kind") for e in editions if isinstance(e, dict)}
    return "pgp" in kinds or (include_fgp and "fgp" in kinds)


def rows_with_scholarly_transcription(
    rows: Iterable[Dict[str, Any]],
    *,
    read_page: Callable[[Dict[str, Any]], Optional[PageRead]],
    include_fgp: bool = True,
) -> ScholarlyCheck:
    """The ``uid`` of every result row whose page has a scholarly transcription, and whether
    every row was answered (``ScholarlyCheck``).

    A page has one when the version chooser offers a PGP or FGP edition with text on it:
    the chooser's own page filter (``filter_sources_for_page``) decides, on the page,
    page count and text Quick View opens the result with, so the results filter, Quick
    View and /browse cannot disagree. Translations do not count. A row found in a
    scholarly transcription has one, its page identified or not, unless that is an FGP
    edition and *include_fgp* is off: then its page is checked like any other.

    *read_page* returns ``(p_num, text, total_pages)`` for a row's page, or ``None``
    when the index has no such page (Quick View then shows the result's own page
    number, text and a count of 1, and so does this). *include_fgp*: the caller's FGP
    switch (the web has its own besides the shared one).

    Only manuscripts with an edition at all are read, so a result list costs one batch
    query per source plus one page read per row on an edited manuscript. Blocking:
    call it off the event loop.
    """
    from shared.document_service import (
        get_all_sources_for_fragment, get_sys_ids_with_editions,
    )
    from shared.fgp_service import (
        displayed_fgp_image_number, displayed_folio_label, filter_sources_for_page,
        get_fgp_sources_for_fragment, get_sys_ids_with_fgp_editions,
        group_transcription_sources,
    )

    found: Set[str] = set()
    by_sys_id: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows or []:
        uid = row.get("uid")
        if not uid:
            continue
        if _found_in_shown_edition(row, include_fgp):
            found.add(uid)
            continue
        sys_id = (row.get("display") or {}).get("id")
        if sys_id:
            by_sys_id.setdefault(sys_id, []).append(row)
    if not by_sys_id:
        return ScholarlyCheck(found, True)

    sys_ids = list(by_sys_id)
    # Strict reads: a query that fails raises here rather than answering "no editions" (Codex
    # review of PR #22). A sidecar that is not there, or the FGP switch off, answers empty, as
    # the chooser shows nothing from it.
    try:
        edited = set(get_sys_ids_with_editions(sys_ids, strict=True) or ())
        if include_fgp:
            edited |= set(get_sys_ids_with_fgp_editions(sys_ids, strict=True) or ())
    except Exception:
        logger.exception("Scholarly transcription check: the edition sets could not be read")
        return ScholarlyCheck(found, False)

    failed = 0
    for sys_id in sys_ids:
        if sys_id not in edited:
            continue
        try:
            raw = list(get_all_sources_for_fragment(sys_id, strict=True) or [])
            if include_fgp:
                raw += list(get_fgp_sources_for_fragment(sys_id, strict=True) or [])
            if not any(group_transcription_sources(raw)[g] for g in ("pgp_editions", "fgp_editions")):
                continue
            for row in by_sys_id[sys_id]:
                page = read_page(row)
                if page is None:
                    p_num, text, total = _row_page_number(row), row.get("full_text") or "", 1
                else:
                    p_num, text, total = page
                # The page filter narrows a PGP text to this page in place: give it copies.
                shown = filter_sources_for_page(
                    [dict(s) for s in raw], p_num,
                    displayed_folio_label(sys_id, p_num, total),
                    displayed_fgp_image_number(sys_id, p_num, total),
                    page_text=text or "")
                groups = group_transcription_sources(shown)
                if groups["pgp_editions"] or groups["fgp_editions"]:
                    found.add(row["uid"])
        except Exception:
            failed += 1
            logger.exception("Scholarly transcription check failed for %s", sys_id)
    if failed:
        logger.warning("Scholarly transcription check: %d manuscript(s) not checked", failed)
    # A manuscript not checked leaves its rows unanswered: the caller must not read them as
    # having none (Codex review of PR #22).
    return ScholarlyCheck(found, failed == 0)
