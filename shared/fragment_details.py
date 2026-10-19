# -*- coding: utf-8 -*-
"""What the Joins Lab Compare window says about a fragment besides its images.

The web Compare modal (``web/components/compare_modal.py``) and the desktop
CompareDialog (``desktop/join_workbench.py``) both show, for each side, one short
info line (library · domain · date · size · material) and a row of links (Ktiv,
the holding library's own viewer, the Princeton Geniza Project). This module is
the one rule for both, so the two windows cannot disagree about a fragment.

``fetch_fragment_details`` reads only local data -- the FJMS sidecar, the NLI
crossref sidecar, pgp.db and, for Oxford, the codicological parts already in
memory -- so it is cheap enough to call on every flip. It never raises: a source
that fails or is missing just leaves its part out. Call it off the UI thread /
event loop all the same; these are SQLite reads.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from shared.local_sys_id import is_local_sys_id
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)

_KTIV_URL = (
    "https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage"
    "?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
)

# FJMS writes these into CopyDate when a record has no date (fjms_service keeps the
# same set for its catalog records).
_NO_DATE = frozenset(('0', '-99', '-1', '0.0', '-99.0', '-1.0', ''))

# Between the parts of the info line.
INFO_SEPARATOR = " · "


def ktiv_url(sys_id: Any) -> Optional[str]:
    """The NLI Ktiv catalogue page for a corpus manuscript, or None.

    None for an empty, a synthetic (metadata-only) or a LOCAL ("My Library") id:
    Ktiv has no page for any of them.
    """
    sid = str(sys_id or "").strip()
    if not sid or is_synthetic_sys_id(sid) or is_local_sys_id(sid):
        return None
    return _KTIV_URL.format(sys_id=sid)


def domain_labels(domains: list[dict], *, hebrew: bool) -> list[str]:
    """The fragment's domains as shown: Hebrew names in the Hebrew UI.

    A domain without a Hebrew name keeps its English one (the rule the browse
    page's Subject Domains and the Visual Similarity window use). Duplicates and
    blanks are dropped; order is kept.
    """
    labels: list[str] = []
    for d in domains or []:
        english = (d.get("domain") or "").strip()
        if not english or english == "--":
            continue
        label = ((d.get("domain_heb") or "").strip() if hebrew else "") or english
        if label not in labels:
            labels.append(label)
    return labels


def dims_text(width_cm: Any, height_cm: Any) -> str:
    """'18×12 cm' when both measurements are known, else ''."""
    try:
        w = float(width_cm)
        h = float(height_cm)
    except (TypeError, ValueError):
        return ""
    if w <= 0 or h <= 0:
        return ""
    return f"{w:.0f}×{h:.0f} cm"


def info_line(
    library_code: str = "",
    domains: Optional[list[str]] = None,
    dating: Optional[str] = None,
    width_cm: Any = None,
    height_cm: Any = None,
    material: str = "",
) -> str:
    """One line: library · domain(s) · date · size · material, skipping what is unknown.

    ``domains`` are labels already chosen for the UI language (``domain_labels``)
    and ``material`` is already translated by the caller; this only orders and
    joins them.
    """
    parts = [
        (library_code or "").strip(),
        ", ".join(d for d in (domains or []) if d),
        (dating or "").strip(),
        dims_text(width_cm, height_cm),
        (material or "").strip(),
    ]
    return INFO_SEPARATOR.join(p for p in parts if p)


def _clean_date(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return None if text in _NO_DATE else text


def _oxford_link(sys_id: str, meta_mgr: Any) -> Optional[dict]:
    """The Bodleian page of the Oxford part this folio belongs to, if known locally."""
    if meta_mgr is None:
        return None
    try:
        part_id = meta_mgr.get_part_for_folio(sys_id)
        if not part_id:
            return None
        url = (meta_mgr.get_part_metadata(part_id) or {}).get("direct_link")
    except Exception as e:
        logger.debug("fragment_details: Oxford part lookup failed for %s: %s", sys_id, e)
        return None
    return {"key": "library", "label": "Oxford Bodleian", "url": url} if url else None


def fetch_fragment_details(
    sys_id: str,
    page: Optional[int] = None,
    *,
    meta_mgr: Any = None,
) -> dict:
    """Library, title, domains, date and external links for one fragment, locally.

    Returns ``{'library_code': str, 'title': str, 'domains': [{domain, domain_heb,
    ...}], 'dating': str | None, 'links': [{'key', 'label', 'url'}]}``. Library
    code and title come from ``meta_mgr`` (libraries.csv) when it is given; the
    caller keeps its own values when these are empty. Links come in a fixed
    order: Ktiv, the holding library's viewer (NLI crossref; Oxford from
    ``meta_mgr``), then the Princeton Geniza Project. ``page`` picks the PGP
    document when a fragment has one per side.
    """
    sid = str(sys_id or "").strip()
    details: dict = {"library_code": "", "title": "", "domains": [], "dating": None, "links": []}
    if not sid or is_local_sys_id(sid):
        return details

    if meta_mgr is not None:
        try:
            row = (getattr(meta_mgr, "csv_bank", None) or {}).get(sid) or {}
            details["library_code"] = (row.get("library_code") or "").strip()
            details["title"] = (row.get("title") or "").strip()
        except Exception as e:
            logger.debug("fragment_details: catalogue row lookup failed for %s: %s", sid, e)

    try:
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        if fjms.is_available():
            details["domains"] = (fjms.get_domains_for_sys_ids([sid]) or {}).get(sid) or []
            details["dating"] = _clean_date((fjms.get_catalog(sid) or {}).get("copy_date"))
    except Exception as e:
        logger.debug("fragment_details: FJMS lookup failed for %s: %s", sid, e)

    links = details["links"]
    ktiv = ktiv_url(sid)
    if ktiv:
        links.append({"key": "ktiv", "label": "Ktiv", "url": ktiv})

    library_link = None
    try:
        from shared.nli_crossref_service import get_nli_crossref_service
        viewer = get_nli_crossref_service(thread_safe=True).get_library_viewer_url(sid)
        if viewer and viewer.get("url"):
            library_link = {
                "key": "library",
                "label": viewer.get("label") or "Library",
                "url": viewer["url"],
            }
    except Exception as e:
        logger.debug("fragment_details: crossref lookup failed for %s: %s", sid, e)
    if library_link is None:
        library_link = _oxford_link(sid, meta_mgr)
    if library_link:
        links.append(library_link)

    try:
        from shared.document_service import get_document_for_fragment
        doc = get_document_for_fragment(sid, page)
        if doc and doc.get("pgp_url"):
            links.append({"key": "pgp", "label": "PGP", "url": doc["pgp_url"]})
    except Exception as e:
        logger.debug("fragment_details: PGP lookup failed for %s: %s", sid, e)

    return details
