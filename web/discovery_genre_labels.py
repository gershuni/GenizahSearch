# -*- coding: utf-8 -*-
"""Hebrew display labels for the findings page's domain facet.

THE GAP THIS CLOSES
-------------------
``works.genre`` in the discovery sidecar is 100% ENGLISH -- measured over both
shipped artifacts, every one of the 38 (public) / 61 (rebuild) distinct values
is an English ``Parent / Leaf`` string such as
``Biblical Exegesis / Biblical Exegesis- Rabbanite``. That column is the
findings page's MAIN facet, so a Hebrew reader met an English filter list.

WHY THIS IS A DISPLAY-TIME LOOKUP AND NOT A RE-BAKE
---------------------------------------------------
FJMS already holds the authority: ``domains.DomainHeb`` and
``domains.ParentDomainHeb``, reachable through
``shared.fjms_service.FjmsService.get_domain_translations``. Nothing here
invents a translation, and nothing here writes to the sidecar -- the stored
value stays the English string the service filters on, and only the LABEL a
reader sees changes. That keeps the facet's VALUE identical in both languages,
so a domain chosen in Hebrew narrows exactly the same result set (and persists
identically) as the same domain chosen in English.

Coverage measured 2026-08-04 against the live FJMS sidecar: 38/38 public and
61/61 rebuild genre strings translate in full, both halves of every compound.
An unmapped part still falls back to its ENGLISH name -- never to a blank, and
never to a guess.

OFF-LOOP DISCIPLINE
-------------------
:func:`genre_display_label` is PURE: it reads the process-wide cache and never
touches a database, so it is safe to call from a renderer on the event loop. It
is :func:`prime_domain_translations` -- and only that -- that reads FJMS, and it
does so through ``web.bounded_io.bounded_io_bound`` (exactly ONE
``run_in_executor`` dispatch, permit released by the worker, never by the
caller's lifetime). Before the prime lands, and forever if FJMS is absent, the
pure function returns the English string.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

#: The separator the stored genre string uses between a parent domain and its
#: leaf (``Bible: Texts and Translations / Aramaic Targumim``). It is the
#: separator `DiscoveryService._project_facets` itself splits the tree on, so
#: parts translated here line up exactly with the facet nodes.
GENRE_PART_SEPARATOR = " / "

#: ``{'map': None}`` = never built. ``{'map': {...}}`` = built, possibly empty
#: (FJMS absent or the query failed), which is a TERMINAL state for this
#: process: an empty map means every label falls back to English rather than
#: re-querying a sidecar that is not there on every render.
_STATE: Dict[str, Optional[Mapping[str, str]]] = {"map": None}
_LOCK = threading.Lock()

#: One permit. Two concurrent first-visitors therefore produce ONE FJMS read,
#: and the second awaits the first rather than dispatching a duplicate scan.
_PRIME_SEMAPHORE = asyncio.Semaphore(1)


def domain_translations() -> Mapping[str, str]:
    """The cached English -> Hebrew domain map; ``{}`` until primed.

    Never builds. A renderer calling this on the event loop can only ever read
    a dict.
    """
    cached = _STATE["map"]
    return cached if cached is not None else {}


def is_primed() -> bool:
    """Whether the map has been built in this process (empty counts as built)."""
    return _STATE["map"] is not None


def build_domain_translations() -> Mapping[str, str]:
    """Read the bilingual domain vocabulary from FJMS and cache it.

    BLOCKING -- call it from a worker thread (see
    :func:`prime_domain_translations`), never from the event loop.

    Fails OPEN: any failure caches an empty map, so the page renders English
    labels instead of raising during a render.
    """
    cached = _STATE["map"]
    if cached is not None:
        return cached
    with _LOCK:
        cached = _STATE["map"]
        if cached is not None:
            return cached
        mapping: Mapping[str, str] = {}
        try:
            from shared.fjms_service import get_fjms_service

            mapping = dict(get_fjms_service(thread_safe=True).get_domain_translations())
        except Exception as e:  # noqa: BLE001 -- a label lookup must never break a page
            logger.warning("discovery_genre_labels: domain translations unavailable: %s", e)
            mapping = {}
        _STATE["map"] = mapping
        return mapping


async def prime_domain_translations() -> bool:
    """Build the map OFF the event loop, at most once per process.

    Returns True when this call dispatched the build, False when the map was
    already there (so a second page load costs no dispatch at all).
    """
    if _STATE["map"] is not None:
        return False
    from web.bounded_io import bounded_io_bound

    await bounded_io_bound(_PRIME_SEMAPHORE, build_domain_translations)
    return True


def _unassigned_label(lang: str) -> Optional[str]:
    """The Hebrew name for the service's "no domain recorded" bucket, or None.

    That bucket is NOT a genre: `DiscoveryService` substitutes the sentinel
    ``DOMAIN_UNASSIGNED`` for a null `works.genre`, so FJMS has no name for it
    and the facet list would otherwise carry one English item on an otherwise
    Hebrew page. The Hebrew word is the one `/catalog-browse` already uses for
    the same bucket (`tr('Unclassified')`), so this introduces no new
    vocabulary; the ENGLISH label is left exactly as the service produces it,
    and the stored VALUE is never touched in either language.

    Returns None whenever the translation table has no Hebrew entry, so a
    missing entry degrades to the English sentinel rather than to the literal
    key `tr()` would hand back.
    """
    if lang != "he":
        return None
    from web.translations import tr

    translated = tr("Unclassified")
    return translated if translated and translated != "Unclassified" else None


def _is_unassigned(text: str) -> bool:
    from shared.discovery_service import DOMAIN_UNASSIGNED

    return text == DOMAIN_UNASSIGNED


def _translate_part(part: str, mapping: Mapping[str, str]) -> str:
    """One half of a compound genre string, or the whole of a simple one.

    Falls back to the ENGLISH part when the vocabulary has no Hebrew name for
    it. A blank would be worse than the wrong language: the reader would lose
    the filter entirely.
    """
    stripped = part.strip()
    if not stripped:
        return part
    return mapping.get(stripped) or stripped


def genre_display_label(value: Optional[str], lang: str = "en") -> str:
    """The reader-facing label for one stored ``works.genre`` value.

    English (and any other language) passes the stored string through
    unchanged; Hebrew translates each part through the FJMS vocabulary and
    rejoins them with the same separator, so the parent/leaf reading survives.
    An unmapped part keeps its English name.
    """
    if value is None:
        return ""
    text = str(value)
    if lang != "he":
        return text
    if _is_unassigned(text):
        return _unassigned_label(lang) or text
    mapping = domain_translations()
    if not mapping:
        return text
    parts: List[str] = text.split(GENRE_PART_SEPARATOR)
    return GENRE_PART_SEPARATOR.join(_translate_part(part, mapping) for part in parts)


def reset_for_tests() -> None:
    """Drop the cache. Test-only -- production builds the map once per process."""
    with _LOCK:
        _STATE["map"] = None


__all__ = [
    "GENRE_PART_SEPARATOR",
    "build_domain_translations",
    "domain_translations",
    "genre_display_label",
    "is_primed",
    "prime_domain_translations",
    "reset_for_tests",
]
