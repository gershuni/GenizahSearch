"""The head tags of a manuscript's /browse page: what its address may put in them.

``/browse`` builds its meta tags and two JSON-LD blocks from the ``sys_id`` and
``shelfmark`` of the request's address. Both are the visitor's own text, so they
reach the head only through this module:

- ``tag_sys_id`` accepts a ``sys_id`` only when the whole value is a system number.
  Anything else gets the page's generic tags, as an address with no ``sys_id`` does.
- ``json_ld_script`` writes a JSON-LD block that no value can close: the data goes
  through ``json.dumps``, and then ``<``, ``>`` and ``&`` become their JSON escapes.

``page_meta`` in ``web/main.py`` escapes the address it writes into attributes.

Until 2026-11-27 the page wrote these values as they came. The line meant to escape
``<`` in the Manuscript block replaced it with itself, so ``</script>`` in the
address closed the block (tests/render_smoke/test_browse_head_escaping.py).
"""
from __future__ import annotations

import json
from typing import Optional

from shared.sys_id_patterns import CORPUS_SYS_ID_RE

# The escapes are built from character codes and never typed out. A JSON escape
# typed into a source file can be decoded on its way to disk, and "replace '<'
# with '<'" is what the guard this module replaces had become.
_BACKSLASH = chr(92)
_JSON_ESCAPES = {ord(ch): f'{_BACKSLASH}u{ord(ch):04x}' for ch in '<>&'}


def tag_sys_id(value: object) -> Optional[str]:
    """``value`` when all of it is a corpus system number, else None.

    The website holds no "My Library" records, so only the corpus namespace counts
    (shared/sys_id_patterns.py). ``fullmatch``: the pattern alone finds a system
    number anywhere in a longer text.
    """
    if not isinstance(value, str):
        return None
    return value if CORPUS_SYS_ID_RE.fullmatch(value) else None


def json_ld_script(data: dict) -> str:
    """``data`` as one ``<script type="application/ld+json">`` block.

    Whatever the values hold, the block's text has no ``<``, ``>`` or ``&``, and it
    parses back to ``data``.
    """
    body = json.dumps(data, ensure_ascii=False, indent=2).translate(_JSON_ESCAPES)
    return '<script type="application/ld+json">\n' + body + '\n</script>'


def breadcrumb_data(shelfmark: str, sys_id: str, site_url: str) -> dict:
    """Home > Browse > this manuscript, as schema.org BreadcrumbList data."""
    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": f"{site_url}/"},
            {"@type": "ListItem", "position": 2, "name": "Browse", "item": f"{site_url}/browse"},
            {"@type": "ListItem", "position": 3, "name": shelfmark,
             "item": f"{site_url}/browse?sys_id={sys_id}"},
        ],
    }


def manuscript_data(shelfmark: str, sys_id: str, site_url: str, library_name: str = '') -> dict:
    """The manuscript as schema.org Manuscript data; the library only when it is known."""
    data = {
        "@context": "https://schema.org",
        "@type": "Manuscript",
        "name": shelfmark,
        "identifier": sys_id,
        "url": f"{site_url}/browse?sys_id={sys_id}",
        "isPartOf": {
            "@type": "Collection",
            "name": "Cairo Genizah",
        },
        "about": "Cairo Genizah manuscript fragment",
        "inLanguage": ["he", "arc", "jrb"],
    }
    if library_name:
        data["holdingArchive"] = {
            "@type": "ArchiveOrganization",
            "name": library_name,
        }
    return data
