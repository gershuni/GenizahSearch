#!/usr/bin/env python3
"""Acquire and normalize the curated Discovery V4 public source set.

Every network response is persisted under the ignored output directory and is
covered by the acquisition manifest.  Licenses fail closed: only the allowlist
in ``discovery_v4_sources.json`` can produce an acquired source.  Unknown,
non-commercial, and no-derivatives versions are quarantined.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import time
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable

import requests

try:
    from scripts.discovery_v4_common import (
        IDENTITY_MODE_PUBLIC_FIRST,
        clean_hebrew,
        compact_stream,
        count_hebrew_letters,
        flatten_text_node,
        load_source_config,
        sha256_file,
        stable_json_dump,
    )
except ModuleNotFoundError:  # direct ``python scripts/...py`` invocation
    from discovery_v4_common import (
        IDENTITY_MODE_PUBLIC_FIRST,
        clean_hebrew,
        compact_stream,
        count_hebrew_letters,
        flatten_text_node,
        load_source_config,
        sha256_file,
        stable_json_dump,
    )

try:
    # ``_unit_offsets`` is the established offset convention (compact_stream,
    # empty-chunk skipping, running length) -- containers reuse it exactly
    # rather than re-deriving it, so their offsets are guaranteed to round-trip
    # against the reference builder that later consumes these same units.
    from scripts.discovery_v4_build_reference import _unit_offsets
except ModuleNotFoundError:  # direct ``python scripts/...py`` invocation
    from discovery_v4_build_reference import _unit_offsets

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# ``heb_numeral``/``daf_label_he`` are the ONE citation-address implementation
# (shared/discovery_locus.py): daf-page acquisition reuses them rather than
# re-deriving Hebrew numeral rendering a second time in this file.
from shared.discovery_locus import daf_label_he, heb_numeral


SEFARIA_BASE = "https://www.sefaria.org"
WIKISOURCE_API = "https://he.wikisource.org/w/api.php"
WIKISOURCE_LICENSE = "CC-BY-SA"
WIKISOURCE_LICENSE_URL = "https://creativecommons.org/licenses/by-sa/4.0/"
USER_AGENT = (
    "GenizahSearchDiscoveryV4/1.0 "
    "(https://genizahsearch.com; public-text research corpus)"
)
_LICENSE_RANK = {
    "public domain": 0,
    "cc0": 1,
    "cc-by": 2,
    "cc by": 2,
    "cc-by-sa": 3,
    "cc by-sa": 3,
}
_LICENSE_URL = {
    "public domain": None,
    "cc0": "https://creativecommons.org/publicdomain/zero/1.0/",
    "cc-by": "https://creativecommons.org/licenses/by/4.0/",
    "cc by": "https://creativecommons.org/licenses/by/4.0/",
    "cc-by-sa": WIKISOURCE_LICENSE_URL,
    "cc by-sa": WIKISOURCE_LICENSE_URL,
}
_HEBREW_NUMERAL_VALUES = {
    "א": 1,
    "ב": 2,
    "ג": 3,
    "ד": 4,
    "ה": 5,
    "ו": 6,
    "ז": 7,
    "ח": 8,
    "ט": 9,
    "י": 10,
    "כ": 20,
    "ך": 20,
    "ל": 30,
    "מ": 40,
    "ם": 40,
    "נ": 50,
    "ן": 50,
    "ס": 60,
    "ע": 70,
    "פ": 80,
    "ף": 80,
    "צ": 90,
    "ץ": 90,
    "ק": 100,
    "ר": 200,
    "ש": 300,
    "ת": 400,
}


class _VisibleTextParser(HTMLParser):
    _SKIP_TAGS = {"script", "style"}
    _SKIP_CLASSES = {
        "mw-editsection",
        "reference",
        "references",
        "navbox",
        "metadata",
        "noprint",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        classes = set(dict(attrs).get("class", "").split())
        if self._skip_depth or tag in self._SKIP_TAGS or classes & self._SKIP_CLASSES:
            self._skip_depth += 1
        elif tag in {"p", "div", "br", "li", "h1", "h2", "h3", "h4", "td"}:
            self._parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if self._skip_depth:
            self._skip_depth -= 1
        elif tag in {"p", "div", "li", "h1", "h2", "h3", "h4", "td"}:
            self._parts.append(" ")

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self._parts.append(data)

    def text(self) -> str:
        return " ".join(html.unescape("".join(self._parts)).split())


def visible_text(value: str) -> str:
    parser = _VisibleTextParser()
    parser.feed(value)
    parser.close()
    return parser.text()


def hebrew_numeral(value: str) -> int | None:
    if any(char.isspace() for char in value.strip()):
        return None
    letters = [char for char in value if char in _HEBREW_NUMERAL_VALUES]
    residue = re.sub(r"[\u05d0-\u05ea׳״'\"]", "", value).strip()
    if not letters or residue:
        return None
    return sum(_HEBREW_NUMERAL_VALUES[char] for char in letters)


def select_chapter_links(
    links: list[dict], prefix: str, *, exclude_pages: list[str] | None = None
) -> list[tuple[int, str]]:
    """Select ``(ordinal, title)`` pairs whose title starts with ``prefix``
    and whose remainder parses as a Hebrew numeral.

    ``exclude_pages`` (discovery-v4.2 A1, the Tur trap): titles to drop
    AFTER selection -- a Hebrew word that gematria-parses to a real siman's
    value (e.g. "הקדמה"), or a letter-transposed
    redirect twin, collides on ordinal with the genuine page. Every named
    title MUST already be present in the selection, or this raises (a stale
    exclusion -- one that no longer matches anything live -- is a source-map
    bug, not a silent no-op: it would hide a real drift instead of guarding
    against one).

    The duplicate-ordinal check below runs on the SURVIVING (post-exclusion)
    selection and names the exact colliding ordinal and titles (A2) -- this
    is deliberately a hard error, never a silent pick-one-and-continue,
    because a Tur-shaped collision means one of the two pages is not what it
    claims to be.
    """
    selected = []
    for link in links:
        # A4 (2026-08-16 finding): MediaWiki's formatversion=2 "parse"
        # response marks a non-existent link target via "exists": false,
        # never "missing" -- a ``link.get("missing")`` filter here would be
        # a dead no-op against this response shape (it was, before this
        # comment). Unwritten ToC links are therefore deliberately KEPT in
        # the selection here; they surface at FETCH time instead, when the
        # per-page request itself fails: recorded as a missing page
        # (coverage_status="partial") for a private_sibling source, a hard
        # completeness-gate error for a public_first source (C8). Filtering
        # them out of the selection here would let a public_first source
        # under-report its own coverage while still claiming "complete".
        if link.get("ns") != 0:
            continue
        title = link.get("title") or ""
        if not title.startswith(prefix):
            continue
        ordinal = hebrew_numeral(title[len(prefix) :])
        if ordinal is not None:
            selected.append((ordinal, title))
    selected.sort(key=lambda item: (item[0], item[1]))
    if exclude_pages:
        present = {title for _, title in selected}
        stale = [title for title in exclude_pages if title not in present]
        if stale:
            raise ValueError(
                f"exclude_pages names page(s) absent from the {prefix!r} "
                f"selection (stale exclusion): {stale}"
            )
        exclude_set = set(exclude_pages)
        selected = [item for item in selected if item[1] not in exclude_set]
    by_ordinal: dict[int, list[str]] = {}
    for ordinal, title in selected:
        by_ordinal.setdefault(ordinal, []).append(title)
    duplicates = sorted(
        (ordinal, titles) for ordinal, titles in by_ordinal.items() if len(titles) > 1
    )
    if duplicates:
        ordinal, titles = duplicates[0]
        raise ValueError(
            f"chapter link prefix {prefix!r} produces duplicate ordinal "
            f"{ordinal} across titles {titles}"
        )
    return selected


# Downstream consumers of the NEW "daf" locus_grain value born in
# ``_acquire_wikisource_daf_pages`` (verified 2026-08-16, discovery-v4.2 C8):
#
# - ``discovery_v4_build_reference.py`` reads the grain from the SOURCE-MAP
#   entry via ``_locus_grain()``: explicit ``locus_grain`` wins, else
#   ``mode == "daf_pages"`` implies "daf" (so a map entry cannot silently
#   fall to "chapter"). Its ``_locus_label()`` "daf" branch uses each unit's
#   parse-verified acquired label, cross-checked against the ordinal
#   geometry (disagreement is a hard error), with citation_pos on the same
#   ``daf*2+amud-1`` convention as daf_bavli.
# - Beyond the locus DB the value is opaque: ``locus_work.grain`` is written
#   verbatim, ``shared/discovery_service.py`` passes it through to envelope
#   meta unbranched, and the verifier groups by grain self-consistently
#   (unlike ``family``, whose vocabulary is frozen).

#: Reverse of ``heb_numeral`` (shared/discovery_locus.py), built BY calling it --
#: never a hand-written gematria table -- so a daf-page title parses back to a
#: value only when ``heb_numeral`` itself would render that exact string for
#: that value. This is what makes the round-trip property hold by construction:
#: ``heb_numeral`` never emits thousands or a geresh (see its docstring), so
#: every key here is a bare run of Hebrew letters, and an "א1"-style suffix, an
#: unrecognized letter run, or a value outside 1..999 has no entry and is
#: refused rather than guessed at.
_DAF_NUMERAL_TO_INT: dict[str, int] = {
    heb_numeral(value): value for value in range(1, 1000)
}
_DAF_AMUD_TO_INT = {"א": 1, "ב": 2}


def parse_daf_page_title(title: str, link_prefix: str) -> tuple[int, int]:
    """Strictly parse a Wikisource daf/amud page title (C8, Zohar-class shape).

    Grammar (the ONLY strings this accepts): ``f"{link_prefix} {daf} {amud}"``
    where ``daf`` is exactly some ``heb_numeral(n)`` for ``1 <= n <= 999`` and
    ``amud`` is exactly ``"א"`` or ``"ב"`` -- one space between each of the
    three pieces, nothing before, between, or after them.

    Every anomaly this source shape is known to produce is a HARD ERROR here,
    never a skip: a title that does not start with ``link_prefix + " "``; a
    remainder that is not exactly two whitespace-separated tokens (this is
    what rejects the observed זהר חדש suffixes -- "א1" glued to one token, or
    "א 1" split into two, both fail for having the wrong shape rather than
    because "1" was specifically disallowed); a daf token outside the reverse
    table (unrecognized letters, a geresh, digits, niqqud); or an amud token
    that is not exactly א/ב.
    """
    if not title.startswith(link_prefix + " "):
        raise ValueError(
            f"page title does not start with {link_prefix!r}: {title!r}"
        )
    remainder = title[len(link_prefix) + 1 :]
    tokens = remainder.split(" ")
    if len(tokens) != 2 or not all(tokens):
        raise ValueError(
            f"page title {title!r} is not {link_prefix!r} + daf + amud "
            f"(remainder {remainder!r} does not split into exactly two tokens)"
        )
    daf_token, amud_token = tokens
    daf = _DAF_NUMERAL_TO_INT.get(daf_token)
    if daf is None:
        raise ValueError(
            f"page title {title!r} has an unrecognized daf numeral: {daf_token!r}"
        )
    amud = _DAF_AMUD_TO_INT.get(amud_token)
    if amud is None:
        raise ValueError(
            f"page title {title!r} has an unrecognized amud letter "
            f"(expected א or ב): {amud_token!r}"
        )
    return daf, amud


def _primary_title(node: dict, lang: str) -> str | None:
    for title in node.get("titles") or []:
        if title.get("lang") == lang and title.get("primary"):
            return title.get("text")
    key = "title" if lang == "en" else "heTitle"
    return node.get(key)


def _schema_leaf_refs(source_ref: str, schema: dict) -> list[tuple[str, str, str]]:
    leaves: list[tuple[str, str, str]] = []

    def walk(
        node: dict,
        path_en: tuple[str, ...],
        path_he: tuple[str, ...],
        *,
        include_title: bool = True,
    ) -> None:
        title_en = _primary_title(node, "en")
        title_he = _primary_title(node, "he")
        default = bool(node.get("default"))
        next_en = path_en + (
            (title_en,) if include_title and title_en and not default else ()
        )
        next_he = path_he + (
            (title_he,) if include_title and title_he and not default else ()
        )
        children = node.get("nodes") or []
        if children:
            for child in children:
                walk(child, next_en, next_he)
            return
        leaf_ref = ", ".join((source_ref, *next_en)) if next_en else source_ref
        leaves.append(
            (
                leaf_ref,
                " / ".join(next_en) or source_ref,
                " / ".join(next_he),
            )
        )

    # The root schema title is the index title already present in ``source_ref``.
    # Including it would fabricate refs such as ``Index, Index, Leaf``.
    walk(schema, (), (), include_title=False)
    unique = []
    seen = set()
    for leaf in leaves:
        if leaf[0] not in seen:
            seen.add(leaf[0])
            unique.append(leaf)
    return unique


class Fetcher:
    def __init__(
        self,
        output_dir: Path,
        *,
        timeout: int = 60,
        request_interval: float = 0.5,
    ) -> None:
        self.output_dir = output_dir
        self.raw_dir = output_dir / "raw"
        self.normalized_dir = output_dir / "normalized"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.normalized_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        # Politeness floor between successive requests (REF6 is a ~4,000-
        # request acquisition; back-to-back requests got this project's IP
        # HTTP-429 throttled by Wikimedia on 2026-08-16).
        self.request_interval = max(0.0, float(request_interval))
        self._last_request_at = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _respect_interval(self) -> None:
        wait = self.request_interval - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)

    @staticmethod
    def _retry_after_seconds(response, attempt: int) -> float:
        """Honor a numeric Retry-After header; otherwise back off
        exponentially (10s, 20s, 40s, ...) capped at 120s -- a provider-side
        429 window outlasts the generic 1.5s-scale retry sleeps."""
        header = (response.headers or {}).get("Retry-After")
        try:
            seconds = float(header)
        except (TypeError, ValueError):
            seconds = 10.0 * (2**attempt)
        return min(max(seconds, 1.0), 120.0)

    def get_json(self, url: str, *, params: dict | None, raw_path: Path) -> dict:
        error: Exception | None = None
        attempts = 6
        for attempt in range(attempts):
            self._respect_interval()
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout, allow_redirects=True
                )
            except requests.RequestException as exc:
                self._last_request_at = time.monotonic()
                error = exc
                if attempt < attempts - 1:
                    time.sleep(1.5 * (attempt + 1))
                continue
            self._last_request_at = time.monotonic()
            if response.status_code == 429:
                error = RuntimeError(f"HTTP 429 (rate limited): {url}")
                if attempt < attempts - 1:
                    time.sleep(self._retry_after_seconds(response, attempt))
                continue
            try:
                response.raise_for_status()
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(response.content)
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                error = exc
                if attempt < attempts - 1:
                    time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"request failed after retries: {url}: {error}")

    def sefaria_text(self, ref: str, version: str, raw_path: Path) -> dict:
        url = f"{SEFARIA_BASE}/api/v3/texts/{urllib.parse.quote(ref, safe='')}"
        return self.get_json(url, params={"version": version}, raw_path=raw_path)

    def sefaria_index(self, ref: str, raw_path: Path) -> dict:
        url = f"{SEFARIA_BASE}/api/index/{urllib.parse.quote(ref, safe='')}"
        return self.get_json(url, params=None, raw_path=raw_path)

    def wikisource_parse(self, page: str, raw_path: Path) -> dict:
        return self.get_json(
            WIKISOURCE_API,
            params={
                "action": "parse",
                "page": page,
                "prop": "text|links|revid|displaytitle",
                "format": "json",
                "formatversion": "2",
                "redirects": "1",
            },
            raw_path=raw_path,
        )


def _license_key(value: str | None) -> str:
    return str(value or "").strip().casefold()


def _pick_open_hebrew_version(available: list[dict], allowlist: set[str]) -> dict:
    candidates = [
        version
        for version in available
        if version.get("language") == "he"
        and _license_key(version.get("license")) in allowlist
    ]
    if not candidates:
        seen = sorted(
            {
                str(version.get("license") or "unknown")
                for version in available
                if version.get("language") == "he"
            }
        )
        raise ValueError(f"no allowlisted Hebrew version; observed licenses={seen}")
    candidates.sort(
        key=lambda version: (
            _LICENSE_RANK.get(_license_key(version.get("license")), 99),
            not bool(version.get("isPrimary")),
            str(version.get("versionTitle") or ""),
        )
    )
    return candidates[0]


def _pick_hebrew_version_for_container(
    available: list[dict], allowlist: set[str], *, has_ruling: bool
) -> dict:
    """Select a child's best Hebrew version for a container acquisition.

    Without a ``license_ruling`` this is exactly ``_pick_open_hebrew_version``:
    only allowlisted licenses are acquirable. A ``license_ruling`` widens
    acceptance to ANY reported, KNOWN license (e.g. a non-allowlisted CC-BY-SA
    on one child of an otherwise Public Domain container) -- the ruling
    reinterprets what a child reports, it never manufactures a report where
    the provider gave none. An absent/unknown license is refused either way.
    """
    if not has_ruling:
        return _pick_open_hebrew_version(available, allowlist)
    candidates = [
        version
        for version in available
        if version.get("language") == "he"
        and _license_key(version.get("license")) not in ("", "unknown")
    ]
    if not candidates:
        seen = sorted(
            {
                str(version.get("license") or "unknown")
                for version in available
                if version.get("language") == "he"
            }
        )
        raise ValueError(
            f"no Hebrew version reports a known license; observed licenses={seen}"
        )
    candidates.sort(
        key=lambda version: (
            _LICENSE_RANK.get(_license_key(version.get("license")), 50),
            not bool(version.get("isPrimary")),
            str(version.get("versionTitle") or ""),
        )
    )
    return candidates[0]


def _check_frozen_children_against_toc(
    children_cfg: list[dict], live_refs: list[str]
) -> None:
    """Verify the frozen ordered child list against a live Sefaria ToC listing.

    The child list in the source map is FROZEN (C7): live ToC discovery may
    VERIFY membership and order but never silently redefines them. A mismatch
    -- a missing member, an extra member, or a reordering -- is a hard fetch
    error surfaced for a source-map edit, never a silent redefinition.
    """
    frozen_refs = [child["source_ref"] for child in children_cfg]
    if frozen_refs == live_refs:
        return
    frozen_set = set(frozen_refs)
    live_set = set(live_refs)
    missing = [ref for ref in frozen_refs if ref not in live_set]
    extra = [ref for ref in live_refs if ref not in frozen_set]
    if missing or extra:
        raise ValueError(
            "frozen child list disagrees with the live Sefaria ToC membership: "
            f"missing={missing} extra={extra}"
        )
    raise ValueError(
        "frozen child list order disagrees with the live Sefaria ToC order: "
        f"frozen={frozen_refs} live={live_refs}"
    )


def _units_from_nested_text(text: Any, ref: str) -> list[dict]:
    if not isinstance(text, list):
        text = [text]
    units = []
    for ordinal, node in enumerate(text, start=1):
        cleaned = clean_hebrew(" ".join(flatten_text_node(node)))
        if cleaned:
            units.append(
                {
                    "ordinal": ordinal,
                    "label": f"{ref} {ordinal}",
                    "provider_ref": f"{ref} {ordinal}",
                    "text": cleaned,
                    "hebrew_letters": count_hebrew_letters(cleaned),
                }
            )
    return units


def _acquire_sefaria(
    fetcher: Fetcher, source: dict, allowlist: set[str]
) -> tuple[dict, list[Path]]:
    key = source["key"]
    ref = source["source_ref"]
    raw_paths: list[Path] = []
    if source.get("mode") == "schema_leaves":
        index_path = fetcher.raw_dir / key / "index.json"
        index = fetcher.sefaria_index(ref, index_path)
        raw_paths.append(index_path)
        schema = index.get("schema")
        if not isinstance(schema, dict):
            raise ValueError("Sefaria index response has no schema")
        leaves = _schema_leaf_refs(ref, schema)
        if not leaves:
            raise ValueError("Sefaria schema contains no text leaves")
        probe_path = fetcher.raw_dir / key / "leaf-0000-all.json"
        probe = fetcher.sefaria_text(leaves[0][0], "all", probe_path)
        raw_paths.append(probe_path)
        if probe.get("error"):
            raise ValueError(f"Sefaria leaf metadata error: {probe['error']}")
        best = _pick_open_hebrew_version(probe.get("available_versions") or [], allowlist)
        version_title = best.get("versionTitle")
        units = []
        for ordinal, (leaf_ref, label_en, label_he) in enumerate(leaves, start=1):
            leaf_path = fetcher.raw_dir / key / f"leaf-{ordinal:04d}.json"
            doc = fetcher.sefaria_text(
                leaf_ref, f"hebrew|{version_title}", leaf_path
            )
            raw_paths.append(leaf_path)
            if doc.get("error"):
                raise ValueError(f"Sefaria leaf fetch failed for {leaf_ref}: {doc['error']}")
            versions = doc.get("versions") or []
            text = versions[0].get("text") if versions else None
            cleaned = clean_hebrew(" ".join(flatten_text_node(text)))
            if cleaned:
                units.append(
                    {
                        "ordinal": ordinal,
                        "label": label_he or label_en,
                        "provider_ref": leaf_ref,
                        "text": cleaned,
                        "hebrew_letters": count_hebrew_letters(cleaned),
                    }
                )
    else:
        all_path = fetcher.raw_dir / key / "all.json"
        metadata = fetcher.sefaria_text(ref, "all", all_path)
        raw_paths.append(all_path)
        if metadata.get("error"):
            raise ValueError(f"Sefaria text metadata error: {metadata['error']}")
        best = _pick_open_hebrew_version(
            metadata.get("available_versions") or [], allowlist
        )
        version_title = best.get("versionTitle")
        text_path = fetcher.raw_dir / key / "selected.json"
        selected = fetcher.sefaria_text(
            ref, f"hebrew|{version_title}", text_path
        )
        raw_paths.append(text_path)
        if selected.get("error"):
            raise ValueError(f"Sefaria selected-version error: {selected['error']}")
        versions = selected.get("versions") or []
        if not versions:
            raise ValueError("Sefaria selected version response has no versions")
        units = _units_from_nested_text(versions[0].get("text"), ref)
    license_name = str(best.get("license") or "")
    license_key = _license_key(license_name)
    return (
        {
            "provider": "sefaria",
            "source_ref": ref,
            "source_url": f"{SEFARIA_BASE}/{urllib.parse.quote(ref.replace(' ', '_'))}",
            "version_title": version_title,
            "version_source": best.get("versionSource"),
            "license": license_name,
            "license_url": _LICENSE_URL.get(license_key),
            "attribution": (
                f'"{ref}", version "{version_title}", via Sefaria.org.'
            ),
            "units": units,
        },
        raw_paths,
    )


def _acquire_container_sefaria(
    fetcher: Fetcher,
    source: dict,
    allowlist: set[str],
    *,
    throttle_seconds: float = 1.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    live_children_refs: list[str] | None = None,
) -> tuple[dict, list[Path]]:
    """Acquire a multi-text container: an ordered list of independent Sefaria
    indices stitched into ONE combined normalized source (C7).

    Each child is fetched via the existing v3 pattern (metadata with
    version=all, pick a version, fetch the selected version's text) plus one
    index lookup for its Hebrew title. A failed child quarantines the WHOLE
    container -- never a partial one. Throttled at ``throttle_seconds`` between
    every network call.
    """
    key = source["key"]
    children_cfg = source["children"]
    license_ruling = source.get("license_ruling")
    if live_children_refs is not None:
        _check_frozen_children_against_toc(children_cfg, live_children_refs)

    raw_paths: list[Path] = []
    units: list[dict] = []
    child_manifest: list[dict] = []
    reported_licenses: set[str] = set()

    for position, child in enumerate(children_cfg, start=1):
        child_key = child["child_key"]
        ref = child["source_ref"]
        stem = f"{position:04d}-{child_key}"

        index_path = fetcher.raw_dir / key / f"{stem}-index.json"
        index_doc = fetcher.sefaria_index(ref, index_path)
        raw_paths.append(index_path)
        sleep_fn(throttle_seconds)
        he_title = _primary_title(index_doc, "he") or ref

        all_path = fetcher.raw_dir / key / f"{stem}-all.json"
        metadata = fetcher.sefaria_text(ref, "all", all_path)
        raw_paths.append(all_path)
        sleep_fn(throttle_seconds)
        if metadata.get("error"):
            raise ValueError(
                f"Sefaria text metadata error for child {child_key} ({ref}): "
                f"{metadata['error']}"
            )
        best = _pick_hebrew_version_for_container(
            metadata.get("available_versions") or [],
            allowlist,
            has_ruling=bool(license_ruling),
        )
        version_title = best.get("versionTitle")

        text_path = fetcher.raw_dir / key / f"{stem}-selected.json"
        selected = fetcher.sefaria_text(ref, f"hebrew|{version_title}", text_path)
        raw_paths.append(text_path)
        sleep_fn(throttle_seconds)
        if selected.get("error"):
            raise ValueError(
                f"Sefaria selected-version error for child {child_key} ({ref}): "
                f"{selected['error']}"
            )
        versions = selected.get("versions") or []
        if not versions:
            raise ValueError(
                f"Sefaria selected version response has no versions for "
                f"child {child_key} ({ref})"
            )
        cleaned = clean_hebrew(" ".join(flatten_text_node(versions[0].get("text"))))
        license_name = str(best.get("license") or "")
        license_key = _license_key(license_name)
        reported_licenses.add(license_key)

        units.append(
            {
                "ordinal": position,
                "label": he_title,
                "provider_ref": ref,
                "text": cleaned,
                "hebrew_letters": count_hebrew_letters(cleaned),
            }
        )
        child_manifest.append(
            {
                "child_key": child_key,
                "source_ref": ref,
                "he_title": he_title,
                "version_title": version_title,
                "version_source": best.get("versionSource"),
                "reported_license": license_name,
                "reported_license_url": _LICENSE_URL.get(license_key),
            }
        )

    if len(reported_licenses) > 1 and not license_ruling:
        raise ValueError(
            "container children report mixed licenses "
            f"({sorted(reported_licenses)}) and no license_ruling resolves them"
        )
    if license_ruling:
        effective_license = str(license_ruling["effective_license"])
    else:
        effective_license = child_manifest[0]["reported_license"]
    effective_license_key = _license_key(effective_license)

    _, offset_rows = _unit_offsets(units)
    offset_by_ordinal = {unit["ordinal"]: start for unit, start in offset_rows}
    for entry, unit in zip(child_manifest, units):
        start = offset_by_ordinal.get(unit["ordinal"])
        if start is None:
            # _unit_offsets silently drops a unit that folds to an empty
            # stream; a container child must never be silently absent instead.
            raise ValueError(
                f"child {entry['child_key']} ({entry['source_ref']}) produced "
                "no usable Hebrew text"
            )
        entry["offset_start"] = start
        entry["offset_end"] = start + len(compact_stream(unit["text"]))

    attribution = (
        str(license_ruling["note"])
        if license_ruling and license_ruling.get("note")
        else (
            f'Combined Sefaria container "{key}" '
            f"({len(child_manifest)} sections), via Sefaria.org."
        )
    )

    return (
        {
            "provider": "sefaria",
            "container": True,
            "children": child_manifest,
            "child_count": len(child_manifest),
            "license": effective_license,
            "license_url": _LICENSE_URL.get(effective_license_key),
            "license_ruling": license_ruling,
            "attribution": attribution,
            "units": units,
        },
        raw_paths,
    )


def _acquire_wikisource(fetcher: Fetcher, source: dict) -> tuple[dict, list[Path]]:
    key = source["key"]
    ref = source["source_ref"]
    main_path = fetcher.raw_dir / key / "page-0000.json"
    main = fetcher.wikisource_parse(ref, main_path)
    raw_paths = [main_path]
    if main.get("error"):
        raise ValueError(f"Wikisource parse error: {main['error']}")
    parsed = main.get("parse") or {}
    selected: list[tuple[int, str]]
    if source.get("link_prefix"):
        selected = select_chapter_links(
            parsed.get("links") or [],
            source["link_prefix"],
            exclude_pages=source.get("exclude_pages"),
        )
        if not selected:
            raise ValueError("Wikisource table of contents yielded no chapter links")
    else:
        selected = [(1, parsed.get("title") or ref)]
    units = []
    revisions = []
    missing_pages = []
    for position, (ordinal, page) in enumerate(selected, start=1):
        if position == 1 and page == (parsed.get("title") or ref) and len(selected) == 1:
            doc = main
        else:
            page_path = fetcher.raw_dir / key / f"page-{position:04d}.json"
            doc = fetcher.wikisource_parse(page, page_path)
            raw_paths.append(page_path)
        if doc.get("error"):
            missing_pages.append({"page": page, "error": doc["error"].get("code")})
            continue
        item = doc.get("parse") or {}
        cleaned = clean_hebrew(visible_text(item.get("text") or ""))
        if cleaned:
            units.append(
                {
                    "ordinal": ordinal,
                    "label": item.get("title") or page,
                    "provider_ref": item.get("title") or page,
                    "revision_id": item.get("revid"),
                    "text": cleaned,
                    "hebrew_letters": count_hebrew_letters(cleaned),
                }
            )
        revisions.append(
            {"page": item.get("title") or page, "revision_id": item.get("revid")}
        )
    if missing_pages and source.get("identity_mode") == IDENTITY_MODE_PUBLIC_FIRST:
        # C8 fail-closed completeness: the generic chapter-link Wikisource
        # path's ``coverage_status="partial"`` escape below is available to a
        # private_sibling source (unchanged, pre-existing behavior) but NOT
        # to a public_first source -- a public_first identity with silently
        # missing pages would misreport its own coverage, so this is a hard
        # error here instead of a recorded status.
        names = ", ".join(entry["page"] for entry in missing_pages)
        raise ValueError(
            f"public_first completeness gate failed for {key}: "
            f"{len(missing_pages)} page(s) missing "
            '(coverage_status="partial" is not available for public_first '
            f"sources -- C8): {names}"
        )
    canonical_title = parsed.get("title") or ref
    return (
        {
            "provider": "hewikisource",
            "source_ref": ref,
            "source_url": "https://he.wikisource.org/wiki/"
            + urllib.parse.quote(canonical_title.replace(" ", "_")),
            "version_title": "Hebrew Wikisource revision snapshot",
            "license": WIKISOURCE_LICENSE,
            "license_url": WIKISOURCE_LICENSE_URL,
            "attribution": f'"{canonical_title}", Hebrew Wikisource contributors.',
            "revisions": revisions,
            "coverage_status": "partial" if missing_pages else "complete",
            "missing_pages": missing_pages,
            "units": units,
        },
        raw_paths,
    )


def _acquire_wikisource_daf_pages(fetcher: Fetcher, source: dict) -> tuple[dict, list[Path]]:
    """Acquire a per-daf/amud Wikisource work (Zohar-class shape, C8).

    Every expected page is REQUESTED by enumeration (daf N in the declared
    range, amud in א/ב) -- an anomalous sibling page under the same prefix
    (e.g. the observed "א1"/"א2" זהר חדש suffixes) is simply never asked for,
    so it cannot contaminate this acquisition. But locus identity for each
    fetched page comes from PARSING that page's own returned title, never
    from the enumeration index: ``daf_bavli``'s Sefaria ordinal geometry does
    not transfer to Wikisource pagination, and a redirect or a mislabeled page
    could otherwise silently hand back text under the wrong (daf, amud). A
    disagreement between what was requested and what the fetched title parses
    as is therefore a hard error (the anti-drift gate), not a warning.

    Completeness is a HARD gate in this mode: every expected page must exist
    and yield non-empty cleaned text. The chapter-link Wikisource path's
    ``coverage_status="partial"`` escape is NOT available here -- C8 says so
    explicitly, because a public_first Zohar-class source with silently
    missing daf pages would misreport its own coverage.
    """
    key = source["key"]
    link_prefix = source["link_prefix"]
    daf_first, daf_last = source["daf_range"]
    raw_paths: list[Path] = []
    units: list[dict] = []
    revisions: list[dict] = []
    missing_pages: list[dict] = []
    position = 0
    for daf in range(daf_first, daf_last + 1):
        for amud_index, amud_letter in enumerate(("א", "ב")):
            position += 1
            amud = amud_index + 1
            expected_title = f"{link_prefix} {heb_numeral(daf)} {amud_letter}"
            page_path = fetcher.raw_dir / key / f"page-{position:04d}.json"
            doc = fetcher.wikisource_parse(expected_title, page_path)
            raw_paths.append(page_path)
            if doc.get("error"):
                missing_pages.append(
                    {"page": expected_title, "error": doc["error"].get("code")}
                )
                continue
            item = doc.get("parse") or {}
            fetched_title = item.get("title")
            if not fetched_title:
                # Falling back to expected_title here would make the
                # cross-check below compare the expectation against itself.
                raise ValueError(
                    f"daf-page parse response for {expected_title!r} carries "
                    "no title -- the anti-drift gate needs the page's own "
                    "fetched title, never the requested one"
                )
            parsed_daf, parsed_amud = parse_daf_page_title(fetched_title, link_prefix)
            if (parsed_daf, parsed_amud) != (daf, amud):
                raise ValueError(
                    "daf-page parse/enumeration mismatch for "
                    f"{key}: requested {expected_title!r} (daf={daf}, "
                    f"amud={amud}) but the fetched title {fetched_title!r} "
                    f"parses as (daf={parsed_daf}, amud={parsed_amud})"
                )
            cleaned = clean_hebrew(visible_text(item.get("text") or ""))
            if not cleaned:
                missing_pages.append({"page": expected_title, "error": "empty"})
                continue
            ordinal = 2 * (daf - daf_first) + amud_index + 1
            units.append(
                {
                    "ordinal": ordinal,
                    "label": daf_label_he(daf, amud),
                    "provider_ref": fetched_title,
                    "revision_id": item.get("revid"),
                    "text": cleaned,
                    "hebrew_letters": count_hebrew_letters(cleaned),
                }
            )
            revisions.append(
                {"page": fetched_title, "revision_id": item.get("revid")}
            )
    if missing_pages:
        names = ", ".join(entry["page"] for entry in missing_pages)
        raise ValueError(
            f"daf-page completeness gate failed for {key}: "
            f"{len(missing_pages)} page(s) missing or empty "
            '(coverage_status="partial" is not available in daf_pages mode '
            f"-- C8): {names}"
        )
    return (
        {
            "provider": "hewikisource",
            "source_ref": link_prefix,
            "source_url": "https://he.wikisource.org/wiki/"
            + urllib.parse.quote(link_prefix.replace(" ", "_")),
            "version_title": "Hebrew Wikisource revision snapshot",
            "license": WIKISOURCE_LICENSE,
            "license_url": WIKISOURCE_LICENSE_URL,
            "attribution": f'"{link_prefix}", Hebrew Wikisource contributors.',
            "revisions": revisions,
            "coverage_status": "complete",
            "missing_pages": [],
            "locus_grain": "daf",
            "daf_range": [daf_first, daf_last],
            "page_count": len(units),
            "units": units,
        },
        raw_paths,
    )


def _acquire_wikisource_page_clusters(fetcher: Fetcher, source: dict) -> tuple[dict, list[Path]]:
    """Acquire a multi-cluster hewikisource work (``page_clusters``, A3): a
    FROZEN ordered list of independent ToC-page/link_prefix pairs stitched
    into ONE combined acquisition, for works whose content spans several ToC
    pages (per-book Torah commentaries, per-tractate Talmud commentaries,
    sm"g's two commandment lists).

    Clusters are processed strictly in list order. Within each cluster this
    does EXACTLY what the existing single-ToC path does: parse the cluster's
    ``toc_page``, run ``select_chapter_links`` with the cluster's own
    ``link_prefix`` (and its own ``exclude_pages``, which already hard-errors
    on a duplicate ordinal or a stale exclusion -- see that function).
    Ordinals restart per cluster in the raw (per-cluster) selection, so each
    surviving unit is reassigned a GLOBAL sequential ordinal (1..N across
    every cluster, in fetch order) -- this keeps the combined stream ordered
    and its offsets contiguous. Labels stay each unit's own fetched page
    title (never the global ordinal or a synthesized "cluster N"), so
    section-grain citation labels remain meaningful per tractate/book (C8's
    parse-vs-enumerate spirit: never silently redefine the frozen cluster
    list from live ToC content).

    Missing pages are checked ACROSS ALL clusters combined, using the exact
    same rule as everywhere else: recorded (coverage_status="partial") for a
    private_sibling source, a HARD ERROR for a public_first source (C8) --
    a Zohar-class source spanning several ToC pages gets no extra leniency
    just because it has more than one ToC to fail on.
    """
    key = source["key"]
    clusters = source["page_clusters"]
    raw_paths: list[Path] = []
    units: list[dict] = []
    revisions: list[dict] = []
    missing_pages: list[dict] = []
    global_ordinal = 0
    for cluster_index, cluster in enumerate(clusters):
        toc_page = cluster["toc_page"]
        link_prefix = cluster["link_prefix"]
        toc_path = fetcher.raw_dir / key / f"cluster-{cluster_index:02d}-toc.json"
        toc_doc = fetcher.wikisource_parse(toc_page, toc_path)
        raw_paths.append(toc_path)
        if toc_doc.get("error"):
            raise ValueError(
                f"page_clusters toc_page fetch failed for {key} cluster "
                f"{cluster_index} ({toc_page!r}): {toc_doc['error']}"
            )
        parsed = toc_doc.get("parse") or {}
        selected = select_chapter_links(
            parsed.get("links") or [],
            link_prefix,
            exclude_pages=cluster.get("exclude_pages"),
        )
        if not selected:
            raise ValueError(
                f"page_clusters cluster {cluster_index} ({toc_page!r}, "
                f"link_prefix {link_prefix!r}) yielded no chapter links for {key}"
            )
        for _local_ordinal, page in selected:
            global_ordinal += 1
            page_path = fetcher.raw_dir / key / f"page-{global_ordinal:04d}.json"
            doc = fetcher.wikisource_parse(page, page_path)
            raw_paths.append(page_path)
            if doc.get("error"):
                missing_pages.append({"page": page, "error": doc["error"].get("code")})
                continue
            item = doc.get("parse") or {}
            cleaned = clean_hebrew(visible_text(item.get("text") or ""))
            if cleaned:
                units.append(
                    {
                        "ordinal": global_ordinal,
                        "label": item.get("title") or page,
                        "provider_ref": item.get("title") or page,
                        "revision_id": item.get("revid"),
                        "text": cleaned,
                        "hebrew_letters": count_hebrew_letters(cleaned),
                    }
                )
            revisions.append(
                {"page": item.get("title") or page, "revision_id": item.get("revid")}
            )
    if missing_pages and source.get("identity_mode") == IDENTITY_MODE_PUBLIC_FIRST:
        names = ", ".join(entry["page"] for entry in missing_pages)
        raise ValueError(
            f"public_first completeness gate failed for {key}: "
            f"{len(missing_pages)} page(s) missing across {len(clusters)} "
            'page_clusters (coverage_status="partial" is not available for '
            f"public_first sources -- C8): {names}"
        )
    display_ref = _source_display_ref(source)
    first_toc_page = clusters[0]["toc_page"]
    return (
        {
            "provider": "hewikisource",
            "source_ref": display_ref,
            "source_url": "https://he.wikisource.org/wiki/"
            + urllib.parse.quote(first_toc_page.replace(" ", "_")),
            "version_title": "Hebrew Wikisource revision snapshot",
            "license": WIKISOURCE_LICENSE,
            "license_url": WIKISOURCE_LICENSE_URL,
            "attribution": f'"{display_ref}", Hebrew Wikisource contributors.',
            "revisions": revisions,
            "coverage_status": "partial" if missing_pages else "complete",
            "missing_pages": missing_pages,
            "cluster_count": len(clusters),
            "units": units,
        },
        raw_paths,
    )


def _combined_raw_hash(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: str(item)):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(sha256_file(path)))
    return digest.hexdigest()


def _source_display_ref(source: dict) -> str:
    """Return a printable/manifest ref for plain, container, and daf_pages sources.

    A container has no single ``source_ref``; it has an ordered list of
    children, each with its own. A daf_pages source (C8) likewise has no
    ``source_ref`` -- it is named by its ``link_prefix`` and ``daf_range``.
    A page_clusters source (A3) has no ``source_ref`` either -- it is named
    by its first cluster's ``toc_page`` plus the cluster count.
    """
    if source.get("container"):
        children = source.get("children") or []
        return f"container/{len(children)} children"
    if source.get("mode") == "daf_pages":
        first, last = source["daf_range"]
        return f"{source['link_prefix']} [daf {first}-{last}]"
    if "page_clusters" in source:
        clusters = source["page_clusters"]
        return f"{clusters[0]['toc_page']} [{len(clusters)} clusters]"
    return source["source_ref"]


def run(args: argparse.Namespace) -> dict:
    config_path = Path(args.source_map)
    config = load_source_config(config_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    allowlist = {_license_key(value) for value in config["license_allowlist"]}
    minimum = int(config["minimum_hebrew_letters"])
    fetcher = Fetcher(
        output_dir,
        timeout=args.timeout,
        request_interval=getattr(args, "request_interval", 0.5),
    )
    entries = []
    for source in config["sources"]:
        display_ref = _source_display_ref(source)
        print(f"[{source['provider']}] {source['key']}: {display_ref}", flush=True)
        normalized_path = output_dir / "normalized" / f"{source['key']}.json"
        try:
            if args.reuse_existing and normalized_path.is_file():
                acquired = json.loads(normalized_path.read_text(encoding="utf-8"))
                # A public_first source has no "mappings" key at all (enforced
                # by load_source_config); both sides must then agree on
                # absent/None, never on a KeyError.
                if acquired.get("mappings") != source.get("mappings"):
                    raise ValueError("existing normalized source mapping drift")
                if acquired.get("provider") != source["provider"]:
                    raise ValueError("existing normalized source provider drift")
                if bool(acquired.get("container")) != bool(source.get("container")):
                    raise ValueError("existing normalized source container-flag drift")
                if ("page_clusters" in source) != (acquired.get("cluster_count") is not None):
                    raise ValueError("existing normalized source page_clusters-flag drift")
                if _license_key(acquired.get("license")) not in allowlist:
                    raise ValueError("existing normalized source license is not allowlisted")
                raw_paths = sorted((output_dir / "raw" / source["key"]).glob("*.json"))
                if not raw_paths:
                    raise ValueError("existing normalized source has no raw responses")
            elif source.get("container"):
                acquired, raw_paths = _acquire_container_sefaria(fetcher, source, allowlist)
            elif source.get("mode") == "daf_pages":
                acquired, raw_paths = _acquire_wikisource_daf_pages(fetcher, source)
            elif "page_clusters" in source:
                acquired, raw_paths = _acquire_wikisource_page_clusters(fetcher, source)
            elif source["provider"] == "sefaria":
                acquired, raw_paths = _acquire_sefaria(fetcher, source, allowlist)
            else:
                acquired, raw_paths = _acquire_wikisource(fetcher, source)
            total_letters = sum(unit["hebrew_letters"] for unit in acquired["units"])
            if total_letters < minimum:
                raise ValueError(
                    f"usable Hebrew text below minimum: {total_letters} < {minimum}"
                )
            normalized = {
                "schema_version": "discovery-v4-acquired-source-v1",
                "key": source["key"],
                **acquired,
                # public_first sources carry no "mappings" key at all
                # (load_source_config forbids it); ``.get`` keeps both sides
                # of the reuse-existing drift check above at None rather
                # than raising a KeyError here.
                "mappings": source.get("mappings"),
                "transformation": (
                    "HTML markup and non-content UI removed; Unicode decomposed; "
                    "combining marks, punctuation, digits, and non-Hebrew characters "
                    "removed; word boundaries retained; final-letter forms preserved."
                ),
            }
            stable_json_dump(normalized, normalized_path)
            entry = {
                "key": source["key"],
                "provider": source["provider"],
                "status": "acquired",
                "source_ref": source.get("source_ref"),
                "license": acquired["license"],
                "license_url": acquired["license_url"],
                "unit_count": len(acquired["units"]),
                "hebrew_letters": total_letters,
                "target_work_ids": [
                    mapping["target_work_id"]
                    for mapping in (source.get("mappings") or [])
                ],
                "raw_response_count": len(raw_paths),
                "raw_responses_sha256": _combined_raw_hash(raw_paths),
                "normalized_file": normalized_path.name,
                "normalized_sha256": sha256_file(normalized_path),
            }
            if source.get("container"):
                entry["container"] = True
                entry["child_count"] = acquired["child_count"]
                entry["children"] = acquired["children"]
                entry["license_ruling"] = acquired.get("license_ruling")
            if source.get("mode") == "daf_pages":
                # C8 manifest fields: daf_range, page count, per-page letters
                # total (the generic ``hebrew_letters``/``unit_count`` above
                # already carry the aggregate), and the NEW "daf" locus_grain
                # value (see the module-level note on downstream consumers).
                entry["mode"] = "daf_pages"
                entry["locus_grain"] = acquired["locus_grain"]
                entry["daf_range"] = acquired["daf_range"]
                entry["page_count"] = acquired["page_count"]
            if "page_clusters" in source:
                # A3 manifest fields: cluster count and the coverage status
                # this acquisition actually reached (the generic
                # ``hebrew_letters``/``unit_count`` above already carry the
                # combined-across-clusters aggregate).
                entry["page_clusters"] = True
                entry["cluster_count"] = acquired["cluster_count"]
                entry["coverage_status"] = acquired["coverage_status"]
            if source.get("identity_mode") == IDENTITY_MODE_PUBLIC_FIRST:
                # Carried through verbatim so discovery_v4_build_reference.py
                # can route a public_first acquisition without re-deriving
                # identity_mode/identity_key from the source map a second
                # time (discovery-v4.2 C5, producer side). Absent for every
                # private_sibling (or identity_mode-absent) source -- this
                # branch is additive-only.
                entry["identity_mode"] = IDENTITY_MODE_PUBLIC_FIRST
                entry["identity_key"] = source["identity_key"]
            entries.append(entry)
            print(
                f"  acquired {len(acquired['units'])} units / {total_letters:,} letters",
                flush=True,
            )
        except Exception as exc:  # fail closed per source; manifest remains complete
            if normalized_path.exists():
                normalized_path.unlink()
            quarantined_entry = {
                "key": source["key"],
                "provider": source["provider"],
                "status": "quarantined",
                "source_ref": source.get("source_ref"),
                "target_work_ids": [
                    mapping["target_work_id"]
                    for mapping in (source.get("mappings") or [])
                ],
                "reason": str(exc),
            }
            if source.get("identity_mode") == IDENTITY_MODE_PUBLIC_FIRST:
                quarantined_entry["identity_mode"] = IDENTITY_MODE_PUBLIC_FIRST
                quarantined_entry["identity_key"] = source["identity_key"]
            entries.append(quarantined_entry)
            print(f"  quarantined: {exc}", flush=True)
    manifest = {
        "schema_version": "discovery-v4-acquisition-manifest-v1",
        "source_map": str(config_path.resolve()),
        "source_map_sha256": sha256_file(config_path),
        "license_allowlist": config["license_allowlist"],
        "minimum_hebrew_letters": minimum,
        "entries": entries,
        "summary": {
            "configured_sources": len(entries),
            "acquired_sources": sum(entry["status"] == "acquired" for entry in entries),
            "quarantined_sources": sum(
                entry["status"] == "quarantined" for entry in entries
            ),
            "acquired_target_work_ids": sum(
                len(entry["target_work_ids"])
                for entry in entries
                if entry["status"] == "acquired"
            ),
            "acquired_hebrew_letters": sum(
                entry.get("hebrew_letters", 0)
                for entry in entries
                if entry["status"] == "acquired"
            ),
        },
    }
    manifest_path = output_dir / "manifest.json"
    stable_json_dump(manifest, manifest_path)
    print(json.dumps(manifest["summary"], ensure_ascii=False, indent=2))
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-map",
        default=str(Path(__file__).with_name("discovery_v4_sources.json")),
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.5,
        help="Politeness floor (seconds) between successive provider requests.",
    )
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Reuse hash-checked normalized/raw files and fetch only absent sources.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
