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
import time
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable

import requests

try:
    from scripts.discovery_v4_common import (
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


def select_chapter_links(links: list[dict], prefix: str) -> list[tuple[int, str]]:
    selected = []
    for link in links:
        if link.get("ns") != 0 or link.get("missing"):
            continue
        title = link.get("title") or ""
        if not title.startswith(prefix):
            continue
        ordinal = hebrew_numeral(title[len(prefix) :])
        if ordinal is not None:
            selected.append((ordinal, title))
    selected.sort(key=lambda item: (item[0], item[1]))
    ordinals = [item[0] for item in selected]
    if len(ordinals) != len(set(ordinals)):
        raise ValueError(f"chapter link prefix produces duplicate ordinals: {prefix}")
    return selected


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
    def __init__(self, output_dir: Path, *, timeout: int = 60) -> None:
        self.output_dir = output_dir
        self.raw_dir = output_dir / "raw"
        self.normalized_dir = output_dir / "normalized"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.normalized_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def get_json(self, url: str, *, params: dict | None, raw_path: Path) -> dict:
        error: Exception | None = None
        for attempt in range(4):
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout, allow_redirects=True
                )
                response.raise_for_status()
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(response.content)
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                error = exc
                if attempt < 3:
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
            parsed.get("links") or [], source["link_prefix"]
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


def _combined_raw_hash(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: str(item)):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(sha256_file(path)))
    return digest.hexdigest()


def _source_display_ref(source: dict) -> str:
    """Return a printable/manifest ref for both plain and container sources.

    A container has no single ``source_ref``; it has an ordered list of
    children, each with its own.
    """
    if source.get("container"):
        children = source.get("children") or []
        return f"container/{len(children)} children"
    return source["source_ref"]


def run(args: argparse.Namespace) -> dict:
    config_path = Path(args.source_map)
    config = load_source_config(config_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    allowlist = {_license_key(value) for value in config["license_allowlist"]}
    minimum = int(config["minimum_hebrew_letters"])
    fetcher = Fetcher(output_dir, timeout=args.timeout)
    entries = []
    for source in config["sources"]:
        display_ref = _source_display_ref(source)
        print(f"[{source['provider']}] {source['key']}: {display_ref}", flush=True)
        normalized_path = output_dir / "normalized" / f"{source['key']}.json"
        try:
            if args.reuse_existing and normalized_path.is_file():
                acquired = json.loads(normalized_path.read_text(encoding="utf-8"))
                if acquired.get("mappings") != source["mappings"]:
                    raise ValueError("existing normalized source mapping drift")
                if acquired.get("provider") != source["provider"]:
                    raise ValueError("existing normalized source provider drift")
                if bool(acquired.get("container")) != bool(source.get("container")):
                    raise ValueError("existing normalized source container-flag drift")
                if _license_key(acquired.get("license")) not in allowlist:
                    raise ValueError("existing normalized source license is not allowlisted")
                raw_paths = sorted((output_dir / "raw" / source["key"]).glob("*.json"))
                if not raw_paths:
                    raise ValueError("existing normalized source has no raw responses")
            elif source.get("container"):
                acquired, raw_paths = _acquire_container_sefaria(fetcher, source, allowlist)
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
                "mappings": source["mappings"],
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
                    mapping["target_work_id"] for mapping in source["mappings"]
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
            entries.append(entry)
            print(
                f"  acquired {len(acquired['units'])} units / {total_letters:,} letters",
                flush=True,
            )
        except Exception as exc:  # fail closed per source; manifest remains complete
            if normalized_path.exists():
                normalized_path.unlink()
            entries.append(
                {
                    "key": source["key"],
                    "provider": source["provider"],
                    "status": "quarantined",
                    "source_ref": source.get("source_ref"),
                    "target_work_ids": [
                        mapping["target_work_id"] for mapping in source["mappings"]
                    ],
                    "reason": str(exc),
                }
            )
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
        "--reuse-existing",
        action="store_true",
        help="Reuse hash-checked normalized/raw files and fetch only absent sources.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
