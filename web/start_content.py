"""Validated, data-driven content for the public ``/start`` launchpad.

The JSON is intentionally the only curation surface.  The renderer never
accepts paths from it: internal destinations are assembled here from typed,
validated fields, which keeps later editorial updates from becoming an open
redirect or script-injection route.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode


CONTENT_PATH = Path(__file__).with_name("start_content.v1.json")
ALLOWED_SEARCH_MODES = frozenset({"exact", "variants", "responsa"})
ALLOWED_DIFFICULTIES = frozenset({"simple", "advanced", "research"})
RESPONSA_FLAGS = frozenset({"variants", "ja", "flex_spaces", "bidirectional"})
_ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_SYS_ID_RE = re.compile(r"^99\d{10,}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_WORK_ID_RE = re.compile(r"^w\d{6}$")


class StartContentError(ValueError):
    """The curated launch content violates its versioned contract."""


def _fail(location: str, message: str) -> None:
    raise StartContentError(f"{location}: {message}")


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _fail(location, "must be an object")
    return value


def _list(value: Any, location: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(location, "must be an array")
    return value


def _nonempty_string(value: Any, location: str, *, max_length: int = 2000) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(location, "must be a non-empty string")
    if len(value) > max_length:
        _fail(location, f"must be at most {max_length} characters")
    return value


def _bilingual(value: Any, location: str) -> None:
    value = _mapping(value, location)
    if set(value) != {"en", "he"}:
        _fail(location, "must contain exactly 'en' and 'he'")
    _nonempty_string(value["en"], f"{location}.en")
    _nonempty_string(value["he"], f"{location}.he")


def _stable_id(value: Any, location: str, seen: set[str]) -> str:
    value = _nonempty_string(value, location, max_length=64)
    if not _ID_RE.fullmatch(value):
        _fail(location, "must be a lowercase kebab-case stable ID")
    if value in seen:
        _fail(location, f"duplicate stable ID {value!r}")
    seen.add(value)
    return value


def _sys_id(value: Any, location: str) -> str:
    value = _nonempty_string(value, location, max_length=32)
    if not _SYS_ID_RE.fullmatch(value):
        _fail(location, "must be a production-shaped manuscript sys_id")
    return value


def _positive_page(value: Any, location: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        _fail(location, "must be a positive integer")


def _editorial_entry(entry: Mapping[str, Any], location: str, seen: set[str]) -> None:
    _stable_id(entry.get("id"), f"{location}.id", seen)
    _bilingual(entry.get("title"), f"{location}.title")
    _bilingual(entry.get("description"), f"{location}.description")


def _validate_start_content(doc: Any) -> dict[str, Any]:
    doc = dict(_mapping(doc, "$"))
    if doc.get("schema_version") != 1:
        _fail("$.schema_version", "must equal 1")
    if doc.get("content_id") != "start-curated-v1":
        _fail("$.content_id", "must equal 'start-curated-v1'")
    if doc.get("status") != "curated_v1":
        _fail("$.status", "must equal 'curated_v1'")

    counts = _mapping(doc.get("featured_counts"), "$.featured_counts")
    expected_count_keys = {"searches", "manuscripts", "works", "computed_candidates"}
    if set(counts) != expected_count_keys:
        _fail("$.featured_counts", f"must contain exactly {sorted(expected_count_keys)!r}")
    for key, value in counts.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            _fail(f"$.featured_counts.{key}", "must be a positive integer")

    seen: set[str] = set()
    searches = _list(doc.get("searches"), "$.searches")
    if len(searches) != 12:
        _fail("$.searches", "launch v1 requires exactly 12 entries")
    difficulties: dict[str, int] = {key: 0 for key in ALLOWED_DIFFICULTIES}
    for index, raw in enumerate(searches):
        location = f"$.searches[{index}]"
        entry = _mapping(raw, location)
        _editorial_entry(entry, location, seen)
        difficulty = entry.get("difficulty")
        if difficulty not in ALLOWED_DIFFICULTIES:
            _fail(f"{location}.difficulty", f"must be one of {sorted(ALLOWED_DIFFICULTIES)!r}")
        difficulties[difficulty] += 1
        mode = entry.get("mode")
        if mode not in ALLOWED_SEARCH_MODES:
            _fail(f"{location}.mode", f"must be one of {sorted(ALLOWED_SEARCH_MODES)!r}")
        _nonempty_string(entry.get("query"), f"{location}.query", max_length=500)
        flags = entry.get("responsa_flags")
        if mode == "responsa":
            flags = _mapping(flags, f"{location}.responsa_flags")
            if set(flags) != RESPONSA_FLAGS or not all(isinstance(v, bool) for v in flags.values()):
                _fail(f"{location}.responsa_flags", f"must contain boolean flags {sorted(RESPONSA_FLAGS)!r}")
        elif flags is not None:
            _fail(f"{location}.responsa_flags", "is allowed only for responsa mode")
    if set(difficulties.values()) != {4}:
        _fail("$.searches", "must contain four simple, four advanced, and four research entries")

    manuscripts = _list(doc.get("manuscripts"), "$.manuscripts")
    if len(manuscripts) != 12:
        _fail("$.manuscripts", "launch v1 requires exactly 12 entries")
    for index, raw in enumerate(manuscripts):
        location = f"$.manuscripts[{index}]"
        entry = _mapping(raw, location)
        _editorial_entry(entry, location, seen)
        _bilingual(entry.get("category"), f"{location}.category")
        sys_id = _sys_id(entry.get("sys_id"), f"{location}.sys_id")
        _nonempty_string(entry.get("shelfmark"), f"{location}.shelfmark", max_length=200)
        _positive_page(entry.get("page"), f"{location}.page")
        expected_prefix = f"/api/nli_image_by_sysid/{sys_id}?"
        thumbnail = _nonempty_string(entry.get("thumbnail"), f"{location}.thumbnail", max_length=300)
        if not thumbnail.startswith(expected_prefix):
            _fail(f"{location}.thumbnail", "must use the internal image endpoint for the same sys_id")
        _bilingual(entry.get("alt"), f"{location}.alt")

    works = _list(doc.get("works"), "$.works")
    if len(works) != 6:
        _fail("$.works", "launch v1 requires exactly 6 entries")
    for index, raw in enumerate(works):
        location = f"$.works[{index}]"
        entry = _mapping(raw, location)
        _editorial_entry(entry, location, seen)
        value = _nonempty_string(entry.get("catalog_value"), f"{location}.catalog_value", max_length=16)
        if not value.isdecimal() or int(value) <= 0:
            _fail(f"{location}.catalog_value", "must be a positive structured catalogue title ID")

    candidates = _list(doc.get("computed_candidates"), "$.computed_candidates")
    if len(candidates) != 10:
        _fail("$.computed_candidates", "launch v1 requires exactly 10 entries")
    for index, raw in enumerate(candidates):
        location = f"$.computed_candidates[{index}]"
        entry = _mapping(raw, location)
        _editorial_entry(entry, location, seen)
        _bilingual(entry.get("category"), f"{location}.category")
        _nonempty_string(entry.get("shelfmark"), f"{location}.shelfmark", max_length=200)
        if not _HEX64_RE.fullmatch(str(entry.get("identification_id", ""))):
            _fail(f"{location}.identification_id", "must be a lowercase sha256 identifier")
        if not _WORK_ID_RE.fullmatch(str(entry.get("work_id", ""))):
            _fail(f"{location}.work_id", "must be a stable discovery work ID")
        _sys_id(entry.get("sys_id"), f"{location}.sys_id")
        _positive_page(entry.get("page"), f"{location}.page")
        hashes = _list(entry.get("frame_content_hashes"), f"{location}.frame_content_hashes")
        if not hashes or any(not isinstance(value, str) or not _HEX64_RE.fullmatch(value) for value in hashes):
            _fail(f"{location}.frame_content_hashes", "must be a non-empty list of lowercase sha256 hashes")
        if len(set(hashes)) != len(hashes):
            _fail(f"{location}.frame_content_hashes", "must not contain duplicates")

    puzzle = _mapping(doc.get("puzzle"), "$.puzzle")
    _editorial_entry(puzzle, "$.puzzle", seen)
    _nonempty_string(puzzle.get("document_id"), "$.puzzle.document_id", max_length=128)
    if not isinstance(puzzle.get("validated_public"), bool):
        _fail("$.puzzle.validated_public", "must be a boolean")

    demos = _mapping(doc.get("demos"), "$.demos")
    if set(demos) != {"parallels", "joins"}:
        _fail("$.demos", "must contain exactly 'parallels' and 'joins'")
    for name in ("parallels", "joins"):
        location = f"$.demos.{name}"
        demo = _mapping(demos[name], location)
        _editorial_entry(demo, location, seen)
        if not isinstance(demo.get("enabled"), bool):
            _fail(f"{location}.enabled", "must be a boolean")
    parallels = demos["parallels"]
    if parallels["enabled"]:
        _nonempty_string(parallels.get("text"), "$.demos.parallels.text", max_length=2000)
    elif not isinstance(parallels.get("text"), str):
        _fail("$.demos.parallels.text", "must be a string")
    _sys_id(demos["joins"].get("sys_id"), "$.demos.joins.sys_id")

    for key, collection in (("searches", searches), ("manuscripts", manuscripts), ("works", works), ("computed_candidates", candidates)):
        if counts[key] > len(collection):
            _fail(f"$.featured_counts.{key}", "cannot exceed the collection size")
    return doc


@lru_cache(maxsize=1)
def load_start_content(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate the versioned launch content once per process."""
    source = Path(path) if path is not None else CONTENT_PATH
    try:
        raw = source.read_text(encoding="utf-8")
        doc = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StartContentError(f"could not load {source.name}: {type(exc).__name__}") from exc
    return _validate_start_content(doc)


def localized(entry: Mapping[str, Any], field: str, lang: str) -> str:
    """Return one already-validated bilingual field without an English fallback leak."""
    selected = "he" if lang == "he" else "en"
    return str(entry[field][selected])


def search_url(entry: Mapping[str, Any]) -> str:
    params: list[tuple[str, str | int]] = [("q", str(entry["query"])), ("mode", str(entry["mode"]))]
    if entry["mode"] == "responsa":
        flags = entry["responsa_flags"]
        for key in ("variants", "ja", "flex_spaces", "bidirectional"):
            if flags[key]:
                params.append((key, 1))
    return "/search?" + urlencode(params)


def manuscript_url(entry: Mapping[str, Any], *, computed: bool = False) -> str:
    params: list[tuple[str, str | int]] = [("sys_id", str(entry["sys_id"])), ("page", int(entry["page"]))]
    if computed:
        params.append(("computed", 1))
    return "/browse?" + urlencode(params)


def work_url(entry: Mapping[str, Any]) -> str:
    return "/catalog-browse?" + urlencode({"work": str(entry["catalog_value"])})


def puzzle_url(entry: Mapping[str, Any]) -> str:
    if entry["validated_public"]:
        return "/puzzle?" + urlencode({"doc": str(entry["document_id"])})
    return "/puzzle"


def demo_url(name: str, entry: Mapping[str, Any]) -> str:
    if name == "parallels":
        return "/parallels?" + urlencode({"text": str(entry["text"])})
    if name == "joins":
        return "/joins-lab?" + urlencode({"sys_id": str(entry["sys_id"])})
    raise ValueError(f"unknown start demo {name!r}")


def live_computed_candidates(content: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return only candidates bound to the exact loaded discovery frame."""
    from web.discovery_assets import discovery_available, discovery_meta

    if not discovery_available():
        return []
    live_hash = discovery_meta("frame_content_hash")
    if not live_hash:
        return []
    return [
        entry for entry in content["computed_candidates"]
        if live_hash in entry["frame_content_hashes"]
    ]
