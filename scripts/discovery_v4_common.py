"""Shared helpers for the Discovery V4 public-reference expansion.

The V4 pipeline deliberately keeps acquired text and generated artifacts out of
the repository.  Only the deterministic transformations and the curated source
map are tracked.  This module contains the small, dependency-free primitives
shared by the audit, acquisition, and reference-build stages.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Iterator


HEBREW_LETTER_RE = re.compile(r"[\u05d0-\u05ea]")
FINAL_LETTER_FOLD = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})
_TITLE_PUNCT_RE = re.compile(r"[\"'׳״“”„‘’`´]+")
_TITLE_OTHER_RE = re.compile(r"[^\u05d0-\u05eaA-Za-z0-9]+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_json_dump(value: Any, path: str | Path) -> None:
    Path(path).write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def normalize_title(value: str | None) -> str:
    """Return a deliberately conservative Hebrew/Latin title comparison key."""
    text = unicodedata.normalize("NFKC", value or "").replace("_", " ")
    text = "".join(
        char
        for char in text
        if unicodedata.category(char) not in {"Mn", "Cf"}
    )
    text = _TITLE_PUNCT_RE.sub("", text)
    text = _TITLE_OTHER_RE.sub(" ", text)
    return " ".join(text.split()).casefold()


def clean_hebrew(value: str | None) -> str:
    """Normalize acquired text to the matcher stream alphabet.

    This mirrors the established REF-2 convention: Hebrew letters survive,
    maqaf becomes a boundary, and every other character is a boundary.  Final
    letters are intentionally preserved; the downstream matcher owns any
    folding it performs.
    """
    text = unicodedata.normalize("NFKD", value or "")
    chars: list[str] = []
    pending_space = False
    for char in text:
        if unicodedata.category(char) in {"Mn", "Cf"}:
            continue
        if HEBREW_LETTER_RE.fullmatch(char):
            if pending_space and chars:
                chars.append(" ")
            chars.append(char)
            pending_space = False
        else:
            pending_space = True
    return "".join(chars).strip()


def compact_stream(value: str) -> str:
    """Return the established matcher stream: space-free, finals folded."""
    return value.replace(" ", "").translate(FINAL_LETTER_FOLD)


def count_hebrew_letters(value: str) -> int:
    return len(HEBREW_LETTER_RE.findall(value))


def flatten_text_node(value: Any) -> Iterator[str]:
    """Yield textual leaves from an arbitrarily nested Sefaria text node."""
    if isinstance(value, str):
        if value.strip():
            yield value
        return
    if isinstance(value, list):
        for child in value:
            yield from flatten_text_node(child)


def iter_sefaria_titles(value: Any, path: tuple[str, ...] = ()) -> Iterator[dict]:
    """Flatten a Sefaria table-of-contents response into title-bearing nodes."""
    if isinstance(value, list):
        for child in value:
            yield from iter_sefaria_titles(child, path)
        return
    if not isinstance(value, dict):
        return
    category = value.get("category") or value.get("heCategory")
    child_path = path + ((str(category),) if category else ())
    if value.get("title") or value.get("heTitle"):
        yield {
            "title": value.get("title"),
            "he_title": value.get("heTitle"),
            "categories": list(value.get("categories") or []),
            "toc_path": list(child_path),
        }
    for child in value.get("contents") or []:
        yield from iter_sefaria_titles(child, child_path)


def load_source_config(path: str | Path) -> dict:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("schema_version") != "discovery-v4-sources-v1":
        raise ValueError("unsupported or missing V4 source-map schema_version")
    sources = doc.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError("V4 source map must contain a non-empty sources list")
    seen_keys: set[str] = set()
    seen_work_ids: set[str] = set()
    for source in sources:
        key = source.get("key")
        if not isinstance(key, str) or not re.fullmatch(r"[a-z0-9_]+", key):
            raise ValueError(f"invalid source key: {key!r}")
        if key in seen_keys:
            raise ValueError(f"duplicate source key: {key}")
        seen_keys.add(key)
        if source.get("provider") not in {"sefaria", "hewikisource"}:
            raise ValueError(f"invalid provider for {key}")
        mappings = source.get("mappings")
        if not isinstance(mappings, list) or not mappings:
            raise ValueError(f"source {key} must have mappings")
        for mapping in mappings:
            work_id = mapping.get("target_work_id")
            if not isinstance(work_id, str) or not re.fullmatch(r"w[0-9]{6}", work_id):
                raise ValueError(f"source {key} has invalid target_work_id")
            if work_id in seen_work_ids:
                raise ValueError(f"target work appears more than once: {work_id}")
            seen_work_ids.add(work_id)
    return doc


def source_target_ids(config: dict) -> set[str]:
    return {
        mapping["target_work_id"]
        for source in config["sources"]
        for mapping in source["mappings"]
    }


def require_hash(path: str | Path, expected: str, label: str) -> None:
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(
            f"{label} SHA-256 mismatch: expected {expected}, got {actual}"
        )


def strip_html_tags(value: str) -> str:
    return _HTML_TAG_RE.sub(" ", value)


def merge_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    ordered = sorted((start, end) for start, end in ranges if end > start)
    merged: list[tuple[int, int]] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged
