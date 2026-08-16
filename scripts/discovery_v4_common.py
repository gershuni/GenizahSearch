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


DEFAULT_REFERENCE_NAMESPACE = "REF4"
REFERENCE_NAMESPACE_RE = re.compile(r"REF[0-9]+")

# discovery-v4.2 C5, producer side. MIRRORS -- deliberately does not import
# -- ``discovery_track1_contract.IDENTITY_MODES`` exactly. This module has no
# import of scripts.discovery_track1_contract today, and importing it here
# would make this file's "dependency-free" character and its direct-
# invocation (``python scripts/...py``) fallback path fragile for no real
# gain; see scripts/discovery_public_first_identity.py's docstring for the
# established precedent of mirroring instead of importing across a module
# pair. Keep the two constants in lock-step if either changes.
IDENTITY_MODE_PRIVATE_SIBLING = "private_sibling"
IDENTITY_MODE_PUBLIC_FIRST = "public_first"
IDENTITY_MODES = (IDENTITY_MODE_PRIVATE_SIBLING, IDENTITY_MODE_PUBLIC_FIRST)
IDENTITY_KEY_RE = re.compile(r"pf-[0-9]{4}")

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


def _validate_exclude_pages(value: Any, label: str) -> None:
    """Shared shape validation for an ``exclude_pages`` list (A1): a
    non-empty list of unique, non-empty title strings. This only checks
    shape -- the "every excluded title must have been present in the live
    selection" guard (stale-exclusion protection) is a fetch-time concern,
    since it needs the live ToC's links, not just the source map."""
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} exclude_pages must be a non-empty list")
    seen: set[str] = set()
    for title in value:
        if not isinstance(title, str) or not title.strip():
            raise ValueError(f"{label} exclude_pages has an invalid title: {title!r}")
        if title in seen:
            raise ValueError(f"{label} exclude_pages has a duplicate title: {title!r}")
        seen.add(title)


def _validate_page_clusters(source: dict, key: str, page_clusters: Any) -> None:
    """Validate a ``page_clusters`` source (A3): a FROZEN ordered list of
    independent hewikisource ToC/link_prefix pairs stitched into ONE
    combined acquisition, for works whose content spans several ToC pages
    (per-book Torah commentaries, per-tractate Talmud commentaries, sm"g's
    two commandment lists). Mutually exclusive with the single-ToC fields
    it replaces; live-ToC verification/ordinal reassignment happens at
    fetch time and never redefines this frozen list (same C7/C8 discipline
    as the container and daf_pages modes)."""
    if source.get("provider") != "hewikisource":
        raise ValueError(f"source {key} page_clusters requires provider 'hewikisource'")
    for mutex_field in ("source_ref", "link_prefix", "mode", "container", "exclude_pages"):
        if mutex_field in source:
            raise ValueError(
                f"source {key} page_clusters is mutually exclusive with {mutex_field}"
            )
    if not isinstance(page_clusters, list) or not page_clusters:
        raise ValueError(f"source {key} page_clusters must be a non-empty list")
    allowed_cluster_keys = {"toc_page", "link_prefix", "exclude_pages"}
    for index, cluster in enumerate(page_clusters):
        if not isinstance(cluster, dict):
            raise ValueError(f"source {key} page_clusters[{index}] must be an object")
        extra = set(cluster) - allowed_cluster_keys
        if extra:
            raise ValueError(
                f"source {key} page_clusters[{index}] has unknown keys: {sorted(extra)}"
            )
        toc_page = cluster.get("toc_page")
        if not isinstance(toc_page, str) or not toc_page.strip():
            raise ValueError(
                f"source {key} page_clusters[{index}] has an invalid toc_page: {toc_page!r}"
            )
        link_prefix = cluster.get("link_prefix")
        if not isinstance(link_prefix, str) or not link_prefix.strip():
            raise ValueError(
                f"source {key} page_clusters[{index}] has an invalid link_prefix: "
                f"{link_prefix!r}"
            )
        if "exclude_pages" in cluster:
            _validate_exclude_pages(
                cluster["exclude_pages"], f"source {key} page_clusters[{index}]"
            )


def load_source_config(path: str | Path) -> dict:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("schema_version") != "discovery-v4-sources-v1":
        raise ValueError("unsupported or missing V4 source-map schema_version")
    namespace = doc.get("reference_namespace", DEFAULT_REFERENCE_NAMESPACE)
    if not isinstance(namespace, str) or not REFERENCE_NAMESPACE_RE.fullmatch(namespace):
        raise ValueError(f"invalid reference_namespace: {namespace!r}")
    sources = doc.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError("V4 source map must contain a non-empty sources list")
    seen_keys: set[str] = set()
    seen_work_ids: set[str] = set()
    seen_identity_keys: set[str] = set()
    for source in sources:
        key = source.get("key")
        if not isinstance(key, str) or not re.fullmatch(r"[a-z0-9_]+", key):
            raise ValueError(f"invalid source key: {key!r}")
        if key in seen_keys:
            raise ValueError(f"duplicate source key: {key}")
        seen_keys.add(key)

        # discovery-v4.2 C5, producer side: ``identity_mode`` is an OPTIONAL
        # per-source field, vocabulary {"private_sibling", "public_first"}.
        # Absent means "private_sibling" -- every pre-C5 map (with no such
        # field at all) keeps validating exactly as it did before this block
        # existed. A "public_first" source mints its opaque work id from the
        # C5 approval artifact alone (scripts/discovery_public_first_identity.py)
        # rather than from a private counterpart, so it carries an
        # ``identity_key`` (a ``pf-####``-shaped key into that artifact)
        # instead of a ``mappings`` list, and it can never be a container (a
        # container's whole point is stitching sources onto ONE PRIVATE
        # target). A "private_sibling" (or identity_mode-absent) source is
        # the mirror image: it must never carry an ``identity_key``.
        identity_mode = source.get("identity_mode")
        if identity_mode is not None and identity_mode not in IDENTITY_MODES:
            raise ValueError(
                f"source {key} has an invalid identity_mode: {identity_mode!r}"
            )
        is_public_first = identity_mode == IDENTITY_MODE_PUBLIC_FIRST
        identity_key = source.get("identity_key")
        if is_public_first:
            if not isinstance(identity_key, str) or not IDENTITY_KEY_RE.fullmatch(
                identity_key
            ):
                raise ValueError(
                    f"public_first source {key} has an invalid identity_key: "
                    f"{identity_key!r}"
                )
            if identity_key in seen_identity_keys:
                raise ValueError(f"duplicate identity_key: {identity_key}")
            seen_identity_keys.add(identity_key)
            if "mappings" in source:
                raise ValueError(
                    f"public_first source {key} must not carry mappings -- it "
                    "has no private target"
                )
            if source.get("container"):
                raise ValueError(f"public_first source {key} cannot be a container")
        elif identity_key is not None:
            raise ValueError(
                f"source {key} has identity_mode {identity_mode!r} but carries "
                f"an identity_key ({identity_key!r}); only public_first sources may"
            )

        # discovery-v4.2 A3: ``page_clusters`` is checked FIRST (ahead of the
        # container branch below) so a source that mistakenly carries both
        # gets the precise "mutually exclusive with container" message
        # rather than being swallowed by container validation. A source with
        # no ``page_clusters`` key is completely unaffected.
        page_clusters = source.get("page_clusters")
        if page_clusters is not None:
            _validate_page_clusters(source, key, page_clusters)
        elif "exclude_pages" in source:
            # discovery-v4.2 A1: only a hewikisource, non-container,
            # non-page_clusters ToC source may carry a top-level
            # exclude_pages (a page_clusters source carries it PER CLUSTER
            # instead -- already enforced as a mutual exclusion above).
            if source.get("provider") != "hewikisource" or source.get("container"):
                raise ValueError(
                    f"source {key} exclude_pages is only allowed on a "
                    "hewikisource ToC source"
                )
            _validate_exclude_pages(source["exclude_pages"], f"source {key}")

        if source.get("container"):
            # Multi-text container (discovery-v4.2 C7): a FROZEN ordered list of
            # independent Sefaria index refs stitched into one target work. The
            # ordered list itself is validated here; live-ToC membership/order
            # verification happens at fetch time and never redefines it.
            if source.get("provider") != "sefaria":
                raise ValueError(f"container source {key} must use provider 'sefaria'")
            children = source.get("children")
            if not isinstance(children, list) or not children:
                raise ValueError(
                    f"container source {key} must have a non-empty children list"
                )
            seen_child_keys: set[str] = set()
            for child in children:
                if not isinstance(child, dict):
                    raise ValueError(f"container source {key} has an invalid child entry")
                child_key = child.get("child_key")
                if not isinstance(child_key, str) or not re.fullmatch(
                    r"[a-z0-9_]+", child_key
                ):
                    raise ValueError(
                        f"container source {key} has an invalid child_key: {child_key!r}"
                    )
                if child_key in seen_child_keys:
                    raise ValueError(
                        f"container source {key} has a duplicate child_key: {child_key}"
                    )
                seen_child_keys.add(child_key)
                child_ref = child.get("source_ref")
                if not isinstance(child_ref, str) or not child_ref.strip():
                    raise ValueError(
                        f"container source {key} child {child_key!r} has an "
                        "invalid source_ref"
                    )
            license_ruling = source.get("license_ruling")
            if license_ruling is not None:
                if not isinstance(license_ruling, dict):
                    raise ValueError(
                        f"container source {key} has an invalid license_ruling"
                    )
                effective_license = license_ruling.get("effective_license")
                if not isinstance(effective_license, str) or not effective_license.strip():
                    raise ValueError(
                        f"container source {key} license_ruling is missing "
                        "effective_license"
                    )
        elif source.get("provider") not in {"sefaria", "hewikisource"}:
            raise ValueError(f"invalid provider for {key}")
        mode = source.get("mode")
        if mode is not None:
            # "schema_leaves" is the PRE-EXISTING Sefaria mode (V4/V4.1's
            # midrash_tanchuma_buber et al.; consumed at fetch time by
            # ``_acquire_sefaria``) -- it was never schema-validated before
            # this block existed, and it still isn't beyond appearing in the
            # vocabulary, so every current map keeps validating exactly as it
            # did. "daf_pages" is the new mode this block actually validates
            # (discovery-v4.2 C8, the per-daf/amud Zohar-class source shape).
            # A source map with no "mode" key at all (still most sources)
            # never enters this block, so it is unaffected either way.
            if source.get("container"):
                raise ValueError(f"container source {key} cannot also declare a mode")
            if mode not in {"schema_leaves", "daf_pages"}:
                raise ValueError(f"source {key} has an unsupported mode: {mode!r}")
            if mode == "daf_pages":
                if source.get("provider") != "hewikisource":
                    raise ValueError(
                        f"source {key} mode {mode!r} requires provider 'hewikisource'"
                    )
                link_prefix = source.get("link_prefix")
                if not isinstance(link_prefix, str) or not link_prefix.strip():
                    raise ValueError(
                        f"source {key} daf_pages mode requires a non-empty link_prefix"
                    )
                daf_range = source.get("daf_range")
                if (
                    not isinstance(daf_range, list)
                    or len(daf_range) != 2
                    or not all(
                        isinstance(value, int) and not isinstance(value, bool)
                        for value in daf_range
                    )
                ):
                    raise ValueError(
                        f"source {key} daf_pages mode requires a 2-integer daf_range"
                    )
                first, last = daf_range
                if first < 1 or last < first or last > 999:
                    raise ValueError(
                        f"source {key} has an insane daf_range: {daf_range!r}"
                    )
        if is_public_first:
            # No private target, no mappings to validate (already confirmed
            # absent above).
            continue
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


def reference_namespace(config: dict) -> str:
    """Return the raw-id namespace this source map appends under.

    V4 predates the field, so an absent value means ``REF4``.  A map that
    declares one is authoritative: the namespace is a property of the reviewed
    source set, not an operator's command-line choice.
    """
    return str(config.get("reference_namespace", DEFAULT_REFERENCE_NAMESPACE))


def raw_id_prefix(namespace: str) -> str:
    if not REFERENCE_NAMESPACE_RE.fullmatch(namespace or ""):
        raise ValueError(f"invalid reference namespace: {namespace!r}")
    return f"{namespace}:"


def resolve_namespace(config: dict, requested: str | None) -> str:
    """Reconcile a CLI ``--reference-namespace`` with the map's declaration.

    Passing one is optional; passing a different one is an error rather than an
    override, so a V4 map can never be run through a REF5 build (or the
    reverse) by mistyping a flag.
    """
    declared = reference_namespace(config)
    if requested and requested != declared:
        raise ValueError(
            f"--reference-namespace {requested} disagrees with the source map's {declared}"
        )
    raw_id_prefix(declared)
    return declared


def source_target_ids(config: dict) -> set[str]:
    # public_first sources have no private target and no mappings list at
    # all -- skipped rather than iterated (discovery-v4.2 C5).
    return {
        mapping["target_work_id"]
        for source in config["sources"]
        if source.get("identity_mode") != IDENTITY_MODE_PUBLIC_FIRST
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
