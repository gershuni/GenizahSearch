#!/usr/bin/env python3
"""C5: the public-first identity approval artifact (discovery-v4.2 plan).

A hash-pinned, pre-match owner-approval artifact keyed by a stable
``identity_key`` (NEVER the provider title, NEVER a not-yet-minted opaque
work id). ``public_first`` raw ids -- V4.2 reference-corpus rows with no
private-work counterpart -- mint an opaque canonical work id ONLY from an
entry in this artifact, and ONLY when that entry's ``verdict`` is
``"approve"``. Reference metadata (Hebrew title, author, genre, domain
assignment) for a public_first identity comes EXCLUSIVELY from the matching
entry here; the raw provider title is evidence shown to the owner at
grading time, never a mutable identifier downstream.

Document shape::

    {
      "schema_version": "discovery-public-first-identities-v1",
      "ruled_on": "YYYY-MM-DD",
      "entries": [
        {
          "identity_key": "pf-0001",           # strict pf-#### syntax
          "title_he": "...",                   # Hebrew; non-empty if approved
          "author": "",                        # MAY be empty, any verdict
          "genre": "...",                      # non-empty if approved
          "domain_parent": "...",               # non-empty if approved
          "domain_leaf": "...",                 # non-empty if approved
          "provider": "sefaria" | "hewikisource" | "",
          "source_ref": "...",                  # non-empty if approved
          "license": "...",                     # non-empty if approved
          "verdict": "approve" | "reject" | "defer",
          "note": "..."
        },
        ...
      ],
      "content_hash": "sha256:<hex>"            # over the `entries` payload
    }

``content_hash`` MIRRORS -- deliberately does not import --
``scripts/discovery_v4_reconcile.py::curated_content_hash``'s exact
algorithm (``json.dumps(entries, sort_keys=True, ensure_ascii=False)`` then
``"sha256:" + sha256(...).hexdigest()``). The two modules cannot share the
helper via import: ``discovery_v4_reconcile.py`` is a CONSUMER of this
module's loader (it reads a ``--public-first-artifact`` built against this
schema), so importing the other way round would be circular. Keep the two
implementations in lock-step if either changes.

Masking gate (C11): this artifact is a TRACKED, reviewed handoff artifact
carrying only independently-stated public facts. No entry field may embed a
hash-shaped hex string (a restricted internal id/path signature) or the
``same_work_spike`` codename -- both are rejected unconditionally, for every
entry, regardless of verdict.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "discovery-public-first-identities-v1"

VERDICTS = ("approve", "reject", "defer")
PROVIDERS = ("sefaria", "hewikisource")

_IDENTITY_KEY_RE = re.compile(r"pf-[0-9]{4}")
_RULED_ON_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_HEBREW_LETTER_RE = re.compile(r"[א-ת]")

# Masking gate (C11): a restricted-artifact hex digest/id is 40+ hex chars
# (git SHA, sha256/sha1 hex, etc). ``same_work_spike`` is the M-source
# review-tooling codename; neither may appear in ANY field of ANY entry.
_HASHLIKE_RE = re.compile(r"[0-9a-f]{40,}")
_RESTRICTED_TOKEN = "same_work_spike"

_DOC_KEYS = frozenset({"schema_version", "ruled_on", "entries", "content_hash"})
_ENTRY_KEYS = frozenset(
    {
        "identity_key",
        "title_he",
        "author",
        "genre",
        "domain_parent",
        "domain_leaf",
        "provider",
        "source_ref",
        "license",
        "verdict",
        "note",
    }
)
# Required non-empty ONLY when the entry's verdict is "approve" (C5).
# "author" is deliberately excluded -- it may be empty at any verdict.
_REQUIRED_WHEN_APPROVED = (
    "title_he",
    "genre",
    "domain_parent",
    "domain_leaf",
    "provider",
    "source_ref",
    "license",
)


class PublicFirstIdentityError(ValueError):
    """Raised for any malformed, inconsistent, or masking-violating artifact."""


def _reject_duplicate_keys(pairs):
    """`object_pairs_hook` that hard-rejects a repeated JSON key at any
    nesting level (the bare stdlib decoder would silently last-write-wins)."""
    seen: dict = {}
    for key, value in pairs:
        if key in seen:
            raise PublicFirstIdentityError(
                f"duplicate JSON key {key!r} rejected (strict parse)"
            )
        seen[key] = value
    return seen


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_hash_for_entries(entries: list) -> str:
    """Return the artifact's content hash over its ``entries`` payload.

    See the module docstring: this MIRRORS
    ``scripts/discovery_v4_reconcile.py::curated_content_hash`` exactly and
    is intentionally a separate implementation (avoids a circular import).
    """
    encoded = json.dumps(entries, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _check_masking_gate(entry: dict) -> None:
    identity_key = entry.get("identity_key")
    for field, value in entry.items():
        if not isinstance(value, str):
            continue
        if _HASHLIKE_RE.search(value):
            raise PublicFirstIdentityError(
                f"entry {identity_key!r} field {field!r} contains a hash-shaped "
                "hex string -- masking gate rejects (C11)"
            )
        if _RESTRICTED_TOKEN in value:
            raise PublicFirstIdentityError(
                f"entry {identity_key!r} field {field!r} references a restricted "
                "source path -- masking gate rejects (C11)"
            )


def _validate_entry(entry: Any) -> None:
    if not isinstance(entry, dict):
        raise PublicFirstIdentityError("public-first entry must be a JSON object")
    keys = set(entry)
    if keys != _ENTRY_KEYS:
        missing = sorted(_ENTRY_KEYS - keys)
        unexpected = sorted(keys - _ENTRY_KEYS)
        raise PublicFirstIdentityError(
            f"public-first entry key drift: missing={missing} unexpected={unexpected}"
        )
    for field in _ENTRY_KEYS:
        if not isinstance(entry[field], str):
            raise PublicFirstIdentityError(
                f"public-first entry field {field!r} must be a string"
            )

    # Masking gate FIRST and unconditionally -- every entry, every verdict.
    _check_masking_gate(entry)

    identity_key = entry["identity_key"]
    if not _IDENTITY_KEY_RE.fullmatch(identity_key):
        raise PublicFirstIdentityError(f"invalid identity_key syntax: {identity_key!r}")

    verdict = entry["verdict"]
    if verdict not in VERDICTS:
        raise PublicFirstIdentityError(
            f"entry {identity_key!r} has an invalid verdict: {verdict!r}"
        )

    provider = entry["provider"]
    if provider and provider not in PROVIDERS:
        raise PublicFirstIdentityError(
            f"entry {identity_key!r} has an invalid provider: {provider!r}"
        )

    if verdict == "approve":
        for field in _REQUIRED_WHEN_APPROVED:
            if not entry[field].strip():
                raise PublicFirstIdentityError(
                    f"approved entry {identity_key!r} has an empty required "
                    f"field: {field!r}"
                )
        if not _HEBREW_LETTER_RE.search(entry["title_he"]):
            raise PublicFirstIdentityError(
                f"approved entry {identity_key!r} title_he must contain Hebrew letters"
            )


def load_public_first_artifact(path: str | Path, *, sha256: str | None = None) -> dict:
    """Load + validate the C5 public-first identity approval artifact.

    Order of enforcement: SHA-256 pin (if supplied) -> strict duplicate-key
    JSON parse -> closed top-level key set -> schema_version -> ``ruled_on``
    syntax -> per-entry closed key set + string typing -> masking gate (C11,
    every entry regardless of verdict) -> ``identity_key`` syntax ->
    uniqueness -> verdict vocabulary -> provider vocabulary -> approved-entry
    required-field + Hebrew-title check -> content-hash match.

    Returns
        ``{schema_version, ruled_on, entries, entries_by_key, content_hash,
        sha256}`` where ``entries_by_key`` maps ``identity_key ->  entry``
        for convenient lookup by callers (e.g. the V4 reconcile step).
    """
    p = Path(path)
    if not p.is_file():
        raise PublicFirstIdentityError(f"public-first artifact not found: {path}")
    actual_sha = _hash_file(p)
    if sha256 is not None and actual_sha != sha256:
        raise PublicFirstIdentityError(
            "public-first artifact SHA-256 pin mismatch -- refusing to load"
        )
    try:
        doc = json.loads(
            p.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys
        )
    except ValueError as e:
        raise PublicFirstIdentityError(f"public-first artifact parse error: {e}") from e
    if not isinstance(doc, dict):
        raise PublicFirstIdentityError(
            "public-first artifact top-level value must be a JSON object"
        )
    keys = set(doc)
    if keys != _DOC_KEYS:
        missing = sorted(_DOC_KEYS - keys)
        unexpected = sorted(keys - _DOC_KEYS)
        raise PublicFirstIdentityError(
            f"public-first artifact key drift: missing={missing} unexpected={unexpected}"
        )
    if doc.get("schema_version") != SCHEMA_VERSION:
        raise PublicFirstIdentityError("unsupported public-first artifact schema_version")
    ruled_on = doc.get("ruled_on")
    if not isinstance(ruled_on, str) or not _RULED_ON_RE.fullmatch(ruled_on):
        raise PublicFirstIdentityError(
            "public-first artifact ruled_on must be an ISO date (YYYY-MM-DD)"
        )
    entries = doc.get("entries")
    if not isinstance(entries, list) or not entries:
        raise PublicFirstIdentityError(
            "public-first artifact must have a non-empty entries list"
        )

    entries_by_key: dict[str, dict] = {}
    for entry in entries:
        _validate_entry(entry)
        identity_key = entry["identity_key"]
        if identity_key in entries_by_key:
            raise PublicFirstIdentityError(f"duplicate identity_key: {identity_key}")
        entries_by_key[identity_key] = entry

    expected_hash = content_hash_for_entries(entries)
    if doc.get("content_hash") != expected_hash:
        raise PublicFirstIdentityError(
            "public-first artifact content_hash does not match its entries"
        )

    return {
        "schema_version": doc["schema_version"],
        "ruled_on": ruled_on,
        "entries": entries,
        "entries_by_key": entries_by_key,
        "content_hash": doc["content_hash"],
        "sha256": actual_sha,
    }
