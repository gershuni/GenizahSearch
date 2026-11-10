#!/usr/bin/env python3
"""Which reference material may ORIGINATE an ordinary source identification.

A reference can match a fragment truthfully and still not identify it. The
clearest case is a performable order of prayer: every prayer book carries the
same text, prayer books are among the most numerous items in the manuscript
corpus, and the corpus already holds purpose-built liturgy references for that
material -- so a large compendium that embeds the liturgy wins those pages on
overlap alone and takes attribution from the reference built to hold it.
Measured before this artifact existed: one prayer-book compendium earned 6,869
live matches across 6,227 manuscripts, ~84% of them in its order-of-prayer
divisions, and it shadowed 6,948 rows -- more than it earned -- including 220
belonging to the dedicated sabbath-musaf Amidah reference.

Masking cannot fix that on its own. Masking blanks what a work QUOTES from an
attested authority, and the available liturgy authorities cover only the core
fixed prayers; the compendia carry whole services, festival rites and piyyutim
for which no authority exists, and liturgical text varies by rite, so one
rite's Amidah does not mask another's. Measured: adding liturgy authorities
removed 1,289 of that compendium's 6,869 rows and 2 of another work's 2,284.

So eligibility is a SEPARATE instrument from masking, and this artifact is its
published, versioned record: the rule, and every reference the rule reaches,
with offsets and rationale. Ineligible rows are PRESERVED -- they are kept out
of the ordinary identification band and out of the shadow competition, never
deleted, so the evidence remains auditable.

Offsets are positions in a reference work's letter stream, so they are only
meaningful against one corpus: the artifact pins its reference-corpus hash and
a caller that knows the corpus it is working with MUST pass it for checking.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

SCHEMA_VERSION = "discovery-identification-eligibility-v1"

_TOP_KEYS = frozenset(
    {
        "schema_version",
        "rule",
        "ruled_on",
        "reference_corpus_sha256",
        "entries",
        "content_hash",
    }
)
_ENTRY_KEYS = frozenset(
    {
        "raw_reference_id",
        "scope",
        "classification",
        "basis",
        "rationale",
        "divisions",
    }
)
_SCOPES = frozenset({"work", "divisions"})
_DIVISION_KEYS = frozenset({"label_he", "start_offset", "end_offset"})
_SHA256_LEN = 64


class EligibilityError(ValueError):
    """Any defect in the artifact. Never a warning: an unreadable eligibility
    artifact must stop a promotion rather than silently promote everything."""


def content_hash_for_entries(entries: list) -> str:
    encoded = json.dumps(entries, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _check_divisions(raw_id: str, divisions: list) -> None:
    if not divisions:
        raise EligibilityError(f"{raw_id}: scope 'divisions' with no divisions listed")
    previous_end = 0
    for division in divisions:
        extra = set(division) - _DIVISION_KEYS
        if extra or set(division) != _DIVISION_KEYS:
            raise EligibilityError(f"{raw_id}: division key drift: {sorted(division)}")
        start, end = division["start_offset"], division["end_offset"]
        if not isinstance(start, int) or not isinstance(end, int):
            raise EligibilityError(f"{raw_id}: division offsets must be integers")
        if start < 0 or end <= start:
            raise EligibilityError(f"{raw_id}: empty or inverted division [{start}, {end})")
        if start < previous_end:
            raise EligibilityError(
                f"{raw_id}: divisions overlap or are unsorted at offset {start}"
            )
        if not str(division["label_he"]).strip():
            raise EligibilityError(f"{raw_id}: division is missing its label")
        previous_end = end


def load_eligibility_artifact(
    path: str | Path,
    *,
    sha256: str | None = None,
    reference_corpus_sha256: str | None = None,
) -> dict:
    artifact_path = Path(path)
    if not artifact_path.is_file():
        raise EligibilityError(f"eligibility artifact not found: {artifact_path}")
    raw = artifact_path.read_bytes()
    if sha256 is not None:
        actual = hashlib.sha256(raw).hexdigest()
        if actual != sha256:
            raise EligibilityError(
                f"eligibility artifact SHA-256 mismatch: expected {sha256}, got {actual}"
            )
    doc = json.loads(raw.decode("utf-8"))
    if doc.get("schema_version") != SCHEMA_VERSION:
        raise EligibilityError("unsupported eligibility artifact schema")
    if set(doc) != _TOP_KEYS:
        raise EligibilityError(f"top-level key drift: {sorted(doc)}")
    if not str(doc.get("rule", "")).strip():
        raise EligibilityError("the artifact must carry the rule it implements")
    corpus_sha = str(doc.get("reference_corpus_sha256", ""))
    if len(corpus_sha) != _SHA256_LEN or not all(c in "0123456789abcdef" for c in corpus_sha):
        raise EligibilityError("reference_corpus_sha256 must be a lowercase SHA-256")
    if reference_corpus_sha256 is not None and corpus_sha != reference_corpus_sha256:
        raise EligibilityError(
            "eligibility artifact was written against a different reference corpus "
            f"({corpus_sha} vs {reference_corpus_sha256}); its offsets do not transfer"
        )
    entries = doc.get("entries")
    if not isinstance(entries, list) or not entries:
        raise EligibilityError("eligibility artifact lists no entries")
    if doc.get("content_hash") != content_hash_for_entries(entries):
        raise EligibilityError("eligibility artifact content hash does not match its entries")

    by_work: dict[str, dict] = {}
    for entry in entries:
        if set(entry) - _ENTRY_KEYS:
            raise EligibilityError(f"entry key drift: {sorted(entry)}")
        raw_id = entry.get("raw_reference_id")
        if not raw_id:
            raise EligibilityError("entry is missing raw_reference_id")
        if raw_id in by_work:
            raise EligibilityError(f"{raw_id} appears twice")
        scope = entry.get("scope")
        if scope not in _SCOPES:
            raise EligibilityError(f"{raw_id}: unknown scope {scope!r}")
        for field in ("classification", "basis", "rationale"):
            if not str(entry.get(field, "")).strip():
                raise EligibilityError(f"{raw_id}: {field} must not be empty")
        if scope == "work":
            if entry.get("divisions"):
                raise EligibilityError(f"{raw_id}: scope 'work' must not list divisions")
            by_work[raw_id] = {"scope": scope, "ranges": None, "entry": entry}
        else:
            divisions = entry.get("divisions") or []
            _check_divisions(raw_id, divisions)
            by_work[raw_id] = {
                "scope": scope,
                "ranges": [(d["start_offset"], d["end_offset"]) for d in divisions],
                "entry": entry,
            }
    doc["by_work"] = by_work
    return doc


def widest_span(ref_spans_json: str | None) -> tuple[int, int] | None:
    """The reference-side span a row rests on: the widest one it reports."""
    if not ref_spans_json:
        return None
    spans = json.loads(ref_spans_json)
    if not spans:
        return None
    best = max(spans, key=lambda span: int(span["rg1"]) - int(span["rg0"]))
    return int(best["rg0"]), int(best["rg1"])


def ineligible_reason(
    doc: dict, work_id: str, ref_spans_json: str | None, *, threshold: float = 0.5
) -> str | None:
    """Why this row may not originate an identification, or None if it may.

    A division-scoped reference suppresses a row when the MAJORITY of the span
    it rests on lies inside an ineligible division -- not merely when it
    touches one. A match that starts in the prayer order and runs on into the
    surrounding halakhic prose is evidence about that prose, and belongs to the
    work.
    """
    record = doc["by_work"].get(work_id)
    if record is None:
        return None
    entry = record["entry"]
    if record["scope"] == "work":
        return f"{entry['classification']}: whole work ineligible"
    span = widest_span(ref_spans_json)
    if span is None:
        return None
    start, end = span
    width = max(1, end - start)
    covered = 0
    for range_start, range_end in record["ranges"]:
        if range_end <= start:
            continue
        if range_start >= end:
            break
        covered += min(range_end, end) - max(range_start, start)
    if covered / width <= threshold:
        return None
    label = next(
        (
            division["label_he"]
            for division in entry["divisions"]
            if division["start_offset"] < end and division["end_offset"] > start
        ),
        "",
    )
    return f"{entry['classification']}: {label}".strip().rstrip(":")
