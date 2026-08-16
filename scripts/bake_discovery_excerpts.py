# -*- coding: utf-8 -*-
"""Bake text-vs-text excerpts into a PUBLIC discovery sidecar (excerpt-v1).

Runs AFTER project_discovery_public.py, on the public asset only, and writes a
new `discovery_excerpt` table: per identification, six PLAIN-TEXT pieces
({before, span, after} for the fragment side and for the reference-work side)
for the identification's best eligible witness-evidence row. The renderer
composes and escapes; no HTML ever enters the sidecar.

Why bake-time and not runtime: the offsets in `discovery_evidence` are
coordinates in normalized letter streams that only exist next to the bake
corpus (the HTR page snapshots and the reference editions). Baking excerpts
freezes exactly what the matcher saw and leaves the web app with zero
runtime normalization and zero reference-corpus dependency.

FRAME-HASH INVARIANCE: this script must never touch `discovery_claim` or
`discovery_evidence` (compute_frame_content_hash reads only those two), so a
baked asset keeps the certified frame hash byte-for-byte. The gate step runs
scripts/check_frame_regression.py on the (input, output) pair to prove it.

MASKED-CORPUS SAFETY (read before editing): ~70% of identifications were
matched against a reference edition whose text must never ship. For the
Tanakh subset (~93% of that mass) this script re-projects the span into the
PUBLIC-DOMAIN Sefaria Tanakh staged in refs_staging: the masked edition's
letter stream is used ONLY as an in-memory alignment query and is never
written to the output DB, a log line, an exception message, or a temp file.
Non-Tanakh masked works get NO work-side pieces (the UI shows an honest
"not available for display" state). The projection's own masking gate
(project_discovery_public.run_masking_gate) re-runs over the final artifact.

Work-side sources, by edition class (crosswalk ref-id prefix):
  REF4/REF5/REF6/...: normalized public-source snapshot from an ORDERED
         reference-manifest CHAIN (V4.2 plan C2, `--sources-bundle`), exact
         offsets. Each stage's reference manifest and acquisition manifest
         are hash-bound; the recomputed stream must equal the pickle stream.
         Recognized namespaces come from the bundle itself, never a
         hard-coded tuple.
  REF2:  refs_staging body file, exact offsets. The recomputed stream must
         EQUAL the pickle stream for that ref (prepped_for discipline) or the
         work side is dropped for that work -- never silently approximated.
  J:     per_doc raw file (whole file, header included -- matching the
         builder), exact offsets, same stream-equality assert.
  M:     re-projection (above) into the public targets named by
         `_REPROJECTION_KEY_PREFIXES` -- Tanakh (REF-3) plus Bavli / Mishnah /
         Yerushalmi / Tosefta (REF-4, 2026-08-13); everything else -> no work
         pane.

Usage (all inputs explicit -- no defaults for asset paths, per the
stage_cd_preview lesson that a defaulted path silently stages stale data):

  python scripts/bake_discovery_excerpts.py <public.db> --out <baked.db> \
      --crosswalk <crosswalk.json> --refs-staging <dir> --ja-dir <dir> \
      --fullcorpus <fullcorpus_v2.db> --ref-pkl <ref_corpus_v2.pkl> \
      --sources-bundle <excerpt_sources_bundle.json> \
      --base-reference-sha256 <hex> \
      [--ctx 90] [--span-cap 600] [--min-align-score 65] [--limit N]

`--sources-bundle` is the ORDERED (REF4->REF5->REF6->...) chain-order
`discovery-excerpt-sources-bundle-v1` JSON (V4.2 plan C2): every stage names
its reference manifest, acquisition manifest, and normalized-source
directory as explicit hash-pinned inputs. This script never dereferences a
reference manifest's own recorded `acquisition_manifest` path string -- that
recorded-absolute-path dereference was an open P2 defect (a depot move left
it pointing at a deleted `_tmp/` path). `--base-reference-sha256` anchors
chain-continuity for the bundle's first stage against the pinned base V2
reference corpus; every later stage's `base_reference_sha256` must equal the
previous stage's `reference_corpus_sha256`, or the bake hard-errors naming
both stages.

`--limit` exists for a fast dev smoke ONLY; every gate runs on a full bake.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import pickle
import re
import shutil
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts"))

from shared.discovery_locus import norm_stream  # noqa: E402

try:  # package-style import (pytest rootdir on sys.path)
    from scripts.discovery_track1_contract import _NAMESPACE_RE as _REF_NAMESPACE_RE
except ModuleNotFoundError:  # direct ``python scripts/...py`` invocation
    from discovery_track1_contract import _NAMESPACE_RE as _REF_NAMESPACE_RE

EXCERPT_SCHEMA_VERSION = "excerpt-v1"

#: Schema version of the ordered `--sources-bundle` JSON (V4.2 plan C2). One
#: bundle names every reference-manifest-chain stage (REF4->REF5->REF6->...)
#: whose public-source texts this bake dereferences by EXPLICIT path -- the
#: fix for the P2 defect where a reference manifest's own recorded
#: `acquisition_manifest` path went dead across a depot move.
EXCERPT_SOURCES_BUNDLE_SCHEMA_VERSION = "discovery-excerpt-sources-bundle-v1"

#: Required keys of one bundle stage, in the schema the plan specifies.
_BUNDLE_STAGE_KEYS = (
    "namespace",
    "reference_manifest",
    "reference_manifest_sha256",
    "acquisition_manifest",
    "normalized_dir",
)

_DDL = """
CREATE TABLE discovery_excerpt (
    identification_id TEXT PRIMARY KEY,
    evidence_id       TEXT NOT NULL,
    a_page_id         TEXT NOT NULL,
    frag_before       TEXT NOT NULL,
    frag_span         TEXT NOT NULL,
    frag_after        TEXT NOT NULL,
    frag_clipped      INTEGER NOT NULL,
    work_before       TEXT,
    work_span         TEXT,
    work_after        TEXT,
    work_clipped      INTEGER,
    work_source       TEXT,
    align_score       REAL,
    attribution       TEXT,
    n_spans           INTEGER,
    text_layer        TEXT,
    frag_hl           TEXT,
    work_hl           TEXT,
    work_markup       TEXT
)
"""

# The ordinary best-row rule remains highest matched_letters, ties broken by
# evidence_id ascending.  One excerpt-only exception is admitted: when the
# public identification's eligible row has no work offsets, a PUBLIC direct
# witness for that SAME DISPLAY WORK may supply the comparison slice even when
# its routing decision is review_only.  That row does not create an
# identification, change a claim, or reach a findings query; it is read only by
# this post-projection text bake.  Eligible offset-bearing evidence always wins
# over the fallback.
_BEST_ROWS_SQL = """
WITH eligible_ids AS MATERIALIZED (
    SELECT DISTINCT di.identification_id
      FROM discovery_identification di
      JOIN discovery_evidence de ON de.sys_id = di.sys_id
      JOIN discovery_claim dc ON dc.claim_id = de.claim_id
      JOIN works canonical_work
        ON canonical_work.work_id = dc.work_id
       AND canonical_work.canonical_work_id = di.canonical_work_id
     WHERE de.evidence_kind = 'witness'
       AND (de.routing_status = 'shipped'
            OR de.adjudication_status = 'human_confirmed')
)
SELECT di.identification_id, de.evidence_id, de.a_page_id,
       de.matched_letters, de.n_spans, de.text_layer,
       de.span_start, de.span_end, de.w_start, de.w_end,
       de.aligned_page_start, de.aligned_page_end, dc.work_id,
       de.evidence_source, de.routing_status, de.adjudication_status,
       de.assertion_visibility
  FROM discovery_identification di
  JOIN discovery_evidence de ON de.sys_id = di.sys_id
  JOIN discovery_claim dc ON dc.claim_id = de.claim_id
  JOIN works canonical_work
    ON canonical_work.work_id = dc.work_id
   AND canonical_work.canonical_work_id = di.canonical_work_id
  LEFT JOIN eligible_ids ON eligible_ids.identification_id = di.identification_id
 WHERE de.evidence_kind = 'witness'
   AND (de.routing_status = 'shipped'
        OR de.adjudication_status = 'human_confirmed'
        OR (dc.work_id = di.display_work_id
            AND de.routing_status = 'review_only'
            AND de.evidence_source = 'track1_direct'
            AND de.assertion_visibility = 'public'
            AND de.w_start IS NOT NULL
            AND de.w_end > de.w_start
            AND eligible_ids.identification_id IS NOT NULL))
"""


def _has_work_span(row: Mapping[str, object]) -> bool:
    """Whether an evidence candidate has a non-empty work-side interval."""
    start, end = row["w_start"], row["w_end"]
    return start is not None and end is not None and int(end) > int(start)


def is_excerpt_only_fallback(row: Mapping[str, object]) -> bool:
    """True only for the narrow public direct-text fallback described above."""
    return (
        row["routing_status"] == "review_only"
        and row["adjudication_status"] != "human_confirmed"
        and row["evidence_source"] == "track1_direct"
        and row["assertion_visibility"] == "public"
        and _has_work_span(row)
    )


def excerpt_candidate_key(row: Mapping[str, object]) -> Tuple[int, int, str]:
    """Prefer showable eligible evidence, then the excerpt-only fallback.

    The final rank retains the pre-existing deterministic best-row order for
    candidates of the same class.
    """
    eligible = (
        row["routing_status"] == "shipped"
        or row["adjudication_status"] == "human_confirmed"
    )
    rank = 0 if eligible and _has_work_span(row) else (
        1 if is_excerpt_only_fallback(row) else 2
    )
    return rank, -int(row["matched_letters"] or 0), str(row["evidence_id"])


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_bake_input_hashes(
    public_db: Path, crosswalk_path: Path, reference_pickle: Path
) -> Dict[str, str]:
    """Bind the excerpt inputs to the already-verified public sidecar.

    Excerpts dereference opaque work ids through the crosswalk and interpret
    work offsets through the reference pickle. Accepting a different file at
    either path can produce internally well-formed but wrongly labelled text.
    The sidecar already records both hashes, so omission is not a compatibility
    posture here: it is a missing provenance edge and fails closed.
    """
    with sqlite3.connect(f"file:{public_db.resolve().as_posix()}?mode=ro", uri=True) as conn:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    for key, path, label in (
        ("crosswalk_sha256", crosswalk_path, "crosswalk"),
        ("reference_corpus_sha256", reference_pickle, "reference pickle"),
    ):
        expected = meta.get(key)
        if not expected:
            raise ValueError(f"public sidecar does not record {key}")
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(
                f"{label} SHA-256 differs from the public sidecar's recorded input"
            )
    return meta


def pieces(nfc: str, offs, lo: int, hi: int, ctx: int,
           cap: int) -> Tuple[str, str, str, int]:
    """{before, span, after} in readable NFC text for stream span [lo, hi).

    `offs[i]` is the NFC index of stream letter i (shared.discovery_locus
    contract), so every slice below is on the NFC string. Spans longer than
    `cap` stream letters keep their first and last cap/2 letters joined by an
    ellipsis -- a capped span is FLAGGED, never silently shortened.
    """
    a0 = int(offs[lo])
    z0 = int(offs[hi - 1]) + 1
    before = nfc[max(0, a0 - ctx):a0]
    after = nfc[z0:z0 + ctx]
    if hi - lo > cap:
        half = cap // 2
        head_end = int(offs[lo + half - 1]) + 1
        tail_start = int(offs[hi - half])
        return before, nfc[a0:head_end] + " ⋯ " + nfc[tail_start:z0], after, 1
    return before, nfc[a0:z0], after, 0


def load_v4_public_sources(
    reference_manifest_path: Path,
    normalized_dir: Path,
    acquisition_manifest_path: Optional[Path] = None,
    *,
    expected_namespace: Optional[str] = None,
) -> Tuple[Dict[str, str], Dict[str, str], str]:
    """Reconstruct readable public-source texts under their pinned matcher streams.

    ``acquisition_manifest_path`` is the P2 fix (V4.2 plan C2): when given, it
    is the caller's OWN explicit path to the acquisition manifest -- verified
    by hash against the reference manifest's recorded
    ``acquisition_manifest_sha256``, but the manifest's own recorded
    ``acquisition_manifest`` path STRING is never read as a path. Omitting it
    falls back to that legacy dereference (kept only so a direct single-
    manifest call -- e.g. this repo's pinned unit test -- keeps working); the
    excerpt bake's ``main()`` always supplies the bundle's explicit path and
    never takes this fallback, which is how the depot-move P2 defect (a
    recorded absolute path going dead) is closed for the real bake.

    ``expected_namespace``, when given, is cross-checked against the
    manifest's own ``reference_namespace`` (absent means ``REF4``) and every
    entry's raw id must carry that namespace as its ``NS:`` prefix.
    """
    manifest = json.loads(reference_manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "discovery-v4-reference-manifest-v1":
        raise ValueError("unsupported V4 reference manifest")
    if expected_namespace is not None:
        actual_namespace = manifest.get("reference_namespace", "REF4")
        if actual_namespace != expected_namespace:
            raise ValueError(
                f"reference manifest namespace {actual_namespace!r} does not "
                f"match the sources bundle's declared namespace "
                f"{expected_namespace!r}"
            )
    if acquisition_manifest_path is None:
        acquisition_path = Path(manifest["acquisition_manifest"])
    else:
        acquisition_path = Path(acquisition_manifest_path)
    if sha256_file(acquisition_path) != manifest["acquisition_manifest_sha256"]:
        raise ValueError("V4 acquisition manifest hash differs from the reference manifest")
    acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
    acquired_by_key = {
        entry["key"]: entry
        for entry in acquisition["entries"]
        if entry.get("status") == "acquired"
    }
    texts: Dict[str, str] = {}
    attributions: Dict[str, str] = {}
    for entry in manifest["entries"]:
        raw_id = entry["raw_reference_id"]
        if expected_namespace is not None and not raw_id.startswith(
            f"{expected_namespace}:"
        ):
            raise ValueError(
                f"reference manifest entry {raw_id!r} does not carry the "
                f"{expected_namespace!r} namespace prefix declared by its "
                "bundle stage"
            )
        source_key = entry["source_key"]
        acquired = acquired_by_key.get(source_key)
        if acquired is None:
            raise ValueError("V4 reference entry has no acquired public-source row")
        path = normalized_dir / acquired["normalized_file"]
        if sha256_file(path) != acquired["normalized_sha256"]:
            raise ValueError("V4 normalized public-source hash mismatch")
        normalized = json.loads(path.read_text(encoding="utf-8"))
        units = {int(unit["ordinal"]): unit["text"] for unit in normalized["units"]}
        ordinals = [int(row["source_ordinal"]) for row in entry["unit_offsets"]]
        if len(ordinals) != len(set(ordinals)) or any(value not in units for value in ordinals):
            raise ValueError("V4 reference manifest names missing or duplicate source units")
        texts[raw_id] = "\n".join(units[value] for value in ordinals)
        attributions[raw_id] = normalized["attribution"]
    return texts, attributions, manifest["acquisition_manifest_sha256"]


def load_excerpt_sources_bundle(bundle_path: Path) -> Dict[str, object]:
    """Load + structurally validate the ordered `--sources-bundle` JSON.

    Schema ``discovery-excerpt-sources-bundle-v1``: ``{"stages": [...]}`` in
    CHAIN ORDER (REF4 -> REF5 -> REF6 -> ...). Each stage names one
    reference-manifest generation's four inputs. This function only checks
    shape; hash pins and chain continuity are verified while walking the
    stages in `load_public_sources_from_bundle`, where the error can name the
    stage(s) involved.
    """
    doc = json.loads(bundle_path.read_text(encoding="utf-8"))
    if doc.get("schema_version") != EXCERPT_SOURCES_BUNDLE_SCHEMA_VERSION:
        raise ValueError("unsupported excerpt sources bundle schema_version")
    stages = doc.get("stages")
    if not isinstance(stages, list):
        raise ValueError("excerpt sources bundle 'stages' must be a list")
    seen: set = set()
    checked_stages = []
    for stage in stages:
        if not isinstance(stage, dict):
            raise ValueError("excerpt sources bundle stage must be an object")
        namespace = stage.get("namespace")
        if not isinstance(namespace, str) or not _REF_NAMESPACE_RE.fullmatch(namespace):
            raise ValueError(
                f"excerpt sources bundle stage has invalid namespace: {namespace!r}"
            )
        if namespace in seen:
            raise ValueError(f"excerpt sources bundle has duplicate namespace: {namespace}")
        seen.add(namespace)
        missing = [key for key in _BUNDLE_STAGE_KEYS if key not in stage]
        if missing:
            raise ValueError(f"bundle stage {namespace} missing keys: {missing}")
        checked_stages.append(dict(stage))
    return {"schema_version": doc["schema_version"], "stages": checked_stages}


def load_public_sources_from_bundle(
    bundle: Dict[str, object], base_reference_sha256: str
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, dict]]:
    """Walk the ordered `--sources-bundle` stages (V4.2 plan C2), verifying:

    - the bundle's pinned ``reference_manifest_sha256`` against the actual
      file at ``reference_manifest``;
    - the stage's declared namespace against what the reference manifest
      itself records (absent means ``REF4``), and every entry's raw id
      against that same namespace (inside `load_v4_public_sources`);
    - the acquisition manifest's hash binding via the bundle's EXPLICIT
      ``acquisition_manifest`` path (never the manifest's own recorded
      string -- the P2 fix, inside `load_v4_public_sources`);
    - hash-CHAIN continuity: stage N's ``base_reference_sha256`` must equal
      stage N-1's ``reference_corpus_sha256`` (stage 0 against the caller's
      pinned ``base_reference_sha256``). A break is a hard error naming both
      stages (C12: manifest-chain discontinuity).

    Returns the merged ``(texts, attributions)`` maps keyed by raw reference
    id, plus a namespace -> stage-report dict (``reference_manifest_sha256``,
    ``acquisition_manifest_sha256``, ``reference_corpus_sha256``, ``raw_ids``)
    the caller uses for its own per-namespace pkl/crosswalk gates.
    """
    texts: Dict[str, str] = {}
    attributions: Dict[str, str] = {}
    stage_reports: Dict[str, dict] = {}
    expected_base = base_reference_sha256
    prev_label = "the pinned base reference corpus"
    for stage in bundle["stages"]:
        namespace = stage["namespace"]
        reference_manifest_path = Path(stage["reference_manifest"])
        actual_manifest_hash = sha256_file(reference_manifest_path)
        if actual_manifest_hash != stage["reference_manifest_sha256"]:
            raise ValueError(
                f"{namespace} reference manifest SHA-256 differs from the "
                "sources bundle's pin"
            )
        manifest = json.loads(reference_manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != "discovery-v4-reference-manifest-v1":
            raise ValueError(
                f"{namespace}: unsupported V4 reference manifest schema_version"
            )
        actual_base = manifest.get("base_reference_sha256")
        if actual_base != expected_base:
            raise ValueError(
                "manifest-chain discontinuity: "
                f"stage {namespace}'s base_reference_sha256 ({actual_base!r}) "
                f"does not equal {prev_label}'s reference_corpus_sha256 "
                f"({expected_base!r})"
            )
        stage_texts, stage_attrs, acquisition_manifest_sha256 = load_v4_public_sources(
            reference_manifest_path,
            Path(stage["normalized_dir"]),
            acquisition_manifest_path=Path(stage["acquisition_manifest"]),
            expected_namespace=namespace,
        )
        overlap = set(stage_texts) & set(texts)
        if overlap:
            raise ValueError(
                f"{namespace} raw reference ids collide with an earlier "
                f"bundle stage: {sorted(overlap)[:5]}"
            )
        texts.update(stage_texts)
        attributions.update(stage_attrs)
        stage_reports[namespace] = {
            "reference_manifest_sha256": actual_manifest_hash,
            "acquisition_manifest_sha256": acquisition_manifest_sha256,
            "reference_corpus_sha256": manifest["reference_corpus_sha256"],
            "raw_ids": set(stage_texts),
        }
        expected_base = manifest["reference_corpus_sha256"]
        prev_label = f"stage {namespace}"
    return texts, attributions, stage_reports


def pkl_namespace_ids(pkl_stream: Mapping[str, str]) -> Dict[str, set]:
    """Group `pkl_stream`'s ``REF*``-prefixed ids by namespace, EXCLUDING
    ``REF2`` (governed by refs_staging's own manifest, not the sources
    bundle). Feeds the generalized pkl/bundle coverage and set-equality
    gates below (V4.2 plan C2/C12)."""
    grouped: Dict[str, set] = {}
    for ref_id in pkl_stream:
        head, sep, _ = ref_id.partition(":")
        if sep and head != "REF2" and _REF_NAMESPACE_RE.fullmatch(head):
            grouped.setdefault(head, set()).add(ref_id)
    return grouped


def check_bundle_covers_pkl_namespaces(
    pkl_namespaces: Mapping[str, set], bundle_namespaces: Iterable[str]
) -> None:
    """Every REF*-namespace present in the pickle must have a registered
    sources-bundle stage -- an unregistered namespace is a hard error, never
    a silent legacy fallback. Generalizes the former REF4-only "REF4
    references require their pinned V4 public-source inputs" gate to any
    reference generation."""
    missing = sorted(set(pkl_namespaces) - set(bundle_namespaces))
    if missing:
        raise ValueError(
            f"{missing[0]} references require their pinned public-source "
            "inputs from the sources bundle"
        )


def check_pkl_source_set_equality(
    pkl_namespaces: Mapping[str, set], stage_reports: Mapping[str, dict]
) -> None:
    """Each bundle namespace's loaded raw-id set must equal the pickle's set
    of ids under that namespace. Generalizes the REF4-only "V4 public-source
    set does not equal the REF4 pickle set" gate."""
    for namespace, ids in pkl_namespaces.items():
        if stage_reports[namespace]["raw_ids"] != ids:
            raise ValueError(
                f"{namespace} public-source set does not equal the "
                f"{namespace} pickle set"
            )


def check_crosswalk_namespace_coverage(
    crosswalk: Mapping[str, str], stage_reports: Mapping[str, dict]
) -> None:
    """No crosswalk raw id under a bundle namespace may be absent from that
    namespace's pickle-derived raw-id set. Generalizes the REF4-only
    "crosswalk contains REF4 ids absent from the reference pickle" gate."""
    for namespace, report in stage_reports.items():
        unknown = {
            raw_id for raw_id in crosswalk
            if raw_id.startswith(f"{namespace}:")
            and raw_id not in report["raw_ids"]
        }
        if unknown:
            raise ValueError(
                f"crosswalk contains {namespace} ids absent from the "
                "reference pickle"
            )


class WorkSources:
    """Lazy, cached access to public reference texts (NFC + stream + offsets).

    Every text handed out is verified against the pickle stream for its ref id
    when the pickle carries one -- a mismatch means the on-disk file drifted
    from what the pipeline matched against, and the work side is dropped for
    that work (counter `work_stream_mismatch`) rather than approximated.
    """

    def __init__(self, refs_staging: Path, ja_dir: Path,
                 man_by_key: Dict[str, dict], pkl_stream: Dict[str, str],
                 counters: Counter, *, pub_text: Optional[Dict[str, str]] = None,
                 pub_attribution: Optional[Dict[str, str]] = None,
                 bundle_namespaces: Optional[Iterable[str]] = None):
        self.refs_staging = refs_staging
        self.ja_dir = ja_dir
        self.man_by_key = man_by_key
        self.pkl_stream = pkl_stream
        self.counters = counters
        # The merged sources-bundle texts/attributions (V4.2 plan C2): keyed
        # by raw reference id across EVERY bundle stage (REF4, REF5, REF6,
        # ...). Which "NS:" prefixes this dispatches here comes from
        # `bundle_namespaces` -- the bundle's own declared namespaces, never
        # a hard-coded tuple, so a new reference generation needs no edit
        # here, only a new bundle stage.
        self.pub_text = pub_text or {}
        self.pub_attribution = pub_attribution or {}
        self.bundle_namespaces = frozenset(bundle_namespaces or ())
        self._cache: Dict[str, Optional[Tuple[str, object]]] = {}

    def _load(self, ref_id: str) -> Optional[Tuple[str, object]]:
        raw = None
        head, sep, _ = ref_id.partition(":")
        if sep and head in self.bundle_namespaces:
            raw = self.pub_text.get(ref_id)
            if raw is None:
                self.counters["work_v4_source_missing"] += 1
                return None
            path = None
        elif ref_id.startswith("REF2:"):
            ent = self.man_by_key.get(ref_id[5:])
            if not ent:
                self.counters["work_no_manifest_entry"] += 1
                return None
            path = self.refs_staging / ent["body_file"]
        elif ref_id.startswith("J:"):
            path = self.ja_dir / (ref_id[2:] + ".txt")
        else:
            return None
        if path is not None:
            try:
                raw = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                self.counters["work_file_error"] += 1
                return None
        nfc = unicodedata.normalize("NFC", raw)
        stream, offs = norm_stream(nfc)
        expected = self.pkl_stream.get(ref_id)
        if expected is not None and stream != expected:
            self.counters["work_stream_mismatch"] += 1
            return None
        return nfc, offs

    def attribution(self, ref_id: str) -> Optional[str]:
        return self.pub_attribution.get(ref_id)

    def get(self, ref_id: str) -> Optional[Tuple[str, object]]:
        if ref_id not in self._cache:
            self._cache[ref_id] = self._load(ref_id)
        return self._cache[ref_id]


#: Manifest key prefixes whose entries are PUBLIC re-projection targets.
#: `tanakh_` came with REF-3 (39/39 exact-title match, the design probe);
#: the four rabbinic corpora came with REF-4 (owner, 2026-08-13: "Add the
#: wiksource talmudic works") -- 170 tractates fetched with the same
#: license-ranked machinery, `title_he` set to the works table's EXACT
#: neutral_title so this same lookup covers them with no new mapping.
_REPROJECTION_KEY_PREFIXES = (
    "tanakh_", "bavli_", "mishnah_", "yerushalmi_", "tosefta_")


class ReprojectionTargets:
    """Public Sefaria editions as re-projection targets, keyed by the masked
    work's neutral_title (exact match)."""

    def __init__(self, refs_staging: Path, man_by_key: Dict[str, dict]):
        self.refs_staging = refs_staging
        self.key_by_title = {
            e["title_he"]: e["key"] for e in man_by_key.values()
            if e["key"].startswith(_REPROJECTION_KEY_PREFIXES)
        }
        self.man_by_key = man_by_key
        self._cache: Dict[str, Optional[Tuple[str, str, object]]] = {}

    def for_title(self, neutral_title: str) -> Optional[Tuple[str, str, object]]:
        """-> (target_key, nfc_text, offsets_with_stream) or None."""
        key = self.key_by_title.get(neutral_title or "")
        if not key:
            return None
        if key not in self._cache:
            path = self.refs_staging / self.man_by_key[key]["body_file"]
            raw = path.read_text(encoding="utf-8", errors="replace")
            nfc = unicodedata.normalize("NFC", raw)
            stream, offs = norm_stream(nfc)
            self._cache[key] = (nfc, stream, offs)
        nfc, stream, offs = self._cache[key]
        return key, nfc, (stream, offs)


#: The J-corpus per_doc structural markers -- `+פסוק~ +כב~`-style label/value
#: tokens (`+` prefix, `~` suffix). Measured over all 92 per_doc files:
#: 1,743 distinct tokens, and OUTSIDE this grammar the corpus contains only
#: three stray `+~` pairs -- `+` and `~` are never content characters, which
#: is what licenses removing them wholesale.
_JA_MARKER_RE = re.compile(r"\+[^+~\n]{0,40}~")


def clean_ja_markers(text: Optional[str]) -> Optional[str]:
    """Strip per_doc structural markers from a J-corpus DISPLAY piece.

    Display-only, applied AFTER slicing and BEFORE the word-highlight pass
    (owner, 2026-08-13: the edition pane showed raw `+פסוק~ +כב~` tokens).
    The marker letters live inside the matcher's coordinate stream, so the
    stream and the stored offsets are never touched -- only the already-cut
    piece text loses them. The two boundary rules handle a token the piece
    slice cut in half; safe because `~` never appears as content.
    """
    if not text or ("+" not in text and "~" not in text):
        return text
    s = _JA_MARKER_RE.sub("", text)
    s = re.sub(r"^[^+~\n]{0,40}~", "", s)    # token cut at the piece start
    s = re.sub(r"\+[^+~\n]{0,40}$", "", s)   # token cut at the piece end
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    s = re.sub(r"\n{2,}", "\n", s)           # marker-only lines vanish
    return s.strip(" ")


def _word_tokens(text: str) -> List[Tuple[int, int, str]]:
    """Whitespace-separated tokens as (start, end, normalized) over `text`.
    The normalized form is the letters-only finals-folded stream of the token
    (norm_stream), so punctuation, nikud, braces and the cap ellipsis never
    participate in matching; a token with no Hebrew letters is dropped."""
    out: List[Tuple[int, int, str]] = []
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        j = i
        while j < n and not text[j].isspace():
            j += 1
        norm, _ = norm_stream(text[i:j])
        if norm:
            out.append((i, j, norm))
        i = j
    return out


def fuzzy_word_intervals(a_text: str, b_text: str,
                         min_ratio: float = 72.0
                         ) -> Tuple[List[List[int]], List[List[int]]]:
    """Char intervals (into each ORIGINAL string) of the words the two spans
    share -- the reader-facing 'these are the same words' highlight.

    Two passes: exact matching on normalized word sequences
    (difflib.SequenceMatcher, order-preserving), then an order-preserving
    greedy fuzzy pass over the unmatched gaps (rapidfuzz ratio >= min_ratio)
    so an HTR miscopy or a plene/defective spelling difference still pairs.
    Runs of adjacent matched words merge into one interval, so the space
    between two matched words is inside the highlight rather than a hole.
    """
    from rapidfuzz.fuzz import ratio as _fz_ratio

    aw, bw = _word_tokens(a_text), _word_tokens(b_text)
    an = [t[2] for t in aw]
    bn = [t[2] for t in bw]
    a_hit = [False] * len(aw)
    b_hit = [False] * len(bw)
    blocks = difflib.SequenceMatcher(None, an, bn,
                                     autojunk=False).get_matching_blocks()
    for ai, bi, size in blocks:
        for k in range(size):
            a_hit[ai + k] = True
            b_hit[bi + k] = True
    prev_a = prev_b = 0
    for ai, bi, size in blocks:  # includes the terminal zero-size block
        cursor = prev_b
        for i2 in range(prev_a, ai):
            best: Optional[Tuple[float, int]] = None
            for j2 in range(cursor, bi):
                score = _fz_ratio(an[i2], bn[j2])
                if score >= min_ratio and (best is None or score > best[0]):
                    best = (score, j2)
            if best is not None:
                a_hit[i2] = True
                b_hit[best[1]] = True
                cursor = best[1] + 1
        prev_a, prev_b = ai + size, bi + size

    def to_intervals(words, hits) -> List[List[int]]:
        iv: List[List[int]] = []
        last = None
        for idx, ((s, e, _), hit) in enumerate(zip(words, hits)):
            if not hit:
                continue
            if iv and last == idx - 1:
                iv[-1][1] = e
            else:
                iv.append([s, e])
            last = idx
        return iv

    return to_intervals(aw, a_hit), to_intervals(bw, b_hit)


def reproject(query: str, tstream: str, prior: float,
              min_score: float) -> Optional[Tuple[int, int, float]]:
    """Locate `query` (a masked-edition letter span, IN MEMORY ONLY) inside a
    public target stream. Position-prior window first, whole book as the one
    fallback. Returns (lo, hi, score) in target-stream space, or None."""
    from rapidfuzz.fuzz import partial_ratio_alignment

    total = len(tstream)
    center = int(prior * total)
    half = max(len(query) * 3, total // 10)
    w_lo, w_hi = max(0, center - half), min(total, center + half)
    res = partial_ratio_alignment(query, tstream[w_lo:w_hi],
                                  score_cutoff=min_score)
    if res is not None:
        return w_lo + res.dest_start, w_lo + res.dest_end, float(res.score)
    res = partial_ratio_alignment(query, tstream, score_cutoff=min_score)
    if res is not None:
        return res.dest_start, res.dest_end, float(res.score)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("public_db")
    ap.add_argument("--out", required=True)
    ap.add_argument("--crosswalk", required=True)
    ap.add_argument("--refs-staging", required=True)
    ap.add_argument("--ja-dir", required=True)
    ap.add_argument("--fullcorpus", required=True)
    ap.add_argument("--ref-pkl", required=True)
    ap.add_argument(
        "--sources-bundle", required=True,
        help="ordered discovery-excerpt-sources-bundle-v1 JSON (V4.2 plan "
             "C2): REF4->REF5->REF6->... chain-order stages, each an "
             "explicit hash-pinned reference-manifest/acquisition-manifest/"
             "normalized-dir input",
    )
    ap.add_argument(
        "--base-reference-sha256", required=True,
        help="pinned base V2 reference-corpus SHA-256 -- the chain-"
             "continuity anchor for the sources bundle's first stage",
    )
    ap.add_argument("--ctx", type=int, default=90)
    ap.add_argument("--span-cap", type=int, default=600)
    ap.add_argument("--min-align-score", type=float, default=65.0)
    ap.add_argument("--limit", type=int, default=0,
                    help="dev smoke only; gates run on full bakes")
    ap.add_argument("--skip-masking-gate", action="store_true",
                    help="dev smoke only; NEVER for a deployable artifact")
    args = ap.parse_args()

    t0 = time.time()
    src, out = Path(args.public_db), Path(args.out)
    if not src.is_file():
        sys.exit(f"no such asset: {src}")
    if out.resolve() == src.resolve():
        sys.exit("--out must not be the input (the pre-excerpt artifact is "
                 "the frame-regression BEFORE side)")

    crosswalk_path = Path(args.crosswalk)
    ref_pickle_path = Path(args.ref_pkl)
    try:
        input_meta = validate_bake_input_hashes(src, crosswalk_path, ref_pickle_path)
    except (OSError, sqlite3.Error, ValueError) as exc:
        sys.exit(str(exc))

    crosswalk = json.load(open(crosswalk_path, encoding="utf-8"))
    ref_by_work: Dict[str, List[str]] = {}
    for rid, wid in crosswalk.items():
        ref_by_work.setdefault(wid, []).append(rid)
    for refs in ref_by_work.values():
        refs.sort()

    refs_staging = Path(args.refs_staging)
    manifest_path = refs_staging / "manifest.json"
    man = json.load(open(manifest_path, encoding="utf-8"))
    man_by_key = {e["key"]: e for e in man["entries"]}

    print("loading reference pickle (streams only)...", flush=True)
    works_pkl = pickle.load(open(ref_pickle_path, "rb"))
    pkl_stream = {w["id"]: w["stream"] for w in works_pkl}
    del works_pkl

    sources_bundle_path = Path(args.sources_bundle)
    try:
        bundle = load_excerpt_sources_bundle(sources_bundle_path)
    except (OSError, ValueError) as exc:
        sys.exit(str(exc))
    try:
        pub_text, pub_attribution, stage_reports = load_public_sources_from_bundle(
            bundle, args.base_reference_sha256
        )
    except (OSError, ValueError) as exc:
        sys.exit(str(exc))
    bundle_namespaces = set(stage_reports)

    # Generalized pkl-namespace coverage + set-equality gates (V4.2 plan
    # C2/C12): every REF*-prefixed namespace found in the pickle -- OTHER
    # than REF2, which is governed by refs_staging's own manifest below, not
    # the bundle -- must have a registered bundle stage AND load exactly the
    # same raw-id set the pickle carries for it. Generalizes the former
    # REF4-only gates to any reference generation.
    pkl_namespaces = pkl_namespace_ids(pkl_stream)
    try:
        check_bundle_covers_pkl_namespaces(pkl_namespaces, bundle_namespaces)
        check_pkl_source_set_equality(pkl_namespaces, stage_reports)
    except ValueError as exc:
        sys.exit(str(exc))

    # REF4's historical cross-check against the canonical-merge input the
    # public sidecar itself recorded stays scoped to REF4: the meta key it
    # reads (`canonical_merges_v4_source_manifest_sha256`) is REF4-specific
    # and owned by build_discovery_sidecar.py (V4.2 plan C3/C4), out of this
    # change's scope. A future namespace gets its own such check when the
    # sidecar builder grows the equivalent meta key.
    if "REF4" in stage_reports:
        expected_source_hash = input_meta.get(
            "canonical_merges_v4_source_manifest_sha256"
        )
        actual_source_hash = stage_reports["REF4"]["acquisition_manifest_sha256"]
        if not expected_source_hash or expected_source_hash != actual_source_hash:
            sys.exit(
                "V4 public-source manifest differs from the canonical-merge input "
                "recorded by the public sidecar"
            )

    try:
        check_crosswalk_namespace_coverage(crosswalk, stage_reports)
    except ValueError as exc:
        sys.exit(str(exc))

    counters: Counter = Counter()
    sources = WorkSources(
        refs_staging, Path(args.ja_dir), man_by_key, pkl_stream, counters,
        pub_text=pub_text, pub_attribution=pub_attribution,
        bundle_namespaces=bundle_namespaces,
    )
    targets = ReprojectionTargets(refs_staging, man_by_key)

    shutil.copyfile(src, out)
    conn = sqlite3.connect(out)
    conn.row_factory = sqlite3.Row
    full = sqlite3.connect(Path(args.fullcorpus).resolve().as_uri()
                           + "?mode=ro", uri=True)

    titles = dict(conn.execute("SELECT work_id, neutral_title FROM works"))

    # best eligible witness row per identification (frozen rule)
    best: Dict[str, sqlite3.Row] = {}
    for r in conn.execute(_BEST_ROWS_SQL):
        k = r["identification_id"]
        cur = best.get(k)
        if cur is None or excerpt_candidate_key(r) < excerpt_candidate_key(cur):
            best[k] = r
    print(f"identifications with an eligible row: {len(best):,}", flush=True)
    fallback_count = sum(is_excerpt_only_fallback(row) for row in best.values())
    if fallback_count:
        counters["work_review_only_text_fallback"] = fallback_count

    conn.execute(_DDL)

    page_cache: OrderedDict = OrderedDict()          # a_page_id -> (nfc, offs)
    align_cache: Dict[Tuple[str, int, int], Optional[Tuple[int, int, float]]] = {}

    def page_text(page_id: str):
        if page_id in page_cache:
            page_cache.move_to_end(page_id)
            return page_cache[page_id]
        row = full.execute("SELECT text FROM pages WHERE page_id = ?",
                           (page_id,)).fetchone()
        if not row or not row[0]:
            res = None
        else:
            nfc = unicodedata.normalize("NFC", row[0])
            _, offs = norm_stream(nfc)
            res = (nfc, offs)
        page_cache[page_id] = res
        if len(page_cache) > 512:
            page_cache.popitem(last=False)
        return res

    inserted = 0
    items = sorted(best.items())
    if args.limit:
        items = items[:args.limit]
    for ident_id, r in items:
        # ---- fragment side (mandatory: no fragment pieces -> no row) ----
        pt = page_text(r["a_page_id"])
        if pt is None:
            counters["frag_page_missing"] += 1
            continue
        nfc, offs = pt
        if r["aligned_page_start"] is not None and r["aligned_page_end"] is not None:
            lo, hi = r["aligned_page_start"], r["aligned_page_end"]
        else:
            lo, hi = r["span_start"], r["span_end"]
            counters["frag_fallback_span"] += 1
        if lo is None or hi is None or hi <= lo or hi > len(offs):
            counters["frag_span_out_of_range"] += 1
            continue
        fb, fs, fa, fclip = pieces(nfc, offs, lo, hi, args.ctx, args.span_cap)

        # ---- work side (optional: fail-soft to no pane, never approximate) ----
        wb = ws = wa = wsrc = attribution = None
        wclip = score = None
        work_id = r["work_id"]
        refs = ref_by_work.get(work_id) or []
        kinds = {x.split(":", 1)[0] for x in refs}
        if r["w_start"] is None or r["w_end"] is None or not refs:
            counters["work_no_offsets_or_refs"] += 1
        elif kinds == {"M"}:
            target = targets.for_title(titles.get(work_id, ""))
            mstream = pkl_stream.get(refs[0])
            if target is None:
                counters["work_masked_no_target"] += 1
            elif mstream is None or r["w_end"] > len(mstream):
                counters["work_masked_span_oor"] += 1
            else:
                tkey, tnfc, (tstream, toffs) = target
                ck = (work_id, r["w_start"], r["w_end"])
                if ck not in align_cache:
                    align_cache[ck] = reproject(
                        mstream[r["w_start"]:r["w_end"]], tstream,
                        r["w_start"] / max(1, len(mstream)),
                        args.min_align_score)
                loc = align_cache[ck]
                if loc is None:
                    counters["work_reproject_below_threshold"] += 1
                else:
                    tlo, thi, score = loc
                    wb, ws, wa, wclip = pieces(tnfc, toffs, tlo, thi,
                                               args.ctx, args.span_cap)
                    wsrc = "reprojected"
                    attribution = man_by_key[tkey].get("attribution_text")
                    counters["work_reprojected"] += 1
        else:
            wt = sources.get(refs[0])
            if wt is None:
                pass  # counter already incremented inside WorkSources
            elif r["w_end"] > len(wt[1]):
                counters["work_span_out_of_range"] += 1
            else:
                wnfc, woffs = wt
                wb, ws, wa, wclip = pieces(wnfc, woffs, r["w_start"],
                                           r["w_end"], args.ctx, args.span_cap)
                wsrc = "direct"
                ref0_head = refs[0].partition(":")[0]
                if refs[0].startswith("REF2:"):
                    attribution = man_by_key[refs[0][5:]].get("attribution_text")
                elif ref0_head in bundle_namespaces:
                    attribution = sources.attribution(refs[0])
                elif refs[0].startswith("J:"):
                    # Structural markers out of the DISPLAY pieces, before
                    # the highlight pass reads them (owner, 2026-08-13).
                    cleaned = tuple(clean_ja_markers(x) for x in (wb, ws, wa))
                    if cleaned != (wb, ws, wa):
                        counters["ja_markers_cleaned"] += 1
                    wb, ws, wa = cleaned
                counters["work_direct"] += 1

        # The word-level parallel highlight (owner, 2026-08-13 round 2):
        # computed only when BOTH spans exist -- with no work side there is
        # nothing to be parallel to, and the renderer falls back to the
        # whole-span treatment on NULL.
        frag_hl = work_hl = None
        if ws:
            fh, wh = fuzzy_word_intervals(fs, ws)
            frag_hl, work_hl = json.dumps(fh), json.dumps(wh)
            if fh:
                counters["hl_computed"] += 1
            else:
                counters["hl_empty"] += 1
        # J-corpus texts mark Hebrew words inside the Judeo-Arabic with {...};
        # the renderer strips the braces and colors the content, keyed on this
        # flag so the transform can never fire on a text that means a literal
        # brace.
        work_markup = ("ja_braces" if wsrc == "direct" and refs
                       and refs[0].startswith("J:") else None)

        conn.execute(
            "INSERT INTO discovery_excerpt VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ident_id, r["evidence_id"], r["a_page_id"], fb, fs, fa, fclip,
             wb, ws, wa, wclip, wsrc, score, attribution,
             r["n_spans"], r["text_layer"], frag_hl, work_hl, work_markup))
        inserted += 1
        if inserted % 5000 == 0:
            print(f"  {inserted:,} rows...", flush=True)

    n = conn.execute("SELECT COUNT(*) FROM discovery_excerpt").fetchone()[0]
    meta_rows = [
        ("excerpt_schema_version", EXCERPT_SCHEMA_VERSION),
        ("expected_rows_discovery_excerpt", str(n)),
        ("excerpt_ctx", str(args.ctx)),
        ("excerpt_span_cap", str(args.span_cap)),
        ("excerpt_refs_manifest_sha256", sha256_file(manifest_path)),
        ("excerpt_sources_bundle_sha256", sha256_file(sources_bundle_path)),
    ]
    for namespace, report in sorted(stage_reports.items()):
        ns_key = namespace.lower()
        meta_rows.append((
            f"excerpt_{ns_key}_reference_manifest_sha256",
            report["reference_manifest_sha256"],
        ))
        meta_rows.append((
            f"excerpt_{ns_key}_acquisition_manifest_sha256",
            report["acquisition_manifest_sha256"],
        ))
    conn.executemany("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                     meta_rows)
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    full.close()

    print(f"\nrows inserted: {n:,} of {len(items):,} candidates "
          f"({time.time() - t0:,.0f}s)")
    for k in sorted(counters):
        print(f"  {k:<34} {counters[k]:>7,}")

    if args.limit and not args.skip_masking_gate:
        print("\n--limit smoke: artifact is NOT deployable; masking gate "
              "still runs.", flush=True)
    if args.skip_masking_gate:
        print("\nMASKING GATE SKIPPED (--skip-masking-gate): artifact must "
              "not be deployed.", flush=True)
        return 0
    import project_discovery_public as proj
    passed, _ = proj.run_masking_gate(str(out))
    print(f"masking gate: {'PASS' if passed else 'FAIL'}", flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
