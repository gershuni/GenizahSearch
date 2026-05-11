#!/usr/bin/env python3
"""Phase 85: Generate synthetic libraries.csv rows for FJMS-only inventories.

Per CONTEXT.md decisions (with REVIEWS-MODE 2026-05-08 revisions):
  D-01    : write synthetic sys_ids into AlmaId column at FJMS sidecar export time (Plan 03)
  D-01a   : export-time collision check vs real-Alma libraries.csv rows (this script)
  D-01b   : sys_ids stay strings; never int() (delegated to shared/synthetic_sys_id.py)
  D-02    : qualifying set = inventories with CUDL manifest OR substantive FJMS metadata.
            REVIEWS-MODE EXPANDED: substantive FJMS metadata = catalog title OR
            bibliography OR free-description OR full-text OR measurement record
            (per CONTEXT.md "inclusive maximum coverage" authority + Codex HIGH).
  D-03    : tier counts in reports/synthetic_coverage.md
  D-04    : libraries.csv is the durable artifact; this script is the source-of-truth process
  D-04a   : idempotent regeneration via marker-fenced block in libraries.csv
  D-05    : hybrid CUDL × FIST cross-product
  D-05a   : ambiguity exclusion → reports/synthetic_ambiguity_residue.csv.
            REVIEWS-MODE STRICT: exclude BOTH multi-inventory AND multi-signature
            ambiguities (Codex HIGH); residue has ambiguity_kind column.
  D-09    : title precedence FJMS Title (Hebrew) → FJMS GenizahTitleText → shelfmark.
            (FIST.db schema lacks a separate TitleHeb column — the existing Title
            column IS Hebrew. See 85-02-SUMMARY.md for schema-mapping deviation.)
  D-12    : call_numbers shape — minimum FJMS canonical form + cheap normalized variants
  D-15    : library_code ∈ {CUL, Mosseri}; no new codes

REVIEWS-MODE additions:
  - synthetic_manifest.json is AUTHORITATIVE for Plan 03 (eliminates parallel SQL predicate)
  - SYNTH-03 narrowed to Title/Shelfmark search modes only (text/Responsa use Tantivy)
  - Phase 86 audit cross-link in coverage.md
  - CSV-injection FAIL-LOUD: rows with leading =/+/-/@ are excluded, not sanitized
  - DETERMINISTIC ORDERING on every SQL query
  - _build_qualifying_inventories accepts injectable connections (testability)

Usage:
    python scripts/generate_synthetic_rows.py --dry-run   # report only
    python scripts/generate_synthetic_rows.py --apply     # rewrite libraries.csv

CSV-injection mitigation (T-85-01 REVISED): leading {'=', '+', '-', '@'} in
title/shelfmark columns causes the row to be EXCLUDED (NOT sanitized with
single-quote prefix). Excluded rows logged to ambiguity-residue with
ambiguity_kind='csv_injection_leader'. Rationale (Codex MEDIUM): csv.reader
does no Excel-sanitization on read, so a single-quote prefix would pollute
runtime-visible Hebrew titles.

SQL-injection mitigation (T-85-02): all dynamic values use parameterized
sqlite3.Cursor.execute(query, params); never f-string interpolation. The SQL
in this script is 100% structural (no params from user input).
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shared.synthetic_sys_id import (  # noqa: E402
    encode_inventory_sys_id,
    is_synthetic_sys_id,
)
from shared.shelfmark_bridge import (  # noqa: E402
    build_alias_index,
    cudl_normalize,
    lookup_cudl,
)
from shared.fist_cudl_bridge import (  # noqa: E402  Phase 86 NEW (Plan 01)
    InventoryRecord,
    build_fist_alias_index,
    explain_fist_by_cudl,
    lookup_fist_by_cudl,
)

CSV_PATH = ROOT / "libraries.csv"
# FIST.db actually lives at fist_data/FIST.db in this repo (NOT FIST_DB_BACKUP/
# as the plan's draft path suggested). See 85-02-SUMMARY.md for path deviation.
FIST_DB = ROOT / "fist_data" / "FIST.db"
NLI_DB = ROOT / "nli_data" / "nli_crossref.db"
MANIFEST_PATH = ROOT / "fist_data" / "synthetic_manifest.json"
RESIDUE_PATH = ROOT / "reports" / "synthetic_ambiguity_residue.csv"
COVERAGE_PATH = ROOT / "reports" / "synthetic_coverage.md"

MARKER_BEGIN = "# BEGIN SYNTHETIC"
MARKER_END = "# END SYNTHETIC"

# T-85-01 REVISED (Codex MEDIUM): CSV-injection fail-loud
_CSV_INJECTION_LEADERS = ("=", "+", "-", "@")


def _has_csv_injection_leader(value: object) -> bool:
    """Return True iff value starts with a CSV-injection char.

    REVIEWS-MODE: rows with this property are EXCLUDED, not sanitized. csv.reader
    performs no Excel-sanitization on read, so a single-quote prefix would
    pollute runtime-visible Hebrew titles in the app UI.
    """
    if not value:
        return False
    s = str(value)
    return bool(s) and s[0] in _CSV_INJECTION_LEADERS


# ---------------------------------------------------------------------------
# Phase 86 helpers (Plan 02) — CUDL-walked rewrite scaffolding
# ---------------------------------------------------------------------------


def _build_real_only_csv_bank(csv_bank: dict) -> dict:
    """Return a synthetic-stripped view of csv_bank for Phase 84's build_alias_index.

    Pass 2 HIGH-1 (idempotency): when Phase 86 has already run --apply once,
    libraries.csv contains the synthetic block AND csv_bank reflects it. If we
    feed that bank straight into build_alias_index, lookup_cudl(classmark) will
    resolve to a synthetic sys_id and the CUDL-walk's step-1 skip predicate will
    treat the classmark as 'already covered' — silently dropping it from the
    new qualifying set. Re-applying then wipes the synthetic block entirely.

    Solution (option A — zero Phase 84 mutation, preserves NORM-04): rebuild a
    synthetic-stripped dict here. The bridge's alias index only sees REAL
    libraries.csv rows, so step-1 short-circuits only when a REAL row covers
    the classmark.

    We do NOT touch csv_bank itself — runtime behaviour relying on synthetic
    sys_ids (browse, FJMS lookups, etc.) continues to work because those
    consumers query csv_bank directly, not the bridge alias index.
    """
    return {
        sys_id: data
        for sys_id, data in csv_bank.items()
        if not is_synthetic_sys_id(sys_id)
    }


def _guess_pattern(cudl_classmark: str) -> str:
    """Categorize residue classmark into one of the known D-02b families.

    Output is a hint for human adjudicators looking at 86-RESIDUE-PATTERNS.md
    (Phase 86 Plan 03); not a load-bearing decision. Returns 'other' for
    classmarks that don't match any of the known prefixes.
    """
    if not cudl_classmark:
        return "other"
    if cudl_classmark.startswith("tsf"):
        return "tsf_flattened_series"
    if cudl_classmark.startswith("tsar"):
        return "tsar_flattened_series"
    if cudl_classmark.startswith("tsns"):
        if "minute" in cudl_classmark or cudl_classmark[-1:].isalpha():
            return "tsns_minute_or_letter"
        return "tsns_other"
    if cudl_classmark.startswith("or"):
        return "or_single_segment"
    if cudl_classmark.startswith("mosseri"):
        return "mosseri_exotic_letter"
    if cudl_classmark.startswith("tsmisc"):
        return "tsmisc_multi_segment"
    return "other"


def _load_parent_shelfmark_set(path: Optional[Path] = None) -> set[str]:
    """Load D-06 parent-shadow filter from reports/synthetic_parent_shelfmarks.csv.

    File schema: parent_shelfmark,synthetic_sys_id,inventory_id,real_child_count,sample_real_children
    Returns empty set if file missing (graceful — Phase 86 first run may not have it).
    """
    if path is None:
        path = ROOT / "reports" / "synthetic_parent_shelfmarks.csv"
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ps = (row.get("parent_shelfmark") or "").strip()
            if ps:
                out.add(ps)
    return out


def _build_csv_bank_from_rows(rows: list[list[str]]) -> dict:
    """Reconstruct a csv_bank dict-of-dicts from raw libraries.csv rows.

    Phase 84 build_alias_index expects the MetadataManager.csv_bank shape:
    keys are digit-normalized sys_ids; each value carries 'shelfmark',
    'library_code', and 'call_numbers_raw' (list of pipe-split variants).

    Marker-block rows ('# BEGIN SYNTHETIC', '# END SYNTHETIC') and the header
    row are skipped via the same heuristics genizah_core._load_csv_bank uses
    (sys_id starts with '#' -> skip; digit-normalized empty -> skip).
    """
    out: dict = {}
    for row in rows:
        if not row or len(row) < 3:
            continue
        raw_sys_id = row[0]
        if not raw_sys_id or raw_sys_id.startswith("#"):
            continue
        sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())
        if not sys_id:
            continue
        raw_shelves = row[2].split("|") if len(row) > 2 else []
        shelf = raw_shelves[0].strip() if raw_shelves else ""
        for s in raw_shelves:
            s = s.strip()
            if s and len(s) < len(shelf):
                shelf = s
        library_code = row[3].strip() if len(row) > 3 else ""
        call_numbers_raw = [s.strip() for s in raw_shelves if s.strip()] or None
        out[sys_id] = {
            "shelfmark": shelf,
            "library_code": library_code,
            "call_numbers_raw": call_numbers_raw,
        }
    return out


# ---------------------------------------------------------------------------
# Phase 86 Plan 02 — CUDL-walked _build_qualifying_inventories rewrite
# (replaces Phase 85's FIST-walked + multi_signature STRICT predicate)
# ---------------------------------------------------------------------------


def _build_qualifying_inventories(
    fist_conn: sqlite3.Connection,
    nli_conn: Optional[sqlite3.Connection] = None,
) -> tuple[dict[int, dict], list[dict]]:
    """CUDL-WALKED rewrite (Phase 86 Plan 02 — D-01).

    Replaces Phase 85's FIST-walked + multi_signature STRICT predicate (which
    produced the reverted 5,035 bib-only rows) with a CUDL-walked, image-bearing-
    only resolver:
      - Walks nli_crossref.db.cambridge_manifests (~141K CUDL classmarks).
      - Skips classmarks Phase 84 already resolves to a REAL libraries.csv row
        via lookup_cudl. The outer caller MUST have built the alias index from
        a synthetic-stripped csv_bank (see _build_real_only_csv_bank) so prior-
        run synthetic rows cannot mask a still-qualifying classmark (Pass 2
        HIGH-1 idempotency invariant).
      - Resolves each remaining classmark through explain_fist_by_cudl (Plan 01
        — status-aware):
          * 'not_found' -> residue ambiguity_kind='no_fist_match' + pattern_guess
          * 'multi_inventory_ambiguous' -> residue ambiguity_kind='multi_inventory'
            (DISTINCT category per Codex HIGH #6) + comma-joined inventory IDs
          * 'single':
              - rec.has_alma=True -> SKIP silently (alias-only existing
                libraries.csv row — audit-only coverage; no synthetic emitted)
              - rec.fist_shelfmark in parent_shadow set (D-06) -> residue
                ambiguity_kind='parent_shadow'
              - CSV-injection leader in shelfmark/label/title_heb/genizah_title
                -> residue ambiguity_kind='csv_injection_leader'
              - else -> emit qualifying row with title_heb / genizah_title
                propagated from the InventoryRecord (Gemini HIGH #8) and
                has_cudl_manifest=True by construction (D-01a image-bearing-
                only invariant).

    Args:
        fist_conn: open sqlite3.Connection to FIST.db (read-only is fine).
        nli_conn:  open sqlite3.Connection to nli_crossref.db. REQUIRED in
            Phase 86 — Phase 85's FIST-walk fallback path is removed. Raises
            ValueError if None.

    Returns: (
        qualifying: dict mapping InventoryId -> {
            'canonical_shelfmark': str,        # rec.fist_shelfmark
            'title_heb':       Optional[str],  # rec.title_heb (HIGH #8)
            'title_eng':       Optional[str],  # always None
            'genizah_title':   Optional[str],  # rec.genizah_title (HIGH #8)
            'library_code':    'CUL' | 'Mosseri',
            'has_cudl_manifest': True,         # D-01a by construction
            'has_fjms_metadata': bool,         # any title present
            'cudl_label':      str,            # CUDL label from cambridge_manifests
        },
        ambiguity_residue: list[dict] with ambiguity_kind in
          {'no_fist_match', 'multi_inventory', 'parent_shadow',
           'csv_injection_leader'} and pattern_guess populated
    )
    """
    if nli_conn is None:
        raise ValueError(
            "Phase 86 D-01 CUDL-walk requires nli_conn (nli_crossref.db). "
            "Phase 85's FIST-walk fallback is removed in Phase 86."
        )

    # Step 1: Walk CUDL classmarks deterministically.
    cudl_rows = list(
        nli_conn.execute(
            "SELECT label, manifest_url, normalized_shelfmark "
            "FROM cambridge_manifests "
            "WHERE normalized_shelfmark IS NOT NULL AND normalized_shelfmark != '' "
            "ORDER BY normalized_shelfmark"
        )
    )

    # Step 2: Build Phase 86 FIST<->CUDL alias index (in-memory, one-shot).
    # Populates title_heb/genizah_title on each InventoryRecord (Gemini HIGH #8)
    # via the 3-table production-correct join (Pass 2 HIGH-2).
    build_fist_alias_index(fist_conn)

    # Step 3: Load D-06 parent-shadow filter.
    parent_shelfmarks = _load_parent_shelfmark_set()

    qualifying: dict[int, dict] = {}
    ambiguity_residue: list[dict] = []

    for label, manifest_url, classmark in cudl_rows:
        # Phase 84 check — classmark already in REAL libraries.csv?
        # Pass 2 HIGH-1: the outer caller built the alias index from a
        # synthetic-stripped csv_bank, so synthetic prior-run rows cannot
        # mask this classmark.
        if lookup_cudl(classmark) is not None:
            continue

        # Phase 86 FIST resolution via status-aware explain_fist_by_cudl.
        status, entries = explain_fist_by_cudl(classmark)

        if status == "not_found":
            ambiguity_residue.append(
                {
                    "inventory_id": "",
                    "signature_id": "",
                    "ambiguity_kind": "no_fist_match",
                    "classmark": classmark,
                    "cudl_label": label or "",
                    "fist_signature_ids": "",
                    "fist_inventory_ids": "",
                    "leading_char": "",
                    "pattern_guess": _guess_pattern(classmark),
                }
            )
            continue

        if status == "multi_inventory_ambiguous":
            # Codex HIGH #6: distinct category from no_fist_match.
            inv_ids = ",".join(str(e.inventory_id) for e in entries)
            ambiguity_residue.append(
                {
                    "inventory_id": "",
                    "signature_id": "",
                    "ambiguity_kind": "multi_inventory",
                    "classmark": classmark,
                    "cudl_label": label or "",
                    "fist_signature_ids": "",
                    "fist_inventory_ids": inv_ids,
                    "leading_char": "",
                    "pattern_guess": _guess_pattern(classmark),
                }
            )
            continue

        # status == 'single'
        rec: InventoryRecord = entries[0]

        # D-01a / Codex HIGH #7: rec.has_alma=True means libraries.csv row
        # exists (alias-only coverage). No new synthetic emitted; Plan 04's
        # coverage report counts these as the distinct
        # `phase86_existing_alma_candidate` tier with explicit framing that
        # app shelfmark search depends on Phase 84 alias coverage.
        if rec.has_alma:
            continue

        # D-06 parent-shadow filter.
        if rec.fist_shelfmark in parent_shelfmarks:
            ambiguity_residue.append(
                {
                    "inventory_id": str(rec.inventory_id),
                    "signature_id": "",
                    "ambiguity_kind": "parent_shadow",
                    "classmark": classmark,
                    "cudl_label": label or "",
                    "fist_signature_ids": "",
                    "fist_inventory_ids": str(rec.inventory_id),
                    "leading_char": "",
                    "pattern_guess": _guess_pattern(classmark),
                }
            )
            continue

        # CSV-injection fail-loud — guard ALL caller-controlled strings
        # (Gemini suggestion fold-in: extend to title_heb/genizah_title).
        inj_source = next(
            (
                s
                for s in (
                    rec.fist_shelfmark,
                    label,
                    rec.title_heb,
                    rec.genizah_title,
                )
                if s and _has_csv_injection_leader(s)
            ),
            None,
        )
        if inj_source is not None:
            ambiguity_residue.append(
                {
                    "inventory_id": str(rec.inventory_id),
                    "signature_id": "",
                    "ambiguity_kind": "csv_injection_leader",
                    "classmark": classmark,
                    "cudl_label": label or "",
                    "fist_signature_ids": "",
                    "fist_inventory_ids": str(rec.inventory_id),
                    "leading_char": str(inj_source)[:1],
                    "pattern_guess": _guess_pattern(classmark),
                }
            )
            continue

        # Emit qualifying row. Title metadata propagates from
        # InventoryRecord (Gemini HIGH #8).
        qualifying[rec.inventory_id] = {
            "canonical_shelfmark": rec.fist_shelfmark,
            "title_heb": rec.title_heb,
            "title_eng": None,  # No English column in dbo_UnitCatalogRec.
            "genizah_title": rec.genizah_title,
            "library_code": _classify_library_code(rec.fist_shelfmark),
            "has_cudl_manifest": True,  # D-01a invariant by construction.
            "has_fjms_metadata": bool(rec.title_heb or rec.genizah_title),
            "cudl_label": label or "",
        }

    return qualifying, ambiguity_residue


def _classify_library_code(fist_shelfmark: str) -> str:
    """Map FIST.Shelfmark to libraries.csv library_code (D-15 Phase 85: CUL or Mosseri only).

    Recognizes Mosseri in three forms (Codex MEDIUM widening):
      - 'Moss. <Roman>,...' (canonical FIST form)
      - 'Mosseri: Moss. <Roman>,...' (FIST data-noise prefix)
      - Any shelfmark whose post-last-':' substring begins with 'Moss.'
        (e.g., 'AIU: Moss. III,27.1' — defensive widening).
    Default: 'CUL'.
    """
    if not fist_shelfmark:
        return "CUL"
    sm_raw = fist_shelfmark.strip()
    sm = sm_raw.lower()
    if sm.startswith("moss."):
        return "Mosseri"
    if sm.startswith("mosseri:"):
        return "Mosseri"
    if ":" in sm_raw:
        tail = sm_raw.rsplit(":", 1)[1].strip().lower()
        if tail.startswith("moss."):
            return "Mosseri"
    return "CUL"


def _resolve_title(qual: dict) -> str:
    """D-09 precedence: FJMS Title (Hebrew) → FJMS GenizahTitleText → shelfmark.

    SCHEMA NOTE: FIST.db dbo_UnitCatalogRec.Title is already Hebrew in this
    corpus; there is no separate TitleHeb column. The title_eng slot is always
    None. See 85-02-SUMMARY.md for the schema-mapping deviation.
    """
    for k in ("title_heb", "title_eng", "genizah_title"):
        v = qual.get(k)
        if v and str(v).strip():
            return str(v).strip()
    return qual["canonical_shelfmark"]


def _generate_call_numbers(canonical: str) -> str:
    """D-12: minimum FJMS canonical + cheap normalized variants (slash, dot, leading-zero).

    Produces a pipe-delimited string per libraries.csv convention. The set is
    deduplicated and the canonical form always appears first (drives the
    csv_bank shelf-picker at genizah_core.py:3382-3387).
    """
    variants = [canonical]
    # Leading-zero strip: 'T-S NS 329/0014' -> 'T-S NS 329/14'
    stripped = re.sub(r"(?<=[/.])0+(\d)", r"\1", canonical)
    if stripped != canonical:
        variants.append(stripped)
    # Slash → dot: 'T-S NS 329/14' -> 'T-S NS 329.14'
    if "/" in canonical:
        variants.append(canonical.replace("/", "."))
    if "/" in stripped and stripped not in variants:
        variants.append(stripped.replace("/", "."))
    # Dedupe preserving order
    seen = set()
    out = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return "|".join(out)


# ---------------------------------------------------------------------------
# libraries.csv marker-block rewrite (D-04a idempotency)
# ---------------------------------------------------------------------------


def _read_libraries_csv(path: Path) -> tuple[list[list[str]], str]:
    """Read all rows; return (rows, line_terminator)."""
    with path.open("rb") as f:
        sample = f.read(8192)
    line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    return rows, line_terminator


def _strip_existing_synthetic_block(rows: list[list[str]]) -> list[list[str]]:
    """Remove any existing rows between MARKER_BEGIN and MARKER_END (inclusive)."""
    out = []
    in_block = False
    for row in rows:
        first = row[0] if row else ""
        if first == MARKER_BEGIN:
            in_block = True
            continue
        if first == MARKER_END:
            in_block = False
            continue
        if not in_block:
            out.append(row)
    return out


def _build_synthetic_rows(
    qualifying: dict, real_alma_ids: set[str]
) -> list[list[str]]:
    """Generate libraries.csv rows for qualifying inventories.

    DETERMINISTIC ORDERING: iterates `sorted(qualifying)` so output rows are
    in ascending InventoryId order. Combined with sorted manifest + sorted
    residue, the entire script output is byte-stable across runs.

    D-01a: assert no synthetic ID collides with a real-Alma ID. Aborts via
    SystemExit on collision (fail-loud).
    """
    out: list[list[str]] = []
    for inv_id in sorted(qualifying):
        qual = qualifying[inv_id]
        sys_id = encode_inventory_sys_id(int(inv_id))
        # D-01a collision check
        if sys_id in real_alma_ids:
            raise SystemExit(
                f"D-01a COLLISION: synthetic sys_id {sys_id} (InventoryId={inv_id}) "
                f"matches a real-Alma row in libraries.csv. Aborting."
            )
        if not is_synthetic_sys_id(sys_id):
            raise SystemExit(
                f"INTERNAL: encode_inventory_sys_id({inv_id}) -> {sys_id} which "
                f"fails is_synthetic_sys_id. Helper invariant violated."
            )
        # CSV-injection re-check at write time (defense-in-depth):
        # _build_qualifying_inventories already excludes injection-leader rows,
        # but if any reach here we abort rather than write.
        title = _resolve_title(qual)
        call_numbers = _generate_call_numbers(qual["canonical_shelfmark"])
        if _has_csv_injection_leader(title) or _has_csv_injection_leader(
            call_numbers
        ):
            raise SystemExit(
                f"INTERNAL: CSV-injection leader reached _build_synthetic_rows for "
                f"InventoryId={inv_id} title={title!r} call_numbers={call_numbers!r}. "
                f"_build_qualifying_inventories filter is broken."
            )
        library_code = qual["library_code"]
        # 8-column shape: system_number, oxford_part_id, call_numbers, library_code,
        # (3 reserved/empty), titles_non_placeholder
        out.append([sys_id, "", call_numbers, library_code, "", "", "", title])
    return out


def _collect_real_alma_ids(rows: list[list[str]]) -> set[str]:
    """Extract sys_ids of real-Alma rows already in libraries.csv (excluding synthetic block)."""
    out = set()
    in_block = False
    for row in rows:
        if not row:
            continue
        first = row[0]
        if first == MARKER_BEGIN:
            in_block = True
            continue
        if first == MARKER_END:
            in_block = False
            continue
        if in_block or first.startswith("#"):
            continue
        # Header row has non-digit first cell
        sys_id = "".join(ch for ch in str(first) if ch.isdigit())
        if sys_id and len(sys_id) >= 10:
            out.add(sys_id)
    return out


# ---------------------------------------------------------------------------
# Audit artifacts — REVIEWS-MODE: manifest is AUTHORITATIVE for Plan 03
# ---------------------------------------------------------------------------


def _write_manifest(path: Path, qualifying: dict) -> None:
    """Write the AUTHORITATIVE qualifying-set manifest.

    REVIEWS-MODE: this manifest is consumed by Plan 03's modified
    scripts/export_fist_enrichment.py as its ONLY InventoryId source for the
    UNION ALL synthetic block. No parallel SQL predicate in Plan 03 — eliminates
    cross-plan divergence (the dominant Phase 85 risk per Codex/Gemini consensus).

    Items sorted by inventory_id ascending for byte-stability.
    """
    items = []
    for inv_id, qual in sorted(qualifying.items()):
        items.append(
            {
                "inventory_id": int(inv_id),
                "synthetic_sys_id": encode_inventory_sys_id(int(inv_id)),
                "source": (
                    "both"
                    if (qual["has_cudl_manifest"] and qual["has_fjms_metadata"])
                    else "cudl_match"
                    if qual["has_cudl_manifest"]
                    else "fjms_metadata"
                ),
                "canonical_shelfmark": qual["canonical_shelfmark"],
                "library_code": qual["library_code"],
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use sort_keys=True for byte-stable JSON across runs.
    path.write_text(
        json.dumps(items, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_residue(path: Path, residue: list[dict]) -> None:
    """Write ambiguity-residue CSV with REVIEWS-MODE ambiguity_kind column.

    Header includes both the legacy columns (cudl_label, fist_signature_ids,
    fist_inventory_ids, leading_char) AND the four required names from the
    plan's sub-feature 3 acceptance criterion (inventory_id, signature_id,
    ambiguity_kind, classmark).

    Phase 86 Plan 02: appended `pattern_guess` as the 9th column for Plan 03
    residue-pattern adjudication (Pass 2 MEDIUM-3: signature stays path-first).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "inventory_id",
                "signature_id",
                "ambiguity_kind",
                "classmark",
                "cudl_label",
                "fist_signature_ids",
                "fist_inventory_ids",
                "leading_char",
                "pattern_guess",  # Phase 86 Plan 02 — D-02c residue category hint.
            ]
        )

        # DETERMINISTIC ORDERING: sort by (cudl_label, ambiguity_kind, classmark).
        # Use string-coerced keys so blank inventory_id/signature_id don't break
        # the comparison against int rows from Phase 85 legacy residue dicts.
        def _sort_key(x: dict) -> tuple:
            return (
                str(x.get("cudl_label", "") or ""),
                str(x.get("ambiguity_kind", "") or ""),
                str(x.get("classmark", "") or ""),
                str(x.get("signature_id", "") or ""),
            )

        for r in sorted(residue, key=_sort_key):
            w.writerow(
                [
                    r.get("inventory_id", ""),
                    r.get("signature_id", ""),
                    r["ambiguity_kind"],
                    r.get("classmark", r.get("cudl_label", "")),
                    r.get("cudl_label", ""),
                    r.get("fist_signature_ids", ""),
                    r.get("fist_inventory_ids", ""),
                    r.get("leading_char", ""),
                    r.get("pattern_guess", ""),  # Phase 86 Plan 02
                ]
            )


def _write_coverage(path: Path, qualifying: dict, residue_count: int) -> None:
    """Write D-03 coverage report with REVIEWS-MODE Phase 86 cross-link.

    SYNTH-03 narrowing note: synthetic rows are discoverable via Title and
    Shelfmark search modes only (genizah_core.py:7372-7373 verified). The
    ROADMAP wording 'all standard search modes' should be amended in a
    follow-up REQUIREMENTS update.
    """
    tier1 = sum(
        1
        for q in qualifying.values()
        if q["has_cudl_manifest"] and q["has_fjms_metadata"]
    )
    tier2 = sum(
        1
        for q in qualifying.values()
        if q["has_cudl_manifest"] and not q["has_fjms_metadata"]
    )
    tier3 = sum(
        1
        for q in qualifying.values()
        if not q["has_cudl_manifest"] and q["has_fjms_metadata"]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# Phase 85 Synthetic Coverage Report\n\n"
        f"Total synthetic rows: {len(qualifying)}\n"
        f"Total ambiguity-excluded keys (D-05a + csv-injection): {residue_count}\n\n"
        f"## Tier 1 (CUDL + FJMS)\n\n"
        f"Count: {tier1}\n\n"
        f"Inventories that have BOTH a CUDL manifest in nli_crossref.db AND\n"
        f"substantive FJMS metadata in FIST.db (catalog title, GenizahTitleText,\n"
        f"bibliography, free-description, full-text, or measurement).\n\n"
        f"## Tier 2 (CUDL only, no FJMS)\n\n"
        f"Count: {tier2}\n\n"
        f"Inventories that have a CUDL manifest but NO substantive FJMS metadata.\n"
        f"Browse renders the CUDL images; no scholarly metadata to display.\n\n"
        f"## Tier 3 (FJMS only, no CUDL)\n\n"
        f"Count: {tier3}\n\n"
        f"Inventories with substantive FJMS metadata but NO CUDL manifest.\n"
        f"Browse renders metadata only (Phase 53 metadata-only path).\n\n"
        f"## SYNTH-03 Search Mode Coverage\n\n"
        f"Synthetic rows are discoverable via **Title** and **Shelfmark** search modes only.\n"
        f"Text/Regex/Responsa modes use the Tantivy index, which has no chunks for synthetic\n"
        f"rows (no transcription text). Per genizah_core.py:7372-7373, only Title and Shelfmark\n"
        f"route through `_execute_metadata_search` (the csv_bank-backed metadata-only path).\n\n"
        f"The ROADMAP wording 'all standard search modes (text/title/shelfmark/Responsa)'\n"
        f"is broader than the current architecture supports. A follow-up REQUIREMENTS\n"
        f"amendment should narrow SYNTH-03 to 'Title and Shelfmark search modes' OR a\n"
        f"future infrastructure phase should add Tantivy stub-rows for synthetic IDs.\n\n"
        f"## Phase 86 Audit Cross-Link\n\n"
        f"AUDIT-01, AUDIT-02, AUDIT-03 are **deferred to Phase 86** per ROADMAP.md\n"
        f"§'Phase 86 -- CUDL Coverage Audit'. They are NOT in scope for Phase 85.\n\n"
        f"- AUDIT-01: `scripts/scan_cudl_orphans.py` re-run after this milestone reports\n"
        f"  fewer than 200 truly-orphan classmarks.\n"
        f"- AUDIT-02: `reports/cudl_coverage.md` (Phase 86 artifact, NOT this file)\n"
        f"  documents the post-milestone breakdown.\n"
        f"- AUDIT-03: 461 NLI Oxford-mislabel rows still resolve correctly.\n\n"
        f"This file (`reports/synthetic_coverage.md`) is the Phase 85 artifact only.\n\n"
        f"---\n\n"
        f"Generated by scripts/generate_synthetic_rows.py\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Generate Phase 85 synthetic libraries.csv rows."
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    # Path overrides — allow running the script from a worktree against
    # main-checkout data dirs (FIST.db / nli_crossref.db are gitignored).
    ap.add_argument("--fist-db", default=str(FIST_DB),
                    help="Path to FIST.db (default: %(default)s)")
    ap.add_argument("--nli-db", default=str(NLI_DB),
                    help="Path to nli_crossref.db (default: %(default)s)")
    ap.add_argument("--csv-path", default=str(CSV_PATH),
                    help="Path to libraries.csv (default: %(default)s)")
    ap.add_argument("--manifest-path", default=str(MANIFEST_PATH),
                    help="Path to write fist_data/synthetic_manifest.json (default: %(default)s)")
    ap.add_argument("--residue-path", default=str(RESIDUE_PATH),
                    help="Path to write reports/synthetic_ambiguity_residue.csv (default: %(default)s)")
    ap.add_argument("--coverage-path", default=str(COVERAGE_PATH),
                    help="Path to write reports/synthetic_coverage.md (default: %(default)s)")
    args = ap.parse_args()

    # Resolve paths from CLI (override module-level defaults).
    fist_db = Path(args.fist_db)
    nli_db = Path(args.nli_db)
    csv_path = Path(args.csv_path)
    manifest_path = Path(args.manifest_path)
    residue_path = Path(args.residue_path)
    coverage_path = Path(args.coverage_path)

    # Open SQLite read-only
    fist_conn = sqlite3.connect(f"file:{fist_db}?mode=ro", uri=True)
    nli_conn = (
        sqlite3.connect(f"file:{nli_db}?mode=ro", uri=True)
        if nli_db.exists()
        else None
    )

    rows, line_terminator = _read_libraries_csv(csv_path)
    real_alma_ids = _collect_real_alma_ids(rows)
    print(f"Real-Alma rows in libraries.csv: {len(real_alma_ids)}")

    # Phase 86 Plan 02 / Pass 2 HIGH-1: build Phase 84 alias index from a
    # synthetic-stripped csv_bank. Without this, a prior --apply run's
    # synthetic block would resolve via lookup_cudl and silently mask a
    # qualifying classmark on the next --apply -> block-wipe risk.
    csv_bank_full = _build_csv_bank_from_rows(rows)
    real_only = _build_real_only_csv_bank(csv_bank_full)
    print(
        f"csv_bank loaded: total={len(csv_bank_full)} "
        f"real_only={len(real_only)} "
        f"stripped_synthetics={len(csv_bank_full) - len(real_only)}"
    )
    build_alias_index(real_only)

    qualifying, ambiguity_residue = _build_qualifying_inventories(fist_conn, nli_conn)
    print(f"Qualifying synthetic inventories: {len(qualifying)}")
    print(f"Ambiguity residue (excluded): {len(ambiguity_residue)}")
    # Breakdown by ambiguity_kind for visibility
    kinds = Counter(r["ambiguity_kind"] for r in ambiguity_residue)
    for kind, count in sorted(kinds.items()):
        print(f"  - {kind}: {count}")

    synthetic_rows = _build_synthetic_rows(qualifying, real_alma_ids)
    print(f"Synthetic rows to emit: {len(synthetic_rows)}")

    if args.dry_run:
        # Phase 86 Plan 02 / Codex HIGH #5: dry-run writes residue to the
        # explicit _dryrun-suffixed path so Plan 03 has a fresh artifact to
        # consume without touching the canonical --apply residue file.
        # When residue_path is at its canonical default, the dry-run path is
        # reports/synthetic_ambiguity_residue_dryrun.csv (explicit suffix —
        # matches the plan's must_have string acceptance criterion).
        # Path-first signature preserved (Pass 2 MEDIUM-3).
        if residue_path == RESIDUE_PATH:
            dryrun_path = ROOT / "reports" / "synthetic_ambiguity_residue_dryrun.csv"
        else:
            dryrun_path = residue_path.with_name(residue_path.stem + "_dryrun.csv")
        _write_residue(dryrun_path, ambiguity_residue)
        print(f"[dry-run] residue: {dryrun_path}")
        for row in synthetic_rows[:5]:
            print(f"  {row[0]}  {row[2]}  {row[3]}  {row[7][:60]}")
        return 0

    # --apply: rewrite libraries.csv with marker-fenced block.
    backup = csv_path.with_suffix(".csv.bak")
    shutil.copy2(csv_path, backup)
    print(f"Backup: {backup}")

    rows_no_block = _strip_existing_synthetic_block(rows)
    final_rows = list(rows_no_block)
    final_rows.append([MARKER_BEGIN, "", "", "", "", "", "", ""])
    final_rows.extend(synthetic_rows)
    final_rows.append([MARKER_END, "", "", "", "", "", "", ""])

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator=line_terminator)
        writer.writerows(final_rows)
    print(f"Wrote {csv_path} ({len(final_rows)} rows; {len(synthetic_rows)} synthetic)")

    _write_manifest(manifest_path, qualifying)
    print(f"Manifest (AUTHORITATIVE for Plan 03): {manifest_path}")
    _write_residue(residue_path, ambiguity_residue)
    print(f"Residue: {residue_path}")
    _write_coverage(coverage_path, qualifying, len(ambiguity_residue))
    print(f"Coverage (with Phase 86 cross-link): {coverage_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
