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
from shared.shelfmark_bridge import cudl_normalize  # noqa: E402

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
# FIST.db harvest — REVIEWS-MODE REVISED (D-02 expanded, D-05a multi-signature,
# explicit ORDER BY, connection-injectable)
# ---------------------------------------------------------------------------


def _build_qualifying_inventories(
    fist_conn: sqlite3.Connection,
    nli_conn: Optional[sqlite3.Connection] = None,
) -> tuple[dict[int, dict], list[dict]]:
    """Walk FIST.db × cambridge_manifests; return (qualifying, ambiguity_residue).

    REVIEWS-MODE REVISIONS (2026-05-08):
    - D-02 EXPANDED: predicate includes bibliography, free-description, full-text,
      and measurement (CatalogMultiSize) signals — not just catalog title.
    - D-05a STRICT: exclude both multi-inventory AND multi-signature ambiguities;
      residue dict includes 'ambiguity_kind' field.
    - DETERMINISTIC ORDERING: every SELECT has explicit ORDER BY.
    - claims tie-break: lowest SignatureId (deterministic), not arbitrary [0].
    - Connections injectable for testability (Gemini LOW accepted).

    SCHEMA MAPPING (deviation from plan's draft column names; see
    85-02-SUMMARY.md):
      - Plan said dbo_BibliographyRef → actual: dbo_UnitBibliographyReference
      - Plan said dbo_FreeDescription → actual: dbo_UnitFreeDescription
      - Plan said dbo_FullText        → actual: dbo_UnitFullText
      - Plan said dbo_UnitSize        → actual: dbo_CatalogMultiSize
      - Plan said sig.Signature (shelfmark column) → actual: shelfmark on
        dbo_Inventory.Shelfmark (Signature does NOT carry the shelfmark text)
      - Plan said cat.TitleHeb / cat.GenizahTitle → actual: cat.Title (already
        Hebrew in this corpus) and cat.GenizahTitleText. No separate TitleHeb
        column exists.

    Returns: (
        qualifying: dict mapping InventoryId -> {
            'canonical_shelfmark': str,
            'title_heb': Optional[str],     # cat.Title (Hebrew per FIST.db corpus)
            'title_eng': Optional[str],     # always None (no English column)
            'genizah_title': Optional[str], # cat.GenizahTitleText
            'library_code': 'CUL' | 'Mosseri',
            'has_cudl_manifest': bool,
            'has_fjms_metadata': bool,
            'cudl_label': str,
        },
        ambiguity_residue: list[dict] with ambiguity_kind ∈
          {'multi_inventory', 'multi_signature', 'csv_injection_leader'}
    )
    """
    # Step 1: Read CUDL classmarks from nli_crossref.db (the "manifest" set).
    cudl_classmarks: set[str] = set()
    if nli_conn is not None:
        cur = nli_conn.execute(
            """
            SELECT normalized_shelfmark
            FROM cambridge_manifests
            ORDER BY normalized_shelfmark
            """
        )
        cudl_classmarks = {row[0] for row in cur if row[0]}

    # Step 2a: Pre-aggregate D-02 EXPANDED signal sets in O(N) time.
    # The "natural" approach (EXISTS subqueries in the main SELECT) is O(N*M)
    # against unindexed FIST tables and runs > 5 minutes on real data
    # (worktree sanity check 2026-05-08). Pre-aggregating each signal table
    # into a Python set takes < 1 second per table.
    bib_sigs = {
        r[0]
        for r in fist_conn.execute(
            "SELECT DISTINCT SignatureId FROM dbo_UnitBibliographyReference "
            "WHERE SignatureId IS NOT NULL ORDER BY SignatureId"
        )
    }
    freedesc_sigs = {
        r[0]
        for r in fist_conn.execute(
            "SELECT DISTINCT SignatureId FROM dbo_UnitFreeDescription "
            "WHERE SignatureId IS NOT NULL ORDER BY SignatureId"
        )
    }
    fulltext_sigs = {
        r[0]
        for r in fist_conn.execute(
            "SELECT DISTINCT SignatureId FROM dbo_UnitFullText "
            "WHERE SignatureId IS NOT NULL ORDER BY SignatureId"
        )
    }
    size_cats = {
        r[0]
        for r in fist_conn.execute(
            "SELECT DISTINCT UnitCatalogRecId FROM dbo_CatalogMultiSize "
            "WHERE UnitCatalogRecId IS NOT NULL ORDER BY UnitCatalogRecId"
        )
    }

    # Step 2b: Walk FIST.db for inventories WITHOUT Alma link.
    # D-02 EXPANDED predicate signals (bib/freedesc/fulltext/size) attached
    # in Python via the pre-aggregated sets above.
    raw_rows = fist_conn.execute(
        """
        SELECT
            inv.InventoryId,
            sig.SignatureId,
            inv.Shelfmark as canonical_shelfmark,
            cat.UnitCatalogRecId,
            cat.Title as title_heb,
            cat.GenizahTitleText as genizah_title
        FROM dbo_Inventory inv
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        LEFT JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
        WHERE alma.AlmaId IS NULL
          AND inv.Shelfmark IS NOT NULL
          AND inv.Shelfmark != ''
        ORDER BY inv.InventoryId, sig.SignatureId, cat.UnitCatalogRecId
        """
    ).fetchall()

    rows = []
    for inv_id, sig_id, shelfmark, cat_id, title_heb, gen_title in raw_rows:
        rows.append(
            (
                inv_id,
                sig_id,
                shelfmark,
                cat_id,
                title_heb,
                gen_title,
                int(sig_id in bib_sigs),
                int(sig_id in freedesc_sigs),
                int(sig_id in fulltext_sigs),
                int(cat_id is not None and cat_id in size_cats),
            )
        )

    # Step 3: D-05a STRICT — group by normalized CUDL key.
    # Exclude when key matches:
    #   (a) multiple distinct InventoryIds → ambiguity_kind='multi_inventory'
    #   (b) single InventoryId BUT multiple distinct SignatureIds → ambiguity_kind='multi_signature'
    by_key: dict[str, list[tuple]] = defaultdict(list)
    for row_tuple in rows:
        (
            inv_id,
            sig_id,
            shelfmark,
            cat_id,
            title_heb,
            gen_title,
            has_bib,
            has_freedesc,
            has_fulltext,
            has_size,
        ) = row_tuple
        if not shelfmark:
            continue
        key = cudl_normalize(shelfmark)
        if not key:
            continue
        by_key[key].append(
            (
                inv_id,
                sig_id,
                shelfmark,
                cat_id,
                title_heb,
                gen_title,
                bool(has_bib),
                bool(has_freedesc),
                bool(has_fulltext),
                bool(has_size),
            )
        )

    qualifying: dict[int, dict] = {}
    ambiguity_residue: list[dict] = []

    # Process keys in DETERMINISTIC sorted order.
    for key in sorted(by_key.keys()):
        claims = by_key[key]
        distinct_inv = sorted({c[0] for c in claims})
        distinct_sig = sorted({c[1] for c in claims})

        # D-05a (a) — multiple inventories → ambiguous.
        if len(distinct_inv) > 1:
            ambiguity_residue.append(
                {
                    "cudl_label": key,
                    "ambiguity_kind": "multi_inventory",
                    "fist_signature_ids": "|".join(str(s) for s in distinct_sig),
                    "fist_inventory_ids": "|".join(str(i) for i in distinct_inv),
                    "leading_char": "",
                    "inventory_id": distinct_inv[0],
                    "signature_id": distinct_sig[0],
                    "classmark": key,
                }
            )
            continue

        # D-05a (b) REVIEWS-MODE — multiple signatures (recto/verso/copies) → ambiguous.
        if len(distinct_sig) > 1:
            ambiguity_residue.append(
                {
                    "cudl_label": key,
                    "ambiguity_kind": "multi_signature",
                    "fist_signature_ids": "|".join(str(s) for s in distinct_sig),
                    "fist_inventory_ids": "|".join(str(i) for i in distinct_inv),
                    "leading_char": "",
                    "inventory_id": distinct_inv[0],
                    "signature_id": distinct_sig[0],
                    "classmark": key,
                }
            )
            continue

        # Unambiguous: exactly 1 inventory, 1 signature.
        # DETERMINISTIC tie-break: pick the claim with lowest (inv, sig, cat).
        # SQL already returns sorted, but re-sort defensively for clarity.
        claim = sorted(claims, key=lambda c: (c[0], c[1], c[3] or 0))[0]
        (
            inv_id,
            sig_id,
            shelfmark,
            cat_id,
            title_heb,
            gen_title,
            has_bib,
            has_freedesc,
            has_fulltext,
            has_size,
        ) = claim

        # CSV-injection FAIL-LOUD (Codex MEDIUM): exclude rows with leading
        # =/+/-/@ in title or shelfmark; log to residue.
        injection_leader = ""
        if _has_csv_injection_leader(shelfmark):
            injection_leader = str(shelfmark)[:1]
        elif _has_csv_injection_leader(title_heb):
            injection_leader = str(title_heb)[:1]
        elif _has_csv_injection_leader(gen_title):
            injection_leader = str(gen_title)[:1]
        if injection_leader:
            ambiguity_residue.append(
                {
                    "cudl_label": key,
                    "ambiguity_kind": "csv_injection_leader",
                    "fist_signature_ids": str(sig_id),
                    "fist_inventory_ids": str(inv_id),
                    "leading_char": injection_leader,
                    "inventory_id": inv_id,
                    "signature_id": sig_id,
                    "classmark": key,
                }
            )
            continue

        has_cudl = key in cudl_classmarks
        # D-02 EXPANDED (REVIEWS-MODE): qualifying = title OR genizah_title
        # OR bib OR freedesc OR fulltext OR size.
        has_fjms = bool(
            title_heb
            or gen_title
            or has_bib
            or has_freedesc
            or has_fulltext
            or has_size
        )
        if not (has_cudl or has_fjms):
            continue

        qualifying[inv_id] = {
            "canonical_shelfmark": shelfmark,
            "title_heb": title_heb,
            "title_eng": None,  # No English title column in dbo_UnitCatalogRec
            "genizah_title": gen_title,
            "library_code": _classify_library_code(shelfmark),
            "has_cudl_manifest": has_cudl,
            "has_fjms_metadata": has_fjms,
            "cudl_label": key,
        }

    return qualifying, ambiguity_residue


def _classify_library_code(shelfmark: str) -> str:
    """D-15: synthetic rows reuse 'CUL' or 'Mosseri'; no new codes."""
    s = (shelfmark or "").lower()
    if "moss" in s:
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
            ]
        )
        # DETERMINISTIC ORDERING: sort by (cudl_label, ambiguity_kind, signature_id).
        for r in sorted(
            residue,
            key=lambda x: (x["cudl_label"], x["ambiguity_kind"], x.get("signature_id", 0)),
        ):
            w.writerow(
                [
                    r.get("inventory_id", ""),
                    r.get("signature_id", ""),
                    r["ambiguity_kind"],
                    r.get("classmark", r["cudl_label"]),
                    r["cudl_label"],
                    r["fist_signature_ids"],
                    r["fist_inventory_ids"],
                    r.get("leading_char", ""),
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
