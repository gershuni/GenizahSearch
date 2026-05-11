"""Phase 86 bridge-aware CUDL coverage scanner.

Walks nli_crossref.db.cambridge_manifests; for each CUDL classmark classifies
into one of 5 + 2 tiers using shared.shelfmark_bridge.lookup_cudl AND
shared.fist_cudl_bridge.explain_fist_by_cudl AND shared.synthetic_sys_id.is_synthetic_sys_id:

  - phase84_hit:                       lookup_cudl(classmark) returns a sys_id AND
                                       is_synthetic_sys_id(sys_id) is False
                                       (Phase 84 alias index resolves to a REAL libraries.csv row)
  - phase86_synthetic:                 lookup_cudl(classmark) returns a sys_id AND
                                       is_synthetic_sys_id(sys_id) is True
                                       (Phase 84 alias index resolves to a NEW Phase 86 synthetic row)
                                       -- Pass 2 HIGH-1 routing
                                       OR (lookup_cudl returns None) AND
                                       explain_fist_by_cudl returns 'single' AND rec.has_alma is False
                                       AND inventory_id IS in synthetic_manifest.json
                                       -- Pass 3 HIGH-1 (Codex) manifest membership routing
  - phase86_existing_alma_candidate:   lookup_cudl returns None AND explain_fist_by_cudl returns
                                       'single' AND rec.has_alma is True
                                       (Pass 2 HIGH-3 RENAMED tier — bridge resolves to
                                       existing-Alma libraries.csv row that the user-typed CUDL
                                       form does NOT actually reach at runtime; documented as a
                                       candidate but NOT counted as resolution)
  - phase86_excluded_parent_shadow:    Pass 3 HIGH-1 (Codex) NEW tier — no-Alma single bridge hit
                                       whose inventory_id is in reports/synthetic_parent_shelfmarks.csv
                                       (D-06 parent-shadow filter rejected)
  - phase86_excluded_csv_injection:    Pass 3 HIGH-1 (Codex) NEW tier — reserved for csv-injection
                                       rejections (not currently classified separately at scan time;
                                       these route to phase86_residue as the safe fallback)
  - multi_inventory_ambiguous:         explain_fist_by_cudl returns 'multi_inventory_ambiguous'
                                       (D-04a exclude)
  - phase86_residue:                   explain_fist_by_cudl returns 'not_found'
                                       (D-02b residue — Pass 2 HIGH-3 RENAMED from 'truly_orphan'
                                       for cudl_coverage.md framing)
                                       OR the no-Alma single bridge hit's inventory_id is NOT in
                                       synthetic_manifest.json AND NOT in parent-shadow set
                                       (Pass 3 HIGH-1 fall-through — safest classification)

Writes reports/cudl_coverage_post_phase86.csv with per-classmark classification.
"""
from __future__ import annotations
import csv
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.shelfmark_bridge import lookup_cudl, build_alias_index  # noqa: E402
from shared.fist_cudl_bridge import build_fist_alias_index, explain_fist_by_cudl  # noqa: E402
from shared.synthetic_sys_id import is_synthetic_sys_id  # noqa: E402  Pass 2 HIGH-1
from shared.synthetic_sys_id import decode_inventory_id  # noqa: E402  Pass 3 LOW-86-04 Gemini

FIST_DB = ROOT / "fist_data" / "FIST.db"
NLI_DB = ROOT / "nli_data" / "nli_crossref.db"
OUT_CSV = ROOT / "reports" / "cudl_coverage_post_phase86.csv"

# Pass 2 HIGH-3 — renamed tiers.
TIER_PHASE84 = "phase84_hit"
TIER_SYNTHETIC = "phase86_synthetic"
TIER_EXISTING_ALMA_CANDIDATE = "phase86_existing_alma_candidate"
TIER_MULTI_INV = "multi_inventory_ambiguous"
TIER_RESIDUE = "phase86_residue"

# Pass 3 HIGH-1 (Codex) — additional tier strings for excluded-but-classified
# no-Alma bridge hits. These were previously misrouted to phase86_synthetic.
TIER_EXCLUDED_PARENT_SHADOW = "phase86_excluded_parent_shadow"
TIER_EXCLUDED_CSV_INJECTION = "phase86_excluded_csv_injection"


def _sys_id_of(lookup_result) -> Optional[str]:
    """Phase 84 lookup_cudl returns Optional[Dict[str, str]] (sys_id key in dict)
    OR None. Defensive extraction. Returns sys_id string or None."""
    if lookup_result is None:
        return None
    if isinstance(lookup_result, dict):
        return lookup_result.get("sys_id")
    return str(lookup_result)


def _load_synthetic_manifest_inventory_ids() -> set:
    """Pass 3 HIGH-1: load the inventory_id set from
    ``fist_data/synthetic_manifest.json``. This is the AUTHORITATIVE list
    of synthetics actually emitted by Plan 02; D-06 parent-shadow and
    CSV-injection rejections leave inventories OUT of the manifest, so a
    no-Alma bridge hit alone is NOT enough to conclude phase86_synthetic.

    Returns an empty set if the manifest is missing (e.g., scanner runs
    before --apply); in that case every no-Alma bridge hit routes to
    phase86_residue and the scanner reports cleanly.
    """
    manifest_path = ROOT / "fist_data" / "synthetic_manifest.json"
    if not manifest_path.exists():
        return set()
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    ids = set()
    for row in data:
        inv_id = row.get("inventory_id") if isinstance(row, dict) else None
        if inv_id is None:
            continue
        try:
            ids.add(int(inv_id))
        except (TypeError, ValueError):
            continue
    return ids


# Module-level cache: loaded once per scanner run.
_SYNTHETIC_MANIFEST_INV_IDS = None


def _load_parent_shadow_inv_ids() -> set:
    """Pass 3 HIGH-1: Best-effort lookup of inventories rejected by the
    D-06 parent-shadow filter. Reads ``reports/synthetic_parent_shelfmarks.csv``
    (Phase 85 audit) and returns the `inventory_id` column. Missing file →
    empty set (callers must still route excluded hits gracefully)."""
    path = ROOT / "reports" / "synthetic_parent_shelfmarks.csv"
    if not path.exists():
        return set()
    out = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            v = row.get("inventory_id")
            if not v:
                continue
            try:
                out.add(int(v))
            except ValueError:
                continue
    return out


_PARENT_SHADOW_INV_IDS = None


def classify_classmark(classmark: str):
    """Return (tier, fist_inventory_id, notes).

    Pass 2 HIGH-1 + HIGH-3 + Pass 3 HIGH-1 (Codex):

    The fallback branch for a no-Alma single-inventory bridge hit MUST
    consult ``fist_data/synthetic_manifest.json`` before declaring
    ``phase86_synthetic``. Inventories rejected by D-06 parent-shadow
    or CSV-injection are NOT in the manifest; classifying them as
    ``phase86_synthetic`` would overstate coverage. They are instead
    routed to:

      - ``phase86_excluded_parent_shadow`` (if the FIST inventory id
        appears in ``reports/synthetic_parent_shelfmarks.csv``);
      - ``phase86_excluded_csv_injection`` (a residual category — we
        cannot positively identify these here without re-reading the
        generation residue CSV, so the scanner labels them generically
        when the manifest gap is unexplained by parent shadow);
      - ``phase86_residue`` as the safe fallback.
    """
    global _SYNTHETIC_MANIFEST_INV_IDS, _PARENT_SHADOW_INV_IDS
    if _SYNTHETIC_MANIFEST_INV_IDS is None:
        _SYNTHETIC_MANIFEST_INV_IDS = _load_synthetic_manifest_inventory_ids()
    if _PARENT_SHADOW_INV_IDS is None:
        _PARENT_SHADOW_INV_IDS = _load_parent_shadow_inv_ids()

    # Phase 84 alias-index lookup.
    lr = lookup_cudl(classmark)
    sys_id = _sys_id_of(lr)
    if sys_id is not None:
        # Pass 2 HIGH-1: synthetic-resolving hits MUST be classified as phase86_synthetic,
        # NOT phase84_hit. is_synthetic_sys_id(sys_id) is the predicate.
        if is_synthetic_sys_id(sys_id):
            # Pass 3 LOW-86-04 (Gemini): populate the fist_inventory_id column
            # via decode_inventory_id(sys_id) instead of leaving it empty.
            inv = decode_inventory_id(sys_id)
            inv_str = str(inv) if inv is not None else ""
            return (
                TIER_SYNTHETIC,
                inv_str,
                f"synthetic libraries.csv sys_id={sys_id} inv_id={inv_str}",
            )
        return (TIER_PHASE84, "", f"libraries.csv sys_id={sys_id}")

    # Phase 86 bridge resolution for un-Phase-84-resolved classmarks.
    status, entries = explain_fist_by_cudl(classmark)
    if status == "not_found":
        return (TIER_RESIDUE, "", "no FIST candidate via Phase 86 bridge (D-02b residue)")
    if status == "multi_inventory_ambiguous":
        inv_ids = ",".join(str(e.inventory_id) for e in entries)
        return (TIER_MULTI_INV, inv_ids, "multiple FIST InventoryIds — D-04a exclude")
    # status == 'single'
    rec = entries[0]
    if rec.has_alma:
        return (
            TIER_EXISTING_ALMA_CANDIDATE,
            str(rec.inventory_id),
            "FIST bridge resolves to existing-Alma libraries.csv row; user-typed CUDL form "
            "does NOT reach this row at runtime (app shelfmark search depends on Phase 84 alias coverage). "
            "Documented candidate — NOT counted as resolution.",
        )

    # rec.has_alma is False. Pass 3 HIGH-1 (Codex): we MUST consult the
    # synthetic_manifest.json to decide whether this no-Alma bridge hit
    # is actually a synthetic row (emitted by Plan 02) or an excluded
    # candidate that was REJECTED by the generation pipeline (D-06 parent
    # shadow or CSV-injection or any future no-emit condition).
    inv_id = int(rec.inventory_id)
    if inv_id in _SYNTHETIC_MANIFEST_INV_IDS:
        return (
            TIER_SYNTHETIC,
            str(inv_id),
            "Phase 86 synthetic libraries.csv row (no-Alma FIST resolution; "
            "manifest membership confirmed)",
        )
    if inv_id in _PARENT_SHADOW_INV_IDS:
        return (
            TIER_EXCLUDED_PARENT_SHADOW,
            str(inv_id),
            "FIST bridge resolves to a no-Alma inventory excluded by D-06 "
            "parent-shadow filter; NOT emitted to synthetic_manifest.json. "
            "Documented exclusion — NOT counted as coverage.",
        )
    # Inventory not in manifest and not parent-shadow: most likely a
    # CSV-injection rejection (the row appeared in the dryrun residue but
    # was skipped at emit time) or a generation-time no-emit condition we
    # haven't enumerated. Route to phase86_residue as the safe fallback;
    # downstream cudl_coverage.md should describe this honestly.
    return (
        TIER_RESIDUE,
        str(inv_id),
        "no-Alma FIST bridge hit NOT present in synthetic_manifest.json "
        "(likely CSV-injection rejection or other generation-time no-emit); "
        "Pass 3 HIGH-1: routed to residue, NOT counted as synthetic coverage.",
    )


def main() -> int:
    # Build BOTH bridge indexes. Phase 84 needs libraries.csv via csv_bank;
    # Phase 86 needs FIST.db. The csv_bank loader must mirror what
    # scripts/scan_cudl_orphans.py uses (genizah_core import path).
    from genizah_core import csv_bank
    # Pass 2 HIGH-1 cross-reference: Plan 02's _build_real_only_csv_bank is the
    # generation-time filter. AT SCAN TIME we WANT lookup_cudl to resolve to
    # synthetic sys_ids (so the scanner can classify them as phase86_synthetic
    # via is_synthetic_sys_id). Therefore the scanner uses the FULL csv_bank,
    # NOT the synthetic-stripped one.
    build_alias_index(csv_bank)

    fist_conn = sqlite3.connect(f"file:{FIST_DB}?mode=ro", uri=True)
    nli_conn = sqlite3.connect(f"file:{NLI_DB}?mode=ro", uri=True)
    try:
        build_fist_alias_index(fist_conn)
        cudl_rows = nli_conn.execute(
            "SELECT normalized_shelfmark FROM cambridge_manifests "
            "WHERE normalized_shelfmark IS NOT NULL AND normalized_shelfmark != '' "
            "ORDER BY normalized_shelfmark"
        ).fetchall()
    finally:
        nli_conn.close()

    rows = []
    counts: Counter = Counter()
    for (classmark,) in cudl_rows:
        tier, inv_id, notes = classify_classmark(classmark)
        rows.append((classmark, tier, inv_id, notes))
        counts[tier] += 1

    fist_conn.close()

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["classmark", "tier", "fist_inventory_id", "notes"])
        for r in rows:
            writer.writerow(r)

    total = sum(counts.values())
    print(f"Phase 86 bridge-aware coverage scan: {total} CUDL classmarks classified.")
    print()
    print(f"{'Tier':40s}  {'Count':>8s}  {'% of total':>10s}")
    # Print all tiers we may emit, even if zero, for stable reports.
    for tier in (
        TIER_PHASE84,
        TIER_SYNTHETIC,
        TIER_EXISTING_ALMA_CANDIDATE,
        TIER_EXCLUDED_PARENT_SHADOW,
        TIER_EXCLUDED_CSV_INJECTION,
        TIER_MULTI_INV,
        TIER_RESIDUE,
    ):
        n = counts.get(tier, 0)
        pct = (100.0 * n / total) if total else 0.0
        print(f"{tier:40s}  {n:>8d}  {pct:>9.2f}%")
    print()
    print(f"Output: {OUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
