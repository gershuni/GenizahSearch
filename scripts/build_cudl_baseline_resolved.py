"""Capture (original_shelfmark, pre_phase_lookup_key, manifest_url) triples that
resolved under the PRE-Phase-84 runtime path.

Codex HIGH #5 (Round 2): the test invariant is "previously-resolved still resolves",
so the baseline must reflect what the runtime actually resolved — NOT the entire
cambridge_manifests universe.

Round 3 Codex HIGH #5: the schema records manifest_url so the post-phase test can
assert URL EQUALITY (get_cambridge_manifest_with_bridge(sm) == manifest_url) instead
of merely non-None — protecting against silent misrouting regressions.

Round 3 Codex HIGH #3: MetadataManager() does NOT auto-load csv_bank. Call
mm._load_csv_bank() explicitly.
"""
from __future__ import annotations
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from genizah_core import normalize_shelfmark, construct_mosseri_cudl_label, MetadataManager
from shared.nli_crossref_service import NliCrossrefService

OUT = ROOT / "tests" / "fixtures" / "cudl_baseline_resolved.csv"

# Locate nli_crossref.db: try the worktree path first, then the main project root.
# The worktree's nli_data/ may be absent if the db was not copied to the worktree;
# fall back to the canonical location one level up (the main repo root).
_NLI_DB_CANDIDATES = [
    ROOT / "nli_data" / "nli_crossref.db",
    ROOT.parent.parent / "nli_data" / "nli_crossref.db",  # main repo if in worktree
]


def _find_nli_db() -> str | None:
    for p in _NLI_DB_CANDIDATES:
        if p.exists():
            return str(p)
    # Walk upward from ROOT to find nli_data/nli_crossref.db in any ancestor
    cur = ROOT.parent
    for _ in range(6):
        candidate = cur / "nli_data" / "nli_crossref.db"
        if candidate.exists():
            return str(candidate)
        cur = cur.parent
    return None


def main() -> int:
    db_path = _find_nli_db()
    if not db_path:
        print(f"ERROR: nli_crossref.db not found in candidates: {[str(p) for p in _NLI_DB_CANDIDATES]}",
              file=sys.stderr)
        print("Try copying or symlinking nli_data/nli_crossref.db to the worktree root.", file=sys.stderr)
        return 1

    svc = NliCrossrefService(db_path=db_path)
    mm = MetadataManager()
    # Round 3 Codex HIGH #3: __init__ does NOT auto-load. Trigger heavy cache load.
    mm._load_csv_bank()
    assert len(mm.csv_bank) > 100000, (
        f"csv_bank load failed: {len(mm.csv_bank)} rows. "
        "Verify libraries.csv is present and _load_csv_bank() ran."
    )

    resolved = []  # list of (original_shelfmark, pre_phase_lookup_key, manifest_url)
    seen_keys: set[tuple] = set()

    for sys_id, data in mm.csv_bank.items():
        lib = data.get('library_code') or ''
        if lib not in ('CUL', 'Mosseri'):
            continue
        variants = list(data.get('call_numbers_raw') or [])
        primary = data.get('shelfmark')
        if primary and primary not in variants:
            variants.append(primary)
        for variant in variants:
            # Pre-bridge canonical path
            try:
                norm = normalize_shelfmark(variant)
            except Exception:
                norm = None
            recorded = False
            if norm:
                url = svc.get_cambridge_manifest(norm)
                if url:
                    key = ('canonical', variant, norm)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        resolved.append((variant, norm, url))
                        recorded = True

            # Pre-bridge Mosseri path (only if canonical did not already resolve)
            if not recorded and lib == 'Mosseri':
                label = construct_mosseri_cudl_label(variant)
                if label:
                    url = svc.get_cambridge_manifest_by_label(label)
                    if url:
                        key = ('label', variant, label)
                        if key not in seen_keys:
                            seen_keys.add(key)
                            resolved.append((variant, label, url))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open('w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['original_shelfmark', 'pre_phase_lookup_key', 'manifest_url'])
        for variant, key, url in resolved:
            w.writerow([variant, key, url])
    print(f"Wrote {len(resolved)} baseline (shelfmark, pre-phase-key, url) rows to {OUT}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
