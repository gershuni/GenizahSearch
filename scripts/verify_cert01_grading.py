"""CERT-01 Task 3 -- mechanical/estimand grading-STARTED validator.

Phase 135, plan 135-09, Task 3. Implements the twelve forge-resistant checks
`docs/specs/discovery-cert01-protocol.md` §5.5/§10 requires before the
CERT-01 "grading STARTED" signal (D-02) can be trusted: it RECOMPUTES every
hash from the deployed sidecar / tracked artifacts and compares -- it never
trusts a stored value on its own word.

Usage:
    python -X utf8 scripts/verify_cert01_grading.py
    (exit 0 iff all twelve checks pass; a human-readable report is printed
    either way)

Checks (numbered to match the plan/protocol exactly):
    1.  the deck exists with ~200-250 candidate cards
    2.  the pre-registration report_id recomputes stably
    3.  the deck manifest's prereg_report_id matches + its deck_manifest_hash
        matches the actual drawn deck
    4.  every recorded verdict uses a value from the allowed NON-EMPTY vocabulary
    5.  every ledger uid is a member of the frozen deck
    6.  grader attribution is present on each verdict (>=1 verdict exists)
    7.  NO grader-visible demotion field exists in the deck
    8.  POPULATION REPRODUCIBILITY -- population_hash + stratum_counts recompute
        against the deployed sidecar
    9.  DECK ALLOCATION -- deck manifest's per-stratum/gold/confirmation counts
        match the pre-registration's allocations
    10. INPUT-HASH PINNING -- the four frozen input hashes + db_content_hash
        recompute against the deployed sidecar/manifest
    11. CLUSTER-MAP REPRODUCIBILITY -- cluster_map_hash recomputes against the
        deployed sidecar
    12. CROSSWALK-HASH PINNING -- crosswalk_sha256 recomputes against the
        deployed sidecar meta

Masking: this script and its output print ONLY opaque uids (`page_id|w000xxx`
form), counts, and hashes -- never a title, never restricted-corpus content.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import cert01_frame as cf  # noqa: E402

PHASE_DIR = REPO_ROOT / ".planning" / "phases" / "135-precision-certificate-confidence-bands"
PREREG_PATH = PHASE_DIR / "cert01_prereg.json"
DECK_MANIFEST_PATH = PHASE_DIR / "cert01_deck_manifest.json"

SIDECAR_DB = REPO_ROOT / "discovery_data" / "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db"
MANIFEST_PATH = REPO_ROOT / "discovery_data" / "manifest.json"
RESEARCH_DB = REPO_ROOT / "same_work_spike" / "probe" / "data" / "fullcorpus_v2.db"
CANONICAL_MERGES_PATH = REPO_ROOT / "discovery_data" / "v2_canonical_merges.build.json"
COMPOSITION_DATES_PATH = REPO_ROOT / "discovery_data" / "composition_dates.json"
SEFTJA_DATES_PATH = REPO_ROOT / "same_work_spike" / "probe" / "rsource" / "data" / "seftja_dates.json"
CROSSWALK_PATH = REPO_ROOT / "discovery_data" / "crosswalk.json"

DECK_KEY_PATH = REPO_ROOT / "same_work_spike" / "probe" / "data" / "cert01_deck_key.json"
VERDICTS_LEDGER_PATH = REPO_ROOT / "same_work_spike" / "probe" / "review" / "cert01_deck_verdicts.json"

CANDIDATE_DECK_SIZE_RANGE = (200, 250)
DEMOTION_FIELDS = ("later_shared_text", "routing_status")


class CheckFailure(Exception):
    pass


def _fail(msg: str):
    raise CheckFailure(msg)


def _load_json(path: Path) -> dict:
    if not path.exists():
        _fail(f"required file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_ledger(path: Path) -> list:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        _fail(f"verdict ledger at {path} must be a JSON list")
    return data


def _deck_manifest_hash_of(deck_cards: list) -> str:
    ordered_uids = [c["uid"] for c in deck_cards]
    payload = json.dumps({"uids": ordered_uids, "cards": deck_cards}, sort_keys=True,
                        separators=(",", ":"), ensure_ascii=False)
    import hashlib
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Checks 1-7: deck + report_id + manifest + ledger + no-demotion-tag
# ---------------------------------------------------------------------------

def check_1_deck_size(ctx: dict):
    n = ctx["deck_manifest"].get("candidate_count")
    if n is None:
        _fail("deck manifest missing candidate_count")
    lo, hi = CANDIDATE_DECK_SIZE_RANGE
    if not (lo <= n <= hi):
        _fail(f"candidate deck size {n} outside the protocol's {lo}-{hi} card range")
    actual_candidates = sum(1 for c in ctx["deck_cards"] if c.get("role") == "candidate")
    if actual_candidates != n:
        _fail(f"deck manifest candidate_count={n} but the deck file has {actual_candidates} candidate cards")


def check_2_report_id_recomputes(ctx: dict):
    prereg = ctx["prereg"]
    recomputed = cf.compute_report_id(prereg)
    if recomputed != prereg.get("report_id"):
        _fail(f"report_id recompute mismatch: stored {prereg.get('report_id')!r} != recomputed {recomputed!r} "
              "-- cert01_prereg.json may have been edited after the draw")
    ctx["recomputed_report_id"] = recomputed


def check_3_deck_manifest_reference_and_hash(ctx: dict):
    dm = ctx["deck_manifest"]
    recomputed_report_id = ctx["recomputed_report_id"]
    if dm.get("prereg_report_id") != recomputed_report_id:
        _fail(f"deck manifest prereg_report_id {dm.get('prereg_report_id')!r} != "
              f"recomputed pre-registration report_id {recomputed_report_id!r}")
    recomputed_hash = _deck_manifest_hash_of(ctx["deck_cards"])
    if recomputed_hash != dm.get("deck_manifest_hash"):
        _fail(f"deck_manifest_hash mismatch: stored {dm.get('deck_manifest_hash')!r} != "
              f"recomputed {recomputed_hash!r} -- the deck file does not match its bound manifest")


def check_4_verdict_vocab(ctx: dict):
    allowed = set(ctx["prereg"].get("allowed_verdicts") or [])
    if not allowed:
        _fail("pre-registration carries no allowed_verdicts vocabulary")
    for entry in ctx["ledger"]:
        v = entry.get("verdict")
        if not v or v not in allowed:
            _fail(f"ledger entry uid={entry.get('uid')!r} has verdict {v!r}, "
                  f"not in the allowed non-empty vocabulary {sorted(allowed)}")


def check_5_uid_membership(ctx: dict):
    deck_uids = {c["uid"] for c in ctx["deck_cards"]}
    for entry in ctx["ledger"]:
        uid = entry.get("uid")
        if uid not in deck_uids:
            _fail(f"ledger entry uid={uid!r} is not a member of the frozen deck")


def check_6_grader_attribution(ctx: dict):
    ledger = ctx["ledger"]
    if not ledger:
        _fail("verdict ledger is empty -- grading has NOT started "
              "(>=1 attributed verdict is required)")
    for entry in ledger:
        grader = entry.get("grader")
        if not grader or not str(grader).strip():
            _fail(f"ledger entry uid={entry.get('uid')!r} has no grader attribution")


def check_7_no_demotion_field(ctx: dict):
    for c in ctx["deck_cards"]:
        for field in DEMOTION_FIELDS:
            if field in c:
                _fail(f"deck card uid={c.get('uid')!r} carries a grader-visible "
                      f"demotion field {field!r} -- catalogue-blind invariant violated")


# ---------------------------------------------------------------------------
# Checks 8-12: recompute against the deployed sidecar / tracked input files
# ---------------------------------------------------------------------------

def check_8_population_reproducibility(ctx: dict):
    prereg = ctx["prereg"]
    estimand_rows = ctx["estimand_rows"]
    pop_hash = cf.population_hash(estimand_rows)
    if pop_hash != prereg.get("population_hash"):
        _fail(f"population_hash recompute mismatch: prereg {prereg.get('population_hash')!r} "
              f"!= recomputed {pop_hash!r}")
    counts = cf.stratum_counts(estimand_rows)
    if counts != prereg.get("stratum_counts"):
        _fail(f"stratum_counts recompute mismatch: prereg {prereg.get('stratum_counts')!r} "
              f"!= recomputed {counts!r}")


def check_9_deck_allocation(ctx: dict):
    prereg = ctx["prereg"]
    dm = ctx["deck_manifest"]
    if dm.get("stratum_drawn_counts") != prereg.get("stratum_allocation"):
        _fail(f"deck manifest stratum_drawn_counts {dm.get('stratum_drawn_counts')!r} != "
              f"pre-registration stratum_allocation {prereg.get('stratum_allocation')!r}")
    if dm.get("gold_allocation_drawn") != prereg.get("gold_allocation", {}).get("n"):
        _fail(f"deck manifest gold_allocation_drawn {dm.get('gold_allocation_drawn')!r} != "
              f"pre-registration gold_allocation.n {prereg.get('gold_allocation', {}).get('n')!r}")
    if dm.get("confirmation_allocation") != prereg.get("confirmation_allocation"):
        _fail("deck manifest confirmation_allocation does not match the pre-registration's "
              "confirmation_allocation")


def check_10_input_hash_pinning(ctx: dict):
    prereg = ctx["prereg"]
    hashes = ctx["input_hashes"]
    for key in ("canonical_merges_sha256", "composition_dates_sha256",
                "seftja_dates_sha256", "db_content_hash"):
        if hashes[key] != prereg.get(key):
            _fail(f"{key} recompute mismatch: prereg {prereg.get(key)!r} != recomputed {hashes[key]!r}")


def check_11_cluster_map_reproducibility(ctx: dict):
    prereg = ctx["prereg"]
    clus_hash = cf.cluster_map_hash(ctx["estimand_rows"])
    if clus_hash != prereg.get("cluster_map_hash"):
        _fail(f"cluster_map_hash recompute mismatch: prereg {prereg.get('cluster_map_hash')!r} "
              f"!= recomputed {clus_hash!r}")


def check_12_crosswalk_hash_pinning(ctx: dict):
    prereg = ctx["prereg"]
    hashes = ctx["input_hashes"]
    if hashes["crosswalk_sha256"] != prereg.get("crosswalk_sha256"):
        _fail(f"crosswalk_sha256 recompute mismatch: prereg {prereg.get('crosswalk_sha256')!r} "
              f"!= recomputed {hashes['crosswalk_sha256']!r}")


CHECKS: List[Tuple[int, str, Callable[[dict], None]]] = [
    (1, "deck exists with ~200-250 candidate cards", check_1_deck_size),
    (2, "pre-registration report_id recomputes stably", check_2_report_id_recomputes),
    (3, "deck manifest references + binds the pre-registration", check_3_deck_manifest_reference_and_hash),
    (4, "every recorded verdict uses the allowed non-empty vocabulary", check_4_verdict_vocab),
    (5, "every ledger uid is a member of the frozen deck", check_5_uid_membership),
    (6, "grader attribution present (>=1 verdict)", check_6_grader_attribution),
    (7, "no grader-visible demotion field in the deck", check_7_no_demotion_field),
    (8, "population/stratum reproducibility against the deployed sidecar", check_8_population_reproducibility),
    (9, "deck allocation matches the pre-registration", check_9_deck_allocation),
    (10, "input-hash pinning against the deployed sidecar/manifest", check_10_input_hash_pinning),
    (11, "cluster_map_hash reproducibility against the deployed sidecar", check_11_cluster_map_reproducibility),
    (12, "crosswalk_sha256 pinning against the deployed sidecar meta", check_12_crosswalk_hash_pinning),
]


def build_context(*, prereg_path=PREREG_PATH, deck_manifest_path=DECK_MANIFEST_PATH,
                  deck_key_path=DECK_KEY_PATH, ledger_path=VERDICTS_LEDGER_PATH,
                  sidecar_db=SIDECAR_DB, research_db=RESEARCH_DB, manifest_path=MANIFEST_PATH,
                  canonical_merges_path=CANONICAL_MERGES_PATH,
                  composition_dates_path=COMPOSITION_DATES_PATH,
                  seftja_dates_path=SEFTJA_DATES_PATH, crosswalk_path=CROSSWALK_PATH) -> dict:
    prereg = _load_json(Path(prereg_path))
    deck_manifest = _load_json(Path(deck_manifest_path))
    deck_file = _load_json(Path(deck_key_path))
    deck_cards = deck_file.get("cards", [])
    ledger = _load_ledger(Path(ledger_path))

    estimand_rows = cf.compute_estimand_rows(str(sidecar_db), str(research_db))
    input_hashes = cf.read_input_hashes(
        str(sidecar_db), str(manifest_path),
        canonical_merges_path=str(canonical_merges_path),
        composition_dates_path=str(composition_dates_path),
        seftja_dates_path=str(seftja_dates_path),
        crosswalk_path=str(crosswalk_path),
    )

    return {
        "prereg": prereg,
        "deck_manifest": deck_manifest,
        "deck_cards": deck_cards,
        "ledger": ledger,
        "estimand_rows": estimand_rows,
        "input_hashes": input_hashes,
    }


def run_all_checks(ctx: dict) -> List[Tuple[int, str, bool, str]]:
    results = []
    for num, name, fn in CHECKS:
        try:
            fn(ctx)
            results.append((num, name, True, "OK"))
        except CheckFailure as e:
            results.append((num, name, False, str(e)))
        except Exception as e:  # noqa: BLE001 -- any unexpected error is a hard FAIL, never silently swallowed
            results.append((num, name, False, f"unexpected error: {e!r}"))
    return results


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prereg", default=str(PREREG_PATH))
    ap.add_argument("--deck-manifest", default=str(DECK_MANIFEST_PATH))
    ap.add_argument("--deck-key", default=str(DECK_KEY_PATH))
    ap.add_argument("--ledger", default=str(VERDICTS_LEDGER_PATH))
    ap.add_argument("--sidecar-db", default=str(SIDECAR_DB))
    ap.add_argument("--research-db", default=str(RESEARCH_DB))
    ap.add_argument("--manifest", default=str(MANIFEST_PATH))
    args = ap.parse_args(argv)

    try:
        ctx = build_context(
            prereg_path=args.prereg, deck_manifest_path=args.deck_manifest,
            deck_key_path=args.deck_key, ledger_path=args.ledger,
            sidecar_db=args.sidecar_db, research_db=args.research_db,
            manifest_path=args.manifest,
        )
    except Exception as e:  # noqa: BLE001
        print(f"FATAL: could not build validation context: {e!r}")
        return 1

    results = run_all_checks(ctx)
    print("CERT-01 grading-STARTED validator -- twelve mechanical/estimand checks")
    print("=" * 78)
    all_ok = True
    for num, name, ok, msg in results:
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] ({num:>2}) {name}")
        if not ok:
            print(f"         -> {msg}")
            all_ok = False
    print("=" * 78)
    print("ALL CHECKS PASSED" if all_ok else "VALIDATION FAILED -- see FAIL lines above")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
