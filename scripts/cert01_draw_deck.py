"""CERT-01 Task 2 -- draw the ~200-250-card deck + blinded diagnostic sample
against the FROZEN pre-registration, bind it in a SEPARATE deck manifest.

Phase 135, plan 135-09, Task 2. Implements `docs/specs/discovery-cert01-protocol.md`
§7 (deck/confirmation draw) and §8 (blinded demoted+retained diagnostic
sample), reusing `scripts/cert01_frame.py` for the estimand/hash recipe
(NEVER re-derived) and the E1 harness's `components_of` (the physMS bipartite
component builder the clustered bootstrap analysis will need once grading is
complete) from the gitignored `same_work_spike/probe/scripts/e1_deck.py`
research tree, via a runtime sys.path bridge -- see `scripts/cert01_freeze.py`'s
module docstring for why that tree stays untracked (commit `5370c20f`) and
this plan does not add new tracked files inside it.

Usage:
    python -X utf8 scripts/cert01_draw_deck.py --write

Writes:
    TRACKED:    .planning/phases/135-precision-certificate-confidence-bands/cert01_deck_manifest.json
    gitignored: same_work_spike/probe/data/cert01_deck_key.json      (drawn deck: uid list + card metadata)
                same_work_spike/probe/data/cert01_diagnostic_tag.json (hidden classifier tag, joined post-verdict-lock only)
                same_work_spike/probe/review/cert01_deck_verdicts.json (empty ledger [] -- Task 3 grading fills this)

`cert01_prereg.json` is NEVER mutated by this script (Codex #B1) -- only
READ. Cards are catalogue-blind: NO `later_shared_text`/`routing_status`
field is written into the grader-visible card metadata; the diagnostic tag
lives ONLY in the separate, gitignored `cert01_diagnostic_tag.json` side
file, joined to verdicts by the (future) analysis script only AFTER verdict
lock.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import cert01_frame as cf  # noqa: E402

_PROBE_SCRIPTS = REPO_ROOT / "same_work_spike" / "probe" / "scripts"


def load_e1_deck():
    """Lazily import `e1_deck` from the gitignored dev-box research tree
    (never at module import time -- so the pure adapter functions below
    stay unit-testable even when that tree is absent, e.g. in CI)."""
    if not _PROBE_SCRIPTS.exists():
        raise RuntimeError(
            f"e1_deck.py not found at {_PROBE_SCRIPTS} -- this script must run "
            "on the dev box carrying the same_work_spike/probe research tree "
            "(gitignored, never shipped)."
        )
    if str(_PROBE_SCRIPTS) not in sys.path:
        sys.path.insert(0, str(_PROBE_SCRIPTS))
    import e1_deck as e1  # noqa: E402
    return e1

PHASE_DIR = REPO_ROOT / ".planning" / "phases" / "135-precision-certificate-confidence-bands"
PREREG_PATH = PHASE_DIR / "cert01_prereg.json"
DECK_MANIFEST_OUT = PHASE_DIR / "cert01_deck_manifest.json"

SIDECAR_DB = REPO_ROOT / "discovery_data" / "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db"
RESEARCH_DB = REPO_ROOT / "same_work_spike" / "probe" / "data" / "fullcorpus_v2.db"
GOLD_POOL_PATH = REPO_ROOT / "same_work_spike" / "probe" / "data" / "e1_adjudicated_a.jsonl"

PROBE_DATA_DIR = REPO_ROOT / "same_work_spike" / "probe" / "data"
PROBE_REVIEW_DIR = REPO_ROOT / "same_work_spike" / "probe" / "review"
DECK_KEY_OUT = PROBE_DATA_DIR / "cert01_deck_key.json"
DIAGNOSTIC_TAG_OUT = PROBE_DATA_DIR / "cert01_diagnostic_tag.json"
VERDICTS_LEDGER_OUT = PROBE_REVIEW_DIR / "cert01_deck_verdicts.json"

# Diagnostic-sample draw is derived deterministically from the frozen
# deck-draw seed (protocol only names 3 seeds: deck/gold/bootstrap; this
# offset keeps the diagnostic draw reproducible from the SAME frozen seed
# without inventing a fourth undeclared seed field).
_DIAGNOSTIC_SEED_OFFSET = 1000
DIAGNOSTIC_SAMPLE_PER_GROUP = 20


def uid_of(page_id: str, canonical_work_id: str) -> str:
    return f"{page_id}|{canonical_work_id}"


def load_prereg() -> dict:
    return json.loads(PREREG_PATH.read_text(encoding="utf-8"))


def draw_stratified_deck(estimand_rows: List[dict], stratum_allocation: Dict[str, int],
                         seed: int) -> List[dict]:
    """SRSWOR within each stratum (protocol §7), using the FROZEN deck-draw
    seed. Returns the drawn subset of `estimand_rows` (order NOT yet
    shuffled for rendering -- caller shuffles separately if needed)."""
    rng = random.Random(seed)
    by_stratum: Dict[str, List[dict]] = {}
    for r in estimand_rows:
        by_stratum.setdefault(r["stratum"], []).append(r)
    for stratum_rows in by_stratum.values():
        stratum_rows.sort(key=lambda r: (r["page_id"], r["canonical_work_id"]))
    drawn: List[dict] = []
    for stratum, n in sorted(stratum_allocation.items()):
        pool = by_stratum.get(stratum, [])
        n = min(n, len(pool))
        drawn.extend(rng.sample(pool, n))
    return drawn


def load_gold_pool() -> List[dict]:
    if not GOLD_POOL_PATH.exists():
        return []
    out = []
    with open(GOLD_POOL_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def draw_gold_cards(gold_pool: Sequence[dict], drawn_deck_clusters: set,
                    n_target: int, seed: int) -> List[dict]:
    """Cluster-disjoint gold draw (protocol §4): previously-adjudicated
    cards re-presented blind for intra-rater repeatability, excluding any
    card whose physMS cluster overlaps the newly-drawn discovery deck."""
    eligible = [g for g in gold_pool if (g.get("phys_ms") or g.get("sys_id")) not in drawn_deck_clusters]
    eligible.sort(key=lambda g: (g.get("page_id", ""), g.get("work_id", "")))
    rng = random.Random(seed)
    n = min(n_target, len(eligible))
    return rng.sample(eligible, n)


def build_diagnostic_sample(sidecar_db_path, seed: int, n_per_group: int) -> Dict[str, List[dict]]:
    """Protocol §8: a blinded sample spanning BOTH demoted (`later_shared_text`)
    AND retained later_shared_text-candidate rows. Demoted candidates are
    `discovery_evidence` rows carrying `routing_reason='later_shared_text'`
    directly. Retained candidates are drawn from pages the build's own
    `discovery_routing_audit` recorded a `kept_tie` decision on (a genuine
    overlapping-span co-claim existed on that page but fell within the
    DELTA=100yr tie window, so nothing was demoted) -- the shipped tier_a
    estimand row(s) on those pages are the retained diagnostic candidates.

    (Documented data-shape note: `discovery_routing_audit.demoted_work_id`
    is NULL on `kept_tie` rows in the shipped v2 asset -- a tie decision
    demotes nobody, so the audit schema never populated an "other side" id
    for that decision kind. This means the retained population is
    identified at PAGE granularity via the audit table, not at the exact
    (page, work) pair the kept_tie comparison itself involved; the sampled
    card is whichever tier_a claim ships on that page. Documented as a
    135-09 deviation.)
    """
    conn = cf._connect_ro(sidecar_db_path)
    try:
        demoted = conn.execute(
            """
            SELECT de.a_page_id AS page_id, de.sys_id, dc.work_id, w.canonical_work_id
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w ON w.work_id = dc.work_id
            WHERE de.routing_reason = 'later_shared_text'
            ORDER BY de.a_page_id, dc.work_id
            """
        ).fetchall()
        retained_pages = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT page_id FROM discovery_routing_audit WHERE decision = 'kept_tie'"
            ).fetchall()
        }
    finally:
        conn.close()

    demoted_candidates = [
        {"page_id": p, "sys_id": s, "work_id": w, "canonical_work_id": cw}
        for (p, s, w, cw) in demoted
    ]

    rng = random.Random(seed)
    demoted_candidates.sort(key=lambda r: (r["page_id"], r["work_id"]))
    n_demoted = min(n_per_group, len(demoted_candidates))
    demoted_sample = rng.sample(demoted_candidates, n_demoted) if demoted_candidates else []

    return {
        "demoted": demoted_sample,
        "retained_pages": sorted(retained_pages),  # retained cards drawn from the estimand below
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sidecar-db", default=str(SIDECAR_DB))
    ap.add_argument("--research-db", default=str(RESEARCH_DB))
    args = ap.parse_args(argv)

    prereg = load_prereg()
    print(f"loaded pre-registration report_id={prereg['report_id']}", flush=True)

    print("recomputing estimand against the deployed sidecar (must match the frozen prereg) ...", flush=True)
    estimand_rows = cf.compute_estimand_rows(args.sidecar_db, args.research_db)
    pop_hash = cf.population_hash(estimand_rows)
    clus_hash = cf.cluster_map_hash(estimand_rows)
    if pop_hash != prereg["population_hash"]:
        raise ValueError(
            f"population_hash drift: prereg {prereg['population_hash']!r} != "
            f"recomputed {pop_hash!r} -- the deployed sidecar has changed since "
            "the freeze; the deck MUST NOT be drawn against a drifted frame"
        )
    if clus_hash != prereg["cluster_map_hash"]:
        raise ValueError(
            f"cluster_map_hash drift: prereg {prereg['cluster_map_hash']!r} != "
            f"recomputed {clus_hash!r}"
        )
    print("  population_hash + cluster_map_hash both match the frozen pre-registration", flush=True)

    drawn = draw_stratified_deck(estimand_rows, prereg["stratum_allocation"], prereg["seed"]["deck_draw"])
    print(f"drew {len(drawn)} discovery cards across {len(prereg['stratum_allocation'])} strata", flush=True)

    # Reuse e1_deck.components_of (never re-derived) to compute the drawn
    # deck's physMS bipartite component structure NOW -- this is the exact
    # cluster membership map the clustered bootstrap analysis needs once
    # verdicts exist (comp_bootstrap resamples these components, not raw
    # cards); computing + recording it at draw time lets a later analysis
    # step reuse it verbatim instead of re-deriving component membership.
    e1 = load_e1_deck()
    e1_style_cards = [
        {"uid": uid_of(r["page_id"], r["canonical_work_id"]),
         "work_id": r["canonical_work_id"], "phys": r["unit_key"]}
        for r in drawn
    ]
    component_of_uid = e1.components_of(e1_style_cards)
    num_components = len(set(component_of_uid.values()))
    print(f"  physMS bipartite components over the drawn deck: {num_components}", flush=True)

    drawn_clusters = {r["unit_key"] for r in drawn}
    gold_pool = load_gold_pool()
    gold_cards = draw_gold_cards(gold_pool, drawn_clusters, prereg["gold_allocation"]["n"],
                                prereg["seed"]["gold_shuffle"])
    print(f"drew {len(gold_cards)} cluster-disjoint gold repeat cards (target {prereg['gold_allocation']['n']})",
          flush=True)

    diagnostic = build_diagnostic_sample(
        args.sidecar_db, prereg["seed"]["deck_draw"] + _DIAGNOSTIC_SEED_OFFSET,
        DIAGNOSTIC_SAMPLE_PER_GROUP,
    )
    retained_candidates = [r for r in estimand_rows if r["page_id"] in set(diagnostic["retained_pages"])]
    retained_candidates.sort(key=lambda r: (r["page_id"], r["canonical_work_id"]))
    rng_ret = random.Random(prereg["seed"]["deck_draw"] + _DIAGNOSTIC_SEED_OFFSET + 1)
    n_retained = min(DIAGNOSTIC_SAMPLE_PER_GROUP, len(retained_candidates))
    retained_sample = rng_ret.sample(retained_candidates, n_retained) if retained_candidates else []
    print(f"blinded diagnostic sample: {len(diagnostic['demoted'])} demoted + {len(retained_sample)} retained",
          flush=True)

    # --- catalogue-blind, grader-visible card metadata (NO demotion tag) ---
    def card_of(r: dict, role: str) -> dict:
        return {
            "uid": uid_of(r["page_id"], r["canonical_work_id"]),
            "role": role,
            "stratum": r.get("stratum"),
            "page_id": r["page_id"],
            "canonical_work_id": r["canonical_work_id"],
            "sys_id": r["sys_id"],
        }

    deck_cards = (
        [card_of(r, "candidate") for r in drawn]
        + [card_of({
            "page_id": g.get("page_id"), "canonical_work_id": g.get("work_id"),
            "sys_id": g.get("sys_id") or g.get("phys_ms") or "",
            "stratum": None,
        }, "gold") for g in gold_cards]
        + [card_of({
            "page_id": d["page_id"], "canonical_work_id": d.get("canonical_work_id", d["work_id"]),
            "sys_id": d["sys_id"], "stratum": None,
        }, "diagnostic_demoted") for d in diagnostic["demoted"]]
        + [card_of(r, "diagnostic_retained") for r in retained_sample]
    )
    # Assert the grader-visible surface never carries a demotion field.
    for c in deck_cards:
        assert "later_shared_text" not in c and "routing_status" not in c

    ordered_uids = [c["uid"] for c in deck_cards]
    deck_manifest_hash_payload = json.dumps(
        {"uids": ordered_uids, "cards": deck_cards}, sort_keys=True,
        separators=(",", ":"), ensure_ascii=False,
    )
    import hashlib
    deck_manifest_hash = hashlib.sha256(deck_manifest_hash_payload.encode("utf-8")).hexdigest()

    drawn_stratum_counts = cf.stratum_counts(drawn)

    deck_manifest = {
        "prereg_report_id": prereg["report_id"],
        "deck_manifest_hash": deck_manifest_hash,
        "deck_size": len(deck_cards),
        "candidate_count": len(drawn),
        "gold_count": len(gold_cards),
        "diagnostic_demoted_count": len(diagnostic["demoted"]),
        "diagnostic_retained_count": len(retained_sample),
        "stratum_drawn_counts": drawn_stratum_counts,
        "gold_allocation_drawn": len(gold_cards),
        "confirmation_allocation": prereg["confirmation_allocation"],
        "candidate_phys_ms_components": num_components,
    }

    diagnostic_tag_side_file = {
        c["uid"]: {"later_shared_text": True} for c in deck_cards if c["role"] == "diagnostic_demoted"
    }
    diagnostic_tag_side_file.update({
        c["uid"]: {"later_shared_text": False} for c in deck_cards if c["role"] == "diagnostic_retained"
    })

    print(json.dumps(deck_manifest, indent=2, sort_keys=True))

    if args.write:
        PHASE_DIR.mkdir(parents=True, exist_ok=True)
        PROBE_DATA_DIR.mkdir(parents=True, exist_ok=True)
        PROBE_REVIEW_DIR.mkdir(parents=True, exist_ok=True)

        DECK_MANIFEST_OUT.write_text(
            json.dumps(deck_manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        DECK_KEY_OUT.write_text(
            json.dumps({"cards": deck_cards, "meta": {
                "prereg_report_id": prereg["report_id"],
                "seed": prereg["seed"],
            }}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        DIAGNOSTIC_TAG_OUT.write_text(
            json.dumps(diagnostic_tag_side_file, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if not VERDICTS_LEDGER_OUT.exists():
            VERDICTS_LEDGER_OUT.write_text("[]\n", encoding="utf-8")

        # Confirm cert01_prereg.json was NOT touched (Codex #B1).
        reloaded = json.loads(PREREG_PATH.read_text(encoding="utf-8"))
        assert reloaded == prereg, "cert01_prereg.json must never be mutated by the deck draw"

        print(f"wrote {DECK_MANIFEST_OUT}")
        print(f"wrote {DECK_KEY_OUT} (gitignored)")
        print(f"wrote {DIAGNOSTIC_TAG_OUT} (gitignored)")
        print(f"wrote {VERDICTS_LEDGER_OUT} (gitignored, empty ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
