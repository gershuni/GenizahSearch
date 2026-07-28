"""CERT-01 Task 1 -- freeze the tier_a estimand into an IMMUTABLE
pre-registration + publish the mandatory pre-outcome OC table.

Phase 135, plan 135-09, Task 1. Implements `docs/specs/discovery-cert01-protocol.md`
§5 (freeze discipline) and §6 (OC table), using `scripts/cert01_frame.py` as the
ONE source of truth for the estimand/hash recipe (never re-derived here).

Usage:
    python -X utf8 scripts/cert01_freeze.py --write

Writes (TRACKED, masking-clean):
    .planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json
    .planning/phases/135-precision-certificate-confidence-bands/cert01_oc_table.md

`cert01_prereg.json` is NEVER mutated after this script writes it once and it
is committed -- see the protocol §5.3. Re-running this script is idempotent
ONLY in the sense that it recomputes the SAME payload from the SAME frozen
inputs (deterministic); it must not be re-run to "fix" the file after the
deck has been drawn.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import List, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import cert01_frame as cf  # noqa: E402

# The E1/Q2 harness lives in the gitignored same_work_spike/probe/scripts/
# research tree (commit 5370c20f deliberately untracked that whole tree as
# part of the M-source masking-history remediation -- this plan does not
# reverse that). We reuse its stdlib-only statistics primitives (never
# re-derived) via a runtime sys.path bridge to the LOCAL dev-box copy, which
# is how this exact measurement is executed (not a portable CI artifact).
_PROBE_SCRIPTS = REPO_ROOT / "same_work_spike" / "probe" / "scripts"
if not _PROBE_SCRIPTS.exists():
    raise RuntimeError(
        f"e1_confirm_sizing.py not found at {_PROBE_SCRIPTS} -- this script "
        "must run on the dev box carrying the same_work_spike/probe research "
        "tree (gitignored, never shipped); see docs/specs/discovery-cert01-protocol.md"
    )
sys.path.insert(0, str(_PROBE_SCRIPTS))
import e1_confirm_sizing as sizing  # noqa: E402

PHASE_DIR = REPO_ROOT / ".planning" / "phases" / "135-precision-certificate-confidence-bands"
PROTOCOL_PATH = REPO_ROOT / "docs" / "specs" / "discovery-cert01-protocol.md"
SIDECAR_DB = REPO_ROOT / "discovery_data" / "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db"
MANIFEST_PATH = REPO_ROOT / "discovery_data" / "manifest.json"
RESEARCH_DB = REPO_ROOT / "same_work_spike" / "probe" / "data" / "fullcorpus_v2.db"
CANONICAL_MERGES_PATH = REPO_ROOT / "discovery_data" / "v2_canonical_merges.build.json"
COMPOSITION_DATES_PATH = REPO_ROOT / "discovery_data" / "composition_dates.json"
SEFTJA_DATES_PATH = REPO_ROOT / "same_work_spike" / "probe" / "rsource" / "data" / "seftja_dates.json"
CROSSWALK_PATH = REPO_ROOT / "discovery_data" / "crosswalk.json"
GOLD_POOL_PATH = REPO_ROOT / "same_work_spike" / "probe" / "data" / "e1_adjudicated_a.jsonl"

PREREG_OUT = PHASE_DIR / "cert01_prereg.json"
OC_TABLE_OUT = PHASE_DIR / "cert01_oc_table.md"

TOTAL_CARDS = 220              # within the protocol's ~200-250 band (§7)
MIN_PER_STRATUM = 15
SEED_DECK_DRAW = 20260728
SEED_GOLD_SHUFFLE = 20260729
SEED_BOOTSTRAP = 7             # matches e1_deck.comp_bootstrap's own default
GOLD_ALLOCATION_TARGET = 20
ALLOWED_VERDICTS = ["A", "B", "C", "INS"]  # standard E1 rubric (protocol §4)

OC_P_GRID = (0.80, 0.85, 0.90, 0.95)
OC_INS_GRID = (0.0, 0.10, 0.20)
# The TRUE within-cluster verdict correlation cannot be measured before any
# card is graded. We report three illustrative ICC scenarios spanning the
# e1_confirm_sizing.py self-test's own documented plausible range
# ("mild clustering (ICC ~ 0.05-0.1)") plus a null (no-clustering) baseline,
# each realized via a Beta-Bernoulli simulation over the ACTUAL frame's
# physMS cluster-size distribution (`cluster_sizes()`) -- so the CLUSTER
# GEOMETRY is real; only the correlation coefficient itself is assumed
# (disclosed as such -- Pitfall-8 "expectations-setting only").
OC_RHO_GRID = (0.0, 0.05, 0.10)


def _simulate_components(cluster_sizes: Sequence[int], p: float, rho: float, rng: random.Random) -> List[List[int]]:
    """Beta-Bernoulli hierarchical simulation: cluster-level latent
    probability drawn from Beta(alpha, beta) parameterized so E[p_cluster]=p
    and the implied intraclass correlation ~= rho (exact for equal cluster
    sizes; a stable approximation otherwise -- verified empirically against
    `anova_icc` at the real frame's cluster-size scale)."""
    if rho <= 0:
        return [[1 if rng.random() < p else 0 for _ in range(s)] for s in cluster_sizes]
    conc = max(1e-6, (1 - rho) / rho)
    alpha = max(1e-6, p * conc)
    beta = max(1e-6, (1 - p) * conc)
    comps = []
    for s in cluster_sizes:
        p_cluster = rng.betavariate(alpha, beta)
        comps.append([1 if rng.random() < p_cluster else 0 for _ in range(s)])
    return comps


def _kmin_for_threshold(n: int, threshold: float, alpha: float = 0.05):
    """Smallest k such that the one-sided Wilson lower bound at level alpha
    clears `threshold` (the same binary-search technique n_det_required uses
    internally, reusing ONLY `wilson_lower_one_sided` -- no new statistics)."""
    lo, hi, kmin = 0, n, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if sizing.wilson_lower_one_sided(mid, n, alpha) >= threshold:
            kmin = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return kmin


def compute_oc_grid(cluster_sizes: Sequence[int], total_cards: int, threshold: float):
    """Protocol §6 grid: p x rho(ICC scenario) x ins_rate. For each cell:
    (i) the discovery-stage joint pass probability at ~total_cards cards,
    (ii) the size_confirmation sizing outcome, (iii) POWER_TARGET as the
    conditional confirmation pass probability whenever (ii) is finite (by
    construction of n_det_required's own 80%-power search)."""
    rng = random.Random(SEED_BOOTSTRAP)
    rows = []
    max_finite_n_drawn = None
    for rho in OC_RHO_GRID:
        for p in OC_P_GRID:
            comps = _simulate_components(cluster_sizes, p, rho, rng)
            icc = sizing.anova_icc(comps)
            for ins in OC_INS_GRID:
                n_det = max(1, round(total_cards * (1 - ins)))
                e_comp = sizing.expected_nonempty_components(cluster_sizes, n_det)
                m_bar = n_det / max(1.0, e_comp)
                deff = 1 + (m_bar - 1) * icc
                n_eff = max(1, round(n_det / deff))
                kmin = _kmin_for_threshold(n_eff, threshold)
                joint_pass = sizing.binom_sf(kmin, n_eff, p) if kmin is not None else 0.0
                conf = sizing.size_confirmation(p, threshold, 1, list(cluster_sizes), comps, ins)
                if not conf["screening"]:
                    n_drawn = conf["n_drawn"]
                    if max_finite_n_drawn is None or n_drawn > max_finite_n_drawn:
                        max_finite_n_drawn = n_drawn
                rows.append({
                    "rho": rho, "p": p, "ins_rate": ins, "icc_realized": round(icc, 4),
                    "deff": round(deff, 3), "n_eff": n_eff,
                    "joint_pass_probability": round(joint_pass, 3),
                    "size_confirmation": conf,
                })
    return rows, max_finite_n_drawn


def render_oc_table_md(oc_rows, max_finite_n_drawn, total_cards, threshold, frame_size, num_clusters) -> str:
    lines = [
        "# CERT-01 Pre-Outcome Operating-Characteristics (OC) Table",
        "",
        "**Computed BEFORE any card is drawn** (protocol §6, RESEARCH.md Pitfall 8)."
        " Expectations-setting only -- the deck is drawn regardless of what this table says.",
        "",
        f"Frame: `tier_a` shipped estimand, {frame_size:,} `(page, canonical_work_id)` rows"
        f" over {num_clusters:,} physMS clusters. Discovery deck size: ~{total_cards} cards"
        f" (within the protocol's 200-250 band). Strict floor: {threshold}."
        " Single gate, k=1, no multiple-testing correction (protocol §2).",
        "",
        "**Methodology note (documented assumption):** the true within-cluster verdict"
        " correlation (ICC) cannot be measured before any card is graded. Three"
        " illustrative correlation scenarios (rho = 0.0 null / 0.05 / 0.10, spanning"
        " `e1_confirm_sizing.py`'s own self-test's documented plausible range) are"
        " realized via a Beta-Bernoulli simulation over the REAL frame's physMS"
        " cluster-size distribution (`cluster_sizes()`), then fed to the REUSED"
        " `anova_icc`/`size_confirmation`/`expected_nonempty_components`/"
        " `wilson_lower_one_sided`/`binom_sf` functions (never re-derived). The"
        " CLUSTER GEOMETRY is real; only the correlation coefficient itself is an"
        " assumption, disclosed as such.",
        "",
        "| rho (ICC scenario) | true p | INS rate | ICC realized | deff | n_eff |"
        " joint pass prob. | confirmation sizing | conditional confirm. pass prob. |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in oc_rows:
        conf = r["size_confirmation"]
        if conf["screening"]:
            conf_txt = f"screening ({conf['reason']})"
            cond_txt = "n/a"
        else:
            conf_txt = f"n_drawn={conf['n_drawn']}"
            cond_txt = f"~{sizing.POWER_TARGET:.2f} (by construction)"
        lines.append(
            f"| {r['rho']:.2f} | {r['p']:.2f} | {r['ins_rate']:.2f} | {r['icc_realized']:.4f} |"
            f" {r['deff']:.3f} | {r['n_eff']} | {r['joint_pass_probability']:.3f} |"
            f" {conf_txt} | {cond_txt} |"
        )
    lines += [
        "",
        f"**Pre-reserved confirmation-draw size (frozen at freeze time):**"
        f" {max_finite_n_drawn} -- the MAXIMUM finite `size_confirmation` `n_drawn`"
        " observed across the whole grid above (conservative: whatever the real"
        " discovery outcome turns out to be, within the workable region, a"
        " single physically-sequestered reserve of this size covers it without"
        " drawing more cards after discovery results land -- protocol §7).",
        "",
        "**Reading this table (Pitfall 8):** at `p` exactly AT the Strict floor"
        " (0.85), the joint pass probability is ~alpha (~0.04-0.05) BY"
        " CONSTRUCTION -- a one-sided lower-bound test of a true value sitting"
        " exactly on its own threshold clears only rarely. At `p=0.80`"
        " (below floor), the joint pass probability is 0 and `size_confirmation`"
        " correctly reports screening (`discovery lower bound below locked"
        " threshold`). At `p=0.90`/`0.95` (comfortably above), joint pass"
        " probability is substantial to near-certain. This matches the"
        " protocol's own framing (`PLAN-e1-round2.md`): Strict at ~200-250"
        " cards is a materially harder target than Broad -- a low pass"
        " probability at `p` near the floor is a KNOWN, DISCLOSED risk, not a"
        " reason to skip the OC step.",
        "",
    ]
    return "\n".join(lines)


def load_gold_pool_size(path) -> int:
    if not Path(path).exists():
        return 0
    with open(path, encoding="utf-8") as f:
        return sum(1 for _ in f)


def build_payload(*, estimand_rows, input_hashes, protocol_sha256, gold_pool_available,
                  confirmation_n_drawn) -> dict:
    stratum_counts = cf.stratum_counts(estimand_rows)
    pop_hash = cf.population_hash(estimand_rows)
    clus_hash = cf.cluster_map_hash(estimand_rows)
    strata_weights = {
        s: round(count / sum(stratum_counts.values()), 6) for s, count in stratum_counts.items()
    }
    stratum_allocation = cf.allocate_stratum_cards(stratum_counts, TOTAL_CARDS, MIN_PER_STRATUM)

    payload = {
        "protocol_sha256": protocol_sha256,
        "seed": {
            "deck_draw": SEED_DECK_DRAW,
            "gold_shuffle": SEED_GOLD_SHUFFLE,
            "bootstrap": SEED_BOOTSTRAP,
        },
        "frame_content_hash": input_hashes["frame_content_hash"],
        "population_hash": pop_hash,
        "cluster_map_hash": clus_hash,
        "stratum_counts": stratum_counts,
        "strata_weights": strata_weights,
        "stratum_allocation": stratum_allocation,
        "card_count": sum(stratum_allocation.values()),
        "cutoffs": {
            "strict_floor": cf.STRICT_FLOOR,
            "lever1_coverage_cliff": cf.COVERAGE_MEDIUM_FLOOR,
            "coverage_high_floor": cf.COVERAGE_HIGH_FLOOR,
            "d17_delta_years": cf.D17_DELTA_YEARS,
        },
        "gold_allocation": {
            "n": min(GOLD_ALLOCATION_TARGET, gold_pool_available),
            "pool_available": gold_pool_available,
            "source": "e1_adjudicated_a.jsonl (individually-adjudicated cards; "
                      "re-presented blind for intra-rater repeatability, drawn "
                      "cluster-disjoint from the discovery deck at Task 2 draw time)",
        },
        "confirmation_allocation": {
            "n_drawn": confirmation_n_drawn,
            "basis": "max finite size_confirmation n_drawn across the pre-outcome OC grid "
                     "(protocol §6/§7) -- sequestered at freeze time, never invented "
                     "ad hoc after discovery results land",
        },
        "allowed_verdicts": ALLOWED_VERDICTS,
        "canonical_merges_sha256": input_hashes["canonical_merges_sha256"],
        "composition_dates_sha256": input_hashes["composition_dates_sha256"],
        "seftja_dates_sha256": input_hashes["seftja_dates_sha256"],
        "db_content_hash": input_hashes["db_content_hash"],
        "crosswalk_sha256": input_hashes["crosswalk_sha256"],
    }
    payload["report_id"] = cf.compute_report_id(payload)
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="write cert01_prereg.json + cert01_oc_table.md")
    ap.add_argument("--sidecar-db", default=str(SIDECAR_DB))
    ap.add_argument("--research-db", default=str(RESEARCH_DB))
    args = ap.parse_args(argv)

    print("computing CERT-01 estimand (tier_a shipped, display-deduped) ...", flush=True)
    estimand_rows = cf.compute_estimand_rows(args.sidecar_db, args.research_db)
    print(f"  {len(estimand_rows):,} estimand rows", flush=True)

    input_hashes = cf.read_input_hashes(
        args.sidecar_db, str(MANIFEST_PATH),
        canonical_merges_path=str(CANONICAL_MERGES_PATH),
        composition_dates_path=str(COMPOSITION_DATES_PATH),
        seftja_dates_path=str(SEFTJA_DATES_PATH),
        crosswalk_path=str(CROSSWALK_PATH),
    )
    protocol_sha256 = cf.hash_file(PROTOCOL_PATH)

    cluster_sizes = cf.cluster_sizes(estimand_rows)
    print("computing pre-outcome OC table (RESEARCH Pitfall 8) ...", flush=True)
    oc_rows, max_finite_n_drawn = compute_oc_grid(cluster_sizes, TOTAL_CARDS, cf.STRICT_FLOOR)
    oc_md = render_oc_table_md(
        oc_rows, max_finite_n_drawn, TOTAL_CARDS, cf.STRICT_FLOOR,
        len(estimand_rows), len(cluster_sizes),
    )

    gold_pool_available = load_gold_pool_size(GOLD_POOL_PATH)
    payload = build_payload(
        estimand_rows=estimand_rows, input_hashes=input_hashes,
        protocol_sha256=protocol_sha256, gold_pool_available=gold_pool_available,
        confirmation_n_drawn=max_finite_n_drawn,
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))

    if args.write:
        PHASE_DIR.mkdir(parents=True, exist_ok=True)
        PREREG_OUT.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        OC_TABLE_OUT.write_text(oc_md, encoding="utf-8")
        print(f"wrote {PREREG_OUT}")
        print(f"wrote {OC_TABLE_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
