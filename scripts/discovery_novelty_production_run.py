"""Phase 136 -- the authorized production run of the pinned novelty gate
(owner rulings J, K, M, N, O).

Funnel-first (ruling J): the deterministic heuristic pass resolves what it can
mechanically, and ONLY the residual reaches the model. The model arm is BATCHED
at `DEFAULT_BATCH_SIZE` (ruling O) -- the pinned system prompt is 88% of a
single-case call's input, so judging 10 cases per call is 5.5x cheaper at the
measured knee where false `fills_gap` promotions stop improving.

Both owner-required guardrails are wired here, not left to the caller:

  * **Connection loss.** Per-candidate checkpointing, flushed after every batch,
    so a killed run resumes without re-billing. A reply that cannot be aligned
    1:1 to the cases sent is retried and then degrades to the separately
    validated single-case contract; nothing partial is ever checkpointed.

  * **Price ballooning.** `--cost-ceiling` is a HARD bound, checked against real
    `usage.cost` read from the provider BEFORE each batch is sent -- never an
    estimate, and never crossable by the batch that discovers it. Hitting it
    stops the run cleanly with the checkpoint intact.

The run writes its own manifest (model, effort, batch prompt hash, batch size,
ceiling) so a later reader can prove which contract produced these verdicts.
Verdicts are keyed by (sys_id, ref_work_id) and are a BUILD-TIME artifact --
per NOVEL-02 the verdict cache NEVER ships in the sidecar.

Usage (the authorized run):

    python scripts/discovery_novelty_production_run.py --cost-ceiling 40

Smoke it first at negligible cost:

    python scripts/discovery_novelty_production_run.py --cost-ceiling 0.05 --limit 30
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Mapping, Optional, Sequence

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from shared.discovery_novelty import (  # noqa: E402
    BATCH_PROMPT_SHA256,
    CACHE_KEY_FIELDS,
    INPUT_NORMALIZATION_SHA256,
    DEFAULT_BATCH_SIZE,
    LLM_MODEL,
    LLM_REASONING_EFFORT,
    NOVELTY_BATCH_PROMPT_TEMPLATE,
    NOVELTY_PROMPT_TEMPLATE,
    PROMPT_SHA256,
)
from scripts.discovery_novelty_funnel import (  # noqa: E402
    CostCeilingExceeded,
    NoveltyCandidate,
    assemble_evidence_bundle,
    candidate_input_fingerprint,
    run_heuristic_funnel,
    run_model_arm_batched,
)
from scripts.discovery_novelty_probe import (
    _MIN_ALIAS_WORDS as _ALIAS_MIN_WORDS,  # noqa: E402
    load_work_witnesses,  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_FGP_DB,
    DEFAULT_FJMS_DB,
    DEFAULT_LIBRARIES_CSV,
    DEFAULT_PGP_DB,
    build_all_candidates,
    sum_real_cost,
)

DEFAULT_CHECKPOINT = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_checkpoint.jsonl")
DEFAULT_COST_LOG = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_cost_log.jsonl")
DEFAULT_OUT = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_verdicts.json")
DEFAULT_MANIFEST = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_manifest.json")

_SOURCE_ORDER = ("catalogue", "bibliography", "pgp", "fgp", "m_source_shelfmark")


def render_case(candidate: NoveltyCandidate) -> str:
    """One case block. Evidence comes from `assemble_evidence_bundle`, so each
    source keeps its OWN provenance and the model sees exactly what the
    single-case arm saw -- only the framing differs."""
    bundle = assemble_evidence_bundle(candidate)
    lines = [f"Claimed work: {candidate.claimed_title} (author: {candidate.claimed_author or 'unknown'})"]
    for source in _SOURCE_ORDER:
        texts = bundle.get(source, ())
        lines.append(f"{source}: {' ||| '.join(t for t in texts if t) if texts else '(none)'}")
    return "\n".join(lines)


def render_batch(candidates: Sequence[NoveltyCandidate]) -> str:
    return "\n\n".join(
        f"### CASE {i + 1}\n{render_case(c)}" for i, c in enumerate(candidates)
    )


def make_openrouter_calls(api_key: str, cost_log_path: str, timeout: float = 300.0, max_attempts: int = 4):
    """Returns (batch_call, single_call). Both log REAL `usage.cost` per call
    and refuse a silently-downgraded model."""
    import requests

    session = requests.Session()

    def _post(system: str, user: str, tag: str) -> Optional[Mapping[str, Any]]:
        payload = {
            "model": f"google/{LLM_MODEL}",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "reasoning": {"effort": LLM_REASONING_EFFORT},
            "usage": {"include": True},
            "response_format": {"type": "json_object"},
        }
        last_exc: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    data=json.dumps(payload),
                    timeout=timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                echo = data.get("model") or ""
                if LLM_MODEL not in echo:
                    raise RuntimeError(
                        f"provider echoed unexpected model {echo!r}, expected to contain {LLM_MODEL!r} "
                        "-- refusing (never silently accept a downgrade)"
                    )
                usage = data.get("usage") or {}
                with open(cost_log_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"tag": tag, "cost": usage.get("cost"), "attempt": attempt}) + "\n")
                return json.loads(data["choices"][0]["message"]["content"])
            except Exception as exc:  # noqa: BLE001 -- real network call, bounded retries
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(min(2 ** attempt, 20))
        # Exhausted: log a zero-cost failure and return None. The caller's
        # alignment check turns this into a retry/degrade, never a recorded guess.
        with open(cost_log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"tag": tag, "cost": 0.0, "error": str(last_exc)}) + "\n")
        return None

    def batch_call(chunk: Sequence[NoveltyCandidate]) -> Optional[Mapping[str, Any]]:
        return _post(NOVELTY_BATCH_PROMPT_TEMPLATE, render_batch(chunk), f"batch:{len(chunk)}")

    def single_call(candidate: NoveltyCandidate) -> Optional[Mapping[str, Any]]:
        return _post(NOVELTY_PROMPT_TEMPLATE, render_case(candidate), "single")

    return batch_call, single_call


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset", default=DEFAULT_ASSET)
    p.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES_CSV)
    p.add_argument("--fjms-db", default=DEFAULT_FJMS_DB)
    p.add_argument("--pgp-db", default=DEFAULT_PGP_DB)
    p.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    p.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    p.add_argument("--cost-log", default=DEFAULT_COST_LOG)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--manifest", default=DEFAULT_MANIFEST)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--cost-ceiling", type=float, required=True,
                   help="HARD ceiling in USD on real cumulative spend. Required -- there is no "
                        "default, so an unbounded run cannot happen by omission.")
    p.add_argument("--limit", type=int, default=None, help="smoke only: cap residual candidates")
    p.add_argument("--max-workers", type=int, default=1,
                   help="concurrent provider calls (default 1 = serial). Batch SIZE and "
                        "composition are unchanged, so verdicts are unaffected; the cost "
                        "ceiling reserves the worst billed batch cost for every request in "
                        "flight, and the checkpoint is still written on one thread.")
    p.add_argument("--work-witnesses", default=None,
                   help="emit_work_witnesses.py output -- M-source's recorded "
                        "witnesses. Required unless --allow-no-witnesses.")
    p.add_argument("--crosswalk", default=os.path.join(
                       REPO_ROOT, "discovery_data", "crosswalk.json"))
    p.add_argument("--allow-no-witnesses", action="store_true",
                   help="deliberately run without the recorded-witness source")
    p.add_argument("--api-key-env", default="OPENROUTER_API_KEY")
    args = p.parse_args(argv)

    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        env_path = os.path.join(REPO_ROOT, ".env")
        if os.path.isfile(env_path):
            for line in open(env_path, encoding="utf-8"):
                if line.strip().startswith(args.api_key_env):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not api_key:
        print(f"ERROR: {args.api_key_env} not set and not found in .env -- refusing to run.")
        return 2

    def log(msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    log(f"loading real data from {args.asset}")
    # M-source's RECORDED WITNESSES (2026-08-08). REQUIRED for a real run, not
    # optional: a `high`-confidence recorded witness resolves deterministically
    # to `confirms`, so omitting the map BUYS ~900 verdicts that need no model
    # call -- and computes every remaining verdict blind to a decision input that
    # is IN the fingerprint, so they would not match the measured population.
    witnesses = load_work_witnesses(args.work_witnesses, args.crosswalk)
    if args.work_witnesses:
        log(f"recorded-witness works: {len(witnesses):,}")
    elif not args.allow_no_witnesses:
        print("ERROR: --work-witnesses not supplied. A production run without it "
              "buys verdicts the funnel can resolve for free and fingerprints "
              "them against a source it never read. Pass --allow-no-witnesses to "
              "override deliberately.")
        return 2
    candidates, works, libraries = build_all_candidates(
        args.asset, args.libraries_csv, args.fjms_db, args.pgp_db, args.fgp_db,
        work_witnesses=witnesses,
    )
    resolved, residual = run_heuristic_funnel(candidates)
    log(f"candidates={len(candidates):,} heuristically_resolved={len(resolved):,} residual={len(residual):,}")

    if args.limit:
        residual = list(residual)[: args.limit]
        log(f"SMOKE: residual capped to {len(residual):,}")

    # discovery-v3 (Codex blocker 3): the per-pair INPUT fingerprint for every
    # candidate the model will judge. Keyed exactly as the model arm keys its
    # results, so a checkpointed answer can be matched against the inputs it came
    # from. BATCH_PROMPT_SHA256 is passed because this run uses the batched
    # contract -- a cache built under the single-case framing must not be reused
    # here, and vice versa.
    fingerprints = {
        f"{c.sys_id}::{c.ref_work_id}": candidate_input_fingerprint(
            c, prompt_sha256=BATCH_PROMPT_SHA256)
        for c in residual
    }
    log(f"input fingerprints computed for {len(fingerprints):,} residual candidates")

    with open(args.manifest, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "llm_model": LLM_MODEL,
                "llm_reasoning_effort": LLM_REASONING_EFFORT,
                "batch_prompt_sha256": BATCH_PROMPT_SHA256,
                "single_prompt_sha256": PROMPT_SHA256,
                "batch_size": args.batch_size,
                # Recorded because it is the one knob that changes HOW the run
                # was executed without changing what was asked -- a reader
                # comparing two verdict files should be able to see that the
                # difference was throughput, not framing.
                "max_workers": args.max_workers,
                "cost_ceiling_usd": args.cost_ceiling,
                "residual_size": len(residual),
                # discovery-v3 (Codex blocker 3): the fingerprint CONTRACT, so a
                # later reader can tell which normalization/field-order produced
                # the `input_fingerprint` values in the output -- without it, a
                # fingerprint is an opaque hex string that proves nothing.
                "input_fingerprint_version": "v3-2026-08-07",
                "input_fingerprint_prompt_sha256": BATCH_PROMPT_SHA256,
                "input_normalization_sha256": INPUT_NORMALIZATION_SHA256,
                "input_fingerprint_fields": list(CACHE_KEY_FIELDS),
                "heuristically_resolved": len(resolved),
                # 2026-08-18. The FUNNEL's configuration, recorded here rather
                # than in the per-pair cache key. An alias changes which pairs the
                # mechanical pass resolves, so two verdict sets produced with and
                # without it are not interchangeable -- but it never changes the
                # question the MODEL is asked (`render_case` does not send it),
                # and the heuristic re-runs before any cache lookup, so putting it
                # in the cache key would invalidate every entry for a reason the
                # model never saw. The manifest is where a reader can tell which
                # funnel produced a verdict file.
                "claim_alias_source": "locus_edition.title_original",
                "claim_alias_min_words": _ALIAS_MIN_WORDS,
                "candidates_carrying_an_alias": sum(
                    1 for c in candidates if c.claimed_aliases),
                "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
            },
            fh,
            indent=2,
        )

    batch_call, single_call = make_openrouter_calls(api_key, args.cost_log)

    def cost_probe() -> float:
        return sum_real_cost(args.cost_log)[0]

    stopped_at_ceiling = False
    try:
        verdicts = run_model_arm_batched(
            residual,
            batch_model_call=batch_call,
            checkpoint_path=args.checkpoint,
            expected_fingerprints=fingerprints,
            batch_size=args.batch_size,
            cost_probe=cost_probe,
            cost_ceiling_usd=args.cost_ceiling,
            max_workers=args.max_workers,
            single_model_call=single_call,
            progress=log,
        )
    except CostCeilingExceeded as exc:
        stopped_at_ceiling = True
        log(f"STOPPED AT CEILING: {exc}")
        verdicts = {}
        if os.path.isfile(args.checkpoint):
            for line in open(args.checkpoint, encoding="utf-8"):
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    verdicts[f"{rec['sys_id']}::{rec['ref_work_id']}"] = {
                        "novelty_status": rec.get("novelty_status"),
                        "divergence_correctness": rec.get("divergence_correctness"),
                        # Carried on the ceiling-recovery path too: without it a
                        # run stopped at the cost ceiling would write an
                        # unfingerprinted output, which then loads as all-miss.
                        "input_fingerprint": rec.get("input_fingerprint"),
                    }

    # BOTH ARMS, or the output is silently wrong (caught by plan 136-12).
    #
    # Ruling J's funnel resolves ~10,016 rows mechanically -- Arm 1's `confirms`
    # name-matches and Arm 3's no-source-text `fills_gap` bypass -- and ONLY the
    # residual reaches the model. Writing the model arm alone produced a file
    # that looked complete but left every heuristically-resolved row at the
    # fail-closed `not_checked` default on ingest, quietly discarding every
    # bypass-path candidate: the 8,327 rows that are the LARGEST single source
    # of "Candidates for new finds".
    # The heuristic arm is fingerprinted too. It never calls the model, so no
    # money rides on it -- but its verdicts are read by the same loader under the
    # same gate, and an unfingerprinted heuristic verdict would load as
    # `not_checked`, silently discarding the 8,327-row bypass path that is the
    # largest single source of "Candidates for new finds".
    heuristic_fingerprints = {
        f"{c.sys_id}::{c.ref_work_id}": candidate_input_fingerprint(
            c, prompt_sha256=BATCH_PROMPT_SHA256)
        for c in candidates
    }
    merged: Dict[str, Dict[str, Optional[str]]] = {
        key: {"novelty_status": r.novelty_status, "divergence_correctness": None,
              "input_fingerprint": heuristic_fingerprints.get(key)}
        for key, r in resolved.items()
    }
    overlap = set(merged) & set(verdicts)
    if overlap:
        raise RuntimeError(
            f"{len(overlap)} row(s) resolved by BOTH the heuristic funnel and the model arm "
            f"(e.g. {sorted(overlap)[:3]}) -- the two arms must partition the candidate set. "
            "Refusing to write an output whose provenance is ambiguous."
        )
    merged.update(verdicts)

    covered, expected = len(merged), len(candidates)
    if not stopped_at_ceiling and not args.limit and covered != expected:
        raise RuntimeError(
            f"output covers {covered:,} of {expected:,} candidates -- a completed run must cover "
            "every one. Refusing to write a partial file that would ingest as `not_checked`."
        )
    log(f"merged arms: heuristic={len(resolved):,} + model={len(verdicts):,} = {covered:,} "
        f"of {expected:,} candidates")

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False)

    verdicts = merged
    total, calls = sum_real_cost(args.cost_log)
    counts: Dict[str, int] = {}
    for v in verdicts.values():
        counts[v["novelty_status"]] = counts.get(v["novelty_status"], 0) + 1
    log(f"verdicts={len(verdicts):,} real_cost=${total:.6f} calls={calls:,}")
    for k in sorted(counts, key=lambda k: -counts[k]):
        log(f"   {counts[k]:7,}  {k}")
    log("DONE" + (" (ceiling)" if stopped_at_ceiling else ""))
    return 3 if stopped_at_ceiling else 0


if __name__ == "__main__":
    raise SystemExit(main())
