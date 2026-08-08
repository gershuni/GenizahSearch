# -*- coding: utf-8 -*-
"""Phase 136 plan 136-04 Task 2/3 -- the committed novelty funnel runner and
its owner-label grading harness.

Implements the funnel-first architecture ruling J adopted
(`.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
section J): a MECHANICAL heuristic pass runs over every identification
first; only the RESIDUAL it cannot resolve is ever presented to the pinned
LLM gate (`shared/discovery_novelty.py`). This is a NEW, committed script --
it does not import from, and its absence-of-import is load-bearing against,
any gitignored research tree: those trees are read-only reference material
for the humans who designed this funnel, never an import target, and their
absence must never break this script.

Reads its model, effort, prompt hash, input-normalization hash and
cache-key spec from `shared/discovery_novelty.py` rather than carrying its
own copies (a CI grep on this file finds the import, never a second
literal).

Ports the corrected design over the REFERENCE implementation's measured
defects (`136-NOVELTY-PRIOR-ART.md` section 6, Codex REWORK verdict on the
gen2 novelty gate's own review log):

- Codex finding 1 / finding 6 (over-demotion on bare source PRESENCE):
  fixed by requiring an ACTUAL textual name-match (checked against each
  source's own free text, normalized) before the mechanical pass ever
  resolves anything to `confirms` -- mere presence of a bibliography row
  or a PGP description is never sufficient by itself.
- Codex finding 2 (the FJMS catalogue field mischaracterized as the
  catalogue-VOLUME/entry-number field, which measured zero matches): this
  script's `catalogue_text` field represents the catalogue's OWN
  identification prose (FJMS `catalog.TitleHeb` / `GenizahTitleOrgTitle`),
  never the volume/entry-number field.
- Codex finding 2/3 (canonical-id collapse: `M:Ytext1000`-style ids
  collapsing up to 39 distinct titles to one representative): this script
  operates entirely at the raw `ref_work` grain -- `NoveltyCandidate.ref_work_id`
  plus its OWN `claimed_title`/`claimed_author`, never a canonical-id
  lookup of any kind.
- Codex finding 4 (silent `.get()`-style page-join drop): an unmapped page
  routes EXPLICITLY to `not_checked` with a logged, counted reason --
  see `UNMAPPED_PAGE_REASON`.
- Codex finding 5 (evidence bundle stripped of provenance, bib/PGP text
  never presented at all): `assemble_evidence_bundle` tags every source's
  free text by its OWN provenance and always includes bibliography/PGP
  text, even though presence alone is never decisive.
- Owner ruling G (free text is a genuine input, not merely a source for
  literal-substring tests against OTHER works' titles): the SAME
  normalized-substring test this script applies to decide "confirms" is
  applied uniformly across every source's free text, including the
  catalogue's own prose -- so a structured field pointing elsewhere never,
  by itself, causes divergence; the free text is always checked FIRST,
  under a looser reading (aliases).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from shared.discovery_novelty import (  # noqa: E402
    CANDIDATE_STATUS,
    DEFAULT_STATUS,
    LLM_MODEL,
    LLM_MODEL_VERSION,
    LLM_REASONING_EFFORT,
    PROMPT_SHA256,
    INPUT_NORMALIZATION_SHA256,
    DEFAULT_BATCH_SIZE,
    BatchResponseInvalid,
    build_cache_key,
    normalize_free_text,
    resolve_batch_model_output,
    resolve_model_output,
)

DEFAULT_LABELS_PATH = os.path.join(REPO_ROOT, "discovery_data", "novelty_hardcase_labels-v1.json")


# ---------------------------------------------------------------------------
# The candidate abstraction the heuristic pass and the model arm both
# operate over. Deliberately carries NO canonical_work_id field -- every
# field is sourced at the raw ref_work grain (Codex finding 2/3).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class NoveltyCandidate:
    sys_id: str
    ref_work_id: str  # the RAW work id this specific identification claims -- never a collapsed id
    claimed_title: str
    claimed_author: Optional[str] = None
    claimed_aliases: Tuple[str, ...] = ()
    # The FJMS/NLI catalogue's OWN identification prose (never the
    # catalogue-volume/entry-number field, which measured zero matches).
    catalogue_text: Optional[str] = None
    # Each row: {"text": "...", "transcription_type": "..."}. TranscriptionType
    # (e.g. "published_full") is carried for provenance/reporting; it is
    # NEVER used as a decisive signal by itself (Codex finding 1).
    bibliography_rows: Tuple[Mapping[str, Any], ...] = ()
    pgp_description: Optional[str] = None
    pgp_transcription: Optional[str] = None
    fgp_texts: Tuple[str, ...] = ()
    # An internal reference-corpus shelfmark attribution -- source name
    # withheld throughout this module; treated as an ordinary checked
    # source, tagged "m_source_shelfmark" and NEVER named.
    m_source_shelfmark_text: Optional[str] = None
    # 2026-08-08. Does the reference corpus RECORD this manuscript as a witness
    # of this work, across its three witness channels? One of "high" / "low" /
    # "ambiguous" (the source table's own precision-first tiers) or None.
    #
    # This is a per-(sys_id, work) fact, not per-work prose, and it is DECISIVE
    # in one direction only: a recorded witness means the claim restates
    # something the corpus already attests. Absence proves nothing -- catalogue
    # coverage is uneven -- so this must never be read as evidence of novelty.
    known_witness_confidence: Optional[str] = None
    # False simulates a page that failed to map to this sys_id (Codex
    # finding 4) -- routes to not_checked rather than being silently
    # dropped or mis-evaluated.
    page_mapped: bool = True


def candidate_input_fingerprint(
    candidate: NoveltyCandidate, *, prompt_sha256: str = PROMPT_SHA256,
) -> str:
    """The per-pair INPUT fingerprint (Codex blocker 3).

    Answers one question: *was this exact question already asked?* The verdict
    cache keys on `(sys_id, ref_work_id)` alone, so a cache hit today proves only
    that SOME question about that pair was answered. `render_case` sends the
    claimed title, the claimed author, and the assembled per-source evidence
    text -- all of which come from artifacts that change between runs (the baked
    `works` row, the alias artifact, the finding-aid DBs). Reusing a verdict
    across a changed title is reusing an answer to a different question, and it
    is silent: the cache's whole-file SHA-256 proves the FILE is the measured
    file, never that the QUESTION is the measured question.

    `build_cache_key` and `CACHE_KEY_FIELDS` already specified this exactly, down
    to the field order, and `INPUT_NORMALIZATION_SPEC` documents it. Nothing
    called them on a real candidate -- the only caller was a demo block. This is
    the bridge from a `NoveltyCandidate` to that pinned key.

    THE FIELD SET IS DERIVED FROM `render_case`, not chosen: every field
    `render_case` interpolates must appear here, or a change to it would be
    invisible to the fingerprint. `render_case` sends `claimed_title`,
    `claimed_author`, and `assemble_evidence_bundle`'s five sources in
    `_SOURCE_ORDER`. `test_the_fingerprint_covers_every_field_render_case_sends`
    pins that correspondence so a future field added to the prompt cannot quietly
    escape the fingerprint.

    `prompt_sha256` is a parameter because the production run uses the BATCHED
    prompt (ruling O), whose text differs from the single-case one. Passing the
    prompt actually used means a cache built under one framing cannot be reused
    under the other -- which is right: the batch and single arms are separately
    validated contracts, not interchangeable ones.

    NOT included, deliberately: `page_mapped`. It gates whether the candidate
    reaches the model at all (an unmapped page routes to `not_checked` without a
    call), so it changes no question that is ever asked.

    MASKING (D-25): returns a hex digest. The normalized source texts are hashed,
    never returned, logged or stored -- so the fingerprint of an M-source
    shelfmark attribution carries no restricted content.
    """
    bundle = assemble_evidence_bundle(candidate)

    def _joined(source: str) -> str:
        # The SAME separator `render_case` uses, so the fingerprint sees the same
        # concatenation the model does -- otherwise two different per-source
        # splits could fingerprint identically.
        return normalize_free_text(" ||| ".join(t for t in bundle.get(source, ()) if t))

    return build_cache_key({
        "llm_model": LLM_MODEL,
        "llm_model_version": LLM_MODEL_VERSION,
        "llm_reasoning_effort": LLM_REASONING_EFFORT,
        "prompt_sha256": prompt_sha256,
        "input_normalization_sha256": INPUT_NORMALIZATION_SHA256,
        "sys_id": candidate.sys_id,
        "ref_work_id": candidate.ref_work_id,
        "claimed_title_normalized": normalize_free_text(candidate.claimed_title),
        "claimed_author_normalized": normalize_free_text(candidate.claimed_author),
        "catalogue_text_normalized": _joined("catalogue"),
        "bibliography_text_normalized": _joined("bibliography"),
        "pgp_text_normalized": _joined("pgp"),
        "fgp_text_normalized": _joined("fgp"),
        "m_source_shelfmark_text_normalized": _joined("m_source_shelfmark"),
        # A DECISION input, so it belongs in the fingerprint: a verdict reached
        # while this was unknown must not be reused once it is known.
        "known_witness_confidence": candidate.known_witness_confidence or "",
    })


def assemble_evidence_bundle(candidate: NoveltyCandidate) -> Dict[str, Tuple[str, ...]]:
    """Tags each checked source's own free text by its OWN provenance --
    NEVER a flattened, untyped, provenance-stripped list (Codex finding 5).
    Bibliography rows and PGP descriptions are INCLUDED even though
    presence alone is never decisive (Codex findings 1/6) -- omitting them
    from the bundle would make them invisible to the model too, which is a
    DIFFERENT and worse defect than merely not treating them as decisive.
    """
    bib_texts = tuple(
        row.get("text", "") for row in candidate.bibliography_rows if row.get("text")
    )
    pgp_texts = tuple(t for t in (candidate.pgp_description, candidate.pgp_transcription) if t)
    return {
        "catalogue": (candidate.catalogue_text,) if candidate.catalogue_text else (),
        "bibliography": bib_texts,
        "pgp": pgp_texts,
        "fgp": tuple(t for t in candidate.fgp_texts if t),
        "m_source_shelfmark": (
            (candidate.m_source_shelfmark_text,) if candidate.m_source_shelfmark_text else ()
        ),
    }


def _claim_appears_in_text(candidate: NoveltyCandidate, text: str) -> bool:
    """Ruling G's "looser reading" test: does the claimed work's OWN
    identity (its title, or any of its known alias spellings) appear in
    this source's free text, under normalization? Applied UNIFORMLY to
    every source (never only to a "does this contradict" test), so a
    structured field pointing elsewhere is never, by itself, sufficient to
    conclude divergence -- the free text is always checked FIRST.
    """
    norm_text = normalize_free_text(text)
    if not norm_text:
        return False
    for title in (candidate.claimed_title, *candidate.claimed_aliases):
        norm_title = normalize_free_text(title)
        if norm_title and norm_title in norm_text:
            return True
    return False


# ---------------------------------------------------------------------------
# The mechanical (zero-model-call) heuristic pass -- ruling J's funnel
# stage. Can only ever produce TWO outcomes: a genuine textual name-match
# (`confirms`) or "unresolved" (the residual). Never itself decides
# diverges_work/diverges_part/refines_granularity/aid_more_specific/
# alias_merge/container_predicts/extends -- those require judgment beyond
# mechanical string matching and are the model's job alone over the
# residual.
# ---------------------------------------------------------------------------

UNMAPPED_PAGE_REASON = "unmapped_page_join"
NO_SOURCE_TEXT_REASON = "no_checked_source_text_arm3"


@dataclass(frozen=True)
class HeuristicResult:
    resolved: bool
    novelty_status: Optional[str]
    reason: str
    evidence_bundle: Dict[str, Tuple[str, ...]] = field(default_factory=dict)


def run_heuristic_pass(candidate: NoveltyCandidate) -> HeuristicResult:
    """The heuristic funnel's mechanical pass over ONE candidate.

    1. An unmapped page (Codex finding 4) resolves EXPLICITLY to
       `not_checked`, never a silent drop.
    2. A candidate with NO checked-source text anywhere (ruling J's "Arm 3"
       accounting) ships as a candidate (`fills_gap`) automatically -- by
       definition, if nothing any checked source says exists for this
       fragment, no aid can possibly name the work.
    3. Otherwise: a genuine textual name-match against ANY source's free
       text (ruling G's looser-reading test) resolves to `confirms`.
    4. Otherwise: UNRESOLVED -- this candidate is part of the residual the
       pinned model gate will see.
    """
    if not candidate.page_mapped:
        return HeuristicResult(True, DEFAULT_STATUS, UNMAPPED_PAGE_REASON, {})

    bundle = assemble_evidence_bundle(candidate)

    # 2026-08-08 -- RECORDED WITNESS, checked BEFORE the no-source-text rule.
    #
    # The reference corpus already attests THIS manuscript as a witness of THIS
    # work, at exact-classmark-plus-agreeing-library confidence. That is not a
    # hint to weigh, it is the answer: the claim restates something recorded.
    # Resolving it here costs no model call.
    #
    # Ordering matters and is the whole point. Rule 2 below ships a candidate as
    # `fills_gap` when NO checked source has text -- and 98 claims measured on
    # the real artifact carry that label while being recorded witnesses. Placing
    # this after rule 2 would leave every one of them mislabelled.
    #
    # ONLY `high` auto-resolves. The source's own tiers put `low` at
    # classmark-without-library agreement and `ambiguous` at a classmark
    # resolving to many manuscripts; both are real signal but not proof, so they
    # travel to the model inside the bundle instead of short-circuiting it.
    if candidate.known_witness_confidence == "high":
        return HeuristicResult(True, "confirms", "recorded_witness:high", bundle)

    has_any_text = any(texts for texts in bundle.values())
    if not has_any_text:
        return HeuristicResult(True, CANDIDATE_STATUS, NO_SOURCE_TEXT_REASON, bundle)

    for source, texts in bundle.items():
        for text in texts:
            if _claim_appears_in_text(candidate, text):
                return HeuristicResult(True, "confirms", f"mechanical_name_match:{source}", bundle)

    return HeuristicResult(False, None, "unresolved_residual", bundle)


def run_heuristic_funnel(candidates: Sequence[NoveltyCandidate]) -> Tuple[
    Dict[str, HeuristicResult], List[NoveltyCandidate]
]:
    """Runs the mechanical pass over every candidate. Returns
    (resolved_by_sys_id_and_ref_work, residual_candidates) -- the residual
    is exactly, and only, the candidates the pinned model gate may ever
    see (ruling J)."""
    resolved: Dict[str, HeuristicResult] = {}
    residual: List[NoveltyCandidate] = []
    for candidate in candidates:
        key = _candidate_key(candidate.sys_id, candidate.ref_work_id)
        result = run_heuristic_pass(candidate)
        if result.resolved:
            resolved[key] = result
        else:
            residual.append(candidate)
    return resolved, residual


def _candidate_key(sys_id: str, ref_work_id: str) -> str:
    return f"{sys_id}::{ref_work_id}"


class CostCeilingExceeded(Exception):
    """Raised to stop a run cleanly when real cumulative spend reaches the
    caller's ceiling. Not an error condition -- the checkpoint is intact and
    the run resumes from exactly where it stopped, having spent no more than
    authorized."""


# ---------------------------------------------------------------------------
# The model arm -- runs ONLY over the residual (ruling J). Checkpointed and
# resumable: a killed-and-restarted run never re-bills a candidate whose
# verdict is already on disk.
# ---------------------------------------------------------------------------

def run_model_arm(
    residual_candidates: Sequence[NoveltyCandidate],
    *,
    model_call: Callable[[NoveltyCandidate], Optional[Mapping[str, Any]]],
    checkpoint_path: Optional[str] = None,
    # discovery-v3 (Codex blocker 3). Present on BOTH arms deliberately: the
    # batched arm degrades to the single-case contract after repeated unaligned
    # replies, and this arm owns its own checkpoint file, so a fingerprint check
    # on the batched arm alone would leave the degraded path resuming stale
    # answers.
    expected_fingerprints: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Optional[str]]]:
    """Runs `model_call` over `residual_candidates`, one at a time,
    checkpointing to `checkpoint_path` (a JSONL file, one record per
    completed candidate) after EVERY call -- so a killed and restarted run
    resumes from the last completed candidate rather than re-billing
    already-answered ones. `model_call` is injected so this function is
    testable at zero cost and with zero network access; the real
    production run (136-NOVELTY-RUN.md) supplies the actual pinned
    provider call here.

    Never computes or logs a cache key itself (that is the caller's
    concern via `shared.discovery_novelty.build_cache_key`) -- this
    function's only job is checkpointed iteration + structured-abstention
    resolution via `shared.discovery_novelty.resolve_model_output`.
    """
    results: Dict[str, Dict[str, Optional[str]]] = {}
    already_done: set = set()
    stale_resumed = 0

    if checkpoint_path and os.path.isfile(checkpoint_path):
        with open(checkpoint_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                key = _candidate_key(rec["sys_id"], rec["ref_work_id"])
                # discovery-v3 (Codex blocker 3): a checkpoint line whose
                # fingerprint no longer matches the candidate's CURRENT inputs is
                # an answer to a superseded question. Dropped from `already_done`
                # so the pair is re-asked rather than resumed -- the resume path
                # is the one place a stale answer looks exactly like a completed
                # one, so a mid-run input refresh would otherwise be absorbed
                # silently and for free.
                if expected_fingerprints is not None:
                    want = expected_fingerprints.get(key)
                    if want is not None and rec.get("input_fingerprint") != want:
                        stale_resumed += 1
                        continue
                results[key] = {
                    "novelty_status": rec.get("novelty_status"),
                    "divergence_correctness": rec.get("divergence_correctness"),
                    "input_fingerprint": rec.get("input_fingerprint"),
                }
                already_done.add(key)

    checkpoint_fh = open(checkpoint_path, "a", encoding="utf-8") if checkpoint_path else None
    try:
        for candidate in residual_candidates:
            key = _candidate_key(candidate.sys_id, candidate.ref_work_id)
            if key in already_done:
                continue
            raw = model_call(candidate)
            resolved = dict(resolve_model_output(raw))
            if expected_fingerprints is not None:
                fp = expected_fingerprints.get(key)
                if fp is not None:
                    resolved["input_fingerprint"] = fp
            results[key] = resolved
            if checkpoint_fh is not None:
                record = {
                    "sys_id": candidate.sys_id,
                    "ref_work_id": candidate.ref_work_id,
                    **resolved,
                }
                checkpoint_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                checkpoint_fh.flush()
    finally:
        if checkpoint_fh is not None:
            checkpoint_fh.close()

    return results


def run_model_arm_batched(
    residual_candidates: Sequence[NoveltyCandidate],
    *,
    batch_model_call: Callable[[Sequence[NoveltyCandidate]], Optional[Mapping[str, Any]]],
    checkpoint_path: Optional[str] = None,
    # discovery-v3 (Codex blocker 3): `{grain_key: input_fingerprint}` for the
    # candidates being judged. Supplied -> each answer records the fingerprint of
    # the inputs it came from, and a checkpointed answer whose fingerprint no
    # longer matches is re-asked instead of resumed. Omitted -> pre-v3 behaviour.
    expected_fingerprints: Optional[Dict[str, str]] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cost_probe: Optional[Callable[[], float]] = None,
    cost_ceiling_usd: Optional[float] = None,
    single_model_call: Optional[Callable[[NoveltyCandidate], Optional[Mapping[str, Any]]]] = None,
    max_batch_attempts: int = 3,
    progress: Optional[Callable[[str], None]] = None,
) -> Dict[str, Dict[str, Optional[str]]]:
    """Batched counterpart to `run_model_arm` (owner ruling O), judging
    `batch_size` candidates per provider call instead of one.

    Guardrails, both requested by the owner before authorizing a production run:

    **Connection loss / malformed replies.** Checkpointing is per CANDIDATE and
    flushed after every batch, so a killed run resumes without re-billing
    answered cases. A reply that cannot be aligned 1:1 to the cases sent raises
    `BatchResponseInvalid` and is retried up to `max_batch_attempts`; if it
    still cannot be aligned, the batch DEGRADES to `single_model_call` -- the
    original, separately-validated single-case contract -- rather than guessing
    at alignment. If no single-case fallback is supplied, the batch is left
    entirely unresolved and retried on the next invocation. Nothing partial is
    ever checkpointed from an unaligned reply, because a positional mis-map
    would silently attribute one fragment's verdict to another and would be
    invisible downstream.

    **Price ballooning.** `cost_probe` is consulted BEFORE each batch is sent
    and must return REAL cumulative spend (read from the provider's own
    `usage.cost`, never an estimate). At or above `cost_ceiling_usd` the run
    raises `CostCeilingExceeded` and stops with its checkpoint intact, so the
    ceiling is a hard bound on authorized spend rather than a warning. The
    check is deliberately before the call, so the ceiling can never be crossed
    by the batch that discovers it.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    results: Dict[str, Dict[str, Optional[str]]] = {}
    already_done: set = set()
    stale_resumed = 0

    if checkpoint_path and os.path.isfile(checkpoint_path):
        with open(checkpoint_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                key = _candidate_key(rec["sys_id"], rec["ref_work_id"])
                # discovery-v3 (Codex blocker 3): a checkpoint line whose
                # fingerprint no longer matches the candidate's CURRENT inputs is
                # an answer to a superseded question. Dropped from `already_done`
                # so the pair is re-asked rather than resumed -- the resume path
                # is the one place a stale answer looks exactly like a completed
                # one, so a mid-run input refresh would otherwise be absorbed
                # silently and for free.
                if expected_fingerprints is not None:
                    want = expected_fingerprints.get(key)
                    if want is not None and rec.get("input_fingerprint") != want:
                        stale_resumed += 1
                        continue
                results[key] = {
                    "novelty_status": rec.get("novelty_status"),
                    "divergence_correctness": rec.get("divergence_correctness"),
                    "input_fingerprint": rec.get("input_fingerprint"),
                }
                already_done.add(key)

    pending = [
        c for c in residual_candidates
        if _candidate_key(c.sys_id, c.ref_work_id) not in already_done
    ]
    if progress is not None:
        progress(
            f"{len(already_done)} already checkpointed; {len(pending)} pending "
            f"in batches of {batch_size}"
            + (f"; {stale_resumed} checkpointed answer(s) DISCARDED as stale "
               "(their inputs changed since they were produced)"
               if stale_resumed else "")
        )

    checkpoint_fh = open(checkpoint_path, "a", encoding="utf-8") if checkpoint_path else None
    try:
        for start in range(0, len(pending), batch_size):
            chunk = pending[start:start + batch_size]

            if cost_ceiling_usd is not None and cost_probe is not None:
                spent = cost_probe()
                if spent >= cost_ceiling_usd:
                    raise CostCeilingExceeded(
                        f"real spend ${spent:.4f} reached the ${cost_ceiling_usd:.2f} ceiling "
                        f"with {len(pending) - start} candidates still pending; checkpoint is "
                        "intact and the run resumes from here once a higher ceiling is authorized"
                    )

            resolved_chunk: Optional[Dict[int, Dict[str, Optional[str]]]] = None
            last_error: Optional[BaseException] = None
            for attempt in range(1, max_batch_attempts + 1):
                try:
                    raw = batch_model_call(chunk)
                    resolved_chunk = resolve_batch_model_output(raw, len(chunk))
                    break
                except BatchResponseInvalid as exc:
                    last_error = exc
                    if progress is not None:
                        progress(f"batch at offset {start} unaligned (attempt {attempt}): {exc}")

            if resolved_chunk is None:
                if single_model_call is None:
                    if progress is not None:
                        progress(
                            f"batch at offset {start} left UNRESOLVED after {max_batch_attempts} "
                            f"attempts ({last_error}); it is not checkpointed and will be retried"
                        )
                    continue
                if progress is not None:
                    progress(
                        f"batch at offset {start} degrading to the single-case contract "
                        f"after {max_batch_attempts} unaligned replies"
                    )
                resolved_chunk = {
                    i + 1: resolve_model_output(single_model_call(c))
                    for i, c in enumerate(chunk)
                }

            for i, candidate in enumerate(chunk):
                key = _candidate_key(candidate.sys_id, candidate.ref_work_id)
                resolved = dict(resolved_chunk[i + 1])
                # The fingerprint of the inputs THIS answer came from, recorded
                # beside the answer so a later reader can PROVE the question
                # rather than assume it (Codex blocker 3).
                if expected_fingerprints is not None:
                    fp = expected_fingerprints.get(key)
                    if fp is not None:
                        resolved["input_fingerprint"] = fp
                results[key] = resolved
                if checkpoint_fh is not None:
                    checkpoint_fh.write(
                        json.dumps(
                            {
                                "sys_id": candidate.sys_id,
                                "ref_work_id": candidate.ref_work_id,
                                **resolved,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
            if checkpoint_fh is not None:
                checkpoint_fh.flush()
    finally:
        if checkpoint_fh is not None:
            checkpoint_fh.close()

    return results


# ---------------------------------------------------------------------------
# The owner-label grading harness (Task 2's own "part with a real failure
# mode behind it"). Three hard rules, per this task's own instruction:
#   1. Owner provenance is required per entry -- excluded, never used, if
#      absent.
#   2. Skipped cases are excluded and counted separately.
#   3. The two error directions are reported SEPARATELY, never folded into
#      one combined accuracy figure.
# ---------------------------------------------------------------------------

class NoOwnerProvenanceLabels(Exception):
    """Raised when ZERO entries in a label file carry owner-supplied
    provenance -- grading against such a file would grade the funnel
    against its own reasoning, and the resulting number would mean
    nothing. The literal message is pinned (mutation-tested, see
    tests/test_discovery_novelty_contract.py) so a caller can never
    mistake a generic exception for this specific, deliberate refusal."""


class LabelHashMismatch(Exception):
    """Raised when a label file's content hash does not match the hash
    recorded in 136-GATE1-DECISIONS.md -- refuses to grade against a file
    that may have been hand-edited post-labelling (T-136-03-06)."""


def _label_file_content_hash(cases: Sequence[Mapping[str, Any]]) -> str:
    canonical_cases = json.dumps(list(cases), sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical_cases.encode("utf-8")).hexdigest()


def load_owner_labels(path: str, *, expected_content_hash: Optional[str] = None) -> Dict[str, Any]:
    """Loads the owner label file (see plan 136-03 Task 4;
    `discovery_data/novelty_hardcase_labels-v1.json`) and, when
    `expected_content_hash` is given, re-verifies it against the hash
    recorded in `136-GATE1-DECISIONS.md`, refusing (raising
    `LabelHashMismatch`) on a mismatch rather than silently grading
    against a file that may have drifted from what the owner actually
    submitted."""
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if expected_content_hash is not None:
        actual_hash = _label_file_content_hash(data["cases"])
        if actual_hash != expected_content_hash:
            raise LabelHashMismatch(
                f"label file content hash mismatch: expected {expected_content_hash!r}, "
                f"got {actual_hash!r} -- refusing to grade against a file that does not match "
                "the hash recorded in 136-GATE1-DECISIONS.md"
            )
    return data


def _has_owner_provenance(case: Mapping[str, Any]) -> bool:
    prov = case.get("label_provenance") or {}
    return prov.get("source") == "owner_supplied"


def _is_skipped(case: Mapping[str, Any]) -> bool:
    return bool((case.get("verdict") or {}).get("skipped"))


def grade_against_owner_labels(
    cases: Sequence[Mapping[str, Any]],
    predictions: Optional[Mapping[Any, Optional[str]]] = None,
) -> Dict[str, Any]:
    """Grades a label-file `cases` array. `predictions` maps `case_id` ->
    a predicted `novelty_status` (or, for identity-question cases, a
    predicted identity token) -- supplied by the caller (a real funnel/model
    run, or a fixture in a test); demotion-question cases (Arm 2) need no
    prediction, since those rows ARE the funnel's own already-executed
    demotion (ruling J) -- grading tallies the owner's OWN
    demotion_correct/false_known verdicts directly.

    Raises `NoOwnerProvenanceLabels` (message EXACTLY "no owner-provenance
    labels") if ZERO cases carry owner-supplied provenance -- this is the
    explicit denominator guard; do not remove it, and do not "simplify" it
    to a bare truthiness check that a division-by-zero or a KeyError could
    equally well satisfy (see the mutation-test discussion in
    tests/test_discovery_novelty_contract.py).
    """
    predictions = predictions or {}

    provenance_cases = [c for c in cases if _has_owner_provenance(c)]
    excluded_no_provenance = len(cases) - len(provenance_cases)

    # THE DENOMINATOR GUARD. Load-bearing; see the docstring above.
    if len(provenance_cases) == 0:
        raise NoOwnerProvenanceLabels("no owner-provenance labels")

    skipped = [c for c in provenance_cases if _is_skipped(c)]
    gradable = [c for c in provenance_cases if not _is_skipped(c)]

    shade_cases = [c for c in gradable if c.get("question_type") == "shade"]
    identity_cases = [c for c in gradable if c.get("question_type") == "identity"]
    demotion_cases = [c for c in gradable if c.get("question_type") == "demotion"]
    no_verdict_cases = [c for c in gradable if c.get("question_type") == "no_verdict_by_design"]

    result: Dict[str, Any] = {
        "total_cases": len(cases),
        "excluded_no_provenance": excluded_no_provenance,
        "skipped": len(skipped),
        "effective_evaluation_size": len(gradable),
        "no_verdict_by_design_count": len(no_verdict_cases),
    }

    if shade_cases:
        result["shade_grading"] = _grade_shade(shade_cases, predictions)
    if identity_cases:
        result["identity_grading"] = _grade_identity(identity_cases, predictions)
    if demotion_cases:
        result["demotion_grading"] = _grade_demotion(demotion_cases)

    return result


# The two shade tokens that mean "genuinely novel" for the purpose of the
# two-directional error split below: `fills_gap` (the novelty shade
# vocabulary) and `same_work` is deliberately NOT included here -- identity
# questions are graded separately (`_grade_identity`), never folded into
# this novelty-direction framing.
_NOVEL_SHADE_TOKENS = frozenset({"fills_gap"})


def _grade_shade(cases: Sequence[Mapping[str, Any]], predictions: Mapping[Any, Optional[str]]) -> Dict[str, Any]:
    """Two-directional error report over shade-question cases (decision
    B / ruling J's own framing):

    - `false_novel_direction`: the prediction claims genuine novelty
      (`fills_gap`) but the owner says it is NOT -- the reputationally
      expensive direction (decision B: "telling a reader a finding is
      unrecorded when it is recorded").
    - `false_known_direction`: the owner says genuinely novel (`fills_gap`)
      but the prediction claims it is recorded under some other shade --
      ruling J's conservative, but real, lost-finding cost.

    These are ALWAYS reported as two separate counts -- never folded into
    one combined accuracy figure.
    """
    agreements = 0
    false_novel: List[Any] = []
    false_known: List[Any] = []
    other_disagreements: List[Any] = []
    missing_predictions: List[Any] = []

    for c in cases:
        cid = c["case_id"]
        owner_value = c["verdict"]["value"]
        if cid not in predictions:
            missing_predictions.append(cid)
            continue
        predicted = predictions[cid]
        if predicted == owner_value:
            agreements += 1
            continue
        predicted_novel = predicted in _NOVEL_SHADE_TOKENS
        owner_novel = owner_value in _NOVEL_SHADE_TOKENS
        if predicted_novel and not owner_novel:
            false_novel.append(cid)
        elif owner_novel and not predicted_novel:
            false_known.append(cid)
        else:
            other_disagreements.append(cid)

    return {
        "graded_count": len(cases) - len(missing_predictions),
        "missing_predictions": missing_predictions,
        "agreements": agreements,
        "false_novel_direction": {
            "count": len(false_novel),
            "case_ids": false_novel,
            "description": (
                "predicted fills_gap (novel) when the owner says it is not -- the "
                "reputationally expensive direction (decision B)"
            ),
        },
        "false_known_direction": {
            "count": len(false_known),
            "case_ids": false_known,
            "description": (
                "owner says fills_gap (genuinely novel) but the prediction claims some "
                "other, recorded shade -- ruling J's conservative lost-finding direction"
            ),
        },
        "other_disagreements": {"count": len(other_disagreements), "case_ids": other_disagreements},
    }


def _grade_identity(cases: Sequence[Mapping[str, Any]], predictions: Mapping[Any, Optional[str]]) -> Dict[str, Any]:
    """A plain agreement/disagreement tally over the identity spot-check
    question (same_work vs different_works) -- NOT the novelty axis, so it
    does not use the fills_gap-based two-directional framing above."""
    agreements = 0
    disagreements: List[Any] = []
    missing_predictions: List[Any] = []
    for c in cases:
        cid = c["case_id"]
        owner_value = c["verdict"]["value"]
        if cid not in predictions:
            missing_predictions.append(cid)
            continue
        predicted = predictions[cid]
        if predicted == owner_value:
            agreements += 1
        else:
            disagreements.append(cid)
    return {
        "graded_count": len(cases) - len(missing_predictions),
        "missing_predictions": missing_predictions,
        "agreements": agreements,
        "disagreements": disagreements,
    }


def _grade_demotion(cases: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Arm 2 rows ARE the funnel's own already-executed demotion (ruling
    J -- these rows never reach a model). Grading is a straight tally of
    the owner's OWN `demotion_correct`/`false_known` verdicts, never a
    comparison against a fresh prediction (there is nothing to predict --
    the demotion already happened)."""
    correct = [c["case_id"] for c in cases if c["verdict"]["value"] == "demotion_correct"]
    false_known = [c["case_id"] for c in cases if c["verdict"]["value"] == "false_known"]
    other = [
        c["case_id"]
        for c in cases
        if c["verdict"]["value"] not in ("demotion_correct", "false_known")
    ]
    return {
        "graded_count": len(cases),
        "demotion_correct_count": len(correct),
        "false_known_count": len(false_known),
        "false_known_case_ids": false_known,
        "unsure_or_other_count": len(other),
    }


# ---------------------------------------------------------------------------
# --dry-run: exercises the abstention path and the grading harness with a
# tiny built-in fixture, at ZERO cost -- no model call, no real sidecar
# reads.
# ---------------------------------------------------------------------------

def _run_dry_run_demo() -> int:
    print(
        "discovery_novelty_funnel --dry-run: exercising the heuristic pass, the structured "
        "abstention path, and the grading harness with a tiny synthetic fixture. No model call, "
        "no real sidecar reads.\n"
        f"  pinned contract: model={LLM_MODEL!r} version={LLM_MODEL_VERSION!r} "
        f"effort={LLM_REASONING_EFFORT!r} prompt_sha256={PROMPT_SHA256[:12]}... "
        f"input_normalization_sha256={INPUT_NORMALIZATION_SHA256[:12]}..."
    )

    candidate_confirmed = NoveltyCandidate(
        sys_id="demo-1",
        ref_work_id="w-demo-1",
        claimed_title="דוגמה",
        catalogue_text="זהו טקסט קטלוג המזכיר דוגמה במפורש",
    )
    candidate_residual = NoveltyCandidate(
        sys_id="demo-2",
        ref_work_id="w-demo-2",
        claimed_title="עבודה שאינה מוזכרת",
        catalogue_text="קטלוג כללי ללא זיהוי ספציפי",
    )
    candidate_no_source_text = NoveltyCandidate(
        sys_id="demo-3", ref_work_id="w-demo-3", claimed_title="עבודה נוספת",
    )

    for candidate in (candidate_confirmed, candidate_residual, candidate_no_source_text):
        result = run_heuristic_pass(candidate)
        print(
            f"  {candidate.sys_id}: resolved={result.resolved} "
            f"novelty_status={result.novelty_status} reason={result.reason}"
        )

    abstained = resolve_model_output({"abstain": True, "reason": "dry-run demo"})
    print(f"  structured abstention -> {abstained}")

    cache_key_demo = build_cache_key(
        {
            "llm_model": LLM_MODEL,
            "llm_model_version": LLM_MODEL_VERSION,
            "llm_reasoning_effort": LLM_REASONING_EFFORT,
            "prompt_sha256": PROMPT_SHA256,
            "input_normalization_sha256": INPUT_NORMALIZATION_SHA256,
            "sys_id": "demo-1",
            "ref_work_id": "w-demo-1",
            "claimed_title_normalized": normalize_free_text("דוגמה"),
            "claimed_author_normalized": "",
            "catalogue_text_normalized": normalize_free_text(candidate_confirmed.catalogue_text),
            "bibliography_text_normalized": "",
            "pgp_text_normalized": "",
            "fgp_text_normalized": "",
            "m_source_shelfmark_text_normalized": "",
        }
    )
    print(f"  cache key (demo) -> {cache_key_demo[:16]}...")

    owner_provenance_cases = [
        {
            "case_id": 1,
            "question_type": "shade",
            "verdict": {"value": "fills_gap", "skipped": False},
            "label_provenance": {"source": "owner_supplied"},
        },
    ]
    grading = grade_against_owner_labels(owner_provenance_cases, predictions={1: "fills_gap"})
    print(f"  grading harness (synthetic, owner-provenance present): {grading}")

    zero_provenance_cases = [
        {
            "case_id": 1,
            "question_type": "shade",
            "verdict": {"value": "fills_gap", "skipped": False},
            "label_provenance": {"source": "pipeline_supplied"},
        },
    ]
    try:
        grade_against_owner_labels(zero_provenance_cases)
    except NoOwnerProvenanceLabels as exc:
        print(f"  grading harness correctly refused a zero-owner-provenance file: {exc}")
    else:
        print("  UNEXPECTED: grading harness did not refuse a zero-owner-provenance file")
        return 1

    print("dry-run OK")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 136 plan 136-04 -- the committed novelty funnel runner (funnel-first, "
            "ruling J) and its owner-label grading harness. This module's public functions "
            "(run_heuristic_pass/run_heuristic_funnel/run_model_arm/grade_against_owner_labels) "
            "are the production entry points; a real corpus-scale run supplies real sidecar "
            "reads and a real pinned model client to run_model_arm's model_call parameter -- "
            "see docs/specs/discovery-novelty-v1.md and 136-NOVELTY-RUN.md."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Exercise the abstention path and the grading harness with a tiny built-in "
             "fixture. No model call, no real sidecar reads, zero cost.",
    )
    parser.add_argument(
        "--labels-file",
        default=DEFAULT_LABELS_PATH,
        help=f"path to the owner label file (default: {DEFAULT_LABELS_PATH})",
    )
    parser.add_argument(
        "--expected-label-hash",
        default=None,
        help="the content hash recorded in 136-GATE1-DECISIONS.md; if given, grading refuses "
             "to proceed on a mismatch",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        return _run_dry_run_demo()

    print(
        "This script's production entry points are library functions intended to be called "
        "with real sidecar data and a real pinned model client (see "
        "docs/specs/discovery-novelty-v1.md, section 4/5). Run with --dry-run to exercise the "
        "abstention and grading paths at zero cost, or see plan 136-04 Task 3 / "
        "136-NOVELTY-RUN.md for the authorized production run's own report."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
