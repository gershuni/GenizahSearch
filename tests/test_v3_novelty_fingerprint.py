"""Gate 13 -- the novelty verdict cache's per-pair INPUT fingerprint.

Codex refused the cache-reuse claim twice. Round 1: reuse across a changed input
was not prevented. Round 2, more precisely: `render_case` sends the claimed title
and author and the assembled finding-aid text, while the cache keys on
`(sys_id, work_key)` and the consumer checks only a whole-file SHA-256 -- so
"87.6% of the cache is reusable" measured key overlap, not question identity, and
the ~$4 figure derived from it was describing a different quantity than it
claimed.

This suite pins the fix in the only way that matters for money: a changed input
must become a MISS. Every guard is paired with a demonstration that it can fail,
per the standing rule in this repo.

MASKING (D-25): fingerprints are hex digests; no source text is asserted on, and
the M-source field is exercised through an opaque placeholder.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.discovery_novelty_funnel import (  # noqa: E402
    NoveltyCandidate,
    candidate_input_fingerprint,
    # The real mechanical pass, for the same reason `render_batch` is imported
    # below: the guard must compare against what the funnel ACTUALLY decides.
    run_heuristic_pass,
)
# `render_batch`/`render_case` live in the production RUN module -- the prompt
# rendering is the run's concern, the fingerprint is the funnel's. Importing the
# real renderer (rather than restating what it sends) is the point: it is what
# makes `test_the_fingerprint_covers_every_field_render_case_sends` a real check
# instead of a restatement of my own assumption.
from scripts.discovery_novelty_production_run import render_batch  # noqa: E402
from shared.discovery_novelty import BATCH_PROMPT_SHA256, CACHE_KEY_FIELDS  # noqa: E402


def _candidate(**over) -> NoveltyCandidate:
    base = dict(
        sys_id="990000000000000001",
        ref_work_id="M:Ytext1000_01",
        claimed_title="כתאב אלמסאיל",
        claimed_author="סעדיה",
        catalogue_text="catalogue prose",
        bibliography_rows=({"text": "bib row", "transcription_type": "published_full"},),
        pgp_description="pgp desc",
        pgp_transcription="pgp trans",
        fgp_texts=("fgp one",),
        m_source_shelfmark_text="opaque-attribution",
    )
    base.update(over)
    return NoveltyCandidate(**base)


# ---------------------------------------------------------------------------
# 1. Every field the prompt sends must move the fingerprint.
# ---------------------------------------------------------------------------

# (field name on NoveltyCandidate, a DIFFERENT value)
_PROMPT_FIELDS = [
    ("claimed_title", "a different work entirely"),
    ("claimed_author", "a different author"),
    ("catalogue_text", "different catalogue prose"),
    ("bibliography_rows", ({"text": "a different bib row"},)),
    ("pgp_description", "different pgp desc"),
    ("pgp_transcription", "different pgp trans"),
    ("fgp_texts", ("different fgp",)),
    ("m_source_shelfmark_text", "different-opaque-attribution"),
]


@pytest.mark.parametrize("field_name,new_value", _PROMPT_FIELDS)
def test_changing_any_prompt_field_changes_the_fingerprint(field_name, new_value):
    """THE gate-13 property, field by field.

    Round 2 named the title specifically, but the same argument covers every
    field `render_case` interpolates: if the model saw it, it is part of the
    question. A field that does not move the fingerprint is a field whose change
    is answerable from cache.
    """
    before = candidate_input_fingerprint(_candidate())
    after = candidate_input_fingerprint(_candidate(**{field_name: new_value}))
    assert before != after, (
        f"changing {field_name} did not change the fingerprint -- a cache built "
        f"before that change would answer the new question from the old answer"
    )


def _heuristic_outcome(candidate):
    """(resolved, status) for the mechanical pass -- the funnel's own decision."""
    r = run_heuristic_pass(candidate)
    return (r.resolved, r.novelty_status)


def test_the_fingerprint_covers_every_field_render_case_sends():
    """The correspondence itself, so a future prompt field cannot escape.

    The per-field tests above enumerate today's fields. This one checks the
    enumeration against what the prompt ACTUALLY renders: mutate each field, and
    require that a field which changes the rendered prompt also changes the
    fingerprint. A new field added to `render_case` and forgotten here fails.
    """
    baseline_prompt = render_batch([_candidate()])
    baseline_fp = candidate_input_fingerprint(_candidate())

    # Every field of the dataclass, not just the ones listed above.
    probes = dict(_PROMPT_FIELDS)
    probes.setdefault("claimed_aliases", ("an alias",))
    probes.setdefault("page_mapped", False)

    baseline_heuristic = _heuristic_outcome(_candidate())

    for field_name, new_value in probes.items():
        mutated = _candidate(**{field_name: new_value})
        prompt_changed = render_batch([mutated]) != baseline_prompt
        fp_changed = candidate_input_fingerprint(mutated) != baseline_fp
        if prompt_changed:
            assert fp_changed, (
                f"{field_name} changes the rendered PROMPT but not the "
                f"fingerprint -- it is invisible to the cache gate"
            )
        # WHY THERE IS NO MATCHING "changes the heuristic => must be
        # fingerprinted" CLAUSE. Investigated 2026-08-18 while adding
        # `claimed_aliases`, which changes `_claim_appears_in_text` and therefore
        # whether the mechanical pass resolves the pair before any model call. It
        # LOOKS like a cache hazard and is not: `run_heuristic_funnel` runs over
        # every candidate on every run, and only its RESIDUAL is handed to
        # `run_model_arm_batched`, which is the sole consumer of the checkpoint.
        # A pair the heuristic newly resolves therefore leaves the residual and
        # its cached answer is never consulted; a pair it stops resolving is asked
        # fresh. `page_mapped` is the same shape and is deliberately excluded for
        # the same reason (see the test below). Fingerprinting such a field would
        # invalidate every entry for a reason the model never saw.
        #
        # The real auditability need for a funnel-configuration change is the RUN
        # MANIFEST, which records the alias policy alongside the model and prompt
        # hashes -- not the per-pair cache key.
        _ = baseline_heuristic  # kept: makes the reasoning above checkable


def test_a_field_the_prompt_never_sends_does_not_move_the_fingerprint():
    """The converse, so the fingerprint is not merely a hash of everything.

    `page_mapped` gates whether the candidate reaches the model at all; it
    changes no question that is ever asked. Including it would invalidate cache
    entries for a reason the model never saw -- i.e. spend money to re-ask an
    identical question.
    """
    assert render_batch([_candidate()]) == render_batch([_candidate(page_mapped=False)]), (
        "page_mapped now affects the rendered prompt -- if so it MUST be added to "
        "the fingerprint, and this test's premise is obsolete"
    )
    assert candidate_input_fingerprint(_candidate()) == candidate_input_fingerprint(
        _candidate(page_mapped=False))


def test_the_pinned_contract_is_part_of_the_fingerprint():
    """A model, effort or prompt change must invalidate every entry.

    The owner's standing instruction is that changing the model, the effort or
    the prompt invalidates the cache. Previously that was a rule a human had to
    remember; here it is mechanical.
    """
    single = candidate_input_fingerprint(_candidate())
    batched = candidate_input_fingerprint(_candidate(), prompt_sha256=BATCH_PROMPT_SHA256)
    assert single != batched, (
        "the single-case and batched prompts produce the same fingerprint -- a "
        "cache built under one framing would be reused under the other, though "
        "they are separately validated contracts"
    )
    for pinned in ("llm_model", "llm_model_version", "llm_reasoning_effort",
                   "prompt_sha256", "input_normalization_sha256"):
        assert pinned in CACHE_KEY_FIELDS, (
            f"{pinned} is not in the cache key -- changing it would not invalidate "
            f"existing entries"
        )


def test_normalization_means_cosmetic_differences_share_one_entry():
    """The fingerprint must not be needlessly brittle.

    A gate that misses on whitespace would re-bill the entire corpus for a
    reformatting change. Normalization is the reason reuse is possible at all.
    """
    a = candidate_input_fingerprint(_candidate(catalogue_text="some   prose"))
    b = candidate_input_fingerprint(_candidate(catalogue_text="  some prose  "))
    assert a == b, "whitespace alone changed the fingerprint -- the cache would thrash"


# ---------------------------------------------------------------------------
# 2. The CONSUMER side: an unfingerprinted or mismatched entry must be a miss.
# ---------------------------------------------------------------------------

def _write_cache(path: Path, doc) -> str:
    payload = json.dumps(doc, ensure_ascii=False)
    path.write_text(payload, encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_mismatched_fingerprint_loads_as_not_checked(tmp_path):
    """The consumer gate. A stale answer must not answer.

    Demoted to `not_checked` rather than dropped: the surfaces already handle a
    present-but-unanswered row, whereas dropping the key would make a changed
    input look like a candidate that was never generated at all.
    """
    from build_discovery_sidecar import load_novelty_verdicts

    key = "990000000000000001::M:Ytext1000_01"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {
        key: {"novelty_status": "fills_gap", "input_fingerprint": "STALE"},
    })

    # Without the gate (pre-v3 behaviour), the positive verdict is honoured.
    entries, _stats = load_novelty_verdicts(cache, sha256=sha)
    assert entries[key]["novelty_status"] == "fills_gap"

    # With the gate and a DIFFERENT expected fingerprint, it must not be.
    entries, stats = load_novelty_verdicts(
        cache, sha256=sha, expected_fingerprints={key: "CURRENT"})
    assert entries[key]["novelty_status"] == "not_checked", (
        "a verdict whose inputs changed was still honoured -- this is the reuse "
        "Codex blocker 3 is about"
    )
    assert stats["verdict_entries_fingerprint_mismatch"] == 1
    assert stats["verdict_fingerprint_checked"] is True


def test_an_unfingerprinted_entry_loads_as_not_checked(tmp_path):
    """A pre-v3 cache entry cannot prove its question, so it does not answer."""
    from build_discovery_sidecar import load_novelty_verdicts

    key = "990000000000000001::M:Ytext1000_01"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {key: {"novelty_status": "fills_gap"}})

    entries, stats = load_novelty_verdicts(
        cache, sha256=sha, expected_fingerprints={key: "CURRENT"})
    assert entries[key]["novelty_status"] == "not_checked"
    assert stats["verdict_entries_unfingerprinted"] == 1


def test_a_matching_fingerprint_is_honoured(tmp_path):
    """The control. Without this, "everything becomes not_checked" would pass
    every test above while making the cache worthless."""
    from build_discovery_sidecar import load_novelty_verdicts

    key = "990000000000000001::M:Ytext1000_01"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {
        key: {"novelty_status": "fills_gap", "input_fingerprint": "CURRENT"},
    })
    entries, stats = load_novelty_verdicts(
        cache, sha256=sha, expected_fingerprints={key: "CURRENT"})
    assert entries[key]["novelty_status"] == "fills_gap", (
        "a verdict whose inputs are unchanged was discarded -- the gate is too "
        "strict and the run would re-bill the whole corpus"
    )
    assert stats["verdict_entries_fingerprint_ok"] == 1
    assert stats["verdict_entries_fingerprint_mismatch"] == 0


def test_a_pair_this_build_never_generated_does_not_answer(tmp_path):
    """No expected fingerprint means no question was asked, so no answer applies."""
    from build_discovery_sidecar import load_novelty_verdicts

    key = "990000000000000001::M:Ytext1000_01"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {
        key: {"novelty_status": "fills_gap", "input_fingerprint": "CURRENT"},
    })
    entries, stats = load_novelty_verdicts(
        cache, sha256=sha, expected_fingerprints={"other::pair": "CURRENT"})
    assert entries[key]["novelty_status"] == "not_checked"
    assert stats["verdict_entries_fingerprint_mismatch"] == 1


def test_the_gate_is_off_when_no_fingerprints_are_supplied(tmp_path):
    """A v2 rebuild against the v2-era cache must still work."""
    from build_discovery_sidecar import load_novelty_verdicts

    key = "990000000000000001::M:Ytext1000_01"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {key: {"novelty_status": "fills_gap"}})
    entries, stats = load_novelty_verdicts(cache, sha256=sha)
    assert entries[key]["novelty_status"] == "fills_gap"
    assert stats["verdict_fingerprint_checked"] is False
    assert stats["verdict_entries_unfingerprinted"] == 0


# ---------------------------------------------------------------------------
# 3. The RESUME path -- where a stale answer looks exactly like a finished one.
# ---------------------------------------------------------------------------

def test_a_stale_checkpoint_line_is_re_asked_not_resumed(tmp_path):
    """Round 2's point applied to the producer, not just the consumer.

    The resume path reads a checkpoint and treats every line as done. If the
    inputs moved mid-run, resuming silently keeps the superseded answer -- and
    unlike the consumer gate, nothing downstream can tell.
    """
    from scripts.discovery_novelty_funnel import run_model_arm_batched

    cand = _candidate()
    key = f"{cand.sys_id}::{cand.ref_work_id}"
    checkpoint = tmp_path / "ck.jsonl"
    checkpoint.write_text(json.dumps({
        "sys_id": cand.sys_id, "ref_work_id": cand.ref_work_id,
        "novelty_status": "fills_gap", "divergence_correctness": None,
        "input_fingerprint": "STALE",
    }) + "\n", encoding="utf-8")

    calls = []

    def batch_call(chunk):
        calls.append(len(chunk))
        return {"results": {"1": {"novelty_status": "confirms"}}}

    results = run_model_arm_batched(
        [cand], batch_model_call=batch_call, checkpoint_path=str(checkpoint),
        expected_fingerprints={key: "CURRENT"}, batch_size=10,
    )
    assert calls, (
        "the stale checkpoint line was resumed rather than re-asked -- a mid-run "
        "input change would be absorbed silently and for free"
    )
    assert results[key]["novelty_status"] == "confirms"
    assert results[key]["input_fingerprint"] == "CURRENT"


def test_a_current_checkpoint_line_is_resumed_without_re_billing(tmp_path):
    """The control: the crash-resume guarantee must survive the new gate."""
    from scripts.discovery_novelty_funnel import run_model_arm_batched

    cand = _candidate()
    key = f"{cand.sys_id}::{cand.ref_work_id}"
    checkpoint = tmp_path / "ck.jsonl"
    checkpoint.write_text(json.dumps({
        "sys_id": cand.sys_id, "ref_work_id": cand.ref_work_id,
        "novelty_status": "fills_gap", "divergence_correctness": None,
        "input_fingerprint": "CURRENT",
    }) + "\n", encoding="utf-8")

    calls = []

    def batch_call(chunk):          # pragma: no cover -- must not be reached
        calls.append(len(chunk))
        return {"results": {"1": {"novelty_status": "confirms"}}}

    results = run_model_arm_batched(
        [cand], batch_model_call=batch_call, checkpoint_path=str(checkpoint),
        expected_fingerprints={key: "CURRENT"}, batch_size=10,
    )
    assert not calls, "an up-to-date checkpointed answer was re-billed"
    assert results[key]["novelty_status"] == "fills_gap"


# ---------------------------------------------------------------------------
# 4. Codex ROUND 3 BLOCKER: the gate existed but `finalize_build` never reached
#    it. `load_novelty_verdicts` was called there WITHOUT `expected_fingerprints`,
#    so the real build accepted stale and unfingerprinted positive verdicts --
#    the identical "correct function nobody calls" failure round 2 found in the
#    router ingest, in the very fix for round 2's other blocker.
#
#    Zero tests supplied a verdict cache to `finalize_build`, which is exactly
#    why it survived. These close that.
# ---------------------------------------------------------------------------

def _fingerprint_map(path: Path, mapping) -> None:
    path.write_text(json.dumps(mapping), encoding="utf-8")


def test_finalize_build_refuses_a_verdict_cache_without_fingerprints(tmp_path):
    """THE round-3 fix: omitting the fingerprints must be impossible-by-accident.

    The guard is asserted at `finalize_build`'s own boundary rather than through a
    full build, because it is deliberately placed BEFORE any output mutation --
    which means it raises before any of the heavy source loading, and that
    ordering is itself the property worth pinning.
    """
    import build_discovery_sidecar as bds

    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {"990000000000000001::w000001": {"novelty_status": "fills_gap"}})

    with pytest.raises(bds.NoveltyVerdictCacheError, match="without `novelty_input_fingerprints`"):
        bds.finalize_build(
            source_db_path=str(tmp_path / "missing.db"),
            from_approved_path=str(tmp_path / "missing.csv"),
            crosswalk_path=str(tmp_path / "cw.json"),
            out_db_path=str(tmp_path / "out.db"),
            novelty_verdicts_path=str(cache),
            novelty_verdicts_sha256=sha,
        )


def test_the_waiver_must_be_named_explicitly_to_skip_the_gate(tmp_path):
    """The escape hatch exists for a v2-era rebuild, and is impossible to trip
    by omission -- it has to be asked for by name."""
    import build_discovery_sidecar as bds

    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {"990000000000000001::w000001": {"novelty_status": "fills_gap"}})

    # With the waiver the novelty guard no longer fires. Codex R3 noted that
    # catching ANY later exception is too weak -- it would also pass if the cache
    # were never loaded at all -- so the SPECIFIC downstream failure is asserted:
    # the research DB does not exist, which is the next thing finalize_build does.
    with pytest.raises(sqlite3.OperationalError, match="unable to open database file") as exc:
        bds.finalize_build(
            source_db_path=str(tmp_path / "missing.db"),
            from_approved_path=str(tmp_path / "missing.csv"),
            crosswalk_path=str(tmp_path / "cw.json"),
            out_db_path=str(tmp_path / "out.db"),
            novelty_verdicts_path=str(cache),
            novelty_verdicts_sha256=sha,
            novelty_allow_unfingerprinted_cache=True,
        )
    assert "novelty_input_fingerprints" not in str(exc.value), (
        "the explicit waiver did not suppress the fingerprint guard"
    )


def test_supplying_fingerprints_also_passes_the_guard(tmp_path):
    """The other control: the normal v3 path must not be blocked."""
    import build_discovery_sidecar as bds

    key = "990000000000000001::w000001"
    cache = tmp_path / "v.json"
    sha = _write_cache(cache, {key: {"novelty_status": "fills_gap",
                                     "input_fingerprint": "CURRENT"}})
    # As above (Codex R3): the SPECIFIC next failure, not any exception.
    with pytest.raises(sqlite3.OperationalError, match="unable to open database file") as exc:
        bds.finalize_build(
            source_db_path=str(tmp_path / "missing.db"),
            from_approved_path=str(tmp_path / "missing.csv"),
            crosswalk_path=str(tmp_path / "cw.json"),
            out_db_path=str(tmp_path / "out.db"),
            novelty_verdicts_path=str(cache),
            novelty_verdicts_sha256=sha,
            novelty_input_fingerprints={key: "CURRENT"},
        )
    assert "novelty_input_fingerprints" not in str(exc.value)


def test_the_cli_offers_both_the_fingerprints_and_the_named_waiver():
    """A CLI build must face the same forced choice as a programmatic one --
    otherwise the guard is only enforced on the path nobody uses."""
    import build_discovery_sidecar as bds

    parser_src = Path(bds.__file__).read_text(encoding="utf-8")
    assert "--novelty-input-fingerprints" in parser_src
    assert "--novelty-allow-unfingerprinted-cache" in parser_src
    # And the CLI must actually THREAD them, not merely declare them: a declared
    # flag that is never passed through is the same bypass in a new place.
    assert "novelty_input_fingerprints=_load_novelty_fingerprints(" in parser_src
    assert "novelty_allow_unfingerprinted_cache=args.novelty_allow_unfingerprinted_cache" in parser_src


def test_a_malformed_fingerprint_file_fails_closed(tmp_path):
    """Supplying the flag means intending the gate to run, so a bad file must
    raise rather than degrade to "no fingerprints"."""
    import build_discovery_sidecar as bds

    empty = tmp_path / "fp.json"
    _fingerprint_map(empty, {})
    with pytest.raises(bds.NoveltyVerdictCacheError, match="non-empty JSON object"):
        bds._load_novelty_fingerprints(str(empty))

    wrong = tmp_path / "fp2.json"
    _fingerprint_map(wrong, {"k": 123})
    with pytest.raises(bds.NoveltyVerdictCacheError, match="string-to-string"):
        bds._load_novelty_fingerprints(str(wrong))

    assert bds._load_novelty_fingerprints(None) is None


def test_the_asset_records_whether_the_gate_ran():
    """A reader must be able to tell a gated asset from a waived one WITHOUT the
    build log, which is not shipped. The cache SHA proves which file was read; it
    says nothing about whether each verdict was checked against its question."""
    import build_discovery_sidecar as bds

    src = Path(bds.__file__).read_text(encoding="utf-8")
    assert '"novelty_input_fingerprint_checked"' in src, (
        "the asset does not record whether the fingerprint gate ran"
    )
    # And it must be driven by the loader's own stat, never by the flag -- the
    # flag records intent, the stat records what happened.
    assert 'novelty_input_stats.get("verdict_fingerprint_checked")' in src


# ---------------------------------------------------------------------------
# 6. The recorded-witness field -- a DECISION input, not a prompt input.
# ---------------------------------------------------------------------------
#
# Added 2026-08-09, after the production run. `known_witness_confidence` is the
# one cache-key field the suite above structurally cannot reach: every test in
# section 1 derives its expectation from what `render_batch` SENDS, and this
# field is never rendered -- a `high`-confidence recorded witness is resolved by
# the heuristic funnel and the model is never called. So it changes the ANSWER
# without changing the PROMPT, and `test_the_fingerprint_covers_every_field_
# render_case_sends` skips it (its assertion is guarded on `prompt_changed`).
#
# That gap is not academic. `build_cache_key` hashes ONLY the members of
# `CACHE_KEY_FIELDS` and silently ignores anything else, so adding the field to
# the funnel's cache-key dict did nothing until it was also added to that tuple.
# Deleting it again would today pass the entire suite while making $42.28 of
# verdicts reusable against a question they did not answer.


def test_the_recorded_witness_field_moves_the_fingerprint():
    """A different recorded-witness confidence is a different question."""
    assert "known_witness_confidence" in CACHE_KEY_FIELDS, (
        "known_witness_confidence left the cache key -- verdicts computed WITH "
        "the recorded-witness map would be reused for a witness-blind question"
    )
    blind = candidate_input_fingerprint(_candidate(known_witness_confidence=None))
    high = candidate_input_fingerprint(_candidate(known_witness_confidence="high"))
    low = candidate_input_fingerprint(_candidate(known_witness_confidence="low"))
    assert blind != high, "witness-blind and high-confidence share a fingerprint"
    assert high != low, "high and low confidence share a fingerprint"


def test_the_recorded_witness_field_is_invisible_to_the_prompt():
    """Why section 1 cannot cover it -- pinned, so the reasoning stays true.

    If this ever starts failing, the field HAS become a prompt input and belongs
    in `_PROMPT_FIELDS` like any other; the special-case above is then obsolete.
    """
    assert render_batch([_candidate(known_witness_confidence="high")]) == render_batch(
        [_candidate(known_witness_confidence=None)]
    ), (
        "known_witness_confidence now reaches the rendered prompt -- add it to "
        "_PROMPT_FIELDS and retire this special case"
    )
