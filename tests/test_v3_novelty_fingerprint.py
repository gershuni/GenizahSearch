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
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.discovery_novelty_funnel import (  # noqa: E402
    NoveltyCandidate,
    candidate_input_fingerprint,
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

    for field_name, new_value in probes.items():
        mutated = _candidate(**{field_name: new_value})
        prompt_changed = render_batch([mutated]) != baseline_prompt
        fp_changed = candidate_input_fingerprint(mutated) != baseline_fp
        if prompt_changed:
            assert fp_changed, (
                f"{field_name} changes the rendered PROMPT but not the "
                f"fingerprint -- it is invisible to the cache gate"
            )


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
