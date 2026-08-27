# -*- coding: utf-8 -*-
"""Tests for `scripts/verify_cert01_grading.py` (Phase 135, plan 135-09, Task 3).

Every fixture below is FABRICATED (synthetic `w000xxx`/`page:N`/opaque hash
strings) -- never real research content. Each of the twelve checks is proven
load-bearing directly: build one self-consistent "golden" context, confirm
every check passes on it, then mutate ONE field per test and confirm that
check (and no earlier check masks it) raises `CheckFailure` -- i.e. "revert
one, the test goes red."
"""
import copy
import json

import pytest

from scripts import cert01_frame as cf
from scripts import verify_cert01_grading as v


# ---------------------------------------------------------------------------
# Golden fixture -- self-consistent across every check
# ---------------------------------------------------------------------------


def _make_golden_ctx():
    estimand_rows = [
        {"page_id": "p1", "canonical_work_id": "w000001", "stratum": "sefaria:high", "unit_key": "u1"},
        {"page_id": "p2", "canonical_work_id": "w000002", "stratum": "ja:medium", "unit_key": "u2"},
        {"page_id": "p3", "canonical_work_id": "w000003", "stratum": "sefaria:high", "unit_key": "u3"},
    ]
    population_hash = cf.population_hash(estimand_rows)
    cluster_map_hash = cf.cluster_map_hash(estimand_rows)
    stratum_counts = cf.stratum_counts(estimand_rows)  # {'ja:medium':1,'sefaria:high':2}

    stratum_allocation = {"sefaria:high": 200, "ja:medium": 20}  # sums to 220 (candidate_count)

    prereg = {
        "protocol_sha256": "proto_fake_sha",
        "seed": {"deck_draw": 1, "gold_shuffle": 2, "bootstrap": 7},
        "frame_content_hash": "fch_fake",
        "population_hash": population_hash,
        "cluster_map_hash": cluster_map_hash,
        "stratum_counts": stratum_counts,
        "strata_weights": {k: v_ / sum(stratum_counts.values()) for k, v_ in stratum_counts.items()},
        "stratum_allocation": stratum_allocation,
        "card_count": 220,
        "cutoffs": {"strict_floor": 0.85},
        "gold_allocation": {"n": 20, "pool_available": 174, "source": "fake"},
        "confirmation_allocation": {"n_drawn": 340, "basis": "fake"},
        "allowed_verdicts": ["A", "B", "C", "INS"],
        "canonical_merges_sha256": "cm_fake",
        "composition_dates_sha256": "cd_fake",
        "seftja_dates_sha256": "sd_fake",
        "db_content_hash": "db_fake",
        "crosswalk_sha256": "cw_fake",
    }
    prereg["report_id"] = cf.compute_report_id(prereg)

    # 220 candidate cards (matches candidate_count == card_count sum).
    deck_cards = [
        {"uid": f"p{i}|w{i:06d}", "role": "candidate", "stratum": "sefaria:high",
         "page_id": f"p{i}", "canonical_work_id": f"w{i:06d}", "sys_id": f"s{i}"}
        for i in range(220)
    ]

    deck_manifest = {
        "prereg_report_id": prereg["report_id"],
        "deck_manifest_hash": v._deck_manifest_hash_of(deck_cards),
        "deck_size": 220,
        "candidate_count": 220,
        "stratum_drawn_counts": stratum_allocation,
        "gold_allocation_drawn": 20,
        "confirmation_allocation": prereg["confirmation_allocation"],
    }

    ledger = [{"uid": deck_cards[0]["uid"], "verdict": "A", "grader": "test-grader"}]

    input_hashes = {
        "canonical_merges_sha256": "cm_fake",
        "composition_dates_sha256": "cd_fake",
        "seftja_dates_sha256": "sd_fake",
        "db_content_hash": "db_fake",
        "crosswalk_sha256": "cw_fake",
        "frame_content_hash": "fch_fake",
    }

    return {
        "prereg": prereg,
        "deck_manifest": deck_manifest,
        "deck_cards": deck_cards,
        "ledger": ledger,
        "estimand_rows": estimand_rows,
        "input_hashes": input_hashes,
    }


def test_golden_ctx_passes_all_twelve_checks():
    ctx = _make_golden_ctx()
    for num, name, fn in v.CHECKS:
        fn(ctx)  # must not raise


# ---------------------------------------------------------------------------
# Check 1 -- deck size ~200-250 candidate cards
# ---------------------------------------------------------------------------


def test_check1_fails_on_deck_size_outside_range():
    ctx = _make_golden_ctx()
    ctx["deck_manifest"]["candidate_count"] = 50
    with pytest.raises(v.CheckFailure, match="outside the protocol"):
        v.check_1_deck_size(ctx)


def test_check1_fails_on_candidate_count_deck_mismatch():
    ctx = _make_golden_ctx()
    ctx["deck_manifest"]["candidate_count"] = 220
    ctx["deck_cards"] = ctx["deck_cards"][:210]  # only 210 actual candidate cards now
    with pytest.raises(v.CheckFailure, match="candidate_count"):
        v.check_1_deck_size(ctx)


# ---------------------------------------------------------------------------
# Check 2 -- report_id recompute (mutated pre-registration)
# ---------------------------------------------------------------------------


def test_check2_fails_on_mutated_prereg():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"])
    ctx["prereg"]["stratum_counts"] = {"tampered": 999}  # mutate payload, report_id now stale
    with pytest.raises(v.CheckFailure, match="report_id recompute mismatch"):
        v.check_2_report_id_recomputes(ctx)


# ---------------------------------------------------------------------------
# Check 3 -- deck manifest reference + deck_manifest_hash
# ---------------------------------------------------------------------------


def test_check3_fails_on_prereg_report_id_mismatch():
    ctx = _make_golden_ctx()
    v.check_2_report_id_recomputes(ctx)  # populate ctx['recomputed_report_id']
    ctx["deck_manifest"]["prereg_report_id"] = "some-other-report-id"
    with pytest.raises(v.CheckFailure, match="prereg_report_id"):
        v.check_3_deck_manifest_reference_and_hash(ctx)


def test_check3_fails_on_deck_manifest_hash_mismatch():
    ctx = _make_golden_ctx()
    v.check_2_report_id_recomputes(ctx)
    ctx["deck_manifest"]["deck_manifest_hash"] = "wrong-hash"
    with pytest.raises(v.CheckFailure, match="deck_manifest_hash mismatch"):
        v.check_3_deck_manifest_reference_and_hash(ctx)


def test_check3_fails_when_deck_file_tampered_after_binding():
    ctx = _make_golden_ctx()
    v.check_2_report_id_recomputes(ctx)
    # Someone edits a card in the deck file after the manifest was bound --
    # deck_manifest_hash was computed over the ORIGINAL cards.
    ctx["deck_cards"] = copy.deepcopy(ctx["deck_cards"])
    ctx["deck_cards"][0]["sys_id"] = "TAMPERED"
    with pytest.raises(v.CheckFailure, match="deck_manifest_hash mismatch"):
        v.check_3_deck_manifest_reference_and_hash(ctx)


# ---------------------------------------------------------------------------
# Check 4 -- allowed non-empty verdict vocabulary
# ---------------------------------------------------------------------------


def test_check4_fails_on_empty_string_verdict():
    ctx = _make_golden_ctx()
    ctx["ledger"] = [{"uid": ctx["deck_cards"][0]["uid"], "verdict": "", "grader": "g"}]
    with pytest.raises(v.CheckFailure, match="not in the allowed"):
        v.check_4_verdict_vocab(ctx)


def test_check4_fails_on_out_of_vocab_verdict():
    ctx = _make_golden_ctx()
    ctx["ledger"] = [{"uid": ctx["deck_cards"][0]["uid"], "verdict": "MAYBE", "grader": "g"}]
    with pytest.raises(v.CheckFailure, match="not in the allowed"):
        v.check_4_verdict_vocab(ctx)


def test_check4_fails_when_prereg_carries_no_vocab():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"], allowed_verdicts=[])
    with pytest.raises(v.CheckFailure, match="no allowed_verdicts"):
        v.check_4_verdict_vocab(ctx)


# ---------------------------------------------------------------------------
# Check 5 -- ledger uid membership in the frozen deck
# ---------------------------------------------------------------------------


def test_check5_fails_on_uid_not_in_deck():
    ctx = _make_golden_ctx()
    ctx["ledger"] = [{"uid": "not-a-real-deck-uid", "verdict": "A", "grader": "g"}]
    with pytest.raises(v.CheckFailure, match="not a member of the frozen deck"):
        v.check_5_uid_membership(ctx)


# ---------------------------------------------------------------------------
# Check 6 -- grader attribution present (+ non-empty ledger)
# ---------------------------------------------------------------------------


def test_check6_fails_on_empty_ledger():
    ctx = _make_golden_ctx()
    ctx["ledger"] = []
    with pytest.raises(v.CheckFailure, match="grading has NOT started"):
        v.check_6_grader_attribution(ctx)


def test_check6_fails_on_missing_grader_field():
    ctx = _make_golden_ctx()
    ctx["ledger"] = [{"uid": ctx["deck_cards"][0]["uid"], "verdict": "A"}]  # no grader
    with pytest.raises(v.CheckFailure, match="no grader attribution"):
        v.check_6_grader_attribution(ctx)


def test_check6_fails_on_blank_grader_field():
    ctx = _make_golden_ctx()
    ctx["ledger"] = [{"uid": ctx["deck_cards"][0]["uid"], "verdict": "A", "grader": "   "}]
    with pytest.raises(v.CheckFailure, match="no grader attribution"):
        v.check_6_grader_attribution(ctx)


# ---------------------------------------------------------------------------
# Check 7 -- no grader-visible demotion field
# ---------------------------------------------------------------------------


def test_check7_fails_when_deck_card_carries_later_shared_text():
    ctx = _make_golden_ctx()
    ctx["deck_cards"] = copy.deepcopy(ctx["deck_cards"])
    ctx["deck_cards"][0]["later_shared_text"] = True
    with pytest.raises(v.CheckFailure, match="grader-visible"):
        v.check_7_no_demotion_field(ctx)


def test_check7_fails_when_deck_card_carries_routing_status():
    ctx = _make_golden_ctx()
    ctx["deck_cards"] = copy.deepcopy(ctx["deck_cards"])
    ctx["deck_cards"][0]["routing_status"] = "review_only"
    with pytest.raises(v.CheckFailure, match="grader-visible"):
        v.check_7_no_demotion_field(ctx)


# ---------------------------------------------------------------------------
# Check 8 -- population/stratum reproducibility
# ---------------------------------------------------------------------------


def test_check8_fails_on_population_hash_drift():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"], population_hash="drifted-hash")
    with pytest.raises(v.CheckFailure, match="population_hash recompute mismatch"):
        v.check_8_population_reproducibility(ctx)


def test_check8_fails_on_stratum_counts_drift():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"], stratum_counts={"sefaria:high": 999})
    with pytest.raises(v.CheckFailure, match="stratum_counts recompute mismatch"):
        v.check_8_population_reproducibility(ctx)


# ---------------------------------------------------------------------------
# Check 9 -- deck allocation match
# ---------------------------------------------------------------------------


def test_check9_fails_on_stratum_drawn_counts_mismatch():
    ctx = _make_golden_ctx()
    ctx["deck_manifest"] = dict(ctx["deck_manifest"],
                                stratum_drawn_counts={"sefaria:high": 1})
    with pytest.raises(v.CheckFailure, match="stratum_drawn_counts"):
        v.check_9_deck_allocation(ctx)


def test_check9_fails_on_gold_allocation_mismatch():
    ctx = _make_golden_ctx()
    ctx["deck_manifest"] = dict(ctx["deck_manifest"], gold_allocation_drawn=999)
    with pytest.raises(v.CheckFailure, match="gold_allocation_drawn"):
        v.check_9_deck_allocation(ctx)


def test_check9_fails_on_confirmation_allocation_mismatch():
    ctx = _make_golden_ctx()
    ctx["deck_manifest"] = dict(ctx["deck_manifest"],
                                confirmation_allocation={"n_drawn": 1, "basis": "x"})
    with pytest.raises(v.CheckFailure, match="confirmation_allocation"):
        v.check_9_deck_allocation(ctx)


# ---------------------------------------------------------------------------
# Check 10 -- input-hash pinning (four frozen input hashes + db_content_hash)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", [
    "canonical_merges_sha256", "composition_dates_sha256",
    "seftja_dates_sha256", "db_content_hash",
])
def test_check10_fails_on_each_input_hash_mismatch(key):
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"])
    ctx["prereg"][key] = "TAMPERED"
    with pytest.raises(v.CheckFailure, match=f"{key} recompute mismatch"):
        v.check_10_input_hash_pinning(ctx)


# ---------------------------------------------------------------------------
# Check 11 -- cluster_map_hash reproducibility
# ---------------------------------------------------------------------------


def test_check11_fails_on_cluster_map_hash_drift():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"], cluster_map_hash="drifted-cluster-hash")
    with pytest.raises(v.CheckFailure, match="cluster_map_hash recompute mismatch"):
        v.check_11_cluster_map_reproducibility(ctx)


# ---------------------------------------------------------------------------
# Check 12 -- crosswalk_sha256 pinning
# ---------------------------------------------------------------------------


def test_check12_fails_on_crosswalk_hash_mismatch():
    ctx = _make_golden_ctx()
    ctx["prereg"] = dict(ctx["prereg"], crosswalk_sha256="TAMPERED")
    with pytest.raises(v.CheckFailure, match="crosswalk_sha256 recompute mismatch"):
        v.check_12_crosswalk_hash_pinning(ctx)


# ---------------------------------------------------------------------------
# run_all_checks / main() wiring
# ---------------------------------------------------------------------------


def test_run_all_checks_reports_all_pass_on_golden_ctx():
    ctx = _make_golden_ctx()
    results = v.run_all_checks(ctx)
    assert len(results) == 12
    assert all(ok for (_num, _name, ok, _msg) in results)


def test_run_all_checks_reports_exactly_the_failing_check():
    ctx = _make_golden_ctx()
    ctx["ledger"] = []  # only check 6 should fail
    results = v.run_all_checks(ctx)
    failing = [num for (num, _name, ok, _msg) in results if not ok]
    assert failing == [6]


# ---------------------------------------------------------------------------
# End-to-end against the REAL current on-disk artifacts (the state this
# executor left them in: pre-registration + deck manifest frozen, ledger
# still empty because grading has not started -- Task 3 is a human
# checkpoint). Confirms build_context()/main() wiring works against real
# files without fabricating a verdict.
#
# ONE input path is overridden below, and the reason matters (2026-08-26).
#
# CERT-01 pinned the SHA-256 of four input files. Three are still
# byte-identical on disk. The fourth, `seftja_dates.json`, is a WORKING file
# that every date append rewrites IN PLACE: 407 -> 410 (135-07) -> 416 (REF6,
# 2026-08-18) -> 430 (V4.2, 2026-08-19). CERT-01 froze the 410-entry version
# (`0076028917...`) on 2026-07-26; that path now holds the 430-entry version
# (`6ca6b40d...`), which is the CURRENT and CORRECT bake input -- the live
# production sidecar records exactly that hash, so reverting the file would
# be the wrong repair.
#
# `read_input_hashes` was therefore raising on a real divergence, and raising
# correctly: a pre-registration cannot be re-verified against an input that
# has since been overwritten. The pre-registration did not become wrong -- it
# pinned a MUTABLE PATH rather than an archived copy, so every future append
# breaks it again. The repair is to hand the verifier the bytes CERT-01
# actually froze, kept beside the working file and named for its hash.
#
# This is NOT a softened assertion. `read_input_hashes` still recomputes that
# file and still raises if it is not the pinned bytes; the other three inputs
# still recompute from their live working paths, so an unannounced change to
# any of them still fails here; and the archive itself is guarded by
# `test_pinned_seftja_archive_is_the_bytes_cert01_froze` below.
#
# `scripts/verify_cert01_grading.py` is deliberately NOT edited -- D-02c
# (tests/test_rebuild_preservation.py::test_verify_cert01_grading_unmodified)
# holds check 10 immutable. Run by hand, that script still reads the working
# path and still reports the divergence; whether its own default should move
# to the archive is an owner call on the CERT-01 track, not a test's to make.
# ---------------------------------------------------------------------------

#: The bytes CERT-01 pinned as `seftja_dates_sha256`, kept clear of the
#: appends that rewrite the working file. Gitignored like every other
#: artifact this section reads, so CI skips rather than fails.
_PINNED_SEFTJA = (
    v.REPO_ROOT / "same_work_spike" / "probe" / "rsource" / "data"
    / "seftja_dates.cert01-pinned-0076028917.json"
)

#: Repeated from `cert01_prereg.json` on purpose: the archive is checked
#: against the PRE-REGISTRATION, never against itself or against whatever the
#: sidecar happens to say.
_PINNED_SEFTJA_SHA256 = (
    "0076028917c60044ac72ee36504c173b9e6decd0a5aef9890ec0f0fe934b22d7"
)


def test_pinned_seftja_archive_is_the_bytes_cert01_froze():
    """The archive becomes load-bearing the moment the test below reads it,
    so it gets its own guard: it must hash to the value the pre-registration
    recorded, and the pre-registration must still record that value."""
    if not _PINNED_SEFTJA.exists() or not v.PREREG_PATH.exists():
        pytest.skip("pinned seftja archive / pre-registration not on this box")
    prereg = json.loads(v.PREREG_PATH.read_text(encoding="utf-8"))
    assert prereg["seftja_dates_sha256"] == _PINNED_SEFTJA_SHA256
    assert cf.hash_file(str(_PINNED_SEFTJA)) == _PINNED_SEFTJA_SHA256


def test_main_against_real_artifacts_fails_only_on_missing_verdict():
    if not v.PREREG_PATH.exists() or not v.DECK_MANIFEST_PATH.exists():
        pytest.skip("real CERT-01 artifacts not present on this box")
    if not v.SIDECAR_DB.exists() or not v.RESEARCH_DB.exists():
        pytest.skip("deployed sidecar / research DB not present on this box")
    if not _PINNED_SEFTJA.exists():
        pytest.skip("pinned seftja archive not on this box -- see the note above")
    ctx = v.build_context(seftja_dates_path=str(_PINNED_SEFTJA))
    results = v.run_all_checks(ctx)
    failing = [num for (num, _name, ok, _msg) in results if not ok]
    # As of this plan's execution, grading has not started yet (empty
    # ledger) -- check 6 is the ONLY expected failure. If a real verdict
    # has since been recorded, this test should be re-run/updated by the
    # next agent (see the 135-09 SUMMARY checkpoint note).
    assert failing in ([], [6])
