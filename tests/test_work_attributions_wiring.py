"""The reference corpus's witness attribution must actually REACH the novelty gate.

`NoveltyCandidate.m_source_shelfmark_text` existed, was threaded through
`assemble_evidence_bundle`, appeared in `_SOURCE_ORDER`, and was an input to the
cache fingerprint -- while the single production assignment was the literal
`None`. The only non-None assignments in the tree were two test fixtures, so the
gate reported checking a source it had never read, and every test passed.

These tests are written against that specific failure: they drive the REAL
candidate builder, not a hand-built dataclass, because a hand-built dataclass is
exactly what hid the gap for as long as it was hidden.

MASKING (D-25): the corpus is M-source; the source field's own name is
restricted, so the neutral `src_attr_note` is used throughout.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.discovery_novelty_probe import (  # noqa: E402
    build_all_candidates,
    load_work_attributions,
    load_work_witnesses,
)
from scripts.discovery_novelty_funnel import (  # noqa: E402
    NO_SOURCE_TEXT_REASON,
    assemble_evidence_bundle,
    candidate_input_fingerprint,
    run_heuristic_pass,
)

ATTRIBUTION = "כ״י קיימברידג׳ T-S 12.123"


def _write_attributions(tmp_path, mapping):
    path = tmp_path / "attr.json"
    path.write_text(json.dumps(
        {"schema": "work-attributions-v1", "neutral_name": "src_attr_note",
         "count": len(mapping), "attributions": mapping},
        ensure_ascii=False), encoding="utf-8")
    return str(path)


def _write_crosswalk(tmp_path, mapping):
    path = tmp_path / "crosswalk.json"
    path.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
    return str(path)


def test_attributions_are_translated_to_the_MINTED_id_space(tmp_path):
    """The file is keyed on RAW ids and the candidates on minted ids. Keying the
    lookup wrong fails SILENTLY -- as an empty source, not an error -- which is
    the failure mode this whole fix exists to close."""
    attr = _write_attributions(tmp_path, {"M:Ytext900": ATTRIBUTION})
    cross = _write_crosswalk(tmp_path, {"M:Ytext900": "w000042"})
    loaded = load_work_attributions(attr, cross)
    assert loaded == {"w000042": ATTRIBUTION}, "must be keyed by MINTED id"
    assert "M:Ytext900" not in loaded, "the raw key must not survive translation"


def test_attributions_without_a_crosswalk_REFUSE_rather_than_silently_miss(tmp_path):
    attr = _write_attributions(tmp_path, {"M:Ytext900": ATTRIBUTION})
    with pytest.raises(SystemExit):
        load_work_attributions(attr, None)


def test_no_attributions_supplied_is_the_old_empty_behaviour():
    assert load_work_attributions(None, None) == {}


def test_a_work_absent_from_the_crosswalk_is_dropped_not_guessed(tmp_path):
    attr = _write_attributions(tmp_path, {"M:Ytext900": ATTRIBUTION,
                                          "M:Yorphan": "כ״י אחר"})
    cross = _write_crosswalk(tmp_path, {"M:Ytext900": "w000042"})
    assert load_work_attributions(attr, cross) == {"w000042": ATTRIBUTION}


# --------------------------------------------------------------------------
# The one that matters: the REAL builder, end to end.
# --------------------------------------------------------------------------

def _minimal_asset(tmp_path, name="asset.db"):
    """Smallest asset `build_all_candidates` will read: one shipped claim on one
    work. Schema mirrors the real one only as far as the loaders touch it."""
    path = tmp_path / name
    con = sqlite3.connect(str(path))
    con.executescript("""
        CREATE TABLE works (work_id TEXT, canonical_work_id TEXT,
            neutral_title TEXT, author TEXT, genre TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (claim_id TEXT, page_id TEXT, work_id TEXT,
            claim_type TEXT, display_evidence_id TEXT, source_corpus TEXT,
            sidecar_version TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, a_page_id TEXT, sys_id TEXT,
            confidence_band TEXT, adjudication_status TEXT, routing_status TEXT,
            matched_letters INT, span_start INT, span_end INT, aligned_len INT);
    """)
    con.execute("INSERT INTO works VALUES (?,?,?,?,?,?)",
                ("w000042", "w000042", "ספר הזיכרון", "מחבר", "halakhah", "msource"))
    con.execute("INSERT INTO discovery_claim VALUES (?,?,?,?,?,?,?)",
                ("c1", "990000000000000001_IE1_P000001_FL1", "w000042", "direct_witness", "e1",
                 "msource", "t"))
    con.execute("INSERT INTO discovery_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("e1", "c1", "witness", "track1_direct", "990000000000000001_IE1_P000001_FL1", "990000000000000001",
                 "tier_a", "unreviewed", "shipped", 500, 0, 500, 500))
    con.commit()
    con.close()
    return str(path)


SYS_ID = "990000000000000001"


def _empty_sidecars(tmp_path):
    """The other four checked sources, present but EMPTY -- so the only source
    with content is the attribution, and any assertion about it cannot be
    satisfied by something else leaking in.

    Column sets mirror the real loaders' queries exactly (`discovery_gate1_evidence`
    lines 1833/1856/1908/1939). They are spelled out rather than stubbed because a
    missing column raises inside the loader, and the first draft of this file
    turned that into a silent skip.
    """
    libs = tmp_path / "libraries.csv"
    libs.write_text(f"{SYS_ID},,T-S 12.123,CUL,,,,כותרת\n", encoding="utf-8-sig")
    paths = {}
    for name, ddl in (
        ("fjms.db",
         "CREATE TABLE catalog (AlmaId TEXT, TitleHeb TEXT, GenizahTitleOrgTitle TEXT,"
         " Title TEXT, GenizahTitleEngTitle TEXT);"
         "CREATE TABLE bibliography (AlmaId TEXT, RunningTitle TEXT, RunningTitleHeb TEXT,"
         " TitleAcronymHeb TEXT, TitleAcronym TEXT, ArticleName TEXT, ArticleAuthorHeb TEXT,"
         " ArticleAuthorEng TEXT, NoteForDisplay TEXT, MentionType TEXT,"
         " TranscriptionType TEXT, TitleYear TEXT);"),
        ("pgp.db",
         "CREATE TABLE documents (pgpid TEXT, description TEXT, transcription TEXT,"
         " has_transcription INT, document_type TEXT);"
         "CREATE TABLE document_fragments (sys_id TEXT, document_id TEXT);"),
        ("fgp.db",
         "CREATE TABLE fgp_transcriptions (sys_id TEXT, title_he TEXT, author_he TEXT,"
         " title_en TEXT);"),
    ):
        p = tmp_path / name
        if not p.exists():                 # one test builds twice in one tmp_path
            con = sqlite3.connect(str(p))
            con.executescript(ddl)
            con.commit()
            con.close()
        paths[name] = str(p)
    return str(libs), paths


_BUILD_SEQ = [0]


def _build(tmp_path, attributions, witnesses=None):
    """NO try/except, DELIBERATELY. The first draft of this file wrapped the call
    and skipped on any exception -- and all three tests below silently SKIPPED on
    a missing fixture column while the run reported "4 passed". A skip here is
    indistinguishable from a pass and would hide exactly the defect these tests
    exist to catch. If the fixture stops satisfying the real loaders, that is a
    failure to fix, not a condition to tolerate."""
    # Distinct asset file per call: one test builds TWICE (with and without the
    # attribution) to compare fingerprints, and a shared path would re-create
    # the same tables.
    _BUILD_SEQ[0] += 1
    asset = _minimal_asset(tmp_path, f"asset{_BUILD_SEQ[0]}.db")
    libs, side = _empty_sidecars(tmp_path)
    cands, _w, _l = build_all_candidates(
        asset_path=asset, libraries_csv=libs, fjms_db=side["fjms.db"],
        pgp_db=side["pgp.db"], fgp_db=side["fgp.db"],
        work_attributions=attributions,
        work_witnesses=witnesses,
    )
    return cands


def test_the_REAL_builder_populates_the_attribution(tmp_path):
    """Drives `build_all_candidates` itself. A dataclass constructed by hand in a
    test is what let the hardcoded `None` survive review -- so this asserts on
    the builder's own output."""
    cands = _build(tmp_path, {"w000042": ATTRIBUTION})
    assert cands, "fixture produced no candidates"
    assert cands[0].m_source_shelfmark_text == ATTRIBUTION


def test_the_attribution_reaches_the_EVIDENCE_BUNDLE_the_model_sees(tmp_path):
    """Populating the field is not enough -- it has to arrive in the bundle, which
    is what the gate actually reasons over."""
    cands = _build(tmp_path, {"w000042": ATTRIBUTION})
    bundle = assemble_evidence_bundle(cands[0])
    assert bundle["m_source_shelfmark"] == (ATTRIBUTION,)


def test_the_attribution_CHANGES_THE_CACHE_KEY(tmp_path):
    """It is a fingerprint input. If it did not move the key, wiring it later
    would silently reuse verdicts computed without it -- which is precisely the
    reason this must land BEFORE any spend."""
    with_attr = _build(tmp_path, {"w000042": ATTRIBUTION})[0]
    without = _build(tmp_path, {})[0]
    assert without.m_source_shelfmark_text is None
    sha = "0" * 64
    assert (candidate_input_fingerprint(with_attr, prompt_sha256=sha)
            != candidate_input_fingerprint(without, prompt_sha256=sha))


# --------------------------------------------------------------------------
# RECORDED WITNESSES (2026-08-08). The owner's correction: the corpus records
# witnesses on THREE channels, not one. Reading only the first understated
# "already known" by ~3x (1,375 flips vs 4,518 on the real artifact), and the
# three-channel table is already resolved to sys_id with confidence tiers.
# --------------------------------------------------------------------------

WITNESS_PAYLOAD = {
    "schema": "work-witnesses-v1", "works": 1, "pairs": 1,
    "witnesses": {"M:Ytext900": {SYS_ID: {"confidence": "high", "channels": [1, 3]}}},
    "attestation": {"M:Ytext900": "M-source records 1 manuscript witness(es) for this work (1 matched with high confidence)."},
}


def _write_witnesses(tmp_path, payload=None):
    path = tmp_path / "wit.json"
    path.write_text(json.dumps(payload or WITNESS_PAYLOAD, ensure_ascii=False),
                    encoding="utf-8")
    return str(path)


def _witnesses(tmp_path, confidence="high", sys_id=SYS_ID):
    payload = json.loads(json.dumps(WITNESS_PAYLOAD))
    payload["witnesses"]["M:Ytext900"] = {sys_id: {"confidence": confidence, "channels": [1]}}
    cross = _write_crosswalk(tmp_path, {"M:Ytext900": "w000042"})
    return load_work_witnesses(_write_witnesses(tmp_path, payload), cross)


def test_witnesses_are_translated_to_the_MINTED_id_space(tmp_path):
    loaded = _witnesses(tmp_path)
    assert set(loaded) == {"w000042"}
    assert SYS_ID in loaded["w000042"]["witnesses"]


def test_witnesses_without_a_crosswalk_REFUSE_rather_than_silently_miss(tmp_path):
    with pytest.raises(SystemExit):
        load_work_witnesses(_write_witnesses(tmp_path), None)


def test_a_recorded_high_confidence_witness_resolves_to_confirms_with_NO_model_call(tmp_path):
    """The whole point of the rewire: a manuscript the corpus already attests as
    a witness of this work is not a discovery, and deciding that costs nothing."""
    cand = _build(tmp_path, {}, _witnesses(tmp_path, "high"))[0]
    assert cand.known_witness_confidence == "high"
    result = run_heuristic_pass(cand)
    assert result.resolved is True
    assert result.novelty_status == "confirms"
    assert result.reason == "recorded_witness:high"


def _witnesses_without_attestation(tmp_path, confidence="high"):
    """Witness present, attestation text ABSENT -- so the evidence bundle is
    genuinely empty and the no-source-text rule WOULD fire.

    This shape is what makes the ordering testable. An earlier version supplied
    the attestation too, which kept `has_any_text` true, so rule 2 never fired
    and moving the witness rule after it changed nothing: the test passed under
    the exact defect it was written to catch."""
    payload = {"schema": "work-witnesses-v1", "works": 1, "pairs": 1,
               "witnesses": {"M:Ytext900": {SYS_ID: {"confidence": confidence,
                                                     "channels": [1]}}},
               "attestation": {}}
    cross = _write_crosswalk(tmp_path, {"M:Ytext900": "w000042"})
    return load_work_witnesses(_write_witnesses(tmp_path, payload), cross)


def test_the_witness_rule_beats_the_auto_fills_gap_rule(tmp_path):
    """Ordering is the defect this fixes, and 98 real claims carry the mislabel.

    Every source is empty here INCLUDING the attestation, so rule 2 ("no checked
    source says anything -> ship as fills_gap") is live. The witness rule must
    win it."""
    without = _build(tmp_path, {}, None)[0]
    assert run_heuristic_pass(without).reason == NO_SOURCE_TEXT_REASON, (
        "fixture must actually reach the no-source-text rule, or this proves nothing"
    )
    with_wit = _build(tmp_path, {}, _witnesses_without_attestation(tmp_path))[0]
    result = run_heuristic_pass(with_wit)
    assert not any(result.evidence_bundle.values()), (
        "the bundle must be empty, so rule 2 is genuinely the competitor"
    )
    assert result.novelty_status == "confirms"
    assert result.reason == "recorded_witness:high"


@pytest.mark.parametrize("conf", ["low", "ambiguous"])
def test_only_HIGH_confidence_auto_resolves(conf, tmp_path):
    """`low` = classmark without library agreement; `ambiguous` = a classmark
    resolving to many manuscripts. Real signal, not proof -- they travel to the
    model rather than short-circuiting it."""
    cand = _build(tmp_path, {}, _witnesses(tmp_path, conf))[0]
    assert cand.known_witness_confidence == conf
    assert run_heuristic_pass(cand).reason != "recorded_witness:high"


def test_a_witness_on_a_DIFFERENT_manuscript_does_not_resolve(tmp_path):
    """The lookup is per (manuscript, work). A per-work answer would mark every
    claim on a well-attested work as already known."""
    cand = _build(tmp_path, {}, _witnesses(tmp_path, "high", sys_id="999999999999999999"))[0]
    assert cand.known_witness_confidence is None
    assert run_heuristic_pass(cand).reason != "recorded_witness:high"


def test_the_witness_confidence_CHANGES_THE_CACHE_KEY(tmp_path):
    """Holds the attestation text IDENTICAL and varies only the confidence.

    Comparing with-witnesses against without also changes the attestation, so
    the key moves through `m_source_shelfmark_text` even if the confidence is
    dropped from the fingerprint entirely -- that version passed under the
    mutation that removed it."""
    sha = "0" * 64
    hit = _build(tmp_path, {}, _witnesses(tmp_path, "high"))[0]
    miss = _build(tmp_path, {},
                  _witnesses(tmp_path, "high", sys_id="999999999999999999"))[0]
    assert hit.m_source_shelfmark_text == miss.m_source_shelfmark_text, (
        "attestation must be identical, or this tests the wrong field"
    )
    assert hit.known_witness_confidence == "high"
    assert miss.known_witness_confidence is None
    assert (candidate_input_fingerprint(hit, prompt_sha256=sha)
            != candidate_input_fingerprint(miss, prompt_sha256=sha))
