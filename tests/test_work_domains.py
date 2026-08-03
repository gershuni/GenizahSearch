"""Tests for the work -> FJMS-domain curation harness (plan 136-09).

Covers the five structural failure classes ``--validate`` must reject, the
``Unassigned`` sentinel's status as a REAL value rather than missing data, the
``needs-ruling`` posture (a held row may never carry a guessed leaf, and the
release gate fails while any held row is unruled), and the Task-3 author alias
map's labelling and order-independence.

Everything here is PURE except the handful of tests explicitly marked as
reading the gitignored local artifacts / the FJMS sidecar -- the vocabulary is
injected as data so the suite never depends on a 388 MB asset.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import curate_work_domains as cwd  # noqa: E402


# ---------------------------------------------------------------------------
# A tiny, injected vocabulary -- the shape `get_domain_hierarchy()` returns.
# ---------------------------------------------------------------------------

TINY_TREE = {
    "Bible: Texts and Translations": {
        "parent_domain_heb": "",
        "count": 3,
        "children": [
            {"domain": "Bible: Texts", "domain_heb": "", "count": 2},
            {
                "domain": "Massorah",
                "domain_heb": "",
                "count": 1,
                "children": [
                    {"domain": "Lists and Counts", "domain_heb": "", "count": 1},
                ],
            },
        ],
    },
    "Documentary": {
        "parent_domain_heb": "",
        "count": 2,
        "children": [{"domain": "Letters", "domain_heb": "", "count": 2}],
    },
    # A childless top-level node -- itself a usable leaf.
    "Historiography and geographical descriptions": {
        "parent_domain_heb": "",
        "count": 1,
        "children": [],
    },
}


@pytest.fixture
def vocab():
    return cwd.vocabulary_from_hierarchy(TINY_TREE, source="test-tree")


CANONICAL_IDS = {"w000001", "w000002", "w000003", "w000004"}


def _row(**over):
    row = {
        "canonical_work_id": "w000001",
        "domain_parent": "Bible: Texts and Translations",
        "domain_leaf": "Bible: Texts",
        "confidence": "high",
        "provenance": "rule:tanakh_book",
    }
    row.update(over)
    return {k: v for k, v in row.items() if v is not cwd}  # identity sentinel unused


def _doc(rows):
    return {
        "artifact": cwd.ARTIFACT_NAME,
        "artifact_version": cwd.ARTIFACT_VERSION,
        "needs_ruling_posture": cwd.NEEDS_RULING_POSTURE_HELD,
        "content_hash": cwd.compute_content_hash(rows),
        "assignments": rows,
    }


def _errors(rows, vocab, **kw):
    return cwd.validate_artifact(_doc(rows), vocab, CANONICAL_IDS, **kw)


# ---------------------------------------------------------------------------
# The vocabulary is READ, not snapshotted.
# ---------------------------------------------------------------------------


def test_vocabulary_is_live_not_snapshotted(vocab):
    """Swapping the tree changes what validates -- proving the vocabulary is
    data the validator reads, never a copy baked into the module."""
    ok = _errors([_row()], vocab)
    assert ok == []

    smaller = cwd.vocabulary_from_hierarchy(
        {"Documentary": {"count": 1, "children": [{"domain": "Letters", "count": 1}]}},
        source="test-tree-smaller",
    )
    now_bad = _errors([_row()], smaller)
    assert now_bad, "a leaf absent from the injected tree must stop validating"
    assert "not in the closed FJMS vocabulary" in now_bad[0]


def test_module_carries_no_copy_of_the_vocabulary():
    """No module-level container in the script is the vocabulary.

    The curation rule table names ~50 nodes as DECISIONS; the live tree carries
    ~190. A module-level object that reproduced the tree would be a snapshot.
    """
    source = open(cwd.__file__, "r", encoding="utf-8").read()
    assert "get_domain_hierarchy" in source, "the vocabulary must be read at runtime"
    for name in ("DOMAIN_VOCABULARY", "DOMAINS", "FJMS_TREE", "LEAVES", "PARENTS"):
        assert not hasattr(cwd, name), f"{name} looks like a snapshotted vocabulary"


def test_empty_hierarchy_fails_closed():
    with pytest.raises(cwd.CurationError):
        cwd.vocabulary_from_hierarchy({}, source="empty")


def test_childless_top_level_node_is_a_usable_leaf(vocab):
    node = "Historiography and geographical descriptions"
    assert vocab.has_pair(node, node)
    assert _errors([_row(domain_parent=node, domain_leaf=node)], vocab) == []


# ---------------------------------------------------------------------------
# The five failure classes -- one test each.
# ---------------------------------------------------------------------------


def test_failure_1_domain_not_in_the_tree(vocab):
    errors = _errors([_row(domain_parent="Bible: Texts and Translations",
                           domain_leaf="Palaeography")], vocab)
    assert any("not in the closed FJMS vocabulary" in e for e in errors)


def test_failure_2_leaf_whose_parent_disagrees(vocab):
    errors = _errors([_row(domain_parent="Documentary", domain_leaf="Bible: Texts")], vocab)
    assert any("is not a child of" in e for e in errors)


def test_failure_3_non_canonical_work_id_rejected(vocab):
    """A raw source work id assigns a duplicate twice -- rejected."""
    errors = _errors([_row(canonical_work_id="w000999")], vocab)
    assert any("is not a CANONICAL work id" in e for e in errors)


def test_failure_4_missing_confidence_or_provenance(vocab):
    row = _row()
    del row["confidence"]
    errors = _errors([row], vocab)
    assert any("missing required field" in e and "confidence" in e for e in errors)

    row = _row(provenance="   ")
    errors = _errors([row], vocab)
    assert any("provenance is missing or blank" in e for e in errors)

    errors = _errors([_row(confidence="probably")], vocab)
    assert any("confidence 'probably' outside" in e for e in errors)


def test_failure_5_duplicate_canonical_key(vocab):
    errors = _errors([_row(), _row()], vocab)
    assert any("duplicate canonical_work_id" in e for e in errors)


# ---------------------------------------------------------------------------
# Unassigned is a REAL value with a parent, not missing data.
# ---------------------------------------------------------------------------


def test_unassigned_validates_as_a_real_value(vocab):
    row = _row(domain_parent=cwd.UNASSIGNED, domain_leaf=cwd.UNASSIGNED,
               confidence="medium", provenance="manual:no rule placed this work")
    assert _errors([row], vocab) == []


def test_unassigned_is_not_treated_as_missing_data(vocab):
    """The Unassigned row survives validation while a null leaf on a non-held
    row does not -- so Unassigned is data, and null is absence."""
    unassigned = _row(canonical_work_id="w000001", domain_parent=cwd.UNASSIGNED,
                      domain_leaf=cwd.UNASSIGNED, confidence="medium")
    nulled = _row(canonical_work_id="w000002", domain_parent=None, domain_leaf=None,
                  confidence="high")
    errors = _errors([unassigned, nulled], vocab)
    assert errors, "a null leaf on a non-held row must fail"
    assert all("assignments[0]" not in e for e in errors), (
        "the Unassigned row must not be reported as an error"
    )
    assert any("assignments[1]" in e for e in errors)


def test_unassigned_must_carry_its_own_parent(vocab):
    row = _row(domain_parent="Documentary", domain_leaf=cwd.UNASSIGNED, confidence="medium")
    errors = _errors([row], vocab)
    assert any("Unassigned must carry domain_parent" in e for e in errors)


# ---------------------------------------------------------------------------
# The needs-ruling posture.
# ---------------------------------------------------------------------------


def _held(**over):
    row = {
        "canonical_work_id": "w000003",
        "domain_parent": None,
        "domain_leaf": None,
        "confidence": "needs-ruling",
        "provenance": "manual:held for owner ruling -- (c) between two adjacent leaves",
        "candidate_leaves": [
            {"domain_parent": "Documentary", "domain_leaf": "Letters", "case": "surface form"},
            {"domain_parent": "Historiography and geographical descriptions",
             "domain_leaf": "Historiography and geographical descriptions",
             "case": "content"},
        ],
    }
    row.update(over)
    return row


def test_held_needs_ruling_row_validates_structurally(vocab):
    assert _errors([_held()], vocab) == []


def test_needs_ruling_row_may_never_carry_a_guessed_leaf(vocab):
    """A concrete leaf on a needs-ruling row without an owner ruling is exactly
    what the posture forbids (threat T-136-09-03 / T-136-09-06)."""
    row = _held(domain_parent="Documentary", domain_leaf="Letters")
    errors = _errors([row], vocab)
    assert any("without an owner_ruling citation" in e for e in errors)


def test_needs_ruling_row_with_a_recorded_owner_ruling_is_accepted(vocab):
    row = _held(domain_parent="Documentary", domain_leaf="Letters",
                owner_ruling="136-GATE1-DECISIONS.md group D, ruled YYYY-MM-DD")
    assert _errors([row], vocab) == []


def test_held_row_without_candidate_leaves_is_rejected(vocab):
    row = _held()
    del row["candidate_leaves"]
    errors = _errors([row], vocab)
    assert any("carries no candidate_leaves" in e for e in errors)


def test_candidate_leaves_are_themselves_vocabulary_checked(vocab):
    row = _held(candidate_leaves=[
        {"domain_parent": "Documentary", "domain_leaf": "Palaeography", "case": "invented"},
    ])
    errors = _errors([row], vocab)
    assert any("is not in the FJMS tree" in e for e in errors)


def test_release_gate_fails_while_a_held_row_is_unruled(vocab):
    assert _errors([_held()], vocab) == []
    errors = _errors([_held()], vocab, release=True)
    assert any("RELEASE GATE" in e for e in errors)


def test_missing_posture_statement_is_rejected(vocab):
    rows = [_row()]
    doc = _doc(rows)
    del doc["needs_ruling_posture"]
    errors = cwd.validate_artifact(doc, vocab, CANONICAL_IDS)
    assert any("needs_ruling_posture is missing" in e for e in errors)


# ---------------------------------------------------------------------------
# The owner's rulings -- a TRACKED input the emitter reads, so a re-emission
# reproduces the ruled rows instead of discarding them.
# ---------------------------------------------------------------------------


_HELD_TABLE = {
    "w000003": {
        "case": "(c) between two adjacent leaves",
        "question": "letters, or history?",
        "candidate_leaves": [
            {"domain_parent": "Documentary", "domain_leaf": "Letters",
             "case": "surface form"},
            {"domain_parent": "Historiography and geographical descriptions",
             "domain_leaf": "Historiography and geographical descriptions",
             "case": "content"},
        ],
        "note": None,
    }
}

_HELD_WORKLIST = [
    {"canonical_work_id": "w000003", "neutral_title": "t", "author": None,
     "source_corpus": "sefaria", "shipped_claims": 1},
]


def _curate_held(vocab, rulings):
    return cwd.curate(
        _HELD_WORKLIST, vocab, rules=[], manual={},
        needs_ruling=_HELD_TABLE, rulings=rulings,
    )


def test_a_ruled_row_emits_its_leaf_its_citation_and_passes_the_release_gate(vocab):
    rows = _curate_held(vocab, {
        "w000003": {"domain_parent": "Documentary", "domain_leaf": "Letters",
                    "owner_ruling": "136-GATE1-DECISIONS.md § Ruling Q",
                    "why": "these are actual letters"},
    })
    row = rows[0]
    assert row["domain_parent"] == "Documentary"
    assert row["domain_leaf"] == "Letters"
    assert row["owner_ruling"] == "136-GATE1-DECISIONS.md § Ruling Q"
    # The citation, not the confidence value, is what unlocks release: the row
    # stays `needs-ruling` so its provenance remains distinguishable.
    assert row["confidence"] == "needs-ruling"
    assert row["provenance"].startswith("owner-ruling:")
    assert "these are actual letters" in row["note"]
    # candidate_leaves is kept: the artifact still records what was chosen between
    assert len(row["candidate_leaves"]) == 2
    assert _errors(rows, vocab, release=True) == []


def test_ruling_on_a_work_that_is_not_held_is_a_build_error(vocab):
    with pytest.raises(cwd.CurationError) as exc:
        _curate_held(vocab, {
            "w000004": {"domain_parent": "Documentary", "domain_leaf": "Letters",
                        "owner_ruling": "somewhere", "why": "x"},
        })
    assert "nothing here to rule on" in str(exc.value)


def test_ruling_naming_a_leaf_outside_the_live_tree_is_a_build_error(vocab):
    with pytest.raises(cwd.CurationError) as exc:
        _curate_held(vocab, {
            "w000003": {"domain_parent": "Documentary", "domain_leaf": "Palaeography",
                        "owner_ruling": "somewhere", "why": "x"},
        })
    assert "is not under" in str(exc.value)


def test_ruling_naming_a_leaf_never_offered_to_the_owner_is_a_build_error(vocab):
    """A ruling answers the question that was put to the owner; it may not
    introduce a fourth option after the fact."""
    with pytest.raises(cwd.CurationError) as exc:
        _curate_held(vocab, {
            "w000003": {"domain_parent": "Bible: Texts and Translations",
                        "domain_leaf": "Bible: Texts",
                        "owner_ruling": "somewhere", "why": "x"},
        })
    assert "was not among the candidate leaves put to the owner" in str(exc.value)


def test_rulings_pair_with_the_needs_ruling_table_they_rule_on(vocab):
    """Injecting a needs-ruling table without a rulings table means NO rulings,
    never this module's own rulings against a one-row test table."""
    rows = cwd.curate(_HELD_WORKLIST, vocab, rules=[], manual={},
                      needs_ruling=_HELD_TABLE)
    assert rows[0]["domain_leaf"] is None
    assert "owner_ruling" not in rows[0]


def test_every_module_ruling_settles_a_module_held_row():
    """Pure structural check over this module's OWN tables -- no sidecar."""
    assert cwd.OWNER_RULINGS, "the rulings table must not be empty"
    for wid, spec in cwd.OWNER_RULINGS.items():
        held = cwd.NEEDS_RULING.get(wid)
        assert held is not None, f"{wid} is ruled but was never held"
        offered = {(c["domain_parent"], c["domain_leaf"])
                   for c in held["candidate_leaves"]}
        assert (spec["domain_parent"], spec["domain_leaf"]) in offered, wid
        assert spec["owner_ruling"] in (cwd.RULING_P, cwd.RULING_Q), wid
        assert spec["why"].strip(), wid


def test_the_posture_statement_records_the_applied_rulings(vocab):
    rows = _curate_held(vocab, {
        "w000003": {"domain_parent": "Documentary", "domain_leaf": "Letters",
                    "owner_ruling": "136-GATE1-DECISIONS.md § Ruling Q",
                    "why": "y"},
    })
    doc = cwd.build_artifact(rows, vocab, "asset")
    assert "DECLINED" in doc["needs_ruling_posture"]
    assert "Ruling Q" in doc["needs_ruling_posture"]
    assert doc["counts"]["needs_ruling_ruled"] == 1
    assert doc["counts"]["needs_ruling_held"] == 0


# ---------------------------------------------------------------------------
# Hash pinning.
# ---------------------------------------------------------------------------


def test_content_hash_covers_the_assignments_only(vocab):
    rows = [_row()]
    doc = _doc(rows)
    doc["generated_utc"] = "2026-01-01T00:00:00Z"
    assert cwd.validate_artifact(doc, vocab, CANONICAL_IDS) == []


def test_edited_artifact_fails_the_hash(vocab):
    rows = [_row()]
    doc = _doc(rows)
    doc["assignments"][0]["domain_leaf"] = "Letters"
    doc["assignments"][0]["domain_parent"] = "Documentary"
    errors = cwd.validate_artifact(doc, vocab, CANONICAL_IDS)
    assert any("content_hash mismatch" in e for e in errors)


def test_missing_content_hash_is_rejected(vocab):
    doc = _doc([_row()])
    del doc["content_hash"]
    errors = cwd.validate_artifact(doc, vocab, CANONICAL_IDS)
    assert any("content_hash is missing" in e for e in errors)


# ---------------------------------------------------------------------------
# The curation rules are decisions INSIDE the closed vocabulary.
# ---------------------------------------------------------------------------


def test_a_rule_naming_a_node_outside_the_tree_is_a_build_error(vocab):
    bad = [cwd._rule("invented", "d", "Documentary", "Palaeography", "high",
                     lambda t, a: True)]
    with pytest.raises(cwd.CurationError):
        cwd.assert_rules_within_vocabulary(vocab, bad)


def test_a_rule_with_a_disagreeing_parent_is_a_build_error(vocab):
    bad = [cwd._rule("misparented", "d", "Documentary", "Bible: Texts", "high",
                     lambda t, a: True)]
    with pytest.raises(cwd.CurationError):
        cwd.assert_rules_within_vocabulary(vocab, bad)


def test_rules_are_ordered_first_match_wins():
    rules = [
        cwd._rule("specific", "d", "Documentary", "Letters", "high",
                  cwd._matches("^a")),
        cwd._rule("general", "d", "Documentary", "Letters", "medium",
                  lambda t, a: True),
    ]
    assert cwd.apply_rules("abc", "", rules)["name"] == "specific"
    assert cwd.apply_rules("zzz", "", rules)["name"] == "general"


def test_curate_precedence_ruling_then_manual_then_rule_then_unassigned(vocab):
    rules = [cwd._rule("letters", "d", "Documentary", "Letters", "high",
                       cwd._starts("letter"))]
    manual = {"w000002": {"domain_parent": "Documentary", "domain_leaf": "Letters",
                          "confidence": "medium", "provenance": "manual:curated",
                          "note": None}}
    held = {"w000003": {"case": "(c) between two adjacent leaves",
                        "question": "which leaf?",
                        "candidate_leaves": [
                            {"domain_parent": "Documentary", "domain_leaf": "Letters",
                             "case": "surface form"}],
                        "note": None}}
    worklist = [
        {"canonical_work_id": "w000001", "neutral_title": "letter to X", "author": None,
         "source_corpus": "sefaria", "shipped_claims": 1},
        {"canonical_work_id": "w000002", "neutral_title": "letter to Y", "author": None,
         "source_corpus": "sefaria", "shipped_claims": 1},
        {"canonical_work_id": "w000003", "neutral_title": "letter to Z", "author": None,
         "source_corpus": "sefaria", "shipped_claims": 1},
        {"canonical_work_id": "w000004", "neutral_title": "no rule matches this",
         "author": None, "source_corpus": "sefaria", "shipped_claims": 1},
    ]
    rows = {r["canonical_work_id"]: r for r in
            cwd.curate(worklist, vocab, rules=rules, manual=manual, needs_ruling=held)}
    assert rows["w000001"]["provenance"] == "rule:letters"
    assert rows["w000002"]["provenance"] == "manual:curated"
    # The held row wins over the rule that would otherwise have placed it, and
    # carries NO leaf.
    assert rows["w000003"]["confidence"] == "needs-ruling"
    assert rows["w000003"]["domain_leaf"] is None
    # A work no rule places lands in the VISIBLE Unassigned bucket.
    assert rows["w000004"]["domain_leaf"] == cwd.UNASSIGNED
    for row in rows.values():
        assert row["confidence"] in cwd.CONFIDENCE_TOKENS
        assert row["provenance"]
    assert cwd.validate_artifact(_doc(list(rows.values())), vocab, CANONICAL_IDS) == []


# ---------------------------------------------------------------------------
# Task 3 -- the author alias map.
# ---------------------------------------------------------------------------

PERSONS = [
    {"person_id": 785, "eng_desc": "Hai b. Sherira Gaon", "heb_desc": "האי בן שרירא גאון"},
    {"person_id": 165, "eng_desc": "Sherira Gaon", "heb_desc": "שרירא גאון"},
    {"person_id": 12, "eng_desc": "Solomon b. Isaac", "heb_desc": "שלמה בן יצחק"},
    {"person_id": 900, "eng_desc": "Solomon b. Isaac (dup)", "heb_desc": "שלמה בן יצחק"},
]


def test_alias_exact_match_is_labelled_exact():
    r = cwd.resolve_author_alias("שרירא גאון", PERSONS)
    assert r["match"] == "exact"
    assert r["person_id"] == 165


def test_alias_containment_match_is_not_recorded_as_exact():
    """The weaker evidence must stay visible as weaker evidence."""
    r = cwd.resolve_author_alias('שלמה בן יצחק (רש"י)', PERSONS)
    assert r["match"] == "containment"
    assert r["match"] != "exact"
    assert r["person_id"] is not None


def test_alias_unmatched_author_is_retained_not_dropped():
    r = cwd.resolve_author_alias("האיי גאון", PERSONS)
    assert r["match"] == "unmatched"
    assert r["person_id"] is None

    worklist = [
        {"canonical_work_id": "w000001", "neutral_title": "t",
         "author": "האיי גאון", "source_corpus": "sefaria", "shipped_claims": 1},
    ]
    doc = cwd.build_alias_artifact(worklist, PERSONS, "asset")
    assert [a["author"] for a in doc["aliases"]] == ["האיי גאון"]
    assert doc["counts"]["by_match"]["unmatched"] == 1


def test_alias_containment_prefers_the_most_specific_catalogue_name():
    """The catalogue carries both a bare given name and the full name of the
    same person. A smallest-id-only tie-break resolved the corpus's second most
    frequent author onto the bare given name; longest-first fixes it."""
    persons = [
        {"person_id": 147, "eng_desc": "Solomon", "heb_desc": "שלמה"},
        {"person_id": 152, "eng_desc": "Solomon b. Isaac", "heb_desc": "שלמה בן יצחק"},
    ]
    r = cwd.resolve_author_alias('שלמה בן יצחק (רש״י)', persons)
    assert r["match"] == "containment"
    assert r["person_id"] == 152, "the bare given name must not beat the full name"
    assert sorted(r["candidates"]) == [147, 152], (
        "both candidates stay visible so the weaker evidence is auditable"
    )
    assert r == cwd.resolve_author_alias('שלמה בן יצחק (רש״י)', list(reversed(persons)))


def test_alias_resolution_is_order_independent():
    forward = cwd.resolve_author_alias("שלמה בן יצחק", PERSONS)
    backward = cwd.resolve_author_alias("שלמה בן יצחק", list(reversed(PERSONS)))
    assert forward == backward
    # The tie between two identical heb_desc rows resolves deterministically to
    # the smallest person_id -- the same "deterministic, order-independent
    # representative" rule shared/discovery_novelty.py uses for alias groups.
    assert forward["person_id"] == 12


def test_alias_map_is_keyed_deterministically():
    worklist = [
        {"canonical_work_id": "w000002", "neutral_title": "b", "author": "שרירא גאון",
         "source_corpus": "sefaria", "shipped_claims": 1},
        {"canonical_work_id": "w000001", "neutral_title": "a", "author": "שרירא גאון",
         "source_corpus": "sefaria", "shipped_claims": 1},
    ]
    a = cwd.build_alias_artifact(worklist, PERSONS, "asset")
    b = cwd.build_alias_artifact(list(reversed(worklist)), PERSONS, "asset")
    assert a["content_hash"] == b["content_hash"]
    assert a["aliases"][0]["works"] == ["w000001", "w000002"]


def test_alias_artifact_validates_and_rejects_a_forced_unmatched_row():
    worklist = [
        {"canonical_work_id": "w000001", "neutral_title": "t", "author": "האיי גאון",
         "source_corpus": "sefaria", "shipped_claims": 1},
    ]
    doc = cwd.build_alias_artifact(worklist, PERSONS, "asset")
    assert cwd.validate_alias_artifact(doc) == []

    doc["aliases"][0]["fjms_person_id"] = 785  # forcing a near-neighbour
    doc["content_hash"] = cwd.compute_content_hash(doc["aliases"])
    errors = cwd.validate_alias_artifact(doc)
    assert any("unmatched author must carry a null fjms_person_id" in e for e in errors)


def test_author_gaps_are_counted_not_invented():
    worklist = [
        {"canonical_work_id": "w000001", "neutral_title": "t", "author": None,
         "source_corpus": "sefaria", "shipped_claims": 1},
        {"canonical_work_id": "w000002", "neutral_title": "t2", "author": "שרירא גאון",
         "source_corpus": "sefaria", "shipped_claims": 1},
    ]
    doc = cwd.build_alias_artifact(worklist, PERSONS, "asset")
    assert doc["counts"]["gaps_left_unfilled"] == 1
    assert doc["counts"]["distinct_authors"] == 1
    assert "NEVER inferred from a title pattern" in doc["author_gap_rule"]


# ---------------------------------------------------------------------------
# The CLI surface.
# ---------------------------------------------------------------------------


def test_help_documents_all_modes(capsys):
    parser = cwd.build_parser()
    text = parser.format_help()
    for mode in ("--emit-worklist", "--validate", "--report", "--emit-artifact",
                 "--emit-aliases", "--validate-aliases"):
        assert mode in text, f"{mode} must be documented in --help"


def test_docstring_names_every_artifact_field():
    doc = cwd.__doc__
    for field in ("canonical_work_id", "domain_parent", "domain_leaf", "confidence",
                  "provenance", "note", "candidate_leaves", "owner_ruling",
                  "content_hash"):
        assert field in doc, f"{field} must be documented in the module docstring"


# ---------------------------------------------------------------------------
# The real, locally-emitted artifacts (gitignored by design -- discovery_data/
# is excluded from the repo, exactly like every sibling curated artifact).
# ---------------------------------------------------------------------------

_DOMAINS = cwd.DEFAULT_DOMAINS_ARTIFACT
_ALIASES = cwd.DEFAULT_ALIASES_ARTIFACT
_have_domains = pytest.mark.skipif(
    not os.path.isfile(_DOMAINS),
    reason="discovery_data/work_domains-v1.json is a gitignored local artifact",
)
_have_aliases = pytest.mark.skipif(
    not os.path.isfile(_ALIASES),
    reason="discovery_data/work_author_aliases-v1.json is a gitignored local artifact",
)


def _load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


@_have_domains
def test_real_artifact_every_row_carries_confidence_and_provenance():
    doc = _load(_DOMAINS)
    assert doc["assignments"], "the artifact must not be empty"
    for row in doc["assignments"]:
        assert row.get("confidence") in cwd.CONFIDENCE_TOKENS, row
        assert isinstance(row.get("provenance"), str) and row["provenance"].strip(), row


@_have_domains
def test_real_artifact_no_needs_ruling_row_carries_a_guessed_leaf():
    doc = _load(_DOMAINS)
    for row in doc["assignments"]:
        if row.get("confidence") != "needs-ruling":
            continue
        if row.get("owner_ruling"):
            continue
        assert row.get("domain_leaf") is None, (
            f"{row['canonical_work_id']} is needs-ruling and unruled but carries "
            f"a concrete leaf {row.get('domain_leaf')!r}"
        )
        assert row.get("candidate_leaves"), row["canonical_work_id"]


@_have_domains
def test_real_artifact_hash_matches_and_posture_is_stated():
    doc = _load(_DOMAINS)
    assert doc["content_hash"] == cwd.compute_content_hash(doc["assignments"])
    assert doc.get("needs_ruling_posture")
    assert "DECLINED" in doc["needs_ruling_posture"]


@_have_domains
def test_real_artifact_holds_no_unruled_row():
    """The release gate's own condition, asserted on the emitted artifact:
    rulings P and Q settled all 29 held rows, so nothing is still held."""
    doc = _load(_DOMAINS)
    held = [r for r in doc["assignments"]
            if r.get("confidence") == "needs-ruling" and not r.get("owner_ruling")]
    assert held == [], [r["canonical_work_id"] for r in held]
    ruled = [r for r in doc["assignments"] if r.get("owner_ruling")]
    assert ruled, "the ruled rows must be present in the emitted artifact"
    for row in ruled:
        assert row["domain_leaf"] and row["domain_parent"], row["canonical_work_id"]
        assert "136-GATE1-DECISIONS.md" in row["owner_ruling"], row["canonical_work_id"]


@_have_domains
def test_real_artifact_keys_are_unique():
    doc = _load(_DOMAINS)
    ids = [r["canonical_work_id"] for r in doc["assignments"]]
    assert len(ids) == len(set(ids))


@_have_aliases
def test_real_alias_artifact_validates():
    doc = _load(_ALIASES)
    assert cwd.validate_alias_artifact(doc) == []


# ---------------------------------------------------------------------------
# 136-13 regression: the author-key coverage check must be satisfiable by the
# artifact that actually exists, against the asset that actually shipped.
#
# The original check (136-12) compared RAW `works.author` strings against an
# index keyed on `normalized`, over ALL works rather than the shipped scope the
# artifact was built from. It passed its unit fixtures -- where raw == normalized
# and every work had a shipped claim -- and failed against the live certified
# asset with 28 uncovered authors (16 from the key-space mismatch, 12 from the
# scope error). These tests execute against the real objects.
# ---------------------------------------------------------------------------

_LIVE_ASSET = os.path.join(
    "discovery_data",
    "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db",
)
_have_live_asset = pytest.mark.skipif(
    not os.path.isfile(_LIVE_ASSET),
    reason="the live discovery asset is a gitignored local artifact",
)


@_have_aliases
def test_alias_index_is_reachable_by_raw_author_not_only_normalized():
    """Every curated row must be findable by the RAW author string, because the
    asset's `works.author` values are raw. 16 of the 96 rows normalize to a
    different string, so a normalized-only index silently loses them."""
    import scripts.build_discovery_sidecar as bds

    doc = _load(_ALIASES)
    index, _ = bds.load_work_author_aliases(_ALIASES, content_hash=doc["content_hash"])
    missing = [r for r in doc["aliases"] if r["author"] not in index]
    assert not missing, (
        f"{len(missing)} curated alias row(s) are unreachable by their raw author key "
        "(strings withheld -- masking)"
    )
    divergent = [r for r in doc["aliases"] if r["author"] != r["normalized"]]
    assert divergent, (
        "this regression is only meaningful while some row normalizes to a different "
        "string; if that stops being true the fixture no longer exercises the bug"
    )


@_have_aliases
@_have_live_asset
def test_author_key_coverage_accepts_the_live_certified_asset():
    """A gate that rejects the asset already in production is wrong by
    construction. This is the check that was never executed against a real
    build."""
    import sqlite3

    import scripts.build_discovery_sidecar as bds

    doc = _load(_ALIASES)
    index, _ = bds.load_work_author_aliases(_ALIASES, content_hash=doc["content_hash"])
    conn = sqlite3.connect(f"file:{_LIVE_ASSET}?mode=ro", uri=True)
    try:
        stats = bds.assert_author_key_coverage(conn, index)
    finally:
        conn.close()
    assert stats["works_author_strings"] > 0
    assert stats["works_author_strings_covered"] == stats["works_author_strings"]


@_have_aliases
@_have_live_asset
def test_author_key_coverage_still_fails_on_genuine_drift():
    """The scope narrowing must not defang the check: an author on a SHIPPED
    work that the artifact has never seen still has to fail."""
    import sqlite3

    import scripts.build_discovery_sidecar as bds

    doc = _load(_ALIASES)
    index, _ = bds.load_work_author_aliases(_ALIASES, content_hash=doc["content_hash"])
    src = sqlite3.connect(f"file:{_LIVE_ASSET}?mode=ro", uri=True)
    conn = sqlite3.connect(":memory:")
    try:
        src.backup(conn)
    finally:
        src.close()
    try:
        victim = conn.execute(
            """SELECT w.work_id FROM works w
                 JOIN discovery_claim dc ON dc.work_id = w.work_id
                 JOIN discovery_evidence e ON e.claim_id = dc.claim_id
                WHERE e.routing_status = 'shipped'
                  AND w.author IS NOT NULL AND w.author != '' LIMIT 1"""
        ).fetchone()
        assert victim, "fixture needs at least one shipped work carrying an author"
        conn.execute(
            "UPDATE works SET author = ? WHERE work_id = ?",
            ("zzz-uncurated-author-not-in-artifact", victim[0]),
        )
        with pytest.raises(bds.CuratedArtifactError):
            bds.assert_author_key_coverage(conn, index)
    finally:
        conn.close()
