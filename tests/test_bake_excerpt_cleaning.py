# -*- coding: utf-8 -*-
"""`scripts/bake_discovery_excerpts.py::clean_ja_markers` (2026-08-13).

The J-corpus per_doc files carry `+פסוק~ +כב~`-style structural markers
(label/value tokens: `+` prefix, `~` suffix). They leaked into the edition
pane raw (owner report). The cleaner strips them from DISPLAY pieces only --
the marker letters live inside the matcher's coordinate stream, so the bake
applies this AFTER slicing and BEFORE the word-highlight pass, and the
stream itself is never touched (these tests pin the function, not that
ordering; the ordering is pinned by the bake's own structure).

Grammar licence, measured over all 92 per_doc files: 1,743 distinct tokens,
and outside the grammar the corpus contains only three stray `+~` pairs --
`+` and `~` are never content characters.
"""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from bake_discovery_excerpts import (  # noqa: E402
    clean_ja_markers,
    excerpt_candidate_key,
    is_excerpt_only_fallback,
    load_v4_public_sources,
    sha256_file,
    validate_bake_input_hashes,
)


def _excerpt_candidate(**updates):
    row = {
        "evidence_id": "e1",
        "matched_letters": 100,
        "w_start": None,
        "w_end": None,
        "evidence_source": "propagated",
        "routing_status": "shipped",
        "adjudication_status": "unreviewed",
        "assertion_visibility": "public",
    }
    row.update(updates)
    return row


def test_excerpt_only_direct_fallback_beats_an_unlocated_propagated_row():
    propagated = _excerpt_candidate(matched_letters=500)
    direct = _excerpt_candidate(
        evidence_id="e2",
        matched_letters=100,
        w_start=10,
        w_end=40,
        evidence_source="track1_direct",
        routing_status="review_only",
    )
    assert is_excerpt_only_fallback(direct)
    assert excerpt_candidate_key(direct) < excerpt_candidate_key(propagated)


def test_eligible_located_evidence_still_beats_the_excerpt_only_fallback():
    shipped = _excerpt_candidate(w_start=10, w_end=40)
    fallback = _excerpt_candidate(
        evidence_id="e2",
        matched_letters=999,
        w_start=20,
        w_end=60,
        evidence_source="track1_direct",
        routing_status="review_only",
    )
    assert excerpt_candidate_key(shipped) < excerpt_candidate_key(fallback)


def test_private_or_unlocated_review_rows_are_never_excerpt_fallbacks():
    base = {
        "evidence_source": "track1_direct",
        "routing_status": "review_only",
        "w_start": 10,
        "w_end": 40,
    }
    assert not is_excerpt_only_fallback(
        _excerpt_candidate(**base, assertion_visibility="private")
    )
    assert not is_excerpt_only_fallback(
        _excerpt_candidate(**(base | {"w_end": 10}))
    )


def test_label_and_value_tokens_are_removed_inline_and_on_their_own_line():
    assert clean_ja_markers(
        "פקאל.\n+פרק~ +א~\n+פסוק~ +א~ אלחק פי קולה"
    ) == "פקאל.\nאלחק פי קולה"


def test_a_token_the_piece_slice_cut_at_the_start_is_removed():
    # The slice landed inside `+פסוק~`, leaving no opening `+`.
    assert clean_ja_markers("סוק~ +כב~ נץ אלכלאם") == "נץ אלכלאם"


def test_a_token_the_piece_slice_cut_at_the_end_is_removed():
    # The slice landed inside the token, leaving no closing `~`.
    assert clean_ja_markers("אכר אלקול +פסו") == "אכר אלקול"


def test_the_stray_empty_marker_is_removed_and_lines_collapse():
    # The one real out-of-grammar shape in the corpus (3 occurrences).
    assert clean_ja_markers("א[…]}\n         +~  \n\nעמא אתפק") == (
        "א[…]}\nעמא אתפק")


def test_text_without_markers_passes_through_unchanged():
    # Braces are the RENDERER's transform (ja_braces), never this one's.
    text = "נץ בלא סימנים {מלים עבריות} ושורה\nשניה"
    assert clean_ja_markers(text) == text


def test_none_and_empty_are_passed_through():
    assert clean_ja_markers(None) is None
    assert clean_ja_markers("") == ""


def test_v4_public_source_loader_replays_pinned_unit_selection(tmp_path):
    normalized_dir = tmp_path / "normalized"
    normalized_dir.mkdir()
    normalized = {
        "attribution": "Synthetic public attribution",
        "units": [
            {"ordinal": 1, "text": "יחידה אחת"},
            {"ordinal": 2, "text": "יחידה שתים"},
            {"ordinal": 3, "text": "יחידה שלש"},
        ],
    }
    normalized_path = normalized_dir / "source.json"
    normalized_path.write_text(json.dumps(normalized), encoding="utf-8")
    acquisition = {
        "entries": [
            {
                "key": "source",
                "status": "acquired",
                "normalized_file": "source.json",
                "normalized_sha256": sha256_file(normalized_path),
            }
        ]
    }
    acquisition_path = tmp_path / "acquisition.json"
    acquisition_path.write_text(json.dumps(acquisition), encoding="utf-8")
    reference = {
        "schema_version": "discovery-v4-reference-manifest-v1",
        "acquisition_manifest": str(acquisition_path),
        "acquisition_manifest_sha256": sha256_file(acquisition_path),
        "entries": [
            {
                "raw_reference_id": "REF4:source:2_3",
                "source_key": "source",
                "unit_offsets": [
                    {"source_ordinal": 2},
                    {"source_ordinal": 3},
                ],
            }
        ],
    }
    reference_path = tmp_path / "reference.json"
    reference_path.write_text(json.dumps(reference), encoding="utf-8")

    texts, attributions, source_hash = load_v4_public_sources(
        reference_path, normalized_dir
    )
    assert texts == {"REF4:source:2_3": "יחידה שתים\nיחידה שלש"}
    assert attributions == {"REF4:source:2_3": "Synthetic public attribution"}
    assert source_hash == sha256_file(acquisition_path)


def test_excerpt_inputs_are_bound_to_the_public_sidecar_hashes(tmp_path):
    crosswalk = tmp_path / "crosswalk.json"
    reference = tmp_path / "reference.pkl"
    crosswalk.write_text("{}", encoding="utf-8")
    reference.write_bytes(b"reference")
    db = tmp_path / "public.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO meta VALUES (?, ?)",
            [
                ("crosswalk_sha256", sha256_file(crosswalk)),
                ("reference_corpus_sha256", sha256_file(reference)),
            ],
        )

    meta = validate_bake_input_hashes(db, crosswalk, reference)
    assert meta["crosswalk_sha256"] == sha256_file(crosswalk)
    reference.write_bytes(b"different")
    try:
        validate_bake_input_hashes(db, crosswalk, reference)
    except ValueError as exc:
        assert "reference pickle" in str(exc)
    else:  # pragma: no cover - mutation control
        raise AssertionError("a changed reference pickle passed its sidecar pin")
