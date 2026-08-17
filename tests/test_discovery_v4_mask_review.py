"""The canonical-mask review packet (C9) -- the owner-facing over-masking gate.

The masker blanks what an edited work quotes from the canonical corpus. When it
blanks MORE than that, nothing fails: the reference simply stops matching, and
the identifications that should have surfaced never do. The packet is how a
human sees that before a bake, so its two load-bearing properties are tested
here: the arithmetic must be honest (a masked fraction is only a sum of range
widths if the ranges are disjoint), and it must name ONLY the namespace under
review -- the rest of the reference corpus carries identifiers a shareable file
must never enumerate.

All fixtures are synthetic: made-up ids, made-up Hebrew filler.
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import pytest

from scripts.discovery_v4_mask_review import (
    build_packet,
    check_ranges,
    excerpt_text,
    group_label,
    masked_letters,
    percentile,
    render_markdown,
    select_longest,
    select_sample,
    summarize_baseline,
    unit_index_for_offset,
)


# --------------------------------------------------------------------------
# range arithmetic
# --------------------------------------------------------------------------


def test_check_ranges_accepts_disjoint_sorted_ranges():
    check_ranges("REF9:x", [[0, 10], [10, 20], [40, 41]])


def test_check_ranges_rejects_overlapping_ranges():
    # Summing widths across an overlap double-counts the shared letters and
    # reports a work as more masked than it is.
    with pytest.raises(ValueError, match="overlap"):
        check_ranges("REF9:x", [[0, 10], [5, 20]])


def test_check_ranges_rejects_unsorted_ranges():
    with pytest.raises(ValueError, match="overlap or are unsorted"):
        check_ranges("REF9:x", [[40, 50], [0, 10]])


def test_check_ranges_rejects_inverted_range():
    with pytest.raises(ValueError, match="inverted"):
        check_ranges("REF9:x", [[20, 10]])


def test_check_ranges_rejects_malformed_pair():
    with pytest.raises(ValueError, match="not a \\[start, end\\] pair"):
        check_ranges("REF9:x", [[1, 2, 3]])


def test_masked_letters_sums_widths():
    assert masked_letters([[0, 10], [30, 45]]) == 25
    assert masked_letters([]) == 0


# --------------------------------------------------------------------------
# addressing a masked interval to its division
# --------------------------------------------------------------------------


def test_unit_index_is_the_division_containing_the_offset():
    starts = [0, 100, 250]
    assert unit_index_for_offset(starts, 0) == 0
    assert unit_index_for_offset(starts, 99) == 0
    assert unit_index_for_offset(starts, 100) == 1  # exactly on a boundary
    assert unit_index_for_offset(starts, 249) == 1
    assert unit_index_for_offset(starts, 10_000) == 2


def test_unit_index_is_negative_before_the_first_division():
    # A whole-work fallback starts at 0, so this only happens on a malformed
    # unit list -- the packet must render it as "before the first division"
    # rather than silently attributing the span to the wrong place.
    assert unit_index_for_offset([50, 100], 10) == -1


# --------------------------------------------------------------------------
# excerpting
# --------------------------------------------------------------------------


def test_excerpt_returns_the_whole_span_when_it_fits():
    assert excerpt_text("אבגדהוז", 1, 4, 220) == "בגד"


def test_excerpt_elides_the_middle_when_over_budget():
    stream = "".join(str(index % 10) for index in range(100))
    excerpt = excerpt_text(stream, 0, 100, 10)
    assert excerpt == "01234…56789"
    # Head AND tail: a masked span is judged by how it starts and how it ends,
    # so a head-only truncation would hide where the mask stopped.
    assert excerpt.startswith(stream[:5])
    assert excerpt.endswith(stream[-5:])


# --------------------------------------------------------------------------
# interval selection
# --------------------------------------------------------------------------


def _rows(*spans):
    return [{"start": start, "length": length} for start, length in spans]


def test_select_longest_orders_by_length_then_position():
    rows = _rows((500, 20), (10, 100), (900, 100), (0, 5))
    assert [row["start"] for row in select_longest(rows, 3)] == [10, 900, 500]


def test_select_sample_excludes_the_intervals_already_shown():
    rows = _rows(*[(index * 100, 10) for index in range(20)])
    longest = {0, 100, 200}
    sample = select_sample(rows, 3, seed=7, exclude=longest)
    assert len(sample) == 3
    assert not {row["start"] for row in sample} & longest


def test_select_sample_is_reproducible_from_the_seed():
    rows = _rows(*[(index * 100, 10) for index in range(20)])
    first = select_sample(rows, 4, seed=7, exclude=set())
    second = select_sample(rows, 4, seed=7, exclude=set())
    other = select_sample(rows, 4, seed=8, exclude=set())
    assert [row["start"] for row in first] == [row["start"] for row in second]
    assert [row["start"] for row in first] != [row["start"] for row in other]


def test_select_sample_returns_everything_when_the_pool_is_small():
    rows = _rows((0, 10), (100, 10))
    assert len(select_sample(rows, 5, seed=1, exclude=set())) == 2


# --------------------------------------------------------------------------
# baseline statistics
# --------------------------------------------------------------------------


def test_percentile_is_nearest_rank():
    # Nearest rank returns an OBSERVED fraction, never an interpolation
    # between two works: every number in the baseline table is one that some
    # real reference actually scored.
    values = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    assert percentile(values, 0.9) == pytest.approx(0.8)
    assert percentile(values, 1.0) == pytest.approx(0.9)
    assert percentile(values, 0.0) == pytest.approx(0.0)
    assert percentile([], 0.9) == 0.0


def test_group_label_is_the_namespace_never_the_id():
    assert group_label("REF2:tanhuma") == "REF2"
    assert group_label("A:01-xyz-plain") == "A"
    assert group_label("bare") == "(unprefixed)"


def test_baseline_counts_unmasked_works_in_the_denominator():
    # The untouched work is a 0% observation, not an absence: a baseline drawn
    # only from masked works would flatter itself and make an over-masked
    # reference look ordinary.
    reference = [
        {"id": "A:one", "stream": "א" * 100},
        {"id": "A:two", "stream": "א" * 100},
    ]
    masks = {"A:one": [[0, 50]]}
    rows = summarize_baseline(reference, masks, "REF9:")
    assert rows == [
        {
            "group": "A",
            "work_count": 2,
            "works_with_masks": 1,
            "letters": 200,
            "masked_fraction_median": 0.25,  # (0% + 50%) / 2, not 50%
            "masked_fraction_p90": 0.5,
            "masked_fraction_max": 0.5,
        }
    ]


# --------------------------------------------------------------------------
# the packet end to end
# --------------------------------------------------------------------------

BASE_ID = "A:07-restricted-witness-id"
BASE_TITLE = "כתב יד שאסור להזכיר"


def _packet_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    reference = [
        {"id": BASE_ID, "stream": "ב" * 1000, "title": BASE_TITLE},
        {"id": "REF9:alpha", "stream": "א" * 1000, "title": "אלפא"},
        {"id": "REF9:beta", "stream": "ג" * 500, "title": "ביתא"},
    ]
    corpus_path = tmp_path / "corpus.pkl"
    with corpus_path.open("wb") as handle:
        pickle.dump(reference, handle)
    manifest = {
        "entries": [
            {
                "raw_reference_id": "REF9:alpha",
                "source_key": "alpha",
                "title": "אלפא",
                "provider": "sefaria",
                "locus_grain": "chapter",
                "stream_len": 1000,
                "unit_offsets": [
                    {"start_offset": 0, "label_he": "אלפא א"},
                    {"start_offset": 400, "label_he": "אלפא ב"},
                ],
            },
            {
                "raw_reference_id": "REF9:beta",
                "source_key": "beta",
                "title": "ביתא",
                "provider": "hewikisource",
                "locus_grain": "section",
                "stream_len": 500,
                "unit_offsets": [{"start_offset": 0, "label_he": "ביתא כולה"}],
            },
        ]
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    masks_path = tmp_path / "masks.json"
    masks_path.write_text(
        json.dumps(
            {
                BASE_ID: [[0, 900]],
                "REF9:alpha": [[100, 150], [500, 700]],
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        reference=str(corpus_path),
        reference_sha256=None,
        masks=str(masks_path),
        masks_sha256=None,
        manifest=str(manifest_path),
        manifest_sha256=None,
        reference_namespace="REF9",
        top_intervals=5,
        sample_per_work=3,
        sample_seed=1,
        excerpt_chars=220,
        context_chars=40,
        detail_works=12,
        detail_include=None,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_packet_reports_masked_fraction_per_work(tmp_path):
    packet = build_packet(_packet_args(tmp_path))
    by_id = {row["raw_reference_id"]: row for row in packet["works"]}
    assert by_id["REF9:alpha"]["masked_letters"] == 250
    assert by_id["REF9:alpha"]["masked_fraction"] == pytest.approx(0.25)
    assert by_id["REF9:alpha"]["interval_count"] == 2
    assert by_id["REF9:alpha"]["longest_interval"] == 200
    assert by_id["REF9:beta"]["masked_fraction"] == 0.0
    assert packet["namespace_totals"]["masked_fraction"] == 0.166667  # 250/1500


def test_packet_addresses_each_interval_to_its_division(tmp_path):
    packet = build_packet(_packet_args(tmp_path))
    alpha = next(
        row for row in packet["works"] if row["raw_reference_id"] == "REF9:alpha"
    )
    labels = {row["start"]: row["unit_label_he"] for row in alpha["longest_intervals"]}
    assert labels == {100: "אלפא א", 500: "אלפא ב"}


def test_packet_sorts_the_most_masked_first(tmp_path):
    packet = build_packet(_packet_args(tmp_path))
    assert [row["raw_reference_id"] for row in packet["works"]] == [
        "REF9:alpha",
        "REF9:beta",
    ]


def test_packet_names_only_the_reviewed_namespace(tmp_path):
    """Containment: no base-corpus id or title, in the JSON or the Markdown."""
    packet = build_packet(_packet_args(tmp_path))
    rendered = json.dumps(packet, ensure_ascii=False) + render_markdown(packet, 12)
    assert BASE_ID not in rendered
    assert BASE_TITLE not in rendered
    # The base corpus is still counted -- aggregated under its group letter.
    assert [row["group"] for row in packet["baseline_groups"]] == ["A"]
    assert packet["baseline_groups"][0]["work_count"] == 1


def test_packet_hard_errors_when_the_manifest_misses_a_reference(tmp_path):
    args = _packet_args(tmp_path)
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    manifest["entries"] = manifest["entries"][:1]
    Path(args.manifest).write_text(
        json.dumps(manifest, ensure_ascii=False), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="manifest does not describe every"):
        build_packet(args)


def test_packet_hard_errors_on_an_empty_namespace(tmp_path):
    with pytest.raises(ValueError, match="no REF8 references"):
        build_packet(_packet_args(tmp_path, reference_namespace="REF8"))


def test_packet_hard_errors_on_overlapping_masks(tmp_path):
    args = _packet_args(tmp_path)
    Path(args.masks).write_text(
        json.dumps({"REF9:alpha": [[0, 600], [500, 700]]}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="overlap"):
        build_packet(args)


def test_markdown_renders_the_tables_and_the_excerpts(tmp_path):
    packet = build_packet(_packet_args(tmp_path))
    rendered = render_markdown(packet, 12)
    assert "# Canonical-mask review — REF9" in rendered
    assert "| אלפא | 1,000 | 250 | 25.00% | 2 | 200 |" in rendered
    assert "**200 letters** at offset 500 — אלפא ב" in rendered
    assert "## Baseline — the rest of the reference corpus" in rendered


def test_markdown_details_a_forced_work_however_little_it_masks(tmp_path):
    packet = build_packet(_packet_args(tmp_path, detail_include=["beta"]))
    beta = next(row for row in packet["works"] if row["source_key"] == "beta")
    assert beta["forced_detail"] is True
    # Unmasked works are not detailed even when forced -- there is nothing to
    # show -- but the flag survives into the packet for the caller to see.
    rendered = render_markdown(packet, 0)
    assert "### אלפא" not in rendered
