"""Seam documents (docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md §6.1): runs, part bridges,
the K-word windows and their provenance.

The claim the seams rest on: for a crossing match whose side is at most K words, the
seam centred on the first break it crosses holds it, so the seams of a chain give
exactly the crossings ``_cross_page_spans`` finds on the chain's whole text.
"""
import random

import pytest

import shared.search_engine as se
from shared.index_seams import ChainStats, chains_for, follows, runs_of, seams_of
from shared.variants import VariantManager

T1, T2 = "תעודדו", "תענגו"
X = "עמוד"


def page(uid, p_num, text, source="V0.8", sys_id="990001", ie="IE1"):
    return {"uid": f"{ie}_P{p_num:06d}_FL{uid}", "p_num": p_num, "content": text, "source": source,
            "sys_id": sys_id, "full_header": f"{sys_id}_{ie}_P{p_num:06d}_FL{uid}"}


@pytest.fixture(scope="module")
def eng():
    e = se.SearchEngine.__new__(se.SearchEngine)
    e.var_mgr = VariantManager()
    return e


# --- runs -------------------------------------------------------------------------

def test_runs_break_at_the_v07_restart_a_volume_change_and_a_missing_page():
    pages = [page(1, 1, "a"), page(2, 2, "b"), page(3, 3, "c"),
             page(1, 1, "a7", source="V0.7"), page(2, 2, "b7", source="V0.7"),
             page(9, 1, "x", ie="IE2"), page(10, 2, "y", ie="IE2"),
             page(12, 4, "z", ie="IE2")]
    runs = runs_of(pages)
    assert [[p["content"] for p in r] for r in runs] == [["a", "b", "c"], ["a7", "b7"], ["x", "y"], ["z"]]


def test_follows_needs_a_page_number():
    assert not follows(page(1, 0, "a"), page(2, 1, "b"))
    assert follows(page(1, 1, "a"), page(2, 2, "b"))


# --- part bridges -----------------------------------------------------------------

def _part(folios):
    """{sys_id: pages} for folios [(sys_id, folio no, [(source, p_num)...], ie)]."""
    system_pages, numbers = {}, {}
    for sid, folio, pages, ie in folios:
        numbers[sid] = folio
        system_pages[sid] = [page(f"{sid}{p}{src}", p, f"{sid}-{src}-{p}", source=src, sys_id=sid, ie=ie)
                             for src, p in pages]
    return system_pages, numbers


def _chain_texts(chains):
    return sorted(tuple(p["content"] for p in c) for c in chains)


def test_consecutive_folios_of_a_part_are_bridged_per_source():
    both = [("V0.8", 1), ("V0.8", 2), ("V0.7", 1), ("V0.7", 2)]
    sp, num = _part([("A", 5, both, "IE10"), ("B", 6, both, "IE11")])
    stats = ChainStats()
    chains = chains_for(sp, {"part": ["A", "B"]}, num.get, stats=stats)
    assert stats.bridges == 2
    assert _chain_texts(chains) == sorted([("A-V0.8-1", "A-V0.8-2", "B-V0.8-1", "B-V0.8-2"),
                                           ("A-V0.7-1", "A-V0.7-2", "B-V0.7-1", "B-V0.7-2")])


@pytest.mark.parametrize("folios, why", [
    ([("A", 5, [("V0.8", 1), ("V0.8", 2)], "IE10"), ("B", 7, [("V0.8", 1)], "IE11")],
     "folio numbers not consecutive"),
    ([("A", 5, [("V0.8", 1), ("V0.8", 2)], "IE10"), ("B", 6, [("V0.8", 2)], "IE11")],
     "a missing page at the folio edge"),
])
def test_a_part_junction_that_is_not_reading_order_is_not_bridged(folios, why):
    sp, num = _part(folios)
    stats = ChainStats()
    chains = chains_for(sp, {"part": ["A", "B"]}, num.get, stats=stats)
    assert stats.bridges == 0 and stats.bridges_refused == {why: 1}
    assert len(chains) == 2


def test_a_chain_runs_on_through_several_bridged_folios():
    one = [("V0.8", 1)]
    sp, num = _part([("A", 1, one, "IE1"), ("B", 2, one, "IE2"), ("C", 3, one, "IE3")])
    chains = chains_for(sp, {"part": ["A", "B", "C"]}, num.get)
    assert _chain_texts(chains) == [("A-V0.8-1", "B-V0.8-1", "C-V0.8-1")]


def test_without_parts_the_chains_are_the_runs():
    sp = {"990001": [page(1, 1, "a"), page(2, 2, "b"), page(1, 1, "c", source="V0.7")]}
    assert _chain_texts(chains_for(sp)) == [("a", "b"), ("c",)]


# --- seams ------------------------------------------------------------------------

def test_one_seam_per_junction_with_k_words_each_side():
    chain = [page(1, 1, "a1 a2 a3 a4"), page(2, 2, "b1 b2 b3 b4"), page(3, 3, "c1 c2")]
    seams = list(seams_of(chain, 2))
    assert [s.text for s in seams] == ["a3 a4\nb1 b2", "b3 b4\nc1 c2"]
    assert [s.text[s.cut:s.cut + 2] for s in seams] == ["b1", "c1"]


def test_a_seam_crosses_further_breaks_when_pages_are_short():
    chain = [page(1, 1, "a1 a2"), page(2, 2, "b1"), page(3, 3, "c1"), page(4, 4, "d1 d2")]
    first = next(iter(seams_of(chain, 3)))
    assert first.text == "a1 a2\nb1\nc1\nd1"
    assert [s["uid"] for s in first.slices] == [p["uid"] for p in chain]


def test_slices_map_back_to_their_pages():
    rng = random.Random(7)
    chain = [page(i, i, " ".join(f"w{i}x{j}" for j in range(rng.randint(0, 9)))) for i in range(1, 12)]
    by_uid = {p["uid"]: p["content"] for p in chain}
    for seam in seams_of(chain, 4):
        for s in seam.slices:
            piece = seam.text[s["start"]:s["end"]]
            text = by_uid[s["uid"]] + "\n"          # the joiner belongs to the page before it
            assert text[s["offset"]:s["offset"] + len(piece)] == piece


def test_the_three_page_match_lies_in_the_seam_of_its_first_break(eng):
    # tests/test_cross_page_spans.py::test_a_match_over_a_one_word_page_is_returned_once
    chain = [page(1, 1, f"{X} {T1}"), page(2, 2, X), page(3, 3, f"{T2} {X}")]
    q = [T1, X, T2]
    rx = eng.build_regex_pattern(q, "literal", 0)
    side = (len(q) - 1) * 1
    first = next(iter(seams_of(chain, side)))
    got = eng._cross_page_spans(rx, first.text, [{"start": 0}, {"start": first.cut}],
                                T1, T2, side, 0, True)
    assert [first.text[s:e] for s, e in got] == [f"{T1}\n{X}\n{T2}"]


def _chain_content(chain):
    content = "\n".join(p["content"] for p in chain)
    starts, cur = [], 0
    for p in chain:
        starts.append(cur)
        cur += len(p["content"]) + 1
    return content, starts


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("gap", [0, 2])
def test_the_seams_give_exactly_the_crossings_of_the_whole_text(eng, seed, gap):
    """For every side up to K, the crossings found in the seams (each at its central
    break, mapped back through the slices) are those _cross_page_spans finds on the
    chain's whole text, as a whole-manuscript document holds it."""
    rng = random.Random(seed)
    vocab = [T1, T2, X, "[" + X, X + "]", "..."]
    chain = [page(i, i, " ".join(rng.choice(vocab) for _ in range(rng.randint(0, 6))))
             for i in range(1, 30)]
    content, starts = _chain_content(chain)
    bounds = [{"uid": p["uid"], "start": s} for p, s in zip(chain, starts)]
    index_of = {p["uid"]: i for i, p in enumerate(chain)}
    K = 6
    crossings = 0
    for query in ([T1, T2], [T1, X, T2]):
        side = (len(query) - 1) * (gap + 1)
        assert side <= K
        rx = eng.build_regex_pattern(query, "literal", gap)
        whole = set(eng._cross_page_spans(rx, content, bounds, query[0], query[-1], side, gap, True))
        from_seams = set()
        for seam in seams_of(chain, K):
            for s, e in eng._cross_page_spans(rx, seam.text, [{"start": 0}, {"start": seam.cut}],
                                              query[0], query[-1], side, gap, True):
                first = seam.slices[0]
                base = starts[index_of[first["uid"]]] + first["offset"] - first["start"]
                from_seams.add((base + s, base + e))
        assert from_seams == whole
        crossings += len(whole)
    assert crossings      # every seed's chain has some: the comparison is not empty
