"""V4.2 container sources and the Mishneh Torah license ruling (C7).

A container stitches an ORDERED list of independent Sefaria indices (each its
own top-level index; Mishneh Torah's book grain has no single source_ref that
covers a book) into one combined normalized source. The child list is FROZEN
in the source map: live ToC discovery may verify membership/order but never
silently redefines it (C7). Licenses fail closed exactly as V4/V4.1 do, with
one narrow widening: a dated ``license_ruling`` lets a container's EFFECTIVE
license differ from a per-child provider-reported one, but never rescues an
absent/unknown report.

The existing exactly-ten REF5 test (tests/test_discovery_v4_1_sources.py)
stays unchanged; this file is REF6's own coverage (C12).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.discovery_v4_build_reference import _unit_offsets
from scripts.discovery_v4_common import (
    compact_stream,
    load_source_config,
    reference_namespace,
    source_target_ids,
)
from scripts.discovery_v4_fetch_sources import (
    _acquire_container_sefaria,
    _check_frozen_children_against_toc,
    _pick_hebrew_version_for_container,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
V4_MAP = SCRIPTS / "discovery_v4_sources.json"
V4_1_MAP = SCRIPTS / "discovery_v4_1_sources.json"
V4_2_MAP = SCRIPTS / "discovery_v4_2_sources.json"
PROBE = (
    Path(__file__).resolve().parents[1]
    / "discovery_builds/discovery_v4/probe_v42/mishneh_torah_container_probe.json"
)


# --------------------------------------------------------------------------
# Fixture helpers
# --------------------------------------------------------------------------


def _write_container_map(
    tmp_path: Path,
    *,
    key: str = "c1",
    children: list[dict] | None = None,
    mappings: list[dict] | None = None,
    license_ruling: dict | None = None,
    provider: str = "sefaria",
) -> Path:
    if children is None:
        children = [
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ]
    if mappings is None:
        mappings = [{"target_work_id": "w000001"}]
    source = {
        "key": key,
        "provider": provider,
        "container": True,
        "children": children,
        "locus_grain": "section",
        "mappings": mappings,
    }
    if license_ruling is not None:
        source["license_ruling"] = license_ruling
    path = tmp_path / "sources.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-sources-v1",
                "reference_namespace": "REF6",
                "sources": [source],
            }
        ),
        encoding="utf-8",
    )
    return path


def _container_source_dict(
    *,
    key: str = "c1",
    children: list[dict],
    license_ruling: dict | None = None,
) -> dict:
    source = {
        "key": key,
        "provider": "sefaria",
        "container": True,
        "children": children,
        "locus_grain": "section",
        "mappings": [{"target_work_id": "w000001"}],
    }
    if license_ruling is not None:
        source["license_ruling"] = license_ruling
    return source


class FakeFetcher:
    """Stand-in for Fetcher: canned index/text responses, no real network."""

    def __init__(
        self,
        raw_root: Path,
        index_responses: dict[str, dict],
        text_responses: dict[tuple[str, str], dict],
    ) -> None:
        self.raw_dir = raw_root
        self._index_responses = index_responses
        self._text_responses = text_responses

    def sefaria_index(self, ref: str, raw_path: Path) -> dict:
        doc = self._index_responses[ref]
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(doc), encoding="utf-8")
        return doc

    def sefaria_text(self, ref: str, version: str, raw_path: Path) -> dict:
        doc = self._text_responses[(ref, version)]
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(doc), encoding="utf-8")
        return doc


def _build_fixture(
    children_specs: list[tuple[str, str, str, str, str]],
) -> tuple[dict[str, dict], dict[tuple[str, str], dict]]:
    """children_specs: (source_ref, he_title, license, raw_text, version_title)."""
    index_responses: dict[str, dict] = {}
    text_responses: dict[tuple[str, str], dict] = {}
    for ref, he_title, license_name, text, version_title in children_specs:
        index_responses[ref] = {"title": ref, "heTitle": he_title}
        text_responses[(ref, "all")] = {
            "available_versions": [
                {
                    "language": "he",
                    "license": license_name,
                    "versionTitle": version_title,
                    "isPrimary": True,
                }
            ]
        }
        text_responses[(ref, f"hebrew|{version_title}")] = {
            "versions": [{"text": text}]
        }
    return index_responses, text_responses


# --------------------------------------------------------------------------
# 1. Container schema validation
# --------------------------------------------------------------------------


def test_container_source_map_loads_with_valid_children(tmp_path: Path):
    path = _write_container_map(tmp_path)
    config = load_source_config(path)
    source = config["sources"][0]
    assert source["container"] is True
    assert [child["child_key"] for child in source["children"]] == ["a", "b"]


def test_container_duplicate_child_key_is_rejected(tmp_path: Path):
    path = _write_container_map(
        tmp_path,
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "a", "source_ref": "Ref B"},
        ],
    )
    with pytest.raises(ValueError, match="duplicate child_key"):
        load_source_config(path)


def test_container_empty_children_list_is_rejected(tmp_path: Path):
    path = _write_container_map(tmp_path, children=[])
    with pytest.raises(ValueError, match="non-empty children"):
        load_source_config(path)


def test_container_must_use_sefaria_provider(tmp_path: Path):
    path = _write_container_map(tmp_path, provider="hewikisource")
    with pytest.raises(ValueError, match="provider 'sefaria'"):
        load_source_config(path)


def test_container_license_ruling_requires_effective_license(tmp_path: Path):
    path = _write_container_map(tmp_path, license_ruling={"ruled_by": "owner"})
    with pytest.raises(ValueError, match="effective_license"):
        load_source_config(path)


# --------------------------------------------------------------------------
# 2. Container license semantics (fetch-time)
# --------------------------------------------------------------------------


def test_container_mixed_reported_licenses_without_ruling_is_a_hard_error(
    tmp_path: Path,
):
    children_specs = [
        ("Ref A", "כותרת א", "Public Domain", "אבגדהוזחטי", "V1"),
        ("Ref B", "כותרת ב", "CC-BY-SA", "כלמנסעפצקר", "V1"),
    ]
    index_responses, text_responses = _build_fixture(children_specs)
    fetcher = FakeFetcher(tmp_path, index_responses, text_responses)
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ]
    )
    with pytest.raises(ValueError, match="mixed licenses"):
        _acquire_container_sefaria(
            fetcher, source, {"public domain", "cc-by-sa"}, sleep_fn=lambda _s: None
        )


def test_container_ruling_resolves_mixed_reported_licenses(tmp_path: Path):
    children_specs = [
        ("Ref A", "כותרת א", "Public Domain", "אבגדהוזחטי", "V1"),
        ("Ref B", "כותרת ב", "CC-BY-SA", "כלמנסעפצקר", "V1"),
    ]
    index_responses, text_responses = _build_fixture(children_specs)
    fetcher = FakeFetcher(tmp_path, index_responses, text_responses)
    ruling = {
        "effective_license": "Public Domain",
        "ruled_by": "owner",
        "ruled_on": "2026-08-16",
        "note": "test ruling",
    }
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ],
        license_ruling=ruling,
    )
    acquired, _raw_paths = _acquire_container_sefaria(
        fetcher, source, {"public domain", "cc-by-sa"}, sleep_fn=lambda _s: None
    )
    assert acquired["license"] == "Public Domain"
    assert acquired["license_ruling"] == ruling
    reported = {child["child_key"]: child["reported_license"] for child in acquired["children"]}
    assert reported == {"a": "Public Domain", "b": "CC-BY-SA"}


def test_container_without_ruling_refuses_a_known_but_non_allowlisted_license():
    available = [
        {"language": "he", "license": "CC-BY-SA", "versionTitle": "V1", "isPrimary": True}
    ]
    with pytest.raises(ValueError, match="no allowlisted Hebrew version"):
        _pick_hebrew_version_for_container(available, {"public domain"}, has_ruling=False)


def test_container_ruling_rescues_a_known_but_non_allowlisted_license():
    available = [
        {"language": "he", "license": "CC-BY-SA", "versionTitle": "V1", "isPrimary": True}
    ]
    best = _pick_hebrew_version_for_container(available, {"public domain"}, has_ruling=True)
    assert best["license"] == "CC-BY-SA"


def test_container_ruling_never_rescues_an_unknown_license():
    available = [
        {"language": "he", "license": "unknown", "versionTitle": "V1", "isPrimary": True}
    ]
    with pytest.raises(ValueError, match="known license"):
        _pick_hebrew_version_for_container(available, {"public domain"}, has_ruling=True)


def test_container_ruling_never_rescues_an_absent_license():
    available = [
        {"language": "he", "license": "", "versionTitle": "V1", "isPrimary": True}
    ]
    with pytest.raises(ValueError, match="known license"):
        _pick_hebrew_version_for_container(available, {"public domain"}, has_ruling=True)


# --------------------------------------------------------------------------
# 3. Frozen child-list drift detection
# --------------------------------------------------------------------------


def test_frozen_child_list_matches_live_toc_is_a_noop():
    children_cfg = [
        {"child_key": "a", "source_ref": "Ref A"},
        {"child_key": "b", "source_ref": "Ref B"},
    ]
    _check_frozen_children_against_toc(children_cfg, ["Ref A", "Ref B"])  # no raise


def test_frozen_child_list_membership_drift_is_a_hard_error():
    children_cfg = [
        {"child_key": "a", "source_ref": "Ref A"},
        {"child_key": "b", "source_ref": "Ref B"},
    ]
    with pytest.raises(ValueError, match="membership"):
        _check_frozen_children_against_toc(children_cfg, ["Ref A", "Ref C"])


def test_frozen_child_list_order_drift_is_a_hard_error():
    children_cfg = [
        {"child_key": "a", "source_ref": "Ref A"},
        {"child_key": "b", "source_ref": "Ref B"},
    ]
    with pytest.raises(ValueError, match="order"):
        _check_frozen_children_against_toc(children_cfg, ["Ref B", "Ref A"])


def test_acquire_container_checks_live_toc_before_any_fetch(tmp_path: Path):
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ]
    )
    # Empty response tables: a KeyError here would mean the drift check did
    # NOT run before the fetcher was touched.
    fetcher = FakeFetcher(tmp_path, {}, {})
    with pytest.raises(ValueError, match="live Sefaria ToC"):
        _acquire_container_sefaria(
            fetcher,
            source,
            {"public domain"},
            sleep_fn=lambda _s: None,
            live_children_refs=["Ref A", "Ref C"],
        )


# --------------------------------------------------------------------------
# 4. Offset-interval correctness
# --------------------------------------------------------------------------


def test_container_offset_intervals_are_correct_and_round_trip(tmp_path: Path):
    # Each child's raw text has one embedded space, so the folded
    # (compact_stream) length is shorter than the raw cleaned length -- a
    # mutation that used the wrong length would be caught here.
    children_specs = [
        ("Ref A", "א", "Public Domain", "אב גד", "V1"),
        ("Ref B", "ב", "Public Domain", "הו זח", "V1"),
        ("Ref C", "ג", "Public Domain", "טי כל", "V1"),
    ]
    index_responses, text_responses = _build_fixture(children_specs)
    fetcher = FakeFetcher(tmp_path, index_responses, text_responses)
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
            {"child_key": "c", "source_ref": "Ref C"},
        ]
    )
    acquired, _raw_paths = _acquire_container_sefaria(
        fetcher, source, {"public domain"}, sleep_fn=lambda _s: None
    )

    # Independently computed expected offsets (not via _unit_offsets).
    expected_starts = []
    running = 0
    for _ref, _he, _lic, text, _ver in children_specs:
        folded_len = len(compact_stream(text))
        expected_starts.append(running)
        running += folded_len
    for child, expected_start in zip(acquired["children"], expected_starts):
        assert child["offset_start"] == expected_start
    assert acquired["children"][-1]["offset_end"] == running

    # Round-trip against the established _unit_offsets convention: recomputing
    # offsets over the SAME units this container produced must agree exactly.
    stream, offset_rows = _unit_offsets(acquired["units"])
    recomputed = {unit["ordinal"]: start for unit, start in offset_rows}
    for child, unit in zip(acquired["children"], acquired["units"]):
        assert child["offset_start"] == recomputed[unit["ordinal"]]
        assert child["offset_end"] == recomputed[unit["ordinal"]] + len(
            compact_stream(unit["text"])
        )
    assert stream == "".join(compact_stream(u["text"]) for u in acquired["units"])


def test_acquire_container_carries_a_source_url_for_the_builder(tmp_path: Path):
    """discovery_v4_build_reference.py reads ``normalized["source_url"]`` for
    EVERY acquired source; the container path must provide one (the first
    child's Sefaria page -- there is no single provider URL for a container
    spanning several indices, and the first child is the work's opening
    section). Its absence broke the first REF6 append (2026-08-17)."""
    children_specs = [
        ("Ref A", "א", "Public Domain", "אבגד הוזח", "V1"),
        ("Ref B", "ב", "Public Domain", "טיכל מנסע", "V1"),
    ]
    index_responses, text_responses = _build_fixture(children_specs)
    fetcher = FakeFetcher(tmp_path, index_responses, text_responses)
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ]
    )
    acquired, _raw_paths = _acquire_container_sefaria(
        fetcher, source, {"public domain"}, sleep_fn=lambda _s: None
    )
    assert acquired["source_url"] == "https://www.sefaria.org/Ref_A"


def test_acquire_container_throttles_between_every_request(tmp_path: Path):
    children_specs = [
        ("Ref A", "א", "Public Domain", "אבגד", "V1"),
        ("Ref B", "ב", "Public Domain", "הוזח", "V1"),
    ]
    index_responses, text_responses = _build_fixture(children_specs)
    fetcher = FakeFetcher(tmp_path, index_responses, text_responses)
    source = _container_source_dict(
        children=[
            {"child_key": "a", "source_ref": "Ref A"},
            {"child_key": "b", "source_ref": "Ref B"},
        ]
    )
    sleeps: list[float] = []
    _acquire_container_sefaria(
        fetcher, source, {"public domain"}, sleep_fn=sleeps.append
    )
    # index + metadata + selected-text, per child, at ~1 req/s.
    assert sleeps == [1.0] * 6


# --------------------------------------------------------------------------
# 5. The V4.2 map itself
# --------------------------------------------------------------------------


def test_v4_2_map_is_stored_without_carriage_returns():
    assert b"\r\n" not in V4_2_MAP.read_bytes()


# The four post-sitting (2026-08-16) private_sibling additions: key -> target.
POST_SITTING_ADDITIONS = {
    "rabbeinu_chananel_bava_kamma": "w000463",
    "sifrei_zuta_bamidbar": "w000524",
    "ben_sira_alfa_beta_a": "w001079",
    "kuzari_ibn_tibbon": "w000194",
}


def test_v4_2_map_has_exactly_fifteen_containers_and_88_children():
    config = load_source_config(V4_2_MAP)
    assert reference_namespace(config) == "REF6"
    containers = [s for s in config["sources"] if s.get("container")]
    # "additions" here means the four post-sitting PRIVATE_SIBLING sources
    # only -- the 31 public_first sources (this session) are neither a
    # container nor mapping-bearing, so they are excluded from this specific
    # pin; they get their own exact-composition test below.
    additions = [
        s
        for s in config["sources"]
        if not s.get("container") and s.get("identity_mode") != "public_first"
    ]
    assert len(config["sources"]) == 50
    assert len(containers) == 15
    total_children = sum(len(source["children"]) for source in containers)
    assert total_children == 88
    container_targets = {
        mapping["target_work_id"]
        for source in containers
        for mapping in source["mappings"]
    }
    assert container_targets == {f"w{n:06d}" for n in range(174, 189)}
    assert all(source.get("license_ruling") for source in containers)
    assert all(
        source["license_ruling"]["effective_license"] == "Public Domain"
        for source in containers
    )
    # The post-sitting additions are pinned exactly: keys, targets, and the
    # absence of any container/license_ruling machinery on them.
    assert {
        s["key"]: s["mappings"][0]["target_work_id"] for s in additions
    } == POST_SITTING_ADDITIONS
    assert all(len(s["mappings"]) == 1 for s in additions)
    assert not any(s.get("license_ruling") for s in additions)


def test_v4_2_map_children_are_copied_verbatim_from_the_probe():
    probe = json.loads(PROBE.read_text(encoding="utf-8"))
    books = probe["mishneh_torah"]["books"]
    probe_refs_by_work = {
        book["private_work_id"]: [child["source_ref"] for child in book["children"]]
        for book in books
    }
    config = load_source_config(V4_2_MAP)
    map_refs_by_work = {
        source["mappings"][0]["target_work_id"]: [
            child["source_ref"] for child in source["children"]
        ]
        for source in config["sources"]
        if source.get("container")
    }
    assert map_refs_by_work == probe_refs_by_work


def test_v4_2_map_inherits_v4s_license_allowlist_and_size_floor():
    v4 = load_source_config(V4_MAP)
    v4_2 = load_source_config(V4_2_MAP)
    assert v4_2["license_allowlist"] == v4["license_allowlist"]
    assert v4_2["minimum_hebrew_letters"] == v4["minimum_hebrew_letters"]


# --------------------------------------------------------------------------
# 6. The 32 public_first REF6 additions (discovery-v4.2 C7/C8, this session)
# --------------------------------------------------------------------------

# Owner-approved identities deliberately absent from the map: three with no
# viable route anywhere (confirmed live 2026-08-16), plus pf-1032 (תוספות
# רי"ד), whose ~319 real pages exist under the long title scheme but are NOT
# linked from the main ToC page (live spot-check 2026-08-16 selected 0 links
# per authored cluster) -- its route needs a dedicated probe round.
PUBLIC_FIRST_UNROUTED_IDENTITY_KEYS = {"pf-1013", "pf-1015", "pf-1031", "pf-1032"}


def test_v4_2_map_has_exactly_thirty_one_public_first_sources():
    config = load_source_config(V4_2_MAP)
    public_first = [
        s for s in config["sources"] if s.get("identity_mode") == "public_first"
    ]
    assert len(public_first) == 31
    assert not any(s.get("container") for s in public_first)
    assert not any("mappings" in s for s in public_first)

    identity_keys = {s["identity_key"] for s in public_first}
    expected_keys = {f"pf-{n:04d}" for n in range(1001, 1036)} - (
        PUBLIC_FIRST_UNROUTED_IDENTITY_KEYS
    )
    assert identity_keys == expected_keys
    assert len(identity_keys) == 31

    by_shape = {"daf_pages": 0, "page_clusters": 0, "schema_leaves": 0, "plain": 0}
    for source in public_first:
        if source.get("mode") == "daf_pages":
            by_shape["daf_pages"] += 1
        elif "page_clusters" in source:
            by_shape["page_clusters"] += 1
        elif source.get("mode") == "schema_leaves":
            by_shape["schema_leaves"] += 1
        else:
            by_shape["plain"] += 1
    assert by_shape == {
        "daf_pages": 3,
        "page_clusters": 5,
        "schema_leaves": 8,
        "plain": 15,
    }


def test_v4_2_map_public_first_zohar_daf_ranges_have_no_daf_token():
    config = load_source_config(V4_2_MAP)
    zohar = {
        s["identity_key"]: s
        for s in config["sources"]
        if s.get("mode") == "daf_pages" and s.get("identity_mode") == "public_first"
    }
    assert set(zohar) == {"pf-1021", "pf-1022", "pf-1023"}
    assert zohar["pf-1021"]["daf_range"] == [1, 250]
    assert zohar["pf-1022"]["daf_range"] == [2, 268]
    assert zohar["pf-1023"]["daf_range"] == [2, 299]
    for source in zohar.values():
        assert "דף" not in source["link_prefix"]


def test_v4_2_map_public_first_page_clusters_are_frozen_and_nonempty():
    config = load_source_config(V4_2_MAP)
    clustered = [
        s
        for s in config["sources"]
        if "page_clusters" in s and s.get("identity_mode") == "public_first"
    ]
    assert {s["key"] for s in clustered} == {
        "baal_haturim_torah",
        "rashbam_torah",
        "yad_rama_talmud",
        "semag",
        "ran_al_harif",
    }
    cluster_counts = {s["key"]: len(s["page_clusters"]) for s in clustered}
    assert cluster_counts == {
        "baal_haturim_torah": 5,
        "rashbam_torah": 5,
        "yad_rama_talmud": 2,
        "semag": 2,
        "ran_al_harif": 14,
    }
    # Every cluster in every page_clusters source has a non-empty toc_page
    # and link_prefix -- the frozen shape load_source_config enforces.
    for source in clustered:
        for cluster in source["page_clusters"]:
            assert cluster["toc_page"]
            assert cluster["link_prefix"]


def test_v4_2_map_public_first_exclude_pages_resolve_named_collisions():
    """The three Tur works and torat_haadam each name the exact collision/
    unwritten page the live probe identified -- pinned verbatim so a future
    edit cannot silently drop or rename the exclusion."""
    config = load_source_config(V4_2_MAP)
    by_key = {s["key"]: s for s in config["sources"]}
    assert by_key["tur_even_haezer"]["exclude_pages"] == ["טור אבן העזר הקדמה"]
    assert by_key["tur_choshen_mishpat"]["exclude_pages"] == ["טור חושן משפט שד"]
    assert by_key["tur_yoreh_deah"]["exclude_pages"] == ["טור יורה דעה שד"]
    assert by_key["torat_haadam"]["exclude_pages"] == ["תורת האדם/שער הסוף"]


def test_v4_2_map_public_first_sources_have_no_daf_bavli_grain():
    # daf_bavli assumes Sefaria's ordinal geometry (C8) -- no public_first
    # source in this map may declare it; the two daf shapes here are
    # "daf" (Zohar) and "section" (everything else with a meaningful
    # fetched-title label).
    config = load_source_config(V4_2_MAP)
    public_first = [
        s for s in config["sources"] if s.get("identity_mode") == "public_first"
    ]
    assert not any(s.get("locus_grain") == "daf_bavli" for s in public_first)


# --------------------------------------------------------------------------
# 7. Existing-map regression
# --------------------------------------------------------------------------


def test_v4_and_v4_1_maps_still_load_unaffected():
    v4 = load_source_config(V4_MAP)
    v4_1 = load_source_config(V4_1_MAP)
    assert reference_namespace(v4) == "REF4"
    assert reference_namespace(v4_1) == "REF5"
    assert len(v4_1["sources"]) == 10
    assert source_target_ids(v4) & source_target_ids(v4_1) == set()
