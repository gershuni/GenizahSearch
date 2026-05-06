"""Phase 84 Plan 05: integration regression guard for shelfmark_bridge."""
import csv
import hashlib
import inspect
import json
from pathlib import Path

import pytest

from shared.shelfmark_bridge import lookup_cudl
from genizah_core import normalize_shelfmark


FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN = FIXTURES / "cudl_must_resolve.csv"
BASELINE = FIXTURES / "cudl_baseline_resolved.csv"
SNAPSHOT = FIXTURES / "normalize_shelfmark_snapshot.json"


@pytest.fixture(scope="module")
def alias_index_built():
    """Construct MetadataManager AND explicitly call _load_csv_bank() so csv_bank is
    populated and the alias index is built.

    Round 3 Codex HIGH #3: MetadataManager.__init__ does NOT auto-load csv_bank.
    Heavy cache normally loads in a background thread via start_background_loading().
    Tests cannot rely on that timing — call _load_csv_bank() directly here so the
    integration tests run against a populated csv_bank and a built alias index.
    """
    from genizah_core import MetadataManager
    try:
        mm = MetadataManager()
        mm._load_csv_bank()  # explicit — see Round 3 Codex HIGH #3
        if len(mm.csv_bank) < 100000:
            pytest.skip(f"csv_bank only loaded {len(mm.csv_bank)} rows; libraries.csv may be missing")
        # _load_csv_bank calls build_alias_index internally (Plan 04 wiring).
        # If the alias index is still empty, build it explicitly.
        from shared.shelfmark_bridge import build_alias_index, _CUDL_ALIAS_INDEX
        if not _CUDL_ALIAS_INDEX:
            build_alias_index(mm.csv_bank)
    except Exception as e:
        pytest.skip(f"MetadataManager() unavailable in this env: {e}")
    return mm


def _load_fixture():
    if not GOLDEN.exists():
        return []
    with GOLDEN.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@pytest.mark.parametrize("row", _load_fixture(),
                         ids=lambda r: r["cudl_classmark"] if r else "no-fixture")
def test_cudl_must_resolve(row, alias_index_built):
    result = lookup_cudl(row["cudl_classmark"])
    assert result is not None, (
        f"{row['cudl_classmark']} ({row['category']}) failed to resolve. "
        f"Notes: {row.get('notes')}"
    )
    expected_sub = (row.get("expected_shelfmark_substring") or "").strip()
    if expected_sub:
        assert expected_sub.lower() in (result["shelfmark"] or "").lower(), (
            f"Resolved sys_id={result['sys_id']} shelfmark={result['shelfmark']!r} "
            f"does not contain expected substring {expected_sub!r}"
        )
    expected_sid = (row.get("expected_sys_id") or "").strip()
    if expected_sid:
        assert result["sys_id"] == expected_sid, (
            f"Expected sys_id {expected_sid}, got {result['sys_id']}"
        )


class TestCanonicalNormalizerUnchanged:
    """D-09.3 + Codex suggestion #12 — assert function source AND outputs are unchanged."""

    @pytest.fixture(scope="class")
    def snapshot(self):
        if not SNAPSHOT.exists():
            pytest.skip(f"snapshot not generated: {SNAPSHOT}")
        with SNAPSHOT.open("r", encoding="utf-8") as f:
            return json.load(f)

    def test_source_sha256_unchanged(self, snapshot):
        src = inspect.getsource(normalize_shelfmark)
        current = hashlib.sha256(src.encode("utf-8")).hexdigest()
        assert current == snapshot["source_sha256"], (
            "normalize_shelfmark() source has changed. D-02 violated. "
            "If this change was intentional, regenerate the snapshot via "
            "running python scripts/build_cudl_fixture.py and re-capturing the hash, "
            "then confirm with the user."
        )

    @pytest.mark.parametrize("case", [
        "MS. Heb. a.1", "MS. Heb. b.10/26",
        "Yevr. III B 1093", "Antonin 906",
        "ENA-MS 2956", "ENA NS 1.4",
        "Heb. e. 30/41", "Halper 331",
        "AIU IX A 1", "Gaster 86",
    ])
    def test_output_unchanged(self, snapshot, case):
        expected = snapshot["cases"][case]
        assert normalize_shelfmark(case) == expected, (
            f"normalize_shelfmark({case!r}) output changed. D-02 violated."
        )


class TestScanDiffBaselineStillResolves:
    """Codex HIGH #1 + Round 3 Codex HIGH #5 — every previously-resolved CUDL
    shelfmark STILL resolves to the SAME manifest URL under the new bridge runtime.
    """

    @pytest.fixture(scope="class")
    def baseline(self):
        if not BASELINE.exists():
            pytest.skip(f"baseline not generated: {BASELINE}")
        with BASELINE.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    @pytest.fixture(scope="class")
    def nli_svc(self):
        """NliCrossrefService with auto-detected db path (worktree-aware)."""
        from shared.nli_crossref_service import NliCrossrefService
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        candidates = [
            root / "nli_data" / "nli_crossref.db",
            root.parent.parent / "nli_data" / "nli_crossref.db",
        ]
        # Walk upward to find the db in any ancestor (worktree support)
        cur = root.parent
        for _ in range(6):
            candidates.append(cur / "nli_data" / "nli_crossref.db")
            cur = cur.parent
        db_path = next((str(p) for p in candidates if p.exists()), None)
        if not db_path:
            pytest.skip("nli_crossref.db not found; skipping scan-diff test")
        return NliCrossrefService(db_path=db_path)

    def test_baseline_shelfmarks_still_resolve_to_same_url(self, baseline, alias_index_built, nli_svc):
        """Round 3 Codex HIGH #5: assert URL EQUALITY, not non-None.

        Previous version: `if not url: failures.append(...)` — accepted any non-None
        result, so a wrong-routing regression where the bridge returns a DIFFERENT
        manifest URL would pass silently. The "no silent misrouting" guarantee
        requires byte-equal URL match against the captured baseline.

        Schema: original_shelfmark, pre_phase_lookup_key, manifest_url.

        Codex HIGH #5 + Gemini LOW: NO sampling. The full baseline must pass.
        """
        failures = []
        for row in baseline:  # FULL baseline, no slice
            sm = row["original_shelfmark"]
            expected_url = row["manifest_url"]
            actual_url = nli_svc.get_cambridge_manifest_with_bridge(sm)
            if actual_url != expected_url:
                failures.append((sm, expected_url, actual_url))
        assert not failures, (
            f"{len(failures)}/{len(baseline)} baseline shelfmarks regressed "
            f"(missing OR mis-routed). Examples: {failures[:5]}. "
            "The new bridge runtime must return the SAME manifest URL for every "
            "shelfmark that was resolvable pre-phase. URL equality is required to "
            "defend against silent misrouting (Round 3 Codex HIGH #5)."
        )


class TestImageSourceInfoBridgeFallback:
    """Phase 84 follow-up: get_image_sources() must reflect Cambridge availability
    for CUDL-form classmarks resolved via the bridge.

    Pre-fix: get_image_sources did a single direct query against
    cambridge_manifests.normalized_shelfmark using the CANONICAL normalize_shelfmark()
    output. CUDL-form classmarks (slash, comma, leading-zero, Mosseri-label,
    Or.-numeric-collapse) miss that query — even when get_cambridge_manifest_with_bridge
    DOES resolve a manifest. Result: browse `_has_cambridge` flag stayed False and the
    Cambridge button + IIIF images were suppressed despite a valid manifest URL.

    Post-fix: when the direct query misses, the service falls through to
    get_cambridge_manifest_with_bridge(shelfmark) and flips cambridge=True iff a
    URL is reachable. UAT Test 2 (Mosseri) and Test 1 sub-issue 1c (T-S F 8/002)
    are blocked on this behavior.
    """

    @pytest.fixture(scope="class")
    def nli_svc(self):
        from shared.nli_crossref_service import NliCrossrefService
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        candidates = [
            root / "nli_data" / "nli_crossref.db",
            root.parent.parent / "nli_data" / "nli_crossref.db",
        ]
        cur = root.parent
        for _ in range(6):
            candidates.append(cur / "nli_data" / "nli_crossref.db")
            cur = cur.parent
        db_path = next((str(p) for p in candidates if p.exists()), None)
        if not db_path:
            pytest.skip("nli_crossref.db not found")
        return NliCrossrefService(db_path=db_path)

    def test_mosseri_label_form_lifts_cambridge_flag_via_bridge(self, nli_svc):
        """Mosseri row: canonical norm gives 'iii27a', no direct cambridge_manifests
        match. Bridge resolves via Tier 3 (Mosseri label MS-MOSSERI-III-00027-A)."""
        from genizah_core import normalize_shelfmark
        sm = "Ms. III 27A"
        sid = "990053834880205171"
        norm = normalize_shelfmark(sm)
        pre = nli_svc.get_image_sources(sid, normalized_shelfmark=norm)
        post = nli_svc.get_image_sources(sid, normalized_shelfmark=norm, shelfmark=sm)
        assert pre["cambridge"] is False, "regression test premise broken"
        assert post["cambridge"] is True, (
            "Phase 84 follow-up regressed: bridge fallback in get_image_sources "
            "no longer detects Mosseri Cambridge availability. "
            "Browse will hide the Cambridge button + images."
        )

    def test_ts_slash_leading_zero_form_lifts_cambridge_flag_via_bridge(self, nli_svc):
        """T-S F 8/002 row: canonical norm gives 'tsf8.002', no direct match.
        Bridge resolves via Tier 2 (cudl_normalize -> 'tsf8.2' which IS in the table)."""
        from genizah_core import normalize_shelfmark
        sm = "Ms. T-S F 8/002"
        sid = "990026242400205171"
        norm = normalize_shelfmark(sm)
        pre = nli_svc.get_image_sources(sid, normalized_shelfmark=norm)
        post = nli_svc.get_image_sources(sid, normalized_shelfmark=norm, shelfmark=sm)
        assert pre["cambridge"] is False, "regression test premise broken"
        assert post["cambridge"] is True, (
            "Phase 84 follow-up regressed: bridge fallback in get_image_sources "
            "no longer detects CUDL-form (slash + leading-zero) Cambridge availability."
        )

    def test_canonical_form_unchanged(self, nli_svc):
        """Or. 1080 J 15 already matched the direct query pre-fix (canonical
        and CUDL forms collide for this shape). Verify the bridge fallback is a
        pure ADDITION that doesn't alter pre-fix behavior for canonical-matching
        shelfmarks."""
        from genizah_core import normalize_shelfmark
        sm = "Or. 1080 J 15"
        norm = normalize_shelfmark(sm)
        pre = nli_svc.get_image_sources("synthetic", normalized_shelfmark=norm)
        post = nli_svc.get_image_sources("synthetic", normalized_shelfmark=norm, shelfmark=sm)
        # Both should detect Cambridge — the direct query already hits, the bridge
        # short-circuits (inside the `if not result["cambridge"]` guard).
        assert pre["cambridge"] is True
        assert post["cambridge"] is True
