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
