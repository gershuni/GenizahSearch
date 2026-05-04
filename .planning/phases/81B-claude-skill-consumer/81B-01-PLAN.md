---
phase: 81B
plan: 01
type: execute
wave: 0
depends_on: []
files_modified:
  - skills/cairo-genizah-research/scripts/__init__.py
  - skills/cairo-genizah-research/scripts/fixtures/__init__.py
  - skills/cairo-genizah-research/scripts/fixtures/search_response.json
  - skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json
  - skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json
  - skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json
  - skills/cairo-genizah-research/scripts/fixtures/parallels_response.json
  - skills/cairo-genizah-research/scripts/fixtures/error_rate_limited.json
  - skills/cairo-genizah-research/state/.gitkeep
  - tests/test_skill_consumer.py
  - tests/test_skill_throttle.py
  - tests/test_skill_smoke.py
  - tests/conftest_skill.py
autonomous: true
requirements: [SKILL-01, SKILL-04, SKILL-05, SKILL-06]
tags: [skill, anthropic-skill, wave-0, tdd, fixtures]
must_haves:
  truths:
    - "Pytest collects all skill test files without import error"
    - "test_skill_consumer.py asserts honesty-annotation behavior (SKILL-04), known-witness flag/exclude (SKILL-05), and merge-by-uid (SKILL-02 prep) — all RED until Plan 02/03 land"
    - "test_skill_throttle.py asserts token-bucket math + file-lock persistence + per-bucket isolation — RED until Plan 02 lands"
    - "test_skill_smoke.py is gated by SKILL_SMOKE=1 env var; skips by default"
    - "Fixture JSON corpus contains representative search/browse/parallels envelopes from the locked Phase 77/79/80 contract"
  artifacts:
    - path: "skills/cairo-genizah-research/scripts/fixtures/search_response.json"
      provides: "Locked /api/search envelope shape for offline testing"
      contains: "schema_version"
    - path: "skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json"
      provides: "Browse response with text_source=pgp_transcription (the 'full' mapping per R2)"
      contains: "pgp_transcription"
    - path: "skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json"
      provides: "Browse response with text_source=snippet for honesty annotation test"
      contains: "snippet"
    - path: "skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json"
      provides: "Browse response with image.url=null and image.sources=[] for image-unavailable test"
      contains: "\"url\": null"
    - path: "tests/test_skill_consumer.py"
      provides: "RED tests for honesty annotation (SKILL-04), known-witness policy (SKILL-05), merge-by-uid (SKILL-02)"
      min_lines: 80
    - path: "tests/test_skill_throttle.py"
      provides: "RED tests for token-bucket math + file lock + per-bucket isolation (SKILL-06)"
      min_lines: 60
    - path: "tests/test_skill_smoke.py"
      provides: "Live smoke harness skipped unless SKILL_SMOKE=1"
      contains: "SKILL_SMOKE"
  key_links:
    - from: "tests/test_skill_consumer.py"
      to: "skills.cairo_genizah_research.scripts.format_output"
      via: "import (will fail RED until Plan 03)"
      pattern: "from skills"
    - from: "tests/test_skill_throttle.py"
      to: "skills.cairo_genizah_research.scripts.throttle"
      via: "import (will fail RED until Plan 02)"
      pattern: "from skills"
---

<objective>
Wave 0 RED scaffolding for Phase 81B Claude Skill Consumer. Establishes the skill source-tree under `skills/cairo-genizah-research/` (in-repo dev location; the installed skill is external per D-02), drops fixture corpus capturing the locked Phase 77/79/80 envelope shapes, and authors RED test files in `tests/` that exercise SKILL-04 (honesty annotations), SKILL-05 (known-witness policy), and SKILL-06 (throttle persistence). Tests fail with `ModuleNotFoundError` until Plan 02 (transport + throttle) and Plan 03 (business logic) land.

Purpose: Without Wave 0, Plans 02–03 have no executable contract. Per VALIDATION.md sampling rule (no 3 consecutive tasks without automated verify), every downstream task must have a test it flips GREEN — those tests live here.

Output: 14 files created (3 `__init__.py`, 6 fixtures, 1 `.gitkeep`, 4 test modules). Zero source code yet.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md
@.planning/phases/81B-claude-skill-consumer/81B-VALIDATION.md
@.planning/phases/79-api-browse-drill-down/79-CONTEXT.md
@.planning/phases/77-serializer-json-export/77-CONTEXT.md
@.planning/phases/80-api-parallels/80-CONTEXT.md
@shared/search_serializer.py
@shared/api_errors.py
@CLAUDE.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: Create skill source-tree skeleton + fixture corpus</name>
  <files>skills/cairo-genizah-research/scripts/__init__.py, skills/cairo-genizah-research/scripts/fixtures/__init__.py, skills/cairo-genizah-research/scripts/fixtures/search_response.json, skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json, skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json, skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json, skills/cairo-genizah-research/scripts/fixtures/parallels_response.json, skills/cairo-genizah-research/scripts/fixtures/error_rate_limited.json, skills/cairo-genizah-research/state/.gitkeep</files>
  <read_first>
    - shared/search_serializer.py (canonical envelope shape; for `schema_version: 1`, `source`, `count`, `total`, `warnings`, `request`, `results[*].uid`, `results[*].locator`)
    - .planning/phases/79-api-browse-drill-down/79-CONTEXT.md (D-10 text_source enum; D-14 image best-effort; D-16 partial-failure warnings)
    - .planning/phases/80-api-parallels/80-CONTEXT.md (D-04 filtered-key always present; D-07 truncated_to_200 warning; D-09 mode property values)
    - .planning/phases/77-serializer-json-export/77-CONTEXT.md (D-13 matches[]; D-04 locator both-fields-always-populated)
    - shared/api_errors.py (error code list including `rate_limited`, `manuscript_page_not_found`, `core_timeout`)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (R2: text_source enum is `pgp_transcription | snippet | none` — NO 'full' value)
  </read_first>
  <action>
    Create the skill source-tree skeleton at `skills/cairo-genizah-research/`. This is the in-repo authoring location (D-02 says installed skill lives external; this directory is the dev source that gets copied to `~/.claude/skills/cairo-genizah-research/` for the live acceptance run).

    1. **Empty `__init__.py` files** at `skills/cairo-genizah-research/scripts/__init__.py` and `skills/cairo-genizah-research/scripts/fixtures/__init__.py` (single-line file with the comment `# Cairo Genizah Research skill — see SKILL.md`). These let the test files import via `skills.cairo_genizah_research.scripts.*` paths.

    2. **`state/.gitkeep`** — empty file. The `state/` dir holds throttle JSON (Plan 02 owns) — the .gitkeep keeps the dir present so `state/throttle.json` writes don't `FileNotFoundError`.

    3. **`fixtures/search_response.json`** — minimal valid `/api/search` envelope per Phase 77 D-13 / 81A request-echo:
    ```json
    {
      "schema_version": 1,
      "source": "search",
      "generated_at": "2026-05-04T12:00:00.000Z",
      "request": {
        "search_mode": "exact",
        "query": "ויאמר משה",
        "gap": 0,
        "limit": 10,
        "filters": {}
      },
      "count": 2,
      "total": 2,
      "warnings": [],
      "results": [
        {
          "uid": "990001234560205171_001r",
          "locator": {"sys_id": "990001234560205171", "volume_ie": "IE12345", "p_num": 1, "fl_id": "T-S 12.123.1r"},
          "score": 0.8731,
          "shelfmark": "T-S 12.123",
          "title": "Liturgical fragment",
          "snippet": "...ויאמר משה אל בני ישראל...",
          "excerpt": "...ויאמר משה אל בני ישראל...",
          "metadata": {"library": "CUL", "library_name": "Cambridge University Library", "domains": ["Liturgy"], "dating": "11th century"},
          "image_url": "/api/nli_image_by_sysid/990001234560205171?page=0"
        },
        {
          "uid": "990009876540205171_002v",
          "locator": {"sys_id": "990009876540205171", "volume_ie": "IE99999", "p_num": 2, "fl_id": "ENA 1234.2v"},
          "score": 0.6512,
          "shelfmark": "ENA 1234",
          "title": "Bible commentary",
          "snippet": "...ויאמר משה...",
          "excerpt": "...ויאמר משה...",
          "metadata": {"library": "JTS", "library_name": "Jewish Theological Seminary", "domains": ["Bible"], "dating": "12th century"},
          "image_url": "/api/nli_image_by_sysid/990009876540205171?page=1"
        }
      ]
    }
    ```

    4. **`fixtures/browse_pgp_full.json`** — browse with full PGP transcription (the value SKILL-04 R2-mapping treats as "full"):
    ```json
    {
      "schema_version": 1,
      "source": "browse",
      "generated_at": "2026-05-04T12:00:01.000Z",
      "locator": {"uid": "990001234560205171_001r", "sys_id": "990001234560205171", "volume_ie": "IE12345", "p_num": 1, "fl_id": "T-S 12.123.1r"},
      "shelfmark": "T-S 12.123",
      "title": "Liturgical fragment",
      "library_code": "CUL",
      "library_name": "Cambridge University Library",
      "text": "ויאמר משה אל בני ישראל ראו קרא ה' בשם בצלאל בן אורי בן חור למטה יהודה",
      "text_source": "pgp_transcription",
      "text_truncated": false,
      "metadata": {"pgp": {"pgpid": 12345, "description": "Piyyut for Shabbat"}, "fjms": null, "nli": {"manifest_id": "abc"}},
      "image": {"url": "/api/nli_image_by_sysid/990001234560205171?page=0", "provider": "NLI", "sources": [{"label": "NLI", "url": "/api/nli_image_by_sysid/990001234560205171?page=0"}]},
      "warnings": []
    }
    ```

    5. **`fixtures/browse_snippet.json`** — browse with `text_source: "snippet"` for SKILL-04 honesty test. Same shape as #4 but `text_source: "snippet"`, `text` is a 120-char snippet, `metadata.pgp: null`, `warnings: [{"code": "enrichment_timeout", "source": "pgp", "message": "PGP enrichment timed out after 1.0s"}]`.

    6. **`fixtures/browse_no_image.json`** — browse with `image: {"url": null, "provider": null, "sources": []}` (Oxford-only fragment). `text_source: "pgp_transcription"`, image-unavailable triggers honesty annotation.

    7. **`fixtures/parallels_response.json`** — `/api/parallels` envelope per Phase 80 D-04/D-08. Top-level `source: "parallels"`, `request: {"text": "...", "chunk_size": 5, "mode": "exact"}`, `results` = list of groups each with `uid`, `locator`, `aggregate_score`, `matches: [{chunk_index, score, source_chunk_text, manuscript_snippet}]`. Include `filtered: []` (D-04 always present).

    8. **`fixtures/error_rate_limited.json`** — error envelope per `shared/api_errors.py`:
    ```json
    {"error": {"code": "rate_limited", "message": "Rate limit exceeded. Try again in 12 seconds."}}
    ```

    All JSON files: 2-space indent; UTF-8; Hebrew literals stored as raw UTF-8 (NOT escaped \uXXXX). Use realistic but synthetic sys_id/uid values — these are test fixtures, not production data.
  </action>
  <verify>
    <automated>python -c "import json, pathlib; root = pathlib.Path('skills/cairo-genizah-research/scripts/fixtures'); files = ['search_response.json', 'browse_pgp_full.json', 'browse_snippet.json', 'browse_no_image.json', 'parallels_response.json', 'error_rate_limited.json']; [json.loads((root / f).read_text(encoding='utf-8')) for f in files]; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File `skills/cairo-genizah-research/scripts/__init__.py` exists.
    - File `skills/cairo-genizah-research/scripts/fixtures/__init__.py` exists.
    - File `skills/cairo-genizah-research/state/.gitkeep` exists.
    - All 6 fixture JSON files parse as valid JSON (verify command above prints `OK`).
    - `grep -l "pgp_transcription" skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json` returns the file.
    - `grep -l "\"text_source\": \"snippet\"" skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json` returns the file.
    - `grep -l "\"url\": null" skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json` returns the file.
    - `grep -l "schema_version" skills/cairo-genizah-research/scripts/fixtures/search_response.json` returns the file.
    - `grep -l "matches" skills/cairo-genizah-research/scripts/fixtures/parallels_response.json` returns the file.
    - `grep -l "rate_limited" skills/cairo-genizah-research/scripts/fixtures/error_rate_limited.json` returns the file.
  </acceptance_criteria>
  <done>Skill source-tree skeleton + fixture corpus committed. Plan 02/03 transport and business logic can import fixtures by relative path; tests can load them without network.</done>
</task>

<task type="auto">
  <name>Task 2: Author RED test scaffolds for SKILL-04, SKILL-05, SKILL-02</name>
  <files>tests/test_skill_consumer.py, tests/conftest_skill.py</files>
  <read_first>
    - skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json (just-created fixture)
    - skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json
    - skills/cairo-genizah-research/scripts/fixtures/search_response.json
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§4 honesty annotation logic; §5 known_witnesses policy; §3 merge-by-uid)
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (D-05 hybrid ranking; D-11 result-shape handling)
    - tests/conftest.py (existing project conftest patterns; do not modify)
  </read_first>
  <action>
    Create `tests/conftest_skill.py` with shared fixture loaders, then `tests/test_skill_consumer.py` with RED tests covering SKILL-04 (honesty annotations), SKILL-05 (known-witness flag/exclude), and SKILL-02 (merge-by-uid staged discovery).

    **`tests/conftest_skill.py`** — module-level helpers (NOT a pytest conftest.py; project conftest.py is untouched):
    ```python
    """Shared loaders for Phase 81B skill tests. Imported explicitly by test files."""
    import json
    from pathlib import Path

    FIXTURE_DIR = Path(__file__).parent.parent / "skills" / "cairo-genizah-research" / "scripts" / "fixtures"

    def load_fixture(name: str) -> dict:
        return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
    ```

    **`tests/test_skill_consumer.py`** — RED tests. All `from skills.cairo_genizah_research.scripts.{format_output,stage,normalize_shelfmark} import ...` lines fail at collection time with `ModuleNotFoundError` (Plan 03 owns those modules) — this is the intended RED state.

    Required test functions (exact names — Plan 03 acceptance grep depends on them):

    1. `test_honesty_annotation_pgp_full_returns_empty()` — loads `browse_pgp_full.json`, calls `honesty_annotation(browse)`, asserts result `== ""`.

    2. `test_honesty_annotation_snippet_appends_text_warning()` — loads `browse_snippet.json`, asserts `"(full text unavailable; based on snippet of" in honesty_annotation(browse)`.

    3. `test_honesty_annotation_includes_char_count()` — loads `browse_snippet.json`; the char count in the annotation `"snippet of N chars"` must equal `len(browse["text"])`.

    4. `test_honesty_annotation_no_image_appends_image_warning()` — loads `browse_no_image.json`, asserts `"(no image available)" in honesty_annotation(browse)`.

    5. `test_honesty_annotation_text_source_none_treated_as_not_full()` — synthesizes `{"text_source": "none", "text": "", "image": {"url": "/x", "sources": []}}`, asserts `"(full text unavailable" in result`.

    6. `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2()` — loads `browse_pgp_full.json` (text_source=pgp_transcription), asserts NO `"full text unavailable"` substring in annotation. **Locks the R2 mapping decision** (REQUIREMENTS.md SKILL-04 says `!= 'full'`; Phase 79 D-10 enum has no `'full'` value; skill maps `pgp_transcription` → "full" per planner decision).

    7. `test_apply_known_witness_policy_flag_marks_known()` — given candidates with `uid` values `["U1", "U2", "U3"]` and `known_uids = {"U2"}`, policy=`"flag"`: returns 3 candidates, `c[1]["known_witness"] is True`, `c[0]["known_witness"] is False`, `c[2]["known_witness"] is False`.

    8. `test_apply_known_witness_policy_exclude_drops_known()` — same input, policy=`"exclude"`: returns 2 candidates, no `uid == "U2"` in result.

    9. `test_apply_known_witness_policy_unknown_raises_valueerror()` — policy=`"foo"` raises `ValueError`.

    10. `test_normalize_shelfmark_collapses_whitespace()` — `normalize("T-S  12.123")` `== normalize("T-S 12.123")`.

    11. `test_normalize_shelfmark_strips_ms_prefix()` — `normalize("MS T-S 12.123")` `== normalize("T-S 12.123")`.

    12. `test_normalize_shelfmark_idempotent()` — `normalize(normalize(s)) == normalize(s)` for `s in ["T-S 12.123", "ENA-MS 1234", "MS Heb c 57"]`.

    13. `test_merge_by_uid_aggregates_phrase_count()` — given 3 per-phrase result lists where uid `"U1"` appears in 2 of them and `"U2"` in 1, `merge_results([...])` returns 2 candidates, `U1` has `_phrase_count == 2`, `U2` has `_phrase_count == 1`.

    14. `test_merge_by_uid_assigns_tier_a_for_3plus_phrases()` — uid appearing in 3+ phrase results → `_tier == "A"`; in 2 → `"B"`; in 1 → `"C"`.

    15. `test_merge_by_uid_sorts_by_phrase_count_desc_then_score()` — first item has highest `_phrase_count`; ties broken by descending `score`.

    File starts with imports:
    ```python
    import pytest
    from tests.conftest_skill import load_fixture
    from skills.cairo_genizah_research.scripts.format_output import honesty_annotation, apply_known_witness_policy
    from skills.cairo_genizah_research.scripts.normalize_shelfmark import normalize
    from skills.cairo_genizah_research.scripts.stage import merge_results
    ```

    These imports fail with `ModuleNotFoundError` until Plan 03. That's RED-by-design.
  </action>
  <verify>
    <automated>pytest tests/test_skill_consumer.py --collect-only 2>&1 | grep -E "(ModuleNotFoundError|errors during collection)" | head -5</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/conftest_skill.py` exists with `load_fixture` function.
    - File `tests/test_skill_consumer.py` exists.
    - `grep -c "^def test_" tests/test_skill_consumer.py` returns `15`.
    - All 15 test names from Action are present (verify each: `grep "test_honesty_annotation_pgp_full_returns_empty" tests/test_skill_consumer.py` etc.).
    - `pytest tests/test_skill_consumer.py --collect-only` exits non-zero with `ModuleNotFoundError` referencing `skills.cairo_genizah_research.scripts.format_output` (intended RED).
    - `grep -c "from skills.cairo_genizah_research.scripts" tests/test_skill_consumer.py` returns `>= 3`.
    - `grep "R2" tests/test_skill_consumer.py` returns at least one line (the R2 mapping lock test references it).
  </acceptance_criteria>
  <done>15 RED tests collected (collection fails on import, which is the intended RED state). Plan 03 will land `format_output.py`, `normalize_shelfmark.py`, `stage.py` to flip them GREEN.</done>
</task>

<task type="auto">
  <name>Task 3: Author RED test scaffolds for SKILL-06 throttle + live smoke harness</name>
  <files>tests/test_skill_throttle.py, tests/test_skill_smoke.py</files>
  <read_first>
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§6 throttle implementation; verification math: 15 search + 10 browse ≈ 50s)
    - .planning/phases/81B-claude-skill-consumer/81B-VALIDATION.md (smoke gating via SKILL_SMOKE=1)
    - skills/cairo-genizah-research/scripts/fixtures/search_response.json
  </read_first>
  <action>
    **`tests/test_skill_throttle.py`** — RED tests for the token-bucket throttle (SKILL-06). All `from skills.cairo_genizah_research.scripts.throttle import acquire, _read_state, _write_state` imports fail until Plan 02.

    Required test functions:

    1. `test_throttle_first_call_does_not_block(tmp_path, monkeypatch)` — sets `CAIRO_GENIZAH_STATE_DIR` to `tmp_path`; calls `acquire("search", rpm=24, burst=5)`; asserts `wait < 0.1` (first call uses initial burst tokens).

    2. `test_throttle_burst_5_then_blocks(tmp_path, monkeypatch)` — calls `acquire("search", rpm=24, burst=5)` six times in a row with a fake clock (monkeypatch `time.time`); asserts call #6 returns `wait >= 60/24 - 0.1` (i.e., must wait ~2.5s after burst exhausted).

    3. `test_throttle_buckets_are_isolated(tmp_path, monkeypatch)` — exhausts `"search"` bucket (5 acquires), then a single `acquire("browse", ...)` returns `wait < 0.1` (different bucket, fresh tokens).

    4. `test_throttle_state_persists_across_processes(tmp_path, monkeypatch)` — call `acquire("search")`, read state file, call `acquire("search")` in a fresh import (simulate via `importlib.reload`), assert token count decreased monotonically across the two calls.

    5. `test_throttle_15_search_plus_10_browse_completes_under_60_seconds(tmp_path, monkeypatch)` — fake clock; 15 calls to `acquire("search")` then 10 to `acquire("browse")`; asserts total simulated wall-clock advance `<= 60.0` seconds. This is the SKILL-06 verification math test.

    6. `test_throttle_handles_corrupt_state_file(tmp_path, monkeypatch)` — write garbage to `state/throttle.json`; `acquire(...)` does NOT raise; recovers by treating state as empty.

    7. `test_throttle_env_override_lowers_rpm(tmp_path, monkeypatch)` — set `GENIZAH_SKILL_REQ_PER_MIN=12`; assert effective rpm is 12 (call frequency halved vs default 24).

    Test scaffold uses `monkeypatch.setattr("time.time", fake_clock)` pattern with a closure-incrementing fake clock — never `time.sleep` in tests (would slow CI).

    **`tests/test_skill_smoke.py`** — live smoke harness, gated by env var:

    ```python
    """Live smoke test for the cairo-genizah-research skill.

    Skipped by default. To run: SKILL_SMOKE=1 pytest tests/test_skill_smoke.py -v
    Hits the production deployment (or override via GENIZAH_API_BASE).
    """
    import os
    import pytest

    pytestmark = pytest.mark.skipif(
        os.environ.get("SKILL_SMOKE") != "1",
        reason="Live smoke test — set SKILL_SMOKE=1 to run",
    )

    def test_smoke_search_endpoint_returns_envelope():
        """Hit /api/search?search_mode=exact&query=test against live deployment."""
        from skills.cairo_genizah_research.scripts.search import call_search
        result = call_search(query="ויאמר", search_mode="exact", limit=5)
        assert result.get("schema_version") == 1
        assert result.get("source") == "search"
        assert "results" in result
        assert isinstance(result["results"], list)

    def test_smoke_browse_endpoint_round_trips_locator():
        """Search → browse round-trip on a single result."""
        from skills.cairo_genizah_research.scripts.search import call_search
        from skills.cairo_genizah_research.scripts.browse import call_browse
        s = call_search(query="ויאמר", search_mode="exact", limit=1)
        if not s["results"]:
            pytest.skip("Live search returned 0 results — try a different query")
        first = s["results"][0]
        b = call_browse(uid=first["uid"])
        assert b["locator"]["sys_id"] == first["locator"]["sys_id"]
        assert "text_source" in b
        assert b["text_source"] in {"pgp_transcription", "snippet", "none"}
    ```
  </action>
  <verify>
    <automated>pytest tests/test_skill_throttle.py tests/test_skill_smoke.py --collect-only 2>&1 | grep -E "(test_|skipped|ModuleNotFoundError)" | head -20</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/test_skill_throttle.py` exists.
    - File `tests/test_skill_smoke.py` exists.
    - `grep -c "^def test_" tests/test_skill_throttle.py` returns `7`.
    - `grep -c "^def test_" tests/test_skill_smoke.py` returns `2`.
    - `grep -l "SKILL_SMOKE" tests/test_skill_smoke.py` returns the file.
    - `grep -l "pytestmark" tests/test_skill_smoke.py` returns the file (skip-by-default mechanism).
    - `pytest tests/test_skill_throttle.py --collect-only` exits non-zero with `ModuleNotFoundError` referencing `skills.cairo_genizah_research.scripts.throttle` (intended RED).
    - `pytest tests/test_skill_smoke.py` collects but skips all tests when `SKILL_SMOKE` is unset (verify with env var unset: `SKILL_SMOKE= pytest tests/test_skill_smoke.py -v 2>&1 | grep -E "skipped"`).
    - `grep "test_throttle_15_search_plus_10_browse_completes_under_60_seconds" tests/test_skill_throttle.py` returns one line.
  </acceptance_criteria>
  <done>Throttle and live-smoke RED tests collected; smoke test skips by default. Plan 02 will land `throttle.py` + `search.py` + `browse.py` to flip throttle tests GREEN; live-smoke gates Plan 05 acceptance run.</done>
</task>

</tasks>

<threat_model>

## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Test fixtures → fixture loaders | Static JSON loaded from disk; no network; no untrusted input. Risk: malformed fixture causes test crash but no production impact. |
| Skill source tree → repo root | New `skills/` directory at repo root. Risk: accidental git-ignore of `state/throttle.json` later if `state/` matches a global pattern. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81B-01 | Tampering | Fixture JSON files | accept | Test-only artifacts; no runtime production read path. Fixtures are committed to git so tampering is reviewable in PR diff. |
| T-81B-02 | Information Disclosure | Fixture sys_id values | mitigate | Use synthetic sys_ids (e.g., `990001234560205171`) that don't match real production manuscripts; no real PII. Verified by grep against libraries.csv: synthetic values must NOT appear in real data. |
| T-81B-03 | Denial of Service | `tests/test_skill_smoke.py` against production | mitigate | Skipped by default via `SKILL_SMOKE` env var gate; only the user (or planner during Plan 05) explicitly opts in. Contract: smoke test makes ≤2 calls per run, well inside the 30 rpm server bucket. |

</threat_model>

<verification>
- `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py --collect-only` exits non-zero with `ModuleNotFoundError` (intended RED).
- `pytest tests/test_skill_smoke.py` collects 2 tests, both `skipped` when `SKILL_SMOKE` unset.
- `python -c "import json; [json.loads(open(f, encoding='utf-8').read()) for f in __import__('glob').glob('skills/cairo-genizah-research/scripts/fixtures/*.json')]"` exits 0.
- Project-wide test count rises by 24 (15 in test_skill_consumer + 7 in test_skill_throttle + 2 in test_skill_smoke). Pre-Plan 01 wider suite: 1465 passed / 15 skipped. Post-Plan 01: collection failure on the two RED files is expected and deliberate; targeted `pytest -k "not skill"` should still report 1465 passed / 15 skipped (no regression in non-skill tests).
</verification>

<success_criteria>
- 14 files created at the listed paths.
- Skill source tree skeleton (`skills/cairo-genizah-research/`) exists with `__init__.py` files allowing Python imports.
- 6 fixture JSON files validate as JSON and contain the locked envelope shapes.
- 24 RED test functions exist across 3 test files; collection fails with `ModuleNotFoundError` for the source tests, smoke tests skip cleanly when `SKILL_SMOKE` unset.
- Plan 02 has executable contracts (throttle tests) it will flip GREEN; Plan 03 has executable contracts (consumer tests) it will flip GREEN; Plan 05 has the live-smoke harness it will run during user-observed acceptance.
</success_criteria>

<output>
After completion, create `.planning/phases/81B-claude-skill-consumer/81B-01-SUMMARY.md` capturing:
- Files created, test counts, intended RED state
- Any deviations from the planned fixture shapes (e.g., if the locked envelope shape from `shared/search_serializer.py` had fields not anticipated in the plan)
- Confirmation that R2 (text_source mapping) is locked into `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2`
</output>
