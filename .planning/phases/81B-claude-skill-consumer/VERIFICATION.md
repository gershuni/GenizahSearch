---
phase: 81B-claude-skill-consumer
verified: 2026-05-05T00:00:00Z
status: passed
score: 6/6 requirements verified (22/22 skill tests GREEN + acceptance gate MET)
overrides_applied: 0
---

# Phase 81B: Claude Skill Consumer Verification Report

**Phase Goal:** A runnable Anthropic Skill (SKILL.md + scripts) drives `/api/search` → `/api/browse` end-to-end via staged phrase discovery, producing ranked candidate witnesses with justifications grounded in browse text and honest reporting of `text_source` and image-unavailability conditions. The v7.10 acceptance harness.

**Verified:** 2026-05-05
**Status:** VERIFIED PASSED (with user-acknowledged notes for v7.11+)
**Re-verification:** No — initial verification

---

## Step 0: Previous Verification

No prior VERIFICATION.md existed. Initial mode.

---

## Goal Achievement

### Observable Truths (from ROADMAP.md Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Skill base URL is configurable (env var or argument), defaults to production, runnable from clean checkout — filesystem location not pinned to repo path | VERIFIED | `_config.py` exports `resolve_base_url(cli_arg)` with env-wins D-09 precedence (`GENIZAH_API_BASE` > `--base-url` > default `https://genizahsearch.com`). `CAIRO_GENIZAH_STATE_DIR` override present. Skill installed to `~/.claude/skills/cairo-genizah-research/` at acceptance run time (user confirmed). |
| 2 | Running on a representative scholarly query produces a ranked candidate list with shelfmark, library, catalog title, tier (A/B/C), known-witness flag, matching phrases, justification grounded in browse text, browse URL, and image URL or `(no image available)` note | VERIFIED | Acceptance run executed 2026-05-05 with query "find letters in Judeo-Arabic mentioning הבחור". Top result T-S 8J41.1 (Tier A, CUL) with verbatim JA transcription quote. All SC-2 fields present in output. 16 browse calls succeeded, 0 failures. Signed off by Hillel Gershuni. |
| 3 | Script handles 429 / timeouts / partial `/api/browse` data without crashing; surfaces failures in plain terms; continues processing remaining candidates | VERIFIED | `unresolvable_filter_value` error encountered during acceptance run; skill recovered, corrected filter value, retried without crashing conversation. Error-code → inline-note table in SKILL.md covers all Phase 78/79/80 error codes. D-07 per-candidate inline note + continue logic present in `stage_search()`. 429 path tested in throttle unit tests (test_throttle_burst_5_then_blocks). |
| 4 | Browse honesty: when `text_source != 'pgp_transcription'`, appends `(full text unavailable; based on snippet of N chars)`; when image unavailable, appends `(no image available)` | VERIFIED | `_FULL_TEXT_SOURCE = "pgp_transcription"` constant in `format_output.py` (R2 mapping locked). 6 dedicated pytest tests all GREEN. Acceptance run confirmed: 2 Tier-A results had no annotation (text_source=pgp_transcription); 4 snippet-tier results had `(full text unavailable; based on snippet of N chars)`. REQUIREMENTS.md SKILL-04 patched to use actual Phase 79 D-10 enum value. |
| 5 | Optional `known_witnesses[]` + `known_witness_policy='flag'|'exclude'` (default `flag`). Two-tier shelfmark normalization: lightweight local (Tier 1) + `/api/search?search_mode=shelfmark` (Tier 2). Skill does NOT depend on `genizah_core`. | VERIFIED | `apply_known_witness_policy()` in `format_output.py` tested by 3 pytest tests (flag marks, exclude drops, unknown raises ValueError). `normalize_shelfmark.py` implements Tier-1 (NFKC + MS prefix strip + whitespace collapse + uppercase); 3 normalization tests GREEN. `grep -E "from (genizah_core|shared)" skills/cairo-genizah-research/scripts/*.py` returns 0 lines across all 11 scripts. |
| 6 | Token-bucket throttle with separate per-endpoint buckets, default ≤24 req/min, burst 5; 15 search + 10 browse calls completes without self-rate-limiting; state persists across process boundaries | VERIFIED | `throttle.py` implements JSON-backed token-bucket with platform-aware file lock (`msvcrt` on Windows, `fcntl` on Unix). 7 throttle tests GREEN including `test_throttle_15_search_plus_10_browse_completes_under_60_seconds`. `state/throttle.json` present with buckets `search`, `browse`, `parallels`. Acceptance run: 17 API calls, 0 rate-limit hits, ~30 seconds wall-clock. |

**Score:** 6/6 requirements verified

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `skills/cairo-genizah-research/SKILL.md` | Anthropic Skill instruction file; ≤500 lines; valid frontmatter | VERIFIED | 204 lines. `name: cairo-genizah-research` (24 chars, lowercase+hyphens, no 'anthropic'/'claude'). Description 625 chars (under 1024 cap). 9 H2 sections as model navigation anchors. |
| `skills/cairo-genizah-research/README.md` | Human-facing install/usage guide; ≥30 lines | VERIFIED | 91 lines. Installation (Claude Code + Desktop + NOT SUPPORTED for Claude API), smoke test, acceptance run procedure, architecture table. |
| `skills/cairo-genizah-research/references/api_contract.md` | Level-3 locked envelope shapes; ≥40 lines | VERIFIED | 155 lines. All three endpoint shapes. R2 mapping note. 6 occurrences of `search_mode`. 3 occurrences of `pgp_transcription`. Full error code catalogue. |
| `skills/cairo-genizah-research/scripts/_config.py` | resolve_base_url + env-wins D-09 + STATE_DIR | VERIFIED | `GENIZAH_API_BASE`, `CAIRO_GENIZAH_STATE_DIR`, `GENIZAH_SKILL_REQ_PER_MIN` all present. D-09 inversion documented inline. |
| `skills/cairo-genizah-research/scripts/_lock.py` | Cross-platform file lock (win32 + Unix) | VERIFIED | `lock_file` and `unlock_file` defined. `sys.platform == "win32"` branch for `msvcrt`; `else` branch for `fcntl`. |
| `skills/cairo-genizah-research/scripts/throttle.py` | Token-bucket acquire with JSON persistence + file lock | VERIFIED | `acquire()`, `_read_state()`, `_write_state()` present. Uses `time.time()` (correct for cross-process persistence; `time.monotonic()` resets per-process). `json.JSONDecodeError` guard for corrupt-state recovery. File lock wired. |
| `skills/cairo-genizah-research/scripts/search.py` | POST /api/search transport + CLI + throttle + Retry-After | VERIFIED | `call_search` exported. `search_mode` field used (not legacy `mode`). `throttle.acquire` present. `Retry-After` header surface. |
| `skills/cairo-genizah-research/scripts/browse.py` | GET /api/browse transport + CLI + throttle + uid preference | VERIFIED | `call_browse` exported. Throttle wired. uid + sys_id + fl_id validation present. |
| `skills/cairo-genizah-research/scripts/parallels.py` | POST /api/parallels transport + CLI + throttle | VERIFIED | `call_parallels` exported. Uses `mode` field (not `search_mode` — Phase 81A D-07). Throttle wired. |
| `skills/cairo-genizah-research/scripts/normalize_shelfmark.py` | Tier-1 shelfmark normalizer | VERIFIED | `normalize()` exported. NFKC + MS prefix + whitespace + uppercase. Idempotent. Zero `genizah_core`/`shared` imports. |
| `skills/cairo-genizah-research/scripts/format_output.py` | honesty_annotation + apply_known_witness_policy + render_markdown/json | VERIFIED | All four functions exported. `_FULL_TEXT_SOURCE = "pgp_transcription"` constant present with R2 mapping comment. SC-2 schema fields all rendered. Zero `genizah_core`/`shared` imports. |
| `skills/cairo-genizah-research/scripts/stage.py` | merge_results + stage_search + CLI | VERIFIED | Both functions exported. Tier A/B/C assignment. Sort by (-phrase_count, -score). Imports `call_search` from sibling module. |
| `skills/cairo-genizah-research/scripts/smoke_test.py` | Three-endpoint smoke harness | VERIFIED | 100 lines. `run_smoke()` defined. Exercises all three endpoints. Reports `OVERALL: PASS` or `OVERALL: FAIL`. |
| `skills/cairo-genizah-research/scripts/fixtures/*.json` | 6 fixture JSON files (locked Phase 77/79/80 envelope shapes) | VERIFIED | All 6 present and parse as valid JSON: search_response.json, browse_pgp_full.json, browse_snippet.json, browse_no_image.json, parallels_response.json, error_rate_limited.json. |
| `skills/cairo-genizah-research/state/throttle.json` | Persisted throttle state (3 buckets) | VERIFIED | Present on disk after acceptance run. Buckets: `search`, `browse`, `parallels`. |
| `.planning/REQUIREMENTS.md` | SKILL-04 patched with `pgp_transcription` enum value | VERIFIED | `text_source != 'full'` removed; `text_source != 'pgp_transcription'` present with R2 mapping attribution note. All 6 SKILL requirements still present. |
| `.planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` | Phase gate evidence; ≥30 lines; sign-off present | VERIFIED | 119 lines. Status: APPROVED WITH NOTES. Signed by Hillel Gershuni 2026-05-05. Gate result: MET for both ROADMAP phase-gate and CONTEXT D-12. |
| `tests/test_skill_consumer.py` | 15 GREEN tests for SKILL-04/05/02 | VERIFIED | 15/15 passed. Includes R2 lock test `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2`. |
| `tests/test_skill_throttle.py` | 7 GREEN tests for SKILL-06 | VERIFIED | 7/7 passed. Includes 15-search+10-browse-under-60s math test. |
| `tests/test_skill_smoke.py` | 2 live smoke tests gated by SKILL_SMOKE=1 | VERIFIED | 2/2 skipped when env var unset (by design). `pytestmark = pytest.mark.skipif(os.environ.get("SKILL_SMOKE") != "1", ...)` present. |
| `tests/conftest_skill.py` | `load_fixture()` helper for test fixtures | VERIFIED | Present. Loads from `skills/cairo-genizah-research/scripts/fixtures/`. |
| `tests/conftest.py` | Import bridge for hyphenated skill directory | VERIFIED | `_register_skill_package()` maps `skills/cairo-genizah-research/` (hyphens) to `skills.cairo_genizah_research` (underscores) using `types.ModuleType`. |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `tests/test_skill_consumer.py` | `skills.cairo_genizah_research.scripts.format_output` | import via conftest bridge | WIRED | `from skills.cairo_genizah_research.scripts.format_output import honesty_annotation, apply_known_witness_policy` — works via `tests/conftest.py` import bridge |
| `tests/test_skill_throttle.py` | `skills.cairo_genizah_research.scripts.throttle` | import via conftest bridge | WIRED | Tests import `acquire`, `_read_state`, `_write_state` cleanly |
| `stage.py` | `search.py` | `from .search import call_search` | WIRED | Fan-out loop calls `call_search()` per phrase |
| `search.py` | `throttle.py` | `throttle.acquire("search")` | WIRED | Called before every HTTP request |
| `browse.py` | `throttle.py` | `throttle.acquire("browse")` | WIRED | Called before every HTTP request |
| `parallels.py` | `throttle.py` | `throttle.acquire("parallels")` | WIRED | Called before every HTTP request |
| `throttle.py` | `state/throttle.json` | filesystem read/write under file lock | WIRED | Lock-then-read-modify-write-then-unlock; corrupt JSON recovers as empty state |
| `_config.py` | `GENIZAH_API_BASE` env var | `os.environ.get("GENIZAH_API_BASE")` | WIRED | env wins over CLI flag per D-09 |
| `SKILL.md` | `scripts/stage.py` | workflow step 2 CLI invocation | WIRED | `python ${CLAUDE_SKILL_DIR}/scripts/stage.py --phrase...` referenced in body |
| `SKILL.md` | `scripts/browse.py` | workflow step 3 CLI invocation | WIRED | `python ${CLAUDE_SKILL_DIR}/scripts/browse.py --uid...` referenced |
| `SKILL.md` | `references/api_contract.md` | "See also" Level-3 pointer | WIRED | `references/api_contract.md` referenced in See also section |
| `smoke_test.py` | `search.py`, `browse.py`, `parallels.py` | `from .{search,browse,parallels} import call_*` | WIRED | All three transport functions imported and called |

---

## Data-Flow Trace (Level 4)

The skill scripts do not render to a persistent DB or web page; they are CLI tools that emit JSON/Markdown to stdout for the model to consume. Data flows: `stage.py` calls `call_search()` which hits `/api/search`; `browse.py` calls the live API; responses are emitted to stdout. Real data flows confirmed by:

- Acceptance run returned real PGP transcriptions (T-S 8J41.1 JA text verbatim in justification)
- `state/throttle.json` contains post-run bucket state (proof of actual execution)
- 7 throttle unit tests validate the token-bucket math with fake clock (deterministic, not static)

No HOLLOW artifacts detected.

---

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| 22 skill unit tests GREEN | `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py -q` | 22 passed, 0 failed, 0.21s | PASS |
| Smoke tests skip cleanly without SKILL_SMOKE | `pytest tests/test_skill_smoke.py -q` (env var unset) | 2 skipped | PASS |
| Zero genizah_core/shared imports in skill scripts | grep across all 11 scripts | 0 matches | PASS |
| SKILL.md frontmatter valid (name ≤64, desc ≤1024, body ≤500 lines) | python validation script | 204 lines, 625 desc chars, valid YAML | PASS |
| Throttle state file has 3 independent buckets | inspect `state/throttle.json` | `{"search": ..., "browse": ..., "parallels": ...}` | PASS |
| REQUIREMENTS.md SKILL-04 R2 patch correct | grep for old text_source != 'full' | 0 matches; `pgp_transcription` present with Phase 79 reference | PASS |
| Live acceptance run (user-observed) | Hillel ran skill against localhost:8081 with scholarly query | APPROVED WITH NOTES 2026-05-05 | PASS |

---

## Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| SKILL-01 | 81B-01, 81B-02, 81B-04, 81B-05 | Runnable skill with configurable base URL; filesystem location not pinned to repo | SATISFIED | `_config.py` D-09 env-wins; skill installed to `~/.claude/skills/`; smoke test confirms production reachable |
| SKILL-02 | 81B-01, 81B-03 | Staged phrase discovery: extract phrases, multiple `/api/search` calls, merge by uid, browse drill-down, ranked candidates with justifications | SATISFIED | `stage.py` merge_results + stage_search + tier A/B/C; 15/15 consumer tests GREEN; live run produced tiered ranked output |
| SKILL-03 | 81B-02, 81B-03, 81B-04, 81B-05 | Handle 429/timeouts/partial browse without crashing; surface in plain terms | SATISFIED | Error-code table in SKILL.md; D-07 continue-on-error in stage_search; `unresolvable_filter_value` handled gracefully in acceptance run |
| SKILL-04 | 81B-01, 81B-03, 81B-04 | Browse honesty annotations (text_source, image availability) | SATISFIED | `honesty_annotation()` with `_FULL_TEXT_SOURCE = "pgp_transcription"` R2 mapping; 6 dedicated tests; REQUIREMENTS.md patched |
| SKILL-05 | 81B-01, 81B-03 | known_witnesses[], two-tier normalization, no genizah_core dependency | SATISFIED | `apply_known_witness_policy()` + `normalize_shelfmark.normalize()`; 0 genizah_core/shared imports confirmed; 6 dedicated tests |
| SKILL-06 | 81B-01, 81B-02 | Token-bucket throttle, separate buckets, ≤24 rpm, burst 5, 15+10 calls completes without self-rate-limiting | SATISFIED | `throttle.py` with 3-bucket JSON state; 7/7 throttle tests GREEN including 60-second math test; acceptance run 17 calls, 0 429s |

---

## Anti-Patterns Found

| File | Pattern | Severity | Impact |
|------|---------|----------|--------|
| `skills/cairo-genizah-research/scripts/throttle.py` | Uses `time.time()` instead of plan-specified `time.monotonic()` | INFO | Intentional deviation documented in 81B-02-SUMMARY.md: `time.monotonic()` resets per process, making cross-process persistence mathematically incorrect. `time.time()` is the correct choice for state written to disk. Tests monkeypatch `time.time` not `time.monotonic` by design. No impact on correctness. |
| `skills/cairo-genizah-research/scripts/format_output.py` | render_markdown uses ASCII `...` not Unicode `…` and plain text instead of emoji `🔖` | INFO | CLAUDE.md prohibits emojis in files; ASCII `...` substituted. No test coverage for exact character; functionally equivalent. Not a stub or placeholder. |
| `skills/cairo-genizah-research/scripts/smoke_test.py` | browse fallback inverts D-13 uid preference (tries sys_id first) | WARNING | Live `/api/browse` locally rejected uid with `Field required` — sys_id was the working path. Documented in 81B-05-SUMMARY.md deviation #3. Phase gate is met. Real fix tracked as v7.11 API item in ACCEPTANCE-RUN.md. Does not affect unit tests or core business logic. |
| `ROADMAP.md` phase tracking table | Phase 81A and 81B both still show "0/0 Not started" in the progress table | INFO | Progress table was not updated to reflect completion. Planning artifact only; does not affect code correctness. |

---

## Human Verification Required

None. All must-haves were verified programmatically or via the user-observed live acceptance run. The acceptance run constitutes the required human gate per ROADMAP Phase 81B and CONTEXT D-12.

**Acceptance run summary:**
- Query: "find letters in Judeo-Arabic mentioning הבחור"
- Base URL: `http://localhost:8081` (D-09 env-var override exercised)
- Results: 16 browse calls, 16 succeeded, top result T-S 8J41.1 (CUL, Tier A)
- Honesty annotations: verified (2 pgp_transcription = no annotation; 4 snippet = annotation present)
- Error handling: unresolvable_filter_value recovered gracefully (no crash)
- Sign-off: APPROVED WITH NOTES by Hillel Gershuni 2026-05-05
- Phase gate: MET (ROADMAP + CONTEXT D-12)

---

## Gaps Summary

No gaps. All 6 requirements satisfied, all 22 skill unit tests GREEN (15 consumer + 7 throttle), 2 smoke tests skip cleanly, live acceptance gate MET.

The "APPROVED WITH NOTES" status records three non-blocking items for v7.11+:

1. Optional clarification turn before committing to a search strategy for under-specified queries
2. Broader-than-domain-filter mode for genre-character manuscript matching (domain coverage is incomplete)
3. API gaps: language filter, `/api/browse` uid support, parallels apostrophe handling, filter-vocabulary discovery endpoint

These are documented in ACCEPTANCE-RUN.md deviations and explicitly called out as non-blocking in the user's sign-off statement. None is a correctness issue with the current skill implementation; all are feature/API requests for future iterations.

---

## Final Phase-Level Verdict

**VERIFIED PASSED**

All 6 SKILL requirements (SKILL-01 through SKILL-06) are satisfied by code, unit tests, and live acceptance evidence. The phase goal — a runnable Anthropic Skill driving the v7.10 API surface end-to-end with grounded justifications, honest reporting, throttle compliance, and known-witness handling — is fully achieved. The user signed off as APPROVED WITH NOTES; the notes are non-blocking candidates for v7.11+. Phase gate per ROADMAP.md and CONTEXT D-12: MET.

---

_Verified: 2026-05-05_
_Verifier: Claude (gsd-verifier)_
