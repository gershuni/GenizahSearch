---
phase: 87
plan: 08
type: execute
wave: 4
depends_on: [87-07]
files_modified:
  - CLAUDE.md
  - docs/OPEN_ISSUES.md
  - .planning/STATE.md
  - .planning/ROADMAP.md
autonomous: false
requirements:
  - FOUND-01
  - FOUND-02
  - FOUND-03
  - FOUND-04
  - FOUND-05
tags:
  - phase87
  - acceptance
  - docs
  - state-update
must_haves:
  truths:
    - "CLAUDE.md 'Recently Changed' section has a Phase 87 entry"
    - "docs/OPEN_ISSUES.md has any v7.11.0 storage-related issues marked Fixed (2026-MM-DD)"
    - ".planning/STATE.md reflects Phase 87 complete; next phase is 88"
    - ".planning/ROADMAP.md Progress table shows Phase 87 row complete"
    - "Human smoke-check of real NiceGUI session storage behavior PASSED — confirms (not discovers) wiring per B1"
  artifacts:
    - path: "CLAUDE.md"
      provides: "Updated Recently Changed entry for Phase 87"
      contains: "Phase 87"
    - path: "docs/OPEN_ISSUES.md"
      provides: "Storage-related issues marked Fixed"
      contains: "Phase 87"
    - path: ".planning/STATE.md"
      provides: "Phase 87 marked complete in phase queue; current_position updated to Phase 88"
      contains: "Phase 88"
    - path: ".planning/ROADMAP.md"
      provides: "Progress table updated; Phase 87 row marked Complete with today's date"
      contains: "Phase 87"
  key_links:
    - from: ".planning/STATE.md phase queue"
      to: "Phase 87 row status"
      via: "Pending -> Complete"
      pattern: "Phase 87.*Complete"
---

<objective>
Final acceptance gate for Phase 87. Wrap up the documentation and project state so:
1. Future Claude sessions see Phase 87 completed in STATE.md
2. CLAUDE.md "Recently Changed" reflects the foundations work
3. OPEN_ISSUES.md marks any v7.11.0 storage-related items as Fixed
4. A human-verifiable smoke-check CONFIRMS (not discovers) that the system behaves correctly under real (non-mocked) NiceGUI conditions

**REVISION (B1 clarification, M4 from 87-REVIEWS.md):**
- Per B1 (clarified in 87-REVIEWS.md Consensus Summary point 7): "Keep Plan 08 smoke check, but make it clear that automated gates must already prove FOUND-01. The smoke check should CONFIRM, not DISCOVER, missing UUID wiring."
- Plan 02 added `test_create_layout_mints_session_uuid` to make the wiring automatically verified BEFORE this plan runs. The smoke check is now strictly a real-NiceGUI confirmation, not a discovery mechanism.
- **M4:** All smoke-check commands are Windows-safe (PowerShell-compatible). No `/tmp`, `cat | json.tool | grep`, etc. Use `python -c` one-liners.

Output: Docs updated; STATE.md reflects Phase 87 complete; manual smoke check confirms real-NiceGUI behavior.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@CLAUDE.md
@docs/OPEN_ISSUES.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-01-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-03-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-04-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-05-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-06-SUMMARY.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-07-SUMMARY.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: Update CLAUDE.md, OPEN_ISSUES.md, STATE.md, ROADMAP.md</name>
  <read_first>
    - CLAUDE.md (FULL — find the "Recently Changed" section near the end; verify the format of recent entries to match style)
    - docs/OPEN_ISSUES.md (FULL — search for any open items related to `app.storage.user`, prune-race, session storage, or the v7.11.0 /browse 500)
    - .planning/STATE.md (FULL — find the "Phase Queue" table and the "Current Position" section)
    - .planning/ROADMAP.md (find Phase 87 entry and the Progress table)
    - All 7 prior plan SUMMARY files for Phase 87 to extract the numbers (sites migrated, tests added, allowlist entries) for the CLAUDE.md entry
  </read_first>
  <files>CLAUDE.md, docs/OPEN_ISSUES.md, .planning/STATE.md, .planning/ROADMAP.md</files>
  <action>
**Step 1: Update CLAUDE.md "Recently Changed" section.**

Find the "## Recently Changed" section near the end of CLAUDE.md. ADD a NEW entry at the TOP of the list (most recent first). Match the style of existing entries (date, version/phase, narrative paragraph).

Today's date is 2026-05-13 (use the actual date when this plan executes).

The entry should describe Phase 87 as an internal milestone:

```markdown
- May 2026: v7.12 Path B Phase 87 (Foundations -- Session UUID and Safe Storage Chokepoint) -- internal milestone, not a release. First phase of v7.12 Multitenant Architecture refactor. Adds `_session_uuid` lazy-mint helper to `web/safe_storage.py` (`get_session_uuid` + `ensure_session_uuid` with strict `^[0-9a-f]{32}$` regex validation per 87-REVIEWS.md M5 — Plan 02), wires `ensure_session_uuid()` into `web/main.py:create_layout()` so every page render mints the UUID automatically (B1 fix per 87-REVIEWS.md), migrates 132 raw `app.storage.user.*` access sites across 12 files to the safe_storage chokepoint helpers (Plans 03-06), creates `.planning/phase87_storage_allowlist.yaml` allowlist with H1-schema `expected_count` per pattern covering the ~13-16 bootstrap sites that intentionally remain raw (web/auth_state.py, web/main.py OAuth callback, web/supabase_client.py:111 captured-handle in get_user_client, web/export_state.py _TEST_BACKEND fallthrough -- each citing the specific downstream phase (88/90/91) that will migrate or delete it), and adds AST-based pytest lint scanner `tests/test_no_raw_storage_access.py` (6 tests covering allowlist schema, B2 chain-order correctness, alias resolution, parent-tracking to avoid double-reporting, H1 count enforcement, and the big-gate production scan). All 6 existing `tests/test_safe_storage.py` tests pass byte-unchanged (FOUND-05 invariant). 100-session UUID uniqueness verified by mock-based concurrency test. Codex round 4 MEDIUM-2 deferred-callback site at `parallels.py:3520` migrated with documenting comment about intentional silent-loss-on-prune tradeoff (vs. crashing the asyncio event loop). `tests/test_browse_state.py` and `tests/test_search_state.py` monkeypatches updated per 87-REVIEWS.md B3 to patch the new `web.safe_storage.app` chokepoint surface. Zero user-visible behavior change. No release -- Phase 88+ depends on this foundation. (web only -- desktop genuinely single-user)
```

**Important:** Adjust the site count (132) and allowlist entry count if Plan 07's final tally differs.

**Step 2: Update docs/OPEN_ISSUES.md.**

Search for any items related to:
- `app.storage.user`
- prune race / pruned session
- AssertionError on session storage
- v7.11.0 /browse 500
- Cross-user state leak (the Codex round 4 findings)

For each item found that is now resolved by Phase 87:
- Change `❌ Open` to `✅ Fixed (YYYY-MM-DD)` where YYYY-MM-DD is today's date
- Add a brief note pointing to Phase 87 (e.g., "Resolved by Phase 87 safe_storage chokepoint migration")

If NO items match (the file may not have tracked these specific issues), don't add anything — Phase 87 is foundations, not a user-facing bug fix.

Update the "Last Updated" timestamp at the bottom of OPEN_ISSUES.md to today's date.

**Step 3: Update .planning/STATE.md.**

In the "Phase Queue (v7.12)" table (around lines 36-43), change Phase 87's status from `Pending` to `Complete`.

In the "Current Position" section (around lines 26-31), update:
- `Phase: 87 of 92` -> `Phase: 88 of 92`
- `Status: Ready to execute` -> `Status: Phase 87 complete; ready for /gsd-discuss-phase 88`
- `Last activity:` -> today's date with summary: `<TODAY> -- Phase 87 complete. 132 raw-access sites migrated to safe_storage chokepoint; _session_uuid helper landed; bootstrap wiring per B1; lint scanner GREEN (6/6 tests).`

In the YAML frontmatter at lines 1-15, update:
- `completed_phases: 0` -> `completed_phases: 1`
- `completed_plans: 0` -> `completed_plans: 8`
- `percent: 0` -> `percent: 17`
- `last_updated:` -> today's ISO timestamp

In the "Accumulated Context" -> "Decisions" subsection (around line 62), append:
- `- Plan 07 lint scanner (tests/test_no_raw_storage_access.py) is the permanent CI guard against raw app.storage.user regression -- all new code must use safe_storage helpers or add an allowlist entry with justification AND expected_count (H1 schema from 87-REVIEWS.md)`
- `- Bootstrap wiring (B1): ensure_session_uuid() is called from web/main.py:create_layout(); this is the canonical mint point — DO NOT add per-page mint calls`

Update the `Next step:` line at the bottom to:
- `Next step: \`/gsd-discuss-phase 88\` (State Separation by Deletion)`

**Step 4: Update .planning/ROADMAP.md.**

Find the Phase 87 entry. Change:
```
**Plans**: TBD
```
to:
```
**Plans**: 8 plans (87-01 through 87-08)
- [x] 87-01-VALIDATION-FOUNDATION-PLAN.md -- Failing test stubs + allowlist scaffold (H1 schema)
- [x] 87-02-SESSION-UUID-HELPERS-PLAN.md -- get_session_uuid + ensure_session_uuid helpers + B1 bootstrap wiring
- [x] 87-03-LEAF-FILE-MIGRATIONS-PLAN.md -- 5 simple files (text_editor, translation_report, home, settings, search_results)
- [x] 87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md -- main.py + api.py (nicegui_app alias) + supabase_client.py (_app alias)
- [x] 87-05-BROWSE-CLUSTER-MIGRATIONS-PLAN.md -- browse.py + browse_state.py + catalog_browse.py + tests/test_browse_state.py (B3)
- [x] 87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md -- parallels.py + search.py + search_state.py + tests/test_search_state.py (B3)
- [x] 87-07-LINT-FINALIZATION-PLAN.md -- Lint scanner + allowlist finalization (H1)
- [x] 87-08-ACCEPTANCE-AND-DOCS-PLAN.md -- Docs + STATE.md + human smoke-check (confirms B1)
```

In the Progress table, change the Phase 87 row to:
```
| 87. Foundations -- Session UUID and Safe Storage Chokepoint | v7.12 | 8/8 | Complete | <TODAY> |
```

(Replace `<TODAY>` with today's date in YYYY-MM-DD format.)

**Step 5: Verify documentation health.**

```
python scripts/check_docs.py
```

Expected: exit 0 with all checks passing. If it fails, read the output and fix the specific complaints.

**Step 6: Final sanity-check (Windows-safe).**

```
python -c "import re; print('Phase 87 in CLAUDE.md:', 'Phase 87' in open('CLAUDE.md').read())"
python -c "import re; src = open('.planning/STATE.md').read(); print('Phase 87 Complete in STATE.md:', bool(re.search(r'Phase 87.*Complete|completed_phases:\\s*1', src)))"
python -c "import re; src = open('.planning/ROADMAP.md').read(); print('Phase 87 Complete in ROADMAP.md:', bool(re.search(r'Phase 87.*Complete|8/8.*Complete', src)))"
python scripts/check_docs.py
```
  </action>
  <verify>
    <automated>python scripts/check_docs.py</automated>
  </verify>
  <acceptance_criteria>
    - CLAUDE.md contains a Phase 87 entry: `python -c "assert 'Phase 87' in open('CLAUDE.md').read(); print('OK')"` prints `OK`
    - CLAUDE.md entry mentions specific Phase 87 substance (site count or "safe_storage chokepoint"): `python -c "src = open('CLAUDE.md').read(); assert 'safe_storage chokepoint' in src or '132' in src or 'sites migrated' in src; print('OK')"` prints `OK`
    - `.planning/STATE.md` reflects `completed_phases: 1`: `python -c "import re; assert re.search(r'completed_phases:\\s*1', open('.planning/STATE.md').read()); print('OK')"` prints `OK`
    - `.planning/STATE.md` `Next step:` references Phase 88: `python -c "import re; assert re.search(r'Phase 88|gsd-discuss-phase 88', open('.planning/STATE.md').read()); print('OK')"` prints `OK`
    - `.planning/ROADMAP.md` Phase 87 row marked Complete: `python -c "import re; assert re.search(r'Phase 87.*Complete|8/8.*Complete', open('.planning/ROADMAP.md').read()); print('OK')"` prints `OK`
    - `.planning/ROADMAP.md` Phase 87 entry has Plans populated (no longer "TBD"): `python -c "import re; src = open('.planning/ROADMAP.md').read(); assert '8 plans' in src; print('OK')"` prints `OK`
    - `python scripts/check_docs.py` exits 0
    - All 22 Phase 87 tests still pass: `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -x` exits 0
    - Full pytest suite still green
  </acceptance_criteria>
  <done>CLAUDE.md, OPEN_ISSUES.md, STATE.md, ROADMAP.md all reflect Phase 87 complete; check_docs green; full test suite green.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 2: Human smoke-check — CONFIRM real NiceGUI session storage behavior (per B1 clarification)</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SUMMARY.md (review Plan 02 mock-based test coverage + B1 wiring location)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (B1 clarification: smoke check CONFIRMS, does not DISCOVER)
    - All 7 prior SUMMARY files
  </read_first>
  <files>(no file edits — manual verification only; checkpoint pauses execution for user decision)</files>
  <action>
**This is a checkpoint task — execution pauses here for the user to run the smoke check manually.**

**Per B1 clarification in 87-REVIEWS.md:** the automated tests (specifically `test_create_layout_mints_session_uuid` from Plan 02) have ALREADY proven the bootstrap wiring works at the unit-test level. This smoke check is now strictly a CONFIRMATION against real NiceGUI runtime — not a discovery mechanism. If the smoke check finds a wiring issue, it indicates a deeper problem (e.g., NiceGUI version drift, deployment environment difference) rather than a planning gap.

What the executor should present to the user before pausing:

---

**Phase 87 is complete in code:**
- `web/safe_storage.py` extended with `get_session_uuid()` and `ensure_session_uuid()` helpers (strict `^[0-9a-f]{32}$` regex per M5)
- `web/main.py:create_layout()` calls `ensure_session_uuid()` as its first action (B1 wiring)
- 132 raw `app.storage.user.*` accesses migrated across 12 files
- AST-based lint scanner in `tests/test_no_raw_storage_access.py` (6 tests passing including H1 count enforcement and B2 parent-tracking)
- 10 concurrency/stability/validation tests for the UUID helper in `tests/test_session_uuid.py` (all passing)
- Bootstrap-wiring unit test `test_create_layout_mints_session_uuid` proves the `ensure_session_uuid()` import + call are present in `create_layout()` AND that calling it has the expected storage side effect
- Allowlist YAML at `.planning/phase87_storage_allowlist.yaml` covers ~13-16 bootstrap sites with H1 expected_count per pattern
- B3 fix: `tests/test_browse_state.py` and `tests/test_search_state.py` monkeypatches updated to web.safe_storage.app
- CLAUDE.md, STATE.md, ROADMAP.md, OPEN_ISSUES.md updated

All ~1878 automated tests pass. ruff clean. check_docs green.

**Why a smoke check is still needed (even with B1 automated coverage):** the automated tests use MOCKED `web.safe_storage.app`. The real NiceGUI session-storage path has not been exercised end-to-end. This smoke check confirms NiceGUI's middleware ordering matches expectations (page handler invokes create_layout invokes ensure_session_uuid before any session-dependent code) and that the JSON storage file on disk contains the minted UUID.

**Manual verification steps (estimated 5-10 minutes — all Windows-safe):**

Step 1: Start the web app locally (PowerShell).
```
python -m web.main
```
Wait for "NiceGUI ready on http://localhost:8080" (or 8081).

Step 2: Open the app in a regular browser window.
1. Navigate to http://localhost:8080/
2. Open DevTools -> Application -> Storage -> Cookies -> localhost:8080
3. Note the `session` cookie value
4. Navigate to /search; type query "כתב"; submit
5. Verify NO AssertionError traces in the Python console

Step 3: Verify _session_uuid was minted in the on-disk storage file (Windows-safe via Python).
```
python -c "import pathlib, json; files = list(pathlib.Path('.nicegui').glob('storage-user-*.json')); print('storage files found:', len(files)); [print(f, '->', json.loads(f.read_text(encoding='utf-8')).get('_session_uuid')) for f in files]"
```
Expected: at least one file printed, with a `_session_uuid` value matching `^[0-9a-f]{32}$`.

Step 4: Verify UUID stability across navigation.
1. Note the UUID printed in Step 3.
2. Navigate to /browse in the same browser tab.
3. Re-run the Python one-liner from Step 3.
4. The `_session_uuid` value MUST be identical to Step 3.

Step 5: Verify UUID isolation across browser sessions.
1. Open a PRIVATE/INCOGNITO browser window; navigate to http://localhost:8080/
2. Re-run the Python one-liner from Step 3.
3. A NEW storage file should appear with a DIFFERENT `_session_uuid`.

Step 6: Verify /browse no longer 500s (the original v7.11.0 bug).
1. Navigate to /browse?sys_id=990065549106000000 (or any valid manuscript ID)
2. Page should render without 500 errors
3. Switch language toggle (if present); verify reading-desk state restoration doesn't 500

Step 7: Verify CI infrastructure.
```
python -m pytest tests/test_no_raw_storage_access.py --collect-only
```
Expected: 6 tests collected.

**Decision:**
- **APPROVE** if all 7 steps pass; no AssertionError logs; _session_uuid is 32-char lowercase hex matching the regex; two sessions have different UUIDs; /browse renders OK.
- **REJECT** with details if any step fails. Provide specific log output and screenshots.

The executor MUST NOT proceed past this checkpoint until the user provides a clear approve/reject signal.
  </action>
  <verify>
    <automated>python -c "print('Manual smoke-check — verification provided by user response to resume-signal')"</automated>
  </verify>
  <acceptance_criteria>
    - User responds with "approved" or equivalent positive signal
    - If "approved": all 7 smoke-check steps from `<action>` confirmed by user to have passed
    - If "issues: ..." or "rejected": specific failure mode described; executor pauses and surfaces to user for follow-up plan
    - Local web app starts cleanly via `python -m web.main` (no startup errors)
    - At least one `.nicegui/storage-user-*.json` file is created during smoke check
    - That file contains a `_session_uuid` key with a value matching pattern `^[0-9a-f]{32}$`
    - **B1 clarification confirmed:** the smoke check CONFIRMS (rather than discovers) the wiring — i.e., `test_create_layout_mints_session_uuid` already proved this at unit-test time; the smoke check is purely a real-NiceGUI sanity gate
  </acceptance_criteria>
  <done>User approved the smoke check; real NiceGUI session storage behavior confirmed correct; B1 wiring works in production runtime.</done>
  <resume-signal>Type "approved" to mark Phase 87 complete and proceed to Phase 88 planning. Type "issues: &lt;description&gt;" to capture problems for follow-up before Phase 88.</resume-signal>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Manual smoke check -> live NiceGUI process | First end-to-end test against real (non-mocked) NiceGUI session storage; verifies the production path works |
| Documentation -> future contributors | CLAUDE.md and STATE.md are the future-Claude session's primary context — if these don't reflect Phase 87 complete, Phase 88 may double-migrate or skip work |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-03 | Information disclosure | UUID visible in browser DevTools / Python one-liner during smoke check | accept | Operator-only access; not a production attack vector. The smoke check explicitly examines the UUID for verification; the UUID is server-side per-session. |
| -- | Repudiation | Documentation drift | mitigate | check_docs gate ensures CLAUDE.md and OPEN_ISSUES.md stay in sync with reality; STATE.md is the authoritative project-state file |

This plan has minimal new security surface — primarily a manual verification step.
</threat_model>

<verification>
After both tasks (Windows-safe):

```
# Documentation health
python scripts/check_docs.py

# Phase 87 final test gate (all 22 tests)
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v

# Full suite still green
python -m pytest tests/ -x --tb=short

# STATE.md reflects completion
python -c "import re; src = open('.planning/STATE.md').read(); assert re.search(r'completed_phases:\\s*1', src); print('STATE.md OK')"

# CLAUDE.md has Phase 87 entry
python -c "assert 'Phase 87' in open('CLAUDE.md').read(); print('CLAUDE.md OK')"
python -c "src = open('CLAUDE.md').read(); assert 'safe_storage chokepoint' in src or '132' in src; print('CLAUDE.md substance OK')"

# ROADMAP.md updated
python -c "import re; assert re.search(r'Phase 87.*Complete|8/8.*Complete', open('.planning/ROADMAP.md').read()); print('ROADMAP.md OK')"

# Human smoke check completed
# (verified by checkpoint approval message)
```
</verification>

<success_criteria>
1. CLAUDE.md "Recently Changed" has new entry describing Phase 87 (site count, helper names, B1 wiring, allowlist, lint scanner)
2. docs/OPEN_ISSUES.md "Last Updated" reflects today's date; any storage-related items marked Fixed
3. .planning/STATE.md shows `completed_phases: 1`, current position Phase 88, next step `/gsd-discuss-phase 88`
4. .planning/ROADMAP.md Phase 87 row marked Complete with today's date AND Plans line populated
5. `python scripts/check_docs.py` exits 0
6. All 22 Phase 87 tests pass (6 + 10 + 6)
7. Full pytest suite passes
8. Human smoke check APPROVED (B1 clarification: confirms, not discovers — real-NiceGUI behavior matches automated test coverage)
9. All 5 ROADMAP Phase 87 Success Criteria verifiable and verified
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-08-SUMMARY.md` summarizing:
- Final Phase 87 status: COMPLETE
- Documentation updates applied (CLAUDE.md entry, OPEN_ISSUES.md, STATE.md, ROADMAP.md)
- Manual smoke check outcome (approved / approved-with-followups / rejected)
- B1 confirmation: the smoke check verified (didn't discover) the bootstrap wiring; `test_create_layout_mints_session_uuid` already proved the wiring at unit-test time
- Cumulative phase metrics:
  - Plans completed: 8
  - Files modified across phase: 14 source files + 4 docs files + 2 test files updated for B3 = 20 files
  - Tests added: 16 (10 in test_session_uuid.py + 6 in test_no_raw_storage_access.py)
  - Tests preserved: 6 (test_safe_storage.py byte-identical to baseline)
  - Tests updated for B3: 2 (test_browse_state.py + test_search_state.py monkeypatch targets)
  - Raw access sites migrated: 132 (or actual count from Plan 07 final tally)
  - Allowlist entries: 4-6 (Plan 01 + any Plan 07 additions) with H1 expected_count schema
- Hand-off to Phase 88: `_session_uuid` is the stable cache key; Phase 88 can begin STATE-01..06 work
- Any follow-ups deferred to later phases (Phase 91 will migrate the OAuth allowlist; Phase 90 will delete the supabase_client allowlist; Phase 88 will delete the export_state allowlist)
</output>
