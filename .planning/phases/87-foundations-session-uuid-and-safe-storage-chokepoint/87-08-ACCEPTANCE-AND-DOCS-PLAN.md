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
    - "Human smoke-check of real NiceGUI session storage behavior PASSED"
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
      via: "Pending → Complete"
      pattern: "Phase 87.*Complete"
---

<objective>
Final acceptance gate for Phase 87. Wrap up the documentation and project state so:
1. Future Claude sessions see Phase 87 completed in STATE.md
2. CLAUDE.md "Recently Changed" reflects the foundations work
3. OPEN_ISSUES.md marks any v7.11.0 storage-related items as Fixed
4. A human-verifiable smoke-check confirms the system behaves correctly under real (non-mocked) NiceGUI conditions

This plan has 1 autonomous task (docs) and 1 checkpoint (human-verify smoke check). The smoke check is critical because all prior plans were mock-based — this is the first time the full system is exercised end-to-end against real session storage.

Output: Docs updated; STATE.md reflects Phase 87 complete; manual smoke check passed.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/ROADMAP.md
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
    - .planning/ROADMAP.md (find Phase 87 entry at lines 244-254 and the Progress table at line 315+)
    - All 7 prior plan SUMMARY files for Phase 87 to extract the numbers (sites migrated, tests added, allowlist entries) for the CLAUDE.md entry
  </read_first>
  <files>CLAUDE.md, docs/OPEN_ISSUES.md, .planning/STATE.md, .planning/ROADMAP.md</files>
  <action>
**Step 1: Update CLAUDE.md "Recently Changed" section.**

Find the "## Recently Changed" section near the end of CLAUDE.md. ADD a NEW entry at the TOP of the list (most recent first). Match the style of existing entries (date, version/phase, narrative paragraph).

Today's date is 2026-05-13 (verify with current date — the entry's date should be the day this plan executes).

The entry should describe Phase 87 as an internal milestone:

```markdown
- May 2026: v7.12 Path B Phase 87 (Foundations — Session UUID and Safe Storage Chokepoint) -- internal milestone, not a release. First phase of v7.12 Multitenant Architecture refactor. Adds `_session_uuid` lazy-mint helper to `web/safe_storage.py` (`get_session_uuid` + `ensure_session_uuid` — Plan 02), migrates 132 raw `app.storage.user.*` access sites across 12 files to the safe_storage chokepoint helpers (Plans 03-06), creates `.planning/phase87_storage_allowlist.yaml` allowlist for the ~13-16 bootstrap sites that intentionally remain raw (web/auth_state.py, web/main.py OAuth callback, web/supabase_client.py:111 captured-handle in get_user_client, web/export_state.py _TEST_BACKEND fallthrough — each citing the specific downstream phase (88/90/91) that will migrate or delete it), and adds AST-based pytest lint scanner `tests/test_no_raw_storage_access.py` that rejects any new raw access outside the allowlist (FOUND-04 CI gate). All 6 existing `tests/test_safe_storage.py` tests pass byte-unchanged (FOUND-05 invariant). 100-session UUID uniqueness verified by mock-based concurrency test. Codex round 4 MEDIUM-2 deferred-callback site at `parallels.py:3520` migrated with documenting comment about intentional silent-loss-on-prune tradeoff (vs. crashing the asyncio event loop). Zero user-visible behavior change. No release — Phase 88+ depends on this foundation. (web only — desktop genuinely single-user)
```

If exact site count differs from 132 (depending on what Plan 07 added), correct the number based on the SUMMARY files.

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

In the "Phase Queue (v7.12)" table at lines ~36-43, change Phase 87's status from `Pending` to `Complete`.

In the "Current Position" section at lines ~26-30, update:
- `Phase: 87 of 92` → `Phase: 88 of 92`
- `Status: Pending discussion` → `Status: Phase 87 complete; ready for /gsd-discuss-phase 88`
- `Last activity:` → today's date with summary: `<TODAY> -- Phase 87 complete. 132 raw-access sites migrated to safe_storage chokepoint; _session_uuid helper landed; lint scanner GREEN.`

In the YAML frontmatter at lines 1-13, update:
- `completed_phases: 0` → `completed_phases: 1`
- `total_plans: 0` → `total_plans: 8`
- `completed_plans: 0` → `completed_plans: 8`
- `percent: 0` → `percent: 17`
- `last_updated:` → today's ISO timestamp (e.g., "2026-05-13T00:00:00.000Z" or current date)

In the "Accumulated Context" → "Decisions" subsection (around line 62), append:
- `- Plan 07 lint scanner (tests/test_no_raw_storage_access.py) is the permanent CI guard against raw app.storage.user regression — all new code must use safe_storage helpers or add an allowlist entry with justification`

Update the `Next step:` line at the bottom (~line 87) to:
- `Next step: \`/gsd-discuss-phase 88\` (State Separation by Deletion)`

**Step 4: Update .planning/ROADMAP.md.**

At line 254, change:
```
**Plans**: TBD
```
to:
```
**Plans**: 8 plans (87-01 through 87-08)
- [x] 87-01-VALIDATION-FOUNDATION-PLAN.md — Failing test stubs + allowlist scaffold
- [x] 87-02-SESSION-UUID-HELPERS-PLAN.md — get_session_uuid + ensure_session_uuid helpers
- [x] 87-03-LEAF-FILE-MIGRATIONS-PLAN.md — 5 simple files (text_editor, translation_report, home, settings, search_results)
- [x] 87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md — main.py + api.py (nicegui_app alias) + supabase_client.py (_app alias)
- [x] 87-05-BROWSE-CLUSTER-MIGRATIONS-PLAN.md — browse.py + browse_state.py + catalog_browse.py
- [x] 87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md — parallels.py + search.py + search_state.py
- [x] 87-07-LINT-FINALIZATION-PLAN.md — Lint scanner + allowlist finalization
- [x] 87-08-ACCEPTANCE-AND-DOCS-PLAN.md — Docs + STATE.md + human smoke-check
```

In the Progress table at line 322, change:
```
| 87. Foundations -- Session UUID and Safe Storage Chokepoint | v7.12 | 0/TBD | Not started | - |
```
to:
```
| 87. Foundations -- Session UUID and Safe Storage Chokepoint | v7.12 | 8/8 | Complete | <TODAY> |
```

(Replace `<TODAY>` with today's date in YYYY-MM-DD format.)

**Step 5: Verify documentation health.**

```bash
python scripts/check_docs.py
```

Expected: exit 0 with all checks passing. If it fails, read the output and fix the specific complaints (missing entries, broken references, etc.).

**Step 6: Final sanity-check.**

```bash
# CLAUDE.md has new entry
grep -c "Phase 87" CLAUDE.md  # expect at least 1 in Recently Changed

# STATE.md reflects completion
grep -E "Phase 87.*Complete|completed_phases: 1" .planning/STATE.md  # expect at least 1 match

# ROADMAP.md table updated
grep -E "Phase 87.*Complete|87.*8/8" .planning/ROADMAP.md  # expect at least 1 match

# check_docs healthy
python scripts/check_docs.py 2>&1 | tail -5
```
  </action>
  <verify>
    <automated>python scripts/check_docs.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "Phase 87" CLAUDE.md` returns at least 1 (new Recently Changed entry exists)
    - `grep -c "132 raw\|sites migrated" CLAUDE.md` returns at least 1 (or the actual site count from Plan 07's final tally) — proves the entry has specific Phase 87 substance, not just a placeholder
    - `grep -c "safe_storage chokepoint" CLAUDE.md` returns at least 1 (terminology used in entry)
    - `.planning/STATE.md` reflects `completed_phases: 1` (or matching count)
    - `.planning/STATE.md` `Next step:` line references Phase 88 (or `/gsd-discuss-phase 88`)
    - `.planning/STATE.md` `last_updated` is today's date
    - `.planning/ROADMAP.md` Progress table shows Phase 87 row status as `Complete` (`grep -c "Phase 87.*Complete\|Foundations.*Complete" .planning/ROADMAP.md` returns at least 1)
    - `.planning/ROADMAP.md` Phase 87 entry has `**Plans**:` populated (no longer "TBD"): `grep -c "Plans.*: 8 plans" .planning/ROADMAP.md` returns at least 1
    - `python scripts/check_docs.py` exits 0
    - `docs/OPEN_ISSUES.md` "Last Updated" line updated to today's date (verify with `grep "Last Updated" docs/OPEN_ISSUES.md | head -1`)
    - All 11 Phase 87 tests still pass: `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -x` exits 0
    - `pytest tests/ -x --tb=line 2>&1 | tail -3 | grep passed` shows full suite green
  </acceptance_criteria>
  <done>CLAUDE.md, OPEN_ISSUES.md, STATE.md, ROADMAP.md all reflect Phase 87 complete; check_docs green; full test suite green.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 2: Human smoke-check — real NiceGUI session storage behavior</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SUMMARY.md (review Plan 02 mock-based test coverage to understand what is NOT yet covered by real-NiceGUI exercise)
    - All 7 prior SUMMARY files (to recall what was changed and ensure smoke-check covers the main areas)
  </read_first>
  <files>(no file edits — manual verification only; checkpoint pauses execution for user decision)</files>
  <action>
**This is a checkpoint task — execution pauses here for the user to run the smoke check manually.**

What the executor (Claude) should present to the user before the pause:

---

**Phase 87 is complete in code:**
- `web/safe_storage.py` extended with `get_session_uuid()` and `ensure_session_uuid()` helpers
- 132 raw `app.storage.user.*` accesses migrated across 12 files
- AST-based lint scanner in `tests/test_no_raw_storage_access.py` (4 tests passing)
- 5 concurrency/stability tests for the UUID helper in `tests/test_session_uuid.py` (all passing)
- Allowlist YAML at `.planning/phase87_storage_allowlist.yaml` covers ~13-16 bootstrap sites
- CLAUDE.md, STATE.md, ROADMAP.md, OPEN_ISSUES.md updated

All ~1871 automated tests pass. ruff clean. check_docs green.

**Why a smoke check is needed:** ALL automated tests use MOCKED `app.storage.user` (per Plan 02 research R-07 decision). The real NiceGUI session-storage path has not been exercised end-to-end. This smoke check verifies the system works against a live NiceGUI process.

**Manual verification steps (estimated 5-10 minutes):**

Step 1: Start the web app locally.
```bash
python -m web.main
```
Wait for "NiceGUI ready on http://localhost:8080" (or 8081).

Step 2: Open the app in a regular browser window.
1. Navigate to http://localhost:8080/
2. Open DevTools → Application → Storage → Cookies → localhost:8080
3. Note the `session` cookie value
4. Navigate to /search; type query "כתב"; submit
5. Verify no AssertionError traces in the Python console

Step 3: Verify _session_uuid was minted.
```bash
ls -la .nicegui/storage-user-*.json
cat ".nicegui/storage-user-<session-id>.json" | python -m json.tool | grep _session_uuid
```
Expected: `"_session_uuid": "<32-char hex>"`

Step 4: Verify UUID stability across navigation.
1. Navigate to /browse in the same browser tab
2. Re-read the storage file
3. The `_session_uuid` value MUST be identical to step 3

Step 5: Verify UUID isolation across browser sessions.
1. Open a PRIVATE/INCOGNITO browser window; navigate to http://localhost:8080/
2. A new storage file should appear in .nicegui/
3. Compare `_session_uuid` values from the 2 files — MUST be different

Step 6: Verify /browse no longer 500s.
1. Navigate to /browse?sys_id=990065549106000000 (or any valid manuscript ID)
2. Page should render without 500 errors
3. Switch language toggle (if present); verify reading-desk state restoration doesn't 500

Step 7: Verify CI infrastructure.
```bash
pytest tests/test_no_raw_storage_access.py --collect-only
```
Expected: 4 tests collected.

**Decision:**
- **APPROVE** if all 7 steps pass; no AssertionError logs; _session_uuid is 32-char hex; two sessions have different UUIDs; /browse renders OK.
- **REJECT** with details if any step fails. Provide specific log output and screenshots.

The executor MUST NOT proceed past this checkpoint until the user provides a clear approve/reject signal.
  </action>
  <verify>
    <automated>echo "Manual smoke-check — verification provided by user response to resume-signal"</automated>
  </verify>
  <acceptance_criteria>
    - User responds with "approved" or equivalent positive signal
    - If "approved": all 7 smoke-check steps from `<action>` confirmed by user to have passed
    - If "issues: ..." or "rejected": specific failure mode described; executor pauses and surfaces to user for follow-up plan
    - Local web app starts cleanly via `python -m web.main` (no startup errors)
    - At least one `.nicegui/storage-user-*.json` file is created during smoke check
    - That file contains a `_session_uuid` key with a value matching pattern `^[0-9a-f]{32}$`
  </acceptance_criteria>
  <done>User approved the smoke check; real NiceGUI session storage behavior confirmed correct.</done>
  <resume-signal>Type "approved" to mark Phase 87 complete and proceed to Phase 88 planning. Type "issues: &lt;description&gt;" to capture problems for follow-up before Phase 88.</resume-signal>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Manual smoke check → live NiceGUI process | First end-to-end test against real (non-mocked) NiceGUI session storage; verifies the production path works |
| Documentation → future contributors | CLAUDE.md and STATE.md are the future-Claude session's primary context — if these don't reflect Phase 87 complete, Phase 88 may double-migrate or skip work |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-03 | Information disclosure | UUID visible in browser DevTools during smoke check | accept | Operator-only access; not a production attack vector. The smoke check explicitly examines the UUID for verification; the UUID is server-side per-session (not transmitted to browser via URL/cookie outside of NiceGUI's session cookie HMAC). |
| — | Repudiation | Documentation drift | mitigate | check_docs gate ensures CLAUDE.md and OPEN_ISSUES.md stay in sync with reality; STATE.md is the authoritative project-state file |

This plan has minimal new security surface — primarily a manual verification step.
</threat_model>

<verification>
After both tasks:

```bash
# Documentation health
python scripts/check_docs.py

# Phase 87 final test gate (all 15 tests)
pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v

# Full suite still green
pytest tests/ -x --tb=short 2>&1 | tail -3

# STATE.md reflects completion
grep -E "completed_phases: 1|Phase 87.*Complete" .planning/STATE.md

# CLAUDE.md has Phase 87 entry
grep -c "Phase 87" CLAUDE.md  # expect at least 1
grep -c "safe_storage chokepoint" CLAUDE.md  # expect at least 1

# ROADMAP.md updated
grep -E "Phase 87.*Complete|8/8.*Complete" .planning/ROADMAP.md

# Human smoke check completed
# (verified by checkpoint approval message)
```
</verification>

<success_criteria>
1. CLAUDE.md "Recently Changed" has new entry describing Phase 87 (site count, helper names, allowlist, lint scanner)
2. docs/OPEN_ISSUES.md "Last Updated" reflects today's date; any storage-related items marked Fixed
3. .planning/STATE.md shows `completed_phases: 1`, current position Phase 88, next step `/gsd-discuss-phase 88`
4. .planning/ROADMAP.md Phase 87 row marked Complete with today's date AND Plans line populated
5. `python scripts/check_docs.py` exits 0
6. All 15 Phase 87 tests pass (6 + 5 + 4)
7. Full pytest suite passes
8. Human smoke check approved (manual verification of real NiceGUI session storage behavior)
9. All 5 ROADMAP Phase 87 Success Criteria verifiable and verified
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-08-SUMMARY.md` summarizing:
- Final Phase 87 status: COMPLETE
- Documentation updates applied (CLAUDE.md entry, OPEN_ISSUES.md, STATE.md, ROADMAP.md)
- Manual smoke check outcome (approved / approved-with-followups / rejected)
- Cumulative phase metrics:
  - Plans completed: 8
  - Files modified across phase: 14 source files + 4 docs files = 18 files
  - Tests added: 9 (5 in test_session_uuid.py + 4 in test_no_raw_storage_access.py)
  - Tests preserved: 6 (test_safe_storage.py byte-identical to baseline)
  - Raw access sites migrated: 132 (or actual count from Plan 07 final tally)
  - Allowlist entries: 4-6 (Plan 01 + any Plan 07 additions)
- Hand-off to Phase 88: `_session_uuid` is the stable cache key; Phase 88 can begin STATE-01..06 work
- Any follow-ups deferred to later phases (note specifically Phase 91 will migrate the OAuth allowlist; Phase 90 will delete the supabase_client allowlist; Phase 88 will delete the export_state allowlist)
</output>
