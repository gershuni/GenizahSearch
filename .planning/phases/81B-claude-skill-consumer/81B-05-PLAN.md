---
phase: 81B
plan: 05
type: execute
wave: 3
depends_on: [81B-02, 81B-03, 81B-04]
files_modified:
  - skills/cairo-genizah-research/scripts/smoke_test.py
  - .planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md
autonomous: false
requirements: [SKILL-01, SKILL-02, SKILL-03]
tags: [skill, smoke, acceptance, live-run, checkpoint]
must_haves:
  truths:
    - "scripts/smoke_test.py hits /api/search, /api/browse, /api/parallels against the configured base URL and reports per-endpoint status to stdout"
    - "Smoke test exits 0 when all three endpoints return valid envelopes; exits non-zero with a per-endpoint breakdown when any fail"
    - "Live acceptance run installs the skill at ~/.claude/skills/cairo-genizah-research/, invokes it with a real scholarly query, and produces a tiered candidate list with browse-grounded justifications and honesty annotations"
    - "User signs off on at least one query per ROADMAP Phase 81B phase gate"
    - "All preceding test suites still GREEN (22 skill tests + 1465 baseline = 1487 passed)"
    - "ACCEPTANCE-RUN.md captures the user's chosen query, the ranked output, the user's sign-off statement, and any deviations encountered"
  artifacts:
    - path: "skills/cairo-genizah-research/scripts/smoke_test.py"
      provides: "Bundled smoke harness for SKILL-01 (production reachable + base-URL config); runnable via bash from inside the skill"
      contains: "/api/search"
    - path: ".planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md"
      provides: "Phase gate evidence: query used, ranked output transcript, user sign-off line"
      min_lines: 30
  key_links:
    - from: "skills/cairo-genizah-research/scripts/smoke_test.py"
      to: "skills/cairo-genizah-research/scripts/{search,browse,parallels}.py"
      via: "imports call_search, call_browse, call_parallels"
      pattern: "from .* import call_"
    - from: ".planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md"
      to: "user sign-off on live run"
      via: "phase gate evidence"
      pattern: "signed off|approved"
---

<objective>
Final phase gate: bundle the smoke harness inside the skill so any future installer can sanity-check connectivity without running the full skill, then execute the user-observed live acceptance run per ROADMAP.md Phase 81B phase gate. Records the query, output, and sign-off in ACCEPTANCE-RUN.md.

Purpose: Per CONTEXT D-12 / ROADMAP phase-gate language, the v7.10 acceptance harness is the skill itself running end-to-end against production with the user observing. No CI substitute — the skill lives external to the repo (D-02), so the only signal that the milestone shipped is a working invocation the user signs off on. This plan turns 22 GREEN unit tests into observed real-world behavior.

Output: 1 smoke script + 1 evidence document. Plan contains a `checkpoint:human-verify` task — execute-phase will pause for the user to run the skill and sign off.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md
@.planning/phases/81B-claude-skill-consumer/81B-VALIDATION.md
@.planning/phases/81B-claude-skill-consumer/81B-01-PLAN.md
@.planning/phases/81B-claude-skill-consumer/81B-02-PLAN.md
@.planning/phases/81B-claude-skill-consumer/81B-03-PLAN.md
@.planning/phases/81B-claude-skill-consumer/81B-04-PLAN.md
@skills/cairo-genizah-research/SKILL.md
@skills/cairo-genizah-research/README.md
@tests/test_skill_smoke.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Bundled smoke_test.py harness</name>
  <files>skills/cairo-genizah-research/scripts/smoke_test.py</files>
  <read_first>
    - skills/cairo-genizah-research/scripts/search.py (call_search signature)
    - skills/cairo-genizah-research/scripts/browse.py (call_browse signature)
    - skills/cairo-genizah-research/scripts/parallels.py (call_parallels signature)
    - skills/cairo-genizah-research/SKILL.md (Sample invocations section)
    - tests/test_skill_smoke.py (the SKILL_SMOKE-gated live tests Plan 01 authored)
  </read_first>
  <action>
    Create `skills/cairo-genizah-research/scripts/smoke_test.py` — a self-contained Python script that runs three sequential live calls against the configured base URL and prints a per-endpoint status report. Used by:
    - The acceptance run (Task 2) as a pre-flight check before invoking the skill via Claude.
    - Any future installer who wants a quick "does this still work?" probe without launching Claude Code.

    ```python
    """Bundled smoke harness for cairo-genizah-research skill.

    Runs one /api/search, one /api/browse (drilling the first search result), and
    one /api/parallels against the configured base URL. Prints a per-endpoint
    pass/fail report to stdout and exits 0 (all pass) or non-zero (any fail).

    Usage:
        python scripts/smoke_test.py [--base-url URL] [--query Q]

    Per SKILL-01: works against any base URL (env var or CLI flag); D-09 env wins.
    """
    from __future__ import annotations
    import argparse
    import json
    import sys
    from typing import Any

    try:
        from . import _config
        from .search import call_search
        from .browse import call_browse
        from .parallels import call_parallels
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
        from scripts import _config  # type: ignore
        from scripts.search import call_search  # type: ignore
        from scripts.browse import call_browse  # type: ignore
        from scripts.parallels import call_parallels  # type: ignore


    def _check(label: str, response: dict, want_keys: list[str]) -> tuple[bool, str]:
        """Return (passed, message) for a single endpoint check."""
        if "error" in response:
            err = response["error"]
            return False, f"{label}: ERROR code={err.get('code')} message={err.get('message')}"
        for key in want_keys:
            if key not in response:
                return False, f"{label}: missing required key '{key}' in response"
        return True, f"{label}: OK (keys: {sorted(response.keys())[:5]}...)"


    def run_smoke(base_url: str | None = None, query: str = "ויאמר") -> int:
        report: list[str] = []
        all_pass = True

        effective_base = _config.resolve_base_url(base_url)
        report.append(f"Base URL: {effective_base}")
        report.append("")

        # /api/search
        s = call_search(query=query, search_mode="exact", limit=3, base_url=base_url)
        ok, msg = _check("search", s, ["schema_version", "source", "results"])
        report.append(msg)
        all_pass = all_pass and ok

        # /api/browse — drill the first result if present
        if ok and s.get("results"):
            first = s["results"][0]
            uid = first.get("uid")
            loc = first.get("locator") or {}
            if uid:
                b = call_browse(uid=uid, base_url=base_url)
            else:
                b = call_browse(
                    sys_id=loc.get("sys_id"),
                    p_num=loc.get("p_num"),
                    volume_ie=loc.get("volume_ie"),
                    base_url=base_url,
                )
            ok2, msg2 = _check("browse", b, ["schema_version", "source", "locator", "text_source"])
            report.append(msg2)
            all_pass = all_pass and ok2
        else:
            report.append("browse: SKIPPED (search returned 0 results or failed)")

        # /api/parallels — short test composition
        p = call_parallels(
            text="ויאמר משה אל בני ישראל ראו קרא ה' בשם",
            chunk_size=5,
            mode="exact",
            base_url=base_url,
        )
        ok3, msg3 = _check("parallels", p, ["schema_version", "source", "results"])
        report.append(msg3)
        all_pass = all_pass and ok3

        report.append("")
        report.append("=" * 50)
        report.append("OVERALL: " + ("PASS" if all_pass else "FAIL"))
        print("\n".join(report))
        return 0 if all_pass else 1


    def _main(argv: list[str] | None = None) -> int:
        parser = argparse.ArgumentParser(description="Smoke test cairo-genizah-research skill")
        parser.add_argument("--base-url", default=None,
                            help="Override base URL (env GENIZAH_API_BASE wins per D-09)")
        parser.add_argument("--query", default="ויאמר",
                            help="Test query for /api/search smoke")
        args = parser.parse_args(argv)
        return run_smoke(base_url=args.base_url, query=args.query)


    if __name__ == "__main__":
        sys.exit(_main())
    ```
  </action>
  <verify>
    <automated>python skills/cairo-genizah-research/scripts/smoke_test.py --query "ויאמר" 2>&1 | tee /tmp/skill-smoke-output.txt; grep -E "^OVERALL: (PASS|FAIL)" /tmp/skill-smoke-output.txt</automated>
  </verify>
  <acceptance_criteria>
    - File `skills/cairo-genizah-research/scripts/smoke_test.py` exists.
    - `grep "^def run_smoke" skills/cairo-genizah-research/scripts/smoke_test.py` returns 1 line.
    - `grep "call_search\|call_browse\|call_parallels" skills/cairo-genizah-research/scripts/smoke_test.py` returns ≥3 lines (all three endpoints exercised).
    - Verify command exits 0 (all-pass) AND stdout contains `OVERALL: PASS` — confirms live production reachable. If `OVERALL: FAIL` appears, the orchestrator surfaces the per-endpoint breakdown and the planner decides whether the failure is environmental (server outage) or a real bug; in the outage case, retry once, then proceed to Task 2 (the acceptance run is the authoritative gate, not this smoke).
    - `grep "Retry-After\|retry_after" skills/cairo-genizah-research/scripts/smoke_test.py` is OK to be 0 lines (smoke does not retry; first failure surfaces in the report).
  </acceptance_criteria>
  <done>Bundled smoke harness runnable. Production-reachability verified. Task 2 acceptance run can run with confidence the transport works.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 2: Live user-observed acceptance run</name>
  <what-built>
    Phase 81B is complete in code:
    - 6 fixture JSON files + 24 RED tests (Plan 01)
    - 6 transport scripts including throttle (Plan 02) — 7 throttle tests GREEN
    - 3 business-logic modules (Plan 03) — 15 consumer tests GREEN
    - SKILL.md + README + references/api_contract.md (Plan 04)
    - REQUIREMENTS.md SKILL-04 patched (R2 closed)
    - smoke_test.py bundled (Task 1 above) — verified production reachable

    What remains is the v7.10 acceptance gate: the user runs the skill end-to-end on a real scholarly query and signs off.
  </what-built>
  <how-to-verify>
    The user (you) executes these steps and reports back:

    **Step 1 — Install skill to Claude Code skills directory:**

    Linux/macOS:
    ```bash
    cp -r skills/cairo-genizah-research ~/.claude/skills/
    ls ~/.claude/skills/cairo-genizah-research/SKILL.md
    ```

    Windows (PowerShell):
    ```powershell
    Copy-Item -Recurse skills\cairo-genizah-research $HOME\.claude\skills\
    Get-Item $HOME\.claude\skills\cairo-genizah-research\SKILL.md
    ```

    Confirm `SKILL.md` is reachable at the install path.

    **Step 2 — Pre-flight smoke from inside the install:**

    ```bash
    python ~/.claude/skills/cairo-genizah-research/scripts/smoke_test.py --query "ויאמר"
    ```

    Expected: `OVERALL: PASS`. If FAIL, paste the per-endpoint breakdown — the
    planner determines whether to retry, debug, or proceed.

    **Step 3 — Run the skill via Claude Code with a scholarly query:**

    Open Claude Code in any directory (skill is installed personal-scope so it's
    available everywhere). Ask Claude a real scholarly Genizah question. Suggested
    starting prompts (pick whichever matches a real research interest you have):

    - "Find Cairo Genizah witnesses to the piyyut 'אין אדיר כיהוה' and rank them
      by evidence strength. List shelfmark, library, and brief justification."
    - "I have a fragment T-S 12.123. Find related fragments in the Genizah corpus
      and tell me which other manuscripts share liturgical content."
    - "Find Genizah responsa concerning [a halakhic topic you care about] —
      list shelfmarks with brief grounding."

    **Step 4 — Verify the output meets SC-2 schema:**

    Claude should invoke `cairo-genizah-research`, run through `stage.py` →
    `browse.py` chain, and produce a Markdown ranked list. Each entry should
    contain:
    - Shelfmark (e.g. `T-S 12.123`)
    - Library (e.g. `Cambridge University Library`)
    - Tier (A / B / C)
    - Known-witness flag (if you supplied known_witnesses; otherwise absent or false)
    - Matching phrases count
    - Justification (1–2 sentences) grounded in browse text
    - Browse URL (e.g. `https://genizahsearch.com/browse?sys_id=...`)
    - Image URL OR `(no image available)` annotation
    - Honesty annotation `(full text unavailable; based on snippet of N chars)`
      ONLY when text_source != `pgp_transcription`

    Final summary line: `Processed N candidates: X succeeded, Y rate-limited,
    Z NLI image unavailable.`

    **Step 5 — Verify error handling (optional but recommended for SKILL-03):**

    Trigger a partial-data condition by asking about a fragment with NLI imagery
    issues, OR set `GENIZAH_SKILL_REQ_PER_MIN=2` (artificially low) and ask a
    multi-phrase query. Confirm:
    - Skill does not crash the conversation.
    - Per-candidate inline notes appear (e.g. `"browse failed: rate-limited,
      retry-after 12s"`).
    - Skill continues processing remaining candidates.

    **Step 6 — Sign off in 81B-ACCEPTANCE-RUN.md:**

    The orchestrator will create `81B-ACCEPTANCE-RUN.md`. You append (or have
    Claude append on your behalf) under the `## User Sign-Off` section:
    - The actual query you ran (verbatim)
    - Top 3 results (shelfmark + tier + brief justification snippet)
    - Whether the ranking matched your scholarly judgment
    - Honesty annotations observed (text_source != pgp_transcription cases)
    - Any bugs / surprises
    - Sign-off statement: `"Approved — phase 81B accepted YYYY-MM-DD"` OR
      `"Rejected — issue: <description>"`.
  </how-to-verify>
  <resume-signal>
    Type one of:
    - `approved` — phase gate met; orchestrator records sign-off in 81B-ACCEPTANCE-RUN.md and proceeds to close-out.
    - `approved with notes: <issues>` — accepted but with documented caveats; close-out captures them.
    - `failed: <description>` — orchestrator opens a gap-closure plan or escalates to revision mode.
    - `skip-acceptance: <reason>` — only if the user explicitly waives the acceptance gate (rare; logs the waiver in ACCEPTANCE-RUN.md).
  </resume-signal>
</task>

<task type="auto">
  <name>Task 3: Author 81B-ACCEPTANCE-RUN.md evidence document</name>
  <files>.planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md</files>
  <read_first>
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (D-12 phase gate language)
    - .planning/ROADMAP.md (Phase 81B phase-gate line)
    - The user's resume-signal output from Task 2
  </read_first>
  <action>
    Author the acceptance evidence document. Run AFTER Task 2 completes and the user has provided the resume-signal. The orchestrator captures the user's response verbatim into the document.

    Template (fill in with actual run data; sections marked `[user fills]` may be supplied by the user during Task 2 sign-off or transcribed by the orchestrator from chat):

    ```markdown
    # Phase 81B — Acceptance Run Evidence

    **Run date:** YYYY-MM-DD
    **Phase gate:** ROADMAP.md Phase 81B — "live end-to-end run against the production deployment with the user observing; user-signed-off ranking against at least one scholarly query, with browse-honesty annotations verified."
    **Skill version:** v7.10 (Phase 81B initial release)
    **Base URL:** https://genizahsearch.com (production)

    ## Pre-flight

    - [ ] Skill installed at `~/.claude/skills/cairo-genizah-research/` (or platform equivalent)
    - [ ] `python ~/.claude/skills/cairo-genizah-research/scripts/smoke_test.py` returned `OVERALL: PASS`
    - [ ] All 22 unit tests GREEN (`pytest tests/test_skill_consumer.py tests/test_skill_throttle.py`)

    ## Query 1

    **User's query (verbatim):**
    > [user fills — the actual prompt sent to Claude]

    **Skill behavior observed:**
    - Phrase extraction: [user fills — what phrases did Claude extract?]
    - stage.py invocation: [user fills — observed via Claude Code transcript]
    - Top-N drill-down: [user fills — how many browse calls?]

    **Top 3 ranked output:**

    1. **[shelfmark]** — Tier [A/B/C] — [library]
       - Justification: [user fills — verbatim from skill output]
       - Honesty annotation: [user fills — present/absent and exact text]
       - Browse URL: [user fills]
       - Image URL: [user fills — URL or "(no image available)"]

    2. **[shelfmark]** — Tier [A/B/C] — [library]
       - [as above]

    3. **[shelfmark]** — Tier [A/B/C] — [library]
       - [as above]

    **Summary line emitted:**
    > [user fills — verbatim from skill output, e.g. "Processed 10 candidates: 9 succeeded, 1 NLI image unavailable."]

    ## SC-2 schema verification

    - [ ] Shelfmark present on every result
    - [ ] Library / library_name present
    - [ ] Tier (A / B / C) assigned to every result
    - [ ] Known-witness flag (if applicable)
    - [ ] Matching phrases count
    - [ ] Justification grounded in browse text (not invented context per R9)
    - [ ] Browse URL clickable
    - [ ] Image URL OR "(no image available)" annotation
    - [ ] Summary line counting successes/failures

    ## SC-3 error handling verification (if exercised)

    - [ ] Tested 429 / timeout / partial-NLI condition
    - [ ] Skill did NOT crash the conversation
    - [ ] Per-candidate inline notes appeared in plain text
    - [ ] Skill continued processing remaining candidates

    Trigger used: [user fills — e.g., "set GENIZAH_SKILL_REQ_PER_MIN=2", or "asked about Oxford-only fragment"]

    ## SC-4 throttle verification

    - [ ] Skill run did not produce its own 429 from `state/throttle.json` exhaustion
    - [ ] Run completed within reasonable wall-clock time (< 2 minutes for typical query)

    ## Honesty annotation verification (R2 mapping locked)

    - [ ] At least one result with `text_source: "pgp_transcription"` had NO honesty annotation
    - [ ] At least one result with `text_source: "snippet"` or `"none"` had `(full text unavailable; based on snippet of N chars)`
    - [ ] At least one result with image unavailable had `(no image available)`

    [user fills — note specific shelfmarks where each was observed]

    ## Deviations / surprises

    [user fills — any unexpected behavior, performance issues, or rough edges]

    ## User Sign-Off

    **Status:** [APPROVED / APPROVED WITH NOTES / REJECTED]

    **Sign-off statement:**
    > [user's verbatim resume-signal response from Task 2]

    **Date:** YYYY-MM-DD

    **Signed by:** [Hillel Gershuni]

    ---

    ## Phase Gate Result

    Per ROADMAP Phase 81B phase-gate: live end-to-end run against production with
    user observing — **[MET / NOT MET]**.

    Per CONTEXT D-12: user-signed-off ranking on at least one scholarly query —
    **[MET / NOT MET]**.

    Phase 81B status: [READY FOR /gsd-verify-work / NEEDS GAP CLOSURE]
    ```

    Implementation: write the template above with whatever the user actually
    provided during Task 2 substituted in. If the user signed off with minimal
    detail, fill in `[user fills]` slots with `(not captured in chat — see Claude
    Code transcript)` and leave the sign-off statement verbatim.
  </action>
  <verify>
    <automated>python -c "
import pathlib
p = pathlib.Path('.planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md')
assert p.exists(), 'ACCEPTANCE-RUN.md not created'
text = p.read_text(encoding='utf-8')
assert len(text.splitlines()) >= 30, f'too short: {len(text.splitlines())} lines'
assert 'User Sign-Off' in text
assert 'Phase Gate Result' in text
assert 'SC-2 schema verification' in text
assert 'pgp_transcription' in text  # R2 mapping referenced
print('OK')
"</automated>
  </verify>
  <acceptance_criteria>
    - File `.planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` exists (≥30 lines).
    - Verify command prints `OK`.
    - `grep "User Sign-Off" .planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` returns ≥1 line.
    - `grep -E "APPROVED|REJECTED" .planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` returns ≥1 line (sign-off status filled in).
    - `grep "Phase Gate Result" .planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` returns ≥1 line.
    - `grep -E "MET|NOT MET" .planning/phases/81B-claude-skill-consumer/81B-ACCEPTANCE-RUN.md` returns ≥2 lines (gate evaluation present).
    - If status is APPROVED: orchestrator proceeds to phase close-out. If REJECTED: orchestrator escalates to gap-closure planning per ROADMAP.
  </acceptance_criteria>
  <done>Acceptance evidence captured. Phase 81B has authoritative sign-off record. /gsd-verify-work can audit must-haves against this document plus the 22 GREEN unit tests.</done>
</task>

</tasks>

<threat_model>

## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Live production deployment → smoke harness | Outbound HTTPS; server is trusted. Risk: server outage during acceptance window blocks the gate. |
| User's Claude Code session → installed skill | Skill files at `~/.claude/skills/`; Claude reads frontmatter for triggering, executes scripts via bash. Risk: stale install (old SKILL.md cached) — mitigated by Claude Code's live change detection per RESEARCH §"Where Skills Live On Disk". |
| User input during sign-off → ACCEPTANCE-RUN.md | Verbatim transcription; user is the source of truth for scholarly judgment. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81B-18 | Spoofing | Skill installed at wrong path; user invokes a stale older version | mitigate | Task 2 step 1 explicitly verifies install path; smoke test in step 2 confirms scripts loadable from install location. |
| T-81B-19 | Denial of Service | Acceptance run blocked by server outage | accept | Smoke test fails-fast with per-endpoint breakdown; orchestrator can defer the run. R5 (server-side rate-limit drift) is captured. |
| T-81B-20 | Repudiation | Sign-off lacks verbatim user statement | mitigate | Task 3 acceptance criterion: `grep -E "APPROVED|REJECTED"` must return ≥1 line; orchestrator transcribes the resume-signal verbatim. |
| T-81B-21 | Tampering | Acceptance evidence later edited without trail | accept | Document is committed to git; any future amendment is a new commit reviewable in PR. |

</threat_model>

<verification>
- `python skills/cairo-genizah-research/scripts/smoke_test.py` exits 0 with `OVERALL: PASS`.
- All 22 skill unit tests GREEN: `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py -v`.
- Wider suite still GREEN: `pytest -k "not skill_smoke"` reports 1487 passed / 15 skipped.
- `tests/test_skill_smoke.py` runs and passes when invoked with `SKILL_SMOKE=1 GENIZAH_API_BASE=https://genizahsearch.com pytest tests/test_skill_smoke.py -v` (optional but recommended at acceptance time).
- `81B-ACCEPTANCE-RUN.md` contains a non-placeholder sign-off line.
- Phase gate per ROADMAP and CONTEXT D-12 either MET (orchestrator proceeds to /gsd-verify-work) or NOT MET (orchestrator opens gap closure).
</verification>

<success_criteria>
- Bundled smoke harness verifies all three endpoints reachable from the installed skill location.
- User has executed at least one scholarly query against the live deployment via Claude Code (or Desktop with network) and observed the skill produce a tiered ranked output.
- ACCEPTANCE-RUN.md captures the query, output sample, SC-2/SC-3 verification checkboxes, and the user's verbatim sign-off statement.
- Phase 81B requirements SKILL-01 (runnable + base-URL config), SKILL-02 (staged discovery end-to-end), and SKILL-03 (graceful error handling) are demonstrated in production behavior, not just unit tests.
- Phase 81B is READY FOR /gsd-verify-work — verifier audits 8 must-haves across the 5 plans against the test suite + ACCEPTANCE-RUN.md evidence.
</success_criteria>

<output>
After completion, create `.planning/phases/81B-claude-skill-consumer/81B-05-SUMMARY.md`:
- Smoke test outcome (PASS/FAIL with per-endpoint detail)
- Acceptance run summary (query, top result shelfmark, sign-off status)
- Cumulative phase metrics: 5 plans, 14 tasks, 24 unit tests authored / 22 GREEN, ACCEPTANCE-RUN evidence captured
- Confirmation phase 81B is ready for /gsd-verify-work OR enumeration of remaining gaps
</output>
