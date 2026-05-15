# Phase 92: Final Sweep and Acceptance — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `92-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 92-final-sweep-and-acceptance
**Areas discussed:** External Codex red-team round on the plan (only)

---

## Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Audit evidence shape (SWEEP-01..04) | One SWEEP-AUDIT.md table covering grep/AST snapshot + per-transcript findings, or fold into VERIFICATION.md | |
| SWEEP-05 cross-user smoke test design | Two real Supabase users in two browsers running search→browse→lists→xlsx export concurrently, with a checked-in pass/fail checklist | |
| MULTITENANT.md scope and structure (SWEEP-06) | Reference doc only? Tutorial? Lessons-learned appendix? Cross-link to lint test? | |
| External Codex red-team round on the plan | Dispatch round-1 Codex review against this CONTEXT.md before planning (same pattern as Phases 88/89/90/91); plan decomposition folded in | ✓ |

**User's choice:** External Codex red-team round on the plan (only)
**Notes:** Same exclusive-delegation pattern as Phases 88/89/90/91. The user consistently delegates technical synthesis to external review for v7.12 phases.

---

## External Codex Red-Team Round on the Plan

### Sub-Q1: How many Codex review rounds for Phase 92?

| Option | Description | Selected |
|--------|-------------|----------|
| One round only (Recommended) | Round-1 against CONTEXT.md. Rely on plan-checker post-plan for residuals | ✓ |
| Two rounds (round-1 + post-plan plan-checker) | Round-1 against CONTEXT.md; second round against PLAN.md before execute | |
| Skip Codex review | Phase 92 is mechanical; rely on plan-checker only | |

**User's choice:** One round only
**Notes:** Phase 91 surface was similarly small and one round sufficed. Phase 92 verification + docs is even smaller.

### Sub-Q2: Plan decomposition for Phase 92?

| Option | Description | Selected |
|--------|-------------|----------|
| Single plan covering audit + smoke + docs | One plan, three task groups | |
| Two plans: audit+smoke / docs (Recommended) | Plan 92-01 = SWEEP-01..05; Plan 92-02 = SWEEP-06 | ✓ |
| Three plans: audit / smoke / docs | Granular but excessive for a closing phase | |

**User's choice:** Two plans
**Notes:** Matches the 2-plan split discipline used in Phases 89/90/91. The human smoke checkpoint sits naturally between Plan 92-01 commit and Plan 92-02 start. Gemini round-1 review later refined this with an explicit gating condition: Plan 92-02 cannot start until the SWEEP-05 smoke checklist is committed back with `Overall: PASS`.

### Sub-Q3: SWEEP-05 smoke test artifact shape?

| Option | Description | Selected |
|--------|-------------|----------|
| Tracked checklist file with checkboxes (Recommended) | `.planning/phases/92-*/92-SWEEP-05-SMOKE.md` with pre-filled scenario steps, pass/fail checkboxes, evidence column | ✓ |
| Inline section in VERIFICATION.md | Smoke results go into standard phase VERIFICATION.md alongside the other criteria | |
| Freeform memo | Short narrative confirming the scenario ran clean | |

**User's choice:** Tracked checklist file with checkboxes
**Notes:** Gemini round-1 expanded the scenarios beyond the baseline R0 (search→browse→lists→xlsx) to add R1 logout-mid-flight race + R2 token refresh race + conditional R3 puzzle-write race (the last gated on SWEEP-01 finding per-user data in joins.db).

### Sub-Q4: MULTITENANT.md scope (SWEEP-06)?

| Option | Description | Selected |
|--------|-------------|----------|
| Reference + 'add new per-user state' tutorial (Recommended) | Architecture reference plus step-by-step tutorial citing lint scanner | ✓ |
| Reference + lessons-learned appendix | Architecture reference plus 4-transcript summary | |
| Pure architecture reference | Just architecture sections | |
| All three (reference + tutorial + appendix) | Most thorough | |

**User's choice:** Reference + 'add new per-user state' tutorial
**Notes:** Gemini round-1 added a HIGH-priority refinement: §7 tutorial MUST include a bright-red warning callout about `set_auth(user, profile=None)` clears-stale-profile semantics, because the `kwarg=None` convention in Python normally means "no change" — violating Principle of Least Astonishment.

---

## Claude's Discretion

- Issue ID slug naming convention for SWEEP-04 thematic-walk audit memo
- Order of audits within Plan 92-01 (recommended: SWEEP-01 first, SWEEP-04 second, SWEEP-05 scaffold last so R3 conditional has data)
- Whether SWEEP-04 dedupes the 23-finding inventory inline or in an appendix (recommended: inline top-table + per-transcript appendix)
- MULTITENANT.md word count target (~2,000-3,000 words)
- Whether to add a CI test asserting MULTITENANT.md anchor presence (recommended: NO — adds maintenance overhead with marginal value)

---

## Deferred Ideas (mentioned during discussion)

- Round-2 Codex review post-2026-05-19 (when quota refreshes) — only re-open if a concrete blocker surfaces
- Widening Phase 87 lint scanner to cover `app.storage.browser` / `app.storage.client` (LOW-priority Gemini catch; different leak semantics; document in MULTITENANT.md §8 instead of lint-enforcing)
- Per-user data warehouse / RLS audit on joins.db (only matters if joins.db gains per-user ownership in a future phase)
- MULTITENANT.md as live executable doc with embedded type-checked snippets
- Server-side smoke automation (Playwright/Selenium) — conflicts with no-background-webserver feedback
- Lessons-learned appendix (explicitly rejected per locked decision; CONTEXT.md files preserve history)
- Cross-process safety for horizontal scaling — out of v7.12 scope
- v7.12.0 release tagging strategy (web-only-no-tag per feedback)

---

## External Review

- **Codex CLI:** Quota-blocked until 2026-05-19. Error: `You've hit your usage limit. To get more access now, send a request to your admin or try again at May 19th, 2026 1:25 PM.` Captured in `_tmp/codex_phase92_err.txt`.
- **Gemini CLI:** Succeeded (`gemini@0.42.0`). Round-1 verdict: 1 CRITICAL + 3 HIGH + 2 MEDIUM + 1 LOW → REFACTOR.
  - Prompt: `_tmp/codex_phase92_discuss_review_prompt.md`
  - Response: `_tmp/gemini_phase92_discuss_review_response.txt`
  - All findings encoded as locked decisions D-02 through D-10 in 92-CONTEXT.md.

**Symmetric fallback pattern:** Phase 91 round 2 used Codex (Gemini 429'd); Phase 92 round 1 used Gemini (Codex 429'd). The two external-review CLIs serve as failover for each other when quota is exhausted on either side.
