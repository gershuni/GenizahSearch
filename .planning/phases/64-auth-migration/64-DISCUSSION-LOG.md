# Phase 64: Auth Migration - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-14
**Phase:** 64-auth-migration
**Areas discussed:** OAuth flow switch, Desktop auth impact, Testing strategy, Review process

---

## OAuth Flow Switch

| Option | Description | Selected |
|--------|-------------|----------|
| Remove old method (Recommended) | Cleaner, more secure. CI + manual test catches breaks before deploy. | :heavy_check_mark: |
| Keep as fallback | Old implicit method stays as backup. More code to maintain but safer rollout. | |
| You decide | Claude picks based on code and external reviewers. | |

**User's choice:** Remove old method
**Notes:** None — straightforward decision.

---

## Desktop Auth Impact

| Option | Description | Selected |
|--------|-------------|----------|
| Just fix the import | Minimal change — keeps migration small and safe. | |
| Also review credential storage | Have external AI audit keyring/session logic for issues. | |
| You decide | Claude decides based on how risky the current code looks. | :heavy_check_mark: |

**User's choice:** You decide
**Notes:** Claude has discretion to expand scope if code looks fragile, but default is minimal.

---

## Testing Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Manual checklist (Recommended) | Clear checklist: web email, web Google, desktop login, token refresh. User verifies before deploy. | :heavy_check_mark: |
| Add automated tests | Integration tests requiring test credentials in CI. More setup. | |
| You decide | Claude picks based on existing test setup. | |

**User's choice:** Manual checklist
**Notes:** Auth is hard to unit test; manual verification is practical for this scope.

---

## Review Process

| Option | Description | Selected |
|--------|-------------|----------|
| External review first (Recommended) | Gemini/Codex verify migration correctness before merge. | :heavy_check_mark: |
| CI + manual test only | Small change scope; CI green + manual login test sufficient. | |
| You decide | Claude decides based on change scope. | |

**User's choice:** External review first
**Notes:** User explicitly said auth is "not my expertise" — external review is important, not optional.

---

## Claude's Discretion

- Legacy compatibility shims in `web/auth_state.py` (`api_call()`, `get_api_base()`) — clean up if safe
- Desktop credential storage broader review — only if code looks fragile
- Exact cleanup of implicit flow JavaScript in callback page

## Deferred Ideas

None — discussion stayed within phase scope
