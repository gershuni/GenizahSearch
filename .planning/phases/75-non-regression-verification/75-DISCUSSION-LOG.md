# Phase 75: Non-Regression Verification - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `75-CONTEXT.md` — this log preserves alternatives considered.

**Date:** 2026-04-17
**Phase:** 75-non-regression-verification
**Areas discussed:** Baseline reference method, Canonical test script, Sign-off artifact format, Regression escape hatch

---

## Discuss (top-level selection)

| Option | Description | Selected |
|--------|-------------|----------|
| Baseline reference method | How do we compare 'pre-refactor' behavior? Checkout pre-phase-67 commit for A/B, or rely on memory? | (delegated) |
| Canonical test script per surface | Fix specific searches and manuscripts, or keep ad-hoc. | (delegated) |
| Sign-off artifact format | Single `75-UAT.md` checklist, inline conversation sign-off, or both. | (delegated) |
| Regression-found escape hatch | Halt + gap plan, defer to Phase 76, or triage inline. | (delegated) |

**User's choice:** "Your discretion" — Claude resolves all four areas using the milestone's established patterns.
**Notes:** User answered with a blanket delegation rather than per-area picks. Claude proceeded to resolve each gray area against existing project artifacts (Phase 71's `docs/desktop-smoke-checklist.md`, Phase 74's `74-HUMAN-UAT.md`, the `--gaps` command, `docs/OPEN_ISSUES.md`). No alternative discussion — the delegation was explicit.

---

## Baseline Reference Method

| Option | Description | Selected |
|--------|-------------|----------|
| User memory / gut feel only | Sign off on "no obvious slowdown" using daily-use recall. Fastest, matches ROADMAP's qualitative bar. | ✓ (primary) |
| Pre-refactor worktree A/B | Checkout `56facc3d` in `C:/tmp/gsd-review/v7.8-baseline/`, run both versions side-by-side. Rigorous but slow. | ✓ (fallback) |
| Hybrid | Memory first; worktree only when user is uncertain on a specific surface. | ✓ (adopted pattern) |

**Resolved decision:** Memory-first, worktree-fallback (D-01, D-02). Data sidecars shared between worktrees (D-03). Fallback is per-surface, not per-session (D-04).

**Notes:** ROADMAP explicitly says "no quantitative thresholds; the bar is 'no obvious slowdown vs pre-refactor.'" That framing leans heavily toward gut-feel. Adding the worktree as a crutch for uncertain surfaces costs ~30s per invocation and preserves rigor where the user needs it.

---

## Canonical Test Script Per Surface

| Option | Description | Selected |
|--------|-------------|----------|
| Fixed minimal script | Locked checklist per surface, specific sys_ids, reproducible across runs. | ✓ |
| Ad-hoc walkthrough | Executor improvises based on each success criterion. | |
| Fully scripted (Playwright/pytest-playwright) | Automated UI script. | (explicitly deferred) |

**Resolved decision:** Fixed minimal script (D-05). Reuses `docs/desktop-smoke-checklist.md` sections 2 and 4 for desktop (D-06). Web side authored inline in `75-UAT.md` with five bullets per surface (D-07). Four fixed test manuscripts: one Cambridge T-S, one NLI-only, one multi-IE (from `.planning/debug/multi_ie_fl_validation.csv`), one JTS DPUL (D-08). Composition search uses a short 2–3 chunk query (D-09).

**Notes:** Reproducibility matters less than in a benchmark, but a fixed script keeps the user's signal clean — if they sign off on run 1 and later re-run, they're testing the same thing. The existing Phase 71 checklist is the biggest reusable asset; not rewriting it is a discipline win.

---

## Sign-off Artifact Format

| Option | Description | Selected |
|--------|-------------|----------|
| `75-UAT.md` with pre-populated checklist | Mirror Phase 74's YAML-frontmatter format. | ✓ |
| Inline conversation sign-off | User says yes/no in chat; executor records in `75-VERIFICATION.md`. | |
| Both UAT + inline | Redundant; chat is ephemeral. | |
| `75-HUMAN-UAT.md` (Phase 74 naming) | Preserve Phase 74's filename. | (rejected — naming drift) |

**Resolved decision:** Single `75-UAT.md` (D-10), five tests (four surfaces + pytest), YAML frontmatter matching Phase 74 (D-11), per-surface sign-off granularity (D-12), final `75-VERIFICATION.md` consumes it (D-13), UAT is a living file during execution (D-14).

**Notes:** Dropping the `HUMAN-` prefix aligns with recent project convention (`.planning/quick/260318-kk1-*-UAT.md`). Phase 74's use of `HUMAN-UAT.md` is not a pattern to preserve — it was a one-off.

---

## Regression-Found Escape Hatch

| Option | Description | Selected |
|--------|-------------|----------|
| Halt + gap plan for everything | Every regression triggers `/gsd-plan-phase 75 --gaps`. Safe but heavy. | |
| Defer everything to Phase 76 | Document regressions in Phase 76. Ships this phase faster. | (rejected) |
| Triage per-regression (blocker vs minor) | User decides; blockers → gap plan, minor → `OPEN_ISSUES.md`. | ✓ |

**Resolved decision:** Two-tier triage with user as arbiter (D-15). Blockers → `/gsd-plan-phase 75 --gaps` (existing project tool, no new mechanism needed). Minor → append to `docs/OPEN_ISSUES.md` under a v7.9 decomposition section. Phase 76 is NOT a dumping ground for fixes (D-16). Pytest failures are automatic blockers (D-17). Pytest runs last so a gap-plan fix is what gets measured (D-18).

**Notes:** The `--gaps` mechanism exists precisely for this case. Using it for blockers, and deferring perceptual nitpicks to `OPEN_ISSUES.md`, splits the incentives cleanly: genuine regressions get fixed; vibes-based observations don't stall phase close.

---

## Claude's Discretion

- Number of plans (expecting 1: pre-populate UAT + walk through + produce VERIFICATION.md).
- Whether to promote the web checklist to `docs/web-smoke-checklist.md` later.
- Whether to include an English query alongside Hebrew `"שלום"`.
- How the executor drives the walkthrough (verbal report vs shared-screen vs URL-paste).
- Whether to tee pytest output to `75-pytest-baseline.txt` (leaning yes — cheap evidence).

## Deferred Ideas

- Web smoke checklist as a `docs/` artifact (future milestone).
- Quantitative performance baseline harness (explicitly rejected by REQUIREMENTS.md non-goals).
- Automated perceptual regression (Playwright + screenshot diffs) — beyond v7.9 scope.
