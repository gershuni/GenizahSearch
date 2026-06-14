# Decision record — one PostHog project or two? (v8.1.0 Desktop Telemetry)

**Date:** 2026-06-14
**Decision:** **Separate desktop-only PostHog project** ("Dicta Genizah Desktop", EU), distinct from the web project (id 134161). **CONFIRMED.**
**Status:** Re-confirmed after a Gemini recommendation to the contrary; validated by independent Codex review + user billing decision.

## Why this was reconsidered

A Gemini "PostHog Telemetry Architecture Recommendation" argued for **consolidating** desktop events into the existing web project (tag with a `client_type`/`platform` super-property, separate via dashboard filters). Its two core claims:
1. **Cross-platform journey tracking** — one researcher = one Distinct ID across web+desktop.
2. **Billing friction** — a separate project "necessitates a new project with a payment method".

## Verdict: keep them separate (Option A). Gemini's premises don't hold here.

| Gemini claim | Reality for THIS project |
|---|---|
| Identity unification (single Distinct ID) | **FALSE.** Desktop = anonymous per-install `uuid4`, `$process_person_profile=false`, no account linkage; web uses a *separate* browser-generated anon id. There is no shared identity to preserve — consolidation would only co-locate unrelated anonymous streams. (Codex: PostHog cross-platform continuity requires intentional `identify`/shared ids — which we deliberately avoid.) |
| Per-project billing / new payment method | **Mostly FALSE.** PostHog billing + free monthly volume are **org-level**, not per-project; no per-project card. **Caveat (Codex, verified via MCP):** the Dicta org currently has **1 project / 1 member = the free 1-project plan**, so a 2nd project requires a one-time **org upgrade to pay-as-you-go** (card on file; free monthly allowance still applies → ~$0 at desktop volume). User accepted this 2026-06-14. |

## Deciding reasons (both reviewers agree)

1. **No real cross-platform identity to gain** — the anonymous, no-linkage design makes Gemini's headline benefit moot.
2. **Clean privacy invariant** — a separate project can guarantee "never receives query text / paths / filenames / content." The web project already carries query text (web `search_executed` → `query: clean_query[:100]`, the WEB-F1 gap), so consolidation weakens and complicates that guarantee.
3. **Isolated desktop volume/quota monitoring** — directly serves the user's perf-event-volume concern (~50 searches/day × dozens of users); no analyst "always filter by platform" tax.

## Sources

- Codex (`gpt-5.5`, xhigh) independent review — verdict Option A; brief `_tmp/codex-posthog-project-decision-brief.md`, output `_tmp/codex-posthog-project-decision-output.md`. Cited PostHog docs: identify (cross-platform requires shared ids), projects (data silos), pricing (free plan = 1 project, PAYG = 6).
- MCP `organization-get` / `projects-get` (2026-06-14): org "Dicta" → 1 project (134161), 1 member → free single-project plan.

## Third option (adopted as follow-up, not a blocker)

Keep separate now **and** fix the web query-text gap separately (already tracked as **WEB-F1** in `.planning/REQUIREMENTS.md` Future). If consolidation were ever forced, the fallback would be a strict `desktop_` event-name namespace + mandatory `platform=desktop` — explicitly second-best.
