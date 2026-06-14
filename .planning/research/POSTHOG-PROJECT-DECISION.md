# Decision record — one PostHog project or two? (v8.1.0 Desktop Telemetry)

**Date:** 2026-06-14
**FINAL DECISION:** **ONE shared PostHog project** — desktop reuses the existing web project (id 134161, EU) + its publishable key, **identity-aligned** with the web app (logged-in users → same Supabase `user.id`). Web↔desktop separation by `platform=desktop` + a `desktop_` event-name namespace.
**Status:** REVERSED from the initial "separate project" call after (a) the user invoked PostHog's documented guidance (separate by **environment**, not platform; keep apps + website in one production project) and (b) the decisive discovery that **the web app already `identify()`s logged-in users by `user.id`** (`web/auth_state.py:160-170`) — so a shared project delivers real cross-surface journeys with zero web changes. The "no identity to unify" argument that drove the initial separate call no longer holds.

## What changed (the decisive fact)

The initial analysis (below) recommended SEPARATE, resting heavily on "there's no shared identity to unify, so consolidation buys little." That premise was **wrong**: the web already calls `posthog.identify(user.id, {email,name})` on login and `posthog.reset()` on logout. Desktop identifying the *same* logged-in user by the *same* `user.id` makes web + desktop merge into one person — the exact cross-surface journey the user wants ("which user downloaded the app; what they search here vs there"). That, plus PostHog's own "one production project for apps + website" guidance, plus zero billing/PAYG friction, flips the verdict to **one shared project**. Desktop still sends no content and only the bare `user.id` for identity (no email/name from desktop).

## Initial analysis (superseded — retained for history)

## Why this was reconsidered

A Gemini "PostHog Telemetry Architecture Recommendation" argued for **consolidating** desktop events into the existing web project (tag with a `client_type`/`platform` super-property, separate via dashboard filters). Its two core claims:
1. **Cross-platform journey tracking** — one researcher = one Distinct ID across web+desktop.
2. **Billing friction** — a separate project "necessitates a new project with a payment method".

## Initial verdict (SUPERSEDED): keep them separate (Option A)

> ⚠️ This was the FIRST-PASS verdict, before discovering the web already identifies users. The fact-check below is still accurate, but the conclusion was reversed — see "What changed" above. The billing fact-check remains useful; the identity fact-check was correct *only* for the original anonymous-no-linkage design, which we then changed.

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
