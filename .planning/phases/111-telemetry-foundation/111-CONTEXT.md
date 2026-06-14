# Phase 111: Telemetry Foundation - Context

**Gathered:** 2026-06-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the **safety foundation** for desktop telemetry: the `desktop/telemetry.py` chokepoint module (8 public callables), consent + anonymous-uuid storage in `config.pkl`, the structural scrubber, the property + event-name allowlist, and backward-compatible additions to `shared/posthog_server.py`. **No events fire yet** — no producers are wired (that's Phases 113–115). This phase exists so that no event can ever reach PostHog before the consent gate, scrubber, and allowlist exist and are tested.

Requirements (from ROADMAP/REQUIREMENTS): CONSENT-01, CONSENT-05, CONSENT-06, CONSENT-07, INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, PRIV-01, PRIV-02, PRIV-06.

**Out of phase (later):** the first-run dialog + Settings toggle (112), exception hooks (113), usage events (114), perf events (115), CI privacy gate (116).
</domain>

<decisions>
## Implementation Decisions

### PostHog Project & Key (REVERSED 2026-06-14 → ONE shared project)
- **D-01:** Desktop sends to the **EXISTING shared web PostHog project** (id 134161, EU) — **NOT a separate project.** This reverses the earlier separate-project decision after the user invoked PostHog's own guidance (separate by ENVIRONMENT, not platform; keep apps + website in one production project) AND the fact that **the web already identifies logged-in users** (so a shared project yields real cross-surface journeys). Full rationale + the Gemini/Codex back-and-forth: `.planning/research/POSTHOG-PROJECT-DECISION.md`. Consequences: no new project, **no pay-as-you-go upgrade**, no deferred project creation, no MCP-can't-create-it problem.
- **D-01a:** Desktop embeds the **same publishable key the web app already uses** (`web/main.py:801` `_posthog_key`; already public in the web JS bundle). Web↔desktop separation in analysis is by the `platform=desktop` base property (USAGE-05) + a `desktop_` event-name namespace (D-07) — NOT by project.
- **D-02:** Phase 111 builds against the env override with a **placeholder** publishable-key constant; the real `phc_...` key drops in before 114.

### Identity & Cross-Surface Journey (NEW 2026-06-14 — reverses "anonymous, no account linkage")
- **D-07:** Desktop telemetry is **identity-aligned with the web app** so a logged-in researcher's web + desktop activity links in the shared project. Match `web/auth_state.py:160-170` EXACTLY: on login, `identify(distinct_id = supabase user.id)`; on logout, reset to anonymous. The web uses the raw **Supabase `user.id` (UUID)** as `distinct_id` — desktop MUST use the same value (a hash would NOT merge). Logged-OUT users → anonymous per-install `uuid4`; on login, emit `$identify` with `$anon_distinct_id = <per-install uuid>` to **alias/merge** the anonymous history into the person. All via the raw `shared/posthog_server.py` queue (hand-rolled `$identify`/alias events) — still **no SDK**.
- **D-08:** Desktop sends **only the `user.id` for identity — NOT email/name.** The web already attaches email/name to the shared person profile; desktop adds no new PII. Desktop still NEVER sends My Library/search content. So the desktop's per-payload guarantee = "no content; identity = the bare Supabase user id."
- **D-09:** Person-profile handling is split: anonymous (logged-out) events keep `$process_person_profile=false` (anonymous tier); identified (logged-in) events use real PostHog person profiles (required for journey stitching). This **amends** the old blanket `$process_person_profile=false` rule (USAGE-05).
- **D-10:** Every desktop event carries `platform=desktop` + uses a `desktop_` event-name namespace prefix, so events never collide with web event names and analysis can filter/break-down by platform within the shared project.
- **Observation (out of scope, flag only):** the web identifies real users (email/name) with **no opt-in consent gate**, while desktop is opt-in. A future "web consent gate" could harmonize this; not part of v8.1.0.

### Key Configuration
- **D-03:** The publishable key + host live as a **module constant in `desktop/telemetry.py`** = the **existing web project key** (`web/main.py:801`), overridable via env: `GENIZAH_TELEMETRY_KEY` (+ host var `GENIZAH_TELEMETRY_HOST`) for dev/staging targeting + the self-test (D-06). The publishable/project key is **write-only → safe to embed & commit** (web already exposes it). NEVER embed a personal `phx_` key.

### Consent-Gate Placement (architecture — LOAD-BEARING)
- **D-04:** The consent gate lives **ONLY in `desktop/telemetry.py`**. `shared/posthog_server.py` stays **UNGATED** so the existing web / NLI-circuit-breaker telemetry is unaffected. `posthog_server` gains only **neutral, backward-compatible additions** (default-`distinct_id` setter, `_flush_before_exit`, an **optional** scrub hook, opt-out queue-drain helper) that change nothing for existing callers or the 5 test monkeypatches targeting `_event_queue`. **This reconciles ROADMAP Phase-111 SC#5** — its `_telemetry_enabled` wording must NOT become a hard global gate inside the shared module (that would suppress web breaker telemetry by desktop consent). Desktop consent is enforced desktop-side, before `enqueue_event` is ever called.
- **D-05:** The desktop chokepoint is the **only** path from `desktop/` to `enqueue_event` (enforced by the PRIV-03 AST guard, mirroring `tests/test_no_raw_storage_access.py`). Every one of the 8 public callables gate-checks `is_enabled()` (a **cached** consent value), then runs the scrubber, then enqueues.

### Pipeline Verification (111 ships zero user-facing events)
- **D-06:** A **dev-only self-test path** (e.g. a `--telemetry-selftest` CLI flag that also honors the env-override key) emits ONE throwaway event so the pipeline can be verified end-to-end into the new project. It is **gated so it never fires in normal use** and ships no user-facing event in 111. This is how we prove the foundation reaches PostHog before real events are wired in 114.

### Claude's Discretion
- Exact event-name registry shape, scrubber redaction regexes, the precise property-allowlist contents, and the `config.pkl` key names are left to research/planner **within** the locked constraints above (allowlist-only for properties; fixed enum for event names; cached no-throw consent in any hook).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (locked)
- `.planning/REQUIREMENTS.md` — the 36 v8.1.0 requirements; Phase 111 covers CONSENT-01/05/06/07, INFRA-01..05, PRIV-01/02/06. MUST read.
- `.planning/ROADMAP.md` §"Phase 111: Telemetry Foundation" — goal + 5 success criteria.

### Research (this milestone)
- `.planning/research/SUMMARY.md` — synthesized decisions (no SDK, reuse queue, chokepoint, scrubber layers).
- `.planning/research/STACK.md` — SDK-vs-raw-queue verdict, zero-new-deps, key-embedding safety, uuid4 pattern.
- `.planning/research/ARCHITECTURE.md` — chokepoint design, the 8-callable API, `config.pkl` storage, scrubber, integration points with line refs.
- `.planning/research/PITFALLS.md` — 13 pitfalls (PII-via-traceback, consent correctness, daemon-thread loss, allowlist) → most belong to this foundation.
- `.planning/research/REQUIREMENTS-CODEX-CRITIQUE.md` — gap analysis that added PRIV-06 / CONSENT-08 / USAGE-06 / CRASH-07 / INFRA-06.

### Code to read / extend
- `shared/posthog_server.py` — the fire-and-forget queue to extend (NEUTRAL additions only; see D-04). Note its daemon thread + `maxsize=10000` + drop counter + EU `/capture` + `POSTHOG_API_KEY`.
- `genizah_core.py` — `Config` (~L2344-2378) + `load_app_config`/`save_app_config` (~L2871-2891) = the `config.pkl` store for consent/uuid (verify line numbers).
- `genizah_app.py:148-170` — existing `_setup_crash_handler` (relevant to Phase 113; the hook must chain, not replace — noted here for awareness).
- `web/analytics.py` — how web emits PostHog (parity awareness; web is browser-side).
- `web/auth_state.py:160-170` — **the identity contract to match**: web `posthog.identify(user.id, {email,name})` on login + `posthog.reset()` on logout. Desktop MUST use the same `supabase user.id` as `distinct_id` (D-07).
- `web/main.py:798-802` — web `posthog.init(_posthog_key, {api_host:'https://eu.i.posthog.com'})`; `_posthog_key` is the shared publishable key the desktop reuses (D-01a/D-03).
- `tests/test_no_raw_storage_access.py` — the AST-guard pattern to mirror for PRIV-03.
- `shared/joins_lab.py` + its Phase-106 AST import guard — precedent for a shared pure module + structural guard.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/posthog_server.enqueue_event(event, properties, distinct_id)` — the transport; reuse as-is, add neutral helpers (D-04). Zero new pip deps (STACK.md).
- `genizah_core.load_app_config` / `save_app_config` (`config.pkl`) — persistence for `telemetry_enabled`, `telemetry_first_run_shown`, `telemetry_install_id`, + consent audit fields. No new file, no QSettings (grep-confirmed zero QSettings).
- `tests/test_no_raw_storage_access.py` — copy its AST-scanner shape for the PRIV-03 chokepoint guard.

### Established Patterns
- **Chokepoint module + AST CI guard** — same pattern as `web/safe_storage.py` (Phase 87) and the Phase-106 `shared/joins_lab.py` import guard.
- **Shared module = no PyQt** — `shared/posthog_server.py` must stay PyQt-free; the consent gate + dialog live in `desktop/` (D-04, D-05).
- **Fire-and-forget, never block the UI thread** — preserved; offline/missing-key degrades silently; events memory-only (no disk spool, INFRA-05).

### Integration Points
- NEW `desktop/telemetry.py` (the only `desktop/`→`posthog_server` path).
- `shared/posthog_server.py` neutral additions (default distinct_id, flush-before-exit, optional scrub hook, queue-drain helper).
- `config.pkl` gains telemetry keys.
- Build/packaging: no PyInstaller spec change needed (STACK.md — `shared` already bundled; pure-Python module auto-discovered).
</code_context>

<specifics>
## Specific Ideas

- PostHog target: the **existing shared web project** (id 134161, EU); reuse the web publishable key (`web/main.py:801`).
- Identity: `distinct_id = supabase user.id` for logged-in users (match `web/auth_state.py`); anonymous per-install uuid4 otherwise, aliased on login.
- Env override vars: `GENIZAH_TELEMETRY_KEY` (+ host var) — dev/staging targeting + the D-06 self-test.
- Self-test entry: a `--telemetry-selftest` CLI flag (dev-gated), never in the normal user path.
- Discipline replacing "separate project": `platform=desktop` base prop + `desktop_` event-name namespace.
</specifics>

<deferred>
## Deferred Ideas

- **(No project creation needed)** — reusing the shared web project removes the create/upgrade/key-drop steps entirely. The web's publishable key is reused as the `desktop/telemetry.py` constant.
- **Web consent gate** (observation, out of scope) — web identifies real users without an opt-in gate; a future harmonization could add one.
- **WEB-F1** (strip web `search_executed` query text) — now nice-to-have for the shared project, but it's a web-side change and stays in Future; the shared project already carries web PII/content regardless, so the desktop guarantee is per-payload (desktop sends none), not project-wide.
- Everything in Phases 112–116 (consent UX, exception hooks, usage/perf events, CI privacy gate) — out of this phase by the foundation-first design.

### Reviewed Todos (not folded)
All 7 pending todos were reviewed against Phase 111 scope; **none relate to telemetry** (the phase-match scores were false positives on generic words):
- Migrate desktop corrections fetch to shared corrections_service — unrelated (corrections, not telemetry).
- Reading Desk UX fixes — unrelated UI.
- Server-side search with email notification — unrelated feature.
- Unified metadata text search with translations — unrelated feature.
- One-click scholarly citations — unrelated feature.
- Fill missing genizah manuscripts from FIST.db — unrelated data work.
- (7th) NLI MARC crawl — unrelated data work.
</deferred>

---

*Phase: 111-telemetry-foundation*
*Context gathered: 2026-06-14*
