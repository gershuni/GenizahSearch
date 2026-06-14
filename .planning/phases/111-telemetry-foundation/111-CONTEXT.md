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

### PostHog Project & Key (operational)
- **D-01:** Target is a NEW desktop-only PostHog project — **"Dicta Genizah Desktop"**, EU host — separate from the web project (id 134161). **The PostHog MCP cannot create projects** (verified: no `project-create`/`team-create`/`environment-create` tool exists; the create API needs a personal key with org-write scope the MCP's OAuth key lacks). **Project creation is DEFERRED** — created via the PostHog UI or REST (with a personal key) **before Phase 114's first real events**. It is NOT a blocker for 111 because 111 fires no real events.
- **D-02:** Phase 111 builds against the env override with a **placeholder** publishable-key constant; the real `phc_...` key drops in before 114.

### Key Configuration
- **D-03:** The publishable key + host live as a **module constant in `desktop/telemetry.py`**, overridable via env: `GENIZAH_TELEMETRY_KEY` (+ a host override var, e.g. `GENIZAH_TELEMETRY_HOST`). This lets dev/staging builds target a test project and enables the self-test (D-06). The publishable/project key is **write-only → safe to embed in the binary and commit** (the web app already exposes its key publicly). NEVER embed a personal `phx_` key or the web project's key.

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

- PostHog project name: **"Dicta Genizah Desktop"** (EU), separate from web project 134161.
- Env override vars: `GENIZAH_TELEMETRY_KEY` (+ a host var) — drives both dev/staging targeting and the D-06 self-test.
- Self-test entry: a `--telemetry-selftest` CLI flag (dev-gated), never in the normal user path.
</specifics>

<deferred>
## Deferred Ideas

- **Real PostHog project + key creation** — operational; do it (UI or REST with a personal key) before Phase 114 wires the first real events. The MCP can't do it.
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
