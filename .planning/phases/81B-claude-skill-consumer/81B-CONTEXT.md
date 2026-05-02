# Phase 81: Claude Skill Consumer — Context

**Gathered:** 2026-05-01
**Status:** ⚠ SUPERSEDED (provisionally) by `81-RESCOPE.md` pending user sign-off. After sign-off this file moves to `81B-claude-skill-consumer/81B-CONTEXT.md` with overrides from RESCOPE §4 applied.

<domain>
## Phase Boundary

A runnable **Anthropic Skill** (SKILL.md + bundled Python script(s)) that drives `/api/search` → `/api/browse` → `/api/parallels` end-to-end against a configurable GenizahSearch deployment, producing a ranked candidate list with browse-text-grounded justifications. The skill itself lives **external to the GenizahSearch repo** — its filesystem location is environment-specific per SKILL-01 / SC-1.

**v7.10 acceptance harness.** Phase gate is a live end-to-end run against the production deployment with the user observing, signed off on at least one scholarly query.

**In scope:** the skill's source (SKILL.md + helper script), its config surface, its error-handling behavior, and one user-observed acceptance run. **Out of scope:** any change to `/api/search`, `/api/browse`, `/api/parallels`, or the serializer — those are locked by Phases 77–80.

</domain>

<decisions>
## Implementation Decisions

### Format & Location

- **D-01: Artifact format — Anthropic Skill (SKILL.md + scripts).** Frontmatter-headed `SKILL.md` instructs the model; bundled Python script(s) handle HTTP, retries, and JSON parsing. Portable across Claude Code, Claude Desktop, and claude.ai. Not a Claude Code slash-command and not a bare Python CLI.
- **D-02: Skill location — external to GenizahSearch repo.** Honors SC-1's "filesystem location is environment-specific and not pinned to a specific repo path." The Phase 81 deliverable is the skill artifact itself (committed to a separate skills repo or `~/.claude/skills/`), not a checked-in directory inside this repo. CONTEXT.md, planning artifacts, and any pointer doc remain in `.planning/phases/81-...`.
- **D-03: Local-data hook — documented, not implemented.** SKILL.md explicitly notes a future extension point: when the user has the GenizahSearch desktop app installed, the skill could optionally read `Genizah_Index/` (Tantivy index) and/or `transcriptions.txt` directly to skip `/api/search` calls. v7.10 ships **API-only**; the local-data path is captured here so a future contributor can wire it without rediscovery. Add to deferred ideas as v7.11+ candidate.

### Endpoint Coverage

- **D-04: Skill exercises all three endpoints — `/api/search`, `/api/browse`, `/api/parallels`.** Demonstrates the full v7.10 surface as a single artifact. Honors the Phase 81 "depends on Phase 80" framing in ROADMAP.md. Justification logic must handle two distinct result shapes (search items vs parallels groups with `matches[]`) — see D-08.

### Ranking & Justifications (SC-2)

- **D-05: Hybrid ranking — API order + Claude-authored justification per candidate.** Skill calls `/api/search` (or `/api/parallels`), takes top N candidates in API order, fetches `/api/browse` for each, then Claude composes a 1–2 sentence justification per candidate **grounded in the fetched browse text** (SC-2 requirement: traceable to a specific browse response). API ordering is trusted; justifications add the grounded reasoning layer. No LLM rerank in v7.10 — keeps output deterministic and auditable.
- **D-06: Top N — default 10, configurable.** Skill fetches and browses 10 candidates by default. Bounded floor/ceiling to be set by planner (suggested: `[1, 25]`) so a runaway query cannot fan out to hundreds of `/api/browse` calls (rate-limit hygiene + per-candidate justification cost).

### Error-Handling UX (SC-3)

- **D-07: Per-candidate inline note + continue.** On 429, timeout, or partial-NLI response from `/api/browse`, the failing candidate gets a one-line plain-text note in the output (e.g., `"browse failed: rate-limited, retry-after 12s"` or `"NLI image unavailable for this fragment"`). The skill **keeps processing remaining candidates**. Final output includes a summary line counting successes and failures. SC-3's "does not crash the conversation; surfaces failure in plain terms; continues processing remaining candidates where possible" — satisfied.
- **D-08: No retry logic in v7.10.** First failure on a candidate produces the inline note and moves on. Retry-with-backoff is a v7.11 candidate (deferred). Rationale: keeps the skill's transport layer simple and the failure surface easy to audit during the user-observed acceptance run.

### Configuration

- **D-09: Base URL — env var + CLI flag, env wins.** `GENIZAH_API_BASE` env var; `--base-url` CLI flag overrides. Both default to `https://genizahsearch.com`. Documented in SKILL.md frontmatter and skill body.
- **D-10: Top-N override.** `GENIZAH_TOP_N` env var or `--top-n` CLI flag. Defaults to 10 (D-06).

### Result-Shape Handling

- **D-11: Justification logic differs by endpoint.**
  - `/api/search`: one justification per result item, grounded in `/api/browse` response for that item's `locator`.
  - `/api/parallels`: one justification per group (per `sys_id`), with the per-group `matches[]` array surfaced as supporting evidence; `/api/browse` fetched once per group using the group-level locator.
  - Skill must read both `uid` (preferred) and `locator: {sys_id, volume_ie, p_num}` (fallback) per Phase 77 D-13.

### Acceptance Run

- **D-12: Phase gate is a live user-observed run.** No in-repo CI smoke test for v7.10. The skill lives external to the repo, so CI cannot reach it; live acceptance is the only gate. ROADMAP.md Phase 81 phase-gate line is authoritative ("live end-to-end run against the production deployment with the user observing; user-signed-off ranking against at least one scholarly query").

### Claude's Discretion

- **Skill name** — `genizah-search`, `cairo-genizah-research`, or similar. Planner picks; user can override in SKILL.md frontmatter.
- **HTTP client** — `httpx` (async-capable) vs `requests` (sync). Sync is simpler for a Skill helper script; async only matters if the skill fans out >5 browses concurrently. Planner picks.
- **Acceptance-query source** — runtime user-supplied vs a sample-query suite shipped with the skill. Planner can include 1–2 example queries in SKILL.md as documentation; the live phase gate uses a query the user picks at run time.
- **Justification length / format** — 1–2 sentences per candidate, plain text. Planner can tune wording; SC-2 only requires "brief justifications grounded in the text."
- **Skill output format** — Markdown (numbered list with shelfmark + justification + locator) or JSON. Markdown is more conversational; JSON is more programmatic. Planner picks Markdown unless user disagrees.
- **`/api/parallels` invocation surface** — does the skill drive parallels on demand (e.g., user provides a composition string), or as part of every search run? Planner picks; default recommendation: separate sub-mode invoked when input looks like a composition (multi-line text > 200 chars), otherwise `/api/search`.
- **Repo for the external skill artifact** — user's personal skills repo, a public skills gallery, or a private gist. Out of GenizahSearch's scope.

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 81: Claude Skill Consumer` — SC-1, SC-2, SC-3 and the live-run phase gate.
- `.planning/REQUIREMENTS.md` §`API Consumer Skill` — SKILL-01, SKILL-02, SKILL-03.
- `.planning/STATE.md` — v7.10 milestone position; Phases 78, 79, 80 verified PASSED before Phase 81 starts.

### API contract (locked — skill consumes, never modifies)
- `.planning/phases/77-serializer-json-export/77-CONTEXT.md` — D-13 (`matches[]` per uid from `chunk_hits`), D-14 (`serialize_parallels_payload` is sole producer of result item shape). Locator-on-every-item lives here.
- `shared/search_serializer.py` — canonical envelope shape. Skill parses `results[]`, `filtered[]`, `warnings[]`, `count`, `total`, `locator`, `uid`.
- `shared/api_errors.py` — error-code taxonomy (`rate_limited`, `composition_required`, `composition_too_long`, `truncated_to_200`, etc.). Skill maps these to plain-text inline notes per D-07.

### Endpoint behavior (locked)
- `.planning/phases/78-api-search-hardening-shell/78-CONTEXT.md` — D-01..D-24 hardening. Especially D-06 (per-route exception handling), D-07 (error-code taxonomy), D-15 (FiltersModel), D-20 (statelessness), D-14 (PostHog event shape).
- `.planning/phases/79-api-browse-drill-down/79-CONTEXT.md` — locator round-trip semantics; partial-NLI behavior the skill must tolerate (SC-3).
- `.planning/phases/80-api-parallels/80-CONTEXT.md` — D-01 (request shape), D-04 (filtered-key always present), D-07 (200-group cap + `truncated_to_200` warning), D-09 (mode property values).
- `web/search_api.py` — concrete request/response surface for all three endpoints. Skill matches this shape exactly.

### Local-data extension point (D-03 — documented, not implemented)
- `genizah_core.py` — Tantivy reader entry points the local-mode shortcut would call.
- `Genizah_Index/` — Tantivy index location on a desktop install.
- `transcriptions.txt` — raw transcription source on a desktop install.

### Skill-format references (external)
- Anthropic Skills documentation (SKILL.md frontmatter format) — the planner / researcher fetches the current spec; not pinned here because it evolves outside our repo.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets (consumed, not modified)
- **`shared/search_serializer.py`** — canonical envelope. Skill reads `uid`, `locator`, `results[].metadata`, `results[].snippet`, `matches[]`, `warnings[]`. No reimplementation of grouping or ranking logic.
- **`shared/api_errors.py`** — error-code taxonomy. Skill maps codes → plain-text inline notes (D-07).
- **`web/search_api.py`** — single source of truth for request shapes (`SearchRequest`, `BrowseRequest`, `ParallelsRequest`, `FiltersModel`).

### Established Patterns
- **Locator round-trip** — every result item carries `uid` + `locator: {sys_id, volume_ie, p_num}`. Phase 79 proved the round-trip; Phase 81 skill is the first cross-endpoint consumer to exercise it in production.
- **Statelessness contract (78-D-20)** — skill can re-issue identical requests without correlation IDs; the API doesn't track sessions.
- **Per-endpoint rate-limit buckets (Phase 78/79/80 D-05)** — three independent 30 req/min buckets. Skill's top-N=10 budget (10 search + 10 browse + occasional parallels = ~21 requests / scholarly query) sits well inside each bucket. No need for client-side coordination.

### Integration Points
- **Skill is external** — no integration into `web/`, `shared/`, `desktop/`, or `tests/`. The only artifact in this repo is `.planning/phases/81-claude-skill-consumer/` (planning + summary docs).
- **Live deployment URL** — `https://genizahsearch.com/api/{search,browse,parallels}` is the default base. Skill uses production for the acceptance run unless the user overrides via `GENIZAH_API_BASE`.

</code_context>

<specifics>
## Specific Ideas

- **One scholarly query for the live gate.** ROADMAP.md phase-gate language is "at least one." Use a real query the user has on hand at acceptance-run time — don't hard-code a curated list.
- **Justification grounding.** Each justification cites the `uid` of the browse response it drew from, so a reviewer can pull the exact `/api/browse` payload and verify the claim (SC-2 traceability).
- **Error-summary line.** End of run: `"Processed 10 candidates: 8 succeeded, 1 rate-limited, 1 NLI image unavailable."` — single-line plain-text summary so the user can spot upstream health at a glance during acceptance.
- **Locator preference order.** Skill uses `uid` when present; falls back to `{sys_id, volume_ie, p_num}` only when `uid` is missing — matches Phase 77 D-13.

</specifics>

<deferred>
## Deferred Ideas

- **Local-data shortcut (D-03).** When desktop app is installed, skill could read `Genizah_Index/` (Tantivy) and/or `transcriptions.txt` directly to bypass `/api/search`. v7.11 candidate. Document the hook in SKILL.md but don't implement.
- **Retry-with-backoff (D-08).** First failure currently terminates that candidate. v7.11 could add one retry with `Retry-After` honor on 429s, exponential backoff on timeouts.
- **LLM rerank.** v7.11 could let Claude reorder top-N after browse, instead of trusting API order. Trade-off: more "intelligent" ranking vs less deterministic / harder to audit.
- **In-repo CI smoke test.** Skill is external to repo per D-02, so no CI hook in v7.10. v7.11 could add a thin in-repo script that hits a live deployment as a CI smoke test, decoupled from the actual Skill artifact.
- **Curated sample-query suite.** v7.11 could ship 5–10 example scholarly queries with the skill for benchmarking and regression.
- **Justification quality eval.** No formal eval of justification quality in v7.10 — gate is one user-observed run. v7.11 could add an LLM-judge or rubric eval.
- **Multi-language UX.** Skill output is English by default; Hebrew or bilingual output is a v7.11 consideration.

</deferred>

---

*Phase: 81-claude-skill-consumer*
*Context gathered: 2026-05-01*
