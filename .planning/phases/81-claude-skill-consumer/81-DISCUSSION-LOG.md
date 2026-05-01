# Phase 81: Claude Skill Consumer — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `81-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-01
**Phase:** 81-claude-skill-consumer
**Areas discussed:** Skill format & location, Local-data hook, Ranking & justifications, Endpoint coverage, Error UX, Top N, Configuration

---

## Gray-area selection

| Option | Selected |
|--------|----------|
| Skill format & location | ✓ |
| Ranking & justification approach | ✓ |
| Endpoint coverage | ✓ |
| Error-handling UX & N candidates | ✓ |

---

## Skill Format

| Option | Description | Selected |
|--------|-------------|----------|
| Anthropic Skill (SKILL.md + scripts) | Frontmatter-headed SKILL.md + helper scripts; portable across Claude Code, Claude Desktop, claude.ai. | ✓ |
| Claude Code slash command + subagent | In-repo `.claude/commands/`, tightly coupled to Claude Code CLI. | |
| Standalone Python CLI (argparse) | Plain Python script, no Claude-specific surface. | |
| Hybrid: SKILL.md wraps Python CLI | Skill envelope + bundled CLI script. | |

**Rationale:** "Reference Claude skill" framing in SKILL-01 matches Anthropic Skill format most naturally.

## Skill Location

| Option | Description | Selected |
|--------|-------------|----------|
| Separate repo / external location | External to GenizahSearch repo; matches SC-1's "not pinned to a specific repo path." | ✓ |
| In-repo at `.claude/skills/genizah-search/` | Checked into GenizahSearch for discoverability. | |
| In-repo at `scripts/genizah_skill/` or `examples/` | Treated as code example. | |

**User note:** "We didn't discuss it, but the skill may make use of access to transcriptions.txt file itself, and/or to the local index, if the user installed the desktop app." → Triggered follow-up local-data discussion.

## Local-Data Hook (follow-up)

| Option | Description | Selected |
|--------|-------------|----------|
| API-only for v7.10; local-mode deferred | v7.10 ships pure-API; document local hook as v7.11. | |
| Auto-detect local data, fall back to API | Probe known paths, use local when present. | |
| Explicit `--local` flag, off by default | Power-user opt-in. | |
| Document the hook only; don't implement | API-only, but SKILL.md notes the extension point. | ✓ |

**Rationale:** Captures the user's insight without expanding v7.10 scope.

## Ranking & Justifications

| Option | Description | Selected |
|--------|-------------|----------|
| Hybrid: API order + Claude writes justification from browse text | Trust API ordering; Claude composes per-candidate justification grounded in browse response. | ✓ |
| Pass-through: API order + raw snippet | No browse fetch in ranking; doesn't satisfy SC-2's "grounded in /api/browse text." | |
| LLM rerank: Claude reorders top N after browse | More intelligent but non-deterministic. | |

## Endpoint Coverage

| Option | Description | Selected |
|--------|-------------|----------|
| search + browse only | Matches SC-1/SC-2 verbatim. | |
| search + browse + parallels (all three) | Full v7.10 surface in one artifact. | ✓ |
| search + browse in v7.10; parallels in v7.11 | Lean now, follow up. | |

## Error-Handling UX (SC-3)

| Option | Description | Selected |
|--------|-------------|----------|
| Per-candidate inline note + continue | One-line plain-text note per failure, skill keeps going, summary at end. | ✓ |
| Retry-once-with-backoff, then per-candidate note | More robust transport; more complex. | |
| Fail-fast: abort on first 429/timeout | Contradicts SC-3. | |

## Top N

| Option | Description | Selected |
|--------|-------------|----------|
| Default 5, configurable | Lean. | |
| Default 10, configurable | More breadth per query. | ✓ |
| Default 3, configurable | Very lean. | |

## Configuration

| Option | Description | Selected |
|--------|-------------|----------|
| Env var + CLI flag, env wins | `GENIZAH_API_BASE` + `--base-url`; both default to production. | ✓ |
| Env var only | One-knob surface. | |
| CLI flag only | Explicit at every invocation. | |

## Wrap-up

| Option | Description | Selected |
|--------|-------------|----------|
| Ship as-is — I have enough | Claude resolves remaining details (skill name, HTTP client, output format) as Claude's Discretion. | ✓ |
| Discuss skill name & invocation surface | | |
| Discuss acceptance-query sourcing | | |
| Discuss CI / smoke-test strategy | | |

---

## Claude's Discretion (deferred to planner)

- Skill name (`genizah-search` vs `cairo-genizah-research` etc.)
- HTTP client (`httpx` vs `requests`)
- Acceptance-query source (runtime-supplied vs sample suite — runtime preferred)
- Justification length and exact wording
- Skill output format (Markdown vs JSON — Markdown preferred)
- `/api/parallels` invocation surface (on-demand vs always)
- External skill artifact's hosting repo

## Deferred Ideas (captured in CONTEXT.md `<deferred>`)

- Local-data shortcut (Tantivy / transcriptions.txt) — v7.11
- Retry-with-backoff transport — v7.11
- LLM rerank after browse — v7.11
- In-repo CI smoke test — v7.11
- Curated sample-query suite — v7.11
- Justification quality eval — v7.11
- Multi-language UX — v7.11
