# GenizahSearch Documentation Index

> Last updated: 2026-11-05

This directory contains all project documentation, organized by category.

---

## Quick Links

| Need to... | Read this |
|------------|-----------|
| **See open bugs & issues** | [OPEN_ISSUES.md](OPEN_ISSUES.md) (defects), [BACKLOG.md](BACKLOG.md) (debt, test gaps, ideas, owner tasks); full text by ID in [ISSUE_DETAILS.md](ISSUE_DETAILS.md) |
| **Understand where code and data live, and who owns what** | [architecture/OVERVIEW.md](architecture/OVERVIEW.md), [architecture/DATA_LIFECYCLE.md](architecture/DATA_LIFECYCLE.md) |
| **See why a decision was made (and not re-open it)** | [decisions/](decisions/README.md) |
| **See feature ideas / what to build next** | [FEATURE_IDEAS.md](FEATURE_IDEAS.md) |
| Find where a file belongs | [architecture/OVERVIEW.md](architecture/OVERVIEW.md) |
| Understand the code structure | [CODE_INDEX.md](CODE_INDEX.md) |
| Check translation stats | [TRANSLATION_STATS.md](TRANSLATION_STATS.md) |

Server operation and admin guides are kept privately.

---

## Directory Structure

```
docs/
├── DOCUMENTATION_INDEX.md    # This file
├── OPEN_ISSUES.md            # Open defects, one line each by ID (AI agents MUST update)
├── BACKLOG.md                # Deferred defects, debt, test gaps, ideas, owner tasks
├── ISSUE_DETAILS.md          # Full text of every tracker/backlog row by GS-<n> (grep only)
├── CODE_INDEX.md             # Symbol-level index of the code
│
├── architecture/             # CURRENT ARCHITECTURE -- authoritative for where code and data live
│   ├── OVERVIEW.md               # Components, dependency rules + their tests, state ownership, decision tables
│   └── DATA_LIFECYCLE.md         # One row per data artifact
│
├── decisions/                # One page per binding decision (README.md indexes them)
├── TRANSLATION_STATS.md      # Translation coverage stats
├── FIST_GAP_FILL_STATS.md    # FIST gap-fill import statistics
│
├── guides/                   # How-to guides
│   ├── DEVELOPER_GUIDE.md        # Local development setup
│   ├── ENV_VARS.md              # Every environment variable (single source of truth)
│   └── SUPABASE_GUIDE.md         # Supabase database guide
│
└── specs/                    # Technical specifications
    ├── JOINS_TECHNICAL_SPEC.md
    └── JOINS_SIMPLIFIED_SPEC.md
```

---

## Architecture and decisions (start here)

| Page | What it answers |
|------|-----------------|
| [architecture/OVERVIEW.md](architecture/OVERVIEW.md) | Where code lives, the dependency rules and the tests that enforce them, who owns which state, and a decision for every top-level directory and root file |
| [architecture/DATA_LIFECYCLE.md](architecture/DATA_LIFECYCLE.md) | One row per data artifact: producer, consumers, class, where its location is decided, rebuild, validation, distribution |
| [decisions/](decisions/README.md) | The binding decisions -- permanent facade, sidecars instead of a backend process, bounded test runner, flag AND readiness, document classes, scripts namespace, flat tests, root assets, no CDN cache for the API |

### Which document is authoritative for what

| Class | Lives in | Rule |
|-------|----------|------|
| Current architecture | `architecture/`, `guides/`, `specs/` pages marked current | authoritative; kept current by `scripts/check_docs.py` and the tests |
| Planning records and archived history | the private repository, outside this tree | not linked from tracked docs |

The instruction files at the repository root -- [AGENTS.md](../AGENTS.md) (canonical commands and
layout rules), [CLAUDE.md](../CLAUDE.md) (project context for Claude Code),
[CONTRIBUTING.md](../CONTRIBUTING.md), [.cursorrules](../.cursorrules) and
[tests/README.md](../tests/README.md) (why the suite runs in bounded lanes) -- are kept consistent
with each other by the doc gates; AGENTS.md states each rule once and the others follow it.

---

## Guides

### For Developers
- **[DEVELOPER_GUIDE.md](guides/DEVELOPER_GUIDE.md)** - Getting started with local development
- **[ENV_VARS.md](guides/ENV_VARS.md)** - Every environment variable, its default, and why the non-obvious ones are set that way
- **[SUPABASE_GUIDE.md](guides/SUPABASE_GUIDE.md)** - Working with the Supabase database
- **[MULTITENANT.md](guides/MULTITENANT.md)** - v7.12 Path B multitenant architecture reference (safe_storage chokepoint, _session_uuid, request-scoped auth, deletion-not-migration discipline, tutorial for adding new per-user state values). Required reading for any developer extending web/ code that touches user-scoped state.
- **[TELEMETRY_RUNBOOK.md](guides/TELEMETRY_RUNBOOK.md)** - Desktop telemetry operational guide (shared PostHog project, key rotation, drop counters, self-test flag, opt-out behavior)

---

## Technical Specifications

Detailed specifications for complex features:

| Spec | Description |
|------|-------------|
| [JOINS_TECHNICAL_SPEC.md](specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [JOINS_SIMPLIFIED_SPEC.md](specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |
| [discovery-sidecar-schema-v1.md](specs/discovery-sidecar-schema-v1.md) | Frozen discovery-claim sidecar schema (Phase 134) |
| [discovery-v4-public-reference-expansion.md](specs/discovery-v4-public-reference-expansion.md) | V4 public Sefaria/Wikisource reference expansion — built, deployment owner-gated |
| [discovery-v4.1-public-source-and-reference-comparison-plan.md](specs/discovery-v4.1-public-source-and-reference-comparison-plan.md) | V4.1 public-source expansion and an internal reference-comparison track |
| [discovery-v4.2-combined-bake-and-public-first-plan.md](specs/discovery-v4.2-combined-bake-and-public-first-plan.md) | V4.2 combined bake, container sources, and public-first identities |
| [discovery-band-labels-v1.md](specs/discovery-band-labels-v1.md) | Honesty-safe band/label vocabulary (no precision percentages) |
| [discovery-relation-matrix-v1.md](specs/discovery-relation-matrix-v1.md) | Frozen relation precedence matrix (semantics frozen 2026-08-12) |
| [discovery-budgets.md](specs/discovery-budgets.md) | Discovery acceptance/performance budgets (PERF-01) |
| [discovery-performance-situation-2026-08-20.md](specs/discovery-performance-situation-2026-08-20.md) | **Situation assessment for a cold session** — the locus-filter outage, findings-page latency, the benchmark that hung the deploy, and the recipe that can no longer rebuild |
| [discovery-deploy.md](specs/discovery-deploy.md) | Discovery sidecar deploy / rollback / rebuild runbook (DATA-08) |
| [discovery-novelty-v1.md](specs/discovery-novelty-v1.md) | Novelty axis design, LLM gate contract, run cost/authorization |
| [atlas-asset-schema-v1.md](specs/atlas-asset-schema-v1.md) | Baked Atlas binary asset schema |
| [passage-matching-algorithm.md](specs/passage-matching-algorithm.md) | **The character-level passage matcher** — normalization, 5-gram indexing, diagonal two-hit seeding, the two acceptance boundaries, Stage-0 hygiene, and the interactive posting budget. Tracked authority for the algorithm |
| [passage-index-build-measurements.md](specs/passage-index-build-measurements.md) | Measured build cost, artifact size, construction comparison and RAM behaviour for the passage index (Phase 142) |
| [parallels-method-comparison.md](specs/parallels-method-comparison.md) | Head-to-head recall/precision of passage matching vs the incumbent chunk search (INTERIM, tuning split) |
| [parallels-holdout-prereg.md](specs/parallels-holdout-prereg.md) | Pre-registration for the one-shot parallels holdout run — endpoints, sampling, frozen inputs (DRAFT until its freeze block is filled) |

---

## Reference Documents

| Document | Description |
|----------|-------------|
| [TRANSLATION_STATS.md](TRANSLATION_STATS.md) | Translation coverage statistics |
| [FIST_GAP_FILL_STATS.md](FIST_GAP_FILL_STATS.md) | FIST gap-fill import statistics |

---

## Code Reference

- **[architecture/OVERVIEW.md](architecture/OVERVIEW.md)** - Where code lives and why (authoritative)
- **[CODE_INDEX.md](CODE_INDEX.md)** - Symbol-level index (classes, functions, line numbers), generated per module with `scripts/gen_code_index_section.py`

---

## Contributing to Documentation

When adding new documentation:

1. **Choose the right location:**
   - `guides/` - How-to guides and tutorials
   - `specs/` - Technical specifications
   - Root `docs/` - General project docs

2. **Use consistent naming:**
   - UPPERCASE for main documents (e.g., `FEATURE_SPEC.md`)
   - Underscores between words
   - Include date if time-sensitive (e.g., `AUDIT_2026-01-30.md`)

3. **Update this index** when adding new documents

4. **Archive** instead of deleting old documents (the archive is kept in the private repository)

---

## Root Directory Documentation

These files remain in the project root:

| File | Purpose |
|------|---------|
| `README.md` | Project overview and quick start |
| `CHANGELOG.md` | Version history |
| `AGENTS.md` | Canonical commands and layout rules (tool-neutral; the other instruction files follow it) |
| `CLAUDE.md` | Project context for Claude Code |
| `CONTRIBUTING.md` | Contributor quick start and the pre-PR gates |
| `.cursorrules` | Cursor IDE rules (points at AGENTS.md) |

---

*Last reorganization: 2026-09-18 (architecture/ and decisions/ added; FILE_INDEX archived)*
