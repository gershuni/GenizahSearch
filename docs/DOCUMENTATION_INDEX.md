# GenizahSearch Documentation Index

> Last updated: 2026-09-18

This directory contains all project documentation, organized by category.

---

## Quick Links

| Need to... | Read this |
|------------|-----------|
| **See open bugs & issues** | [OPEN_ISSUES.md](OPEN_ISSUES.md) |
| **Understand where code and data live, and who owns what** | [architecture/OVERVIEW.md](architecture/OVERVIEW.md), [architecture/DATA_LIFECYCLE.md](architecture/DATA_LIFECYCLE.md) |
| **See why a decision was made (and not re-open it)** | [decisions/](decisions/README.md) |
| **See feature ideas / what to build next** | [FEATURE_IDEAS.md](FEATURE_IDEAS.md) |
| **Know what v9.0.0 actually shipped vs. planned** | [v9.0.0 Milestone Audit](../.planning/milestones/v9.0.0-MILESTONE-AUDIT.md) — 81 verified gaps, ranked; supersedes the archived v9.0.0 roadmap wherever they disagree |
| Deploy the website | [guides/DEPLOYMENT_TECHNICAL.md](guides/DEPLOYMENT_TECHNICAL.md) |
| Manage browser extension | [guides/DEPLOYMENT_TECHNICAL.md#browser-extension](guides/DEPLOYMENT_TECHNICAL.md#browser-extension-genizahsearch-image-helper) |
| Manage the website (non-technical) | [guides/WEBSITE_ADMIN_GUIDE.md](guides/WEBSITE_ADMIN_GUIDE.md) |
| Find where a file belongs | [architecture/OVERVIEW.md](architecture/OVERVIEW.md) -- the 2026-03 file listing is archived at `archive/FILE_INDEX_2026-03-26.md` |
| Understand the code structure | [CODE_INDEX.md](CODE_INDEX.md) |
| See future implementation plans | [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) |
| Check translation stats | [TRANSLATION_STATS.md](TRANSLATION_STATS.md) |

---

## Directory Structure

```
docs/
├── DOCUMENTATION_INDEX.md    # This file
├── OPEN_ISSUES.md            # Active issue tracker (AI agents MUST update)
├── CODE_INDEX.md             # Symbol-level index of the code
│
├── architecture/             # CURRENT ARCHITECTURE -- authoritative for where code and data live
│   ├── OVERVIEW.md               # Components, dependency rules + their tests, state ownership, decision tables
│   └── DATA_LIFECYCLE.md         # One row per data artifact
│
├── decisions/                # One page per binding decision (README.md indexes them)
├── TRANSLATION_STATS.md      # Translation coverage stats
├── FJMS_API_REFERENCE.md     # FJMS API working reference
├── FIST_GAP_FILL_STATS.md    # FIST gap-fill import statistics
│
├── guides/                   # How-to guides
│   ├── WEBSITE_ADMIN_GUIDE.md    # For site admins (non-technical)
│   ├── DEPLOYMENT_TECHNICAL.md   # Technical deployment guide
│   ├── DEVELOPER_GUIDE.md        # Local development setup
│   ├── ENV_VARS.md              # Every environment variable (single source of truth)
│   └── SUPABASE_GUIDE.md         # Supabase database guide
│
├── plans/                    # Future implementation plans
│   ├── PLANS_INDEX.md            # Index of plans
│   ├── MOBILE_RESPONSIVE_PLAN.md
│   ├── JOIN_FINDER_IMPLEMENTATION_PLAN.md
│   └── USER_TEXT_SEARCH_PLAN.md
│
├── specs/                    # Technical specifications
│   ├── JOINS_TECHNICAL_SPEC.md
│   ├── JOINS_SIMPLIFIED_SPEC.md
│   └── PUZZLE_WEB_TECHNICAL_SPEC.md
│
└── archive/                  # Historical documents; each names its successor where one exists
    ├── plans/                    # Completed plan files + responsa-search/
    ├── codebase-snapshot-2026-02-05/  # The GSD codebase map, pre-v8.3.0 (superseded by architecture/)
    ├── FILE_INDEX_2026-03-26.md  # The old file listing (superseded by architecture/OVERVIEW.md)
    ├── CODE_REVIEW_*.md          # One-time code reviews
    ├── POSTHOG_ANALYTICS_*.md    # Point-in-time analytics snapshots
    ├── TEST_REPORT_AREAS_*.md    # One-time test reports
    ├── *_HANDOFF.md              # Session handoffs
    ├── SUPABASE_MIGRATION_PLAN.md
    └── ...                       # Other historical docs
```

---

## Architecture and decisions (start here)

| Page | What it answers |
|------|-----------------|
| [architecture/OVERVIEW.md](architecture/OVERVIEW.md) | Where code lives, the dependency rules and the tests that enforce them, who owns which state, and a decision for every top-level directory and root file |
| [architecture/DATA_LIFECYCLE.md](architecture/DATA_LIFECYCLE.md) | One row per data artifact: producer, consumers, class, where its location is decided, rebuild, validation, distribution |
| [decisions/](decisions/README.md) | The binding decisions -- permanent facade, sidecars instead of a backend process, bounded test runner, flag AND readiness, document classes, scripts namespace, flat tests, root assets, masked-corpus rule |

### Which document is authoritative for what

| Class | Lives in | Rule |
|-------|----------|------|
| Current architecture | `architecture/`, `guides/`, `specs/` pages marked current | authoritative; kept current by `scripts/check_docs.py` and the tests |
| Active planning | `.planning/ROADMAP.md`, `STATE.md`, `PROJECT.md`, `.planning/phases/` | owned by the GSD tooling; edited only through it |
| Audit evidence | `.planning/milestones/*-MILESTONE-AUDIT.md`, `.planning/phase87_storage_allowlist.yaml`, phase decision files tests read by path | cited as evidence; never moved |
| Archived history | `archive/`, `.planning/milestones/*-phases/`, `.planning/quick/`, `.planning/debug/` | banner names the successor; excluded from the doc gates |

The instruction files at the repository root -- [AGENTS.md](../AGENTS.md) (canonical commands and
layout rules), [CLAUDE.md](../CLAUDE.md) (project context for Claude Code),
[CONTRIBUTING.md](../CONTRIBUTING.md), [.cursorrules](../.cursorrules) and
[tests/README.md](../tests/README.md) (why the suite runs in bounded lanes) -- are kept consistent
with each other by the doc gates; AGENTS.md states each rule once and the others follow it.

---

## Guides

### For Site Administrators
- **[WEBSITE_ADMIN_GUIDE.md](guides/WEBSITE_ADMIN_GUIDE.md)** - Non-technical guide for managing the website
  - Quick commands cheat sheet
  - Troubleshooting common issues
  - Supabase dashboard guide
  - Cockpit server management

### For Developers
- **[DEVELOPER_GUIDE.md](guides/DEVELOPER_GUIDE.md)** - Getting started with local development
- **[ENV_VARS.md](guides/ENV_VARS.md)** - Every environment variable, its default, and why the non-obvious ones are set that way
- **[DEPLOYMENT_TECHNICAL.md](guides/DEPLOYMENT_TECHNICAL.md)** - Technical deployment and configuration
- **[SUPABASE_GUIDE.md](guides/SUPABASE_GUIDE.md)** - Working with the Supabase database
- **[MULTITENANT.md](guides/MULTITENANT.md)** - v7.12 Path B multitenant architecture reference (safe_storage chokepoint, _session_uuid, request-scoped auth, deletion-not-migration discipline, tutorial for adding new per-user state values). Required reading for any developer extending web/ code that touches user-scoped state.
- **[TELEMETRY_RUNBOOK.md](guides/TELEMETRY_RUNBOOK.md)** - Desktop telemetry operational guide (shared PostHog project, key rotation, drop counters, self-test flag, opt-out behavior)

---

## Future Plans

| Plan | Status | Description |
|------|--------|-------------|
| [MOBILE_RESPONSIVE_PLAN.md](plans/MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOIN_FINDER_IMPLEMENTATION_PLAN.md](plans/JOIN_FINDER_IMPLEMENTATION_PLAN.md) | Planned | Direction-aware join finder for manuscript view |
| [USER_TEXT_SEARCH_PLAN.md](plans/USER_TEXT_SEARCH_PLAN.md) | Planned | User-added text search |

See [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) for details.

---

## Technical Specifications

Detailed specifications for complex features:

| Spec | Description |
|------|-------------|
| [JOINS_TECHNICAL_SPEC.md](specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [JOINS_SIMPLIFIED_SPEC.md](specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |
| [PUZZLE_WEB_TECHNICAL_SPEC.md](specs/PUZZLE_WEB_TECHNICAL_SPEC.md) | Web puzzle architecture, blockers, and solution paths |
| [discovery-sidecar-schema-v1.md](specs/discovery-sidecar-schema-v1.md) | Frozen discovery-claim sidecar schema (Phase 134) |
| [discovery-frames.md](specs/discovery-frames.md) | discovery-v1 build frame (superseded — never deployed) |
| [discovery-frames-v2.md](specs/discovery-frames-v2.md) | discovery-v2 build frame — first artifact deployed (2026-07-28) |
| [discovery-frames-v2.1.md](specs/discovery-frames-v2.1.md) | discovery-v2.1 additive rebuild — the artifact currently serving `/computed-identifications` |
| [discovery-v2-bake-plan.md](specs/discovery-v2-bake-plan.md) | discovery-v2 bake procedure and owner sign-off |
| [discovery-v3-bake-plan.md](specs/discovery-v3-bake-plan.md) | discovery-v3 (gen-2 evidence pipeline) bake plan — Codex-approved, NOT deployed |
| [discovery-v3-naming.md](specs/discovery-v3-naming.md) | Resolves the "v2.1" naming collision between the deployed rebuild and the gen-2 refresh |
| [discovery-v3-masking-evidence.md](specs/discovery-v3-masking-evidence.md) | Masking-scan evidence for the discovery-v3 track |
| [discovery-v4-public-reference-expansion.md](specs/discovery-v4-public-reference-expansion.md) | V4 public Sefaria/Wikisource reference expansion — built, deployment owner-gated |
| [discovery-v4.1-public-source-and-r-shadow-plan.md](specs/discovery-v4.1-public-source-and-r-shadow-plan.md) | V4.1 public-source expansion and the R-source shadow track |
| [discovery-v4.2-combined-bake-and-public-first-plan.md](specs/discovery-v4.2-combined-bake-and-public-first-plan.md) | V4.2 combined bake, container sources, and public-first identities |
| [discovery-band-labels-v1.md](specs/discovery-band-labels-v1.md) | Honesty-safe band/label vocabulary (no precision percentages) |
| [discovery-relation-matrix-v1.md](specs/discovery-relation-matrix-v1.md) | Frozen relation precedence matrix (semantics frozen 2026-08-12) |
| [discovery-budgets.md](specs/discovery-budgets.md) | Discovery acceptance/performance budgets (PERF-01) |
| [discovery-performance-situation-2026-08-20.md](specs/discovery-performance-situation-2026-08-20.md) | **Situation assessment for a cold session** — the locus-filter outage, findings-page latency, the benchmark that hung the deploy, and the recipe that can no longer rebuild |
| [discovery-deploy.md](specs/discovery-deploy.md) | Discovery sidecar deploy / rollback / rebuild runbook (DATA-08) |
| [discovery-coordination.md](specs/discovery-coordination.md) | Cross-phase discovery coordination notes |
| [discovery-forward-ledger.md](specs/discovery-forward-ledger.md) | Forward ledger of deferred discovery decisions |
| [discovery-cert01-protocol.md](specs/discovery-cert01-protocol.md) | CERT-01 precision-certificate protocol (pre-registration, draw, grading) |
| [discovery-novelty-v1.md](specs/discovery-novelty-v1.md) | Novelty axis design, LLM gate contract, run cost/authorization |
| [atlas-asset-schema-v1.md](specs/atlas-asset-schema-v1.md) | Baked Atlas binary asset schema |
| [v3-review-viewer-spec.md](specs/v3-review-viewer-spec.md) | Owner-only LOCAL discovery-v3 grading server (never deployed) |
| [passage-matching-algorithm.md](specs/passage-matching-algorithm.md) | **The character-level passage matcher** — normalization, 5-gram indexing, diagonal two-hit seeding, the two acceptance boundaries, Stage-0 hygiene, and the interactive posting budget. Tracked authority for the algorithm |
| [passage-index-build-measurements.md](specs/passage-index-build-measurements.md) | Measured build cost, artifact size, construction comparison and RAM behaviour for the passage index (Phase 142) |
| [parallels-method-comparison.md](specs/parallels-method-comparison.md) | Head-to-head recall/precision of passage matching vs the incumbent chunk search (INTERIM, tuning split) |
| [parallels-holdout-prereg.md](specs/parallels-holdout-prereg.md) | Pre-registration for the one-shot parallels holdout run — endpoints, sampling, frozen inputs (DRAFT until its freeze block is filled) |

---

## Reference Documents

| Document | Description |
|----------|-------------|
| [FJMS_API_REFERENCE.md](FJMS_API_REFERENCE.md) | Friedberg Manuscript Society API reference |
| [TRANSLATION_STATS.md](TRANSLATION_STATS.md) | Translation coverage statistics |
| [FIST_GAP_FILL_STATS.md](FIST_GAP_FILL_STATS.md) | FIST gap-fill import statistics |

---

## Code Reference

- **[architecture/OVERVIEW.md](architecture/OVERVIEW.md)** - Where code lives and why (authoritative)
- **[CODE_INDEX.md](CODE_INDEX.md)** - Symbol-level index (classes, functions, line numbers), generated per module with `scripts/gen_code_index_section.py`

---

## Archive

The `archive/` directory contains the historical documents that are no longer maintained (see the
directory listing; each names its successor where one exists):

- **Superseded structure docs** - `archive/FILE_INDEX_2026-03-26.md`, `archive/codebase-snapshot-2026-02-05/` (replaced by `architecture/`)
- **Completed plans** - `archive/plans/` (plan files + responsa-search design docs)
- **One-time reports** - Code reviews, analytics snapshots, bug reports
- **Test reports** - One-time code review/test reports
- **Handoff documents** - Session handoffs between developers
- **Old guides** - Outdated documentation (e.g., for removed backend)
- **Pre-launch checklists** - Historical testing artifacts

These files are kept for historical reference but should not be used for current development.

---

## Contributing to Documentation

When adding new documentation:

1. **Choose the right location:**
   - `guides/` - How-to guides and tutorials
   - `plans/` - Implementation plans and roadmaps
   - `specs/` - Technical specifications
   - Root `docs/` - General project docs

2. **Use consistent naming:**
   - UPPERCASE for main documents (e.g., `FEATURE_SPEC.md`)
   - Underscores between words
   - Include date if time-sensitive (e.g., `AUDIT_2026-01-30.md`)

3. **Update this index** when adding new documents

4. **Archive** instead of deleting old documents

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
