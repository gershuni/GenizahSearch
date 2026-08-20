# GenizahSearch Documentation Index

> Last updated: 2026-08-16

This directory contains all project documentation, organized by category.

---

## Quick Links

| Need to... | Read this |
|------------|-----------|
| **See open bugs & issues** | [OPEN_ISSUES.md](OPEN_ISSUES.md) |
| **See feature ideas / what to build next** | [FEATURE_IDEAS.md](FEATURE_IDEAS.md) |
| Deploy the website | [guides/DEPLOYMENT_TECHNICAL.md](guides/DEPLOYMENT_TECHNICAL.md) |
| Manage browser extension | [guides/DEPLOYMENT_TECHNICAL.md#browser-extension](guides/DEPLOYMENT_TECHNICAL.md#browser-extension-genizahsearch-image-helper) |
| Manage the website (non-technical) | [guides/WEBSITE_ADMIN_GUIDE.md](guides/WEBSITE_ADMIN_GUIDE.md) |
| Find a specific file | [FILE_INDEX.md](FILE_INDEX.md) |
| Understand the code structure | [CODE_INDEX.md](CODE_INDEX.md) |
| See future implementation plans | [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) |
| Check translation stats | [TRANSLATION_STATS.md](TRANSLATION_STATS.md) |

---

## Directory Structure

```
docs/
├── DOCUMENTATION_INDEX.md    # This file
├── OPEN_ISSUES.md            # Active issue tracker (AI agents MUST update)
├── FILE_INDEX.md             # Complete file listing for the project
├── CODE_INDEX.md             # Code structure and architecture
├── TRANSLATION_STATS.md      # Translation coverage stats
├── FJMS_API_REFERENCE.md     # FJMS API working reference
├── FIST_GAP_FILL_STATS.md    # FIST gap-fill import statistics
│
├── guides/                   # How-to guides
│   ├── WEBSITE_ADMIN_GUIDE.md    # For site admins (non-technical)
│   ├── DEPLOYMENT_TECHNICAL.md   # Technical deployment guide
│   ├── DEVELOPER_GUIDE.md        # Local development setup
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
└── archive/                  # 40+ historical documents
    ├── plans/                    # 15 completed plan files + responsa-search/
    ├── CODE_REVIEW_*.md          # One-time code reviews
    ├── POSTHOG_ANALYTICS_*.md    # Point-in-time analytics snapshots
    ├── TEST_REPORT_AREAS_*.md    # One-time test reports
    ├── *_HANDOFF.md              # Session handoffs
    ├── SUPABASE_MIGRATION_PLAN.md
    └── ...                       # Other historical docs
```

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

---

## Reference Documents

| Document | Description |
|----------|-------------|
| [FJMS_API_REFERENCE.md](FJMS_API_REFERENCE.md) | Friedberg Manuscript Society API reference |
| [TRANSLATION_STATS.md](TRANSLATION_STATS.md) | Translation coverage statistics |
| [FIST_GAP_FILL_STATS.md](FIST_GAP_FILL_STATS.md) | FIST gap-fill import statistics |

---

## Code Reference

- **[CODE_INDEX.md](CODE_INDEX.md)** - Overview of codebase structure
  - File organization
  - Key modules and their responsibilities
  - Data flow diagrams

---

## Archive

The `archive/` directory contains 40+ historical documents that are no longer actively maintained:

- **Completed plans** - `archive/plans/` (15 plan files + responsa-search design docs)
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
| `CLAUDE.md` | Instructions for AI assistants |

---

*Last reorganization: 2026-03-26*
