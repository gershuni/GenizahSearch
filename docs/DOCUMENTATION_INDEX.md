# GenizahSearch Documentation Index

> Last updated: 2026-03-13

This directory contains all project documentation, organized by category.

---

## Quick Links

| Need to... | Read this |
|------------|-----------|
| **See open bugs & issues** | [OPEN_ISSUES.md](OPEN_ISSUES.md) |
| Deploy the website | [guides/DEPLOYMENT_TECHNICAL.md](guides/DEPLOYMENT_TECHNICAL.md) |
| Manage the website (non-technical) | [guides/WEBSITE_ADMIN_GUIDE.md](guides/WEBSITE_ADMIN_GUIDE.md) |
| Find a specific file | [FILE_INDEX.md](FILE_INDEX.md) |
| Understand the code structure | [CODE_INDEX.md](CODE_INDEX.md) |
| See implementation plans | [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) |
| Check translation stats | [TRANSLATION_STATS.md](TRANSLATION_STATS.md) |

---

## Directory Structure

```
docs/
├── DOCUMENTATION_INDEX.md    # This file
├── OPEN_ISSUES.md            # Active issue tracker (AI agents MUST update)
├── FILE_INDEX.md             # Complete file listing for the project
├── CODE_INDEX.md             # Code structure and architecture
├── TRANSLATION_STATS.md      # Translation coverage stats (v6.5.0)
├── FJMS_API_REFERENCE.md     # FJMS API working reference
├── DOCUMENTATION_MAINTENANCE.md  # How to maintain these docs
│
├── guides/                   # How-to guides
│   ├── WEBSITE_ADMIN_GUIDE.md    # For site admins (non-technical)
│   ├── DEPLOYMENT_TECHNICAL.md   # Technical deployment guide
│   ├── DEVELOPER_GUIDE.md        # Local development setup
│   └── SUPABASE_GUIDE.md         # Supabase database guide
│
├── plans/                    # Implementation plans
│   ├── PLANS_INDEX.md            # Index of all plans
│   ├── TRANSLATION_MASTER_PLAN.md
│   ├── TRANSLATION_QA_IMPROVEMENT_PLAN.md
│   ├── LISTS_IMPLEMENTATION_PLAN.md
│   ├── LISTS_UNIFICATION_PLAN.md  # ⚠️ Pre-dates Supabase migration
│   ├── MOBILE_RESPONSIVE_PLAN.md
│   ├── JOINS_FEED_PLAN.md         # ⚠️ Pre-dates Supabase migration
│   ├── USER_TEXT_SEARCH_PLAN.md
│   ├── HELP_UPDATE_PLAN.md
│   ├── IN_APP_UPDATE_PLAN.md
│   ├── FIX_PLAN.md
│   └── responsa-search/          # Responsa search design docs
│
├── specs/                    # Technical specifications
│   ├── JOINS_TECHNICAL_SPEC.md
│   └── JOINS_SIMPLIFIED_SPEC.md
│
└── archive/                  # Archived/historical documents
    ├── PRE_LAUNCH_CHECKLIST.md   # v5.4 testing (archived 2026-03-13)
    ├── PRE_LAUNCH_SUMMARY.md     # v5.2 testing (archived 2026-03-13)
    ├── Oxford_Nav.md             # Bug investigation (archived 2026-03-13)
    ├── SUPABASE_MIGRATION_PLAN.md # Completed (Jan 2026)
    ├── LIBRARY_LOCATION_PLAN.md   # Completed (Feb 2026)
    ├── BOUNDARY_SEARCH_SPEC.md    # Completed (Feb 2026)
    ├── START_SERVERS_README.md    # Outdated (backend removed)
    ├── TEST_REPORT_AREAS_*.md     # One-time test reports
    ├── CODE_QUALITY_AUDIT_*.md    # Code audit reports
    ├── *_HANDOFF.md               # Session handoffs
    └── ...                        # Other historical docs
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

---

## Plans (Roadmap & Implementation)

### Future Plans

| Plan | Status | Description |
|------|--------|-------------|
| [LISTS_UNIFICATION_PLAN.md](plans/LISTS_UNIFICATION_PLAN.md) | Planned | Unifying Projects & Lists |
| [MOBILE_RESPONSIVE_PLAN.md](plans/MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOINS_FEED_PLAN.md](plans/JOINS_FEED_PLAN.md) | Planned | Fragment joins in discovery feed |
| [USER_TEXT_SEARCH_PLAN.md](plans/USER_TEXT_SEARCH_PLAN.md) | Planned | User-added text search |

### Completed Plans

| Plan | Completed | Description |
|------|-----------|-------------|
| [TRANSLATION_MASTER_PLAN.md](plans/TRANSLATION_MASTER_PLAN.md) | Mar 2026 | ~580K Dicta translations (v6.5.0) |
| [TRANSLATION_QA_IMPROVEMENT_PLAN.md](plans/TRANSLATION_QA_IMPROVEMENT_PLAN.md) | Mar 2026 | Translation QC heuristics & reporting |
| [HELP_UPDATE_PLAN.md](plans/HELP_UPDATE_PLAN.md) | Mar 2026 | Help page updates |
| [IN_APP_UPDATE_PLAN.md](plans/IN_APP_UPDATE_PLAN.md) | Feb 2026 | Desktop in-app updates |
| Responsa Search | Feb 2026 | Syntax parsing, grammatical expansion |
| FIST Integration | Feb 2026 | Domain classifications, catalog enrichment |
| External Data Integration | Feb 2026 | PGP, NLI, Cambridge/Manchester/JTS IIIF |
| Supabase Migration | Jan 2026 | Cloud backend migration |

See [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) for the complete list.

---

## Technical Specifications

Detailed specifications for complex features:

| Spec | Description |
|------|-------------|
| [JOINS_TECHNICAL_SPEC.md](specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [JOINS_SIMPLIFIED_SPEC.md](specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |

> Note: Completed specs (BOUNDARY_SEARCH_SPEC.md, USER_CORRECTIONS_SPEC.md, SEARCHABLE_CORRECTIONS_SPEC.md) moved to archive.

---

## Reference Documents

| Document | Description |
|----------|-------------|
| [FJMS_API_REFERENCE.md](FJMS_API_REFERENCE.md) | Friedberg Manuscript Society API reference |
| [TRANSLATION_STATS.md](TRANSLATION_STATS.md) | Translation coverage statistics |
| [FJMS_EXPORT_AND_TRANSLATION_BUGS.md](FJMS_EXPORT_AND_TRANSLATION_BUGS.md) | Known FJMS data issues |

---

## Code Reference

- **[CODE_INDEX.md](CODE_INDEX.md)** - Overview of codebase structure
  - File organization
  - Key modules and their responsibilities
  - Data flow diagrams

---

## Archive

The `archive/` directory contains historical documents that are no longer actively maintained:

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

*Last reorganization: 2026-03-13*
