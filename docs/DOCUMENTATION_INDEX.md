# GenizahSearch Documentation Index

> Last updated: 2026-02-03

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
| See what's in the next release | [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) |
| Check pre-launch status | [PRE_LAUNCH_CHECKLIST.md](PRE_LAUNCH_CHECKLIST.md) |

---

## Directory Structure

```
docs/
├── DOCUMENTATION_INDEX.md    # This file
├── OPEN_ISSUES.md            # Active issue tracker (AI agents MUST update)
├── FILE_INDEX.md             # Complete file listing for the project
├── CODE_INDEX.md             # Code structure and architecture
├── PRE_LAUNCH_CHECKLIST.md   # Pre-launch tasks checklist
├── Oxford_Nav.md             # Oxford Bodleian navigation notes
│
├── guides/                   # How-to guides
│   ├── WEBSITE_ADMIN_GUIDE.md    # For site admins (non-technical)
│   ├── DEPLOYMENT_TECHNICAL.md   # Technical deployment guide
│   ├── DEVELOPER_GUIDE.md        # Local development setup
│   └── SUPABASE_GUIDE.md         # Supabase database guide
│
├── plans/                    # Implementation plans
│   ├── PLANS_INDEX.md            # Index of all plans
│   ├── SUPABASE_MIGRATION_PLAN.md # Supabase migration (COMPLETED)
│   ├── LISTS_IMPLEMENTATION_PLAN.md
│   ├── LISTS_UNIFICATION_PLAN.md
│   ├── JOINS_FEED_PLAN.md
│   ├── MOBILE_RESPONSIVE_PLAN.md
│   └── FIX_PLAN.md
│
├── specs/                    # Technical specifications
│   ├── JOINS_TECHNICAL_SPEC.md
│   └── JOINS_SIMPLIFIED_SPEC.md
│
└── archive/                  # Archived/historical documents
    ├── START_SERVERS_README.md   # Outdated (backend removed)
    ├── TEST_REPORT_AREAS_*.md    # One-time test reports
    ├── CODE_QUALITY_AUDIT_*.md   # Code audit reports
    ├── *_HANDOFF.md              # Session handoffs
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

---

## Plans (Roadmap & Implementation)

Active plans for upcoming features:

| Plan | Status | Description |
|------|--------|-------------|
| [SUPABASE_MIGRATION_PLAN.md](plans/SUPABASE_MIGRATION_PLAN.md) | Completed | Migration from FastAPI to Supabase |
| [LISTS_UNIFICATION_PLAN.md](plans/LISTS_UNIFICATION_PLAN.md) | In Progress | Unifying Projects & Lists |
| [MOBILE_RESPONSIVE_PLAN.md](plans/MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOINS_FEED_PLAN.md](plans/JOINS_FEED_PLAN.md) | Planned | Fragment joins in discovery feed |

See [plans/PLANS_INDEX.md](plans/PLANS_INDEX.md) for the complete list.

---

## Technical Specifications

Detailed specifications for complex features:

| Spec | Description |
|------|-------------|
| [JOINS_TECHNICAL_SPEC.md](specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [JOINS_SIMPLIFIED_SPEC.md](specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |
| [BOUNDARY_SEARCH_SPEC.md](plans/BOUNDARY_SEARCH_SPEC.md) | Cross-paragraph search (implemented in Web + Desktop) |

> Note: Old correction specs (USER_CORRECTIONS_SPEC.md, SEARCHABLE_CORRECTIONS_SPEC.md) moved to archive after Supabase migration.

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

*Last reorganization: 2026-02-03*
