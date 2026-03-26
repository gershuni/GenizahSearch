# Documentation Maintenance Guide

> Routine checks and updates for GenizahSearch documentation

---

## Maintenance Schedule

### Monthly (or after major changes)

Run the documentation check script:
```bash
python scripts/check_docs.py
```

Or manually review:

- [ ] **Architecture docs are current** - DEPLOYMENT_TECHNICAL.md, WEBSITE_ADMIN_GUIDE.md
- [ ] **No references to removed features** (FastAPI backend, etc.)
- [ ] **Links work** - no broken internal/external links
- [ ] **Dates are updated** - "Last updated" headers

### After Architecture Changes

When the system architecture changes:

1. Update `docs/guides/DEPLOYMENT_TECHNICAL.md`
2. Update `docs/guides/WEBSITE_ADMIN_GUIDE.md`
3. Update `CLAUDE.md`
4. Update `README.md` if needed

### After Adding Features

1. Update relevant specs in `docs/specs/`
2. Update `docs/guides/DEVELOPER_GUIDE.md` if APIs changed
3. Update `docs/guides/SUPABASE_GUIDE.md` if schema changed

---

## Critical Documents

These documents must always be up-to-date:

| Document | Purpose | Update When |
|----------|---------|-------------|
| `CLAUDE.md` | AI assistant context | Architecture changes |
| `README.md` | Project overview | Major features added |
| `docs/guides/DEPLOYMENT_TECHNICAL.md` | Server deployment | Server/infra changes |
| `docs/guides/WEBSITE_ADMIN_GUIDE.md` | Admin operations | Admin workflow changes |
| `docs/guides/DEVELOPER_GUIDE.md` | Developer onboarding | Dev workflow changes |
| `docs/guides/SUPABASE_GUIDE.md` | Database operations | Schema changes |

---

## Common Issues to Check

### Outdated Architecture References

Search for these terms that may indicate outdated content:

```bash
# FastAPI backend (removed Jan 2026)
grep -r "FastAPI" docs/
grep -r "port 8000" docs/
grep -r "genizah-backend" docs/

# PostgreSQL (replaced by Supabase)
grep -r "PostgreSQL" docs/ | grep -v "Supabase"
grep -r "psql" docs/

# Old file paths
grep -r "backend/" docs/
```

### Broken Links

```bash
# Find markdown links
grep -r "\[.*\](.*)" docs/ --include="*.md"
```

### Missing "Last Updated" Dates

All guide documents should have a header like:
```markdown
> Last updated: 2026-01-31
```

---

## Documentation Checklist

### After Every Sprint/Release

- [ ] Update CHANGELOG.md
- [ ] Review and update outdated docs
- [ ] Archive completed plans (move to docs/archive/)
- [ ] Update DOCUMENTATION_INDEX.md if structure changed

### Quarterly Review

- [ ] Audit all documents for accuracy
- [ ] Remove or archive obsolete documents
- [ ] Update screenshots if UI changed
- [ ] Review and update examples in code snippets

---

## Automated Checks

### Documentation Check Script

Create `scripts/check_docs.py`:

```python
#!/usr/bin/env python3
"""Check documentation for common issues."""

import os
import re
from datetime import datetime, timedelta
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent / 'docs'
ROOT_DIR = Path(__file__).parent.parent

# Terms that may indicate outdated content
OUTDATED_TERMS = [
    ('FastAPI', 'Backend was removed in Jan 2026'),
    ('port 8000', 'Backend port no longer used'),
    ('genizah-backend', 'Service removed'),
    ('backend/requirements.txt', 'File no longer exists'),
    ('create_admin.py', 'Admin creation moved to Supabase'),
]

# Required files
CRITICAL_DOCS = [
    'CLAUDE.md',
    'README.md',
    'docs/guides/DEPLOYMENT_TECHNICAL.md',
    'docs/guides/WEBSITE_ADMIN_GUIDE.md',
    'docs/guides/DEVELOPER_GUIDE.md',
    'docs/DOCUMENTATION_INDEX.md',
]


def check_outdated_terms():
    """Search for potentially outdated terms."""
    issues = []
    for md_file in DOCS_DIR.rglob('*.md'):
        if 'archive' in str(md_file):
            continue
        content = md_file.read_text(encoding='utf-8')
        for term, reason in OUTDATED_TERMS:
            if term.lower() in content.lower():
                issues.append(f"{md_file.relative_to(ROOT_DIR)}: Contains '{term}' - {reason}")
    return issues


def check_critical_docs_exist():
    """Ensure critical documents exist."""
    missing = []
    for doc in CRITICAL_DOCS:
        if not (ROOT_DIR / doc).exists():
            missing.append(f"Missing critical document: {doc}")
    return missing


def check_last_updated():
    """Check if docs have recent 'Last updated' dates."""
    old_docs = []
    cutoff = datetime.now() - timedelta(days=90)

    for md_file in DOCS_DIR.rglob('*.md'):
        if 'archive' in str(md_file):
            continue
        content = md_file.read_text(encoding='utf-8')
        match = re.search(r'Last updated[:\s]+(\d{4}-\d{2}-\d{2})', content)
        if match:
            date = datetime.strptime(match.group(1), '%Y-%m-%d')
            if date < cutoff:
                old_docs.append(f"{md_file.relative_to(ROOT_DIR)}: Last updated {match.group(1)}")
    return old_docs


def main():
    print("=" * 60)
    print("Documentation Health Check")
    print("=" * 60)

    # Check for missing docs
    print("\n📁 Checking critical documents...")
    missing = check_critical_docs_exist()
    if missing:
        for m in missing:
            print(f"  ❌ {m}")
    else:
        print("  ✅ All critical documents exist")

    # Check for outdated terms
    print("\n🔍 Checking for outdated terms...")
    outdated = check_outdated_terms()
    if outdated:
        for o in outdated:
            print(f"  ⚠️  {o}")
    else:
        print("  ✅ No outdated terms found")

    # Check update dates
    print("\n📅 Checking document freshness...")
    old = check_last_updated()
    if old:
        for o in old:
            print(f"  ⚠️  {o}")
    else:
        print("  ✅ All documents recently updated")

    print("\n" + "=" * 60)
    total_issues = len(missing) + len(outdated) + len(old)
    if total_issues == 0:
        print("✅ All checks passed!")
    else:
        print(f"⚠️  Found {total_issues} issue(s) to review")
    print("=" * 60)


if __name__ == '__main__':
    main()
```

### Running the Check

```bash
python scripts/check_docs.py
```

---

## Git Hooks (Optional)

Add a pre-commit hook to remind about documentation:

`.git/hooks/pre-commit`:
```bash
#!/bin/bash
# Check if code files changed but docs didn't

CODE_CHANGED=$(git diff --cached --name-only | grep -E '\.(py)$' | wc -l)
DOCS_CHANGED=$(git diff --cached --name-only | grep -E '\.md$' | wc -l)

if [ "$CODE_CHANGED" -gt 5 ] && [ "$DOCS_CHANGED" -eq 0 ]; then
    echo "⚠️  Significant code changes detected without documentation updates."
    echo "   Consider updating relevant docs in docs/"
    echo ""
fi
```

---

## Archive Policy

Documents should be moved to `docs/archive/` when:

1. **Plans completed** - Move to archive after implementation
2. **One-time reports** - Test reports, audits, etc.
3. **Superseded docs** - When replaced by newer version
4. **Session notes** - Handoff documents after they're processed

**Never delete** documentation - archive it for historical reference.

---

## Contact

For documentation questions or suggestions:
- Create an issue on GitHub
- Tag with `documentation` label
