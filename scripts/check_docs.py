#!/usr/bin/env python3
"""
Documentation Health Check Script

Checks GenizahSearch documentation for:
- Missing critical documents
- Outdated terminology (removed features)
- Stale documents (not updated recently)
- Broken internal links

Usage:
    python scripts/check_docs.py
"""

import re
from datetime import datetime, timedelta
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
DOCS_DIR = ROOT_DIR / 'docs'

# Terms that may indicate outdated content
# Format: (term, reason, exclude_files)
OUTDATED_TERMS = [
    ('genizah-backend', 'Service removed - only genizah-web exists', []),
    ('backend/requirements.txt', 'File no longer exists', []),
    ('DATABASE_URL', 'No longer used - replaced by SUPABASE_URL', []),
]

# Files to skip entirely (they intentionally reference old terms)
SKIP_FILES = [
    'DOCUMENTATION_MAINTENANCE.md',  # Contains terms as examples
    'SUPABASE_MIGRATION_PLAN.md',    # Historical document about migration
]

# Critical documents that must exist
CRITICAL_DOCS = [
    ('CLAUDE.md', 'AI assistant context'),
    ('README.md', 'Project overview'),
    ('docs/DOCUMENTATION_INDEX.md', 'Documentation index'),
    ('docs/guides/DEPLOYMENT_TECHNICAL.md', 'Deployment guide'),
    ('docs/guides/WEBSITE_ADMIN_GUIDE.md', 'Admin guide'),
    ('docs/guides/DEVELOPER_GUIDE.md', 'Developer guide'),
    ('docs/guides/SUPABASE_GUIDE.md', 'Supabase guide'),
]

# How old is "stale" (days)
STALE_THRESHOLD_DAYS = 90


def print_header(text: str):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f" {text}")
    print('=' * 60)


def print_status(ok: bool, message: str):
    """Print a status line."""
    icon = '✅' if ok else '❌'
    print(f"  {icon} {message}")


def print_warning(message: str):
    """Print a warning."""
    print(f"  ⚠️  {message}")


def check_critical_docs() -> list:
    """Check that all critical documents exist."""
    issues = []
    for doc_path, description in CRITICAL_DOCS:
        full_path = ROOT_DIR / doc_path
        if not full_path.exists():
            issues.append(f"Missing: {doc_path} ({description})")
    return issues


def check_outdated_terms() -> list:
    """Search for terms that may indicate outdated content."""
    issues = []

    for md_file in DOCS_DIR.rglob('*.md'):
        # Skip archived documents
        if 'archive' in str(md_file):
            continue

        # Skip files that intentionally reference old terms
        if md_file.name in SKIP_FILES:
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)

        for term, reason, exclude_files in OUTDATED_TERMS:
            # Skip if file is in exclude list for this term
            if md_file.name in exclude_files:
                continue

            # Case-insensitive search
            if re.search(re.escape(term), content, re.IGNORECASE):
                issues.append(f"{relative_path}: Contains '{term}' - {reason}")

    return issues


def check_stale_docs() -> list:
    """Check for documents that haven't been updated recently."""
    issues = []
    cutoff = datetime.now() - timedelta(days=STALE_THRESHOLD_DAYS)

    for md_file in DOCS_DIR.rglob('*.md'):
        # Skip archived documents
        if 'archive' in str(md_file):
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)

        # Look for "Last updated: YYYY-MM-DD" pattern
        match = re.search(r'Last updated[:\s]+(\d{4}-\d{2}-\d{2})', content, re.IGNORECASE)
        if match:
            try:
                date = datetime.strptime(match.group(1), '%Y-%m-%d')
                if date < cutoff:
                    days_old = (datetime.now() - date).days
                    issues.append(f"{relative_path}: Last updated {match.group(1)} ({days_old} days ago)")
            except ValueError:
                pass

    return issues


def check_broken_links() -> list:
    """Check for broken internal links."""
    issues = []

    for md_file in DOCS_DIR.rglob('*.md'):
        if 'archive' in str(md_file):
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)
        md_dir = md_file.parent

        # Remove code blocks before searching for links (to avoid false positives)
        content_no_code = re.sub(r'```[\s\S]*?```', '', content)
        content_no_code = re.sub(r'`[^`]+`', '', content_no_code)

        # Find markdown links: [text](path)
        links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content_no_code)

        for link_text, link_path in links:
            # Skip external links
            if link_path.startswith(('http://', 'https://', 'mailto:')):
                continue

            # Skip anchors
            if link_path.startswith('#'):
                continue

            # Remove anchor from path
            link_path = link_path.split('#')[0]

            if not link_path:
                continue

            # Strip trailing line-number suffix used by `file:N` / `file:N-M`
            # code references (e.g. `web/api.py:680`, `genizah_core.py:3940-3961`).
            # The link points at a real file; the suffix is editor-jump metadata.
            line_ref_path = re.sub(r':\d+(?:-\d+)?$', '', link_path)

            # Try BOTH resolution strategies: docs-relative AND project-root-relative.
            # Many doc references use project-root-relative paths (e.g.
            # `[web/api.py](web/api.py)` from inside docs/) instead of `../web/api.py`.
            # A link is "broken" only if NEITHER candidate exists.
            if line_ref_path.startswith('/'):
                candidates = [ROOT_DIR / line_ref_path.lstrip('/')]
            else:
                candidates = [md_dir / line_ref_path, ROOT_DIR / line_ref_path]

            if not any(c.resolve().exists() for c in candidates):
                issues.append(f"{relative_path}: Broken link to '{link_path}'")

    return issues


def main():
    """Run all documentation checks."""
    print_header("GenizahSearch Documentation Health Check")
    print(f"Checking: {DOCS_DIR}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    total_issues = 0

    # 1. Check critical documents
    print("\n📁 Critical Documents")
    print("-" * 40)
    missing = check_critical_docs()
    if missing:
        for m in missing:
            print_status(False, m)
        total_issues += len(missing)
    else:
        print_status(True, "All critical documents exist")

    # 2. Check for outdated terms
    print("\n🔍 Outdated Terminology")
    print("-" * 40)
    outdated = check_outdated_terms()
    if outdated:
        for o in outdated:
            print_warning(o)
        total_issues += len(outdated)
    else:
        print_status(True, "No outdated terms found")

    # 3. Check for stale documents
    # NOTE: staleness is an INFORMATIONAL freshness reminder, not a CI failure.
    # Docs cross the day threshold on a rolling calendar basis with no code
    # change, so counting them toward the exit code turns the build red for the
    # wrong reason. Printed below for visibility but excluded from total_issues.
    print("\n📅 Document Freshness")
    print("-" * 40)
    stale = check_stale_docs()
    if stale:
        for s in stale:
            print_warning(s)
    else:
        print_status(True, f"All documents updated within {STALE_THRESHOLD_DAYS} days")

    # 4. Check for broken links
    print("\n🔗 Internal Links")
    print("-" * 40)
    broken = check_broken_links()
    if broken:
        for b in broken:
            print_warning(b)
        total_issues += len(broken)
    else:
        print_status(True, "All internal links valid")

    # Summary
    print_header("Summary")
    # total_issues counts only BLOCKING checks (missing / outdated / broken).
    # Stale docs are reported separately as a non-blocking freshness reminder.
    if stale:
        print(f"ℹ️  {len(stale)} stale doc(s) over {STALE_THRESHOLD_DAYS} days "
              f"(informational — does not fail CI)")
    if total_issues == 0:
        print("✅ All blocking checks passed! Documentation is healthy.")
    else:
        print(f"❌ Found {total_issues} blocking issue(s):")
        print(f"   - Missing documents: {len(missing)}")
        print(f"   - Outdated terms: {len(outdated)}")
        print(f"   - Broken links: {len(broken)}")
        print("\nReview docs/DOCUMENTATION_MAINTENANCE.md for guidance.")

    return 0 if total_issues == 0 else 1


if __name__ == '__main__':
    exit(main())
