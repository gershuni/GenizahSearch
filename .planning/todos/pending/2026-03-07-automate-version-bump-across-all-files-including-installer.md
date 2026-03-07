---
created: 2026-03-07T22:15:00Z
title: Automate version bump across all files including installer
area: tooling
files:
  - genizah_core.py
  - genizah_app.py
  - web/main.py
  - installer.iss
  - CHANGELOG.md
---

## Problem

Version numbers are scattered across multiple files and must be updated manually for each release. This is error-prone — easy to forget a file, leading to version mismatches between the web app, desktop app, installer, and documentation.

Known locations that need version updates:
- genizah_core.py (VERSION constant)
- genizah_app.py (window title, about dialog)
- web/main.py (footer, metadata)
- installer.iss (Inno Setup installer version)
- CHANGELOG.md (new version header)
- Possibly: README.md, package metadata, git tags

## Solution

Create a `scripts/bump_version.py` (or similar) that:
1. Accepts a version string (e.g., `6.5.0`) or bump type (`major`/`minor`/`patch`)
2. Updates all known version locations via regex replacement
3. Adds a CHANGELOG.md header with date
4. Creates a git tag
5. Optionally commits the changes
6. Reports all files modified for review

Could also integrate with `/gsd:complete-milestone` workflow.
