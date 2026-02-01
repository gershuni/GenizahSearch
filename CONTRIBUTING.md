# Contributing to GenizahSearch

## Quick Start

1. Read `CLAUDE.md` for project context
2. Read `docs/guides/DEVELOPER_GUIDE.md` for setup instructions
3. Follow the code style guidelines below

## Documentation Requirements

**Keep documentation updated when making changes!**

| If you change... | Update these docs |
|------------------|-------------------|
| Architecture | `CLAUDE.md`, `docs/guides/DEPLOYMENT_TECHNICAL.md` |
| Database schema | `docs/guides/SUPABASE_GUIDE.md` |
| Environment variables | `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md` |
| Major features | `CHANGELOG.md`, `README.md` |

### Before submitting a PR

Run the documentation health check:
```bash
python scripts/check_docs.py
```

## Code Style

- Python 3.10+
- Type hints encouraged
- Hebrew comments are acceptable
- Follow existing patterns in the codebase

## For AI Agents

If you're an AI assistant (Claude, Cursor, Copilot, etc.):
1. Read `CLAUDE.md` first - it has important context
2. Update documentation when making significant changes
3. Run `python scripts/check_docs.py` before committing
4. Avoid using outdated terms (FastAPI, genizah-backend, DATABASE_URL)

## Questions?

Open an issue on GitHub.
