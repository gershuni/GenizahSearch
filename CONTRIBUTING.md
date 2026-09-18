# Contributing to GenizahSearch

## Quick Start

1. Read `AGENTS.md` (canonical commands, layout rules) and `CLAUDE.md` (project context)
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

Run the tests through the bounded runner, then the documentation health check:
```bash
python scripts/run_local_tests.py
python scripts/check_docs.py
```
Never run the suite as one `pytest tests/` process; a single named file (`pytest tests/test_x.py`)
is fine. Where a new file goes is decided in
[docs/architecture/OVERVIEW.md](docs/architecture/OVERVIEW.md).

## Code Style

- Python 3.11 (what CI runs)
- Type hints encouraged
- Hebrew comments are acceptable
- Follow existing patterns in the codebase

## For AI Agents

If you're an AI assistant (Claude, Cursor, Copilot, etc.):
1. Read `AGENTS.md` and `CLAUDE.md` first - commands, layout rules and project context
2. Update documentation when making significant changes
3. Run `python scripts/check_docs.py` before committing
4. Avoid outdated terms: the standalone `genizah-backend` process, `DATABASE_URL`, port 8000 (FastAPI itself is still live -- it serves `/api/*`).

## Questions?

Open an issue on GitHub.
