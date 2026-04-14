# Phase 65: Repo Hygiene - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-14
**Phase:** 65-repo-hygiene
**Areas discussed:** Root debris cleanup, Silent exceptions, Monkey-patch isolation, General

---

## Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Silent exceptions audit | How to handle ~3 silent 'except Exception: pass' in genizah_core.py | |
| Monkey-patch isolation | Moving 2 NiceGUI patches from web/main.py to framework_patches.py | |
| Root debris cleanup | Which ~30+ temp/debug/backup files to gitignore vs relocate vs delete | |
| Gitignore patterns | What wildcard patterns to add to prevent future debris | |

**User's choice:** "Ask me in plain English for general issues, technicalities I'll ask AIs"
**Notes:** User requested plain-English questions with technical details left to Claude's discretion. Proceeded with simplified questions covering all 4 areas.

---

## Root Debris Cleanup

| Option | Description | Selected |
|--------|-------------|----------|
| Gitignore them all (Recommended) | Add patterns to .gitignore so git stops tracking them, keep actual files untouched | ✓ |
| Delete and gitignore | Remove from git tracking AND delete files from disk | |
| Move to a folder | Relocate temp files to _scratch/ folder, gitignore that folder | |

**User's choice:** Gitignore them all (Recommended)
**Notes:** Simplest and safest approach — no risk of losing local data files.

---

## Legacy Data Files

| Option | Description | Selected |
|--------|-------------|----------|
| All legacy, gitignore them | CSV/JSON files are leftover from earlier import work | |
| Some are still used | User will specify which to keep | |
| You decide | Claude checks which files are referenced in code, keeps those, gitignores rest | ✓ |

**User's choice:** You decide
**Notes:** Claude will audit code references to determine which root data files are still in use.

---

## Silent Exception Handlers

| Option | Description | Selected |
|--------|-------------|----------|
| Add logging (Recommended) | Add log.warning/debug so errors visible in logs without crashing app | ✓ |
| Just add comments | Document why each exception is silenced, leave behavior unchanged | |
| You decide per case | Claude judges each — some may deserve logging, others intentionally silent | |

**User's choice:** Add logging (Recommended)
**Notes:** Best practice for diagnosing issues later.

---

## Anything Else

| Option | Description | Selected |
|--------|-------------|----------|
| That covers it | Four requirements are clear enough, move to planning | |
| I have something to add | Another concern or preference to mention | |

**User's choice:** "I don't know"
**Notes:** User unsure if there's more to discuss. Phase scope is well-defined by HYGN-01 through HYGN-04; proceeding with context creation.

---

## Claude's Discretion

- Exact log levels per exception handler
- Whether to narrow bare `except:` to specific types
- Classification of borderline root files (check code references)
- Gitignore pattern syntax and ordering
- Which legacy data CSVs/JSONs are still referenced in code

## Deferred Ideas

None — discussion stayed within phase scope
