# Phase 114: Usage Analytics - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 114-usage-analytics
**Areas discussed:** Feature/tab usage scope, Search-event shape, Identity triggers, Session & active-user (+ Codex review)

---

## Feature/tab usage scope (USAGE-02)

### Tab tracking granularity
| Option | Description | Selected |
|--------|-------------|----------|
| First-activation per session | Once per tab per session; cleaner adoption signal, lower volume | |
| Every switch | Emit on every tab change; full navigation fidelity, noisier | ✓ |
| Both (count + first) | First-activation flagged + per-session switch count in session_end | |

**User's choice:** Every switch.
**Notes:** Hillel wants maximal navigation fidelity. Volume is fine for tens of desktop users (Codex
confirmed no debounce needed) — but programmatic/restore tab changes must be suppressed (Codex MED-2).

### Non-tab surfaces (multi-select)
| Option | Description | Selected |
|--------|-------------|----------|
| Joins Lab | Component A Join Workbench open | ✓ |
| Fragment Puzzle | Puzzle/join-documents workspace open | ✓ |
| Major dialogs | ResultDialog, FJMS catalog, Visual Similarity, export (dialog_name enum) | ✓ |
| Export actions | Which export format used, via the `action` prop | ✓ |

**User's choice:** All four.
**Notes:** Codex HIGH-2 — these values MUST be hardcoded enum constants, never dialog/window titles
(Visual Similarity title embeds shelfmark; FJMS dialogs carry sys_id/shelfmark).

---

## Search-event shape (USAGE-03)

### search_mode enum granularity
| Option | Description | Selected |
|--------|-------------|----------|
| Exact UI mode | Each mode_combo entry + responsa/composition/parallels as its own enum | ✓ |
| Collapsed families | Just the 4 named families | |

**User's choice:** Exact UI mode.
**Notes:** Codex LOW — map from combo index (not currentText, labels are translated); report effective
mode for prefix-parsed searches.

### Result-count bucket timing
| Option | Description | Selected |
|--------|-------------|----------|
| Include coarse bucket now | result_count_bucket (0/1-9/10-99/100+) on the usage event | ✓ |
| Defer to Phase 115 | All counts/timings in the perf summary | |

**User's choice:** Include coarse bucket now.

### What counts as one execution
| Option | Description | Selected |
|--------|-------------|----------|
| User-initiated completed only | Skip cancelled + auto reruns | |
| Include cancelled too | Also emit on cancel with a status marker | ✓ |

**User's choice:** Include cancelled too.
**Notes:** Status via `action`=completed|cancelled; cancelled carries no bucket; auto/incremental
reruns still skipped. Codex MED-1 — per-run state object, emit exactly once, don't count shutdown
cancellation; LabSearchThread lacks a cancel check.

---

## Identity triggers (IDENT-01/02)

### Startup identify timing
| Option | Description | Selected |
|--------|-------------|----------|
| Immediately after consent + auto-login resolve | Identify restored sessions; max merge | ✓ |
| Only on explicit login | Restored sessions stay anon until re-login | |

**User's choice:** Immediately after consent + auto-login resolve.

### Event ordering
| Option | Description | Selected |
|--------|-------------|----------|
| identify() before session_start | session_start attributes to the person | ✓ |
| session_start first, then identify | Rely on alias-merge | |

**User's choice:** identify() before session_start.
**Notes:** Codex HIGH-1 (VERIFIED) — identity source must be `current_user._uuid`, NOT `.id` (a hash).
Codex HIGH-3 — needs a single startup coordinator; alias-merge only as timeout fallback.

---

## Session & active-user (USAGE-04/06)

### Heartbeat
| Option | Description | Selected |
|--------|-------------|----------|
| Daily heartbeat while running | Long-lived sessions still count toward DAU | ✓ |
| session_start only | DAU = launches; undercounts long-open users | |

**User's choice:** Daily heartbeat while running.
**Notes:** Codex MED-3 — focus/resume-aware, at most once per UTC day, not same UTC day as
session_start; add ACTIVE_PING to DesktopEvent.

### session_end delivery
| Option | Description | Selected |
|--------|-------------|----------|
| Best-effort on clean exit | closeEvent/atexit; absent on crash | ✓ |
| Skip session_end entirely | Derive length from session_start + last-seen | |

**User's choice:** Best-effort on clean exit.
**Notes:** Codex LOW — exactly-once guard if wired via both closeEvent and atexit.

---

## External review

Hillel requested a Codex opinion before finalizing CONTEXT.md (`feedback_codex_during_discuss_phase`).
`codex exec` reviewed against live code: verdict "would not approve as written" with 1 HIGH bug
(wrong identity source — `.id` hash vs `._uuid`), 2 more HIGH (dynamic-UI-string leak vectors; startup
ordering coordinator), 3 MEDIUM, 6 LOW. All adopted into CONTEXT.md decisions D-02/D-04/D-09/D-10/
D-12/D-16/D-17. Full critique: `114-CODEX-CRITIQUE.md`. None reversed Hillel's product choices.

## Claude's Discretion

Exact enum string values (tab/dialog/feature names); search index→mode-enum map; heartbeat interval +
focus/resume detection; per-run search state shape; startup coordinator location.

## Deferred Ideas

- Perf timings / per-session perf summary → Phase 115 (PERF-01..03).
- Privacy CI audit + ops runbook → Phase 116 (PRIV-04, INFRA-06).
- Handled/non-fatal error counting → ERR-01 (Future, out of v8.1.0).
