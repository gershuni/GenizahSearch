# Phase 100: LOCAL PDF Image in ResultDialog + Browse - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 100-local-pdf-image-in-resultdialog-browse
**Areas discussed:** Loading & failure placeholders, Re-render timing on navigation
**Areas locked to recommended defaults (not deep-discussed):** Worker scope & lifecycle, Image pane visibility & toggle

---

## Area selection

| Option | Description | Selected |
|--------|-------------|----------|
| Worker scope & lifecycle | One shared app-level worker vs per-surface | |
| Image pane visibility & toggle | Auto-show for PDF, toggle behavior, non-PDF | |
| Loading & failure placeholders | Loading state, message granularity, language | ✓ |
| Re-render timing on nav | Debounce vs immediate, consistency across surfaces | ✓ |

---

## Loading & failure placeholders

### Q1 — Loading state during render

| Option | Description | Selected |
|--------|-------------|----------|
| "Loading…" status text | Show viewer's existing status-message placeholder, swap to image when ready | ✓ |
| Keep previous image | Leave last page visible until new one arrives | |
| Blank pane | Clear to empty while rendering | |

**User's choice:** "Loading…" status text

### Q2 — Failure message granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Per-reason messages | Map each PdfRenderFailure reason to a short human message | ✓ |
| Generic message | One message for all failures, reason only in log | |

**User's choice:** Per-reason messages

### Q3 — Placeholder / failure text language

| Option | Description | Selected |
|--------|-------------|----------|
| Bilingual HE + EN | Show both, e.g. "טוען… / Loading…" | |
| Match UI language | Follow current language setting | ✓ |
| English only | Simplest | |

**User's choice:** Match UI language
**Notes:** Verified post-answer that the desktop has a global `CURRENT_LANG` (`'he'`/`'en'`); "match UI language" maps to selecting the HE or EN variant by `CURRENT_LANG`, with bilingual fallback if unavailable.

---

## Re-render timing on navigation

### Q1 — Render firing on rapid navigation

| Option | Description | Selected |
|--------|-------------|----------|
| Short debounce (~150ms) | Wait for nav to settle before enqueuing; matches Browse's image debounce | ✓ |
| Fire immediately | Enqueue every step, rely on latest-wins to discard stale results | |
| Immediate + drop stale from queue | Coalesce at enqueue if newer request pending | |

**User's choice:** Short debounce (~150ms)
**Notes:** Controller-side debounce; does not contradict Phase 99 D-03 which prohibited debounce *in the worker*. Latest-wins token still discards stale results that do arrive.

### Q2 — Consistency across surfaces

| Option | Description | Selected |
|--------|-------------|----------|
| Same policy both surfaces | Identical timing in ResultDialog and Browse | ✓ |
| Differ per surface | Immediate in ResultDialog, debounced in Browse | |

**User's choice:** Same policy both surfaces

---

## Done check

| Option | Selected |
|--------|----------|
| Create context | ✓ |
| Revisit worker scope | |
| Revisit pane visibility | |

---

## Claude's Discretion

- Exact HE + EN placeholder wording per failure reason.
- Controller shape (standalone `PdfImageController` class vs methods on `GenizahGUI`).
- Debounce timer ownership (per-surface vs shared).
- Toggle-button hide-vs-disable for non-PDF LOCAL.
- Filepath resolution path at enqueue time.

## Deferred Ideas

- Per-surface differing timing policy (rejected in favor of D-05 consistency).
- Re-render-on-zoom / adaptive DPI (Phase 99 milestone-level deferral).
- Unrelated todos surfaced as keyword matches only — not folded.
