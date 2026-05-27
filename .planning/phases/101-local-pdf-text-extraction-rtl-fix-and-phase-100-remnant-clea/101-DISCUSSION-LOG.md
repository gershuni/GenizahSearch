# Phase 101: LOCAL PDF text extraction RTL fix and Phase 100 remnant cleanup - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea
**Areas discussed:** RTL fix technique, Backfill existing indexes, RTL detection gate, RTL verification fixture

---

## Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| RTL fix technique | python-bidi vs x-coord reordering vs string-reversal helper | ✓ |
| Backfill existing indexes | Auto-reindex vs document-only vs flag-and-prompt | ✓ |
| RTL detection gate | Unconditional vs ratio-gated bidi pass | ✓ |
| RTL verification fixture | Synthetic vs real PDF vs unit-test-only | ✓ |

**User's choice:** All four areas.

---

## RTL fix technique

| Option | Description | Selected |
|--------|-------------|----------|
| python-bidi | Unicode Bidi Algorithm on extracted lines; already in lock-file; needs requirements.txt + PyInstaller spec wiring | ✓ |
| x-coordinate reordering | get_text("dict") + sort spans by descending x on RTL lines; no new dep but hand-rolls bidi | |
| Let researcher compare | Empirically test both, default to python-bidi | |

**User's choice:** python-bidi.
**Notes:** Reported bug = word-order reversed with letters correct → plain char reversal would corrupt words; bidi handles it correctly. Fix lands in shared `extract_pdf_pages` so index + display both benefit.

---

## Backfill existing indexes

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-reindex via version bump | Bump extractor/index version → stale detection → auto reindex on next launch (reuse recovery machinery) | ✓ |
| Document-only — manual rebuild | Fix new content only; tell users to rebuild | |
| Flag stale, prompt user | One-time prompt offering reindex | |

**User's choice:** Auto-reindex via version bump.
**Notes:** Researcher to confirm exact staleness-detection hook (extractor-version constant vs `.meta.json` schema-version). Text-layer reindex only — no image re-render.

---

## RTL detection gate

| Option | Description | Selected |
|--------|-------------|----------|
| Unconditional, bidi-safe | Run bidi on every line; no-op on pure LTR | ✓ |
| Gate on RTL-char ratio | Reorder only above _rtl_ratio threshold (0.4) | |
| Let researcher verify idempotency | Default unconditional, confirm no-op on LTR first | |

**User's choice:** Unconditional, bidi-safe.
**Notes:** Researcher must still confirm python-bidi is a true no-op on pure-LTR/numeric lines (existing single_word_per_line.pdf as LTR guard); fall back to ratio-gate only if regression found.

---

## RTL verification fixture

| Option | Description | Selected |
|--------|-------------|----------|
| Synthetic RTL PDF fixture | Build a deterministic PDF reproducing the reversal | |
| Real Hebrew PDF from failing book | Commit excerpt from the Phase 100 UAT book | ✓ |
| Unit-test reorder function directly | Known reversed-string in/out, no PDF | |

**User's choice:** Real Hebrew PDF from the failing book.

### Follow-up: Provenance / copyright

| Option | Description | Selected |
|--------|-------------|----------|
| I'll provide a small excerpt | User supplies the PDF; commit 1–2 page excerpt + copyright note | ✓ |
| Single page, public-domain only | Use only PD-licensed Hebrew book | |
| Real PDF kept out of git | Verify with real PDF stored outside repo; assert against snapshots | |

**User's choice:** User (Hillel) will provide a small excerpt — prerequisite inbound asset for the planner/executor.

---

## Cleanup trio (WR-01 / WR-02 / test-flake)

| Option | Description | Selected |
|--------|-------------|----------|
| I'm ready for context | Capture the three cleanups as specified in 100-REVIEW.md | ✓ |
| Discuss the cleanup trio | Talk through fix aggressiveness before locking | |

**User's choice:** Ready for context — cleanups captured as prescriptive (D-07/D-08/D-09).

## Claude's Discretion

- Module placement/naming of the bidi-reorder helper; whether to retire the dead-code `_fix_rtl_*` helpers.
- Exact flake-fix mechanism once the polluting sibling test is identified.

## Deferred Ideas

- PDF OCR for image-only PDFs (D-F2) — out of scope for v7.15.
