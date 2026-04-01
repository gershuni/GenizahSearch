# Phase 61 — Cross-AI Reviews

**Date:** 2026-04-01
**Reviewers:** Gemini 2.5 Pro (2 rounds), Codex gpt-5.4 (no output both rounds)

---

## Round 1 — Plan Review (2026-03-31)

### Gemini: HIGH — Read-path ie_id filtering gap

Plans added ie_id to WRITE paths (create_correction, create_comment) but not READ paths. `get_corrections()` and `get_document_comments()` fetched all volumes regardless of active volume.

**Status:** FIXED — Added ie_id filter to `get_corrections()`, `get_comments()` in web/supabase_client.py and supabase_corrections_client.py. Filter: `ie_id.eq.{ie_id},ie_id.is.null`. Also threaded ie_id through notes_display.py (`fetch_document_comments`, `create_notes_panel`, `create_notes_button`).

---

## Round 2 — Final Code Review (2026-04-01)

### Gemini Review of Uncommitted Changes

**Summary:** v7.7 updates successfully address the image/text mismatch for multi-IE manuscripts. browse_map repair logic is comprehensive, shared cache prevents pickle corruption race condition.

**Strengths:**
- Robust data recovery without full re-index
- Thread-safety improvements via shared cache
- Consistent ie_id context propagation to community features
- Manchester auto-default improves UX (follows JTS pattern)

**Concerns:**
- **MEDIUM — Primitive Locking**: Boolean flag with sleep loop is not atomic. `threading.Lock` would be more robust.
- **LOW — Repair Performance**: Transcriptions.txt scan is O(N) but runs once per process lifecycle.
- **LOW — Regex Specificity**: UID regex `(IE\d+_P\d+_FL\d+)` assumes stable NLI format.

**Risk Assessment:** MEDIUM overall — browse_map is core to all 217K manuscripts, but shared cache reduces risk frequency.

### Codex

No output (CLI exit-only, both rounds).

---

## Action Items

| Concern | Severity | Status |
|---------|----------|--------|
| Read-path ie_id filtering | HIGH | FIXED |
| Boolean locking → threading.Lock | MEDIUM | ACCEPTED (adequate for NiceGUI single-process) |
| Transcriptions.txt scan perf | LOW | ACCEPTED (runs once, cached) |
| Regex format assumption | LOW | ACCEPTED (NLI format is stable) |
