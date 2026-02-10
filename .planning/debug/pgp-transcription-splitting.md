---
status: diagnosed
trigger: "PGP transcription shows full text on all pages instead of splitting by recto/verso markers"
created: 2026-02-05T10:00:00Z
updated: 2026-02-05T10:25:00Z
symptoms_prefilled: true
goal: find_root_cause_only
---

## Current Focus

hypothesis: CONFIRMED - PGP transcription is stored and passed as a single blob, no parsing of section markers exists anywhere in the codebase
test: Traced full data flow: Supabase -> document_service -> browse.py -> version_selector -> UI
expecting: Expected to find either parsing logic or page_info awareness - found neither
next_action: Document findings and return diagnosis

## Symptoms

expected: PGP transcription should display only the relevant section for each page/image (recto text on recto image, verso text on verso image, margin sections on their respective images)
actual: Full transcription text displays on ALL pages - recto shows full text, verso shows full text
errors: None (functional bug, not crash)
reproduction: View any PGP document with transcription that has section markers (e.g., T-S 8J22.21)
started: Always been this way - initial implementation

## Eliminated

(none yet)

## Evidence

- timestamp: 2026-02-05T10:10:00Z
  checked: web/document_service.py - get_document_for_fragment()
  found: Returns full document including transcription as single blob, no parsing
  implication: Source of transcription data makes no attempt to split

- timestamp: 2026-02-05T10:12:00Z
  checked: web/pages/browse.py lines 872-887
  found: PGP transcription loaded once per sys_id, stored in state.pgp_transcription as {'content': full_blob}
  implication: Same content passed to version_selector regardless of page_number

- timestamp: 2026-02-05T10:15:00Z
  checked: web/components/version_selector.py - create_version_selector()
  found: Accepts pgp_transcription dict but uses pgp_transcription['content'] directly (line 119, 124, 170)
  implication: No awareness of page context - full content shown on every page

- timestamp: 2026-02-05T10:18:00Z
  checked: pgp_data/transcriptions_linked.csv
  found: Section markers exist in transcription content (Recto, Verso, Recto - right margin, Verso - address, etc.)
  implication: Markers ARE present in data, just not parsed

- timestamp: 2026-02-05T10:20:00Z
  checked: Supabase schema (document_fragments table)
  found: page_info column exists with values like "recto", "verso", "recto and verso"
  implication: Per-fragment page info IS available but not used for transcription splitting

- timestamp: 2026-02-05T10:22:00Z
  checked: browse.py line 2100-2106 (version_selector creation)
  found: page_number passed (page.p_num) but only used for user corrections, not PGP splitting
  implication: page awareness exists but not connected to transcription parsing

## Resolution

root_cause: PGP transcription content is stored as a single blob in the 'documents' table and passed through unchanged to the UI. No logic exists to:
  1. Parse section markers (Recto, Verso, Recto - right margin, etc.) from the transcription text
  2. Map parsed sections to page numbers or page_info values
  3. Filter transcription content based on current page context

fix: (not applied - diagnosis only)
verification: (not verified - diagnosis only)
files_changed: []
