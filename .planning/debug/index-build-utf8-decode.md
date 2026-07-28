---
slug: index-build-utf8-decode
status: root_cause_found
trigger: |
  DATA_START
  User got the 8.1 version desktop app and installed it, with the transcription.txt
  file (initially in the same folder, but the problem was also when transferring it
  to another folder). He cannot build index, got error like (relayed over phone,
  not verbatim): "UTF-8 Codec cannot decode byte 0xd7 in position 0: invalid
  continuation byte"
  DATA_END
created: 2026-07-21
updated: 2026-07-21
---

# Debug Session: Desktop index build fails with UTF-8 decode error

## Symptoms
- **Expected:** Settings -> Build / Rebuild Index reads `Transcriptions.txt` and builds the Tantivy index.
- **Actual:** Build aborts immediately; error dialog shows raw `UnicodeDecodeError`.
- **Error (relayed by phone, approximate):** `'utf-8' codec can't decode byte 0xd7 in position 0: invalid continuation byte`
- **Environment:** Desktop app v8.1, Windows, a supplied `Transcriptions.txt`.
- **Timeline:** Fails on a fresh install with the supplied file; persists after moving the file to another folder (so NOT a file-location problem — location would produce "Input file not found", not a decode error).

## Current Focus
- hypothesis: The supplied `Transcriptions.txt` is NOT valid UTF-8 (re-encoded to a legacy Windows/Hebrew codepage or otherwise corrupted during transfer). The indexer opens the corpus with a bare `encoding='utf-8'` and re-raises the raw error, so any non-UTF-8 file crashes the build with no guidance.
- test: Dump the first 16 bytes of the user's file and/or attempt a cp1255 decode.
- expecting: user's file byte 0 is a Hebrew byte (0xD7.. / 0xE0..) instead of the ASCII header `3d 3d 3e 20` (`==> `).
- next_action: get first bytes of the user's file from the collaborator; convert their file to UTF-8; (permanent) harden the corpus reader.

## Evidence
- timestamp 2026-07-21: A valid local `Transcriptions.txt` (1.47 GB) starts with ASCII `==> 990000412990...` = bytes `3d 3d 3e 20 39 39...`. This decodes as UTF-8 cleanly; a genuine corpus file cannot fail at "position 0" on a Hebrew byte because its header is ASCII.
- timestamp 2026-07-21: `shared/indexer.py::create_index` opens the corpus with a bare `encoding='utf-8'` and NO error handling — at `count_lines` (line ~290) and the main read loop (line ~297). No BOM detection, no fallback, no `errors=`.
- timestamp 2026-07-21: `shared/metadata_manager.py:511` opens `FILE_V7` with bare `encoding='utf-8'` (same fragility). CONTRAST: `libraries.csv` at line 294 is read defensively with `errors='replace'`.
- timestamp 2026-07-21: `gui_threads.py::IndexerThread.run` (lines 44-49) wraps `create_index` in `try/except Exception as e: self.error_signal.emit(str(e))` — it surfaces the RAW `UnicodeDecodeError` message to the user, which matches the reported text exactly.
- timestamp 2026-07-21: `shared/config.py:63,71` — when frozen, `FILE_V8 = <dir of exe>/Transcriptions.txt`. Moving the file away from the exe would yield `FileNotFoundError` ("Input file not found"), not a decode error — confirming the failure is about file CONTENT/encoding, not location.

## Eliminated
- hypothesis: File-location / "not next to exe" problem. ELIMINATED — that path raises FileNotFoundError with a different message; the user gets a decode error regardless of folder.
- hypothesis: Bug in the parsing/separator logic. ELIMINATED — failure is at file decode (open/iterate), before any line parsing.

## Root Cause
Two layers:
1. **Data (proximate):** The `Transcriptions.txt` distributed to the user is not valid UTF-8. Most likely it was re-encoded to a legacy Windows Hebrew codepage (cp1255 / ISO-8859-8) or otherwise altered during transfer (opened+resaved in Notepad/Word as ANSI, or a corrupt/partial copy). The genuine master file on the dev machine is valid UTF-8.
2. **Code (contributing / recurrence):** `shared/indexer.py::create_index` (and `metadata_manager.py:511`) read the corpus with a bare `encoding='utf-8'` and no BOM/encoding fallback; `IndexerThread` re-emits the raw exception. Result: a cryptic, unactionable crash for any non-UTF-8 corpus file.

## Fix
- **Immediate unblock (user's machine):** re-encode the file to UTF-8 (cp1255 -> utf-8) OR re-obtain a known-good UTF-8 copy of the corpus.
- **Permanent (code, future desktop release):** make the corpus reader BOM-aware and encoding-tolerant (try `utf-8-sig`, detect UTF-16 BOM, fall back to cp1255) and replace the raw error surface with an actionable message ("Transcriptions.txt is not UTF-8 — re-save as UTF-8"). Mirrors the existing `errors='replace'` treatment of `libraries.csv`.
