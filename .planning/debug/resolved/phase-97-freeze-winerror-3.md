---
slug: phase-97-freeze-winerror-3
status: resolved
trigger: "Phase 97 just shipped. User added a huge Dropbox folder with deeply-nested Hebrew filenames. UI freezes after Cancel→Discard; thousands of `scan_all: stat failed: [WinError 3]` errors spam the console for paths that exist."
created: 2026-05-25
updated: 2026-05-26
resolved: 2026-05-26
diagnose_only: true
phase: 97
fix_commit: pending
---

# Debug Session: phase-97-freeze-winerror-3

## Symptoms

### Expected behavior
After clicking Cancel → Discard on an in-progress LOCAL scan of a large Dropbox folder, the indexing should stop quickly (within seconds), discard partial run data, and return UI to interactive state. Console should not flood with errors during scan.

### Actual behavior
1. **UI freezes** for a long time after Cancel → Discard click. Window does not respond.
2. **Thousands of `scan_all: stat failed for <path>: [WinError 3] The system cannot find the path specified`** lines spam the console — for files that demonstrably exist (they were enumerated by os.walk).

### Error messages (verbatim sample)
```
scan_all: stat failed for c:\users\gersh\dropbox\ספרים\geniza books\library\complete scanned books and dissertations\geniza\אמיר אשור\books\magnes\נירית רייכל --- סיפורה של מערכת החינוך הישראלית- בין ריכוזיות לביזור; בין מוצהר לנסתר; בין חיקוי לייחוד --- הוצאת מאגנס --- book.magnes-4348.pdf: [WinError 3] The system cannot find the path specified: 'c:\\users\\gersh\\dropbox\\ספרים\\geniza books\\library\\complete scanned books and dissertations\\geniza\\אמיר אשור\\books\\magnes\\נירית רייכל --- סיפורה של מערכת החינוך הישראלית- בין ריכוזיות לביזור; בין מוצהר לנסתר; בין חיקוי לייחוד --- הוצאת מאגנס --- book.magnes-4348.pdf'

MuPDF error: format error: cmsOpenProfileFromMem failed
MuPDF error: syntax error: no XObject subtype specified

scan_all: stat failed for c:\users\gersh\dropbox\ספרים\geniza books\library\complete scanned books and dissertations\אמיר אשור\books\magnes\editors יותם חותם, מתיאס שמידט, נועם זדוף --- היסטוריה כשליחות- אסופת מאמרים לכבודו של משה צימרמן במלאת לו שישים --- הוצאת מאגנס --- book.magnes-1121.pdf: [WinError 3] The system cannot find the path specified: ...
```

### Timeline
- **2026-05-25**: Phase 97 closed (`97-VERIFICATION.md`). User loaded the just-shipped build with a huge Dropbox tree of Hebrew-named PDFs. Both symptoms surfaced immediately on first scan.
- Pre-Phase-97 (v7.14 Phase 95): Hard ceiling 5K/2GB blocked these large folders, so this code path was effectively unreachable in production.

### Reproduction
On Windows 11, in the desktop app:
1. Open MyLibraryTab.
2. Add a folder containing >>10K files with deeply nested Hebrew + Latin filenames (user's case: `C:\Users\gersh\Dropbox\ספרים\geniza books\library\complete scanned books and dissertations\...`).
3. Accept the >50K/>50GB soft warning if shown; let the scan begin.
4. While the scan is running, click Cancel → Discard.
5. Observe: console floods with `stat failed: [WinError 3]`; window becomes unresponsive for an extended period.

## Initial hypotheses (from orchestrator handoff)

**H1 — Windows MAX_PATH for the WinError 3 storm:**
- `shared/local_sys_id.py:73-86` (`_canonical_filepath`) does `Path(p).resolve(strict=False)` then `os.path.normcase()`. Neither step adds the `\\?\` long-path prefix.
- `shared/local_indexer.py:1502` calls `os.stat(filepath)` on the canonical path.
- On Windows, paths > 260 chars without `\\?\` prefix fail with ERROR_PATH_NOT_FOUND from `GetFileAttributesEx`, even though `os.walk` (using `FindFirstFile/FindNextFile`) found them.

**H2 — UI freeze: blocking wait on UI thread:**
- `desktop/my_library_tab.py:1458` calls `self._worker.wait(5000)` from the UI thread after Cancel → Discard.
- If worker is mid-`os.walk` or stuck logging thousands of warnings, it cannot honor the cancel flag within 5s.

**H3 — UI freeze: greedy `disk_files` build:**
- `shared/local_indexer.py:1459-1462` consumes the entire `_iterate_supported_files` generator into a dict before any cancel-check fires in the main per-file loop.
- For 100K+ files, this phase alone can take minutes; the inner-loop cancel_check at line 1496 is unreachable until it completes.

**H4 — UI freeze: logger flood on Qt main thread:**
- Thousands of `logger.warning("scan_all: stat failed...")` calls in tight loop. If the project's logging handler bridges to a Qt log widget (or any UI element), each call queues a UI event, drowning the event loop.

**H5 — Lock contention between scan thread and `discard_run`:**
- `discard_run` (`shared/local_indexer.py:~2898`) does Tantivy writer ops + SQLite BEGIN IMMEDIATE on 3 tables.
- Scan thread holds writer + may be mid-commit; discard waits for those locks.

## Current Focus

```yaml
hypothesis: H1 CONFIRMED, H2 CONFIRMED, H3 CONFIRMED (partial), H4 ELIMINATED, H5 CONFIRMED (race condition)
test: completed — path length measurement, os.stat vs \\?\ prefix test, code-trace of cancel/wait/discard sequence, logging handler audit
expecting: all four bugs interact; H1 is the root cause of the error storm; H2+H3 jointly cause freeze; H5 is a secondary race
next_action: write fix plan (diagnose_only gate — do not apply)
reasoning_checkpoint: all hypotheses resolved with file:line evidence or live measurement
tdd_checkpoint: null
```

## Evidence

- timestamp: 2026-05-25T00:00:00Z
  type: measurement
  finding: >
    Both sample failing paths measure 265 and 267 Unicode characters respectively
    (Python `len()` on the raw string from the error console). `Path.resolve(strict=False)`
    does NOT add the `\\?\` UNC prefix on Windows 11 — the resolved path is still
    265 chars. `os.path.normcase()` does not add it either.
    Script: `_tmp/measure_paths.py`.

- timestamp: 2026-05-25T00:00:00Z
  type: live_test
  finding: >
    `os.stat("c:\\users\\gersh\\dropbox\\ספרים\\...\\book.magnes-4348.pdf")` →
    `[WinError 3] The system cannot find the path specified` (len=265, no UNC prefix).
    `os.stat("\\\\?\\c:\\users\\gersh\\dropbox\\ספרים\\...\\book.magnes-4348.pdf")` →
    SUCCESS, size=7774055 bytes. The file physically exists; the API call fails
    because the path exceeds 260 chars without the long-path prefix.
    Script: `_tmp/test_walk_vs_stat.py`.

- timestamp: 2026-05-25T00:00:00Z
  type: code_trace
  finding: >
    `_canonical_filepath` (local_sys_id.py:85-86) produces paths without `\\?\` prefix.
    `scan_all` at local_indexer.py:1502 calls `os.stat(filepath)` on these paths.
    Both `_iterate_supported_files:1870` (`os.path.getsize`) and `scan_all:1502`
    (`os.stat`) are affected, but only the latter emits the `scan_all: stat failed`
    warning (the former swallows OSError with fsize=0).

- timestamp: 2026-05-25T00:00:00Z
  type: code_trace
  finding: >
    H2 (blocking wait): `_on_cancel_clicked` (my_library_tab.py:1457-1464) calls
    `self._worker.cancel()` then `self._worker.wait(5000)`. If the worker is
    mid-`_iterate_supported_files` walking 100K files (which alone can take >5 s
    on a Dropbox folder), `wait(5000)` times out and returns False. There is NO
    check of `self._worker.isRunning()` after wait — `discard_run()` is called
    unconditionally whether or not the worker actually stopped.

- timestamp: 2026-05-25T00:00:00Z
  type: code_trace
  finding: >
    H3 (greedy disk_files build): `_scan_all_impl` (local_indexer.py:1459-1462)
    runs `for filepath, file_size in self._iterate_supported_files(folder_path, cancel_check): disk_files[filepath] = file_size`
    which consumes the ENTIRE generator into a dict BEFORE the per-file cancel_check
    at line 1496. The generator checks cancel_check only at the START of each
    DIRECTORY (local_indexer.py:1863), not per file within a directory. For a
    Dropbox folder with many large flat directories (hundreds of files per dir),
    cancel can be delayed by the length of that directory's file list.
    Consequence: if disk_files build is in progress when Cancel is clicked,
    the worker cannot honor the cancel flag for up to the duration of the
    current directory traversal (potentially minutes for 100K-file trees).

- timestamp: 2026-05-25T00:00:00Z
  type: code_trace
  finding: >
    H4 (logger flood on Qt main thread): ELIMINATED. `configure_logger()` in
    genizah_core.py:2608-2628 registers only `SafeRotatingFileHandler` (file)
    and `StreamHandler` (stderr/stdout). No Qt widget handler. The warning flood
    is on the scan thread and goes to stderr; it does not enqueue Qt events.

- timestamp: 2026-05-25T00:00:00Z
  type: code_trace
  finding: >
    H5 (race condition): `self._writer` is an instance-level attribute (not
    thread-local) shared by the scan thread (LocalIndexerWorker.run →
    scan_all → _index_one_file → self._writer.add_document / commit) and any
    UI-thread caller. `discard_run()` (local_indexer.py:2923-2942) calls
    `self._writer.rollback()` and then `self._writer = None` from the UI thread,
    potentially while the scan thread is mid-`self._writer.add_document()` or
    mid-`_commit_writer_with_retry()`. Tantivy writers are NOT documented as
    thread-safe for concurrent access from multiple threads. This is a race
    condition. The QMutex (D-25) in MyLibraryTab serializes Refresh/Add/Remove
    requests but does NOT protect `discard_run` against a still-running scan.

## Eliminated

- **H4** — Logger not Qt-bridged. `configure_logger()` (genizah_core.py:2608-2628) uses
  only `SafeRotatingFileHandler` + `StreamHandler`. Warning flood goes to stderr
  on the scan thread; no UI event queue involvement.

## Resolution

```yaml
root_cause: >
  Three compounding bugs:

  BUG-1 (H1 — WinError 3 storm, PRIMARY): `_canonical_filepath` (local_sys_id.py:85-86)
  uses `Path.resolve(strict=False)` + `os.path.normcase()`, neither of which adds the
  Windows `\\?\` long-path prefix. `os.stat()` in scan_all (local_indexer.py:1502) and
  `_index_one_file` (line:1919) then fail with ERROR_PATH_NOT_FOUND ([WinError 3]) for
  any path > 260 chars. `os.walk` (using FindFirstFile/FindNextFile internally) enumerates
  these files successfully, creating the paradox of "file exists but stat fails." Measured:
  both sample paths are 265-267 chars; os.stat fails; os.stat with \\?\ prefix succeeds.

  BUG-2 (H2 + H3 — UI freeze, PRIMARY): The Cancel→Discard path blocks the UI thread at
  `self._worker.wait(5000)` (my_library_tab.py:1458). For 100K-file Dropbox trees the
  `disk_files` build loop (`_scan_all_impl` lines 1459-1462) can take >>5 s, causing
  wait() to time out and return False. After timeout, `discard_run()` is called
  unconditionally while the worker is still running. The `_iterate_supported_files` generator
  checks cancel_check only per-directory (line:1863), not per-file — so directories with
  hundreds of files hold the cancel check for the entire file list. The UI is unresponsive
  for the entire duration of `wait(5000)` (5 s guaranteed) plus whatever time `discard_run`
  spends on Tantivy writer ops and SQLite.

  BUG-3 (H5 — Tantivy writer race, SECONDARY): `self._writer` is a shared instance
  attribute. When wait(5000) times out and the scan thread is still running,
  `discard_run` (UI thread) calls `self._writer.rollback()` + `self._writer = None`
  concurrently with the scan thread calling `self._writer.add_document()` or
  `_commit_writer_with_retry()`. This is an unsynchronized cross-thread mutation of a
  non-thread-safe Tantivy writer object. May cause silent data corruption or crash.

fix: not applied (diagnose_only gate)
fix_plan: |
  FIX-1 (BUG-1): In `_canonical_filepath` (local_sys_id.py:85-86), after resolve(),
  if running on Windows and the path length > 260, prepend `\\?\` to the resolved
  path string before returning (and before normcase). Also apply `\\?\` in
  `_iterate_supported_files` (local_indexer.py:1867-1870) for the `os.path.getsize` call,
  and ensure any direct `os.stat` caller uses the same prefixed path.
  Alternatively: enable long-path support system-wide via the Windows registry key
  `HKLM\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled=1` and note it
  in deployment docs — but this requires admin rights and a reboot, so the code fix
  is safer.

  FIX-2 (BUG-2, part a): Replace the blocking `self._worker.wait(5000)` on the UI
  thread with a non-blocking approach: show a "Stopping..." progress dialog, connect
  `worker.finished_signal` to dismiss it and trigger discard_run asynchronously.
  Or: use a QTimer-poll (isRunning check every 100 ms) so the event loop stays alive.

  FIX-2 (BUG-2, part b): Add per-file cancel check inside `_iterate_supported_files`
  inner loop (line:1866) so cancel_check fires between individual files, not just
  between directories. This reduces cancel latency from "one directory worth of files"
  to "one file."

  FIX-3 (BUG-3): After `self._worker.wait(5000)`, check `self._worker.isRunning()`.
  If still running, do NOT call discard_run immediately — wait for finished_signal and
  trigger discard_run in the signal handler. This guarantees the writer is not in use
  when discard_run accesses it.
```

## Specialist Review

(pending — specialist dispatch by session-manager)
