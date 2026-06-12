---
status: resolved
trigger: "Prod log 2026-06-12 12:08:44: Parallels Error: create_parallels_page.<locals>.execute_parallels.<locals>.progress_cb() missing 1 required positional argument: 'total' — TypeError raised from genizah_core.py:1064 via lab_composition_search"
created: 2026-06-12
updated: 2026-06-12
---

## Symptoms

DATA_START
- **Expected:** Running a Lab composition search from the web Parallels page (`web/pages/parallels.py::execute_parallels` → `state.lab_engine.lab_composition_search`) completes and reports progress.
- **Actual:** Search crashes with a TypeError before producing results; user sees "Parallels Error" on the web.
- **Error (verbatim from prod journalctl):**
  ```
  Parallels Error: create_parallels_page.<locals>.execute_parallels.<locals>.progress_cb() missing 1 required positional argument: 'total'
  Traceback (most recent call last):
    File "/home/ubuntu/GenizahSearch/web/pages/parallels.py", line 2220, in run_search
      result = state.lab_engine.lab_composition_search(
    File "/home/ubuntu/GenizahSearch/genizah_core.py", line 1638, in lab_composition_search
      for score, doc_addr in iterator:
    File "/home/ubuntu/GenizahSearch/genizah_core.py", line 1064, in _execute_batched_search
      progress_callback(f"Scanning items {i}-{min(i+BATCH_SIZE, total_hits)} / {total_hits}...")
    File "/home/ubuntu/GenizahSearch/genizah_core.py", line 1628, in <lambda>
      batch_cb = lambda *args: progress_callback(*args) if callable(progress_callback) else None
  TypeError: create_parallels_page.<locals>.execute_parallels.<locals>.progress_cb() missing 1 required positional argument: 'total'
  ```
- **Timeline:** Observed live on genizah-web prod 2026-06-12 12:08:44 (process 4107819). Likely a callback-contract mismatch: `_execute_batched_search` calls `progress_callback(msg_string)` with ONE positional arg, but the web page's `progress_cb` is defined as `(current, total)`. Possibly introduced/exposed by batched-search or Phase 110 composition/LAB work.
- **Reproduction:** On genizahsearch.com Parallels page, run a composition search that goes through the LAB engine path (`state.lab_engine.lab_composition_search`). Crashes once the batched search emits its first progress message.
DATA_END

## Evidence

- timestamp: 2026-06-12
  checked: genizah_core.py:1052-1067 (_execute_batched_search progress block)
  found: Dual-protocol callback — line 1058 calls progress_callback(i, total_hits) WRAPPED in try/except (re-raises InterruptedError/KeyboardInterrupt, swallows Exception); line 1064 calls progress_callback("Scanning items ...") with ONE string arg, UNPROTECTED.
  implication: Any two-arg-only callback crashes on the string call; asymmetric protection means the numeric call is fail-safe but the string call is fatal.

- timestamp: 2026-06-12
  checked: web/pages/parallels.py:2137-2144 (progress_cb definition)
  found: def progress_cb(current, total) — two REQUIRED positional args, no string handling. Shared by both lab and standard branches of run_search. Raises InterruptedError on cancel.
  implication: Exactly matches prod TypeError "missing 1 required positional argument: 'total'" when core sends single string.

- timestamp: 2026-06-12
  checked: gui_threads.py:259-292 (desktop LabCompositionThread)
  found: Desktop cb is def cb(arg1, arg2=None) with isinstance dispatch — comment "Callback handler that supports both (int, int) and (str)". Strings go to status_signal, (int,int) to progress_signal.
  implication: The dual protocol is the ESTABLISHED core contract; the desktop has always handled it. The web callback never did — web-side bug, not a core regression.

- timestamp: 2026-06-12
  checked: grep progress_callback( across genizah_core.py (15 call sites)
  found: Line 1064 is the ONLY single-string invocation in the entire core. All others are (i, total) or (i, total, sid). Line 1064 is only reachable via _execute_batched_search, which only runs when deep_scan=True (parallels.py:2149 — deep_scan only honored in lab mode).
  implication: Crash requires web Lab mode + Deep Scan — matches prod trigger; explains why standard composition works fine.

- timestamp: 2026-06-12
  checked: genizah_core.py:1420-1429 (sibling batch_cb in the other deep-scan path) vs :1626-1629 (lab_composition_search batch_cb)
  found: The 1422 batch_cb wraps progress_callback(*args) in try/except Exception → pass; the 1628 lambda forwards with NO protection.
  implication: Same string call is non-fatal on the 1429 path but fatal on the 1629 path used by lab_composition_search — explains why the TypeError escaped through the generator at :1638.

- timestamp: 2026-06-12
  checked: genizah_core.py:1712 + web/pages/parallels.py:2250
  found: lab_composition_search catches InterruptedError → was_interrupted=True (partial results); web run_search also catches InterruptedError. Cancellation flows via InterruptedError raised inside the progress callback.
  implication: Any hardening of the string call must re-raise InterruptedError/KeyboardInterrupt (same pattern as lines 1057-1062) or cancellation breaks for both apps.

## Eliminated

- hypothesis: Core regression — Phase 110 changed the callback contract that the web previously satisfied
  evidence: Desktop has handled the dual (int,int)/(str) protocol via isinstance dispatch since the Lab deep-scan feature existed (gui_threads.py:265 comment documents it); core line 1064's string protocol is long-standing. The web progress_cb simply never implemented the string half of the contract — it only crashed now because lab+deep_scan was exercised on the web.
  timestamp: 2026-06-12

## Current Focus

hypothesis: CONFIRMED and FIXED — web progress_cb lacked the string half of the core's dual-protocol progress contract; fix applied in web/pages/parallels.py (dual-protocol cb) + genizah_core.py (string-call guard); regression tests added and red/green verified
next_action: Await human verification — Hillel to run a Lab-mode deep-scan composition search on the web Parallels page (locally or after deploy) and confirm it completes without "Parallels Error"

reasoning_checkpoint:
  hypothesis: "genizah_core._execute_batched_search emits a dual-protocol progress callback — (i, total) numeric AND a single-string status message (line 1064). The web's progress_cb (parallels.py:2137) requires exactly two positional args, so the string call raises TypeError. The lab_composition_search pass-through lambda (1628) forwards without exception protection, so the TypeError escapes the generator and kills the search."
  confirming_evidence:
    - "Prod traceback frames match exactly: parallels.py:2220 → genizah_core.py:1638 (iterator loop) → :1064 (string call) → :1628 (lambda) → TypeError missing 'total'"
    - "Direct read of genizah_core.py:1064 shows progress_callback(f'Scanning items ...') — one string arg, unprotected; web progress_cb signature is (current, total) with both required"
    - "Desktop cb (gui_threads.py:265) explicitly handles both protocols with isinstance dispatch — proving the dual protocol is the intended core contract and the web side never implemented it"
    - "grep shows line 1064 is the only single-string callback site in the core, reachable only with deep_scan=True — matching the lab deep-scan trigger in prod"
  falsification_test: "If the web progress_cb already accepted a single string arg, or if line 1064 were wrapped in the same try/except as line 1058, the prod TypeError could not have been raised from that frame chain. Both checked directly — neither is true."
  fix_rationale: "Root-cause fix: give the web callback the same dual-protocol handling the desktop has (string → cancel-check only since the numeric call immediately precedes each string call and already drives the web progress UI; numeric → existing progress update). Defense-in-depth: wrap the core's string call in the identical guard the numeric call already has, so a misbehaving callback can degrade progress display but never kill a long search — for ANY caller. Cancellation preserved by re-raising InterruptedError/KeyboardInterrupt, which both web and desktop rely on."
  blind_spots: "Cannot reproduce the prod lab deep-scan run locally end-to-end (no prod-size LAB index loaded in this session); verifying via targeted unit tests of the callback contract instead. Other web pages defining (current,total) callbacks were not audited — but grep shows no other path reaches the string-emitting _execute_batched_search."

## Resolution

root_cause: web/pages/parallels.py progress_cb is defined as (current, total) with both args required, but genizah_core._execute_batched_search's dual-protocol contract also invokes the callback with a single string status message (genizah_core.py:1064, unprotected). The lab_composition_search forwarding lambda (genizah_core.py:1628) provides no exception shielding (unlike the sibling batch_cb at 1422-1427), so the TypeError propagates through the batched-search generator and aborts the entire lab deep-scan composition search. Desktop is unaffected because its callback (gui_threads.py:265) handles both protocols.
fix: |
  (1) web/pages/parallels.py — progress_cb signature changed (current, total) → (arg1, arg2=None) with isinstance dispatch, mirroring desktop gui_threads.LabCompositionThread.cb: string status messages are accepted (cancel-check, content ignored since the preceding numeric call already drives the web progress UI); numeric (current, total) behavior unchanged.
  (2) genizah_core.py:_execute_batched_search — string status call wrapped in the same try/except guard the numeric call already had: InterruptedError/KeyboardInterrupt re-raised (cancellation preserved for web + desktop), other exceptions swallowed so a callback arity mismatch can never again abort a deep-scan search for ANY caller.
  (3) tests/test_batched_search_progress_protocol.py — 4 regression tests: dual-protocol emission pinned; two-required-arg callback no longer aborts the generator; InterruptedError propagation pinned; AST guard that web progress_cb stays callable with one positional arg.
verification: |
  - Red phase proven: with the two source fixes stashed, test_two_required_arg_callback_does_not_abort_search and test_web_progress_cb_accepts_single_positional_arg FAIL exactly as the prod incident predicts (2 failed, 2 passed); fix restored via stash pop.
  - Green phase: all 4 new tests pass.
  - Targeted regression: tests/test_comp_corpus_scope.py + test_lab_composition_chunk_hits.py + test_search_serializer.py + test_smoke_round2_export_gaps.py + new file = 166 passed, 1 skipped (pre-existing), 0 failures.
  - ruff check genizah_core.py web/pages/parallels.py tests/test_batched_search_progress_protocol.py — clean.
  - Desktop safety: desktop cb already dual-protocol (unchanged); core guard re-raises InterruptedError (test 3); CompositionThread's two-arg cb feeds search_composition_logic which never reaches _execute_batched_search (grep: only LabEngine lines 1429/1629 call it).
  - Codex cross-AI review (gpt-5.5, _tmp/codex-debug-fixes-CRITIQUE-2026-06-12.md): APPROVE, zero findings; confirmed the 3-arg (i,total,sid) callback sites are in MetadataManager.batch_fetch_shelfmarks and unreachable from the web progress_cb.
  - Human verification: Hillel confirmed fixed 2026-06-12.
files_changed:
  - web/pages/parallels.py
  - genizah_core.py
  - tests/test_batched_search_progress_protocol.py
