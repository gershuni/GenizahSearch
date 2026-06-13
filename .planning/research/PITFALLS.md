# Pitfalls Research

**Domain:** Opt-in telemetry + crash reporting for a privacy-sensitive PyQt6 frozen-binary desktop app
**Researched:** 2026-06-13
**Confidence:** HIGH (based on direct codebase inspection of existing excepthook, posthog_server.py, and session persistence; supplemented by known PyInstaller/PostHog desktop patterns)

---

## Critical Pitfalls

### Pitfall 1: PII Leakage via Full Tracebacks — Frame Locals Containing File Paths and Query Text

**What goes wrong:**
The existing `_setup_crash_handler()` in `genizah_app.py` (line 160) calls `traceback.format_exception(exc_type, exc_value, exc_tb)` and writes the full traceback — with all frame locals — to `crash_log.txt`. If that same raw traceback is forwarded to PostHog, every frame in the call stack that has a local variable named `query`, `search_text`, `filename`, `path`, `folder`, or anything containing a My Library file path will leak into the event payload.

Concrete examples of locals that appear in the stack during real search operations:
- `SearchThread.run()` has `self.query` (the full search string the user typed)
- My Library indexer frames have `file_path`, `folder_path`, `filepath`
- `session.json` restore frames have `state_dict` keys that may include `last_query`
- Exception messages themselves can echo user input (e.g., `FileNotFoundError: [WinError 2] The system cannot find the file specified: 'C:\Users\hillel\Documents\my-documents\...'`)

**Why it happens:**
`traceback.format_exception` by default does NOT include locals (Python's standard `format_exception` only includes the source line, not variable values), but many crash reporting integrations call `traceback.format_tb` with `chain=True` or use `cgitb`-style formatters that DO capture locals. The risk is real if anyone adds `traceback.TracebackException.from_exception(e, capture_locals=True)` — a natural upgrade when the default output isn't informative enough. Additionally, the exception message string itself (`str(exc_value)`) often contains user-visible data: `ValueError: Invalid query term: "האמת"`, `OSError: [Errno 2] No such file or directory: 'C:\Users\hillel\private\thesis.pdf'`.

**How to avoid:**
- Build a `scrub_traceback(tb_str: str) -> str` function that applies these transforms before any network emission:
  1. Redact absolute paths: replace `C:\Users\<anything>` and `/home/<anything>` with `<path>` using a regex `r'[A-Za-z]:\\[^\\"\s]+(?:\\[^\\"\s]+)*'` (Windows) and `r'/(?:home|Users)/[^\s"\']+(?:/[^\s"\']+)*'` (Unix)
  2. Strip the exception message body (or use an allowlist of safe exception types — `AttributeError`, `TypeError`, `KeyError` — and strip the message for everything else)
  3. Never call `capture_locals=True` on any traceback formatter used for PostHog emission
- Emit only: exception type name, module + line number per frame (no source line text from My Library modules), app version, OS version
- Apply the scrubber in the excepthook BEFORE enqueuing, not in the drain thread (the drain thread must never see raw data)

**Warning signs:**
- Any frame from `shared/local_indexer*.py`, `desktop/my_library_tab.py`, or `gui_threads.py:SearchThread` appearing in a PostHog event `traceback` property
- Exception messages containing backslashes or forward-slash paths in PostHog
- `str(exc_value)` containing Hebrew text (would be a leaked query term)

**Phase to address:** Phase 111 (consent + telemetry infrastructure) — the scrubber must exist and be tested BEFORE the crash hook sends its first event. This is a Day 1 requirement, not a polish item.

---

### Pitfall 2: Exception-Hook Clobbering — Not Chaining to the Existing Handler

**What goes wrong:**
`genizah_app.py` already installs a `sys.excepthook` at module load time (line 168: `sys.excepthook = exception_hook`). This hook writes to `crash_log.txt` and calls `sys.__excepthook__`. If v8.1.0 installs a NEW `sys.excepthook` for PostHog reporting naively (`sys.excepthook = my_posthog_hook`), it silently replaces the existing crash-log hook. The old `crash_log.txt` behavior stops working entirely, and developers don't notice because the new hook sends to PostHog — but the local crash log that serves as the fallback when the user is offline or has opted out disappears.

The inverse problem also exists: if the new hook raises an exception itself (e.g., `enqueue_event` is not yet initialized at hook installation time, or the scrubber fails on a rare Unicode edge case), and it doesn't protect with `try/except`, the application will hit `sys.excepthook` re-entrancy, which Python handles by invoking `sys.__excepthook__` — silently swallowing both the original exception and the hook's crash.

**Why it happens:**
Developers write `sys.excepthook = new_hook` without reading the existing code at line 168. The existing hook is installed at module-level (line 170: `_setup_crash_handler()`), which runs before any class is instantiated, making it easy to miss in a code search that only looks at `GenizahGUI.__init__`.

**How to avoid:**
- Read and chain: capture the hook that is currently installed (`_prior_hook = sys.excepthook`) BEFORE replacing it, and call it at the end of the new hook
- The recommended pattern:

```python
def _install_telemetry_excepthook():
    _prior = sys.excepthook  # captures the crash_log hook already installed
    def _telemetry_hook(exc_type, exc_value, exc_tb):
        try:
            if _telemetry_enabled() and exc_type is not KeyboardInterrupt:
                _emit_crash_event(exc_type, exc_value, exc_tb)
        except Exception:
            pass  # hook must never raise
        _prior(exc_type, exc_value, exc_tb)  # always chain
    sys.excepthook = _telemetry_hook
```

- Guard the entire telemetry body in `try/except Exception: pass` — the hook is not allowed to raise
- For `threading.excepthook`: install separately, same chaining pattern. QThread worker crashes (e.g., `SearchThread`, `IndexerThread`, `LocalIndexerWorker`) are NOT delivered to `sys.excepthook` — they go to `threading.excepthook` (Python 3.8+). Without a separate hook there, 100% of worker-thread crashes are invisible to the telemetry system. Check for `threading.__excepthook__` (Python 3.10+) to chain correctly.

**Warning signs:**
- `crash_log.txt` stops being written after v8.1.0 ships (regression in existing behavior)
- Worker-thread crashes (e.g., `LocalIndexerWorker` PDF extraction failures) never appear in PostHog despite being real exceptions

**Phase to address:** Phase 111 — the chaining requirement must be an explicit success criterion, not just mentioned in comments. A test should verify `crash_log.txt` is still written after the telemetry hook is installed.

---

### Pitfall 3: Network I/O Inside the Exception Hook

**What goes wrong:**
If PostHog emission (`requests.post(...)`) is called directly inside `sys.excepthook` (not via the queue), the app blocks on network I/O at crash time. On a slow connection or when PostHog's endpoint is unreachable, this adds a 2–30 second hang before the app closes — during a crash, which is precisely when the user most wants the app to die quickly. Worse, if the crash was caused by a network-related exception (e.g., the NLI circuit breaker, an httpx timeout), calling `requests.post` inside the hook can trigger the same exception again, causing re-entrancy.

**Why it happens:**
The natural instinct is to emit the crash event synchronously to ensure it's delivered before the process exits. The flaw is that `sys.excepthook` runs in the main thread, synchronously, before any exit cleanup.

**How to avoid:**
The existing `shared/posthog_server.py` is already the correct answer: `enqueue_event()` is fire-and-forget (uses `queue.put_nowait`; never blocks; never raises). The exception hook must call `enqueue_event(...)` only — never `requests.post(...)` directly. The drain thread handles the actual HTTP call.

The critical companion issue: the drain thread is a daemon thread (`daemon=True`, line 133 in `posthog_server.py`). Daemon threads are killed when the main thread exits. At crash time, the main thread exits immediately after `sys.excepthook` returns (Python calls `os._exit(1)` for unhandled exceptions). The enqueued crash event will be lost unless the hook adds a brief `_event_queue.join()` or `time.sleep(0.3)` to give the drain thread time to flush. This is the only acceptable blocking operation inside the hook, and it must have a hard timeout (e.g., `queue.join()` with a background flag, or simply `time.sleep(0.5)` as an approximation).

**Warning signs:**
- Crash events never appear in PostHog (daemon thread killed before drain)
- App appears to hang after a crash before closing (synchronous HTTP call in hook)

**Phase to address:** Phase 111 — add a flush helper `_flush_queue_before_exit(timeout=0.5)` and call it in the crash hook after enqueueing. Test it with a mock queue.

---

### Pitfall 4: Consent Incorrectly Implemented — Telemetry Fires Before Consent is Recorded

**What goes wrong:**
Telemetry fires on first launch before the consent dialog is displayed and confirmed. This can happen in three ways:
1. `enqueue_event()` is called from `GenizahGUI.__init__` (e.g., an "app_started" event) before the consent dialog is shown
2. The consent dialog is shown asynchronously (e.g., via `QTimer.singleShot(0, show_consent)`) and events fire in the window between launch and dialog display
3. Session restore runs before the consent dialog and emits a "session_restored" event

**Why it happens:**
The app has a complex multi-phase startup: `__init__` initializes tabs, loads session state, triggers auto-rescan (all before any dialog). If telemetry emission is wired into these paths without a consent gate, events escape before the user has been asked.

**How to avoid:**
- The telemetry module must expose a single gate: `is_telemetry_enabled() -> bool` that checks the persisted consent flag before allowing any emission. `enqueue_event()` should NOT be called directly from application code — all call sites must go through a wrapper that checks the gate first
- The consent flag must be read from persistent storage (QSettings with key `telemetry_consent`, values: `unset` / `granted` / `denied`) — `unset` means not yet shown, which must be treated as denied
- The first-run consent dialog must be shown as a BLOCKING modal (not a timer-deferred show) before any session restore or search activity begins. The existing `_setup_crash_handler()` pattern of module-level initialization is a model for how to run something unconditionally at startup
- The install-ID UUID must NOT be minted until consent is granted. Minting it at install time and then asking for consent later is a GDPR mistake — the UUID is generated before any legitimate interest basis exists

**Warning signs:**
- PostHog receives "app_started" or "session_restored" events from users who later appear in the "opted out" cohort
- The `distinct_id` for a crash event is a UUID that was generated before the consent dialog was shown

**Phase to address:** Phase 111 — the consent gate (`is_telemetry_enabled()`) must be the first thing built. All subsequent telemetry call sites are gated on it. The gate's behavior on `unset` must be tested explicitly.

---

### Pitfall 5: "Opt-In" That Defaults On, or Opt-Out That Doesn't Stop Emission

**What goes wrong:**
Two failure modes:
1. The consent dialog defaults to "Accept" (pre-checked checkbox or Enter key mapped to Accept), making most users effectively opted in by default without reading the dialog
2. The user clicks "Opt out" in Settings, but events that were already enqueued in `_event_queue` before the opt-out are drained and sent by the daemon thread anyway

**Why it happens:**
(1) UX pressure: "Accept" feels like the primary action. Pre-checking "Help improve the app" is a common dark pattern that slips in during implementation.
(2) The drain thread in `posthog_server.py` has no knowledge of the opt-out state — it drains everything in the queue regardless. If a user opts out and there are 5 queued events from the previous session, they all get sent.

**How to avoid:**
- (1) The consent dialog must default to the UNCHECKED / DECLINED state. The "Decline" button must be the visually equal or primary button. Test this: if the user presses Enter without reading the dialog, they must be opted out. This is a GDPR requirement for opt-in consent.
- (2) On opt-out, drain and discard the queue without sending: iterate `_event_queue` with `get_nowait()` until empty, discarding all items. Add a `disable_and_flush()` function to `posthog_server.py`. The drain thread should check the consent state before each `requests.post()` call. Delete the install-ID UUID from QSettings on opt-out so the `distinct_id` cannot be reused if the user later opts back in (a new UUID should be minted on next opt-in).

**Warning signs:**
- PostHog cohort analysis shows >60% opt-in rate (typical for opt-in consent is 20-40%; >60% suggests the dialog defaults to accepted)
- Events appearing in PostHog for users whose QSettings shows `telemetry_consent=denied` (queue-race condition)

**Phase to address:** Phase 111 — consent dialog UX + queue-purge on opt-out are both Day 1 requirements with explicit tests.

---

### Pitfall 6: Non-Anonymous Install ID — The UUID Contains or Derives From PII

**What goes wrong:**
The "anonymous per-install UUID" is generated using `uuid.uuid1()` (which embeds the MAC address) or `uuid.uuid3(uuid.NAMESPACE_DNS, username)` or `uuid.uuid5(uuid.NAMESPACE_URL, socket.gethostname())`. These produce deterministic IDs tied to hardware or user identity. A researcher who runs two instances of the app on the same machine will produce the same UUID, and any third party with network access logs can correlate the UUID to the machine.

**Why it happens:**
Developers reach for `uuid.uuid1()` or `uuid.uuid5()` because they seem "more unique" without realizing they encode the MAC address.

**How to avoid:**
- Use `uuid.uuid4()` (random, no PII) stored in QSettings. Generate once on first opt-in, never regenerate unless the user explicitly resets (or opts out and back in).
- Do NOT derive the UUID from anything machine-specific: no hostname, no MAC, no username, no Windows SID.
- The UUID must ONLY be used as `distinct_id` in PostHog. It must never appear in filenames, log files, or any place where it could be correlated with user identity by a third party.
- Document in the code comment why `uuid4()` is required (not `uuid1()`).

**Warning signs:**
- `uuid.getnode()` or `socket.gethostname()` anywhere near UUID generation
- Two PostHog events from the same user showing the same `distinct_id` on two different machines (would indicate collision or MAC-based generation)

**Phase to address:** Phase 111 — one-line implementation choice, but must be explicit in requirements. Add a test that the generated UUID passes `re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', uuid_str)` (UUID v4 pattern check).

---

### Pitfall 7: First-Run Consent Dialog Re-Prompts Every Launch

**What goes wrong:**
The consent dialog appears on every launch because the persistence layer for the consent decision is not initialized correctly. Common causes: (a) the QSettings organization/application name changes between app versions (e.g., `"Dicta"/"GenizahSearchPro"` → `"Dicta"/"DictaGenizahSearchPro"`), causing QSettings to not find the existing key; (b) the consent flag is stored in `session.json` which gets cleared by the "Re-index All" or "Reset My Library" flows; (c) the flag key is checked with a different name than it was written.

**Why it happens:**
`desktop/my_library_tab.py` already uses `QSettings("Dicta", "GenizahSearchPro")` — the consent state MUST use the same organization/application pair, otherwise the settings live in a different registry hive/file.

**How to avoid:**
- Store the consent flag in QSettings under the SAME organization/application key already in use (`"Dicta"/"GenizahSearchPro"`), not in a new settings file
- Never store the consent flag in `session.json` (it gets cleared on reset/crash recovery)
- Use a single constant for the key name: `TELEMETRY_CONSENT_KEY = "telemetry/consent"` — never inline the string at two call sites
- The consent dialog must check: if QSettings contains the key with a non-`unset` value, skip the dialog. If the key is absent or `unset`, show the dialog.
- Write a test that simulates the full consent-accept → app restart → no-dialog path

**Warning signs:**
- Consent dialog appearing on every launch (visible immediately in UAT)
- QSettings written under `"Dicta"/"DictaGenizahSearchPro"` instead of `"Dicta"/"GenizahSearchPro"`

**Phase to address:** Phase 111 — the persistence key and QSettings identifier must be locked in the design doc before implementation. One extra test covers this.

---

### Pitfall 8: Event Volume Blowup From Per-Search Events at Heavy-User Scale

**What goes wrong:**
An "app_search_performed" event is emitted on every search. The project already knows Hillel performs ~50 searches/day. With a few dozen researchers as users (the realistic early-adopter cohort for a specialized Genizah tool), that's ~50 × 30 users = 1,500 events/day minimum. PostHog's free tier allows 1M events/month, so raw count is not the issue — but if search events carry properties like `result_count`, `duration_ms`, `search_mode`, and `filter_state`, PostHog's autocapture and person-property updates can generate multiple backend operations per event, and the property cardinality for `result_count` (0–50,000) creates an effectively unbounded histogram. The real risk is if the event body is not carefully bounded and a future developer adds `query_length` (OK) → `query_hash` (OK) → `query_text` (catastrophic PII leak).

**Why it happens:**
Event properties grow organically. Once a PostHog event exists, adding a new property is a one-liner. Without a formal allowlist, someone adds `search_text` thinking "it's just for debugging" and it ships.

**How to avoid:**
- Define a STATIC allowlist of permitted event properties for each event type. The allowlist lives as a module-level constant in the telemetry module and is enforced by a `_sanitize_properties(event_name, props)` function that drops any key not in the allowlist before calling `enqueue_event()`.
- For search events: permitted properties are `search_mode` (enum: keyword/responsa/composition/parallels), `corpus_scope` (genizah/local/all), `result_count_bucket` (binned: 0/1-10/11-50/51-200/200+), `duration_bucket_ms` (binned: <500/<2000/<5000/5000+), `app_version`. NEVER `query_text`, `query_length`, `filter_content`, `exclusion_list_size`.
- Sample search events: emit only 1 in N (e.g., N=5 or N=10) for heavy search usage. Session/crash events are emitted at 1:1 (they're low-frequency and high-value).

**Warning signs:**
- PostHog event schema showing a `query_*` or `search_text` property (immediate blocker)
- Monthly event count growing faster than user count (suggests per-keystroke events)
- PostHog property cardinality warnings on any numeric property

**Phase to address:** Phase 111 for the allowlist infrastructure; Phase 112 (or whichever phase adds search telemetry) must enforce the allowlist as a test.

---

### Pitfall 9: PyInstaller Frozen Binary — Daemon Thread Killed Before Queue Drains on Exit

**What goes wrong:**
`shared/posthog_server.py` starts a daemon thread (`daemon=True`, line 132). In a normal Python process, the interpreter waits for non-daemon threads before exiting. Daemon threads are killed immediately when all non-daemon threads finish. In a PyInstaller frozen `.exe`, the exit sequence is: Qt app exec loop ends → Python interpreter begins teardown → all daemon threads receive SIGKILL (Windows: `TerminateThread`). The `posthog-shared-drain` thread may have an event in its `requests.post()` call mid-flight, or may not have been scheduled yet, when it is killed. Any events enqueued in the final seconds before exit — including the most valuable "app_closed" and crash events — are silently lost.

**Why it happens:**
This is an inherent property of daemon threads + process exit, not a bug in the code. The design was chosen deliberately for the web use case (web process lives forever; crash events are rare). The desktop use case has a distinct exit path that the web use case does not.

**How to avoid:**
- Add an `atexit` handler (registered at import time in `posthog_server.py`) that calls a `_flush_before_exit(timeout=2.0)` function. This function: stops accepting new events (sets a `_shutting_down` flag), then drains the queue synchronously (bypassing the daemon thread) with a hard timeout. The queue's `maxsize=10000` means this can at worst block for `10000 × 2ms_per_request = 20s` — the timeout cap prevents this.
- For crash events specifically: the `sys.excepthook` must call `_flush_before_exit(timeout=0.5)` BEFORE returning, since `atexit` handlers do NOT run on unhandled exceptions in CPython (the process calls `os._exit()` after the excepthook returns).
- Test this explicitly: write a test that enqueues 3 events, calls `_flush_before_exit()`, and verifies all 3 were POSTed to a mock server.

**Warning signs:**
- "app_closed" events missing from PostHog for sessions that have "app_started" events (indicates drain-before-exit is not working)
- Crash events from the crash hook never appear in PostHog (daemon thread killed before drain)

**Phase to address:** Phase 111 — `_flush_before_exit()` must be part of the initial `posthog_server.py` desktop extension, not added later when someone notices events are missing.

---

### Pitfall 10: PyInstaller Frozen Binary — SSL Certificate Bundle and `requests` at Runtime

**What goes wrong:**
In a PyInstaller frozen `.exe`, the `certifi` CA bundle used by `requests` may not be found at its expected path (`certifi.where()`), causing `requests.post()` to raise `SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]` on the first PostHog call. The drain thread catches all exceptions silently (`except Exception: pass`, line 124 in posthog_server.py), so this failure is invisible — events are silently dropped.

**Why it happens:**
PyInstaller bundles Python packages, but `certifi`'s CA bundle is a data file (`cacert.pem`) that must be included explicitly via a `.spec` `datas` entry or `certifi.where()` hook. If it's missing, `requests` falls back to the system's certificate store on Windows — which usually works — but on some Windows configurations (air-gapped machines, enterprise certificate stores, older Windows versions) it can fail.

**How to avoid:**
- Verify in `GenizahSearchPro.spec` that `certifi` data files are collected: add `from PyInstaller.utils.hooks import collect_data_files` and include `collect_data_files('certifi')` in `datas`
- Add a startup self-test: attempt a `requests.get("https://eu.i.posthog.com/", timeout=2)` and if it raises `SSLError`, log a warning and disable telemetry (do not crash the app)
- The drain thread's `except Exception: pass` is correct for production, but add a `logger.debug(f"posthog drain error: {e}", exc_info=True)` so the problem appears in `crash_log.txt` during debugging

**Warning signs:**
- Zero PostHog events from any user on Windows 10 (pre-2021 OS with older cert store)
- `SSLError` messages in `crash_log.txt` from the drain thread (once logging is added)

**Phase to address:** Phase 111 — verify `.spec` configuration before first release. Add the SSL self-test.

---

### Pitfall 11: Fully Offline / Air-Gapped Machines — Telemetry Must Degrade Silently

**What goes wrong:**
Academic researchers working with sensitive manuscripts sometimes operate on air-gapped networks (no internet access). If the telemetry system fails non-silently on network unavailability — showing an error dialog, slowing startup, or causing the GUI to freeze — it creates a support burden and may cause users to distrust the app.

**Why it happens:**
`requests.post(POSTHOG_CAPTURE_URL, json=payload, timeout=2.0)` with `timeout=2.0` already handles the per-request case, but if DNS resolution blocks (which happens before the TCP timeout starts on some Windows configurations), the drain thread can block for up to 30 seconds per event. On an air-gapped machine, this means the drain thread is permanently blocked and the queue fills up to `maxsize=10000`. The queue is bounded, so `put_nowait()` raises `queue.Full` and events are dropped — but 10,000 dropped events per session means `get_dropped_event_count()` reports a very large number that could mislead diagnostics.

**How to avoid:**
- The drain thread's `requests.post(timeout=2.0)` timeout covers the read but not always DNS. Use `requests.post(timeout=(1.0, 2.0))` — `(connect_timeout, read_timeout)` — so connection attempts fail fast on unreachable hosts
- After 5 consecutive network failures, the drain thread should enter a 60-second backoff (exponential retry) rather than hammering the queue. This prevents the queue from filling on sustained offline use
- At app startup, check `POSTHOG_API_KEY` is set; if not (which is the case on development machines without the env var), skip all telemetry silently. This is already the behavior of `posthog_server.py` (line 112: `if not api_key: continue`), but it must also apply to the install-ID minting and consent dialog (no point showing a consent dialog if the API key is not bundled)

**Warning signs:**
- User reports of slow startup on VPN or air-gapped network
- Drop counter (`get_dropped_event_count()`) at 10,000 (queue full) for users who are known to be offline

**Phase to address:** Phase 111 — the `(connect_timeout, read_timeout)` tuple and backoff logic are small changes to the drain loop, but must be in the initial implementation.

---

### Pitfall 12: PostHog Project Key Exposed in Binary — Misunderstood as a Secret

**What goes wrong:**
The PostHog project API key (a `phc_...` token) is embedded in the frozen `.exe` binary. Someone runs `strings GenizahSearchPro.exe | grep phc_` and finds the key. They now can POST arbitrary events to the GenizahSearch PostHog project, inflating event counts, poisoning cohorts, or exhausting the monthly quota.

**Why it happens:**
Developers confuse the PostHog "project API key" with a secret. It is explicitly a PUBLISHABLE key (PostHog's documentation calls it "Project API Key (public)"). The key is also visible in the web app's JavaScript payload, so it was never secret.

**How to avoid:**
- The PostHog project key is NOT a secret and does not need to be treated as one. Embedding it in the binary is correct.
- The real mitigation is on the PostHog side: enable "Allowed Domains / Origins" for the project to restrict which origins can ingest events. For desktop apps this is harder (there is no HTTP Origin header), but PostHog's free-tier projects have no per-IP rate limit. Set a PostHog rate limit alert at 10x expected daily volume.
- Use a SEPARATE PostHog project for desktop telemetry (distinct from the web app's project). This contains any blast radius from abuse: desktop events can't corrupt web analytics, and vice versa.
- Do NOT use `SUPABASE_ANON_KEY` for any telemetry purpose. That key, while also publishable, has different abuse implications (write access to Supabase RLS-gated tables).

**Warning signs:**
- PostHog showing events from `distinct_id` values that are not UUID-v4 format (indicates external injection)
- Daily event count 10x higher than user count × searches_per_user

**Phase to address:** Phase 111 — one design decision (separate PostHog project for desktop), one PostHog dashboard alert. Not a code change.

---

### Pitfall 13: Privacy Law Framing — Consent Record Not Persisted, Disclosure Not Bilingual

**What goes wrong:**
Two compliance gaps:
1. The consent decision is stored in QSettings (local registry), but there is no timestamped consent record. If a user claims "I never consented," there is no evidence of when consent was granted or declined.
2. The app serves Hebrew-language users (the entire academic audience for Cairo Genizah studies). A consent dialog that exists only in English fails GDPR's "intelligible and easily accessible" requirement for non-English speakers.

**Why it happens:**
Consent timestamp logging feels like over-engineering for a small academic tool. But the app already has bilingual infrastructure (`tr()` + `TRANSLATIONS`) — not using it for the consent dialog is an oversight, not a deliberate choice.

**How to avoid:**
- Store the consent decision with a timestamp in QSettings: `telemetry/consent` = `granted` or `denied`, `telemetry/consent_date` = ISO-8601 timestamp. This provides a minimal audit trail without writing to any remote system.
- The consent dialog must be bilingual (Hebrew + English) using the existing `tr()` infrastructure. The privacy disclosure text must be consistent with what the app already says in `docs/OPEN_ISSUES.md` and the Help page section on privacy (the app already has bilingual disclosures — use the same language).
- The disclosure must clearly state: (a) what is collected (feature usage counts, crash tracebacks — scrubbed), (b) what is NOT collected (search query content, My Library file paths/filenames), (c) who processes the data (PostHog EU, Dicta/Hebrew University), (d) how to opt out at any time (Settings → About or equivalent).
- Opt-out must work even after consent is granted: re-prompting in Settings with the same bilingual dialog.

**Warning signs:**
- Consent dialog text exists only in English in the codebase
- QSettings has `telemetry/consent` but no `telemetry/consent_date`
- The disclosure text claims "we do not collect search queries" but the property allowlist allows `query_length` (close but still a vector — drop it)

**Phase to address:** Phase 111 — bilingual text and timestamp storage are part of the consent dialog implementation. These are not post-release compliance fixes.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Using `traceback.format_exception` without scrubbing for crash events | Easy to implement | Leaks file paths + query text to PostHog | Never — scrubber must exist before first crash event is sent |
| Storing consent in `session.json` instead of QSettings | Consistent with other state | Gets wiped by Reset/crash recovery; consent dialog re-prompts every launch | Never — consent is not session state |
| Emitting events without a property allowlist | Fast to add new properties | One accidental `query_text` property leaks user data | Never for production events |
| `uuid.uuid1()` for install ID | "More unique" | Embeds MAC address, non-anonymous | Never |
| Sharing PostHog project with the web app | One dashboard | Desktop event schema pollution; desktop abuse inflates web analytics | Never |
| Calling `requests.post()` inside `sys.excepthook` | Guaranteed delivery | Blocks app at crash time; re-entrancy risk | Never |
| Not installing `threading.excepthook` | Simpler setup | Worker-thread crashes (SearchThread, LocalIndexerWorker, all QThreads) are invisible | Never — this is where most desktop crashes actually originate |
| No `atexit` flush + no hook flush | Simpler exit path | Crash events and "app_closed" events reliably lost in frozen binary | Never for crash events |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| PostHog EU endpoint | Using `https://app.posthog.com/capture` (US) | Use `https://eu.i.posthog.com/capture` — already correct in `posthog_server.py:44` |
| PostHog `distinct_id` | Passing `"system"` for all desktop events | Mint a UUID-v4 per install on consent; pass that as `distinct_id` for all user-session events; use `"desktop-system"` only for events with no user context (very rare) |
| QSettings persistence | Using a new organization/app string | Use `QSettings("Dicta", "GenizahSearchPro")` — same as `desktop/my_library_tab.py:1047` |
| `threading.excepthook` chaining | `threading.excepthook = new_hook` without saving prior | Capture `_prior = threading.excepthook` (or `threading.__excepthook__` for Python 3.10+) and chain |
| PyInstaller `certifi` | Not including cert bundle in `.spec` | Add `collect_data_files('certifi')` to `datas` in `GenizahSearchPro.spec` |
| `sys.excepthook` re-entrancy | Calling `enqueue_event()` which calls `_start_drain_thread_once()` which calls `threading.Thread()` | All of these are safe as long as the entire hook body is wrapped in `try/except Exception: pass` |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Synchronous PostHog call in UI thread | GUI freeze during event emission | Never call `requests.post()` from any UI-thread code path; all emission goes through `enqueue_event()` | Every call |
| Unbounded `result_count` property | PostHog histogram with 50,000 distinct values | Use bucketed property `result_count_bucket` (enum: 0/1-10/11-50/51-200/200+) | At first search |
| Per-keystroke events in search box | 50 events/second from active user | Only emit on search COMPLETION, not on query text changes | At first search session |
| Loading `QSettings` on every `enqueue_event()` call | QSettings reads registry on every call; at 50 searches/day this is fine but if called per-frame it's not | Cache the consent flag in a module-level `_telemetry_enabled: bool` variable, updated only on Settings change | If called from a render loop |
| Crash traceback scrubbing regex on a 100KB traceback | CPU spike at crash time | Pre-compile the scrubbing regexes at module import time | On multi-megabyte tracebacks (unlikely but possible with deep call stacks) |

---

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Sending raw exception message string (`str(exc_value)`) to PostHog | Leaks Hebrew query text, file paths, Supabase error details | Strip exception message body or allow only safe exception types' messages |
| Sending frame locals in traceback | Leaks search queries, file paths, Supabase tokens | Never use `capture_locals=True`; scrub before enqueue |
| Using Supabase `anon_key` or JWT token as PostHog `distinct_id` | Exposes auth token in PostHog event history | Only use the separately minted UUID-v4 install ID |
| Logging the install UUID to `crash_log.txt` | Correlates anonymous PostHog ID to local machine (if log file is shared in a support ticket) | Do not include the UUID in `crash_log.txt` |
| Emitting OS username or hostname | Links telemetry to real-world identity | Never include `os.getlogin()`, `socket.gethostname()`, or environment variables in events |

---

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Consent dialog on every launch (persistence bug) | Researchers find the app annoying; some will uninstall | Fix persistence before release; test consent-persist → restart cycle |
| English-only consent dialog | Hebrew-primary users cannot understand what they are agreeing to | Bilingual dialog using existing `tr()` infrastructure |
| No way to change consent decision after first run | Users who want to opt in later cannot | Settings → About (or dedicated Settings toggle) with the same consent dialog |
| Generic "Help improve the app" language | Academic users don't know what is/isn't collected | Specific: "We collect: tab usage counts, search mode counts, crash reports (no query text, no file names)" |
| Consent dialog appearing before the main window is visible | Jarring; user hasn't seen the app yet | Show consent after the main window is rendered and visible, triggered by `QTimer.singleShot(300, show_consent_if_needed)` — but ensure no events fire in those 300ms |

---

## "Looks Done But Isn't" Checklist

- [ ] **Crash hook chaining:** Verify `crash_log.txt` is STILL written after the telemetry hook is installed — the old hook must be chained, not replaced
- [ ] **Worker-thread crashes:** Verify `threading.excepthook` is installed — `sys.excepthook` only fires for main-thread crashes; all QThread/worker crashes require the separate threading hook
- [ ] **Queue drain on exit:** Verify that the "app_closed" event actually appears in PostHog (daemon thread is killed before it can drain without explicit flush)
- [ ] **Consent gate on crash hook:** Verify that if `telemetry/consent = denied`, crash events are NOT sent to PostHog (the crash hook must check consent state before enqueuing)
- [ ] **Queue purge on opt-out:** Verify that opting out in Settings purges the existing queue (pre-opt-out events must not drain after the user clicks "Opt out")
- [ ] **UUID version:** Verify the install UUID is UUID v4 (random), not UUID v1 (MAC-based) — `assert uuid_str[14] == '4'`
- [ ] **Offline silence:** Verify the app starts normally with no network connection and telemetry silently drops events (no dialog, no delay, no crash)
- [ ] **Bilingual consent:** Verify the consent dialog renders in Hebrew when the app language is set to Hebrew
- [ ] **No query text in events:** Verify via PostHog schema that no `query_*` or `search_text` property exists on any event type
- [ ] **PyInstaller SSL:** Verify PostHog events are received from the frozen `.exe` on a fresh Windows machine without Python installed

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| PII leak via traceback | HIGH — requires coordinated disclosure, PostHog event deletion, patch release | Delete PostHog events via the PostHog API, ship patched version, notify affected users if any PII was actually captured |
| Consent defaults on | MEDIUM — patch release + re-prompting existing users | Detect "consent was set before v8.1.1" via `telemetry/consent_date`, re-show dialog for those users |
| UUID is UUID v1 | LOW — UUID v1 is not actually identifying in practice unless attacker has access to PostHog AND MAC address | Rotate the UUID on next opt-in; add migration to regenerate on next launch |
| Crash hook replaces instead of chains | LOW — `crash_log.txt` stops being written; detectable immediately | One-line fix to capture and chain prior hook |
| Events lost at process exit | LOW — events are lost, not leaked | Add `atexit` flush; no user-visible impact while the bug exists |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| PII via traceback (#1) | Phase 111 — consent + infrastructure | Test: emit a fake crash with a frame local containing a file path; verify the PostHog event contains no path |
| Hook clobbering + chaining (#2) | Phase 111 | Test: verify `crash_log.txt` is written AND PostHog event enqueued for the same exception |
| Sync network in hook (#3) | Phase 111 | Test: `sys.excepthook` must complete in <50ms (mock queue, no network) |
| Telemetry before consent (#4) | Phase 111 | Test: events enqueued before consent is recorded must be 0 |
| Opt-in defaults on / opt-out race (#5) | Phase 111 | Test: Enter key on consent dialog produces `denied`; queue is empty after opt-out |
| Non-anonymous UUID (#6) | Phase 111 | Test: UUID v4 format assertion |
| Consent re-prompts every launch (#7) | Phase 111 | Test: consent dialog shown exactly once across two simulated launch cycles |
| Event volume blowup (#8) | Phase 111 (allowlist infrastructure); Phase 112 (search events) | Test: `_sanitize_properties` rejects any key not in the allowlist |
| PyInstaller daemon thread exit (#9) | Phase 111 | Test: mock drain, enqueue event, call `_flush_before_exit()`, verify event was POSTed |
| PyInstaller SSL certs (#10) | Phase 111 | Manual: run frozen `.exe` on clean VM, verify PostHog event received |
| Offline / air-gapped machines (#11) | Phase 111 | Test: drain loop with mock socket timeout; verify app does not block or crash |
| PostHog key misunderstood as secret (#12) | Phase 111 | Non-code: use separate PostHog project; set volume alert |
| Privacy law / bilingual disclosure (#13) | Phase 111 | Review: consent dialog text reviewed for GDPR compliance; Hebrew translation present |

---

## Sources

- Codebase: `genizah_app.py` lines 148-170 — existing `_setup_crash_handler()` and `sys.excepthook` installation
- Codebase: `shared/posthog_server.py` — existing fire-and-forget queue; daemon thread; `POSTHOG_HOST`; `maxsize=10000`; no atexit flush; no opt-out gate
- Codebase: `desktop/my_library_tab.py` line 1047 — `QSettings("Dicta", "GenizahSearchPro")` (must match consent key)
- Codebase: `gui_threads.py` — QThread workers that raise exceptions not caught by `sys.excepthook`
- Codebase: `.planning/PROJECT.md` — v8.1.0 milestone definition: opt-in, anonymous UUID, scrubbed tracebacks, reuse `posthog_server.py`
- [Python docs: `sys.excepthook`](https://docs.python.org/3/library/sys.html#sys.excepthook) — "If an exception is not otherwise handled, the interpreter calls `sys.excepthook`"; does not run for `threading.Thread` exceptions
- [Python docs: `threading.excepthook`](https://docs.python.org/3/library/threading.html#threading.excepthook) — Python 3.8+; handles exceptions from `threading.Thread.run()`; must be installed separately from `sys.excepthook`
- [PostHog Python SDK docs](https://posthog.com/docs/libraries/python) — publishable project API key design; EU endpoint; `distinct_id` guidance
- [GDPR Article 7: Conditions for consent](https://gdpr-info.eu/art-7-gdpr/) — opt-in must be freely given, specific, informed, unambiguous; pre-checked checkboxes are not valid consent
- [PyInstaller certifi bundle](https://pyinstaller.org/en/stable/hooks-contrib.html) — `certifi` data files must be explicitly included for `requests` SSL verification in frozen binaries
- [PostHog: "What is the project API key?"](https://posthog.com/docs/getting-started/install?tab=snippet#get-your-project-api-key) — "This is your project API key (also known as a write key). It can be made public."

---
*Pitfalls research for: opt-in telemetry + crash reporting, PyQt6 frozen-binary desktop app (v8.1.0)*
*Researched: 2026-06-13*
