# Feature Research

**Domain:** Opt-in, privacy-first desktop telemetry for a scholarly PyQt6 research tool
**Researched:** 2026-06-13
**Confidence:** HIGH

---

## Context: What Already Exists

The project does NOT start from zero. These pieces are already built and constrain the design:

- `shared/posthog_server.py` — fire-and-forget, thread-safe, daemon-queue emit to EU PostHog.
  Accepts `event`, `properties`, `distinct_id`. Already used by `shared/nli_circuit_breaker.py`.
  The desktop just needs to call `enqueue_event()` after checking the opt-in flag.
- `web/api_hardening.py` — already has `latency_bucket()` and `result_count_bucket()` helpers,
  and the proven pattern of bucketed metrics (no raw values). Reuse these.
- The web app already captures `search_executed` (with query text!), `browse_manuscript`,
  `parallels_search`, `result_opened`, `login_success/failed`. **The desktop must NOT copy the
  web's `query` field** — the web's `search_executed` includes `query: clean_query[:100]` which
  violates the hard rule for the desktop ("never transmit search/query content"). Desktop events
  are strictly counts, modes, enums, durations, and booleans.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist if an app claims "privacy-respecting telemetry." Missing any of these
makes the feature feel untrustworthy or incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Opt-in default (OFF) | Any privacy-respecting app must default to OFF; pre-ticked boxes are dark patterns under GDPR and general scholarly-tool norms | LOW | Consent flag stored in the desktop's persistent config file, not the registry. UUID minted only AFTER the user explicitly consents. |
| Bilingual first-run consent dialog | The app already has full EN/HE i18n infrastructure (tr()); a consent dialog only in English contradicts the app's design | LOW | Reuse tr() and existing TRANSLATIONS dict; both languages must say what IS and IS NOT collected. |
| Settings/About toggle to change anytime | Users must be able to revoke consent post-hoc without reinstalling | LOW | Wire into the existing desktop About/Settings dialog. Changing to OFF must immediately stop all enqueue_event() calls (flag checked before every emit). |
| Clear "what is collected / what is NOT" disclosure | Scholarly users distrust opaque telemetry; listing excluded categories (no query text, no My Library paths) is load-bearing for trust | LOW | Can be a bulleted list in the dialog body or a "Learn more" expander within the same dialog. |
| Anonymous per-install UUID | Correlate events per install without account or PII linkage | LOW | Use `uuid.uuid4()`, store in the app's config dir (e.g., `%APPDATA%\GenizahSearchPro\telemetry_id`). Only mint on first consent, delete on opt-out. |
| PostHog `$process_person_profile: false` on every event | Keeps events in PostHog's anonymous-tier table; 4x cheaper ingestion; no person profile ever created | LOW | Add this property to every `enqueue_event()` call's properties dict in the desktop emitter wrapper. The existing `shared/posthog_server.py` passes through whatever properties dict is given. |
| Hard code-level guard: no query/path content in any event | Without a code guard, content can creep into events via copy-paste bugs | MEDIUM | Static AST test (like `tests/test_no_raw_storage_access.py`) that asserts no desktop telemetry call passes a property named `query`, `path`, `filename`, `text`, `content`, or `shelfmark_raw`. Only allow an allowlist of safe property names. |
| Crash reporting: global exception hook + scrubbed traceback | Users expect crash data to be useful without leaking their environment | MEDIUM | `sys.excepthook` replacement + `threading.excepthook` (Python 3.8+) for background threads. Scrub: replace all OS path strings in frame filenames with basename-only. Strip frame `locals` entirely (locals can contain query text, file objects, etc.). |
| Crash reporting: handled/non-fatal error counts | Important errors that are caught (e.g., indexing failure, NLI fetch error) must still surface in telemetry even though they don't crash | LOW | Call `enqueue_event('desktop_error', {...})` in existing except blocks at high-value sites (LocalIndexer, search thread, NLI fetches). |

### Differentiators (Competitive Advantage)

Features that set this telemetry implementation apart from a generic analytics bolt-on and make
it trustworthy to scholarly users.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Session-summary performance event (not per-search) | Reduces volume ~50x for heavy users (50 searches/day) while still providing accurate aggregate performance data; shows thoughtful volume design | MEDIUM | Emit one `desktop_session_performance_summary` at app close or periodic flush (e.g., every 30 min), containing bucketed aggregates: search count by mode, median/p95 duration bucket, result count distribution. See Performance Metrics section below. |
| Feature/tab usage as counts (not presence/absence) | Counts of how many times each tab or feature was used per session are more actionable than a binary "used yes/no" | LOW | Use in-memory counters per session; flush in the session_summary event. |
| Responsa search mode breakdown | This app's unique grammar-expansion search is the research differentiator; knowing which Responsa sub-options are toggled (expansion, fuzzy, Judeo-Arabic) drives development priority | LOW | Enum property `responsa_options_bitmask` — a compact integer encoding which sub-options were active. Never includes the query text itself. |
| Per-crash deduplication key | Prevents a single reproducible crash from inflating crash counts; the `error_fingerprint` property (exception type + scrubbed module + line number) lets PostHog deduplicate | LOW | Compute from: `type(exc).__name__` + sanitized module (basename only, no full path) + line number only. |
| Opt-out removes the UUID file | An app that says "anonymous" but retains a persistent ID after opt-out is not fully anonymous | LOW | On opt-out: delete the `telemetry_id` file, stop all enqueue_event() calls. PostHog EU data residency already applies; local cleanup is the UX signal. |
| "What this helps us improve" copy in the dialog | A consent dialog that only lists what is collected reads as corporate compliance; one that explains what decisions the data drives (e.g., "which search modes to invest in", "which OS versions to support") reads as a genuine value exchange | LOW | Copywrite both EN and HE. One paragraph. |

### Anti-Features (Explicitly Excluded)

Features that seem natural but violate the privacy constraints or are inappropriate for a
scholarly research tool. Document these so scope creep is prevented.

| Anti-Feature | Why Requested | Why Excluded | Alternative |
|--------------|---------------|-------------|-------------|
| Transmit query / search text | "Real search queries would show what users are actually looking for" | Hard rule from project owner: query content is research data and may be unpublished. Even truncated queries (the web's `query[:100]`) are excluded for desktop. | Transmit only `search_mode` enum and `result_count_bucket`. Query patterns are visible in Responsa option usage. |
| Transmit My Library file paths, filenames, or document count | "Path info helps debug indexing problems" | My Library is personal documents. Paths reveal research subjects and file naming habits. Even an MD5 of a path is re-identifiable. | Use error type + indexing stage only (e.g., `stage: 'extract_pdf'`, `error_type: 'CorruptPDF'`). |
| Transmit shelfmark or sys_id of opened manuscripts | "Would show which manuscripts are being studied" | Shelfmarks identify research subjects. A scholar studying a specific manuscript may not want that visible even aggregately. | Use count-of-results-opened per session (integer), not identifiers. |
| Hardware fingerprinting (MAC address, CPU ID, screen resolution) | "Would improve install uniqueness" | Fingerprinting is not "anonymous" in any meaningful sense; MAC and CPU IDs are stable identifiers. Violates the spirit of opt-in consent. | Random UUID stored in the app config dir. If the user reinstalls, a new UUID is minted. |
| Always-on default (opt-out instead of opt-in) | "Higher participation rate" | VSCode's opt-out model has generated years of community friction and GDPR objections. For a scholarly tool, trust matters more than participation rate. | Accept lower numbers; be explicit that the data represents volunteers. |
| Third-party ad / marketing SDKs | No one would request this explicitly, but generic "add analytics" tickets can end up including SDKs with their own data collection (e.g., Google Analytics, Amplitude free tier with data sharing) | Violates consent; EU data residency policy. Already using EU PostHog self-serve — this is the only analytics destination. | Continue using EU PostHog exclusively. |
| Session recording / screen capture | "Would help diagnose UX issues" | Captures research content verbatim. Completely incompatible with any privacy promise. | Structured event taxonomy covers UX flows without capturing screen state. |
| Transmit Supabase user ID or email | "Would correlate desktop with web usage" | The desktop opt-in is for anonymous telemetry; linking it to an authenticated identity breaks the anonymity guarantee. | Track desktop as an independent anonymous install. The user ID is never passed to `enqueue_event()`. |
| Automatic opt-in upgrade on app update | "User already said yes once to something" | Consent must be fresh and specific. An update that silently enables telemetry for users who previously declined is a dark pattern. | If the user declined, keep declined across updates. Only re-ask if the collected data categories change materially. |
| Per-search performance events at full rate (~50/day) | "Granular data is better" | 50 searches/day x 30 users = 1,500 events/day just for search performance — noisy and expensive. PostHog EU free tier is 1M events/month; keeping headroom is good practice. | Session-summary aggregation (see Differentiators). |
| Crash dialog that shows raw traceback to user | "Transparency" | Raw tracebacks include full file paths (exposes My Library folder structure) and local variable values (may include query text or document content) | Show a friendly generic error message in the UI; send the scrubbed report silently in the background. |

---

## Detailed Specifications

### 1. Consent UX Specification

**First-run dialog behavior:**
- Appears exactly once, on first launch after install (or after a fresh config)
- Modal (blocks app launch until dismissed — no "skip for now" that defers indefinitely)
- Default: "No thanks" / decline is the visually equal option; the "Yes" button is not pre-selected or more prominent
- Two explicit action buttons only: "Yes, help improve the app" / "No thanks" (both EN/HE via tr())
- No checkbox pre-ticked. No "I agree to Terms" pattern.

**Dialog body must contain (both languages):**

```
Help improve Dicta Genizah Search Pro

We'd like to collect anonymous usage data to understand
which features are most useful and which platforms to support.

What IS collected:
  - Which tabs and search modes you use (counts only)
  - How long searches take (time ranges, not exact times)
  - App version, OS version, UI language
  - Crash reports (error type and location, no content)

What is NOT collected:
  - Your search queries or any search content
  - My Library filenames, folder paths, or document content
  - Your identity, account, or IP address
  - Any manuscript shelfmarks you look up

You can change this anytime in Settings / About.
```

**Settings/About toggle:**
- A single checkbox / toggle: "Send anonymous usage data" (bilingual)
- Changing it takes effect immediately (checked at the call site, not just at startup)
- When the user turns it OFF: the UUID file is deleted; subsequent enqueue_event() calls are no-ops

**What mature privacy-respecting apps do (reference patterns):**

VSCode defaults to opt-out and has faced years of GDPR community friction — this is the
anti-pattern. Zotero only transmits site-translator error reports (not usage) and gates them
on a user-visible "Report errors" setting. The best scholarly tool pattern (used by tools like
OpenRefine, JOSM the OSM editor) is: opt-in dialog at first run, clear disclosure, toggle
in settings, open-source code so users can audit. The app already has all of these traits
except the dialog.

### 2. Event Taxonomy

All events go through `shared/posthog_server.py::enqueue_event()` with:
- `distinct_id`: the per-install UUID (checked from config; assert it exists before calling)
- `properties`: always includes `$process_person_profile: False` + base properties below

**Base properties on every event (set once, included everywhere):**
```python
BASE_PROPS = {
    '$process_person_profile': False,   # anonymous tier, no person profile
    'app_version': VERSION,             # e.g. '8.1.0'
    'os': platform.system(),            # 'Windows' / 'Linux' / 'Darwin'
    'os_version': platform.version(),   # e.g. '10.0.26200' — safe, no PII
    'ui_lang': current_language(),      # 'he' or 'en'
    'install_id': _get_install_uuid(),  # the per-install UUID (also distinct_id)
}
```

Note on `os_version`: `platform.version()` on Windows returns a string like `10.0.26200`.
This is safe — it contains no PII. Do NOT include username, hostname, or
the result of `os.environ.get('USERNAME')`.

**Event catalog:**

| Event name | When emitted | Key properties |
|------------|-------------|----------------|
| `desktop_session_start` | App launch, after consent check passes | `app_version`, `os`, `os_version`, `ui_lang` |
| `desktop_tab_activated` | User switches to a tab | `tab_name` (enum: `search`, `browse`, `parallels`, `composition`, `my_library`, `joins_lab`, `puzzle`, `reading_desk`, `lists`) |
| `desktop_search_executed` | Search completes (SearchThread finishes) | `search_mode` (enum: `keyword`, `responsa`, `composition`, `parallels`), `corpus` (enum: `genizah`, `local`, `all`), `result_count_bucket`, `was_cancelled` (bool), `has_filters` (bool) |
| `desktop_responsa_options` | Responsa search executed | `expansion_enabled` (bool), `fuzzy_enabled` (bool), `judeo_arabic_enabled` (bool), `spacing_enabled` (bool), `option_count` (int 0-4) |
| `desktop_result_opened` | User opens a result in detail view (ResultDialog) | (no properties beyond BASE_PROPS — count only; no shelfmark/sys_id) |
| `desktop_browse_opened` | User opens Browse tab for a manuscript | (no properties beyond BASE_PROPS — count only) |
| `desktop_joins_lab_action` | User performs a Joins Lab action | `action` (enum: `anchor_set`, `search_run`, `candidate_compared`, `join_added`, `add_to_puzzle`) |
| `desktop_puzzle_action` | User performs a Puzzle action | `action` (enum: `opened`, `fragment_added`, `exported`, `published`) |
| `desktop_my_library_action` | User performs a My Library action | `action` (enum: `folder_added`, `folder_removed`, `reindex_all`, `search_executed`) |
| `desktop_export` | User exports results | `format` (enum: `xlsx`, `csv`, `txt`, `docx`), `row_count_bucket` |
| `desktop_session_performance_summary` | App close OR every 30 minutes | See Performance Metrics section |
| `desktop_crash` | Unhandled exception via sys.excepthook | `error_type`, `error_module`, `error_line`, `error_fingerprint`, `is_background_thread` (bool) |
| `desktop_error` | Caught non-fatal error at instrumented sites | `error_type`, `stage` (enum: `indexing`, `search`, `nli_fetch`, `export`, `puzzle`), `error_module` |

**Property name allowlist for the static guard test:**

Safe properties (allowed in telemetry calls): `app_version`, `os`, `os_version`, `ui_lang`,
`install_id`, `$process_person_profile`, `tab_name`, `search_mode`, `corpus`,
`result_count_bucket`, `duration_bucket`, `was_cancelled`, `has_filters`,
`expansion_enabled`, `fuzzy_enabled`, `judeo_arabic_enabled`, `spacing_enabled`,
`option_count`, `action`, `format`, `row_count_bucket`, `search_count`,
`median_duration_bucket`, `p95_duration_bucket`, `error_type`, `error_module`,
`error_line`, `error_fingerprint`, `is_background_thread`, `stage`,
`session_duration_seconds`, `mode_counts`.

Forbidden properties (blocked by AST guard): `query`, `text`, `content`, `path`,
`filename`, `shelfmark`, `sys_id`, `fl_id`, `email`, `user_id`, `username`,
`supabase_id`, `jwt`, `token`, `clean_query`, `query_text`.

### 3. Performance Metrics — Volume Strategy

**The problem:** 50 searches/day x 30 users = 1,500 raw search-performance events/day.
With multiple properties per event, these events accumulate quickly and are the noisiest
part of the stream. The web API uses `SEARCH_API_POSTHOG_SAMPLE_N` as a sampling valve.
The desktop needs a different approach because the user count is low (dozens, not thousands)
and total daily event volume is already low, but the per-user density is high.

**Recommended approach: session-summary aggregation**

Rather than emitting one performance event per search, accumulate in-memory counters for
the session and emit a single `desktop_session_performance_summary` event at app close
(or every 30 minutes for long-running sessions). This reduces search-performance events
from ~1,500/day to ~30-90/day (one per session per user).

```python
# In-memory accumulator (reset at session start), protected by a threading.Lock
_perf = {
    'search_count': 0,
    'search_durations': [],           # collect raw seconds, bucket at emit time
    'mode_counts': defaultdict(int),  # keyword/responsa/composition/parallels
}

# At app close or 30-min flush:
def _emit_performance_summary():
    durations = sorted(_perf['search_durations'])
    if not durations:
        return
    n = len(durations)
    median = durations[n // 2]
    p95 = durations[int(n * 0.95)] if n >= 20 else durations[-1]
    enqueue_event('desktop_session_performance_summary', {
        **BASE_PROPS,
        'search_count': _perf['search_count'],
        'median_duration_bucket': latency_bucket(median),
        'p95_duration_bucket': latency_bucket(p95),
        'mode_counts': dict(_perf['mode_counts']),  # {'keyword': 3, 'responsa': 47}
        'session_duration_seconds': int(time.monotonic() - _session_start),
        '$process_person_profile': False,
    })
```

**Bucket helpers to reuse from `web/api_hardening.py`:**
- `latency_bucket(seconds)` — `'lt_100ms' | 'lt_500ms' | 'lt_2s' | 'lt_10s' | 'gte_10s'`
- `result_count_bucket(n)` — `'zero' | 'count_1_10' | 'count_11_50' | ... | 'count_1001_plus'`

Both are already exported in `api_hardening.__all__` so the desktop can import them from there.

**Per-search events that ARE still emitted immediately (not deferred to summary):**

`desktop_search_executed` is still emitted per-search because it carries `was_cancelled`
and `has_filters` which are meaningful per-event. BUT: omit raw `duration_seconds` from it
(duration data goes only into the performance summary accumulator, bucketed at flush time).
`result_count_bucket` per-search is kept (it answers "what fraction of Responsa searches
return zero results?" which needs per-search granularity).

Final volume split for 50 searches/day x 30 users:
- `desktop_search_executed`: ~1,500/day (mode, corpus, result count, filters, no duration)
- `desktop_session_performance_summary`: ~60-90/day (all duration/performance data)
- All other events: ~200-400/day (tabs, actions, crash, errors)
- Total: ~1,800-2,000/day vs PostHog EU free tier of ~33,000/day (1M/month). Comfortable.

**My Library indexing duration:** Indexing is a background task that can run for minutes or
hours. Emit a single `desktop_my_library_action` with `action: 'reindex_all'` + an optional
`duration_bucket` when the indexing job completes. Do NOT emit per-file events.

### 4. Crash and Error Reporting

**Hard-crash capture (unhandled exceptions):**

```python
import sys, threading, re, os

def _scrub_path(s: str) -> str:
    """Replace Windows absolute paths with basename only."""
    return re.sub(r'[A-Za-z]:\\(?:[^\n"\\]+\\)*([^\n"\\]+)', r'<path>/\1', s)

def _make_crash_props(exc_type, exc_tb, is_background: bool) -> dict:
    error_module = 'unknown'
    error_line = 0
    if exc_tb:
        frame = exc_tb
        while frame.tb_next:
            frame = frame.tb_next
        error_module = os.path.basename(frame.tb_frame.f_code.co_filename)
        error_line = frame.tb_lineno
    return {
        **BASE_PROPS,
        'error_type': exc_type.__name__,
        'error_module': error_module,      # basename only, e.g. 'gui_threads.py'
        'error_line': error_line,          # integer
        'error_fingerprint': f'{exc_type.__name__}:{error_module}:{error_line}',
        'is_background_thread': is_background,
        '$process_person_profile': False,
    }

def _desktop_excepthook(exc_type, exc_value, exc_tb):
    if _telemetry_enabled():
        enqueue_event('desktop_crash', _make_crash_props(exc_type, exc_tb, False))
    sys.__excepthook__(exc_type, exc_value, exc_tb)

sys.excepthook = _desktop_excepthook

def _thread_excepthook(args):
    if _telemetry_enabled():
        enqueue_event('desktop_crash', _make_crash_props(
            args.exc_type, args.exc_traceback, True))

threading.excepthook = _thread_excepthook
```

**What a scrubbed crash report contains:**
- `error_type`: Python exception class name (`TypeError`, `AttributeError`, `KeyError`, etc.)
- `error_module`: basename of the innermost frame's file (`gui_threads.py`, not the full path)
- `error_line`: integer line number only
- `error_fingerprint`: `{ExcType}:{module}:{line}` — deterministic dedup key in PostHog
- BASE_PROPS: `app_version`, `os`, `os_version`, `ui_lang`

**What a scrubbed crash report does NOT contain:**
- Any OS path strings (basename only)
- Frame local variables (never included — they can contain query strings, file handles, document content)
- The exception message string. Exception messages commonly contain file paths
  (`FileNotFoundError: [Errno 2] No such file or directory: 'C:\Users\hillel\manuscript.pdf'`)
  or query content (`ValueError` with repr of bad input). Type name only is safe.
- The full traceback text (only the innermost frame's location, not the call chain)

**Exception message is NOT safe.** Do not include `str(exc_value)` in the crash payload.
`exc_type.__name__` only.

**Handled / non-fatal errors — instrumented sites:**

Instrument the following existing except-blocks (HIGH value, currently invisible):
- `LocalIndexerWorker` file extraction failures — `stage: 'indexing'`
- `SearchThread.run()` search engine errors — `stage: 'search'`
- NLI fetch paths in `genizah_core.py` — `stage: 'nli_fetch'`
- Export functions on format-specific failures — `stage: 'export'`

Do NOT instrument every except block. Only places where a failure is invisible to the user
but important to see in aggregate. Each site passes `error_type: type(e).__name__` only.

---

## Feature Dependencies

```
Consent dialog + UUID minting
    └──required by──> All telemetry emission (gate checked before every enqueue_event())
                          └──required by──> Usage events (tab, search, actions)
                          └──required by──> Performance summary
                          └──required by──> Crash reporting

BASE_PROPS (version, OS, lang)
    └──required by──> Every event (included in every properties dict)

latency_bucket() + result_count_bucket() from web/api_hardening.py
    └──required by──> desktop_search_executed (result_count_bucket)
    └──required by──> desktop_session_performance_summary (latency_bucket)

Settings/About toggle
    └──extends──> Consent dialog (change-anytime affordance; same flag)

Static AST guard (forbidden property names)
    └──enforces──> Privacy hard rule across all current and future events

sys.excepthook scrubber
    └──required by──> desktop_crash
    └──required by──> desktop_error (same _make_crash_props helper)

In-memory perf accumulator (threading.Lock-protected)
    └──required by──> desktop_session_performance_summary
    └──fed by──> desktop_search_executed (duration measured in SearchThread)
```

### Dependency Notes

- **Consent dialog required before any event.** The UUID is only minted on opt-in, and
  `enqueue_event()` must be wrapped in a `_telemetry_enabled()` guard at every call site.
  All other features depend on this gate working correctly.
- **BASE_PROPS requires `version.py` + `platform` stdlib.** Both are already available in
  the desktop app. No new dependencies.
- **Session performance summary requires a thread-safe in-memory accumulator.** SearchThread
  runs in a background thread and increments counters there; the accumulator needs a
  `threading.Lock`. The flush can happen on the main thread at app close.
- **Crash hook must fire regardless of telemetry state**, but `enqueue_event()` inside it
  must still check `_telemetry_enabled()`. Even in opted-out state, `sys.__excepthook__()`
  must be called for normal crash behavior.
- **`latency_bucket()` and `result_count_bucket()` can be imported from `web/api_hardening.py`**
  directly from the desktop — they have no NiceGUI dependencies and are pure functions.

---

## MVP Definition

### Launch With (v8.1.0)

Minimum viable feature set to make the telemetry useful and trustworthy from day one.

- [ ] Consent dialog (first-run, bilingual, modal, opt-in default OFF) — gate for everything
- [ ] UUID generation and storage in app config dir (only on consent; deleted on opt-out)
- [ ] Settings/About toggle (change anytime; takes effect immediately)
- [ ] `_telemetry_enabled()` guard checked before every `enqueue_event()` call
- [ ] BASE_PROPS helper (version + OS + lang, `$process_person_profile: False`)
- [ ] `desktop_session_start` event (version adoption + OS distribution)
- [ ] `desktop_tab_activated` event (which features are used)
- [ ] `desktop_search_executed` event (mode + corpus + result_count_bucket + was_cancelled + has_filters; no content)
- [ ] In-memory perf accumulator + `desktop_session_performance_summary` at app close
- [ ] `desktop_crash` via `sys.excepthook` + `threading.excepthook` with scrubbed props
- [ ] Static AST guard test (forbidden property names; CI-enforced, like test_no_raw_storage_access.py)
- [ ] Privacy disclosure text (bilingual, in consent dialog and in Settings/About)

### Add After Validation (v8.1.x)

Features to add once core telemetry is working and PostHog dashboards are built.

- [ ] `desktop_responsa_options` event (Responsa sub-option usage — needs dashboard to be useful)
- [ ] `desktop_joins_lab_action` (Join Lab adoption — helps decide Component B priority)
- [ ] `desktop_error` at high-value handled-error sites (identify sites from early crash data)
- [ ] `desktop_export` (format breakdown)

### Future Consideration (v2+)

- [ ] Aggregate session summaries across users into a PostHog dashboard insight — operational,
  not a code change
- [ ] Re-ask consent dialog if data categories materially expand (e.g., adding composition
  search performance not in the original disclosure)

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Consent dialog (bilingual, opt-in OFF) | HIGH — trust foundation | LOW | P1 |
| UUID + persistence + opt-out deletion | HIGH — anonymity guarantee | LOW | P1 |
| Settings/About toggle | HIGH — revocability | LOW | P1 |
| BASE_PROPS + `$process_person_profile: false` | HIGH — PostHog cost + anonymity | LOW | P1 |
| `desktop_session_start` | HIGH — version adoption dashboard | LOW | P1 |
| `desktop_tab_activated` | HIGH — feature usage distribution | LOW | P1 |
| `desktop_search_executed` (mode + corpus + result bucket, no content) | HIGH — core usage metric | LOW | P1 |
| `desktop_session_performance_summary` (aggregated) | HIGH — performance visibility without volume | MEDIUM | P1 |
| `desktop_crash` via excepthook (scrubbed) | HIGH — crash visibility | MEDIUM | P1 |
| Static AST guard test | HIGH — prevents future content leaks | LOW | P1 |
| Privacy disclosure text | HIGH — legal + trust | LOW | P1 |
| `desktop_responsa_options` | MEDIUM — Responsa tuning insight | LOW | P2 |
| `desktop_joins_lab_action` | MEDIUM — new feature adoption signal | LOW | P2 |
| `desktop_error` (handled errors) | MEDIUM — silent failure visibility | MEDIUM | P2 |
| `desktop_export` | LOW — nice to have | LOW | P3 |
| `desktop_puzzle_action` | LOW — puzzle is secondary feature | LOW | P3 |

---

## Reference Analysis

This is not a competitive market (Cairo Genizah research tools are niche), so "competitor"
means comparable scholarly/developer desktop tools with telemetry.

| Practice | VSCode | Zotero | OpenRefine | Our Approach |
|----------|--------|--------|------------|-------------|
| Default | Opt-out (sends before consent) | Error reports opt-out; no usage telemetry | No telemetry | Opt-in, OFF by default |
| First-run dialog | No (just a settings note) | No | N/A | Yes, modal, bilingual |
| What is collected | Usage, errors, performance | Translator error reports only | Nothing | Usage (modes/tabs), performance (aggregated), crashes (scrubbed) |
| Query / content | No query text | No content | N/A | No query text, no content (hard rule + AST guard) |
| PII scrubbing | Basic (no file paths in crash) | N/A | N/A | Explicit: basename-only frames, no locals, no exception message string |
| Revocability | Settings toggle | Preferences toggle | N/A | Settings/About toggle + UUID deletion |
| EU data residency | Azure (EU regions configurable) | Zotero servers | N/A | EU PostHog (already configured) |

---

## Sources

- `shared/posthog_server.py` — existing fire-and-forget emission infrastructure
- `web/api_hardening.py` — existing `latency_bucket`, `result_count_bucket`, `capture_api_event` patterns
- `web/pages/search.py:4439` — web `search_executed` event (note: sends `query` text; desktop must NOT copy this)
- `.planning/PROJECT.md` — v8.1.0 milestone target features and fixed constraints
- [PostHog anonymous vs identified events](https://posthog.com/docs/data/anonymous-vs-identified-events) — `$process_person_profile: false` semantics and anonymous-tier cost
- [PostHog person properties](https://posthog.com/docs/product-analytics/person-properties) — anonymous tier ingestion
- [VSCode telemetry docs](https://code.visualstudio.com/docs/configure/telemetry) — reference for opt-out anti-pattern
- [Zotero privacy policy](https://www.zotero.org/support/privacy) — scholarly tool minimal-telemetry pattern
- [Sentry Python options](https://docs.sentry.io/platforms/python/configuration/options/) — `before_send` + scrubbing hooks reference
- [GDPR telemetry guidance — activeMind.legal](https://www.activemind.legal/guides/telemetry-data/) — consent before first collection requirement
- Archon project telemetry issues — UUID-per-install, `$process_person_profile: false`, config-dir storage pattern

---
*Feature research for: opt-in privacy-first desktop telemetry (v8.1.0)*
*Researched: 2026-06-13*
