# Research search resource policy

Web transcription, lab, and parallels searches run in disposable subprocesses.
Every job a worker runs -- an interactive search, a completion, a replay, an API
background job -- stops after `Config.WEB_SEARCH_TIME_LIMIT` (180 s; owner ruling
2026-09-28) and returns what it had checked: the worker's progress callback raises
the engine's Stop, the engine returns its verified rows with the cut-off signal
`interrupted`, the page shows the count as "N+" with "stopped after 3 minutes",
and the API adds `results_cut_off` (on `/api/search`, and on `/api/parallels` when
the composition search comes back `partial`). The page counts the limit itself as
well, so a list the limit cut short shows "N+" even from an engine that did not say
it was stopped. Every engine does: the text search, the Lab search and Lab
composition search (fast and deep scan, My Library included) and the composition
search note `interrupted` when a stop ends them. A part of a search that cannot
return partial rows reports the limit instead (the page's time-limit message). A
worker that has not stopped 60 s after its limit (`TIME_LIMIT_GRACE_SECONDS`, stuck
outside the engine's checks) is killed; the rows a text search had already shown
are kept, marked "N+" and partial, as Stop keeps them. The limit counts from when
the job leaves the queue. The steps of a refinement chain run again together
(restored after a reload, re-evaluated, or completed before Search within) share one
limit (`web/research_jobs.py::shared_time_limit`): each step gets what the steps
before it left, and a step started after it is spent stops at once, cut off. The ordinary synchronous API keeps its own HTTP
deadlines; the desktop has none.

The letter-level (passage) search is checked through `checkpoint`, not its progress
callback (the desktop passes one that must not be called during a letter-level
scan): between witnesses, and before each candidate is verified -- where a long
query spends its time. Stopped, it returns the witnesses searched so far, the last
one's matches from the candidates it had verified, marked `partial`. Gathering one
witness's candidates and rendering the rows found have no checkpoint; both are
bounded (the posting budget, `verify_cap`), and the kill after the grace period
remains the fallback for them.

The web process owns a FIFO queue. Stop removes a queued request or kills its
running process; its slot is released after process exit. A crashed worker does
not take down the server. Workers also exit when their parent server disappears.
Waiting uses a separate thread pool so it does not occupy the browsing pool.
Each job carries its own variant and Lab settings: the website defaults
(`web/variant_preferences.py::WEBSITE_DEFAULTS`) with what its search sent on top
(the visitor's level, Num Changes and Settings-page preferences; an API job sends
nothing, so it runs with the defaults). A search never changes the server's
settings, and later UI changes do not alter jobs that are already queued.

Workers hold shared leases for the LOCAL index directories. My Library atomic
rebuild/reset operations hold exclusive leases through handle closure, directory
swap, and reload. Maintenance waits for existing readers; workers starting during
a swap wait until it finishes and remain cancellable by their supervisor. Leases
use dedicated SQLite lock files beside the index directories, so they survive
directory swaps and their OS locks are released if a process exits unexpectedly.

Workers use low scheduling priority, one native compute thread, and (when CPU
affinity is available) one CPU excluding the first allowed CPU. Before importing
engines they install an OS allocation limit. The parent also checks resident
memory and available system/container memory every 100 ms. This reduces resource
competition; it is not a guarantee against all host overload or storage contention.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `GENIZAH_RESEARCH_WORKERS` | 1 | Concurrent computations per web process (maximum 4) |
| `GENIZAH_RESEARCH_QUEUE_SIZE` | 16 | Waiting computations (maximum 64) |
| `GENIZAH_RESEARCH_MEMORY_MB` | 4096 | Maximum worker allocation and resident-memory allowance |
| `GENIZAH_WEB_RESERVE_MB` | 1024 | Free memory reserved for website/host activity |
| `GENIZAH_RESEARCH_RESULT_MB` | 512 | Maximum compressed worker transfer (maximum 512) |
| `GENIZAH_RESEARCH_PRESTART` | 1 | `0` = start each worker only when its search arrives (see below) |

A worker waits when available memory is too low to start. Its allocation limit
is reduced at launch if the configured allowance would consume the reserve.
Memory pressure during a run may stop it with an explicit error. A full queue
also returns an explicit error. Neither case is reported as a successful empty
search. Existing expansion, candidate, and result caps still apply; this change
does not promise exhaustive results beyond the engines' existing limits.
Transfers use streaming compression, with expanded serialized size bounded by
the worker's allocation allowance. After the child exits, the server waits for
free memory covering its reserve plus twice that expanded size before loading
results. This wait remains cancellable. The size estimate is conservative for
the current result structures; it is not an OS allocation limit on the web process.

These allowances are **per web process**. Use one serving process for this queue
design. Multiple server processes multiply capacity and do not share API job
records; supporting that deployment requires an external queue/result store.
Start with one worker and measure real broad queries, peak worker memory, queue
wait, and concurrent browse response times before increasing concurrency.

With one worker, every visitor's text search, Parallels search and API search
waits in the same FIFO queue: a search starts only when the one before it ends.
`GENIZAH_RESEARCH_WORKERS=2` (in the server's `.env`, then restart) runs two at
once. Workers are pinned to one CPU each, never the first: two get separate CPUs
only with at least three (`nproc`); on two CPUs they share one. Each running
search may hold up to `GENIZAH_RESEARCH_MEMORY_MB` (the allowance is divided
among the slots at launch), and each slot keeps a warm worker (below).

### Warm workers (2026-10-05)

Each query still runs in a fresh worker, released with its memory and native
threads when it ends. But a worker spent ~6 s (measured in a 4-CPU dev container)
loading the catalogue (`libraries.csv`, the CUDL alias index, the Oxford parts)
before it could search, on every query -- the likely gap between 19 s on the
website and 13 s on the desktop for the same search (owner, 2026-10-05). Now each slot starts its next worker as soon as the previous one ends
(and at server start): it loads the catalogue and waits for its input, which the
parent hands over by renaming `input.pkl` into its directory. The index itself is
opened per search, under the index lease. Measured on a synthetic 60,000-page
index, `אם אין אני לי`: first rows 7.1 s -> 1.1 s, complete 8.4-8.8 s -> 2.6 s.

The cost: an idle worker holds the loaded catalogue, ~400 MB resident, one per
slot. A slot does not start one when memory is below the admission threshold
(the reserve plus 128 MB per slot); a search then starts its own worker, as
before. A warm worker that died while waiting is replaced by a fresh one for the
search. `GENIZAH_RESEARCH_PRESTART=0` restores the old behaviour. A search
arriving while its warm worker is still loading waits only for the rest of the load.

### Early rows

A text search on the search page asks the worker for its first rows
(`execute_search(preview_callback=)`, the desktop's streaming): the worker writes
them to `preview.pkl` as they are found (at most 50 rows, each preview the start
of the final list), the parent reads it on its 100 ms tick, and the page shows
them under "Still searching" while the search runs. Offered where the engine
offers them (no NOT-words, not Responsa, Genizah scope -- the website has no My
Library) and only when no post-search filter (exclusions, library, printed, PGP,
domain, measurements, the all-terms view) could hide a row. Stop keeps the rows
already shown, marked partial. The API does not ask for them.

## Matching in supervised workers

Unbounded matching inside a disposable Linux worker uses Python's original `re`
matcher after arming `PR_SET_PDEATHSIG` with `SIGKILL` and checking that the parent
has not changed. This makes parent-death cleanup independent of the worker's
GIL. If that protection is unavailable or fails, the worker retains the
GIL-releasing matcher and its watchdog thread. The parent can terminate a live
worker even if native matching cannot check a cooperative deadline.
Matching in the web/desktop process, and explicit
nested budgets such as result highlighting, retain the interruptible `regex`
matcher. Selecting the matcher happens for each operation, so a pattern created
inside a worker does not disable deadlines when used outside that context.

A standalone, low-priority diagnostic on the production index on 2026-09-15
used `לא יתזוג עליהא`, variants, gap 0, and the saved 30-pair settings. It
retrieved 920 candidates containing 85.9 million characters. Loading and
stripping brackets took 1.14 seconds; matching took 28.32 seconds with the
deployed adapter and 3.60 seconds with the supervised-worker path. Every first
match span (including absent matches) was identical, with no diagnostic timeouts.
These are candidate-matching timings, not end-to-end API latency. The earlier
279-candidate, 514-second logged run could not be identified conclusively with
this request and must not be presented as this benchmark's baseline.

The search performance log retains `materialize_ms` and adds `doc_load_ms`
and `candidate_match_ms` for the initial candidate checks. Highlighting,
metadata formatting, and other per-hit work remain in `materialize_ms`.
Workers report preparing, checking candidate texts, and saving results through
the existing progress channel; polling does not start another computation.

## Background API

POST `/api/search/jobs` or `/api/parallels/jobs` with the same JSON body as the
corresponding synchronous endpoint. A 202 response supplies `job_id`, `status_url`,
and `result_url`. Poll the status URL; retrieve the result when the state is
`completed` or `failed`. Results preserve the endpoint's response body and status.
DELETE the status URL to cancel. Pending results return 409.

Mode gates and existing query validation/rate limits remain in force. Records
are bound to the client IP resolved by the existing trusted-proxy rules, and use
unguessable IDs. There are at most two active jobs per client IP and 32 retained
jobs per web process. Shared institutional IPs therefore share the active-job
allowance. Requests are limited to 64 KiB and API result files to 16 MiB. Finished
records expire after ten minutes; expired files are removed on subsequent job
requests. Jobs/results are temporary and do not survive a server restart. Clients
must use the same server process and client IP to retrieve them.

## Matching and rendering

Query matching uses the pinned `regex` dependency with GIL release. In isolated
workers it has no per-match timer. In-process callers retain the 10-second
`GENIZAH_REGEX_TIMEOUT_SECONDS` guard and optional
`GENIZAH_SEARCH_BUDGET_SECONDS` total deadline (default 0, meaning none).
Nested explicit deadlines cannot be extended. Optional viewer highlighting has
its own 0.25-second budget and may fall back to escaped plain text without
aborting the research search.

Compilation checks the active deadline before and after native compilation and
between Unicode initialization blocks. Completed blocks are cached, so later
renders retain progress if a short budget interrupts cold initialization. A
single native compilation remains non-interruptible in process; research-worker
isolation and resource limits still apply. Lab snippets fall back to plain text
if either compilation or matching exhausts their budget.

Windows uses [Job Object memory limits](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_extended_limit_information).
Linux uses [RLIMIT_DATA](https://man7.org/linux/man-pages/man2/getrlimit.2.html),
which includes anonymous mmap allocation on Linux 4.7 and newer, plus the parent
watchdog and cgroup-aware available-memory check. Read-only index mappings are
not charged as anonymous allocations but their resident pages are watched.

## Rollout and verification

Install updated requirements and restart the service to activate the workers.
Updating files alone does not replace an already-running search. Validate broad
Responsa and parallels queries on production-sized data while opening browse
pages; record both search completion and browse latency. Exercise Stop, queue
saturation, and memory pressure before raising allowances. Do not remove the
resource protections simply to make a benchmark pass.

Local tests cover actual subprocess cancellation, FIFO admission, worker crash
recovery, OS allocation rejection, memory watchdogs, event-loop responsiveness,
background API lifecycle, and existing search/API behavior. They do not replace
production-sized load testing or the Windows Python 3.11 render-smoke CI job.

The local corpus smoke check for plain Responsa `ראובן AND שמעון` processed
8,428 candidates and returned 4,471 results through the subprocess boundary.
Matching took about 35 seconds; the transfer was 276 MiB compressed / 758 MiB
expanded. Startup and transfer add time beyond matching. This is one local
measurement, not a production latency guarantee. It exposed why small fixed
memory/transfer allowances rejected a legitimate broad query.
