# Research search resource policy

Web transcription, lab, and parallels searches run in disposable subprocesses.
Interactive searches and the background API have no elapsed-time cutoff inside
those workers. A slow valid query may finish as long as it stays within resource
limits. The ordinary synchronous API retains its existing HTTP deadlines.

The web process owns a FIFO queue. Stop removes a queued request or kills its
running process; its slot is released after process exit. A crashed worker does
not take down the server. Workers also exit when their parent server disappears.
Waiting uses a separate thread pool so it does not occupy the browsing pool.

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
Each query starts a fresh worker and reloads engine metadata, adding startup
latency in exchange for releasing its memory and native threads after completion.

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
