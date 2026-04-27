---
status: diagnosed
trigger: "Investigate 'bad parameter or other API misuse' SQLite errors across multiple services in a NiceGUI web app"
created: 2026-03-29T00:00:00Z
updated: 2026-03-29T00:00:00Z
---

## Current Focus

hypothesis: Multiple threads concurrently execute queries on a single shared sqlite3.Connection object. While check_same_thread=False allows this, Python's sqlite3 module does NOT make concurrent cursor operations on the same connection thread-safe. When two run.io_bound() calls run in parallel (via asyncio.gather), they create and iterate cursors on the same connection simultaneously, causing "bad parameter or other API misuse" errors.
test: Examine code for concurrent access patterns — asyncio.gather + run.io_bound on same singleton service
expecting: Finding parallel calls to same service from different thread-pool threads
next_action: Confirm hypothesis and propose fix

## Symptoms

expected: SQLite queries should succeed for all sys_id lookups
actual: Intermittent "bad parameter or other API misuse" errors across multiple services (FjmsService, NliCrossrefService, PgpService)
errors: "bad parameter or other API misuse" — generic SQLite3 error code
reproduction: Happens under production load over hours (7h uptime, 6.3G memory, 4.5h CPU)
started: Errors scattered across 7h of runtime; app is a NiceGUI web server handling concurrent web requests

## Eliminated

(none yet)

## Evidence

- timestamp: 2026-03-29T00:01:00Z
  checked: Connection pattern in all three services
  found: All three services (FjmsService, NliCrossrefService, PgpService) use identical pattern — single sqlite3.Connection stored as self._conn, opened once in __init__ with check_same_thread=False, exposed as module-level singleton via get_*_service() functions. No per-query locking on the connection itself (FjmsService has locks only for cache population, not for general queries).
  implication: A single connection is shared across all threads in the NiceGUI thread pool.

- timestamp: 2026-03-29T00:02:00Z
  checked: How services are called from web pages
  found: search.py uses asyncio.gather() with multiple run.io_bound() calls that hit the SAME service connection simultaneously. Example: `asyncio.gather(run.io_bound(collect_fjms_enrichment, ids), run.io_bound(get_sys_ids_with_transcriptions, ids), ...)`. browse.py does the same: `asyncio.gather(fetch_pgp(), fetch_fjms(), fetch_crossref())` where each calls run.io_bound internally. Each run.io_bound dispatches to a thread pool thread.
  implication: Multiple thread-pool threads execute cursor.execute() + cursor.fetchall()/iteration on the same sqlite3.Connection concurrently. This is the classic cause of "bad parameter or other API misuse".

- timestamp: 2026-03-29T00:03:00Z
  checked: Python sqlite3 thread safety documentation
  found: check_same_thread=False only disables Python's thread-identity check. It does NOT make concurrent operations on the same connection safe. Python's sqlite3 docs: "If you want to use the same connection from multiple threads, you'll have to serialize the access yourself." SQLite itself supports serialized mode at the C level, but Python's sqlite3 module wraps it in ways that make concurrent cursor operations on the same connection unsafe.
  implication: The root cause is confirmed — concurrent cursor operations from multiple threads on a single shared connection.

- timestamp: 2026-03-29T00:04:00Z
  checked: Why it's intermittent
  found: The error only manifests when two thread-pool tasks happen to execute SQLite operations on the same connection at overlapping moments. Under light load, operations complete fast enough to rarely overlap. Under production load with multiple concurrent users, overlap probability increases over time.
  implication: Explains why errors are scattered across 7h and affect different services — any concurrent access to any shared connection can trigger it.

## Resolution

root_cause: All three SQLite service singletons (FjmsService, NliCrossrefService, PgpService) share a single sqlite3.Connection across all NiceGUI thread-pool threads. When asyncio.gather() dispatches multiple run.io_bound() calls that query the same DB, concurrent cursor operations on the shared connection cause SQLite "bad parameter or other API misuse" errors. check_same_thread=False only disables Python's thread check — it does not serialize access.

fix: Add a threading.Lock to each service that serializes all database operations on the shared connection. Alternative: use per-thread connections via threading.local(). The lock approach is simpler and sufficient for read-only workloads.

verification: (pending)
files_changed: []
