---
status: shipped
---

# SEED-015 — Desktop image loader: NLI circuit-breaker wiring + TLS host policy (MINIMAL)

> Source: 2026-06-23 product-quality audit, findings **#1** (HIGH, CONFIRMED) and **M2** (MED, NEW).
> Register: `.planning/audit-2026-06-23-product-quality/MASTER.md`.
> Decision gate (ANSWERED 2026-06-23 — do NOT reopen):
> *"MINIMAL NOW — wire desktop loader into the NLI breaker + short timeouts; defer the full 4-path
> unification. TLS: keep `verify=False` but restrict to known NLI/Rosetta hosts + suppress warnings
> explicitly + document (don't chase the cert chain now)."*
> → IN SCOPE: **#1** (breaker-wire desktop), **M2** (TLS host-restrict + document), small resilience hygiene.
> → **DEFERRED** to a later milestone: **#2** (4 divergent cache impls), **M1** (web-puzzle browser-side
> direct-NLI fallback), **M4** (unify failure taxonomy / `ImageFetchResult`).

## Problem (findings #1 + M2)

NLI's image servers (`iiif.nli.org.il`, `rosetta.nli.org.il`) periodically go slow/down. The **web** app
was hardened in **Phase 98** with a shared **circuit breaker** (`shared/nli_circuit_breaker.py`): after
`NLI_CIRCUIT_THRESHOLD` (3) consecutive failures it trips, making further NLI calls fail instantly for
`NLI_CIRCUIT_WINDOW` (60s) so the app doesn't hang. The **desktop** image fetch sites were never wired in:

- `desktop/image_loader.py::_download_bytes` — blanket `timeout = 30 if rosetta else 10`,
  `requests.get(..., verify=False)`, and a bare `except Exception` that never fed the breaker.
- `desktop/join_workbench.py` `ThumbBatchWorker.run` — a direct `requests.get(url, timeout=5, verify=False)`.

So during an NLI outage the desktop app waited the **full** read timeout on **every** image (10s, 30s for
Rosetta TIFFs), felt frozen, and its failures never tripped the shared breaker. **M2:** the blanket
`verify=False` also disabled TLS for *any* host on the same code path (Cambridge / Oxford / JTS images),
and the resulting `InsecureRequestWarning` was not host-scoped.

## Scope (precisely #1 + M2 — desktop)

1. **Shared host policy** — `shared/nli_fetch.py` (NEW, dependency-free so web + desktop share it without a
   cycle): `NLI_IMAGE_HOSTS = {iiif.nli.org.il, rosetta.nli.org.il}`, `is_nli_host(url)`,
   `nli_verify_for(url)` (verify=False ONLY for those hosts; True elsewhere), and `nli_image_get(...)` — a
   thin `requests.get` wrapper that applies the verify policy and suppresses `InsecureRequestWarning`
   **host-scoped** (inside a `warnings.catch_warnings()` block around the one NLI call; never a global
   filter). Exact-host match (NOT suffix) — a spoofed `iiif.nli.org.il.evil.com` stays `verify=True`.

2. **`desktop/image_loader.py::_download_bytes`** — copy the canonical breaker pattern from
   `genizah_core.py::fetch_iiif_manifest`:
   - Host-gate the breaker: only NLI hosts consult `is_open()` (fail fast) and feed `record_failure` /
     `record_success`. A Cambridge/Oxford failure must NOT trip the NLI breaker, and an NLI outage must NOT
     block a non-NLI image.
   - Replace the magic `10/30` with a `(connect, read)` tuple: connect = `NLI_CONNECT_TIMEOUT` (3s) so a
     dead host fails in ~3s; read = `NLI_IMAGE_READ_TIMEOUT` (5) for IIIF, a generous `30` for Rosetta
     full-res TIFFs, `10` for external libraries.
   - `200` → `record_success`; `429`/`5xx` → typed `record_failure`; `Timeout`/`ConnectionError` → typed
     `record_failure`; `404`/other-4xx → no breaker touch (D-07). Log non-200 responses (#36 observability).
   - Route the actual GET through `nli_image_get` (TLS policy + host-scoped warning suppression).

3. **`desktop/join_workbench.py` `ThumbBatchWorker.run`** — same treatment: host-gated breaker consult +
   feed, `(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)` tuple, `nli_image_get` for the TLS policy, breaker
   short-circuit when open. (`get_thumbnail` resolves to NLI IIIF URLs, but host-gating keeps it correct.)

## Out of scope (DEFERRED — note in the PR)

- **#2** — the 4 divergent image-loading implementations + incompatible cache keys.
- **M1** — the web-puzzle browser-side direct-NLI fallback that bypasses server policy.
- **M4** — unifying the image-fetch failure taxonomy into a shared `ImageFetchResult`.

These are a later milestone (HARD). `shared/nli_fetch.py` is deliberately tiny — do NOT grow it into the
unification.

## Tests (`tests/test_desktop_image_loader_breaker.py`)

- **Host policy:** `is_nli_host` / `nli_verify_for` for NLI vs external vs garbage vs spoofed-prefix host;
  `host_of` lowercases.
- **`nli_image_get`:** NLI host → `verify=False`; non-NLI → `verify=True`; the `InsecureRequestWarning`
  ignore filter is active **during** the NLI call and **restored afterwards** (host-scoped, not global);
  no suppression for non-NLI hosts.
- **`_download_bytes`** (built via `ImageLoaderThread.__new__` — pure method, no QThread/QApplication):
  breaker-open short-circuits **without** a network call; 200 → `record_success`; 5xx/429/timeout/
  connection-error → matching `record_failure`; 404 does NOT trip; **non-NLI failure does NOT trip** the
  breaker; non-NLI host is **not** short-circuited while the breaker is open; `(connect, read)` tuple has
  the short connect + generous Rosetta read; cancelled → None.
- **Source guards:** neither `desktop/image_loader.py` nor `desktop/join_workbench.py` retains a blanket
  `verify=False`; both reference `nli_image_get` + the breaker; join_workbench uses the
  `join_workbench_thumb` path. (The `ThumbBatchWorker.run` QThread is CI-skipped to construct; its logic
  mirrors `_download_bytes` through the same shared helper, so a source guard covers it.)

## Verification

22 targeted tests green (`GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest
tests/test_desktop_image_loader_breaker.py`); adjacent `test_join_workbench_*` + `test_nli_breaker_cross
_module_invariants` green; `test_join_workbench_construct` green; ruff clean on changed files. Codex
code-review of the diff, then PR → squash-merge when CI green. **Human UAT:** during a real NLI slowdown
the desktop Joins-Lab/Browse images should fail fast (≈3s, not 10-30s) and recover after the window —
flag for a manual desktop pass.

## Done-when

Desktop image fetches consult + feed the shared breaker, fail fast on dead NLI, and disable TLS verify only
for NLI hosts (documented, host-scoped warning suppression). `OPEN_ISSUES.md` updated; Codex-reviewed; CI
green; PR squash-merged. #2/M1/M4 remain deferred.
