# Phase 134: Discovery Data Spine - Pattern Map

**Mapped:** 2026-07-21
**Files analyzed:** 15 new/modified files (+ 10 new test files + 3 doc artifacts)
**Analogs found:** 13 / 15 direct code analogs (2 have shape-only precedent — the offline
distillation script and the doc-artifact exit files have no committed *code* twin, only
committed *doc/CLI* twins cited below)

> **Provenance note:** every code excerpt below is quoted from a COMMITTED repo file only
> (`web/`, `shared/`, `scripts/`, `tests/`). The gitignored `same_work_spike/probe/**` tree
> (research pipeline scripts, `fullcorpus_v2.db`, band/frame files) is referenced ONLY by
> path and role — never quoted — per the masking hard constraint. Restricted sources are
> named only as "M-source" / "R-source" throughout, matching the codebase convention.

> **Contract note:** the CONTEXT.md "CONTRACT CORRECTION (2026-07-21)" (C-1..C-9) SUPERSEDES
> the 6-table DDL sketch and the 4-band model in 134-RESEARCH.md. Wherever a pattern below
> touches schema shape, it is annotated against the corrected **two-table split**
> (`discovery_claim` + `discovery_evidence`, C-5) and the **orthogonal-columns** model (C-3),
> not the research doc's `work_witness_claims` / `ms_ms_claims` sketch. The *service/loader/
> flag/masking* patterns are unaffected by the correction — they map 1:1 regardless of schema.
>
> **REWORK CORRECTION (2026-07-21, ground-truth data investigation — F5/F6):** two analog notes
> below were CORRECTED after inspecting the gitignored research data + committed analogs.
> **F5:** `scripts/export_translation_audit_sample.py` and `web/components/translation_report.py`
> are EMISSION-ONLY / append-only analogs — NEITHER contains an "approved CSV re-consume" reader.
> The 134-04 `--from-approved` reader (+ the frozen CANDIDATE/APPROVED headers + rejection rules +
> round-trip) is genuinely NEW behavior, modeled on the audit-sample *writer* only. **F6:**
> `tests/test_no_back_edges_core.py` does NOT enforce a general `shared/`→`web/` ban — it only bans
> module-level `genizah_core` imports in a registry of shared modules. The 134-06
> `tests/test_no_back_edges_discovery.py` guard is a NEW essential test that bans `web`/`nicegui`/
> `fastapi` (via `ast.Import` + `ast.ImportFrom`, incl. guarded top-level imports), reusing only
> the core guard's scope-aware traversal SHAPE.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `scripts/build_discovery_sidecar.py` (NEW) | script (offline ETL/build) | batch | `scripts/build_atlas_asset.py` (bake shape + `--smoke`/`--golden` synthetic mode) + `scripts/export_translation_audit_sample.py` (DB-read → sample → CSV review-artifact EMISSION shape only — the approved-reader is NEW, F5) | role-match (shape only; the actual research-DB read pipeline is gitignored, see "No Analog Found") |
| `docs/specs/discovery-sidecar-schema-v1.md` (NEW) | config/spec doc | — | `docs/specs/atlas-asset-schema-v1.md` | exact (doc-genre twin) |
| `discovery-frames.md` (NEW, committed exit artifact) | doc | — | `scripts/export_translation_audit_sample.py::write_summary` (human-readable report-from-data pattern) | partial |
| `discovery-budgets.md` (NEW, committed exit artifact) | doc | — | same as above; numeric-caps convention also mirrors `PERF-01`-style budget docs referenced in RESEARCH.md | partial |
| `shared/discovery_service.py` (NEW) | service | request-response (async read) | `shared/fjms_service.py` (sidecar service pattern) + `web/search_api.py` (off-loop async + timeout + semaphore) | exact (composite of two exact analogs) |
| `shared/discovery_errors.py` (NEW, small — optional split) | utility (exception type) | — | `shared/api_errors.py` (neutral, web-free exception module) | exact |
| `web/discovery_assets.py` (NEW) | config/loader | file I/O (startup load) | `web/atlas_assets.py` | exact |
| `web/main.py` (EXTEND — call `load_discovery_state()`) | config (startup wiring) | file I/O | `web/main.py:757` (`load_atlas_state()` module-level call) | exact (same file, same idiom) |
| `web/feature_flags.py` (EXTEND — add `DISCOVERY_ENABLED`) | config | — | itself: `_env_enabled` + `ATLAS_PREVIEW_ENABLED` (lines 8-24) | exact |
| `scripts/check_atlas_masking.py` (EXTEND — new `scan_sqlite()` + `--scan-sqlite`) | script (CI guard) | batch/scan | itself: `scan_asset()` (1047-1146), `PatternMatcher`/`build_matcher()` (449-708), `parse_args()`/`main()` (1193-1256) | exact (same file, additive mode) |
| `MASKING_SCAN_PATTERNS_FILE` pattern file (gitignored — add R-source tokens) | config | — | `scripts/check_atlas_masking.py::load_patterns()` (170-189) | exact |
| `tests/test_discovery_loader.py` (NEW) | test | — | `tests/test_atlas_flag_gating.py` (Groups 3/3B: absent/corrupt/incompatible fail-closed) | exact |
| `tests/test_discovery_flag.py` (NEW) | test | — | `tests/test_atlas_flag_gating.py` (Group 2: flag-off clean-hide) | exact |
| `tests/test_discovery_release_contract.py` (NEW) | test | — | `tests/atlas_bake/test_atlas_bake.py` (`_enforce_regression_floor`, manifest/hash checks) | role-match |
| `tests/test_masking_sqlite.py` (NEW) | test | — | `tests/test_atlas_masking_scan.py` (fabricated-token, never-echo pattern) | exact |
| `tests/test_discovery_ids.py` (NEW, golden hash test) | test | — | `tests/atlas_bake/test_atlas_bake.py::test_determinism` + `::test_content_hash_changes` | exact |
| `tests/test_discovery_schema.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py` (NEW) | test | — | `tests/atlas_bake/test_atlas_bake.py` (golden-fixture invariant style) | role-match (new domain, same test *shape*) |
| `tests/test_no_back_edges_discovery.py` (NEW essential guard) | test | — | `tests/test_no_back_edges_core.py` (traversal SHAPE only — the core guard bans genizah_core; this NEW guard bans web/nicegui/fastapi, F6) | role-match (shape only; NEW target modules) |
| `tests/test_discovery_service.py` (NEW) | test | — | no direct async-service test precedent found in `tests/`; model on `web/search_api.py`'s own overload behavior + `test_atlas_flag_gating.py`'s monkeypatch-fixture style | partial |
| `tests/fixtures/discovery/discovery-v1-fixture.db` (NEW, deterministic masking-safe fixture) | test fixture | file I/O | `tests/fixtures/atlas/golden-v1.bin` (+builder `scripts/build_atlas_asset.py::synthetic_dataset`, `--golden` CLI mode) | exact |

## Pattern Assignments

### `scripts/build_discovery_sidecar.py` (script, batch/ETL)

**Analogs:** `scripts/build_atlas_asset.py` (bake shape, `--smoke`/`--golden` synthetic mode,
regression-floor gate) + `scripts/export_translation_audit_sample.py` (DB-read → transform →
review-CSV EMISSION shape — writer only, F5).

**Why not the gitignored probe scripts:** CONTEXT.md and RESEARCH.md both point at
`same_work_spike/probe/scripts/build_reuse_graph.py` / `build_atlas_draft.py` as the pipeline
to "fork." Those files are gitignored/off-repo research — they inform the *shape* (read
research DB → cluster/classify → emit product artifact) but contain no code this agent may
quote, and the new script must not import or `cat` them into the product tree. Treat them as
a design reference only; the concrete Python patterns below come from committed twins that do
the same job (bake a masked/versioned/deterministic asset from a research DB, with a synthetic
mode CI can run without the real DB).

**Synthetic/golden mode for CI (masking-safe fixture DB)** — `scripts/build_atlas_asset.py`
lines 260-297 (verified):
```python
# Synthetic dataset -- for --smoke / --golden. NEVER derived from real
# research data; purely fabricated sys_ids/shelfmarks/titles for pipeline
# validation (masking-safe by construction).
_SYNTHETIC_BASE_SYS_ID = 990_000_000_000_000_000
_SYNTHETIC_TITLES = [
    "Piyyut fragment for the Sabbath", "Bible commentary leaf", "Halakhic responsum",
    "Liturgical poem for festivals", "Talmudic gloss", "",
]
_SYNTHETIC_LIBS = ["CUL", "JTS", "RNL", "Oxford", "BL"]

def synthetic_dataset(n: int, seed: int = SEED, malicious: bool = False, n_island: Optional[int] = None):
    """Build a small, deterministic, fabricated ms_pairs/meta/domains dataset.
    ...
    Returns (ms_pairs, sys_meta, domains, sys_ids).
    """
```
And the CLI contract (line 29, 1279-1298): `python scripts/build_atlas_asset.py --golden
tests/fixtures/atlas/golden-v1.bin` — `db_path` is required UNLESS `--smoke N` or `--golden
PATH` is given. **Apply the same shape to `build_discovery_sidecar.py`**: a
`synthetic_discovery_dataset(n, seed)` fabricator (fake opaque `work_id`s, fake `page_id`s
shaped like real ones, fake bands/evidence — never derived from the real research DB) +
`--smoke`/`--golden` CLI flags so `tests/fixtures/discovery/discovery-v1-fixture.db` can be
regenerated and CI never touches the 3.1 GB `fullcorpus_v2.db`.

**Regression-floor / real-vs-synthetic gate** — `scripts/build_atlas_asset.py` lines 1198-1212:
```python
def _enforce_regression_floor(result: BakeResult, source_db_hash: str) -> None:
    """... a REAL research-DB bake (never --smoke/--golden, identified via
    the `source_db_hash` marker) must place at least REGRESSION_FLOOR nodes ...
    Synthetic --smoke/--golden bakes stay correctly exempt via the source-DB marker."""
    if (not source_db_hash.startswith(("smoke-", "golden-"))
            and result.placed_count < REGRESSION_FLOOR):
        raise ValueError(...)
```
Mirror this for discovery: a `source_db_hash`/`sidecar_version` marker distinguishes a real
distillation from a synthetic one, and a real distillation must clear a documented row-count
floor (ties into `discovery-frames.md`'s per-band deduped counts, D-11/C-7).

**Review-artifact EMISSION (D-08 neutral-title curation) — EMISSION ONLY (F5)** —
`scripts/export_translation_audit_sample.py` lines 56-110 (verified), the row-shape +
QC/status-column WRITER convention to copy:
```python
AUDIT_COLUMNS = [
    'dataset', 'record_id', 'field_name', 'direction',
    'source_text', 'translated_text', 'model_version', 'translated_at',
    'source_length', 'target_length', 'length_ratio',
    'qc_score', 'qc_flags', 'sample_reason',
    'review_status', 'review_notes',
]

def make_audit_row(dataset, record_id, field_name, direction, source_text, translated_text,
                    model_version='', translated_at='', sample_reason='random') -> dict:
    ...
    return {
        'dataset': dataset, 'record_id': str(record_id), ...,
        'review_status': '', 'review_notes': '',
    }
```
and the writer (lines 515-527):
```python
def write_csv(rows: List[dict], filename: str, columns: List[str] = None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename
    if not columns:
        columns = list(rows[0].keys()) if rows else []
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
```
**D-08 adaptation (CORRECTED, F5):** the audit-sample file is an EMISSION-ONLY analog — it WRITES
a review CSV but has **no approved-CSV re-consume reader**, and `web/components/translation_report.py`
is an append-only live-UI status variant (also NOT an approved-round-trip reader). So only the
CANDIDATE *writer* is modeled on these analogs; the `--from-approved` READER is **genuinely NEW
behavior** to build fresh. Freeze BOTH CSV schemas as distinct exact headers:
- CANDIDATE (emitted): `[work_id, candidate_neutral_title, author, genre, source_corpus, review_status, review_notes]` (source_corpus is the MASKED code — never the raw codename per D-08). Reuse the audit-sample `utf-8-sig` write + `OUTPUT_DIR.mkdir(parents=True, exist_ok=True)` idiom.
- APPROVED (owner-edited, re-consumed): `[work_id, neutral_title, author, genre, source_corpus, review_status]`.
- REJECTION RULES (fail-closed, NEW): the `--from-approved` reader reads ONLY rows with `review_status == 'approved'` AND a non-empty `neutral_title` AND a crosswalk-known `work_id` AND a valid `source_corpus` code; everything else is EXCLUDED (never a research-title fallback).
- ROUND-TRIP (NEW test): emit → (owner edits) → `--from-approved` reconstructs the approved works deterministically.

**Deterministic hashing (DATA-01/DATA-10 `claim_id`/`unit_id`/`evidence_id`)** — no committed
code analog exists (this is genuinely new); RESEARCH.md's recipe (SHA-256 over a frozen `|`-
delimited UTF-8 key, sorted-string member lists) is the correct shape to implement fresh, and
`tests/atlas_bake/test_atlas_bake.py::test_determinism` / `::test_content_hash_changes` (below)
is the exact test-shape analog to prove it.

---

### `shared/discovery_service.py` (service, async request-response)

**Analog 1 — sidecar open + graceful-absent + `meta` version table** —
`shared/fjms_service.py` lines 690-810 (verified):
```python
class FjmsService:
    def __init__(self, db_path: str = None, thread_safe: bool = True):
        self._conn = None  # ThreadLocalConnection or sqlite3.Connection
        ...
        if db_path is None:
            import os
            user_path = os.path.join(
                os.environ.get('LOCALAPPDATA', ''),
                'GenizahSearchPro', 'data', _SIDECAR_DIR, _SIDECAR_FILENAME
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)
        ...
        try:
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                self._conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row, timeout=10.0)
            else:
                self._conn = sqlite3.connect(uri, uri=True, check_same_thread=True, timeout=10.0)
                self._conn.row_factory = sqlite3.Row
            logger.info(f"FjmsService: Connected to {db_path}")
        except Exception as e:
            logger.error(f"FjmsService: Failed to connect to {db_path}: {e}")
            self._conn = None

    def is_available(self) -> bool:
        return self._conn is not None

    def get_version(self) -> Optional[str]:
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute("SELECT value FROM meta WHERE key = 'version'")
            row = cursor.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error(f"FjmsService.get_version error: {e}")
            return None
```
Every read method follows the same `if self._conn is None: return []` / `except Exception:
log + return empty` shape (verified repeated at lines 800, 823, 857, 1037, 1363, 1552, 1591,
1760, 1889, 2310, 2466, 2557, 2581, 2671, 2715, 2755, 2863, 2930, 2966, 3056, 3245, 3386 — a
project-wide invariant, not incidental). Reuse verbatim for `discovery.db`'s `meta` release
contract (`schema_version`/`sidecar_version`/`source_db_sha256`/etc., per DATA-08). **F15
caveat:** unlike FjmsService (which opens `_conn` in `__init__`), `DiscoveryService` must build
its `ThreadLocalConnection` LAZILY on first available call (never in `__init__` from
`path_provider()`, so importing `web/discovery.py` before the loader runs binds no stale path)
and RECREATE it when the resolved path/version changes.

**Analog 2 — a join-resolution read (shape for `discovery_evidence` reads, incl. DATA-10
witness units)** — `shared/fjms_service.py::get_join_group` lines 2651-2700 (verified) shows
the GROUP_CONCAT-aggregation-then-Python-split pattern for "other rows related to this key,"
directly reusable for `discovery_service.get_evidence_for_claim(page_id, work_id)` (C-5:
1-to-many `discovery_claim` → `discovery_evidence`) and for witness-unit member lookups
(DATA-10). `_split_concat` is the helper to reuse/mirror.

**Analog 3 — per-thread connection pool** — `shared/thread_local_db.py` (verified, full file,
150 lines): `ThreadLocalConnection(uri, row_factory=sqlite3.Row, timeout=10.0)` is the exact
primitive `discovery_service.py` must construct its `_conn` from (never a bare
`sqlite3.connect`); it already handles per-thread lazy connect + dead-thread pruning + a
`__bool__` truthy override so `if self._conn:` availability checks work.

**Analog 4 — off-event-loop query + timeout → overload (DATA-06)** —
`web/search_api.py::_acquire_heavy_slot` lines 379-458 (verified) — the non-blocking semaphore
pattern for bounded concurrency:
```python
class _HeavySemaphoreState:
    sem: asyncio.Semaphore = asyncio.Semaphore(DEFAULT_HEAVY_CONCURRENCY)
    _capacity: int = DEFAULT_HEAVY_CONCURRENCY
    @classmethod
    def reset(cls, capacity: int) -> None:
        cls.sem = asyncio.Semaphore(capacity)
        cls._capacity = capacity

async def _acquire_heavy_slot():
    ...
    if sem.locked():
        raise APIError('heavy_search_busy', 'heavy search concurrency limit reached; retry shortly',
                        http_status=503, headers={'Retry-After': '5'})
    await sem.acquire()
    def _release():
        sem.release()
    return _release
```
and the `asyncio.wait` (NEVER `wait_for`) off-loop dispatch — `web/search_api.py` lines
1104-1166 (verified):
```python
def _run_search_sync():
    res = state.searcher.execute_search(...) or []
    ...
    return res, _consume_last_responsa_downgrade(), _consume_meta_inner()

loop = asyncio.get_event_loop()
_heavy_release = None
if req.search_mode in HEAVY_SEARCH_MODES:
    _heavy_release = await _acquire_heavy_slot()
try:
    _search_fut = loop.run_in_executor(None, _run_search_sync)
    if _heavy_release is not None:
        _search_fut.add_done_callback(lambda _f, _r=_heavy_release: _r())
        _heavy_release = None
    _done, _pending = await asyncio.wait({_search_fut}, timeout=core_timeout)
    if _search_fut in _pending:
        logger.warning('search core_timeout after %ss ...', core_timeout, req.search_mode, timeout_env)
        raise APIError('core_timeout', f'search did not complete within {core_timeout}s ...', http_status=504)
    results, downgrade_msg, cascade_meta = _search_fut.result()
finally:
    if _heavy_release is not None:
        _heavy_release()
```
**Load-bearing comment to preserve verbatim in spirit** (`web/search_api.py` lines 1129-1140):
`run_in_executor` cannot cancel a running thread, so a timed-out search keeps occupying a
threadpool worker; releasing a concurrency slot in a plain `finally` would re-admit new heavy
work while the timed-out thread still runs — release the slot from the future's
`add_done_callback` instead, only when the thread truly finishes. This is EXACTLY the DATA-06
"never `asyncio.wait_for(run_in_executor(...))`" landmine from RESEARCH.md — copy the
`asyncio.wait` + done-callback release shape, not `wait_for`.

**Analog 5 — a neutral, web-free exception type** — `shared/api_errors.py` (verified, full
file, 92 lines): `APIError(code, message, http_status=400, headers=None)` deliberately lives
in `shared/` (not `web/api_hardening.py`) specifically so `shared/*_service.py` modules never
import from `web/`. **Model `DiscoveryUnavailable`/`DiscoveryOverload` the same
way** — a small exception class in `shared/` (e.g. a tiny `shared/discovery_errors.py`), never
importing FastAPI/Starlette/NiceGUI, so `web/pages/*` can catch it and render the "temporarily
unavailable" copy without `shared/discovery_service.py` depending on `web/`. **Back-edge GUARD
(CORRECTED, F6):** `tests/test_no_back_edges_core.py` is NOT a general `shared/`→`web/` ban — it
only bans module-level `genizah_core` imports for a REGISTRY of shared modules. So the discovery
layering guard `tests/test_no_back_edges_discovery.py` is a genuinely NEW essential test: it
reuses the core guard's scope-aware traversal SHAPE (descend import-time compound statements,
stop at function bodies) but targets `web`/`nicegui`/`fastapi` via BOTH `ast.Import` and
`ast.ImportFrom` (including top-level guarded `try:`/`if:` imports), over
`shared/discovery_service.py` + `shared/discovery_errors.py`.

**Library-filter-mode off-loop precedent (extra async-DB-read example)** —
`web/search_api.py` lines 342-364 (verified) shows the exact
`loop.run_in_executor(None, sync_fn, *args)` one-liner shape for a *cheap* (non-heavy) async
read that still must not block the loop — appropriate for `DiscoveryService`'s
browse-enrichment-scale reads (no semaphore needed there, just `run_in_executor` + a per-query
timeout via `asyncio.wait`).

---

### `web/discovery_assets.py` (config/loader, file I/O)

**Analog:** `web/atlas_assets.py` (verified, full file, 321 lines) — copy the module structure
wholesale, per RESEARCH.md's own Pattern 1 and the Phase 133 precedent this phase explicitly
reuses:
```python
@dataclass
class _AtlasState:
    ready: bool = False
    manifest_bytes: Optional[bytes] = None
    manifest: Optional[dict] = None
    bin_name: Optional[str] = None
    plain_bytes: Optional[bytes] = None
    br_bytes: Optional[bytes] = None
    etag: Optional[str] = None

_state = _AtlasState()
_lock = threading.Lock()

def load_atlas_state() -> bool:
    global _state
    new_state = _AtlasState(ready=False)
    try:
        ... # open manifest, validate content_hash, validate structure
        new_state = _AtlasState(ready=True, ...)
    except Exception as exc:  # fail-closed: never raise out of startup load
        logger.info("Atlas preview asset not loaded (fail-closed): %s", exc)
        new_state = _AtlasState(ready=False)
    with _lock:
        _state = new_state
    return _state.ready

def atlas_preview_available() -> bool:
    return bool(ATLAS_PREVIEW_ENABLED and _state.ready)
```
**Direct DATA-08 mapping:** `_DiscoveryState(ready, path, meta)` replaces `_AtlasState`;
`load_discovery_state()` replaces `load_atlas_state()`; validate (a) `PRAGMA integrity_check
== 'ok'`, (b) `meta.schema_version == _EXPECTED_SCHEMA_VERSION` (reject-incompatible, DATA-08),
(c) release-contract row counts vs `meta` expectations (DATA-08) — same try/except-wraps-
everything/fail-closed/lock-protected-swap shape, same "no per-request `os.path.exists`" model
(atlas doc lines 16-23: asset is loaded ONCE at startup, deploy posture is scp-then-restart).
`discovery_available()` mirrors `atlas_preview_available()` exactly: `bool(DISCOVERY_ENABLED
and _state.ready)`. **F14 note:** because `discovery_available()` ANDs the OFF-this-phase
`DISCOVERY_ENABLED` flag, the 134-08 benchmark must inject a benchmark-only readiness predicate
(`lambda: _state.ready`, flag bypassed WITHOUT changing it) so it measures real query work.

**Startup wiring point** — `web/main.py` line 757 (verified):
```python
load_atlas_state()
```
called at MODULE level (not inside an `@app.on_event('startup')` hook) immediately after the
import of `web.atlas_assets`. Call `load_discovery_state()` the same way, right after importing
`web.discovery_assets`, with a comment mirroring lines 754-756's rationale (fail-closed: a
missing/broken sidecar just leaves `discovery_available()` False and every discovery surface
hides cleanly).

---

### `web/feature_flags.py` (EXTEND — add `DISCOVERY_ENABLED`)

**Analog:** itself — `web/feature_flags.py` lines 1-24 (verified, full existing file):
```python
def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

WEB_PUZZLE_ENABLED = _env_enabled("WEB_PUZZLE_ENABLED", True)

# Phase 133 (ATLAS-01) ... Default OFF ...
# NOTE: this flag is necessary but NOT sufficient — the single authoritative
# predicate web/atlas_assets.py::atlas_preview_available() ANDs it with the
# baked-asset readiness state, so a flag-ON/asset-missing window still hides.
ATLAS_PREVIEW_ENABLED = _env_enabled("ATLAS_PREVIEW_ENABLED", False)
```
**Add**, same idiom, default OFF (this phase ships no UI, but the flag gates any Phase 135+
surface reading through `discovery_available()`):
```python
DISCOVERY_ENABLED = _env_enabled("DISCOVERY_ENABLED", False)
```
Document it in `CLAUDE.md`'s "Environment Variables" section next to `ATLAS_PREVIEW_ENABLED`,
same one-line comment convention (necessary-but-not-sufficient AND'd with sidecar readiness).

---

### `scripts/check_atlas_masking.py` (EXTEND — new `scan_sqlite()` + `--scan-sqlite` mode)

**Analog:** itself. Reuse `build_matcher()`/`PatternMatcher.scan()` (the ONE canonical
decode→unescape→casefold→match pipeline, verified lines 449-708) unchanged; add a new scan
surface that mirrors `scan_asset()`'s fail-closed traversal shape (lines 1047-1146, verified)
but iterates SQLite schema + cells instead of files:
```python
# scan_asset()'s fail-closed shape to mirror (verified, lines 1047-1067, 1111-1121):
def scan_asset(path, patterns, *, strict: bool = False) -> list[Issue]:
    matcher = build_matcher(patterns)
    ...
    try:
        top = os.lstat(p)
        exists = True
    except FileNotFoundError:
        exists = False
    except OSError as exc:
        raise ScanError(f"cannot stat asset path (fail-closed): {matcher.redact_diagnostic(str(path))}") from exc
    ...
    try:
        size = os.lstat(c).st_size
    except OSError as exc:
        if strict:
            raise ScanError(f"cannot stat asset file (fail-closed): {display}") from exc
        continue
```
`PatternMatcher.scan(self, data: bytes, rel_path: str, *, stream_mode: bool = False) ->
list[Issue]` (verified signature, line 561) is the exact call every new `scan_sqlite` cell
check must route through — same as RESEARCH.md's sketch, now confirmed against the real
signature. New `scan_sqlite(db_path, patterns) -> list[Issue]` (the FROZEN single signature,
mirroring `scan_asset(path, patterns)` — F4; builds the matcher internally) should: connect
`sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)`; scan `sqlite_master.sql` (schema/DDL,
catches leaky column/table names) through `matcher.scan(_encode_text(sql), f"{db_path}::schema")`;
then iterate every table/every row/every string AND bytes/BLOB cell through the same
`matcher.scan(...)` call, tagging `surface=f"{db_path}::{tbl}.{col}"` (mirrors `Issue.surface`
usage, e.g. `'filename'` vs default `'content'`, verified line 159); wrap ANY connect/read
failure in `ScanError` (the project's blanket fail-closed convention — verified 40+
`ScanError(...)` raise sites in this file, e.g. lines 720, 750, 789, 977, 1031, 1066,
1071-1074, 1086-1089).

**CLI wiring** — `parse_args()`/`main()` lines 1193-1256 (verified): add `--scan-sqlite
PATH` as a third `store`-style arg alongside `--scan-repo`/`--scan-asset`, following the exact
same `asset_requested = args.scan_asset is not None` / `is not None` presence-test convention
(HIGH-10 comment at lines 1210-1220: NEVER test truthiness of an option value) and the same
`_require_patterns(patterns)` gate before any scan runs (lines 1238-1243). The existing
`--strict` gate at ~line 1228 requires `scan_repo` AND `scan_asset`; the VALID ship form
`--scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict` supplies both, so no gate change is
needed — just include the sqlite issues in the combined report + non-zero exit.

**R-source pre-registration (D-03c)** — `load_patterns()` lines 170-189 (verified): patterns
are loaded from the gitignored file at `MASKING_SCAN_PATTERNS_FILE`, one non-comment,
non-blank line per pattern. Add R-source name/aliases/sigla as additional lines in that
gitignored file NOW (defense-in-depth per D-03c) — no code change needed in
`check_atlas_masking.py` itself for this step, only an operational addition to the local
pattern file (never committed).

---

### Test files

**`tests/test_discovery_loader.py` / `tests/test_discovery_flag.py`** — analog
`tests/test_atlas_flag_gating.py` (verified, full file, 656 lines). Copy the fixture-building
shape:
```python
@pytest.fixture
def ready_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    basename, chash, plain, br_bytes = _write_asset(tmp_path)
    assert aa.load_atlas_state() is True
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", True)
    assert aa.atlas_preview_available() is True
    yield {...}
    aa.load_atlas_state()  # restore
```
and the fail-closed-per-defect-mode test list (each independently monkeypatches one broken
field and asserts `ready=False`, then restores):
`test_malformed_magic_fails_closed`, `test_truncated_section_table_fails_closed`,
`test_out_of_bounds_section_fails_closed`, `test_missing_content_hash_fails_closed`,
`test_non_content_hashed_basename_fails_closed`, `test_corrupt_brotli_sidecar_falls_back_to_plain`
(lines 337-441, verified). For discovery: `test_absent` (empty dir), `test_corrupt_integrity`
(malformed SQLite bytes / failed `PRAGMA integrity_check`), `test_incompatible_version`
(`meta.schema_version` mismatch) — same one-fixture-per-defect-mode shape, same
"assert False, then reload to restore module state" idiom (every test above ends with a bare
`aa.load_atlas_state()` restore call — copy this so tests don't leak state into each other).

Also copy the flag-off / asset-not-ready clean-hide tests (lines 244-257, 305-320, verified) —
`test_flag_off_clean_hides_without_delegating` / `test_data_routes_404_when_asset_not_loaded` —
for `test_discovery_flag.py`'s `test_flag_off_hides` (DATA-07).

**`tests/test_masking_sqlite.py`** — analog `tests/test_atlas_masking_scan.py` (verified,
opening 90 lines read). Reuse the fabricated-fake-token convention verbatim (never the real
restricted string):
```python
FAKE = 'ZZZ_FAKE_MASKING_TOKEN_ZZZ'
FAKE_HE = 'צצצ_...'  # Hebrew fake token
FAKE_GREEK = 'ζζζ_masking_token_ζζζ'  # casefold-bridging test
ALL_FAKES = (FAKE, FAKE_HE, FAKE_DOMAIN, FAKE_GREEK, FAKE_ACC, FAKE_SPACE)

def _assert_never_echoes(issues, extra_text=''):
    for issue in issues:
        rendered = issue.format() + ' ' + repr(issue) + ' ' + extra_text
        for tok in ALL_FAKES:
            assert tok not in rendered, f"never-echo violated for {tok!r}: {rendered!r}"
```
Build a tiny SQLite file containing `FAKE`/`FAKE_HE` in a cell value, a BLOB cell, and a column
name; call `scan_sqlite(db, [FAKE, FAKE_HE, ...])` (PATTERNS list, F4 — scan_sqlite builds the
matcher internally); assert it flags both surfaces (`::schema` and `::table.column`) for str AND
BLOB and that `_assert_never_echoes` holds over the returned `Issue` list — directly parallels
this file's `matcher` fixture (`cam.build_matcher([FAKE, FAKE_HE, ...])`, line 87-88).

**`tests/test_discovery_ids.py` (golden hash reproducibility)** — analog
`tests/atlas_bake/test_atlas_bake.py::test_determinism` / `::test_content_hash_changes`
(verified, lines 359-388):
```python
def test_determinism():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(200, seed=42)
    r1 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    r2 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    e1 = bake.encode_asset(r1)
    e2 = bake.encode_asset(r2)
    assert e1.plain_bytes == e2.plain_bytes

def test_content_hash_changes():
    ... # one input byte changed -> hash1 != hash2
```
Directly transplantable: `test_claim_id_golden` re-runs `claim_id(...)`/`evidence_id(...)` (per
C-5) on frozen fixed inputs twice and asserts identical output, PLUS asserts a committed golden
hex digest (freeze it in `docs/specs/discovery-sidecar-schema-v1.md` per RESEARCH.md Landmine 6);
a companion test changes one input field and asserts the hash changes (no accidental collision).
Also test `corroborated_predicate` (two-seed True / one-seed False, F3) + `claim_type_for_work_witness`
/ `resolve_claim_type` totality + the `select_display_evidence` lattice.

**`tests/fixtures/discovery/` fixture DB** — analog `tests/fixtures/atlas/golden-v1.bin` +
`golden-v1-expected.json` (verified present at `tests/fixtures/atlas/`) + the regeneration CLI
contract `python scripts/build_atlas_asset.py --golden tests/fixtures/atlas/golden-v1.bin`
(verified, `scripts/build_atlas_asset.py` line 29). Build
`tests/fixtures/discovery/discovery-v1-fixture.db` the same way: a small, deterministic,
fabricated (never real-corpus-derived) SQLite file regenerable via
`python scripts/build_discovery_sidecar.py --golden tests/fixtures/discovery/discovery-v1-fixture.db`,
committed alongside a `discovery-v1-fixture-expected.json` of asserted invariants (row counts,
valid evidence combinations, one display_evidence_id per claim, frame_content_hash, etc. — NOT
"one band per key", DROPPED per F8) mirroring `golden-v1-expected.json`'s role.

---

## Shared Patterns

### Sidecar service open + graceful-absent (applies to ALL of `shared/discovery_service.py`)
**Source:** `shared/fjms_service.py` lines 690-811 (verified) + `shared/thread_local_db.py`
(verified, full file).
**Apply to:** every method on the new `DiscoveryService` class — `is_available()`,
`get_version()`/release-contract accessors, every claim/evidence/unit read.
```python
def is_available(self) -> bool:
    return self._conn is not None

def get_version(self) -> Optional[str]:
    if self._conn is None:
        return None
    try:
        cursor = self._conn.execute("SELECT value FROM meta WHERE key = 'version'")
        row = cursor.fetchone()
        return row["value"] if row else None
    except Exception as e:
        logger.error(f"...: {e}")
        return None
```
Every read method: `if self._conn is None: return []` (or `None`), wrap the query in
`try/except Exception: logger.error(...); return <empty>` — never raise out of a read method.
**F15:** the connection is built LAZILY on first use (not in `__init__`) and recreated on
path/version change.

### Off-loop async query + timeout → overload (DATA-06)
**Source:** `web/search_api.py` lines 379-458 (semaphore) + 1104-1166 (`asyncio.wait`, NOT
`wait_for`) (verified).
**Apply to:** every `DiscoveryService` async wrapper method.
```python
_search_fut = loop.run_in_executor(None, _run_search_sync)
_done, _pending = await asyncio.wait({_search_fut}, timeout=core_timeout)
if _search_fut in _pending:
    raise APIError('core_timeout', f'... did not complete within {core_timeout}s', http_status=504)
results = _search_fut.result()
```
**Critical rationale to preserve:** `run_in_executor` threads cannot be cancelled;
`asyncio.wait_for` would abandon-but-not-kill the thread and could re-admit new heavy work past
a concurrency budget while the old thread still runs. Use `asyncio.wait` + release any
concurrency slot from a `future.add_done_callback`, never from a bare `finally` around the
await.

### Fail-closed versioned asset loader (DATA-08)
**Source:** `web/atlas_assets.py` (verified, full file, 321 lines).
**Apply to:** `web/discovery_assets.py::load_discovery_state()`.
```python
def load_atlas_state() -> bool:
    global _state
    new_state = _AtlasState(ready=False)
    try:
        ... # open + validate everything
        new_state = _AtlasState(ready=True, ...)
    except Exception as exc:
        logger.info("Atlas preview asset not loaded (fail-closed): %s", exc)
        new_state = _AtlasState(ready=False)
    with _lock:
        _state = new_state
    return _state.ready
```
One giant `try/except Exception` wrapping the ENTIRE load (never a partial state escapes); a
`threading.Lock()`-protected atomic module-state swap; a public `*_available()` predicate that
ANDs the feature flag with `_state.ready`.

### Feature flag gating (DATA-07)
**Source:** `web/feature_flags.py` lines 8-24 (verified) + `web/atlas_assets.py::atlas_preview_available`
(line 287-294, verified).
**Apply to:** `DISCOVERY_ENABLED` + `discovery_available()`.
```python
def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

DISCOVERY_ENABLED = _env_enabled("DISCOVERY_ENABLED", False)

def discovery_available() -> bool:
    return bool(DISCOVERY_ENABLED and _state.ready)  # flag AND readiness — never flag alone
```

### Masking scan — single canonical matcher, every surface fail-closed (DATA-05)
**Source:** `scripts/check_atlas_masking.py` (verified — module docstring, `PatternMatcher`,
`scan_asset`, `main`).
**Apply to:** the new `scan_sqlite()` mode AND the existing `--scan-repo`/`--scan-asset` modes
now also pointed at `discovery_data/` + any new committed docs (`discovery-frames.md`,
`docs/specs/discovery-sidecar-schema-v1.md`).
```python
patterns = load_patterns()             # MASKING_SCAN_PATTERNS_FILE, gitignored
_require_patterns(patterns)             # hard-fails a zero-pattern (false-green) scan
issues = scan_repo(patterns) + scan_asset(path, patterns) + scan_sqlite(db_path, patterns)
_report(issues)
return 0 if not issues else 1
```
Never hardcode restricted strings in code; never echo a matched pattern or a leaky path in any
diagnostic (`Issue.format()` / `_sanitize()` route every message through the active matcher's
redactor).

### Neutral, web-free exception types in `shared/`
**Source:** `shared/api_errors.py` (verified, full file).
**Apply to:** any new `shared/discovery_service.py`-raised exception (`DiscoveryUnavailable` /
`DiscoveryOverload`).
```python
class APIError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400, headers: Optional[dict] = None):
        ...
```
Keep it in `shared/` (never import FastAPI/Starlette/NiceGUI there). The layering is enforced by
a NEW essential guard `tests/test_no_back_edges_discovery.py` (F6 — the existing
`tests/test_no_back_edges_core.py` only bans `genizah_core`, not `web`; the new guard reuses its
traversal SHAPE to ban `web`/`nicegui`/`fastapi` in the discovery shared modules).

---

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| The actual research-DB read/cluster/classify logic inside `scripts/build_discovery_sidecar.py` (band assignment from the FOUR DISJOINT track1_direct source populations + the PROPAGATED witness collection's literal corroborated predicate + the shared_text family; `shadowed_by IS NULL` filtering; opaque-id minting from raw `M:`/`J:`/`REF` work_ids) | script (offline ETL) | batch | The only real precedent (`same_work_spike/probe/scripts/build_reuse_graph.py`, `build_atlas_draft.py`, `q2_router.py`, `q2_shared_text.py`, `q2_candidates.py`) is gitignored/off-repo research and may not be quoted or forked verbatim per the masking constraint; RESEARCH.md's "fork the shape" guidance stands, but no committed code excerpt can back it — implement fresh against RESEARCH.md's DDL/hashing recipe, the corrected C-1..C-9 contract, and the ground-truth field/key/predicate map frozen in `docs/specs/discovery-sidecar-schema-v1.md` (134-01) + the 134-04 interfaces block. |
| The `--from-approved` APPROVED-CSV reader (owner-approved neutral-title re-consume) | script (offline ETL) | batch | EMISSION analogs exist (`export_translation_audit_sample.py` writer, `translation_report.py` append-only status UI) but NEITHER reads an approved CSV back (F5); the reader + its frozen APPROVED header + fail-closed rejection rules + round-trip are genuinely NEW behavior to build fresh. |
| `discovery-frames.md` / `discovery-budgets.md` generation logic (the frozen-frame per-band count report + PERF-01 numeric-caps report) | doc-generation | batch | No committed "frozen frame" doc-generator exists yet; closest shape is `scripts/export_translation_audit_sample.py::write_summary` (human-readable report from data, verified lines 530-557) — a partial match only (it summarizes QC stats, not a schema-versioned frozen frame with a content hash). Build fresh, informed by `docs/specs/atlas-asset-schema-v1.md`'s versioning-doc conventions (cited, not read in full this pass — planner should read it directly when drafting `discovery-sidecar-schema-v1.md`). |
| `tests/test_discovery_service.py` (async overload/pagination-bound unit tests against the NEW service, using a monkeypatched slow sync query) | test | — | No existing `tests/` file drives an async `DiscoveryService`-shaped chokepoint in isolation (the closest behavior — `web/search_api.py`'s heavy-mode timeout — is tested end-to-end via the FastAPI route, not as a bare service unit). Compose the `asyncio.wait`+timeout assertion style from `web/search_api.py`'s own runtime behavior with `test_atlas_flag_gating.py`'s monkeypatch-fixture idiom. |
| `tests/test_no_back_edges_discovery.py` (NEW essential layering guard) | test | — | `tests/test_no_back_edges_core.py` supplies the scope-aware AST-traversal SHAPE only; it bans `genizah_core`, NOT `web`/`nicegui`/`fastapi`, so the discovery guard's target-module logic is genuinely new (F6). |

## Metadata

**Analog search scope:** `web/*.py`, `shared/*.py`, `scripts/*.py`, `tests/*.py`,
`tests/atlas_bake/*.py`, `tests/fixtures/atlas/*`, `docs/specs/*.md` (Phase 133 precedent).
Gitignored `same_work_spike/probe/**` was NOT read or quoted in the ORIGINAL pattern pass
(masking constraint); the 2026-07-21 REWORK inspected the gitignored research DATA (field names,
counts, join keys, the spike's own predicate CODE) to freeze the ground-truth contract, but
committed material carries only field names / join keys / predicates / masked codes — never
restricted text, sigla, corpus names, or raw work_id values.
**Files scanned (read in full or targeted ranges):** `web/atlas_assets.py` (full, 321 lines),
`web/feature_flags.py` (full, 42 lines), `web/search_api.py` (targeted: 330-460, 1080-1170),
`shared/fjms_service.py` (targeted: 1-90, 130-199, 690-811, 2651-2701), `shared/thread_local_db.py`
(full, 150 lines), `shared/api_errors.py` (full, 92 lines), `scripts/check_atlas_masking.py`
(targeted: 1-100, 126-200, 449-530, 705-745, 1047-1146, 1193-1260), `scripts/export_translation_audit_sample.py`
(targeted: 1-120, 508-567), `web/components/translation_report.py` (full, 179 lines),
`scripts/build_atlas_asset.py` (targeted: 255-300, 1190-1220), `tests/test_atlas_flag_gating.py`
(full, 656 lines), `tests/test_atlas_masking_scan.py` (targeted: 1-90), `tests/atlas_bake/test_atlas_bake.py`
(targeted: 355-460), `tests/test_no_back_edges_core.py` (full, 2026-07-21 rework).
**Pattern extraction date:** 2026-07-21 (REWORK F5/F6 corrections: 2026-07-21)
