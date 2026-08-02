# Phase 136: Read Surfaces — Connections Panel & Work→Witnesses - Pattern Map

**Mapped:** 2026-07-31
**Files analyzed:** 20 (7 offline/build, 8 web, 5 test)
**Analogs found:** 16 strong/role-match, 2 partial (self-file extension only), 4 explicit NO-ANALOG (flagged for design budget)

All line numbers below were re-verified with `Grep`/`Read` against HEAD on 2026-07-31 (repo HEAD
`f7f7a2205a9b8c6ea1c0d8f3490bd405f3c07b80` per 136-RESEARCH.md). Where CONTEXT.md's cited numbers had
drifted, the corrected number is used and the drift is noted.

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `scripts/verify_rebuild_preservation.py` (NEW) | utility / offline verifier | batch (old/new DB diff) | `scripts/verify_discovery_sidecar.py` (CLI/exit-code shape) + `scripts/cert01_frame.py` (hash reuse) | role-match (mechanism itself is new — see No-Analog) |
| `136-REBUILD-PRESERVATION-EXPECTED.json` (NEW artifact) | config / pinned artifact | file-I/O | `docs/specs/discovery-budgets.md` / `discovery-frames.md` versioned-artifact discipline + `cert01_prereg.json` shape | role-match |
| `scripts/build_discovery_sidecar.py` — coverage_ppm persistence (MODIFY) | ETL / batch script | batch, transform | itself: `_attach_coverage`/`_mk_evidence`/`_ingest_tier_a` (lines 2641-2714) | exact (same file, mechanical extension) |
| `scripts/build_discovery_sidecar.py` — D-02a tier_a registry row (MODIFY) | ETL / batch script | batch, transform | itself: `_frozen_real_band_precision_rows` (1690-1775) + `_validate_precision_spec` (3867-4039) | exact (same file, mechanical extension) |
| `scripts/build_discovery_sidecar.py` — materialized `band_rank`/indexes (MODIFY) | ETL / batch script | batch, transform | `shared/discovery_service.py::_BAND_RANK_ORDER`/`_band_rank`/`_build_band_rank_case_sql` (145-199) — the RUNTIME lattice to mirror at build time | role-match |
| `scripts/build_discovery_sidecar.py` — novelty tri-state ingestion (MODIFY) | ETL / batch script | batch, transform, event-driven (LLM call) | pinned-input-artifact pattern (`--canonical-merges`/`v2_canonical_merges.build.json`, cited but not re-read this session) | **NO ANALOG for the computation itself** — see below |
| `scripts/build_discovery_sidecar.py` — VIS-01 closed-graph public projection (NEW function) | ETL / batch script | batch, transform | none in-tree | **NO ANALOG** — see below |
| `scripts/verify_discovery_sidecar.py` — D-02a lockstep checks + one-result-per-claim novelty check + VIS-01 projection checks (MODIFY) | utility / offline verifier | batch | itself: `check_band_precision` (M4, ~505-560), `check_no_mixed_enum_state` (570-589), `verify()` (1061-1108) | exact (same file, add-a-check pattern) |
| `scripts/bench_discovery.py` — `bench_findings_page()` probe (MODIFY) | utility / perf harness | batch | itself: `run_benchmark`/`warm_burst`/`main()` (260-389) | exact (same file) |
| `web/pages/work.py` (NEW `/work/{id}` page) | page / route | request-response, CRUD (paginated read) | `/atlas` route + gate in `web/main.py` (2599-2650) for gating; `web/pages/catalog_browse.py` `_fetch_results_blocking`/`fetch_results` (280-333) for filter/sort/pagination-off-loop | role-match (two analogs, see below for which governs which part) |
| `web/pages/findings.py` (NEW corpus-wide findings page, name TBD — NOT `discoveries.py`) | page / route | request-response, CRUD (paginated read) | same two as above | role-match |
| `web/main.py` — nav entry + `@ui.page` routes + imports (MODIFY) | route / config | request-response | itself: `/atlas` route (2599-2650), `nav_items` list (1769-1789), `atlas_assets`/`discovery_assets` import block (705-719) | exact |
| `web/pages/browse_enrichment.py` — panel integration, 5th `enrichment_refs` placeholder (MODIFY) | component / enrichment loader | event-driven (deferred Phase-B render) | itself: the 4 existing sections in `update_enrichment_sections()` (488-557) | exact |
| Offset-highlight renderer (NEW pure function, e.g. `shared/discovery_render.py`) | utility (pure fn) | transform | consumer: `web/components/typography.py::render_line_numbered_html` (66-186); algorithm: `sketch-findings-genizahsearch` skill reference, NOT `web/pages/browse.py::highlight_text` (1577-1601, anti-pattern) | **CORRECTED — see below, RESEARCH.md's own snippet is wrong** |
| Findings-page counts off event loop (part of `web/pages/findings.py`) | service call site | request-response | `web/pages/catalog_browse.py::_fetch_results_blocking` + `await run.io_bound(...)` (280-333) | exact — **NOT** `_CatalogFacetWorker` (that's a PyQt6 `QThread` in `genizah_app.py`, desktop-only, architecturally inapplicable) |
| `web/pages/help.py` — methods-section rewrite (MODIFY) | page component | request-response | itself: `_render_confidence_section`/`_render_one_band`/`_CONFIDENCE_FIELD_LABELS` (58-260) | exact (same file, remove-fields-not-add) |
| `shared/discovery_service.py` — `{status, items, total}` envelope, D-17a display fields, count query, D-13a/b grouping (MODIFY) | service | CRUD, async chokepoint | itself: `get_claims_for_page`/`get_work_witnesses`/`_run_off_loop` (598-847, 887-919) | exact (same file) |
| `web/pages/catalog_browse.py` — computed-identification integration (MODIFY) | page component | request-response | itself: `_fetch_results_blocking`/shelfmark resolution (280-333, 262-309/376-438 per CONTEXT — unread this session, cited only) | role-match |
| `tests/render_smoke/test_panel_render_smoke.py` (NEW) | test / render-smoke | request-response (simulated) | `tests/render_smoke/test_help_methods_render_smoke.py` (full file, Phase 135-02) | exact |
| `tests/render_smoke/test_work_page_render_smoke.py` (NEW) | test / render-smoke | request-response (simulated) | same | exact |
| `tests/render_smoke/test_findings_page_render_smoke.py` (NEW) | test / render-smoke | request-response (simulated) | same | exact |
| `tests/test_vis02_positive_control.py` (NEW) | test / integration (masking) | batch | `scripts/check_atlas_masking.py::_run_self_test` (1292-1322) + CLI `--scan-sqlite`/`--scan-asset --strict` | role-match |
| `tests/test_discovery_schema.py` / `tests/test_discovery_build.py` extensions (D-02a both branches) | test / unit | batch | itself: `test_ingest_tier_a_shadowed_filter_and_largest_span` (855) and neighboring fixture-based tests using `synthetic_discovery_dataset()` | exact |

---

## Pattern Assignments

### `scripts/verify_rebuild_preservation.py` (utility, batch/offline-diff)

**Analog:** `scripts/verify_discovery_sidecar.py` (CLI + exit-code + `verify()` dispatcher shape) and
`scripts/cert01_frame.py` (hash recipes to REUSE, never reimplement).

**CLI / argument-parsing pattern** (`scripts/verify_discovery_sidecar.py:1115-1145`):
```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("db_path", help="Path to the discovery.db sidecar to verify")
    parser.add_argument("--expected-frame-hash", metavar="HEX", default=None, ...)
    parser.add_argument("--expected-band-vocabulary", choices=["v1", "v2"], default=None, ...)
    return parser

def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    ...
    return verify(args.db_path, args.expected_frame_hash, expected_band_vocabulary=expected_vocab)

if __name__ == "__main__":
    sys.exit(main())
```
Model `verify_rebuild_preservation.py <old_db> <new_db> --expected <pinned.json>` on this exact shape:
positional DB path(s) + `--expected-*` flags, a single `verify(...)`-style dispatcher that accumulates a
`violations: List[str]` and returns `0`/`1`, one `print("VIOLATION: ...", file=sys.stderr)` per finding
(see `verify()` at 1061-1108) — never raise for an expected data problem, only for a genuine usage error.

**Hash reuse — do NOT reimplement** (`scripts/cert01_frame.py:293-306`):
```python
def population_hash(rows: Sequence[dict]) -> str:
    """SHA-256 over the sorted `(page_id, canonical_work_id, stratum)` triples..."""
    lines = sorted(f"{r['page_id']}|{r['canonical_work_id']}|{r['stratum']}" for r in rows)
    key = "\n".join(lines)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def cluster_map_hash(rows: Sequence[dict]) -> str:
    """SHA-256 over the sorted `(page_id, canonical_work_id, unit_key)` triples..."""
    lines = sorted(f"{r['page_id']}|{r['canonical_work_id']}|{r['unit_key']}" for r in rows)
    key = "\n".join(lines)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
```
`verify_rebuild_preservation.py` must **import and call** `scripts.cert01_frame.population_hash` /
`cluster_map_hash` — never re-derive the triple-key/delimiter convention locally (a second copy would
drift).

**Frame-hash comparison pattern to mirror** (`scripts/verify_discovery_sidecar.py:1041-1054`):
```python
def check_frame_content_hash(conn, meta: dict, expected_frame_hash) -> List[str]:
    violations = []
    recomputed = sidecar_build.compute_frame_content_hash(conn)
    meta_hash = meta.get("frame_content_hash")
    if recomputed != meta_hash:
        violations.append(f"frame_content_hash mismatch: recomputed {recomputed} != meta.frame_content_hash {meta_hash}")
    if expected_frame_hash is not None and recomputed != expected_frame_hash:
        violations.append(...)
    return violations
```
Same idiom for the rebuild-preservation gate: recompute on the NEW asset, compare against the value
PINNED in `136-REBUILD-PRESERVATION-EXPECTED.json` (never against the new build's own manifest — this is
the direct fix for Codex F-04).

**Masking discipline in violation messages** (`scripts/build_discovery_sidecar.py:_validate_precision_spec`,
excerpted below) — violation strings must never echo a raw cell value, only a primary key / position.
This is the SAME discipline `verify_rebuild_preservation.py`'s mismatch report must follow (per D-02b's
own "never echo raw cell values" instruction).

**NO ANALOG for the mechanism itself.** No script in the repo performs a full per-table streamed
content-hash diff between two SQLite files with a column allowlist. `verify_discovery_sidecar.py` and
`cert01_frame.py` supply the *shape* (CLI, hash recipes, violation-accumulation) but the actual
"stream every row of 6 tables, project out allowlisted columns, hash the remainder, compare old vs new"
loop must be designed fresh. **Budget real design time here** — this is RESEARCH.md's own assessment
("the highest-risk, least-precedented item") and this pattern search did not find a stronger analog.

---

### `136-REBUILD-PRESERVATION-EXPECTED.json` (pinned artifact)

**Analog:** the versioned-artifact discipline already established by `docs/specs/discovery-budgets.md`
("tunable ONLY by versioning this artifact... never silently loosened or tightened in code") and the
`cert01_prereg.json` / `deck manifest` pattern read by `scripts/verify_cert01_grading.py`
(`check_10_input_hash_pinning`, 206-212):
```python
def check_10_input_hash_pinning(ctx: dict):
    prereg = ctx["prereg"]
    hashes = ctx["input_hashes"]
    for key in ("canonical_merges_sha256", "composition_dates_sha256",
                "seftja_dates_sha256", "db_content_hash"):
        if hashes[key] != prereg.get(key):
            _fail(f"{key} recompute mismatch: prereg {prereg.get(key)!r} != recomputed {hashes[key]!r}")
```
Same shape: a small, flat, hash-and-count-only JSON, generated ONCE from the currently-live asset
BEFORE the rebuild runs, never regenerated from the candidate. Masking discipline: opaque `w000xxx`
ids/hashes/counts only — mirrors `discovery-frames.md`'s own masking-clean-by-construction shape (per
RESEARCH.md Pitfall 6).

---

### `scripts/build_discovery_sidecar.py` — persist `coverage_ppm` (MODIFY)

**Analog:** itself — the mechanism is 90% built and only needs its output KEPT.

**Coverage is already computed and then discarded** (`scripts/build_discovery_sidecar.py:2641-2657`):
```python
def _attach_coverage(spec: Dict, page_index, page_id: str, matched_letters: Optional[int]) -> Dict:
    """Set the Lever-1 routing input spec['coverage'] = matched_letters / page_norm_letters..."""
    norm_fn = getattr(page_index, "norm_letters", None)
    if norm_fn is None:
        return spec
    page_norm_letters = norm_fn(page_id)
    spec["coverage"] = compute_page_coverage(matched_letters, page_norm_letters)
    return spec
```
called from `_ingest_tier_a` (2689-2714) and `_ingest_e1_rows` (2660-2686) — both build a `spec = _mk_evidence(...)`
dict and then call `_attach_coverage(spec, page_index, page_id, matched_letters)`. `_mk_evidence`'s
returned dict shape (1316-1346) has **no `coverage` key at all** — nothing persists it downstream. The
fix is mechanical: add `coverage` (or a renamed field) to `_mk_evidence`'s return dict, thread it through
the two `INSERT INTO discovery_evidence` column lists (grep `INSERT INTO discovery_evidence` — two call
sites, ~1861 and ~3245), storing `round(spec['coverage'] * 1_000_000)` as `coverage_ppm INTEGER` plus a
validity/status column (`compute_page_coverage` already returns `0.0` on a missing denominator — that
must map to a `'no_denominator'` sentinel, not indistinguishable from "never computed").

**The underlying metric function to leave untouched** (532-568):
```python
def norm_stream_letter_count(text: Optional[str]) -> int:
    if not text:
        return 0
    n = 0
    for ch in unicodedata.normalize("NFC", text):
        folded = _FINAL_FOLD.get(ord(ch))
        code = ord(ch) if folded is None else ord(folded)
        if _HEB_MIN <= code <= _HEB_MAX:
            n += 1
    return n

def compute_page_coverage(matched_letters, page_norm_letters) -> Optional[float]:
    if matched_letters is None:
        return None
    if not page_norm_letters:
        return 0.0
    return min(1.0, matched_letters / page_norm_letters)
```
D-08a requires this only for `evidence_source='track1_direct'` (direct family) — `_ingest_propagated_witness`
never computes `matched_letters` against a page-length denominator at all, so propagated rows correctly
get no `coverage_ppm`.

---

### `scripts/build_discovery_sidecar.py` — D-02a tier_a registry row + lockstep (MODIFY)

**Analog:** itself — `_frozen_real_band_precision_rows` (1690-1775) is the ONE source of truth;
`_validate_precision_spec` (3867-4039) is the gate that must widen to check it.

**The frozen tier_a row to amend** (1747-1756):
```python
{
    "scope": "band", "collection_id": "e1_certification_registry_v1",
    "evidence_source": _TRACK1, "confidence_band": _TIER_A,
    "numerator": None, "denominator": None, "precision": None,
    "ci_low": None, "ci_high": None, "method": None,
    "sampling_frame": None, "ins_policy": None, "weighting": None,
    "notes": "H3: tier_a carries NO measured precision in the frozen contract -- "
             "NEVER a fabricated number in a real/release build...",
},
```
D-02a requires adding `"ci_low": 0.9084` and `"measurement_status": "measured_pass"` to exactly this
dict — `precision` stays `None`.

**The dict-literal override mechanism that makes this reach the DB** (`4417-4428`):
```python
cur.executemany(
    """
    INSERT INTO band_precision (
        scope, collection_id, evidence_source, confidence_band, numerator, denominator,
        precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes,
        measurement_status
    ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
               :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
               :ins_policy, :weighting, :notes, :measurement_status)
    """,
    [{"measurement_status": None, **r} for r in bp_rows],
)
```
`{"measurement_status": None, **r}` — a later `**r`-supplied key wins over the earlier dict-literal
default, so simply adding `"measurement_status"` to the frozen row is sufficient for it to reach the
INSERT. **Non-obvious from reading the INSERT alone — write a fixture test proving this mechanism**, per
D-02a lockstep site #2.

**The gate that must widen** (`_validate_precision_spec`, 3867-4039, excerpt of the currently-narrow
value check at 4011-4027):
```python
for key, frozen_row in frozen_band_by_key.items():
    matches = band_rows_by_key.get(key, [])
    if len(matches) != 1:
        problems.append(...)
        continue
    actual_precision = matches[0].get("precision")
    expected_precision = frozen_row["precision"]
    if not _precision_matches(actual_precision, expected_precision):
        problems.append(f"band {key}: precision mismatch (expected frozen {expected_precision!r})")
```
This currently checks **only `precision`** per band row — it does NOT cross-check `ci_low`/
`measurement_status`. D-02a lockstep site #3 requires widening this loop to also assert
`matches[0].get("ci_low")`/`matches[0].get("measurement_status")` equal the frozen amended values
exactly, using the SAME masking-safe idiom already used here (`_precision_matches` compares by tolerance
and the violation message never echoes the supplied value — see `_precision_matches`/the "masking
(Codex R5)" comments at 3991-4027) — copy that idiom for the two new fields, do not invent a
differently-worded check.

**`scripts/verify_discovery_sidecar.py` M4 (`~553-560`) is the parallel release-verifier gate** that must
independently assert the same two fields (lockstep site #4) — see its excerpt in the next section.

---

### `scripts/build_discovery_sidecar.py` — materialized `band_rank` + indexes (MODIFY)

**Analog:** `shared/discovery_service.py`'s RUNTIME band-rank lattice (145-199) — the build must
materialize the SAME ordering as a stored, indexed column, not invent a new ranking.

```python
_BAND_RANK_ORDER: List[Tuple[str, str]] = [
    ("track1_direct", "high_confidence_algorithmic"),
    ("track1_direct", "expert_verified"),
    ("track1_direct", "tier_a"),
    ("propagated", "corroborated"),
    ("track1_direct", "screening_rb"),
    ("track1_direct", "screening_canon"),
    ("propagated", "weak"),
    ("propagated", "not_evaluated"),
]
_BAND_RANK_INDEX: Dict[Tuple[str, str], int] = {pair: i for i, pair in enumerate(_BAND_RANK_ORDER)}

def _band_rank(evidence_source, confidence_band) -> int:
    """Lower is "stronger" (rank 0 = expert_verified, the strongest band)."""
    return _BAND_RANK_INDEX.get((evidence_source, confidence_band), _UNRANKED_BAND)
```
The bake must write this SAME lattice into a new `band_rank INTEGER` column on `discovery_evidence` (or
`discovery_claim`, whichever the findings-page query needs to `ORDER BY`/index), computed with the exact
same `(evidence_source, confidence_band)` → rank mapping — a second, drifted copy of this table would be
a correctness bug (the runtime service and the stored sort key disagreeing). Reuse `_band_rank` (import
it, or port the identical dict) rather than re-deriving band order from scratch.

---

### `scripts/build_discovery_sidecar.py` — novelty tri-state ingestion (NEW logic in existing file)

**NO ANALOG for the computation.** `scripts/discovery_identified_gate.py` and `scripts/title_gate_llm.py`,
which CONTEXT.md cites as "the pre-built NOVEL-01 funnel and title gate to rewire," **do not exist in the
committed tree** — confirmed via `git check-ignore`/`git ls-files` in RESEARCH.md; they live at
`same_work_spike/probe/scripts/*.py`, gitignored. There is no in-repo committed novelty-computation code
to copy a pattern from.

**The closest available PATTERN (for the hand-off shape, not the computation) is the pinned-build-input
artifact convention** already used for canonical merges: a `--canonical-merges <path>` /
`v2_canonical_merges.build.json`-style flag that ingests a pre-computed, hash-pinned JSON and records its
SHA-256 in `meta` (mirrors `canonical_merges_sha256`/`composition_dates_sha256`/`seftja_dates_sha256`,
referenced in `check_10_input_hash_pinning` above). Recommended shape per RESEARCH.md Pitfall 4: run the
gitignored LLM-gate scripts as a local, uncommitted pre-build step, stage their verdict-cache output as
`discovery_data/<name>.build.json`, and add a new `--novelty-verdicts <path>` flag to
`build_discovery_sidecar.py` that ingests and hash-pins it — never attempt to "edit"
`scripts/discovery_identified_gate.py` in place, that file is not there.

**Do budget design time** for: (a) the tri-state ingestion/column-write logic itself, (b) the
"every evidence row of a claim inherits ONE result" invariant (new verifier check, see below), (c) the
reviewed `novelty_work_key` (D-23d) — none of these have an in-tree precedent.

---

### `scripts/build_discovery_sidecar.py` — VIS-01 closed-graph public projection (NEW function)

**NO ANALOG.** No existing script in this repo derives a second, filtered/projected SQLite database from
a source database with FK-closure verification. The closest *conceptually* related code is
`scripts/check_atlas_masking.py`'s scan-after-the-fact approach (masking is checked on the OUTPUT, not
enforced by a closed-graph construction), and `scripts/verify_discovery_sidecar.py`'s
`check_integrity_and_fk` (`PRAGMA foreign_key_check`, 369-377) which is a verifier, not a projector.
**Budget real design work here** — this was flagged as HIGH by Codex (F-05) and is architecturally novel
in this codebase. The one thing to reuse: `PRAGMA foreign_key_check` as the closed-graph verification
step AFTER projection (do not reinvent FK-closure checking; `check_integrity_and_fk` already knows how).

---

### `scripts/verify_discovery_sidecar.py` (MODIFY — add checks)

**Analog:** itself — every one of the ~15 existing checks follows the identical shape: a function
`check_X(conn) -> List[str]` returning violation strings, wired into `verify()`'s accumulation list.

**The check-function shape to copy exactly** (`check_source_corpus_consistency`, 319-333):
```python
def check_source_corpus_consistency(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute("""
        SELECT dc.claim_id, dc.source_corpus, w.source_corpus
        FROM discovery_claim dc JOIN works w ON w.work_id = dc.work_id
        WHERE dc.source_corpus != w.source_corpus
    """)
    for claim_id, claim_src, work_src in cur.fetchall():
        violations.append(f"claim {claim_id}: source_corpus {claim_src!r} != parent work source_corpus {work_src!r} (F4)")
    return violations
```

**M4 tier_a-precision-NULL check to widen** (`~553-560`, D-02a lockstep site #4):
```python
tier_a_key = (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A)
tier_a_row = _single_row(tier_a_key)
if tier_a_row is not None:
    precision = tier_a_row[0]
    if precision is not None:
        violations.append(f"release band_precision (M4): tier_a precision must be NULL, got {precision}")
```
Must gain a companion assertion that `ci_low`/`measurement_status` on this SAME row equal exactly the
D-02a-authorized values (0.9084 / 'measured_pass') — otherwise a future build could smuggle an arbitrary
`ci_low` through this slot undetected (RESEARCH.md's own point).

**Registration point** — `verify()`'s accumulation list (1074-1096) is where every new check function
gets ADDED as one more `violations += check_new_thing(conn)` line; follow that ordering convention
(structural checks first, content checks after, hash checks last).

**The "one result per claim" novelty check** should follow the SAME shape as `check_per_side_drift`
(340-362) — iterate grouped rows, append one violation string per offending claim_id, never raise.

---

### `scripts/bench_discovery.py` — `bench_findings_page()` probe (MODIFY)

**Analog:** itself — `pick_live_keys`/`run_benchmark`/`warm_burst`/`main()` (unread portions between
90-260, referenced via the excerpted `warm_burst`/`main` at 260-389).

```python
async def warm_burst(service, keys, passes):
    """Warm the connection + LRU by re-querying every key `passes` times.
    Returns the total rows touched (a nonzero-result sanity signal)."""
    touched = 0
    for _ in range(max(1, passes)):
        for k in keys["claim_pages"]:
            touched += len(await service.get_claims_for_page_async(k, page=1, page_size=50) or [])
        ...
    return touched
```
The findings-page probe should follow this EXACT nonzero-result-assertion discipline (F14: never
silently benchmark an empty query) and the SAME `--write-budgets` flag mechanism (`main()`, 283-389) to
record the measured number into `docs/specs/discovery-budgets.md` — do not print a bare number to stdout
only. The KNOWN FAILING BASELINE (3.41-3.55s vs 1.5s cap, per Codex F-10) makes this the strongest kind
of regression test to add: assert the NEW query sits under budget with margin, referencing the
already-measured prior failure in the assertion message.

---

### `web/pages/work.py` and `web/pages/findings.py` (NEW pages) — gating

**Analog:** `/atlas` — the exact, already-hardened (4 Codex rounds) model CONTEXT.md itself names, with
corrected line numbers (CONTEXT.md's `web/main.py:2086-2907` had drifted ~16-20 lines; verified via
`grep -n "^@ui.page"` this session: routes now span 2102-2923, `/atlas` itself at 2599).

**Import block to mirror** (`web/main.py:705-719`):
```python
from web.atlas_assets import (
    load_atlas_state, atlas_preview_available, atlas_bin_name,
    atlas_plain_bytes, atlas_br_bytes, atlas_manifest_bytes, atlas_manifest_etag,
)
from web.discovery_assets import load_discovery_state, discovery_available  # noqa: F401
from web.discovery import (
    discovery_methods_noindex, get_all_band_precision, get_band_claim_counts,
)
```
`web.discovery_assets.discovery_available` is ALREADY imported and used (for `/help`) — no new import
plumbing needed for the base availability gate; a new page module needs its own
`from web.discovery_assets import discovery_available` (or read it via `web.discovery`, whichever the
plan's file-boundary favors) plus, per D-13/F-14, the NEW envelope-aware calls into
`shared.discovery_service`/`web.discovery` (see next section).

**The route + clean-hide gate to copy verbatim in shape** (`web/main.py:2599-2650`):
```python
@ui.page('/atlas', title='...')
def atlas_page_route():
    safe_user_set('current_page', '/atlas')
    ui.add_head_html(page_meta('/atlas', title='...', description='...', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT); ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES); ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        if not atlas_preview_available():
            is_hebrew = get_language() == 'he'
            with ui.column().classes('w-full max-w-3xl mx-auto p-6'):
                with ui.card().classes('w-full p-8'):
                    ui.icon('construction').classes('text-4xl text-amber-600 mb-3')
                    ui.label('... is temporarily unavailable' if not is_hebrew else '...').classes('text-2xl font-bold mb-2')
                    ui.label('The preview will appear here as soon as it is ready.' if not is_hebrew else '...')
                    with ui.row().classes('gap-2 mt-4'):
                        ui.button('Home', on_click=lambda: ui.navigate.to('/')).props('flat')
            return
        from web.pages.atlas import create_atlas_page
        create_atlas_page()
```
`/work/{id}` and the findings page must each: gate on `discovery_available()` (or a page-specific
availability wrapper per D-13's envelope, since a query TIMEOUT must render differently from "flag off" —
see the D-13 envelope note under `shared/discovery_service.py` below), early-return with a
"temporarily unavailable" card BEFORE ever importing/calling the real page-builder module, and defer the
heavy page-builder import to inside the `if available` branch (`from web.pages.atlas import
create_atlas_page` pattern) so an unavailable render never even imports the heavy module.

**Nav-item gating to copy verbatim** (`web/main.py:1769-1789`):
```python
nav_items = [
    ('/', 'home', tr('Home'), None),
    ...
    ('/catalog-browse', 'category', tr('Browse by Identification'), None),
    ('/discoveries', 'lightbulb', tr('Community'), None),   # <-- UNRELATED, pre-existing
    ...
]
if WEB_PUZZLE_ENABLED:
    nav_items.append(('/puzzle', 'extension', tr('Fragment Puzzle'), None))
if atlas_preview_available():
    nav_items.append(('/atlas', 'hub', tr('The Genizah Atlas'), tr('Beta')))
```
**Naming hazard confirmed exact:** `/discoveries` (nav label "Community", route registered at
`web/main.py:2750`) is the pre-existing Supabase community feature — completely unrelated to this
phase's module, but same English word. D-19 already settles the new page must NOT be named
"Discoveries" and must NOT route to `/discoveries`; this pattern search found nothing to add beyond
confirming the collision is real and adjacent in this exact list.

The findings page's nav entry: append it the SAME way, gated on its OWN availability predicate (not just
`DISCOVERY_ENABLED` — mirror `atlas_preview_available()`'s AND-of-flag-and-asset-readiness shape).

---

### `web/pages/work.py` and `web/pages/findings.py` — pagination/filter/off-loop counts

**Analog:** `web/pages/catalog_browse.py::_fetch_results_blocking` + `fetch_results` (255-333) —
**confirmed the correct model; CONTEXT.md's cited `_CatalogFacetWorker` is WRONG** (it is a PyQt6
`QThread` subclass in `genizah_app.py`, the DESKTOP app — there is no QThread in a NiceGUI/asyncio
process; grep confirms `_CatalogFacetWorker` appears ONLY in `genizah_app.py`).

```python
def _fetch_results_blocking(offset, domain, author, work, date_from, date_to, undated,
                             text_all, text_any, text_not, pgp_state, ed_state,
                             library_codes, library_mode):
    """Blocking browse fetch (runs in io_bound). ... All resolution runs here —
    off the async event loop — so the count + pagination apply to the FULL filtered set."""
    pgp_ids = ed_ids = None
    if pgp_state in ('has_pgp', 'no_pgp') or ed_state in ('has_edition', 'no_edition'):
        pgp_ids, ed_ids = _get_filter_sets()
    lib_sys_ids = None
    if library_codes:
        from web.state import state as _state
        resolved = resolve_library_sys_ids(library_codes, _state.meta_mgr)
        lib_sys_ids = resolved if resolved else None
    return fjms.get_browse_results(domain, author, work, offset, PAGE_SIZE, ...)

async def fetch_results():
    """Fetch paginated browse results for current filters."""
    offset = (current_page['value'] - 1) * PAGE_SIZE
    data = await run.io_bound(_fetch_results_blocking, offset, current_domain['value'], ...)
    return data
```
For `/work/{id}` and the findings page: build a `_fetch_work_page_blocking(...)`/`_fetch_findings_page_blocking(...)`
sync function carrying ALL filter args (tier, novelty, coverage, library — D-16), call it via
`await run.io_bound(...)` from an async `fetch_results()`-equivalent — never call
`shared.discovery_service`'s sync methods directly from the render path, and never invent a
desktop-style worker-thread class. `shared.discovery_service.DiscoveryService` already has its OWN
off-loop dispatch (`_run_off_loop`, using `run_in_executor` + `asyncio.wait`, see below) for its async
wrapper methods — for anything NOT already wrapped by an async `DiscoveryService` method (e.g. a new
D-17a count query), prefer adding it AS a new async method there over reaching for `run.io_bound` a
second time in the page module; `run.io_bound` is the right tool only for page-module-local blocking
work (like the library-sys_id resolution above), not for sidecar reads that belong in the service.

---

### `web/pages/browse_enrichment.py` — panel integration (MODIFY)

**Analog:** itself — the 4 existing lazily-loaded sections in `update_enrichment_sections()` (488-557)
are the established "fifth placeholder" pattern the sketch-findings skill explicitly names.

```python
def update_enrichment_sections(state: BrowseState, refs: BrowsePageRefs):
    """Update enrichment placeholder containers after Phase B completes."""
    pgp_container = refs.enrichment_refs.get('pgp_link_container')
    if pgp_container:
        pgp_container.clear()
        if state.pgp_metadata and state.pgp_metadata.get('pgp_url'):
            with pgp_container:
                ...
    version_container = refs.enrichment_refs.get('version_container')
    if version_container and (state.pgp_transcription or state.all_sources):
        version_container.clear()
        ...
    joins_container = refs.enrichment_refs.get('joins_container')
    if joins_container:
        joins_container.clear()
        ...
    bib_catalog_container = refs.enrichment_refs.get('bib_catalog_container')
    if bib_catalog_container:
        populate_bib_catalog_buttons(bib_catalog_container, state, state.current_page)
```
Add a **5th** `refs.enrichment_refs['discovery_panel_container']`-style placeholder, populated the SAME
way: `container.clear()` then a conditional `with container:` block. The panel's data source is
`load_enrichment()` (72-486) — follow its `asyncio.gather(fetch_pgp(), fetch_fjms(), fetch_crossref(),
fetch_browse_enrichment())` shape (309-311) by adding a `fetch_discovery_panel()` coroutine to the
gather, each wrapped in `await run.io_bound(_sync_fn)` / `try/except` returning a safe empty default on
failure (mirrors every existing `fetch_*` closure in this file, e.g. `fetch_crossref`, 144-163).

**The two staleness/liveness guards that are NON-NEGOTIABLE for the new section** (already present,
re-checked at 319, 452, 458, and again at 469-477):
```python
# Stale check (immediately after the asyncio.gather)
if generation != refs.load_generation['value']:
    return
...
# Re-check generation before committing final state (guards any awaits above)
if generation != refs.load_generation['value']:
    return
# Client-liveness guard — bail quietly when the page is DEFINITELY gone
_cc = refs.content_container
_page_gone = False
if _cc is not None:
    try:
        _page_gone = _cc.is_deleted or getattr(_cc.client, '_deleted', False)
    except (RuntimeError, AttributeError):
        _page_gone = True
if _page_gone:
    return
```
Per `reference_io_bound_safe_storage_trap` (memory) and the sketch-findings skill's own "obligation 2":
`run.io_bound` silently degrades `safe_user_*` to `{}`, and any `ensure_future`d awaited call must
re-check the generation token AFTER the await, not just before it.

---

### Offset-highlight renderer — **RESEARCH.md's proposed snippet is WRONG; follow the skill instead**

**Analog (consumer, correct):** `web/components/typography.py::render_line_numbered_html` (66-186) — the
renderer this new function MUST feed. It splits `highlight_html` on `\n` per grid row (132-142):
```python
if highlight_html is not None:
    normalized = (highlight_html.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n'))
    body_lines = normalized.split('\n')
...
for idx, line_content in enumerate(body_lines):
    rows.append(
        f'<span class="line-number-gutter" ...>{idx + 1}</span>'
        f'<div class="line-numbered-body-row" ...>{line_content}</div>'
    )
```
A single `<span class="discovery-match">` crossing one of these split points leaves an unclosed tag in
one grid row and NO highlight in the middle rows (confirmed by the skill: 72/148 rows highlight when done
correctly vs 1/148 when done naively).

**Anti-pattern, explicitly do NOT reuse** (`web/pages/browse.py::highlight_text`, 1577-1601):
```python
def highlight_text(text: str) -> str:
    escaped_text = html_module.escape(text)          # escapes FIRST
    if not state.highlight_terms:
        return escaped_text
    terms = state.highlight_terms.split()
    highlighted = escaped_text
    for term in terms:
        escaped_term = html_module.escape(term)
        pattern = re.compile(re.escape(escaped_term), re.IGNORECASE)
        highlighted = pattern.sub(f'<span class="highlight-term">{escaped_term}</span>', highlighted)
    return highlighted
```
Escapes the WHOLE string first, then regex-substitutes search TERMS on the escaped result — stored
OFFSETS cannot survive this (escaping `&`→`&amp;` shifts every later index by 4).

**CRITICAL CORRECTION — RESEARCH.md's own recommended function (136-RESEARCH.md, "Three Narrow Technical
Unknowns" §2) is ALSO wrong, and the planner must use the skill's algorithm instead:**

RESEARCH.md proposes:
```python
def render_offset_highlight(raw_text: str, start: int, end: int) -> str:
    """Slice RAW text at stored offsets BEFORE escaping (never after)."""
    before, middle, after = raw_text[:start], raw_text[start:end], raw_text[end:]
    return (html_module.escape(before) + f'<span class="discovery-match">{html_module.escape(middle)}</span>'
            + html_module.escape(after))
```
This is **factually wrong** per the `sketch-findings-genizahsearch` skill (validated against the real
deployed asset, 13 real manuscripts): **the stored `span_start`/`span_end` offsets index the NORMALIZED
Hebrew-letter stream (`norm_stream_letter_count`'s output — see `compute_page_coverage` above), NOT the
raw page text.** Slicing raw text directly at these offsets ends the highlight ~652 characters early on
a sampled manuscript (Moss. V,374: stored offsets 638-2374 correspond to RAW indices 809-3026). Evidence:
`span_end - span_start == matched_letters` in 11/14 sampled rows.

**The correct algorithm** (`.claude/skills/sketch-findings-genizahsearch/references/browse-integration-and-highlighting.md`,
JS shown there, must be ported to Python — same logic, `str.normalize('NFC')` + codepoint iteration):
```python
_HEB_MIN, _HEB_MAX = 0x05D0, 0x05EA
_FINAL_FOLD = {0x05DA: 0x05DB, 0x05DD: 0x05DE, 0x05DF: 0x05E0, 0x05E3: 0x05E4, 0x05E5: 0x05E6}

def _norm_map(raw: str):
    """map[i] = raw-string index of the i-th normalized Hebrew base letter."""
    nf = unicodedata.normalize('NFC', raw)
    idx_map = []
    for i, ch in enumerate(nf):
        code = _FINAL_FOLD.get(ord(ch), ord(ch))
        if _HEB_MIN <= code <= _HEB_MAX:
            idx_map.append(i)
    return nf, idx_map

def _span_to_raw(raw: str, a: int, b: int):
    """normalized [a, b) -> raw [rs, re) via the index map, or None if out of range."""
    nf, idx_map = _norm_map(raw)
    if not idx_map or a >= len(idx_map):
        return None
    rs = idx_map[a]
    re_ = idx_map[b - 1] + 1 if b - 1 < len(idx_map) else idx_map[-1] + 1
    return nf, rs, re_

def render_offset_highlight(raw_text: str, start: int, end: int) -> str:
    """Map normalized offsets -> raw indices, THEN clip the highlight span to
    each '\\n'-delimited line (render_line_numbered_html splits on '\\n'), so a
    span crossing a newline emits a properly closed/reopened <span> per line."""
    mapped = _span_to_raw(raw_text, start, end)
    if mapped is None:
        return html_module.escape(raw_text)
    nf, rs, re_ = mapped
    out = []
    pos = 0
    for line in nf.split('\n'):
        ls, le = pos, pos + len(line)
        s, e = max(rs, ls), min(re_, le)
        if s < e:
            out.append(
                html_module.escape(line[:s - ls])
                + f'<span class="discovery-match">{html_module.escape(line[s - ls:e - ls])}</span>'
                + html_module.escape(line[e - ls:])
            )
        else:
            out.append(html_module.escape(line))
        pos = le + 1
    return '\n'.join(out)
```
Also required (both from the skill, neither in RESEARCH.md):
- **Only the largest span is stored** for `tier_a` — the evidence view must either qualify the coverage
  label or say "showing one span of several" when `n_spans > 1` (Moss. V,374: label says 2,809 matched
  letters, only 1,736 are highlightable from the stored span).
- **The version selector invalidates offsets.** `handle_version_change` (`web/pages/browse.py:4207`)
  swaps the displayed text; the highlight must be DROPPED (not re-validated) on any source change, since
  offsets belong to one HTR snapshot only. D-12's snapshot-hash drift check covers drift-over-time; it
  does NOT cover a live source switch — that is a second, separate invalidation trigger this function's
  caller must implement.
- **Search-term highlighting and discovery spans compete for the same render parameter** — `browse.py`
  computes `highlight_html = highlight_text(text) if state.highlight_terms else None` at the single call
  site feeding `render_line_numbered_html`. One renderer must emit BOTH marks (term marks nested inside
  the discovery-match span, or vice versa) — two separate renderer calls cannot share that one parameter
  slot.

**Where this function lives:** it is pure (no NiceGUI/DB dependency) — put it in `shared/` (e.g.
`shared/discovery_render.py`) so both the panel and the evidence view can import it without a `web/pages`
circular import, mirroring how `web/components/typography.py`'s `render_line_numbered_html` is itself a
dependency-free pure function.

---

### `web/pages/help.py` — methods-section rewrite (D-06a) (MODIFY)

**Analog:** itself — `_render_confidence_section`/`_render_one_band`/`_CONFIDENCE_FIELD_LABELS` (58-260)
is the exact section to rewrite IN PLACE; the pattern to follow is REMOVAL of fields, not addition of a
new section.

**Fields currently rendered that D-06a PROHIBITS going forward** (`_render_one_band`, 245-251):
```python
estimate = _precision_copy_safe(row, lang)          # <-- renders a percentage + CI via format_precision_copy
weighting = row.get('weighting')
if weighting:
    estimate = f"{estimate} ({weighting})"
ui.label(f"{fl['estimate'][lk]}: {estimate}").style(text_style)
```
and the strata/CI-bearing collection-scope block (`_render_confidence_section`, 182-188):
```python
collection_row = (precision or {}).get('collection') or {}
ui.label(f"{_CONFIDENCE_COLLECTION_LABEL[lk]}: {_precision_copy_safe(collection_row, lang)}").style(text_style)
```
Both must be REMOVED (D-06a: "no percentage, no CI, no strata table"). Everything else in
`_render_one_band` is qualitative and D-06a-compliant AS-IS and should be KEPT unchanged: population
(224-225, from `_band_population`/`band_counts` — a real count, not a percentage), unit-of-measurement
prose (228), the THREE distinct sample-size numbers (230-239, draw/determinate/successes — none of these
is a percentage), the four CERT-01 registry fields — measurement_date/grader/audit_status/report_id
(253-260, placeholder-safe via `.get()`).

**The word-gate discipline to keep exercising** (already present, `_render_confidence_section`'s callers
are tested by the render-smoke suite's no-"certified" assertion) — any NEW qualitative wording added here
must be run through the SAME `tests/render_smoke/test_help_methods_render_smoke.py`-style word-gate
(`assert 'certified' not in section.lower()` plus the HE equivalents) before considering this task done.

**Field-sourcing discipline to preserve:** every field is read via `.get()` with an explicit placeholder
(`_CONFIDENCE_PLACEHOLDERS`, 73-77) — NEVER fabricate a value when a registry field is absent. This
pattern must extend to any NEW qualitative field D-06a's "grading happened / the unit / sample size /
grader / date / method / audit state / report id" list requires that isn't already one of the 4 existing
registry fields (all 8 items D-06a lists already have a field here except possibly an explicit
"grading happened: yes/no" boolean-as-prose — check against `band_measurement_status`'s existing states
in `shared/discovery_band_labels.py` (232-264) before adding a new field; `not_measured` /
`measured_pass` / `measured_fail` / `insufficient_evidence` / `measured_audit_pending` may already say
this qualitatively).

---

### `shared/discovery_service.py` — envelope, D-17a display fields, count query (MODIFY)

**Analog:** itself — every existing sync/async method pair follows one fixed shape; the envelope and new
count query are ADDITIONS to this same shape, not a new module.

**The graceful-absent / never-raise shape to extend, NOT replace** (`get_claims_for_page`, 598-643):
```python
def get_claims_for_page(self, page_id, page=1, page_size=None, include_review=False) -> List[Dict]:
    if not self.is_available():
        return []
    conn = self._get_conn()
    if conn is None:
        return []
    ...
    try:
        cur = conn.execute(f"""SELECT ... FROM discovery_claim dc JOIN discovery_evidence de ...""", (...))
        return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logger.error("DiscoveryService.get_claims_for_page error for %s: %s", page_id, e)
        return []
```
D-13's `{status, items, total}` envelope is a NEW return shape layered on top of this — the safest change
is a NEW wrapper method (e.g. `get_claims_for_page_enveloped`) that calls the existing sync method and
classifies the THREE distinguishable outcomes (`is_available()` False → `status='unavailable'`; the
`_run_off_loop` timeout path raising `DiscoveryUnavailable` → `status='timeout'`; a real empty list →
`status='ok', items=[], total=0'`) rather than rewriting the existing method's return type in place (which
would break every existing caller/test of the current `[]`-returning shape).

**The off-loop dispatch to reuse for anything new** (`_run_off_loop`, 887-919):
```python
async def _run_off_loop(self, sync_fn, *args, timeout: float, heavy: bool = False):
    loop = asyncio.get_event_loop()
    _release = None
    if heavy:
        _release = await self._acquire_heavy_slot()
    try:
        fut = loop.run_in_executor(None, sync_fn, *args)
        if _release is not None:
            fut.add_done_callback(lambda _f, _r=_release: _r())
            _release = None
        done, pending = await asyncio.wait({fut}, timeout=timeout)
        if fut in pending:
            logger.warning("DiscoveryService query timed out after %ss ...")
            raise DiscoveryUnavailable("temporarily unavailable")
        return fut.result()
    finally:
        if _release is not None:
            _release()
```
**Never** `asyncio.wait_for` over `run_in_executor` (the docstring's own load-bearing comment: executor
threads are not cancellable). A new D-17a count-query async method should follow this EXACT shape,
`heavy=True` if it scans a large table (mirrors `get_work_witnesses_async`, 1030-1043).

**D-17a display fields — the CTE to extend, not replace** (`_WORK_WITNESSES_RANKED_CTE_SQL`, 209-226):
```python
_WORK_WITNESSES_RANKED_CTE_SQL = f"""
  SELECT
    COALESCE(wum.unit_id, 'sys:' || de.sys_id) AS unit_key,
    wum.unit_id AS unit_id, dc.page_id AS page_id, dc.work_id AS work_id,
    dc.claim_id AS claim_id, dc.claim_type AS claim_type, de.sys_id AS sys_id,
    de.evidence_source AS evidence_source, de.confidence_band AS confidence_band,
    {_BAND_RANK_CASE_SQL} AS band_rank
  FROM discovery_claim dc
  JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
  LEFT JOIN witness_unit_members wum ON wum.sys_id = de.sys_id
  WHERE dc.work_id = ? AND dc.claim_type IN ('direct_witness', 'quotes_this_work')
"""
```
This CTE has NO shelfmark/library/coverage/novelty column today (confirmed exact, RESEARCH.md). D-17a's
new "manuscript-display lookup" sidecar table (normalized library + shelfmark sort keys, built at bake
time) must be LEFT JOINed in here — same style as the existing `LEFT JOIN witness_unit_members` — and the
new columns added to BOTH the CTE's SELECT list and the final projected dict in `get_work_witnesses`
(831-844), which currently hardcodes exactly 8 output keys (`work_id`, `unit_id`,
`representative_sys_id`, `representative_page_id`, `representative_claim_id`, `claim_type`,
`evidence_source`, `confidence_band`, `member_sys_ids`) — extend this dict literal, do not build a
second parallel projection.

**The count query (D-17a)** must use "identical grouped predicates" to the paginated query — i.e. wrap
the SAME `WITH ranked AS (...), unit_best AS (...) WHERE rn = 1{where_extra}` shape in
`SELECT COUNT(*) FROM (...)` rather than hand-writing a second, potentially-drifted WHERE clause. This
mirrors how `_project_work_witnesses` (239-355, the pure-Python reference) and the SQL projection
(797-844) are already kept lockstep via shared tie-break ordering — the count query is a third view onto
the SAME predicate, not an independent one.

**D-13a/b grouping (collapse by canonical_work_id; identical-span-group extraction)** has NO existing
service-layer analog — these are NEW pure functions. Model them on `_project_work_witnesses` (239-355):
a pure, DB-free function taking an iterable of row-dicts plus a grouping key, returning a grouped/filtered
list, directly unit-testable with fabricated data (no fixture DB required) — that is the established
"pure projection helper next to its SQL twin" shape this file already uses twice
(`_project_work_witnesses` / `_WORK_WITNESSES_RANKED_CTE_SQL`).

---

### `tests/render_smoke/test_panel_render_smoke.py` / `test_work_page_render_smoke.py` / `test_findings_page_render_smoke.py` (NEW)

**Analog:** `tests/render_smoke/test_help_methods_render_smoke.py` (full file, 424 lines) — copy this
file's STRUCTURE exactly, only the page/fixtures differ.

**The async-user-context harness to copy verbatim in shape** (125-178):
```python
@asynccontextmanager
async def _help_user_context(lang='en', discovery_on=True, noindex=True):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-methods-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(patch('web.pages.help.discovery_available', return_value=discovery_on))
            stack.enter_context(patch('web.main.discovery_methods_noindex', return_value=noindex))
            stack.enter_context(patch('web.main.get_all_band_precision', new=AsyncMock(return_value=FAKE_PRECISION)))
            stack.enter_context(patch('web.main.get_band_claim_counts', new=AsyncMock(return_value=FAKE_COUNTS)))
            stack.enter_context(patch('web.main._resolve_ui_language', return_value=lang))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app), base_url='http://test') as client:
                        yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
```
For the panel test: patch the ACTUAL call sites in `web.pages.browse_enrichment` (the module-level
functions/`DiscoveryService` calls the panel uses), not `web.discovery`'s module-level singleton directly
(mirrors the Codex #11 lesson already learned here — patch where the CALLER imports it, since `/help`'s
own gate was found to live in `web.pages.help`, not `web.discovery`).

**The scoped-text extraction helper to copy** (219-234, adapt the marker class name):
```python
def _confidence_section_text(user) -> str:
    from web.pages.help import _CONFIDENCE_SECTION_CLASS
    parts = []
    with user._client:
        for e in user._client.elements.values():
            classes = getattr(e, '_classes', None) or []
            if _CONFIDENCE_SECTION_CLASS in classes:
                for d in e.descendants(include_self=True):
                    for attr in ('text', 'content'):
                        v = getattr(d, attr, None)
                        if isinstance(v, str) and v:
                            parts.append(v)
    return '\n'.join(parts)
```
Give the panel/work-page/findings-page each their own unique marker CSS class (mirrors
`_CONFIDENCE_SECTION_CLASS`) so the no-percentage/no-CI/no-"certified"/no-"Expert-reviewed" word-gates
(Success Criterion 7 in VALIDATION.md) can scope their assertions to just that surface, exactly as this
file scopes to avoid tripping on the Joins-Lab's legitimate "מאושר" usage elsewhere on `/help`.

**The three-state flag-off/absent/timeout test shape (Success Criterion 8)** should follow
`test_help_flag_off_section_absent_and_indexed` (378-398) for the "absent, zero errors" case, but needs a
GENUINELY NEW third state this file's precedent does not cover: a query-timeout that must show a visible
retry state, not silent absence (`web/discovery_assets.py`'s binary ready/absent model has no equivalent —
this is RESEARCH.md's own "the one truly NEW test class this phase needs").

---

### `tests/test_vis02_positive_control.py` (NEW)

**Analog:** `scripts/check_atlas_masking.py::_run_self_test` (1292-1322, confirmed unread-in-full this
session but line range matches CONTEXT.md/RESEARCH.md citation) and its CLI surface:
```
python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery.db
python scripts/check_atlas_masking.py --scan-asset discovery_data/discovery.db --scan-repo --strict
```
`_run_self_test` proves the MATCHING ALGORITHM is encoding-robust against a synthetic throwaway token —
it does NOT exercise the real `.masking_patterns` file or prove a real leak in the ACTUAL public
projection would be caught. The new test must: build a throwaway copy of the public projection DB,
`INSERT` one row copied verbatim from the private asset that legitimately carries restricted (msource)
origin, invoke the SAME `--scan-sqlite`/`--scan-asset --strict` CLI (or its Python entry point directly)
against that seeded copy with the REAL `MASKING_SCAN_PATTERNS_FILE`, and assert **nonzero exit**. This is
a NEW integration test — `_run_self_test` supplies the CLI/self-test convention to mirror, not the test
content itself.

---

### `tests/test_discovery_schema.py` / `tests/test_discovery_build.py` extensions (D-02a both branches)

**Analog:** itself — `synthetic_discovery_dataset()` and the neighboring fixture tests
(`test_ingest_tier_a_shadowed_filter_and_largest_span`, 855; `test_propagated_corroborated_vs_weak_predicate`,
890) already establish the "build a small in-memory/tmp_path SQLite fixture, run the real ingestion
function, assert on the output rows" pattern. New tests for D-02a's PASS/FAIL branches should call
`_validate_precision_spec` (or the widened version) directly with a hand-built `rows` list — no DB fixture
needed for this one, since `_validate_precision_spec` is a pure function over `List[Dict]` — mirroring how
`_frozen_real_band_precision_rows`/`_validate_precision_spec` are already unit-tested as pure functions
elsewhere in this suite (per the file's own existing test density: 84 `test_` functions).

---

## Shared Patterns

### Fail-closed asset loading (any new asset/availability consumer)
**Source:** `web/atlas_assets.py::load_atlas_state`/`atlas_preview_available` (mirrored 1:1 by
`web/discovery_assets.py::load_discovery_state`/`discovery_available`, which is ALREADY BUILT — no new
loader is needed this phase, only new CONSUMERS of `discovery_available()`).
**Apply to:** every new page-route gate (`/work/{id}`, findings page) and every new nav-link gate.
```python
def atlas_preview_available() -> bool:
    return bool(ATLAS_PREVIEW_ENABLED and _state.ready)
```
One predicate, read live (never captured at import), gates the page route AND the data routes AND the
nav link — so they can never disagree.

### Off-event-loop dispatch (never `asyncio.wait_for` over `run_in_executor`)
**Source:** `shared/discovery_service.py::_run_off_loop` (887-919) / `web/pages/catalog_browse.py`'s
`await run.io_bound(...)` (280-333).
**Apply to:** any new blocking DB/CPU work reachable from an `async def` page handler.

### Generation-token staleness guard + `page_client` binding
**Source:** `web/pages/browse_enrichment.py` (319, 452, 458, 469-477).
**Apply to:** the panel's lazy load, any `/work/{id}` on-demand expansion (`get_work_witnesses`'s pagination
click handler), per `reference_io_bound_safe_storage_trap`.

### Band/review rendering exclusively through `shared/discovery_band_labels.py`
**Source:** `shared/discovery_band_labels.py::band_label`/`review_overlay`/`serialize_banded_claim`
(entire file).
**Apply to:** every new surface that displays a band or a review status — never hardcode a competing
string; `serialize_banded_claim` RAISES rather than silently omitting band fields (SC#1), and this
invariant must hold for the panel, the work page, and the findings page alike.

### Versioned-artifact discipline ("tunable only by re-versioning the doc")
**Source:** `docs/specs/discovery-budgets.md` (header: "tunable ONLY by versioning this artifact").
**Apply to:** `136-REBUILD-PRESERVATION-EXPECTED.json`, any new findings-page budget entry, the new
build-time rebuild-preservation budget section — never hardcode a competing cap in code.

### Masking discipline — never echo raw values, never name the restricted corpus
**Source:** `scripts/build_discovery_sidecar.py::_validate_precision_spec`'s "masking (Codex R4/R5)"
comments (3918-3924, 3991-4027) + `scripts/check_atlas_masking.py`.
**Apply to:** every new violation/error message in `verify_rebuild_preservation.py`, every new committed
artifact (per RESEARCH.md Pitfall 6) — M-source/R-source codenames only, opaque `w000xxx` ids/hashes/
counts only, run `check_atlas_masking.py --scan-repo` after creating each new artifact type.

---

## No Analog Found

| File / Mechanism | Role | Data Flow | Reason |
|---|---|---|---|
| `scripts/verify_rebuild_preservation.py`'s core diff loop (per-table streamed content hash, old vs new, with a column allowlist) | utility | batch | No script in the repo performs an exact old/new allowlisted diff between two SQLite files; `verify_discovery_sidecar.py`/`cert01_frame.py` supply CLI shape and hash-recipe reuse only. RESEARCH.md itself calls this "the highest-risk, least-precedented item." Budget real design time. |
| `scripts/build_discovery_sidecar.py`'s VIS-01 closed-graph public projection function | ETL | batch, transform | No existing script derives a filtered/projected second SQLite DB with FK-closure verification from a source DB. `check_integrity_and_fk`'s `PRAGMA foreign_key_check` is reusable as the POST-projection verification step only. Codex flagged this HIGH (F-05); genuinely new in this codebase. |
| Novelty tri-state computation logic itself (the funnel + LLM gate) | ETL (producer-side) | batch, event-driven | The only prior implementation is gitignored (`same_work_spike/probe/scripts/discovery_identified_gate.py` / `title_gate_llm.py`) — not accessible as an in-repo pattern to copy. The pinned-build-input-artifact HANDOFF shape (`v2_canonical_merges.build.json` convention) is reusable for the plumbing; the computation is not. |
| D-13d granularity-separation display-time heuristic (same-work-different-granularity vs. different-works-same-passage) | utility (pure fn) | transform | RESEARCH.md's own "Three Narrow Technical Unknowns" §1 confirms no existing signal/heuristic in the codebase distinguishes these two cases; recommends a NEW title-containment heuristic plus a NEW owner-reviewed allowlist artifact (mirroring the canonical-merges census, but itself unbuilt). Do not lock a threshold in planning — gate 1 per CONTEXT.md's own deferral. |

---

## Metadata

**Analog search scope:** `web/main.py`, `web/atlas_assets.py`, `web/discovery_assets.py`, `web/discovery.py`,
`shared/discovery_service.py`, `shared/discovery_band_labels.py`, `web/pages/browse_enrichment.py`,
`web/pages/catalog_browse.py`, `web/pages/help.py`, `web/pages/browse.py` (highlight_text region),
`web/components/typography.py`, `scripts/build_discovery_sidecar.py` (targeted regions: 525-575, 805-855,
1300-1350, 1690-1780, 2530-2730, 3860-4040, 4390-4435), `scripts/verify_discovery_sidecar.py` (1-80,
300-400, 530-590, 1030-1145), `scripts/cert01_frame.py` (270-325), `scripts/verify_cert01_grading.py`
(190-230), `scripts/check_atlas_masking.py` (CLI region), `scripts/bench_discovery.py` (1-90, 260-390),
`tests/render_smoke/test_help_methods_render_smoke.py` (full file), `tests/test_discovery_build.py`
(test-name grep), `docs/specs/discovery-budgets.md` (full file), and both files of the
`sketch-findings-genizahsearch` skill (full).
**Files scanned:** ~24 files read/grepped directly this session, plus every citation cross-checked against
CONTEXT.md/RESEARCH.md/VALIDATION.md's own prior verification.
**Pattern extraction date:** 2026-07-31
