# Phase 135: Precision Certificate & Confidence Bands - Pattern Map

**Mapped:** 2026-07-23
**Files analyzed:** 18 (6 Track A, 12 Track B)
**Analogs found:** 18 / 18 (every file has a strong same-repo or same-tree analog; several files ARE their own analog — this phase extends an existing Phase-134 module in place rather than creating a parallel one)

**Masking note:** every excerpt below was verified masking-clean before inclusion (no M-source title, no restricted codename, functional description only for the census/date-table artifacts).

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `shared/discovery_band_labels.py` (NEW) | service/values module | transform (enum→display copy) | `scripts/discovery_ids.py` (frozen-enum module shape) + `web/discovery_assets.py::_CONFIDENCE_BANDS_BY_SOURCE` (inlined dual-purpose spot-check dict) | exact (shape) |
| `tests/test_discovery_band_labels.py` (NEW) | test | unit | `tests/test_discovery_ids.py` (golden/totality tests) + `tests/test_discovery_bands.py` (fixture-driven "no code change" test) | exact |
| `shared/discovery_service.py` (MODIFY — add `band_precision` read) | service | request-response (async chokepoint) | itself: `get_version` / `get_version_async` (same file, simplest existing read-one-row pattern) | exact |
| `web/pages/help.py` (MODIFY — new section) | component (SSR page section) | request-response | itself: the `help-api` / `help-my-library` sections + TOC + `WEB_PUZZLE_ENABLED` gating (same file) | exact |
| `web/main.py` (MODIFY — `/help` route noindex) | route | request-response | itself: `/atlas` route's `noindex=True` + `page_meta()` (same file) | exact |
| `tests/render_smoke/test_help_methods_render_smoke.py` (NEW) | test (render-smoke) | request-response | `tests/render_smoke/test_atlas_render_smoke.py` (full file — direct structural template) | exact |
| `docs/specs/discovery-v2-bake-plan.md` (REWRITE) | config/spec doc | batch (offline bake plan) | itself (current DRAFT — structure to keep, content to replace) + `docs/specs/discovery-band-labels-v1.md` (versioning discipline) | exact |
| `docs/specs/discovery-frames-v2.md` (NEW) | config/spec doc | batch | `docs/specs/discovery-frames.md` (v1 — section-by-section template) | exact |
| `docs/specs/discovery-sidecar-schema-v1.md` (dated amendment) | config/spec doc | batch | itself (§"Frozen Enum Vocabularies" + closing "dated amendment" clause) | exact |
| `docs/specs/discovery-band-labels-v1.md` (dated amendment §4) | config/spec doc | batch | itself (already versioned once — 2026-07-23 revision history at top of file) | exact |
| `scripts/discovery_ids.py` (MODIFY — enum rename + `routing_reason` amendment) | utility (frozen id/enum module) | transform | itself: `ROUTING_REASONS` frozenset construction + `CONFIDENCE_BAND_EXPERT_VERIFIED` constant | exact |
| `scripts/build_discovery_sidecar.py` (MODIFY — 4 v2 build changes) | migration/batch-build script | batch (ETL) | itself: DDL block, `_insert_works_real`, `finalize_build` order-of-operations | exact |
| `scripts/verify_discovery_sidecar.py` (MODIFY — new invariants) | utility (release verifier) | batch (validation) | itself: `check_evidence_combinations` / `check_display_pointer_ownership` / `check_band_precision` | exact |
| `scripts/check_atlas_masking.py` (MODIFY — register M-source date-table vocab) | utility (masking gate) | batch (scan) | itself: `load_patterns()` / `MASKING_SCAN_PATTERNS_FILE` env-sourced pattern registration | exact |
| `tests/test_discovery_ids.py` (MODIFY — extend golden digests) | test | unit | itself (existing golden-digest convention) | exact |
| `tests/test_discovery_v2_bake.py` (NEW, or extend `tests/test_discovery_build.py`) | test | unit (fixture ETL) | `tests/test_discovery_build.py` (fixture-DB + `finalize_build` invocation harness) | exact |
| CERT-01 written protocol doc (NEW, gitignored `same_work_spike/probe/` tree — exact filename is a planner/Track-B decision) | config/spec doc (research protocol) | batch | `same_work_spike/probe/results/PLAN-e1-round2.md` (section-heading template) + `PLAN-e1-round3-canon.md` | exact |
| CERT-01 harness adapter script (NEW, gitignored `same_work_spike/probe/scripts/`, only if needed — see Open Question) | utility (offline research script) | batch | `same_work_spike/probe/scripts/e1_r2_confirm.py` / `e1_band_frame.py` (frame-freeze + deck-pointing scripts) | exact |

## Pattern Assignments

### `shared/discovery_band_labels.py` (NEW — values module, BAND-01/BAND-02/CERT-02)

**Analog 1:** `scripts/discovery_ids.py` (full file read, 413 lines) — the frozen-enum module shape to import from, never re-declare.

**Frozen enum this module must stay TOTAL over** (`scripts/discovery_ids.py` lines 63-83):
```python
CONFIDENCE_BAND_EXPERT_VERIFIED = "expert_verified"
CONFIDENCE_BAND_TIER_A = "tier_a"
CONFIDENCE_BAND_SCREENING_RB = "screening_rb"
CONFIDENCE_BAND_SCREENING_CANON = "screening_canon"
CONFIDENCE_BAND_CORROBORATED = "corroborated"
CONFIDENCE_BAND_WEAK = "weak"
CONFIDENCE_BAND_NOT_EVALUATED = "not_evaluated"

CONFIDENCE_BANDS_BY_SOURCE: Dict[str, frozenset] = {
    EVIDENCE_SOURCE_TRACK1_DIRECT: frozenset({
        CONFIDENCE_BAND_EXPERT_VERIFIED, CONFIDENCE_BAND_TIER_A,
        CONFIDENCE_BAND_SCREENING_RB, CONFIDENCE_BAND_SCREENING_CANON,
    }),
    EVIDENCE_SOURCE_PROPAGATED: frozenset({
        CONFIDENCE_BAND_CORROBORATED, CONFIDENCE_BAND_WEAK, CONFIDENCE_BAND_NOT_EVALUATED,
    }),
}
```

**Existing precedent for "inline a lightweight defensive copy of the enum rather than importing the build-script tree"** — `web/discovery_assets.py` lines 118-130 (read in full):
```python
# Frozen enum vocab spot-check ... Deliberately
# inlined as plain string constants here rather than importing
# scripts/discovery_ids.py -- this module must stay a lightweight web/
# runtime dependency, not couple to the offline-build script tree.
_CLAIM_TYPES = frozenset({"direct_witness", "quotes_this_work", "shared_text"})
_CONFIDENCE_BANDS_BY_SOURCE: Dict[str, frozenset] = {
    "track1_direct": frozenset({"expert_verified", "tier_a", "screening_rb", "screening_canon"}),
    "propagated": frozenset({"corroborated", "weak", "not_evaluated"}),
}
```
`shared/discovery_service.py` makes the SAME "inline, don't import `scripts/`" decision for `_BAND_RANK_ORDER` (lines 152-160, docstring at 145-150: *"Inlined here (rather than importing scripts/discovery_ids.py) to keep this runtime module decoupled from the offline-build script tree — mirrors the same decision already made in web/discovery_assets.py"*). **Recommendation for the planner:** `shared/discovery_band_labels.py` is itself part of `shared/`, so it MAY import `scripts.discovery_ids` directly (both are dev/offline-adjacent, and CONTEXT.md's own "Claude's Discretion" explicitly wants the drift-guard test to *check against* `scripts/discovery_ids.CONFIDENCE_BANDS_BY_SOURCE`, i.e. an import-based check, not a third inlined copy) — cite this decision precedent but do not blindly re-inline a fourth copy of the enum.

**Word-gate + label source of truth** — `docs/specs/discovery-band-labels-v1.md` §2 (lines 47-73, full table read) is the literal, hand-typed EN/HE string source; §5 (lines 187-203) is the exact 7-file lockstep list this module joins as an 8th consumer:
```
- scripts/discovery_ids.py (frozen enum vocab)
- scripts/build_discovery_sidecar.py (band assignment + band_precision rows)
- scripts/verify_discovery_sidecar.py (enum invariant + any hardcoded literal)
- web/discovery_assets.py (_CONFIDENCE_BANDS_BY_SOURCE spot-check)
- shared/discovery_service.py (_BAND_RANK_ORDER + _BAND_RANK_CASE_SQL)
- docs/specs/discovery-sidecar-schema-v1.md + discovery-frames.md (frozen vocab + C-7)
- the label map in §2 of this file
```

**Precision-presentation rule to hand-code** (band-labels-v1.md §3 rule 1 + the CERT-02 "no bare number" test, RESEARCH.md illustrative shape — cite directly, not re-derive):
```python
# tier_a: band_precision.precision IS NULL until CERT-01 lands.
def format_precision_copy(row):
    if row["precision"] is None:
        return "precision not yet measured"  # + HE equivalent
    return f"estimated band precision {row['precision']:.1%} [{row['ci_low']:.1%}, {row['ci_high']:.1%}]"
```

---

### `tests/test_discovery_band_labels.py` (NEW — drift guard, BAND-01/02/03/04, CERT-02)

**Analog 1:** `tests/test_discovery_ids.py` (full file read, 360 lines) — golden-digest + totality-over-enum test shape. Directly reusable idiom (lines 33-48, `test_valid_evidence_combinations_over_fixture` style — "every row in the frozen enum has a home in my lookup table"):
```python
def test_validate_source_corpus_code_passes_frozen_codes():
    for code in ("sefaria", "ja", "msource"):
        assert d.validate_source_corpus_code(code) == code
```
Apply the SAME shape as the drift guard:
```python
def test_every_frozen_band_has_a_label():
    for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items():
        for band in bands:
            key = (source, _canon_band_key(band))
            assert key in BAND_LABELS, f"missing label for {key}"

def test_no_orphan_labels():
    frozen_keys = {(source, _canon_band_key(band))
                   for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items() for band in bands}
    assert set(BAND_LABELS.keys()) <= frozen_keys
```

**Analog 2:** `tests/test_discovery_bands.py` (full file read, 132 lines) — the fixture-DB-driven "flip data, same code, output changes" idiom used for BAND-02's no-code-change requirement (lines 12-14, 33-48 pattern):
```python
FIXTURE_DB = Path(__file__).resolve().parent / "fixtures" / "discovery" / "discovery-v1-fixture.db"
conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
rows = conn.execute("SELECT evidence_kind, evidence_source, confidence_band FROM discovery_evidence").fetchall()
```
For a WRITE-mutation variant (BAND-02 needs to mutate `band_precision.precision`/`ci_low`/`ci_high` on a COPY of the fixture and re-render), mirror `tests/test_discovery_ids.py::_copy_fixture` / `_connect_rw` (lines 17-26 of `tests/test_discovery_bands.py`):
```python
def _copy_fixture(tmp_path, name="corrupt.db"):
    dest = tmp_path / name
    shutil.copyfile(FIXTURE_DB, dest)
    return dest

def _connect_rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn
```
Fixture location to reuse/extend: `tests/fixtures/discovery/discovery-v1-fixture.db` (+ `discovery-v1-fixture-expected.json`, `manifest.json`).

---

### `shared/discovery_service.py` (MODIFY — add a `band_precision` read helper)

**Analog:** itself — `get_version` / `get_version_async` (lines 481-493 and 867-868, the simplest existing "read one/a few rows off the sidecar" sync+async pair; read directly):
```python
def get_version(self) -> Optional[str]:
    if not self.is_available():
        return None
    conn = self._get_conn()
    if conn is None:
        return None
    try:
        cur = conn.execute("SELECT value FROM meta WHERE key = 'sidecar_version'")
        row = cur.fetchone()
        return row["value"] if row else None
    except Exception as e:
        logger.error("DiscoveryService.get_version error: %s", e)
        return None
```
```python
async def get_version_async(self) -> Optional[str]:
    return await self._run_off_loop(self.get_version, timeout=self._browse_timeout())
```
A new `get_band_precision(evidence_source, confidence_band)` / `get_band_precision_async(...)` should follow this EXACT shape: `is_available()` guard → `_get_conn()` guard → `try/except Exception: logger.error(...); return None/[]` (never raise) → dispatch via `self._run_off_loop(..., timeout=self._browse_timeout())` (this is a cheap browse-tier read, NOT a `heavy=True` call like `get_work_witnesses_async`). `_browse_timeout()` is defined at lines 817-818. No caching is needed (values-module calls are cheap, low-cardinality — 7 rows total) so do NOT route this through `_browse_cached_call` (that LRU is version-keyed for per-page browse reads, not needed here).

**Note for the planner (schema gap, not yet closed by this repo):** as of this read, `shared/discovery_service.py` has NO existing `band_precision` reader at all — the table has existed since the v1 bake (`scripts/build_discovery_sidecar.py`'s `band_precision` INSERT, DDL at lines 181-197 of that file) but nothing in the service layer reads it yet. This is new code, not a rename — plan it as a Wave-0 task.

---

### `web/pages/help.py` (MODIFY — new bilingual "Confidence Bands & Methods" section, BAND-05)

**Analog:** itself — the file already has 16 bilingual sections with a proven TOC + anchor + flag-gating convention; the MOST RECENT section added (`help-my-library`, Phase 95) and the flag-gated section (`WEB_PUZZLE_ENABLED`) are the two templates to combine.

**TOC + flag-gating** (lines 44-70, EN; mirrored at 710-732 for HE):
```python
toc_items = [
    ('intro', 'Introduction: How it Works'),
    # ...
    ('export', 'Exporting Data'),
    ('api', 'Public API & AI Tools'),
    ('my-library', 'My Library — Local Documents'),
]
if WEB_PUZZLE_ENABLED:
    toc_items.insert(9, ('puzzle', 'Fragment Puzzle'))
    toc_items.insert(10, ('community-publish', 'Community Publishing'))
for anchor, title in toc_items:
    if not WEB_PUZZLE_ENABLED and anchor in {'puzzle', 'community-publish'}:
        continue
    ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline')
```
Apply the SAME shape, gated on `discovery_available()` (from `web.discovery_assets`) instead of `WEB_PUZZLE_ENABLED` — RESEARCH.md's Pattern 2 already sketches this exactly:
```python
if discovery_available():
    toc_items.append(('confidence', 'Confidence Bands & Methods'))
for anchor, title in toc_items:
    if not discovery_available() and anchor == 'confidence':
        continue
    ...
```

**Section body shape** — `help-api` section, full read (lines 649-675):
```python
with ui.card().classes('w-full p-6'):
    ui.element('a').props(f'name="help-api"')
    with ui.row().classes('items-center gap-3 mb-4'):
        ui.icon('api').classes('text-2xl text-primary')
        h2('Public API & AI Tools', classes='text-xl font-bold', style='color: var(--text-primary);')
    ui.markdown('''
...markdown body...
    ''').style('color: var(--text-secondary);')
```
`help-my-library` (lines 677-688) shows the terser, single-card, no-sub-headings variant (closer in size to what BAND-05 needs per band, ×7). D-10's per-band anchor requirement (`Help#confidence-tier_a`) needs finer-grained anchors INSIDE the one `help-confidence` card — follow the SAME `ui.element('a').props('name="help-confidence-<band_key>"')` convention at a nested level (h3-per-band inside the one outer `ui.card()`), there is no existing multi-anchor-per-card precedent in this file to copy verbatim, but the anchor mechanism itself (`ui.element('a').props(f'name="..."')`) is proven 17× already.

**Import to add:** `from web.discovery_assets import discovery_available` (mirrors `from web.feature_flags import WEB_PUZZLE_ENABLED` at line 11).

---

### `web/main.py` (MODIFY — `/help` route, conditional noindex)

**Analog:** itself — the `/atlas` route's conditional-noindex-until-REL-01 pattern (lines 2137-2153, full read) is the EXACT precedent for BAND-05's "noindex until REL-01" requirement, per Pitfall 9's recommendation (whole-route noindex, since NiceGUI has no per-section robots directive):
```python
@ui.page('/atlas', title='The Visual Genizah Atlas | ...')
def atlas_page_route():
    """... noindex until the REL-01 gate (D-16)."""
    safe_user_set('current_page', '/atlas')
    ui.add_head_html(page_meta(
        '/atlas',
        title='...',
        description='...',
        noindex=True,
    ))
```
`page_meta()` signature (lines 920-933, full read):
```python
def page_meta(
    path: str = '/', title: str = _DEFAULT_TITLE, description: str = _DEFAULT_DESCRIPTION,
    og_type: str = 'website', noindex: bool = False, needs_iiif: bool = False,
) -> str:
    ...
    robots = '<meta name="robots" content="noindex, noarchive, follow">\n' if noindex else ''
```
Current `/help` route call site (lines 2245-2256, full read — has NO `noindex` kwarg today, defaults `False`):
```python
@ui.page('/help', title='Help | Dicta Genizah Search')
def help_page_route():
    safe_user_set('current_page', '/help')
    ui.add_head_html(page_meta(
        '/help',
        title='Help | Dicta Genizah Search',
        description='User guide for Dicta Genizah Search: full-text search, Responsa syntax, catalog browsing, manuscript viewer, fragment puzzle, and research tools.',
    ))
```
Change to `noindex=discovery_available()` (per RESEARCH.md Pitfall 9's recommendation — the WHOLE `/help` page goes `noindex` only while the discovery section is actually rendering; reverts to indexed the instant `DISCOVERY_ENABLED` is off, which it is by default through Phase 138). **Flag this as an Open Question for owner/planner confirmation**, per RESEARCH.md Assumption A1 — it is a genuinely new mechanism (whole-page noindex driven by a sub-section's availability), not a verbatim copy.

---

### `tests/render_smoke/test_help_methods_render_smoke.py` (NEW, BAND-05)

**Analog:** `tests/render_smoke/test_atlas_render_smoke.py` (full file, 231 lines, read directly) — direct structural template: NiceGUI `User` over `httpx.ASGITransport(core.app)`, `core.app._startup_handlers` cleared, `patch('web.main.atlas_preview_available', return_value=True)` → for help, patch `web.main.discovery_available` (or wherever it's imported at call time — confirm the actual reference used inside `help_page_route`/`create_help_page`) the same way. Key reusable helpers to copy verbatim: `_atlas_user_context` (lines 54-84) → rename to a help-page equivalent; `_run_atlas_smoke` (lines 87-100); `_label_texts` / `_column_directions` (lines 107-128) for the bilingual/RTL assertions (mirrors D-05/BAND-05's "EN and HE both render, correct direction" requirement). `tests/render_smoke/conftest.py`'s `_joins_lab_user_context` (full file read) is the OTHER live example of this same harness shape wired to a different page, useful if the help-page render needs additional service-layer mocks (e.g. a fake `band_precision` row set via `DiscoveryService`).

---

### `docs/specs/discovery-v2-bake-plan.md` (REWRITE — Track-B gating task)

**Analog:** itself (full 146-line file read) — CONFIRMED STALE exactly as RESEARCH.md states: §2 "7 merges + 3 relations" (superseded by D-13's 16 merges / 0 relations), §4.3 `work_relations` table design (KILLED by D-15/D-17), §6 order-of-operations placing relation-population at step 4 before Lever-1 (WRONG order per `chronological_demotion_rule.md`, must move D-17 demotion to step 5, per RESEARCH.md Pitfall 3).

**Sections to KEEP the shape of, replace the content of:**
- §4 "Build changes to build_discovery_sidecar.py" (lines 69-96) — same 4-subsection shape (4.1 canonical merge / 4.2 drop list / 4.3 ~~work_relations~~ → **replace with D-17 chronological demotion** / 4.4 Lever-1 coverage routing), same "must land behind the strict masking gate and the all-invariant verifier" framing (line 71).
- §6 "Order of operations" (lines 110-120) — same numbered-list shape; RESEARCH.md's Pitfall 3 gives the corrected 7-step order to substitute.
- §7 "Gates" (lines 124-132) — same 6-gate shape (Codex review / all-invariant verifier / strict masking gate / band-enum absence / coverage sanity / frame doc), add a 7th gate for the `routing_reason` enum amendment (Pitfall 1) and a 8th for the never-orphan-shipped invariant.
- §9 "Relationship to Phase 134 closure" (lines 144-146) — keep verbatim framing ("data refresh, not re-opening the spine").

**Content sources for the rewrite:** the census handoff artifact (D-13, functionally: 16 merges / 1 ratified `part_of` / 1 contested-resolved-by-drop / 174 unloaded provisional / 8 residual_direct / 1 drop — reference by count only, never by title, per the masking discipline already used by the CURRENT stale doc's own §2 table, which already shows the "never quote a raw M-source title, use w0000xx ids only" discipline to copy) + `same_work_spike/probe/rsource/results/chronological_demotion_rule.md` (D-17 bake spec, gitignored) for the new §4.3.

---

### `docs/specs/discovery-frames-v2.md` (NEW)

**Analog:** `docs/specs/discovery-frames.md` (v1, full section-heading list read: Frame identity & provenance / Corpus scope statement / Stored row counts / Per-band × per-evidence_source counts / Within-key dedup formula / DATA-10 unit×work projection / C-7 per-band precision reporting / Frame lineage). The v2 doc should carry the SAME 8 section headings with corrected v2 counts + a new `frame_content_hash` (computed by `scripts/build_discovery_sidecar.py::compute_frame_content_hash`, lines 213-237 of that file — reused unchanged, never re-derived) + DB `content_hash`, per `docs/specs/discovery-v2-bake-plan.md` §7 gate 6.

---

### `docs/specs/discovery-sidecar-schema-v1.md` (dated amendment — `routing_reason` enum)

**Analog:** itself — the document's own closing discipline (line 612-614, full read): *"This document is FROZEN as of 2026-07-22 ... any correction requires a new dated amendment"*. The Frozen Enum Vocabularies block to amend (lines 31-46, full read):
```
routing_reason      in {impurity, runner_up_conflict, co_citation, none}
```
→ add `later_shared_text` (Pitfall 1 — the D-17 demotion tag has no home in the current frozen set). Add a new dated section (e.g. `## 11. Amendment 2026-07-23 (Phase 135, D-17)`) rather than silently editing the frozen block in place — mirror the doc's own precedent at its top (lines 6-13: *"Amended 2026-07-23: ... surfaced by SEED-029 ..."* is the exact style already used once in `discovery-band-labels-v1.md`'s revision-history header — reuse that phrasing convention here too).

---

### `scripts/discovery_ids.py` (MODIFY — enum rename + `routing_reason` amendment)

**Analog:** itself (full 413-line file read) — the exact frozenset-construction idiom to extend (lines 103-112, read directly):
```python
ROUTING_REASON_IMPURITY = "impurity"
ROUTING_REASON_RUNNER_UP_CONFLICT = "runner_up_conflict"
ROUTING_REASON_CO_CITATION = "co_citation"
ROUTING_REASON_NONE = "none"
ROUTING_REASONS = frozenset({
    ROUTING_REASON_IMPURITY, ROUTING_REASON_RUNNER_UP_CONFLICT,
    ROUTING_REASON_CO_CITATION, ROUTING_REASON_NONE,
})
```
Add `ROUTING_REASON_LATER_SHARED_TEXT = "later_shared_text"` + include it in `ROUTING_REASONS`, following the EXACT same 2-line-constant-then-frozenset-member idiom (do not restructure).

**Enum rename** (lines 63-64, `expert_verified` → v2's `high_confidence_algorithmic`): the v1/v2 dual-key normalization helper (`_canon_band_key`, cited in the values-module section above) is the mechanism by which `scripts/discovery_ids.py` itself stays a SINGLE source of truth without needing two competing constants — confirm with the bake-plan rewrite whether the rename happens by literally changing the stored string constant's VALUE (`CONFIDENCE_BAND_EXPERT_VERIFIED = "high_confidence_algorithmic"`, breaking any v1-DB-shaped test fixture that still expects `"expert_verified"`) or by adding a NEW `CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC` constant alongside the old one (additive, non-breaking) — `docs/specs/discovery-band-labels-v1.md` §5 implies the OLD name is retired entirely in v2, so plan for the rename-in-place option, with `tests/test_discovery_ids.py`'s existing golden-digest tests (which hardcode the string `"expert_verified"` as a literal test input, e.g. lines 88, 101, 151, 157) needing a parallel update — these tests use the literal string, not the constant, in several places, so a rename does NOT automatically break them, but any NEW golden-digest test added for v2 should use the NEW string.

---

### `scripts/build_discovery_sidecar.py` (MODIFY — 4 v2 build changes)

**Analog:** itself (3,440-line file; targeted non-overlapping reads of the DDL block, the real-mode insert helpers, and the orchestrator).

**1. DDL — `routing_reason` CHECK constraint amendment site** (lines 116-125, full read):
```python
CREATE TABLE discovery_evidence (
  ...
  routing_status    TEXT NOT NULL CHECK (routing_status IN ('shipped','review_only')),
  routing_reason    TEXT NOT NULL CHECK (routing_reason IN ('impurity','runner_up_conflict','co_citation','none')),
  ...
```
→ add `'later_shared_text'` to the CHECK list, in lockstep with `scripts/discovery_ids.py::ROUTING_REASONS` (Pitfall 1). Constant-naming convention to extend (lines 264-268, read directly):
```python
_SHIPPED = ids.ROUTING_STATUS_SHIPPED
_REVIEW_ONLY = ids.ROUTING_STATUS_REVIEW_ONLY
_NONE_REASON = ids.ROUTING_REASON_NONE
_CO_CITATION = ids.ROUTING_REASON_CO_CITATION
```
→ add `_LATER_SHARED_TEXT = ids.ROUTING_REASON_LATER_SHARED_TEXT` alongside.

**2. `canonical_work_id` population site** — currently self-canonical (NO merge logic exists yet in real-mode), `_insert_works_real` (lines 2107-2114, read directly):
```python
def _insert_works_real(cur: sqlite3.Cursor, works: List[Dict]) -> None:
    ...
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [(w["work_id"], ids.canonical_work_id(w["work_id"]), w["neutral_title"], ...) for w in works],
    )
```
`ids.canonical_work_id(work_id, cross_corpus_map=None)` (already exists, `scripts/discovery_ids.py` lines 243-251) already accepts an OPTIONAL `cross_corpus_map` and documents itself as "gen-2 versioned REBUILD, never a v1 migration ... `cross_corpus_map` is None at v1 launch" — this is the EXACT extension point for D-13's merge population (4.1): build a `cross_corpus_map` dict from the census handoff's 16 merges (+ the RCh-Shabbat canonical flip per D-14) and thread it into this call. No new function needed, just a non-None argument at the real-mode call site.

**3. Order-of-operations / master orchestrator** — `finalize_build(...)` (signature at lines 2867-2891, docstring 2892-2935, body continuing to ~3090+, all read directly). The exact sequencing today: `_resolve_band_precision_spec` (H3 gate, no I/O) → `_connect_research_ro` → load JSONL inputs → `_assert_release_inputs_complete` (H2 gate) → delete prior `.db` → `select_shown_works` → `assign_opaque_work_ids` (crosswalk mint/persist) → `load_approved_works` → `build_claims_and_evidence(...)` → `build_witness_units(...)` → hash source/crosswalk/HTR → open `out_conn` → `create_schema` → `_insert_works_real` → `_insert_claims_and_evidence_real` → `_insert_witness_units_real` → INSERT `band_precision` rows. **Insert the D-13 canonical-merge (drop list §4.2 too) BEFORE `build_claims_and_evidence` runs** (drop-list exclusion must happen before claim-gen sees the excluded works) and **insert the D-17 chronological demotion AFTER whatever performs Lever-1 coverage routing, inside/after `build_claims_and_evidence`** (per RESEARCH.md Pitfall 3's corrected order: merge+drop → claim-gen → distinctive/shared routing → Lever-1 coverage → D-17 chronology → tier-A assignment → bake+verify+masking+manifest). `build_claims_and_evidence` itself starts at line 1935 (signature only read via grep so far — the planner's Track-B implementation task should Read this function's full body before writing the coverage-routing/demotion insertion point, since it was NOT fully read this session; budget a dedicated targeted read there).

**4. `band_precision` insertion shape** (lines 3071-3079, read directly) — reused verbatim, no change needed structurally:
```python
cur.executemany(
    """
    INSERT INTO band_precision (
        scope, collection_id, evidence_source, confidence_band, numerator, denominator,
        precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes
    ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
               :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
               :ins_policy, :weighting, :notes)
    """,
    bp_rows,
)
```
The CERT-01 measurement result (once graded) is written here as a NEW row (or an UPDATE to the existing `tier_a` row) via `--precision-spec <json>` (the existing H3 mechanism at lines 2913-2922 / `_resolve_band_precision_spec` at 2834-2864) — no new insertion code path needed, reuse the existing explicit-precision-spec CLI argument.

---

### `scripts/verify_discovery_sidecar.py` (MODIFY — new invariants)

**Analog:** itself (full function inventory via grep; `VALID_EVIDENCE_COMBOS` at line 54, `check_evidence_combinations` at 146, `check_display_pointer_ownership` at 185, `check_band_precision` at 323). Same "return a list of violation strings, empty = pass" idiom throughout — new checks should return `List[str]` and be registered in `main()` (line 553) alongside the existing ones. New checks needed per RESEARCH.md pitfalls:
- v1 enum-name absence in the v2 asset (`docs/specs/discovery-v2-bake-plan.md` §7 gate 4 — grep the shipped DB for the literal string `expert_verified`).
- never-orphan-shipped invariant for D-17 (a `review_only`-tagged co-claimant must never be the sole reachable evidence for a claim that also has a shipped sibling — mirrors the EXISTING `check_display_pointer_ownership` shape at lines 185-211, read directly as the closest structural analog for "assert a pointer never crosses into forbidden territory").
- unknown-date-never-demoted assertion (any `routing_reason='later_shared_text'` row must correspond to a work-pair where BOTH dates were resolved, never UNKNOWN).

---

### `scripts/check_atlas_masking.py` (MODIFY — register M-source date-table vocabulary)

**Analog:** itself — `load_patterns()` (line 184, docstring 184-190, body starting 190, read directly):
```python
def load_patterns() -> list[str]:
    """... reads MASKING_SCAN_PATTERNS_FILE. Returns [] on ANY failure ..."""
    path_str = os.environ.get('MASKING_SCAN_PATTERNS_FILE')
```
No code change needed to REGISTER new vocabulary — per the module's own design (and the Don't-Hand-Roll table entry citing this exact mechanism), the M-source date table's distinguishing terms are added to the GITIGNORED `.masking_patterns` file (owner-operational step, same as the R-source Phase 134 D-03c precedent), never hardcoded into this script. The CLI invocation shape to use unchanged (docstring lines 78-84, read directly):
```
python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery.db \
    --scan-asset discovery_data/discovery.db --scan-repo --strict
```

---

### CERT-01 written pre-registration protocol (NEW, gitignored research doc)

**Analog:** `same_work_spike/probe/results/PLAN-e1-round2.md` (162 lines) — section-heading template confirmed via direct grep (do not re-derive a structure from scratch):
```
## Objective
## Step 1 — label table (frozen + SHA)
## Step 2 — ONE pre-registered model (#3, #4, #5 — no selection step)
## Step 3 — round-2 frame (#1)
## Step 4 — pre-outcome operating-characteristic analysis (#12, #13)
## Step 5 — fresh discovery deck
## Step 6 — analysis, certification, estimand (#14)
## Deviations register
```
CERT-01's protocol should mirror this EXACT shape (Objective → frozen label table+hash → the ONE pre-registered decision rule, Strict ≥0.85 per D-07 → the v2 shipped tier_a frame definition, per D-05's exact display-deduplicated `(page_id, canonical_work_id)` population → pre-outcome OC table (mandatory per RESEARCH.md Pitfall 8 — do not skip) → the card-draw/deck mechanics → analysis+outcome-branch+estimand → a deviations register). `PLAN-e1-round3-canon.md` (380 lines, same tree) is the SAME template's next iteration, useful for seeing how the template evolved across a real 2nd round — read it if the round-2 template alone under-specifies a needed section (e.g. gold-repeatability gate details).

**Reusable machinery to CITE, not re-derive** (`same_work_spike/probe/scripts/e1_deck.py`, lines 434-485, read directly — confirmed stdlib-only, `math`/`random`/`collections` only):
```python
def wilson_bounds(k, n, z=1.96):
    if n == 0:
        return 0.0, 0.0, 1.0
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return p, (c - h) / d, (c + h) / d

def components_of(cards):
    """Bipartite work<->physMS connected components over cards."""
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    for c in cards:
        union(('w', c['work_id']), ('m', c['phys']))
    return {c['uid']: find(('w', c['work_id'])) for c in cards}

def comp_bootstrap(rows, B=10000, seed=7, pct=2.5):
    """rows: [(comp, isA)] over DETERMINATE cards -> (p, lo, hi, n_components)."""
    # resamples WHOLE COMPONENTS (physical MSS), not raw cards -- this
    # IS D-05's "CI clustered by physical MS" requirement, verbatim.
```
`same_work_spike/probe/scripts/e1_confirm_sizing.py` (function inventory via grep, confirmed): `wilson_lower_one_sided`, `n_det_required`, `anova_icc`, `expected_nonempty_components`, `m_bar_fixed_point`, `size_confirmation` — this is the ICC-adjusted sample-size module CERT-01's pre-outcome OC table must reuse (RESEARCH.md's Don't-Hand-Roll table + Pitfall 8), NOT a fresh power calculation. `same_work_spike/probe/scripts/e1_band_frame.py` (function inventory: `band_of`, `main`) is the frame-freeze precedent for CERT-01's "freeze manifest BEFORE any card is drawn" requirement (Pitfall 7's checkable completion signal).

---

### `tests/test_discovery_v2_bake.py` (NEW, or extend `tests/test_discovery_build.py`)

**Analog:** `tests/test_discovery_build.py` (1,851 lines; header + fixture-helper block read directly, lines 1-90). The exact fixture-construction idiom to reuse for v2 test cases (merge pair / drop-list row / low-coverage row / synthetic chronological-demotion cluster):
```python
def _mk_track1_row(page_id, sys_id, work_id, cat, *, genre=None, author=None, title=None,
                    matched_letters=10, best_density=0.5, n_spans=1,
                    spans_json="[[0, 10, 0.5]]", shadowed_by=None):
    return (page_id, sys_id, work_id, cat, genre, author, title, None,
            matched_letters, best_density, n_spans, spans_json, shadowed_by)

def _build_track1_db(tmp_path, rows, name="research.db"):
    """Small sqlite db with a `track1_matches` (+ empty `pages`) table shaped
    exactly like the real research DB's schema (verified column set)."""
    ...
```
Every fixture value in this file is FABRICATED (`raw:`-prefixed work ids, obviously-synthetic titles) per its own documented masking convention (lines 4-11) — mirror this convention exactly for any new v2 fixture rows (never a real M-source/Sefaria/JA title or id shape). The file also shows (lines 26-36) the exact import trick needed if new tests call `finalize_build` and need to catch `MaskingGateFailure`/`ScanError` — import `check_atlas_masking` the SAME flat way `build_discovery_sidecar.py` does internally, or the exception classes will have two distinct identities.

---

## Shared Patterns

### Frozen-enum / hand-typed-display TOTAL-coverage discipline
**Source:** `scripts/discovery_ids.py` (module docstring + `CONFIDENCE_BANDS_BY_SOURCE`) + `tests/test_discovery_ids.py` (golden/totality tests).
**Apply to:** `shared/discovery_band_labels.py`, its drift-guard test, and every `routing_reason`/enum-rename touch point in `scripts/discovery_ids.py` / `scripts/build_discovery_sidecar.py` / `scripts/verify_discovery_sidecar.py`.
Never redeclare a frozen enum as a duplicate literal set without a totality/no-orphan test tying it back to the ONE source of truth.

### Fail-closed, no-raise sidecar reads
**Source:** `web/discovery_assets.py::load_discovery_state()` + `shared/discovery_service.py`'s `is_available()`/every sync getter (`get_version`, `get_evidence`, etc. — `try/except Exception: logger.error(...); return None/[]`, never raise).
**Apply to:** the new `band_precision` reader in `shared/discovery_service.py` and any adapter code CERT-01 writes to read the v2 frame.

### Versioned-artifact / dated-amendment discipline
**Source:** `docs/specs/discovery-sidecar-schema-v1.md` (closing clause, lines 610-614) + `docs/specs/discovery-band-labels-v1.md` (its own 2026-07-23 amendment header, lines 3-13).
**Apply to:** every touched spec doc this phase (`discovery-band-labels-v1.md` §4/D-18, `discovery-sidecar-schema-v1.md` §Frozen-Enum for `later_shared_text`, the `discovery-v2-bake-plan.md` rewrite itself). Never silently edit a FROZEN doc — add a new dated section.

### Whole-route conditional `noindex`
**Source:** `web/main.py::atlas_page_route()` (lines 2137-2153) + `page_meta()` (lines 920-933).
**Apply to:** `/help` route (`noindex=discovery_available()`), per RESEARCH.md Pitfall 9.

### render-smoke harness shape (NiceGUI `User` + ASGITransport + startup-handler clearing)
**Source:** `tests/render_smoke/test_atlas_render_smoke.py` (full file) + `tests/render_smoke/conftest.py` (full file).
**Apply to:** `tests/render_smoke/test_help_methods_render_smoke.py`.

### Masking-gate registration via gitignored pattern file, never hardcoded
**Source:** `scripts/check_atlas_masking.py::load_patterns()` + `MASKING_SCAN_PATTERNS_FILE`.
**Apply to:** the M-source composition-date table's new vocabulary (Pitfall 6) — an owner-operational step, not a code change.

### physMS-clustered bootstrap / ICC-adjusted sizing — reuse, never reimplement
**Source:** `same_work_spike/probe/scripts/e1_deck.py::components_of`/`comp_bootstrap`/`wilson_bounds`; `e1_confirm_sizing.py::size_confirmation` et al.
**Apply to:** the CERT-01 protocol doc and any thin adapter script pointing the harness at the v2 frame.

## No Analog Found

None. Every file in this phase's scope has at least an exact same-repo structural analog (in several cases the file IS its own analog — this phase deliberately extends Phase-134 artifacts in place rather than introducing a parallel design). The two items with the LOWEST analog confidence (flagged, not "no analog"):

| File/Task | Role | Data Flow | Reason for lower confidence |
|---|---|---|---|
| CERT-01 harness adapter script (only if new Python glue code is needed to point `e1_*.py` at the v2 frame instead of the spike-internal frame) | utility | batch | RESEARCH.md's own Open Question #1 in the Wave-0-gaps table: "if the harness is invoked with zero new Python code (pure CLI args pointing at a new DB path), this gap may not apply" — confirm at plan time whether any new code is even needed before assigning an analog-driven `read_first`. |
| `build_claims_and_evidence` insertion point for Lever-1/D-17 (inside `scripts/build_discovery_sidecar.py`, function starts line 1935) | migration/batch-build | batch | This session located the function by signature/grep only, did not read its full body (est. several hundred lines) — the planner's Track-B implementation task MUST do a dedicated full read of this function before writing the insertion code; do not assume the exact insertion line from this document alone. |

## Metadata

**Analog search scope:** `shared/`, `web/pages/`, `web/components/`, `web/main.py`, `web/discovery_assets.py`, `web/feature_flags.py`, `scripts/discovery_ids.py`, `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py`, `scripts/check_atlas_masking.py`, `docs/specs/discovery-*.md`, `tests/test_discovery_*.py`, `tests/render_smoke/`, `tests/fixtures/discovery/`, `same_work_spike/probe/scripts/` + `same_work_spike/probe/results/` (gitignored research tree, read for D-09 harness reuse).
**Files scanned (read in full or via targeted grep+offset reads):** `scripts/discovery_ids.py` (full, 413 lines), `web/discovery_assets.py` (full, 323 lines), `shared/discovery_service.py` (targeted: 1-230, 350-450, 860-921 + grep), `web/pages/help.py` (targeted: 35-110, 617-700 + grep of all 34 anchor sites), `web/main.py` (targeted: 918-958, 2137-2270 + grep), `scripts/build_discovery_sidecar.py` (targeted: 88-290, 2855-3090 + extensive grep across all 3440 lines), `scripts/verify_discovery_sidecar.py` (grep only — function inventory), `scripts/check_atlas_masking.py` (grep only), `docs/specs/discovery-band-labels-v1.md` (full, 216 lines), `docs/specs/discovery-v2-bake-plan.md` (full, 146 lines), `docs/specs/discovery-sidecar-schema-v1.md` (targeted: 1-70, 590-615), `docs/specs/discovery-frames.md` (grep headings only), `tests/test_discovery_ids.py` (full, 360 lines), `tests/test_discovery_bands.py` (full, 132 lines), `tests/test_discovery_build.py` (targeted: 1-90 + grep), `tests/render_smoke/test_atlas_render_smoke.py` (full, 231 lines), `tests/render_smoke/conftest.py` (full, 336 lines), `same_work_spike/probe/scripts/e1_deck.py` (targeted: 430-519), `e1_confirm_sizing.py` / `e1_band_frame.py` (grep function inventory only), `same_work_spike/probe/results/PLAN-e1-round2.md` (grep headings only).
**Pattern extraction date:** 2026-07-23
