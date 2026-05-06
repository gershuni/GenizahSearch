# Phase 84: CUDL Shelfmark Normalization - Context

**Gathered:** 2026-05-06
**Status:** Ready for planning

<domain>
## Phase Boundary

Bridge layer that maps CUDL classmark forms (`mosseriiii27o`, `tsar48.211`, `or1080j15`,
`tsf8.2`, `add863.2`, `tsns329.14`) to the shelfmark variants already present in
`libraries.csv`. No `libraries.csv` schema change. No new rows. Phase 85 picks up the
FJMS-only synthetic-row residue.

In scope: NORM-01, NORM-02, NORM-03, NORM-04 from `.planning/REQUIREMENTS.md`.
Out of scope: synthetic libraries.csv rows (Phase 85), coverage audit + cudl_coverage.md (Phase 86).

</domain>

<decisions>
## Implementation Decisions

### Normalizer Architecture
- **D-01:** Add new module `shared/shelfmark_bridge.py` with `cudl_normalize()` and
  related helpers. Used ONLY at cross-system lookup sites — not a replacement for the
  canonical normalizer.
- **D-02:** `genizah_core.normalize_shelfmark()` is **untouched** this phase. Existing
  search / joins / exclusion / corrections / parallels semantics are frozen. NORM-04
  regression surface is therefore confined to the 4 wiring sites in D-08.

### Mosseri Reverse Mapping
- **D-03:** Build a pre-computed alias index at startup. For every Mosseri row in
  libraries.csv, run the existing forward `construct_mosseri_cudl_label()`
  (`genizah_core.py:259`) on each variant in `call_numbers` and populate a
  `{cudl_label_normalized -> sys_id}` dict. ~3.9K entries, O(1) lookups, reuses the
  already-tested forward parser.
- **D-04 (Claude's discretion):** Whether the bridge returns the `sys_id` directly or
  the canonical shelfmark string `Moss. III,27O` for the existing shelfmark-search code
  path to resolve. Decide during planning based on per-call-site fit.

### Or. + Dot/Zero/Comma Rules (NORM-02 / NORM-03)
- **D-05:** Cover BOTH Or. patterns this phase — letter-suffix
  (`or1080j15` ↔ `Or. 1080 J 15`) AND numeric-collapse
  (`or1080.11` ↔ `Or. 1080.1.1`). Push toward parity with Mosseri (>95% match rate).
  Document residue in Phase 86's `reports/cudl_coverage.md`, not here.
- **D-06:** Audit-first / fail-loud for leading-zero collapse. Before enabling the
  `8.002` → `8.2` and `329.0014` → `329.14` rules in the bridge, write a one-shot audit
  script that walks every CUL sys_id and detects whether stripping leading zeros from
  any variant produces a key already owned by a different sys_id. If collisions are
  found, log them and EXCLUDE those normalized keys from the alias index — never silently
  merge two distinct fragments.
- **D-07:** NORM-03 also covers slash, comma, and dot-after-letter normalization
  (`T-S F 8/002` ↔ `tsf8.2`, `Add. 863, 2` ↔ `add863.2`, `T-S Ar. 48.211` ↔ `tsar48.211`,
  `T-S NS 329/0014` ↔ `tsns329.14`). All four rules apply uniformly across all CUL/Cambridge
  collections and live in the bridge module, not in canonical.

### Wiring (Call Sites)
- **D-08:** Bridge plugs into all four sites:
  1. **Shelfmark search fallback** — `genizah_core.py` shelfmark-mode search (~line 4487):
     when canonical lookup yields no hit, fall through to bridge's CUDL-form lookup.
     Satisfies NORM-01/02 for users pasting CUDL classmarks.
  2. **Browse CUDL external-link builder** — replace naive `shelfmark.replace(' ', '-')`
     at `web/pages/browse.py:3607` with the bridge's libraries.csv → CUDL function
     so the "Cambridge" button lands on the correct CUDL viewer page.
  3. **cambridge_manifests reverse lookup** — `shared/nli_crossref_service.py:313` and
     `:337`. Bridge maps libraries.csv shelfmark → CUDL classmark when looking up the
     manifest URL, so Mosseri/Or browse pages display CUDL images.
  4. **Orphan-scanner unification** — `scripts/scan_cudl_orphans.py:37` imports from
     the bridge module instead of its private `normalize()` copy. One source of truth
     between runtime and audit; makes Phase 86's re-run trustworthy.

### Tests / Regression Guard (NORM-04)
- **D-09:** Three-layer regression guard.
  1. **Golden fixture:** `tests/fixtures/cudl_must_resolve.csv` with ~50 hand-picked
     classmarks spanning Mosseri / Or letter-suffix / Or numeric-collapse / T-S F /
     Add. / T-S Ar. / T-S NS. Pytest asserts each resolves correctly through the
     bridge end-to-end (search and/or sys_id lookup).
  2. **Scan diff:** Run `scripts/scan_cudl_orphans.py` before and after; assert the
     orphan count strictly drops AND no previously-matched classmark becomes orphan.
     CI step. Target: ≤300 residual orphans (per roadmap).
  3. **Canonical-untouched assertion:** Pytest set covering representative non-CUL
     shelfmarks (Oxford, JTS, RNL, Mosseri-non-CUDL, Halper, ENA-MS) to assert
     `normalize_shelfmark()` output is byte-identical pre/post-phase. Defends NORM-04
     for the 140K already-matching CUL rows AND the ~89K non-CUL rows.

### Claude's Discretion
- D-04 (sys_id vs canonical-shelfmark return type from bridge).
- Internal organization of `shared/shelfmark_bridge.py` (one function or several;
  whether the alias index is a class or a module-level dict; cache-invalidation strategy
  on libraries.csv reload).
- Exact set of Or. classmark patterns beyond letter-suffix and numeric-collapse if the
  orphan-with-neighbor scan reveals additional regular structures.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & Roadmap
- `.planning/REQUIREMENTS.md` §Normalization (NORM-01 … NORM-04) — locked acceptance criteria.
- `.planning/ROADMAP.md` §"Phase 84 -- CUDL Shelfmark Normalization" — goal + 5 success criteria.

### Existing Normalization Code
- `genizah_core.py:194` — `normalize_shelfmark()` canonical normalizer. **Untouched per D-02.**
- `genizah_core.py:259` — `construct_mosseri_cudl_label()` forward Mosseri parser. **Reused for the alias index per D-03.**
- `genizah_core.py:4487` — shelfmark-mode search call site (wiring target #1).
- `scripts/scan_cudl_orphans.py:37` — current bespoke `normalize()` (wiring target #4 + the audit script that defines the ≤300 target).

### Wiring Targets
- `web/pages/browse.py:3605-3624` — current naive CUDL external-link builder (wiring target #2).
- `shared/nli_crossref_service.py:313`, `:337` — `cambridge_manifests` reverse-lookup queries (wiring target #3).

### Reports (Reference Data)
- `reports/cudl_orphans_all.csv` (6,053 rows pre-phase) — full orphan inventory.
- `reports/cudl_orphans_with_neighbor.csv` (105 rows pre-phase) — high-confidence merge candidates.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `construct_mosseri_cudl_label()` already handles libraries.csv → CUDL forward
  direction across all `_MOSSERI_CUDL_SERIES` (I, IA, II, III, IIIA, IV, V, VI, VII,
  VIII, IX, X). The alias-index strategy in D-03 stands on this foundation.
- `scripts/scan_cudl_orphans.py:normalize()` already encodes the dot-after-letter and
  leading-zero rules required by NORM-03 — port these into the bridge module rather
  than re-deriving them.
- `nli_crossref.db.cambridge_manifests` table: indexed by `normalized_shelfmark`,
  manifest_url present for ~141K rows.

### Established Patterns
- Shared services live under `shared/` (e.g., `shared/nli_crossref_service.py`,
  `shared/document_service.py`); web and desktop both import from there. Bridge module
  belongs there per repo convention.
- Service-level normalizers are pure functions (no I/O on hot path); the alias index is
  loaded once at startup from `libraries.csv` (already loaded for `csv_bank`).
- Tests live under `tests/`; fixtures under `tests/fixtures/` (precedent: translation_qc).

### Integration Points
- libraries.csv parsing happens once at startup in `genizah_core.py` — alias index
  build can hook there to amortize the cost.
- `cambridge_manifests` is read-only at runtime; bridge does not mutate the sidecar.

### Constraints
- **Both apps must be maintained** (web + desktop). Bridge function and its callers must
  not be NiceGUI-specific or PyQt6-specific.
- Phase 85 will add synthetic sys_ids (`99` + InventoryId-padded-10 + `000000`). The
  bridge should not assume sys_ids are short or short-numeric; treat them as opaque strings
  so Phase 85 doesn't need to retrofit.

</code_context>

<specifics>
## Specific Ideas

- "Layered, not extended" — user explicitly wants the canonical normalizer left alone
  and a separate bridge module. This is a reversibility lever: if the bridge causes a
  regression, it can be unwired at the four call sites without touching search/joins.
- "Fail-loud on collisions" — for leading-zero collapse, the user prefers excluding
  collision keys with logging over silent merges. Audit script must precede the rule
  enablement.
- The Mosseri alias index reusing `construct_mosseri_cudl_label()` (forward parser)
  rather than writing a new reverse parser is the user's preferred symmetry — one
  parser of record.

</specifics>

<deferred>
## Deferred Ideas

- Synthetic libraries.csv rows for FJMS-only inventories (T-S NS 329.96 et al.) →
  **Phase 85**.
- `reports/cudl_coverage.md` and post-milestone orphan re-run + AUDIT-03 regression
  pass → **Phase 86**.
- Extending `normalize_shelfmark()` (canonical) to absorb the bridge rules — explicitly
  rejected for this phase (D-02). Could be reconsidered post-milestone if the bridge
  proves benign across all 4 call sites for an extended period.
- Mosseri "2nd series" (`Ms. L 241`, etc.) and `Ms. MOSS NS` patterns — out of scope of
  `construct_mosseri_cudl_label()`'s `_MOSSERI_CUDL_SERIES`. If they surface in the
  Phase 86 residue, address in a follow-up.

</deferred>

---

*Phase: 84-cudl-shelfmark-normalization*
*Context gathered: 2026-05-06*
