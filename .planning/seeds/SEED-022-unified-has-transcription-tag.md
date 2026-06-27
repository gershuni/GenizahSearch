---
id: SEED-022
status: shipped
planted: 2026-06-23
planted_during: User feature request (relayed by Hillel) — "can users see which mss have a manual transcription?" Mapped via two Explore passes. NOT part of the 2026-06-23 audit; a new feature.
trigger_when: After audit PRs #298 (genizah_app.py/gui_threads.py) and #300 (web/pages/search.py, search_results.py) MERGE — SEED-022 edits the same files and would conflict. Build on the merged base. Codex-review this seed before coding (per the project's seed-review gate).
scope: small-medium (one union helper + a NEW additive indicator on both apps, alongside the unchanged PGP tag)
---

# SEED-022: "Has transcription" tag/column — NEW, additive, source-agnostic

> User intent (Hillel, 2026-06-23): a reader wants to know **"is there a manual transcription to read here"**
> regardless of source. Add a NEW indicator = **PGP ∪ FGP** transcriptions (translations included).

## Decisions (locked by Hillel)
- **KEEP the PGP tag exactly as-is** — do NOT repurpose/relabel/rewire it. The new "has transcription"
  indicator is a **SEPARATE, ADDITIONAL** badge (web) / column (desktop) that sits alongside the PGP one.
  A PGP manuscript will show both its PGP tag AND the transcription tag (intended — different signals:
  "has PGP specifically" vs "has any manual transcription").
- **Include translations** — the new tag = "has any manual text (transcription OR translation)". Use the
  EXISTING any-source presence functions; no edition-only filtering.
- **No connection to `is_printed`** — independent axis; don't filter by it.
- **User-contributed transcriptions: SKIP for now** (none exist — no `transcriptions` table; `corrections`
  are edits, `discoveries` are discussion). Build the union helper EXTENSIBLE so a future user-source is a
  one-line add.

## Design — ADD a new indicator (the PGP wiring is untouched)
The version chooser already shows source-specific PGP/FGP groups (unchanged). The PGP tag stays. We add one
new "has transcription" signal computed from a union, and render it as a new badge/column.

### New primitive (the only net-new logic)
`shared/document_service.py` (or a thin new `shared/transcription_service.py`):
```python
def get_sys_ids_with_manual_transcriptions(sys_ids, *, include_user=True):
    """Union of manual-transcription presence across sources (translations included).
    PGP ∪ FGP today; user-source slot reserved for the future. Does NOT replace the PGP-only set."""
    out = set()
    out |= get_sys_ids_with_transcriptions(sys_ids)        # PGP, any-source (document_service.py:438)
    out |= get_sys_ids_with_fgp_sources(sys_ids)           # FGP, any-source (fgp_service.py:923)
    # if include_user: out |= get_sys_ids_with_user_transcriptions(sys_ids)  # FUTURE — no store yet
    return out
```
Graceful: FGP returns `set()` when the sidecar/flag is absent → the new tag then equals the PGP set (still
fine; it's a superset by construction).

### WEB — additive (PGP badge/filter/state all unchanged)
- **State** `web/pages/search_state.py`: ADD a new field `manual_transcription_sys_ids: Set[str]` NEXT TO the
  existing `transcription_sys_ids` (PGP) — do not rename or reuse the PGP field.
- **Enrichment** `web/pages/search.py:~4655-4679`: keep the existing `get_sys_ids_with_transcriptions` call
  (feeds PGP); ADD `get_sys_ids_with_manual_transcriptions` to the `asyncio.gather` (or compute the union
  as `pgp_set | fgp_set` to avoid double-querying PGP) → store in `manual_transcription_sys_ids`.
- **Badge** `web/pages/search_results.py:~463`: keep the PGP badge block; ADD a SECOND small badge next to it,
  shown when `sys_id in search_state.manual_transcription_sys_ids`, with the new label + a distinct color
  (PGP is green `#22c55e`; pick a different hue for the transcription tag).
- **Filter (OPTIONAL):** the existing 3-state `pgp_filter` stays PGP-only. If a transcription filter is
  wanted, ADD a separate 3-state filter cloning that pattern against `manual_transcription_sys_ids` — but
  default scope is tag/column only (confirm if filter is desired).
- **API (optional):** keep `has_pgp`; optionally ADD `has_transcription` to the serializer envelope
  (additive, export-opt-in). Check the public skill/API docs before exposing.

### DESKTOP — additive (COL_PGP unchanged)
- **Worker** `gui_threads.py`: keep `PGPBadgeWorker`/`get_sys_ids_with_transcriptions` for the PGP column;
  ADD the FGP/union fetch (extend the worker to also emit the union set, or a small sibling worker) → a new
  `self._manual_transcription_sys_ids` set. Do NOT change `_pgp_transcription_sys_ids`.
- **Column** `genizah_app.py:6986-6994`: ADD a new column constant (e.g. `COL_TRANSCRIPTION`) + header +
  tooltip AFTER `COL_PGP` (shift subsequent column indices or append at the end); fixed-width like COL_PGP.
- **Cell render** `genizah_app.py:18243-18248`: keep the PGP cell; ADD a parallel cell for the new column
  populated from `_manual_transcription_sys_ids`. Update the render loop + the badge handler (:19267-19280).
- (Excel export already has a "Has PGP" column at :2939 — optionally add a "Has transcription" column too.)

## Label — DECIDED (Hillel, 2026-06-23)
PGP tag keeps "PGP" untouched. The NEW tag is an **icon + tooltip** (not a long text label — "Transcription"
misleads because the GENIZAH corpus is itself MiDRASH *automatic* transcriptions).
- **Tooltip text (exact):** EN `"scholarly transcription/translation available"` · HE
  `"תעתיק/תרגום מדעי זמין"`. (Note: includes translation — matches the "include translations" decision.)
- **Visible element:** a compact icon, distinct color from PGP-green. Propose a scholarly/edition icon
  (e.g. a document-with-check / scroll / ✓-in-badge) at implementation; confirm the exact glyph in review.
  Optionally pair with a 1-word label ("מדעי"/"Scholarly") if the icon alone reads as unclear.
- Add the tooltip string as an EN+HE key in `genizah_translations.py` (batch with SEED-014's new keys).

## Tests required
- Union helper: PGP∪FGP; FGP-absent degrades to PGP set; extensible signature.
- Web: NEW badge renders for an FGP-only sys_id AND for a PGP sys_id; PGP badge unchanged; (filter if added).
- Desktop: new column populated from the union; COL_PGP behavior unchanged; column indices correct after insert.
- i18n: new label key EN+HE (no English leak under Hebrew).

## Done when
PGP tag UNCHANGED; a NEW additive "has transcription" badge (web) + column (desktop) reflects PGP∪FGP
(translations included), labeled with a source-agnostic term (EN+HE), distinct color; user-source slot
reserved; tests green; ruff clean. Codex-reviewed before code.

## NOT in scope
Touching/relabeling the PGP tag; edition/translation splitting; printed coupling; a NEW user-transcription
store (separate feature — when it ships, flip `include_user` to a real query).

---

## Codex review corrections (2026-06-24) — BUILD PER THESE (override the above where they differ)
Codex verdict: **GO-WITH-CHANGES**. The feature is feasible; build with these corrections.

### BLOCKERS (must fix before coding)
- **B1 — PGP predicate is the WRONG one for "has readable text."** `get_sys_ids_with_transcriptions`
  (document_service.py:438) only checks `document_fragments.sys_id` = *link* presence, NOT actual
  transcription/translation TEXT (it does not look at `document_sources.content` / `has_transcription` /
  `has_translation`); tests lock this as linked-fragment presence (test_document_service.py:498). For the
  feature ("is there manual text to READ here?") build the new tag from a **real text predicate**:
  `document_fragments JOIN document_sources` where relation ∈ {Edition, Translation} and `content` is
  non-empty → that PGP-text set, **unioned with** `get_sys_ids_with_fgp_sources` (FGP). Add this as a NEW
  helper (e.g. `get_sys_ids_with_pgp_text(sys_ids)` in document_service) — **do NOT modify the existing PGP
  badge helper** (the PGP badge keeps its current link-presence semantics, per Hillel). CONSEQUENCE (intended,
  confirmed): a PGP-linked-but-textless mss shows the PGP badge but NOT the new transcription tag — correct,
  there's nothing to read.
- **B2 — helper signature must match the real ones.** Both existing helpers are `List[str]` + use `len()`/
  slicing; a generic iterable/generator breaks them. `get_sys_ids_with_manual_transcriptions(sys_ids)` must
  `list(sys_ids or [])` once up front (or type as `Sequence[str]`).
- **B3 — desktop column APPEND, do not insert.** Current logical cols: `COL_PGP=9`, `COL_DOMAIN=10`,
  `COL_PRINTED=11`, `setColumnCount(12)` (genizah_app.py:6980,6993). Inserting after COL_PGP shifts indices
  and breaks headers/tooltips (7004/7019), cell population (18244), badge handlers (19296), persisted filter
  indices (26702/26916). **Append `COL_TRANSCRIPTION=12` + `setColumnCount(13)`.** If visual adjacency is
  ever wanted, do a header VISUAL move while preserving logical indices — not now.

### SHOULD-FIX
- **Stale web anchors.** Real enrichment sites are `web/pages/search.py:4702` AND `:4757` (TWO passes —
  stage 1 + stage 2), not `~4655-4679`. Fetch PGP-text once + FGP once, compute `manual = pgp_text | fgp` in
  BOTH passes; never call a union helper that re-queries PGP inside the gather.
- **Web restore + PGP-tag branch missed.** Session restore (`search.py:4965`) only reloads PGP IDs — also
  restore `manual_transcription_sys_ids`. PGP-tag search has a SEPARATE hardcoded badge path
  (`search.py:4890`) — render the new badge there too.
- **Desktop worker — no second PGP query.** Existing `PGPBadgeWorker` (gui_threads.py:793,803) emits only the
  PGP set; extend it to emit BOTH `pgp_ids` and `manual_ids = pgp_text_ids | fgp_ids` (one pass).
- **Init/clear new desktop state at ALL lifecycle sites.** Existing init at genizah_app.py:3382; reset/
  no-result paths (18010, 18359) clear printed but not the new set — explicitly init+clear
  `_manual_transcription_sys_ids` on new-search, reset, no-results, session-restore, PGP-tag search.
- **FGP translation-only test.** `get_sys_ids_with_fgp_sources` already includes Digital Translation (no
  edition-only filter) — add a test that an FGP translation-only row surfaces.

### NICE-TO-HAVE / DEFER
- Perf is fine batched (PGP+FGP both chunk at 500, both indexed on sys_id) — keep one extra FGP batch lookup
  per chunk, no per-row queries.
- **API/export columns: DEFER** unless asked. `has_pgp` is export/session-opt-in only and NOT in public API
  docs; if `has_transcription` is ever added keep it export-opt-in and update SEARCH_API.md/OpenAPI/tests.
- **XLSX column: DEFER** — export dossier is 12-col pinned (export_dossier.py:230,253); adding a column means
  shared headers + web/desktop exporters + image-URL index + parity tests.
- i18n: add the exact EN `"scholarly transcription/translation available"` / HE `"תעתיק/תרגום מדעי זמין"`
  key before rendering (not yet present).
- Version chooser: leave unchanged (already separates PGP/FGP editions/translations).
