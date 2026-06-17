# Requirements: GenizahSearch — v8.2.0 Web Joins Lab

**Defined:** 2026-06-17
**Core Value:** Researchers can find what they need in the Genizah corpus.

> **Milestone scope:** Port the desktop Joins Lab (Component A) to the web at **full parity**, riding the complete, web-reusable `shared/joins_lab.py` core (v8.0.0 Phase 106). Human-in-the-loop only — the scholar is the ranker and confirmer; there is NO automated join-finder. Decisions (user 2026-06-17): full parity (not streamlined), **Component A only**, **Visual Similarity included**, **persistence device-local for everyone** (no Supabase, no login wall). Phase numbering continues from **117**. Web-only milestone.
>
> **Pre-lock review:** refined per the Codex code-grounded critique (`.planning/v8.2.0-REQ-CODEX-CRITIQUE.md`, 2026-06-17) — all 4 blockers + 9 HIGH findings folded in.

## v1 Requirements

Requirements for this milestone. Each maps to a roadmap phase.

### Foundation & Entry (FND)

- [x] **FND-01**: A web `SearchExecutor` adapter implements the `shared/joins_lab.py` Protocol (`execute_search` / `get_browse_page` / `get_meta_for_id` / `get_library_for_id`) by wrapping the web search engine **directly** (`state.searcher.execute_search` — NOT `/api/search`, which omits `text_position`/`corpus_scope` and caps modes). Search runs **off the event loop** (`run.io_bound`-style, as `web/pages/search.py` does) with timeout, cancellation, and stale-generation (latest-wins) handling — the NiceGUI event loop is never blocked. No search logic re-implemented.
- [ ] **FND-02**: User can open the Joins Lab at a dedicated `/joins-lab` web route.
- [ ] **FND-03**: User can cold-start the Joins Lab by entering a shelfmark or sys_id (no prior search required).
- [ ] **FND-04**: User can launch the Joins Lab on a specific fragment via a "Find joins" action on `/search` result cards.
- [ ] **FND-05**: User can launch the Joins Lab on a specific fragment via a "Find joins" action on `/browse`.
- [ ] **FND-06**: The Joins Lab works without login (no auth wall); every per-user state access goes through `web/safe_storage.py` (zero raw `app.storage.user`, preserving the Phase 87 CI-guarded invariant — `tests/test_no_raw_storage_access.py`, allowlist `[]`).
- [ ] **FND-07**: The entire Joins Lab UI is bilingual (EN/HE) with correct RTL layout, consistent with the rest of the web app.
- [ ] **FND-08**: The entry/deep-link URL contract is explicit — anchor identified by `sys_id` (with shelfmark / `fl_id` / page / `volume_ie` where relevant) so `/search`, `/browse`, and cold-start all resolve the same anchor (incl. multi-IE volumes). The deep link carries the anchor (and optional initial VS mode) only; builder/candidate/triage state is device-local, NOT encoded in shareable URLs.

### Anchor Pane (ANC)

- [ ] **ANC-01**: The anchor pane shows the pinned fragment's image with zoom/pan and folio (page) navigation.
- [ ] **ANC-02**: Anchor (and Compare) images load through the **existing per-provider image-proxy resolution** used by `/browse` (NLI, Oxford, Cambridge, Manchester, JTS) — reusing those endpoints and the Phase-98 NLI circuit breaker — rather than inventing direct image URLs. No unguarded NLI/IIIF fetch.
- [ ] **ANC-03**: The anchor pane shows the fragment's transcription as right-aligned (RTL) numbered lines.
- [ ] **ANC-04**: The anchor pane shows known joins (PGP + FJMS + user + community) as a connected group with source attribution.
- [ ] **ANC-05**: Known-joins display is multitenant-safe — it surfaces only public/confirmed joins (or is user/status-aware cache-isolated), so the process-global join cache can never leak one user's unconfirmed (creator-only, RLS-scoped) joins to another user.

### Query Builders (BLD)

- [ ] **BLD-01**: User can build a line-by-line query for the anchor side — rows of OR-grouped word-boxes corresponding to manuscript lines.
- [ ] **BLD-02**: User can build a line-by-line query for the OTHER side of the leaf with cross-side narrow/widen (`apply_cross_side`), over a web-defined page contract: which page identifier feeds `resolve_other_side_pages` (`p_num` vs internal index), multi-IE (`volume_ie`) behavior, unknown total-page counts, and sparse / metadata-only pages.
- [ ] **BLD-03**: User can set per-line modifiers (line-start ⊢ / line-end ⊣, plene/defective, etc.) on any builder row.
- [ ] **BLD-04**: User can set global search toggles (variants, Judeo-Arabic, flexible spacing, bidirectional) and an inline gap; these are applied to BOTH sides via the `_merge_globals`-equivalent before/around `compose()` (which hardcodes JA/flex/bidirectional to false) — so a toggle is never silently dropped.
- [ ] **BLD-05**: Running a built query composes into the engine's syntax (`compose`) and executes against the web search engine via the FND-01 adapter, returning candidates.

### Candidate Surface (CND)

- [ ] **CND-01**: Candidates render deduped to one entry per image/fragment (`dedup_candidates`).
- [ ] **CND-02**: User can view candidates in a grid surface (thumbnail + key metadata + per-card actions).
- [ ] **CND-03**: User can view candidates in a table surface (sortable columns, multi-select).
- [ ] **CND-04**: User can triage each candidate Yes / Maybe / No; triage is keyed by `sys_id`, reflected consistently across grid, table, and Compare, and resets on re-anchor.
- [ ] **CND-05**: The Lab surfaces a self-match readout when the anchor appears in its own candidate results (`detect_self_match`).
- [ ] **CND-06**: User can filter candidates (material / dimensions / size-mismatch / triage state), matching the desktop candidate filters.
- [ ] **CND-07**: The candidate surface is bounded — pagination and/or a result cap so a large candidate set never renders unbounded (event-loop / payload safety).
- [ ] **CND-08**: Candidate metadata (shelfmark/title/library/material/dimensions/thumbnail) is enriched off the event loop, batched, and breaker-guarded for image/network lookups.

### Compare (CMP)

- [ ] **CMP-01**: User can open a side-by-side Compare of the anchor and a chosen candidate (image + transcription).
- [ ] **CMP-02**: Compare supports per-pane zoom and folio navigation.
- [ ] **CMP-03**: User can record a Yes/Maybe/No verdict from Compare, synced with the `sys_id`-keyed candidate triage state.

### Visual Similarity (VSM)

- [ ] **VSM-01**: A single Visual Similarity (👁) toggle merges FIST look-alike candidates into the candidate surface (text-only when OFF; VS-merged / intersection when ON) via a web VS-service adapter feeding `merge_candidates`. The toggle tracks the loaded anchor sid so look-alikes invalidate on re-anchor, and has explicit disabled / no-VS-data / empty-intersection states.
- [ ] **VSM-02**: Visually-similar candidates carry a consistent 👁 badge across grid, table, and Compare.

### Actions (ACT)

- [ ] **ACT-01**: User can add a confirmed candidate as a join via the existing pairwise-join path (login-gated — community write).
- [ ] **ACT-02**: User can add the anchor + selected candidates to the Fragment Puzzle. Because `/puzzle?add=` currently accepts a single fragment, this milestone adds a **bulk staging handoff** (multi-fragment payload/API) so the anchor + the selected candidates open together in `/puzzle`.
- [ ] **ACT-03**: User can add candidates to a saved list and/or export the candidate set.

### Persistence (PST)

- [ ] **PST-01**: The Joins Lab persists builder **inputs**, triage verdicts, filter, and view state — NOT candidate result blobs (no `full_text` / image data). On restore the search is **re-run** from the persisted inputs (mirrors desktop `join_workbench`; avoids the search-history payload-bloat class of bug). Survives a page refresh without login.
- [ ] **PST-02**: Persistence uses server-side **per-browser-session** state via `web/safe_storage.py` (`safe_user_*`, keyed by the NiceGUI session cookie — survives refresh for anonymous users; NOT the volatile `browser`/`client`/`tab` stores, NOT a new localStorage wrapper). Per-session/per-user isolated (multitenant-safe, no cross-user leakage); compact schema with caps and a schema-version invalidation field. No Supabase, no cross-device sync.
- [ ] **PST-03**: User can clear/reset the Joins Lab working state.

## Future Requirements

Deferred to a later milestone. Tracked, not in this roadmap.

### Component B — Join Workbench search-support algorithms (both apps)

- **JSA-01**: Seed parallels/composition search from the anchor passage.
- **JSA-02**: Corpus-driven suggest-then-search completion of a torn line's first/last N words.
- **JSA-03**: `[`/`]`-aware torn-word completion.
- **JWB-05**: Conservative tear-side assist (start-`]` = LEFT / end-`[` = RIGHT; "both edges torn" first-class; silent when unclear).

### Web persistence enhancements

- **PST-F1**: Cloud cross-device sync of candidate lists / triage via a new Supabase table (web-only; no desktop interop today). Deferred — desktop keeps working state local-only, so there is nothing to sync with yet.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Automated / auto-ranked join finder | Human-in-the-loop by design — the scholar is the ranker (consistent with v8.0.0 JOINS-F4). Slow, low-recall, no parallels for ~40% of cases. |
| New Supabase schema for Joins Lab working state | Persistence is device-local (per-browser-session, server-side via safe_storage) this milestone; desktop is local-only too. Cloud sync → PST-F1 (future). |
| Component B (JSA-01/02/03, JWB-05) | User decision 2026-06-08, reaffirmed 2026-06-17 — v8.2.0 is the Component-A web port only. |
| Web↔desktop capability divergence | Full parity is the goal; divergence would re-introduce dual-maintenance drift. |
| Richer N-fragment "other side" / multi-leaf join model | Design deferral #1 from v8.0.0; keep the pairwise→group model (no new schema). |

## Traceability

Which phases cover which requirements. Populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| FND-01 | Phase 117 | Complete |
| FND-02 | Phase 117 | Pending |
| FND-03 | Phase 117 | Pending |
| FND-04 | Phase 118 | Pending |
| FND-05 | Phase 118 | Pending |
| FND-06 | Phase 117 | Pending |
| FND-07 | Phase 121 | Pending |
| FND-08 | Phase 117 | Pending |
| ANC-01 | Phase 117 | Pending |
| ANC-02 | Phase 117 | Pending |
| ANC-03 | Phase 117 | Pending |
| ANC-04 | Phase 118 | Pending |
| ANC-05 | Phase 118 | Pending |
| BLD-01 | Phase 117 | Pending |
| BLD-02 | Phase 118 | Pending |
| BLD-03 | Phase 118 | Pending |
| BLD-04 | Phase 118 | Pending |
| BLD-05 | Phase 117 | Pending |
| CND-01 | Phase 117 | Pending |
| CND-02 | Phase 117 | Pending |
| CND-03 | Phase 119 | Pending |
| CND-04 | Phase 119 | Pending |
| CND-05 | Phase 119 | Pending |
| CND-06 | Phase 119 | Pending |
| CND-07 | Phase 119 | Pending |
| CND-08 | Phase 119 | Pending |
| CMP-01 | Phase 119 | Pending |
| CMP-02 | Phase 119 | Pending |
| CMP-03 | Phase 119 | Pending |
| VSM-01 | Phase 119 | Pending |
| VSM-02 | Phase 119 | Pending |
| ACT-01 | Phase 120 | Pending |
| ACT-02 | Phase 120 | Pending |
| ACT-03 | Phase 120 | Pending |
| PST-01 | Phase 120 | Pending |
| PST-02 | Phase 120 | Pending |
| PST-03 | Phase 120 | Pending |

**Coverage:**

- v1 requirements: 37 total
- Mapped to phases: 37 (complete)
- Unmapped: 0

---
*Requirements defined: 2026-06-17*
*Last updated: 2026-06-17 — traceability table filled at roadmap creation*
