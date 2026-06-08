# Requirements: GenizahSearch — Milestone v8.0.0 Dicta Rebrand & Joins Lab

**Defined:** 2026-06-02 (folds the v7.17 cycle into v8.0.0 per user decision 2026-06-02)
**Core Value:** Researchers can find what they need in the Genizah corpus
**Flagship:** **Joins Lab** — an interactive, human-in-the-loop join-hunting workbench (both apps)

## Milestone Goal

Ship **v8.0.0** as the flagship "Dicta Genizah Search Pro" release: the desktop rebrand
(delivered) and LOCAL ("My Library") export support (delivered — Phases 103 + 105) bundled
with the new **Joins Lab**. In the Joins Lab a scholar keeps **one anchor fragment in view**
(image + numbered transcription) and drives the app's **existing** search tools to find the
fragments that physically join it. **Human-in-the-loop: the scholar is the ranker and confirmer
— there is NO automated join-finder.** Both apps.

**Apps:** Both web and desktop. The dual-app maintenance rule applies to the Joins Lab.

> **Status note:** Requirements are written; the **roadmap is intentionally deferred** until
> after a **Genizah-scholar design-critique session** (user-led) that will pressure-test the
> JWB/JSA requirements against the real material nature of the corpus before phases are locked.

---

## Delivered (folded in from the v7.17 cycle)

These shipped during the v7.17 work and release **under the v8.0.0 tag**. Full original
requirement text is preserved in git history (prior `REQUIREMENTS.md`) and `MILESTONES.md`.

### Rebrand (BRAND)
- [x] **BRAND-01**: Desktop app **display name** → "Dicta Genizah Search Pro" (window title, About
  EN+HE, updater, exported-file credits incl. the shared xlsx Credits sheet, puzzle PNG footer,
  version metadata, installer, README/CHANGELOG, web download-page title). **Binary identifiers**
  (`GenizahSearchPro.exe`, `.spec`, `.iss`, `dist`/AppData folders, auto-update fetch) **UNCHANGED**
  so installs upgrade in place. Web brand ("Dicta Genizah Search") unchanged. — delivered (commit
  `6e0c312d` + follow-ups; Codex-reviewed SHIP-WITH-FIXES, resolved `6c343bd3`).
- [x] **BRAND-02**: i18n gap closure during rebrand polish (223 desktop+web missing-`tr()` gaps /
  246 keys). — delivered.

### LOCAL Export (LEXP / EXPUX) — Phases 103 + 105
- [x] **LEXP-01, 03, 04, 05, 06, 07, 08** (Phase 103) — Search-results LOCAL export across
  XLSX / CSV / TXT / DOCX, dedicated bilingual "Local Documents" sheet, Genizah sub-sheets exclude
  LOCAL, LOCAL-only workbook usable, single-table fallbacks, cross-parity non-regression (DOCX
  redesigned by design). Closes **D-F17**.
- [⏸] **LEXP-02** → deferred to Future **EXP-F3** (Composition Search has no LOCAL corpus path).
- [x] **EXPUX-01, 02, 03, 04** (Phase 105) — export UX polish: Open File/Folder dialog (EXPUX-01 UI
  **UAT pending**), LOCAL-only domain-warning suppression, LOCAL-only MiDRASH-credit omission,
  capped full-text context in DOCX/TXT.

---

## New Build — Joins Lab

### Component A — Join Workbench (JWB) — primary, both apps

- [ ] **JWB-01**: A dedicated **"Join Workbench"** tab/page exists in both web and desktop.
- [ ] **JWB-02**: A scholar opens the Workbench with a fragment **pinned as the anchor** via a
  **"Find joins"** action from the desktop **ResultDialog** and from **Browse (web + desktop)**;
  also openable **by shelfmark** for a cold start.
- [ ] **JWB-03**: The anchor fragment — its **image** and its **numbered transcription** — stays in
  view while the scholar runs searches.
- [ ] **JWB-04**: The Workbench shows the joins **already known** for the anchor (PGP
  `document_fragments` + FJMS scholarly joins + user pairwise joins + community puzzle joins), so the
  hunt starts beyond what is already recorded.
- [ ] **JWB-05** *(DEFERRED from Phase 108 → Phase 110 disposition, 2026-06-04; see Amendments)*: A
  **conservative** tear-side assist reads the anchor's `[` / `]` transcription markers (**corrected
  rule** below — start-`]` = LEFT, end-`[` = RIGHT) and **suggests** the likely side / search
  direction **only when the evidence is clear**; otherwise it stays silent. The scholar can always
  override. (Real fragment state is messy — the assist must never assert a guess.) **108 is the
  *manual* finder; the side-assist belongs to the *algorithmic* Component B.**
- [x] **JWB-06**: From the anchor (a selected line / torn word + a **direction**: rest-of-line,
  line-above, lines-below, lines-above, previous/next page), the scholar **seeds a search** that
  pre-fills the **existing** search module (variants / fuzzy / Responsa / regex); the seeded query is
  fully editable (try-and-error preserved).
- [x] **JWB-07**: The scholar collects search results as **candidates in a list** within the Workbench.
- [x] **JWB-08**: The scholar compares the anchor and a candidate **side-by-side** (image +
  transcription) to confirm a join by eye.
- [ ] **JWB-09**: On a confirmed join the scholar can **act**: add it via the **existing joins
  button**, **export** the details (clipboard / file), and **add candidates to a list**.
  (Open-in-Puzzle remains available as optional polish — both apps have the Puzzle.)

### Component B — Search-support algorithms (JSA) — ⏸ DEFERRED to post-v8.0.0 (2026-06-08)

> **DEFERRED in full (2026-06-08, /gsd-discuss-phase 110).** Per user decision, **all of Component B
> is pushed out of v8.0.0** so the milestone can ship. The Phase 110 slot was repurposed for the
> LOCAL-composition wiring the user wants before release (see § New Build — Composition LOCAL Corpus
> below + Phase 110 in ROADMAP.md). These requirements move to **Future Requirements → Component B
> (search-support algorithms)** and will be re-scoped in a post-v8.0.0 milestone.

- [⏸] **JSA-01** *(deferred)*: The scholar **seeds parallels** (composition search) from the anchor
  passage to surface shared-distinctive-phrase candidates across the corpus.
- [⏸] **JSA-02** *(deferred)*: **Corpus-driven suggest-then-search completion** — from the first/last
  *N* words of a torn line, the Workbench surfaces candidate **completions** found in the corpus; the
  scholar picks one to search.
- [⏸] **JSA-03** *(deferred)*: **`[` / `]`-aware torn-word completion** — the torn-word markers drive
  a completion search (e.g., `…את הש[` → candidates beginning `[מים ואת הארץ`).

### New Build — Composition / Parallels LOCAL Corpus (COMP-LOC) — Phase 110, desktop

> **Added 2026-06-08 (/gsd-discuss-phase 110)** as the repurposed Phase 110 scope (Component B
> deferred). Wires the LOCAL ("My Library") corpus into composition/parallels search and un-gates the
> deferred composition-report LOCAL export (EXP-F3). Desktop-only (web has no composition UI; LOCAL is
> desktop-only).

- [ ] **COMP-LOC-01**: A **pre-search Genizah / Local / ALL corpus selector** on the composition /
  parallels tab (mirroring the existing Search-tab selector, `genizah_app.py:5953`) scopes which
  corpus composition searches. The selector is **orthogonal to the composition search MODE** — both
  standard and **Lab** modes honor it; **"Lab Mode" is NOT hardwired to LOCAL** (it searches whichever
  corpus the dropdown selects, exactly like regular search). *(No post-search LOCAL filter activation
  this phase — pre-search scoping only, user decision 2026-06-08.)*
- [ ] **COMP-LOC-02**: Composition search **executes against the selected corpus** — **Local** returns
  only LOCAL hits, **ALL** returns Genizah + LOCAL **merged**, **Genizah** is unchanged from today;
  results render into the existing composition results surface. A **stale LOCAL LAB index** surfaces a
  rebuild / staleness signal rather than silently omitting LOCAL composition hits.
- [ ] **EXP-F3** *(promoted from Future)*: **Composition-report LOCAL export** — `export_comp_report`
  (`genizah_app.py:20447`) becomes LOCAL-aware so a Local/ALL composition run exports LOCAL hits with
  local-meaningful columns (filename / folder / filepath / page / matched-text), reusing the Phase 103
  export helpers (`shared/export_dossier.py`, `shared/docx_export.py`). No longer gated — Phase 110
  builds the LOCAL composition-search UI it was waiting on.

---

## Design-Critique Conclusions & Amendments (2026-06-03)

The deferred **Genizah-scholar design-critique session is COMPLETE** — run as a build-a-throwaway-
sketch exploration (`desktop/join_workbench.py`, validated across ~6 UAT iterations). Evidence:
`.planning/spikes/002-assisted-join-workbench/` (CODEX-CRITIQUE, CODEX-PRODUCTIONIZE-CRITIQUE,
DESKTOP-INTEGRATION-NOTES). **The roadmap is now UNBLOCKED.**

### Validated shape (what the scholar reacted to and approved)
Anchor pane (image via the proven `enrich_metadata` → `images_nli/ext` route + zoom + folio
prev/next + brief metadata, dark-mode/RTL safe) | a **line-by-line query builder for BOTH sides of
the leaf** | candidates as grid/table (deduped one-per-image) with material + visual-similarity +
highlighted snippet + Y/?/N triage + four actions | side-by-side compare.

### Amendments to existing requirements
- **JWB-05 (tear-side assist) — FIX THE INVERTED RULE.** Verified vs the corpus: **start-`]` = LEFT
  half (beginning torn), end-`[` = RIGHT half (end torn)** (8.2:1 / 3.35:1). The original text was
  backwards. Also DOWNGRADE prominence: on 2,178 known physical joins a clean complementary L+R read
  fires on only **2.5%**; the dominant verdict is "both edges torn" (55%). Make "both edges"
  first-class; stay silent ~38% of the time.
  - **DEFERRED out of Phase 108 (2026-06-04 discuss-phase).** 108 is the *manual* finder; the
    side-verdict UI earns its keep in the *algorithmic* Component B → **reassigned to Phase 110's
    discuss-phase disposition** (keep / spike / cut, alongside JSA-03 — both are `[`/`]`-driven).
    NOTE: bracket-aware *matching correctness* (a leading `]` must not defeat line-start / self-match
    — 106 R-02) stays in Phase 108; only the side-verdict assist UI is deferred.
- **JWB-06 (seed search) — REFRAME.** Do NOT pre-seed the anchor's own line text; the scholar hunts
  what is **MISSING** (the continuation), not what's present. Replaced by the builder (JWB-10).
- **JWB-04 / JWB-09 (joins) — JOIN MODEL DECIDED (user, 2026-06-03): reuse the existing
  pairwise→group pattern.** Persist 2-fragment join records (`fragment_a/b` + `relationship_type` +
  notes, via `corrections_client.create_join` / `JoinsManager.create_join_local` → Supabase +
  joins_cache.pkl); present GROUPS via the existing BFS transitive closure
  (`JoinsManager.get_connected_fragments_by_id` — A+B, A+C, B+D → {A,B,C,D}). **No new schema.**
  JWB-04 shows the anchor's connected group; JWB-09 "Add as Join" persists pairwise + refreshes it.

### New requirements (validated by the critique)
- [ ] **JWB-10 — Line-by-line query builder.** Rows = manuscript lines; per-row line START/END
  anchors; "↓ N lines" gap → composes the engine's line-break syntax (`|` groups, `[|N]` line-gaps).
  RTL: the line-START anchor sits on the right (Hebrew line start).
- [ ] **JWB-11 — Cross-side AND/OR.** An identical builder for the OTHER side of the leaf (= adjacent
  image p±1; first→+1, last→−1, middle→both). Query B runs through the engine; matching is
  `(sys_id, page±1)` set membership. AND narrows a flood; OR widens a poor yield. (Distinct from
  JOINS-F1's cross-LINE offset; this is cross-SIDE and needs no spike.)
- [x] **JWB-12 — Unified candidate sources (folds in Visual Similarity).** ✅ Phase 109 (2026-06-08; 3/3 SC verified, UAT-approved). NOTE: the badge scheme evolved during gap rounds to a single 👁 eye badge (G-06/G-09, replacing ★both/⊙VS/✎text), the 3-radio selector became a Visual Similarity toggle (G-04), and the JoinsDialog pick-mode hook was retired (G-08) rather than kept — all user-approved. One surface, three
  sources: text / visual-similarity look-alikes / combined (provenance badges ★both / ⊙VS / ✎text,
  both-first ordering). Soft-retire the standalone Visual Similarity dialog (reach parity → reroute
  its entry points → deprecate; keep the JoinsDialog pick-mode hook). Every candidate carries the
  four actions (Browse / Puzzle / Add to List / Add as Join). + a self-match verification readout
  ("anchor matches this query ✓/✗") and an "include anchor itself" toggle.

### Build constraints (architecture — Codex productionize critique, agreed)
- Extract the **pure logic** (query composition, cross-side membership, dedup/compaction, VS/text
  merge, snippet/page helpers) into a **shared, tested module** (web-reusable; no PyQt, no direct
  `fist_data/*.db`) behind a `SearchExecutor` adapter. **Unit-test before UI.**
- Use shared services (visual_similarity / FJMS-measurement / metadata-image), not ad-hoc sqlite.
- Replace private `_vs_*` calls with **public action APIs**.
- **i18n from the start** (acceptance criteria, not cleanup). **Desktop-first UI; web is a LATER
  phase on the same shared API** (web-usable now, not built now). Batch per-candidate calls (perf).
- The throwaway sketch remains the **executable spec** (reversible: `JOINS-SKETCH` markers + REVERT.md).

### Deferrals / discuss-phase questions (CARRY FORWARD — discuss before phases lock)
1. **N-fragment join richness** beyond transitive grouping — tentative/uncertain joins, per-edge
   evidence/confidence/notes? (Codex's top data-model risk; pairwise→group chosen for v8 — confirm
   the richer model stays deferred.)
2. **Dimensions = evidence badges + soft warnings, NOT hard filters** (true vertical-tear halves
   differ in width). **CONFIRMED (2026-06-04 discuss-phase):** soft evidence + a soft size-mismatch
   hint, **never an *automatic* cull** — PLUS an *opt-in* explicit min/max size filter, **off by
   default** (power-user cull, user-invoked). SC#7's "never an automatic hard filter" holds.
3. **Builder depth**: per-row variation columns? editable raw composed-query preview? page-level
   Text START/END in addition to per-row line START/END?
4. **VS-dialog retirement timing** — reroute this phase or a follow-up?
5. **Web-parity phase** — when, and which UI subset?
6. **JSA-02 / JSA-03** — keep / spike / cut for v8? (Earlier lean: JSA-01 only; spike JSA-03; cut JSA-02.)
7. **"Other side" = adjacent image p±1** confirmed; revisit for multi-leaf manuscripts?

---

## Future Requirements (not v8.0.0)

- **JOINS-F1**: **Relative-offset cross-line positional search** — find a fragment with word A near
  line *i* and word B near line *i+k* (the "נשמע at line x, אמר at line x+4" example). Fits the
  two-phase architecture (Tantivy candidates → post-filter by line distance over `L{n}:word` /
  per-line arrays) but the relative-offset matcher needs a perf + correctness **SPIKE** before
  committing to a phase.
- **JOINS-F2**: **Dicta / Sefaria citation-ID** as a sharper completion source for canonical texts
  (Bible / Talmud / fixed liturgy) — stronger than corpus frequency for cited material.
- **JOINS-F3**: **Batch export** of many suggestions for offline review + a **persisted personal
  candidate list with re-import** back into the Workbench (beyond plain "add to list").
- **JOINS-F4**: **Automated ranked join-finder** (research-only v7/v8 two-hop + visual rerank) —
  XL, ~90s/fragment, ≤47% Recall@50, no code exists; explicitly **OUT** of the human-in-the-loop
  product.
- **PERF-F1** (carried): D-F12 — regular Search ~constant ~8s wall-clock (profile-first effort).
- ~~**EXP-F3** (was LEXP-02): Composition-report LOCAL export~~ → **PROMOTED into v8.0.0 Phase 110**
  (2026-06-08) — the LOCAL composition-search UI it was gated on is now being built in the same phase.

### Component B — Search-support algorithms (DEFERRED from v8.0.0, 2026-06-08)

The full Component B (Join Workbench search-support algorithms) was deferred out of v8.0.0 at
`/gsd-discuss-phase 110` so the milestone could ship. These ride the **completed** Component A
(Phases 106–109) and re-enter scope in a **post-v8.0.0 milestone**:

- **JSA-01**: Seed parallels (composition search) **from the anchor passage** in the Join Workbench →
  shared-distinctive-phrase candidates into the candidate surface. *(Note: distinct from COMP-LOC-01
  above — JSA-01 is anchor-driven seeding inside the Workbench; COMP-LOC is the corpus selector on the
  standalone composition tab.)*
- **JSA-02**: Corpus-driven suggest-then-search completion (first/last *N* words of a torn line).
- **JSA-03**: `[` / `]`-aware torn-word completion.
- **JWB-05**: Conservative `[` / `]` tear-side assist (start-`]` = LEFT, end-`[` = RIGHT; "both edges
  torn" first-class; silent when unclear). Deferred from Phase 108 → was Phase 110 disposition → now
  deferred to post-v8.0.0 with the rest of Component B.

## Out of Scope (v8.0.0)

| Feature | Reason |
|---------|--------|
| Automated / auto-ranked join finder | Human-in-the-loop by design — the scholar is the ranker. The auto-algorithm is slow + low-recall + has no code (research-only). → JOINS-F4 |
| New search-engine modes | The Workbench rides the **existing** search module (variants/fuzzy/Responsa/regex); no new modes for the MVP. |
| New index or sidecar | `line_starts` / `line_ends` already present (web + most desktop users); no new index/DB needed for the MVP. |
| One-click scholarly citations | Parked by user ("keep in the bucket — not sure how useful"); stays in `docs/FEATURE_IDEAS.md` backlog. |
| Relative-offset positional search | Spike-gated → Future (JOINS-F1). |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| BRAND-01, 02 | pre-release polish (no phase) | Delivered |
| LEXP-01, 03–08 | 103 | Delivered |
| EXPUX-01–04 | 105 | Delivered (EXPUX-01 UAT pending) |
| (foundational logic for JWB-10/11/12 + build constraints) | 106 (shared core) | Active |
| JWB-01, 02, 03, 04, 09 | 107 (desktop frame + actions + join model) | Active |
| JWB-06 (reframed), 07, 08, 10, 11 | 108 (desktop builders + candidates + compare) | Active |
| JWB-12 (unified sources + VS merge) | 108 (text/combined surface) + 109 (VS source + soft-retire) | ✅ Complete (Phase 109, 2026-06-08) |
| COMP-LOC-01, 02 + EXP-F3 | 110 (composition/parallels LOCAL corpus + LOCAL export) | Active |
| ~~JSA-01, 02, 03 + JWB-05~~ | ~~110~~ → **DEFERRED to post-v8.0.0** (2026-06-08) | Deferred |

**Coverage:**
- Delivered (folded): BRAND (2) + LEXP (7) + EXPUX (4) = 13.
- New active: JWB (9, Phases 106–109 ✅ complete) + COMP-LOC (2) + EXP-F3 (1) = 12, mapped to Phases
  106–110. **Component B / JSA (3) + JWB-05 DEFERRED** out of v8.0.0 (2026-06-08, /gsd-discuss-phase
  110) — Phase 110 repurposed to the LOCAL-composition wiring. Web Joins Lab UI deferred to a later
  phase. After Phase 110 → `/release` v8.0.0.

---
*Requirements defined: 2026-06-02. v7.17 folded into v8.0.0 per user decision; Phases 103/105 kept as delivered (no destructive phase-clear). Roadmap intentionally deferred pending a Genizah-scholar design-critique session.*
