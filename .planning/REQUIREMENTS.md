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
- [ ] **JWB-05**: A **conservative** tear-side assist reads the anchor's `[` / `]` transcription
  markers (lines ending `]` → right side; starting `[` → left side; mid-line marks a torn word) and
  **suggests** the likely side / search direction **only when the evidence is clear**; otherwise it
  stays silent. The scholar can always override. (Real fragment state is messy — the assist must
  never assert a guess.)
- [ ] **JWB-06**: From the anchor (a selected line / torn word + a **direction**: rest-of-line,
  line-above, lines-below, lines-above, previous/next page), the scholar **seeds a search** that
  pre-fills the **existing** search module (variants / fuzzy / Responsa / regex); the seeded query is
  fully editable (try-and-error preserved).
- [ ] **JWB-07**: The scholar collects search results as **candidates in a list** within the Workbench.
- [ ] **JWB-08**: The scholar compares the anchor and a candidate **side-by-side** (image +
  transcription) to confirm a join by eye.
- [ ] **JWB-09**: On a confirmed join the scholar can **act**: add it via the **existing joins
  button**, **export** the details (clipboard / file), and **add candidates to a list**.
  (Open-in-Puzzle remains available as optional polish — both apps have the Puzzle.)

### Component B — Search-support algorithms (JSA) — secondary, independent, both apps

- [ ] **JSA-01**: The scholar **seeds parallels** (composition search) from the anchor passage to
  surface shared-distinctive-phrase candidates across the corpus.
- [ ] **JSA-02**: **Corpus-driven suggest-then-search completion** — from the first/last *N* words of
  a torn line, the Workbench surfaces candidate **completions** found in the corpus; the scholar picks
  one to search.
- [ ] **JSA-03**: **`[` / `]`-aware torn-word completion** — the torn-word markers drive a completion
  search (e.g., `…את הש[` → candidates beginning `[מים ואת הארץ`).

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
- **EXP-F3** (carried, was LEXP-02): Composition-report LOCAL export — gated on a LOCAL
  composition-search UI.
- **PERF-F1** (carried): D-F12 — regular Search ~constant ~8s wall-clock (profile-first effort).

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
| JWB-01..09 | TBD — roadmap pending scholar critique | Active |
| JSA-01..03 | TBD — roadmap pending scholar critique | Active |

**Coverage:**
- Delivered (folded): BRAND (2) + LEXP (7) + EXPUX (4) = 13.
- New active: JWB (9) + JSA (3) = 12. Unmapped to phases ON PURPOSE — roadmap deferred until after the Genizah-scholar design-critique session.

---
*Requirements defined: 2026-06-02. v7.17 folded into v8.0.0 per user decision; Phases 103/105 kept as delivered (no destructive phase-clear). Roadmap intentionally deferred pending a Genizah-scholar design-critique session.*
