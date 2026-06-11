---
phase: 107
reviewers: [codex]
reviewed_at: 2026-06-04
plans_reviewed: [107-01-PLAN.md, 107-02-PLAN.md, 107-03-PLAN.md]
verdict: FIX-FIRST
overall_risk: HIGH
note: >
  Codex was run twice. Pass 1 (sandbox default) FAILED to read the repo on Windows
  ("windows sandbox failed: spawn setup refresh") — all 9 plan↔code drift checks came back
  UNVERIFIABLE; it produced a text-only review. Pass 2 (--sandbox danger-full-access) read the
  live codebase and resolved every drift claim with real file:line evidence. Pass 2 is the
  authoritative review; Pass 1's unique concerns are folded into the consensus below.
---

# Cross-AI Plan Review — Phase 107 (Desktop Join Workbench)

## Codex Review (Pass 2 — grounded against live code, AUTHORITATIVE)

**Plan↔Code Drift Findings**

1. **DRIFT** — `JoinsManager.get_connected_fragments_by_id` exists at `genizah_core.py:10127` (not ~9936).
   Signature: `def get_connected_fragments_by_id(self, document_id: str) -> dict`. Return shape:
   `{'document_id', 'fragments', 'joins', 'total_fragments', 'total_joins'}` (`genizah_core.py:10193`) —
   **a dict, NOT a flat list of fragments**. Returned `joins` are raw cache joins and can carry `source`
   (`source` from server sync at `:10316`; local `source='user'` at `:10399`). It does **NOT** fetch
   PGP/FJMS/community. PGP and FJMS are separate (`corrections_ui.py:3750` / `:3547`); community is
   separate via Supabase client (`supabase_corrections_client.py:1700`).

2. **CONFIRMED** — `JoinsDialog.__init__` supports anchor-only open (`corrections_ui.py:3281-3293`; no
   partner arg). Fragment A read-only (`:3409-3418`); Fragment B empty/free-entry (`:3426-3428`).

3. **CONFIRMED** — `_vs_open_joins_with_partner(orig_sys_id, orig_shelfmark, partner_sys_id, partner_shelfmark)`
   at `genizah_app.py:5239`; `_vs_add_to_puzzle(partner_sys_id)` at `:5261`. Public wrappers are
   conceptually correct, but these are UI methods — call ONLY from UI-thread button handlers.

4. **CONFIRMED, with adjacent drift** — ResultDialog uses `self.current_p_num` (declared
   `desktop/result_dialog.py:68`, assigned `:2341`/`:2500`). `self.p_num` is wrong (plan already fixed
   this). **ALSO:** live text state is `self.current_page_text` (`:70`, `:2347`), and the live result dict
   is **`self.data`** (`:2026`), **NOT `self.current_result`** (which does not exist).

5. **DRIFT / PARTIAL** — `enrich_metadata` returns `images_nli` + `images_ext` (`genizah_core.py:4518-4519`);
   NLI entries are base URLs (`:4475-4476`); `ImageLoaderThread(url)` takes a URL (`desktop/image_loader.py:26`).
   `iiif_full(base + "/full/2000,/0/default.jpg")` is right for NLI bases. BUT blindly choosing
   `images_nli or images_ext` (NLI-first) **drifts from live priority**:
   `current_meta['images'] = images_ext if images_ext else images_nli` (`:4517`, ext-FIRST), with external-
   provider comments at `:4489-4497`. Use `meta['images']` or mirror that ext/NLI priority — not NLI-first.

6. **CONFIRMED** — `apply_line_numbered_text(widget, rendered_html_or_text, *, source_text=None, pages=None, is_html=True)`
   at `desktop/widgets/line_number_text_edit.py:332-339`. Planned keyword-call shape is valid.

7. **DRIFT** — `TRANSLATIONS` is in `genizah_translations.py:4`; `tr()` is in `genizah_core.py:2735`
   (imports TRANSLATIONS at `:46`). Existing keys present for `Browse manuscript`, `Add to Puzzle`,
   `Add to List`, `User`, `Community`, `No image`, and **`Add as Join` ALREADY EXISTS** (`genizah_translations.py:3226`).
   The "12 added keys all absent" claim is false — **11 absent; `Add as Join` would clobber an existing
   same-value key** (harmless value-wise, but the bootstrap/verify list and i18n closed-set count are off).

8. **CONFIRMED** — `tests/test_pgp_filter_cascade.py` exists with an AST parse/walk guard (`:12-66`). Proposed
   static guards are sound for `desktop/join_workbench.py`, **but the i18n guard does NOT cover the new
   Plan-03 strings in `genizah_app.py` / `desktop/result_dialog.py`** (SC#6 is broader than the guard scope).

9. **DRIFT** — `_create_action_button` exists at `genizah_app.py:18561`, and `ActionsHoverWidget.add_btn(...)`
   is used in result TABLES (`:17056-17077`). **But ResultDialog's action row uses direct `QPushButton`s
   (`desktop/result_dialog.py:270-348`), and Browse `ext_info_row` also uses direct `QPushButton`s
   (`genizah_app.py:6966-7037`)** — the planned `_create_action_button`+`add_btn` pattern is wrong for these
   two hosts. **ALSO Browse page state is `self.current_browse_p` (`genizah_app.py:3200`, `:23705`), NOT `self.p`.**

**Summary.** The plans are directionally strong but not ready as written. The biggest drift is in host entry
state (`current_result`/`p` do not exist), image-list priority, community-source access assumptions, and the
known-joins group model. The plan can land cleanly once these are fixed before execution.

**Strengths.** JoinsDialog anchor-only reuse confirmed/low-risk; public-wrapper direction correct for SC#5;
anchor image via `enrich_metadata`+`iiif_full`+`ImageLoaderThread` is the right family; AST-guard pattern is
appropriate.

**Concerns.**
- **HIGH** — Plan 03 Browse hook uses `self.p`; live code is `self.current_browse_p` → anchors the wrong folio.
- **HIGH** — Plan 03 ResultDialog hook uses `self.current_result`; live code is `self.data` → may launch a
  skeletal result dict, losing text/title/uid context.
- **HIGH** — Plan 02 known-joins rows are built per deduped pair, not per connected fragment. For A-B-C
  transitive joins the "other side" logic can miss C or duplicate B. (Return shape is a `{fragments, joins}`
  dict, not a flat list.)
- **HIGH** — Plan 02 NLI-first image list can ignore live external-image priority and hit NLI stubs/503s for
  external-provider manuscripts.
- **MEDIUM** — Community joins only on `SupabaseCorrectionsClient`; REST fallback lacks
  `get_published_joins_for_fragment`.
- **MEDIUM** — Re-anchor during in-flight loads needs generation tokens; `cancel()`/`quit()` alone won't stop
  blocking worker `run()` bodies (stale repaint).
- **MEDIUM** — Thumbnail worker should not create/emit `QPixmap` from a worker thread; emit `QImage`/bytes,
  convert on the UI thread.
- **LOW** — i18n guard only scans `join_workbench.py`; won't catch hardcoded strings in host files.

**Suggestions (mapped to plan/task).**
- **03 Task 2:** build ResultDialog anchor from `self.data`, `self.current_sys_id`, `self.current_p_num`,
  `self.current_page_text`, `self.current_page_uid`.
- **03 Task 3:** replace `getattr(self, "p", 1)` with `self.current_browse_p`.
- **03 Tasks 2/3:** entry buttons are direct `QPushButton`s (mirror `result_dialog.py:270-348` /
  `genizah_app.py:6966-7037`), not `_create_action_button`+`add_btn`.
- **02 Task 1:** select images via `meta.get("images")` first, or exactly mirror `enrich_metadata`'s ext/NLI
  priority.
- **02 Task 2:** build connected-fragment/member rows from `result['fragments']` + all endpoints, not one row
  per edge; assign provenance from the best incident edge; generic `Known join` when ambiguous.
- **02 Task 2:** guard community calls with `hasattr(client, "get_published_joins_for_fragment")`.
- **02 (all workers):** add latest-wins generation/token checks for anchor load, page text, known joins, thumbnails.
- **01 Task 4 / 03:** extend i18n guarding (or add targeted AST tests) to the new `genizah_app.py` and
  `desktop/result_dialog.py` strings.
- **01 Task 1:** drop `Add as Join` from the "added" set (already exists at `genizah_translations.py:3226`);
  it's 11 new keys, not 12. Update the closed-set count and verify list.

**Risk Assessment.** Overall **HIGH as written**. Verdict: **FIX-FIRST**. Must-fixes: ResultDialog state
source, Browse page attr, image-list priority, connected-group row model, community-client fallback, and
stale-worker generation tokens. Once corrected, the phase is executable without scope creep into candidate
search or Compare.

---

## Codex Review (Pass 1 — text-only; sandbox could not read repo)

Pass 1 could not read the codebase (Windows sandbox spawn failure), so its 9 drift checks were all
UNVERIFIABLE. Its non-drift concerns (still valid, several reinforced by Pass 2):
- **HIGH** — workers lack latest-wins tokens → stale-anchor repaint after rapid re-anchor (✓ confirmed Pass 2).
- **HIGH** — `ThumbBatchWorker` emits `QPixmap` from a worker thread → emit `QImage`/bytes, convert in slot (✓ Pass 2).
- **MEDIUM** — `_BADGE_CONFIG` stores `tr("User")`/`tr("Community")` at **import time** → freezes labels if the
  language changes after import; call `tr()` inside `badge_for_source` instead. **(Unique to Pass 1 — fold in.)**
- **MEDIUM** — i18n AST guard scans only `join_workbench.py` but Phase 03 adds strings in `genizah_app.py` and
  `desktop/result_dialog.py` (✓ Pass 2).
- **MEDIUM** — `build_known_join_rows` anchor matching by shelfmark vs sys_id can mislabel "other" when sources
  mix ids and shelfmarks (✓ related to Pass 2 group-model finding).
- **LOW** — cold-start ambiguity handling should cover `options` length 1 with no top-level `sys_id`.

---

## Consensus Summary — Must-Fixes Before Execution

These are the actionable items for `/gsd-plan-phase 107 --reviews` to fold into the plans. All are
plan↔code-drift or PyQt-correctness fixes; none change phase scope or the locked CONTEXT decisions.

### BLOCKER-class (HIGH) — wrong/nonexistent live symbols, will break at runtime or render
1. **Plan 03 Task 2** — ResultDialog anchor must read `self.data` (not `self.current_result`), plus
   `self.current_sys_id`, `self.current_p_num`, `self.current_page_text`, `self.current_page_uid`. (`result_dialog.py:68/70/2026/2341/2347`)
2. **Plan 03 Task 3** — Browse anchor must read `self.current_browse_p` (not `self.p`). (`genizah_app.py:3200/23705`)
3. **Plan 03 Tasks 2/3** — entry buttons are direct `QPushButton`s (hosts don't use `_create_action_button`+`add_btn`);
   mirror `result_dialog.py:270-348` and `genizah_app.py:6966-7037`.
4. **Plan 02 Task 1** — image selection must follow live ext/NLI priority: prefer `meta.get("images")` (already
   `images_ext if images_ext else images_nli`), NOT `images_nli or images_ext`. (`genizah_core.py:4517-4519/4489-4497`)
5. **Plan 02 Task 2** — known-joins must be built from the `{fragments, joins}` dict (return of
   `get_connected_fragments_by_id`, `genizah_core.py:10127/10193`) as per-connected-MEMBER rows, not per-deduped-edge;
   PGP/FJMS/community are fetched SEPARATELY (`corrections_ui.py:3750/3547`, `supabase_corrections_client.py:1700`) — the
   transitive-closure dict alone does not include them.

### SHOULD-FIX (MEDIUM) — correctness / robustness
6. **Plan 02 Task 2** — guard community fetch with `hasattr(client, "get_published_joins_for_fragment")` (REST fallback lacks it).
7. **Plan 02 (workers)** — add latest-wins generation tokens for anchor/page-text/known-joins/thumbnail workers; `cancel()`/`quit()` won't interrupt a blocking `run()`.
8. **Plan 02 Task 2** — `ThumbBatchWorker` emits `QImage`/bytes, converts to `QPixmap` on the UI thread.
9. **Plan 01 Task 2** — badge config must call `tr()` at call time inside `badge_for_source`, not store translated labels at import time.
10. **Plan 01 Task 4 + Plan 03** — extend i18n AST guard (or add sibling guards) to cover new strings in `genizah_app.py` and `desktop/result_dialog.py` (SC#6 is broader than `join_workbench.py`).
11. **Plan 01 Task 1** — `Add as Join` already exists (`genizah_translations.py:3226`): 11 new keys, not 12; update bootstrap list, verify assertion, and the closed-set count.

### NICE-TO-HAVE (LOW)
12. **Plan 03 (cold start)** — handle `options` of length 1 / no top-level `sys_id` in the shelfmark resolver path.

### Agreed across both passes
Latest-wins worker tokens (HIGH), `QPixmap`-on-worker-thread (HIGH→fix), and i18n-guard-scope vs SC#6 (MEDIUM)
were independently raised in both passes — highest confidence.

### Not in scope (correctly excluded by the plans)
No candidate search, query builder, Compare dialog, visual-similarity, or parallels leaked in — Pass 2 confirms
phase boundary is clean.
