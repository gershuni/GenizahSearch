# SEED-006 — Adversarial code review of the *execution*

> **Date:** 2026-06-21 · **Branch:** `claude/gracious-darwin-4r6vdu` · **Commit reviewed:** `9ee4156`
> **Inputs:** `SEED-006-PLAN-hebrew-search-tokenizer.md`, `SEED-006-CODEX-BRIEF-hebrew-search-tokenizer.md`
> (plan **not** taken at face value). Ruthless, pitfall-first; severity-tagged.

## Method (what was actually checked, not assumed)

- Read the plan, brief, seed, and the full diff across all 6 touched files
  (`shared/search_tokenizer.py`, `genizah_core.py`, `shared/local_indexer.py`,
  `tests/test_hebrew_search_tokenizer.py`, + 2 patched existing test files).
- Enumerated **all 10** `tantivy.Index(...)` / `Index.open(...)` sites and every
  `parse_query` / `build_tantivy_query` call site; traced tokenizer-registration
  coverage and the Stage-2 gating at each.
- Empirically probed the `hebword` tokenizer on tantivy **0.25.1** (comma split,
  dot/bracket/apostrophe/gershayim kept, maqaf kept, sof-pasuq kept, `⟦⟧` split,
  `\w` Unicode overlap).
- Ran suites: **35/35** `test_hebrew_search_tokenizer.py`; **51** local-index
  tests (incl. the unpatched raw-construction files); **119** search/composition/
  responsa tests (2 unrelated failures = missing `supabase` module in the probe
  env, not SEED-006). `ruff` clean on all changed files.
- Verified the failure mode: an unregistered `hebword` raises
  `ValueError: Schema error: 'Error getting tokenizer for field: content'` at
  write time → registration is load-bearing (and is in fact complete).

## Verdict

The **retrieval** engineering is sound and the staged architecture is the right
call. But: the change is **not safe to ship to desktop as-is**, the **headline
production bug stays unfixed on the web** until a separate manual step that
nothing enforces, and **one query path was silently missed by both the plan and
the execution**. Net: solid core, unsafe rollout, one real coverage gap.

**Genuinely good (verified):** registration covers all 10 index sites; the compat
gate degrades cleanly on an old index (no crash); stored `content` is untouched
so display + highlight-offset are safe; bracket-gating is correct and tested;
position fields correctly stay `whitespace`; the pivot away from the brief's
original "repoint retrieval to `content_search`" to the in-place staged approach
was the correct correctness call (avoids the highlight-offset mismatch).

---

## CRITICAL

**None.** In the intended forward rollout (code-first, compat-gated) there is no
crash or data-loss path. The compat gate and registration coverage were
specifically attacked and held.

---

## HIGH

### H1 — Ships the trigger for a UI-thread freeze while deferring the fix the plan called for
`build_local_schema` changed (new `content_search` field + `content` →
`hebword`), so `_compute_schema_marker` produces a new hash. For **every existing
desktop user**, `shared/local_indexer.py:1739-1742` sees `_schema_mismatch` →
`_needs_rebuild` → **synchronous `rebuild_main_index_atomic(...)` inside
`LocalIndexer.__init__`** (`shared/local_indexer.py:1777`), which runs on the UI
thread (`desktop/my_library_tab.py:1082`). It re-adds every page from
`cached_text` *and* now computes `strip_search_diacritics` per page → a
multi-second-to-minutes first-launch freeze that scales with library size.

The plan adopted "**LOCAL rebuild moved off the UI thread (HIGH-5)**" as one of
the Codex BLOCK fixes — it was deferred, while the schema bump that *causes* the
freeze was shipped. This codebase has been burned by this exact pattern before
(CLAUDE.md: Phase 101 D-04 rollback "froze 12K-PDF library at launch (synchronous
`startup_recovery()` … on UI thread)"; D-F13c). **The two must ship together:**
gate the rebuild behind `is_searchable=False` on a background `QThread` *before*
merging the schema change. Web is unaffected (it `.open()`s; no marker rebuild).

---

## MEDIUM

### M1 — LOCAL composition / parallels silently skips Stage 2 (missed by plan *and* execution)
`genizah_core.py:9461` builds `_local_fields_scl = ["content", "content_head",
"content_tail"]` (no `content_search`) and `:9464` calls
`build_tantivy_query(_chunk_scl, mode)` **without** `content_search_field`, and
never strips diacritics off the chunk terms. So `צמאן` finds `צ̇מאן` in a
*regular* LOCAL search but **not** in a LOCAL composition/parallels run —
inconsistent with the regular LOCAL path (`:7504-7506`) and the main composition
path (`:9322`, which *does* pass `_cs_field`). The plan enumerated three
retrieval sites (main / main-composition / LOCAL-regular); there are **four** —
the Phase-110 LOCAL-composition hook was never accounted for. Untested and
*undocumented* (not in the deferral notes) → a silent gap.
**Fix:** one-line parity — append `content_search` to `_local_fields_scl` when
`_local_has_content_search`, and strip diacritics off the chunk terms (mirror
`_query_local_index`).

### M2 — Corpus-wide ranking shift on `content`, with the plan's required regression not done
`content` `whitespace` → `hebword` changes token DF/IDF for every
punctuation-adjacent word → BM25 re-scores across the corpus once the index is
rebuilt (bounded to the ~3% affected words, but it reorders real result sets).
The plan explicitly required a "**Golden-query regression set across modes**";
the execution shipped only synthetic tiny-index tests
(`test_ranking_exact_above_fold` checks *content vs content_search* ordering, not
the `whitespace`→`hebword` DF shift on real data). The intended improvement is
plausible but **unverified against the corpus it actually affects.**

### M3 — GENIZAH rollout fully deferred with nothing enforcing it
The headline bug (`בסגן` ↛ `בסגן,`) stays **unfixed on the live web corpus** until
a manual `create_index` rebuild + redeploy. Unlike LOCAL, the main index has **no
schema marker** — nothing detects staleness. The fix is inert in production on
merge, and easy to forget. **Rollback asymmetry:** safe order is code-first
(compat gate handles the old index); but once the *new* index is deployed,
rolling back the *code only* makes every `content` query raise
`Schema error: 'Error getting tokenizer for field: content'` (reproduced). Needs
an explicit runbook line + ideally a main-index version marker.

---

## LOW

- **L1 — Tests mirror the retrieval layer instead of driving it.**
  `_genizah_retrieve` / `_local_retrieve` re-implement what `execute_search` /
  `_query_local_index` do, so a regression in the real call site (e.g. `_cs_field`
  threading, the metacharacter-strip fallback) wouldn't be caught — compensated
  only by brittle `inspect.getsource(...)` string guards (§8). **Invariant 1
  (bracket-exact) is tested for Genizah but not LOCAL**; the real LOCAL `[סגן`
  path hits the pre-existing metacharacter-strip fallback (`:7516` strips `[`), so
  LOCAL bracket-exactness leans entirely on the regex backstop the helper bypasses.

- **L2 — Hebrew-script punctuation isn't actually split.** The explicit
  `֐-׿` class keeps sof-pasuq (U+05C3), paseq (U+05C0), maqaf inside
  tokens (verified `דבר׃` → one token). The plan's "splits … period/colon" isn't
  met for the Hebrew-script equivalents. Not a regression (whitespace kept them
  too); the ~75% ASCII-punctuation bulk *is* handled.

- **L3 — Pattern partly redundant / comment misleading.** Rust-regex `\w` already
  matches Hebrew letters *and* U+0300–036F marks; those ranges are largely
  redundant. The Hebrew range is load-bearing only for maqaf/Hebrew-punct — i.e.
  for *keeping* them, the opposite of splitting. `search_tokenizer.py:44`'s
  "keeps Hebrew/marks" rationale overstates the ranges' role.

- **L4 — `_index_has_field` matches error-message substrings.** Keys on
  `'does not exist'`/`'not defined'` in the `ValueError` text (`:6683-6688`).
  Confirmed `schema.get_field` genuinely doesn't exist on 0.25.1, so this is the
  pragmatic option — but it fails **open** if a future tantivy reworks the wording
  (→ emits a `content_search:` clause against a fieldless index → crash). Pin the
  behaviour with a version guard or a test that re-asserts the wording.

- **L5 — LAB index left behind.** `build_local_lab_schema` (`content` = `simple`)
  is untouched → LAB-mode search/composition keeps the punctuation bug. The brief
  explicitly asked about LAB; the exclusion is defensible ("barely used") but
  neither closed nor documented.

- **L6 — Nits.** OPEN_ISSUES.md + CLAUDE.md attribute SEED-006 to branch
  `claude/optimistic-brown-m0gffa`, but the commit landed on
  `claude/gracious-darwin-4r6vdu`. Three repeated lazy
  `from shared.search_tokenizer import register_search_tokenizers` imports in
  `genizah_core.py` (`:5748/:7209/:7738`) where one module-top import would do.
  The test module pulls `build_local_schema` from `shared.local_indexer`,
  dragging in `fitz`/`docx` — mildly undercutting the "dependency-light, web needs
  no PyMuPDF" rationale at test time (cosmetic; CI has the deps).

---

## Plan-vs-execution scorecard

- **Faithfully executed:** staged in-place tokenizer (no wholesale repoint),
  additive `content_search`, `^5`/`^0.5` boost preservation, bracket-gating,
  position fields on whitespace, compat gate, per-process registration, the three
  *enumerated* retrieval sites.
- **Plan was wrong/incomplete (do not take it for granted):** it enumerated 3
  retrieval sites; there are 4 (missed Phase-110 LOCAL composition → **M1**). It
  claimed a golden-query regression that wasn't done at unit scope (**M2**).
- **Plan said to do, execution deferred:** HIGH-5 off-thread rebuild (**H1**) and
  the Genizah rebuild (**M3**) — both documented as deferred, but **H1 shouldn't
  ship without its trigger's mitigation.**

## Merge-gating recommendation

1. **Fix M1** (one-line parity with the regular LOCAL path) — it's a silent
   correctness gap, cheap to close.
2. **Resolve H1** before the desktop schema bump lands: implement the background
   `is_searchable=False` rebuild, or hold the schema change until it exists.
3. **M2/M3** are release-process items: a real-corpus ranking spot-check, and an
   *enforced* Genizah rebuild step (+ a main-index version marker) so the fix
   isn't inert/forgotten in production.
4. Everything in LOW is cleanup.
