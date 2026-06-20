# Fix Hebrew retrieval: punctuation- and diacritic-attached words are unfindable (LOCAL + Genizah)

## Context

Desktop **My Library** user: searching `בסגן` doesn't find a doc whose text is `בסגן,` (comma), while `סגן`
does (highlighting `בסגן`); re-indexing didn't help.

Search is two-phase — **Tantivy retrieval → regex filter/highlight**. The bug is in **retrieval**: the
searchable `content` field uses **`tokenizer_name="whitespace"`** in both indexes
(`shared/local_indexer.py:682-686`, `genizah_core.py:5726-5730`), which splits only on spaces, so punctuation
stays attached: `בסגן,` → token `בסגן,`, never matched by a clean `בסגן` query. Bare `סגן` finds the page via
another clean token; the substring/mark-tolerant regex then highlights `בסגן`, masking the gap.

Sampling `Transcriptions.txt` (35,912 pages / ~120 MB): ~**3.0%** of Hebrew words appear *only* attached on
their page → unretrievable. ~75% editorial punctuation (`⟦⟧`,`[]`,`.`,`()`,`:`,`/`), ~25% combining diacritics
(U+0307 Judeo-Arabic dot `צ̇מאן`, geresh/apostrophe).

**Existing infra to preserve/reuse:** `strip_search_diacritics` (`:6592`) folds marks+apostrophe+geresh+
gershayim+curly-quotes (queries already call it `:1319/:1534/:8681`); regex filter/highlight is mark-tolerant
(`:6651`) + bracket-aware (`_query_has_brackets:6632`,`_strip_brackets:6642`); `_add_bracket_variants` (`:6616`,
used `:7790/:7863`) OR-expands bracket forms; `L{n}:word` position markers live in `line_starts`/`line_ends`
(`_extract_position_fields:5542`, lines 5559/5561) and **depend on the colon staying in the token**.

## User-confirmed invariants (must all hold)

1. `[סגן` → **only** `[סגן` (bracket-in-query = exact). 2. `סגן` → loose: also finds `[סגן`/prefixed forms on
pages containing `סגן`; **no prefix-expansion**. 3. `בסגן` → finds `בסגן,`. 4. `צמאן`/`צ'מאן` → finds `צ̇מאן`.
5. Results display original text (dots/brackets visible).

## Codex review → BLOCK, incorporated (`_tmp/codex-hebrew-search-review.md`)

Codex's wholesale-repoint critique was correct; this plan adopts its **safer staged** architecture and fixes:
in-place tokenizer (no repoint) for the punctuation class; a narrow **additive** `content_search` OR-fallback
only for the dot; **position fields left on whitespace** (HIGH-4, verified — `L{n}:word` markers); bracket
variants **gated** on bracket-free queries (HIGH-2); LOCAL rebuild **moved off the UI thread** (HIGH-5); a
rollout **compat gate** + per-process tokenizer registration (MEDIUM-6); exact-form **ranking boost** preserved
(MEDIUM-7); `⟦⟧`/niqqud handled explicitly (MEDIUM-8). (HIGH-1 "pattern has spaces" was a brief-formatting
artifact — the real pattern has **no literal whitespace**.)

## Fix — two stages

### Tokenizer `hebword` (shared)
Regex tokenizer (tantivy 0.25.0 `Tokenizer.regex`, verified — matches token spans), pattern **with no literal
whitespace**: `[\w֐-׿̀-ͯ'"[]]+` — keeps `\w`, Hebrew (incl. nikud,
maqaf), combining marks, apostrophe, gershayim, and `[ ]`; **splits** space/comma/period/colon/parens/`⟦⟧`/
slash. Register on **every** Index object after each open/create, **per process** (uvicorn workers; mirror
`_ensure_lab_tokenizers:737`) or `parse_query` raises "unknown tokenizer".

### Stage 1 — punctuation fix (the reported bug): in-place, NO query repoint
- Change **only** `content`'s tokenizer `whitespace`→`hebword` in both schemas (`local_indexer.py:682`,
  `genizah_core.py:5726`). Stored value unchanged (display intact); queries still target `content`, so all
  hard-coded `content:` paths (line-break `:8371/:8384`, LOCAL `["content",…]:7442`, composition `:9237`,
  `search_field:8908`) keep working unchanged. `בסגן,`→token `בסגן`. ✅ invariant 3.
- **Leave `content_head/content_tail/line_starts/line_ends` on `whitespace`** — preserves `L{n}:word` colon
  markers (HIGH-4). `content` itself has no `L:` markers, so splitting colon there is safe.
- **Gate `_add_bracket_variants` on bracket-free terms** (`'[' not in term and ']' not in term`) so `[סגן`
  stays exact (HIGH-2) while bare `סגן` still expands to bracketed forms. ✅ invariants 1 & 2. Brackets stay in
  tokens (kept in `hebword`).
- `⟦⟧` (U+27E6/7): split (treated as punctuation; documented minor loss of exact-`⟦⟧` search — `_add_bracket_
  variants` covers only `[ ]`). Optionally normalize `⟦⟧`→`[]` if exact-angle-bracket search is wanted.

### Stage 2 — diacritic/dot fold (Judeo-Arabic): additive `content_search` fallback
- Add non-stored `content_search` (both schemas) = `strip_search_diacritics(page_content)`, tokenized
  `hebword`. `צ̇מאן`→`צמאן`, `אמ'`→`אמ`.
- In `build_tantivy_query`, **add** a lower-weighted `content_search:` OR-clause for the normalized term
  alongside the existing `content` clauses (keep the exact-`content` `^5` boost so original forms still rank
  first — MEDIUM-7). Add `content_search` to the parse_query field list at the **word-search retrieval** sites
  only (`:8434` main, `:9237` composition, `:7442` LOCAL); position/line-break (`line_starts`/`line_ends`) and
  title/shelfmark modes are **not** touched. Queries already `strip_search_diacritics`, so `צמאן`/`צ'מאן`→
  `צמאן` matches the normalized token. ✅ invariant 4. Filter/highlight stay on stored `content` (mark-tolerant)
  → display unchanged ✅ invariant 5.
- niqqud (U+0591-05C7) is **not** folded by `strip_search_diacritics` (Genizah is largely unvocalized; note as
  a separate enhancement if a vocalized corpus needs it — MEDIUM-8).

## Rollout (per index)

- **LOCAL** — changing `build_local_schema` bumps the source-hash `.schema_version` marker
  (`_compute_schema_marker:705`) → auto-rebuild from `cached_text` (SQLite; **no PDF re-extraction**;
  re-normalize `content_search` there). **HIGH-5 fix:** do **not** rebuild synchronously in
  `LocalIndexer.__init__` (`:1744`, runs on the UI thread `my_library_tab.py:1082`). Instead detect
  "needs rebuild" without building the writer, keep `is_searchable=False` (Phase 97 R-01 gate already exists),
  and run `rebuild_main_index_atomic` on a background QThread worker that opens its **own** SQLite connection
  (respect `check_same_thread`), then flip `is_searchable=True`. No manual "Re-index All".
- **GENIZAH** — no schema marker. Add a main-index version/marker. **Compat gate (MEDIUM-6):** querying
  `content_search` against an old index (no such field) fails → **deploy the rebuilt index before the query
  code**, or have the query fall back to `content`-only when `content_search` is absent. Web: rebuild
  `create_index` over `Transcriptions.txt` server-side + deploy (data-first, per deploy lesson). Desktop:
  background `create_index` rebuild gated by the marker (corpus already on disk; never UI-thread) or ship a
  prebuilt index via the sidecar channel.

## Files

- `shared/local_indexer.py` — `hebword` + `content_search` in `build_local_schema`; populate at add/rebuild
  sites (`rebuild_main_index_atomic:3639`); `_ensure_local_tokenizers` + all index-open sites; move marker
  rebuild to background worker.
- `genizah_core.py` — `hebword` + `content_search` in `create_index` (populate `:5779/:5812/:5925`); register
  `hebword` at main-index open/searcher sites; gate `_add_bracket_variants`; add `content_search` OR-clause +
  field at word-search retrieval sites; main-index marker + compat fallback.
- `tests/`, `docs/OPEN_ISSUES.md`, `CLAUDE.md`. No `extraction_format_version` bump.

## Tests (assert each invariant + Codex risks)

- Comma/punctuation: `מצותה בסגן, הקשו`+`עוד בסגן כאן`+`(סגן)`+`עליה.` → `בסגן`/`סגן` retrieve all.
- Bracket-exact: `[סגן` returns **only** `[סגן` (not bare `סגן`/`בסגן`); bare `סגן` returns the `[סגן` page too.
- Diacritics: `צמאן`,`צ'מאן`,`צ̇מאן` all retrieve `צ̇מאן`; not shattered.
- **Position regression (HIGH-4):** an `L{n}:word` per-line search still works (line_starts/ends untouched).
- **Ranking (MEDIUM-7):** exact-form hit ranks above diacritic-normalized fallback.
- Display intact; guards that position fields stay whitespace and `_add_bracket_variants` is bracket-gated.
- Both schemas. Golden-query regression set across modes. Per `feedback_full_suite_testing_windows`: GUI tests
  in the marker split (`GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`); `ruff check .`.

## Verification

- `pytest tests/` + ruff. LOCAL e2e (desktop): `.txt` with `מצותה בסגן, ועוד בסגן כאן`, a `[סגן`/`סגן` pair,
  an `L{n}:` per-line case, and `צ̇מאן`/`צ'מאן` → all invariants; index auto-rebuilds on first launch **off the
  UI thread** (no freeze). Genizah e2e: rebuild behind the compat gate; reproduce the user's case + a bracket
  page + a Judeo-Arabic dotted page; **no regression** across exact/title/shelfmark/variants/fuzzy/responsa/
  composition/parallels/position or the highlight phase; spot-check ranking on common normalized forms.

## Decisions during execution

1. Desktop Genizah rebuild trigger: background `create_index` (default) vs shipped prebuilt index.
2. `⟦⟧` → split (default) vs normalize to `[]` for exact-angle-bracket search.
3. niqqud folding for vocalized corpora — out of scope unless needed.
