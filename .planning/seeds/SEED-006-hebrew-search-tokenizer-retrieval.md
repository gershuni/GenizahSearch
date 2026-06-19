---
id: SEED-006
status: dormant
planted: 2026-06-19
planted_during: v8.2.0 / Phase 119 (Web Joins Lab)
trigger_when: next search-quality, indexing, or My-Library milestone — anything touching Tantivy tokenization, LOCAL indexing, or Hebrew search recall
scope: large
---

# SEED-006: Fix Hebrew search retrieval — punctuation- and diacritic-attached words are unretrievable (LOCAL + Genizah)

> Captured as a seed (NOT implemented). Intended to run on the cloud on a separate branch
> (e.g. `fix/hebrew-search-tokenizer`) off `origin`, isolated from active phase-119 work.
> Full plan: `C:\Users\gersh\.claude\plans\linked-beaming-hamster.md`. Codex review (BLOCK →
> all findings incorporated): `_tmp/codex-hebrew-search-review.md`; brief: `_tmp/codex-hebrew-search-tokenizer-brief.md`.

## Why This Matters

A desktop **My Library** user reported (2026-06-19): searching `בסגן` does **not** find a document whose
text is `בסגן,` (word immediately followed by a comma), while searching `סגן` *does* surface it (highlighting
`בסגן`). Re-indexing the library did not help.

**Root cause:** Both Tantivy indexes' searchable `content` field uses `tokenizer_name="whitespace"`
(`shared/local_indexer.py:682-686`; `genizah_core.py:5726-5730`), which splits **only on spaces**, so any word
adjacent to punctuation is indexed *with* the punctuation: `מצותה בסגן,` → tokens `['מצותה','בסגן,']`. A clean
`בסגן` query (tokenized the same way → `בסגן`) never matches the token `בסגן,`. Bare `סגן` still retrieves the
page via a *different* clean token on it, and the substring/mark-tolerant **regex** then highlights `בסגן` —
masking the gap. **The gap is in Tantivy RETRIEVAL, not the regex filter/highlight** (which is already
mark-tolerant and bracket-aware). Re-indexing reproduces the same comma-bearing token, so it can't help.

**Corpus impact (measured):** sampling `Transcriptions.txt` (35,912 pages / ~120 MB) → **~3.0% of distinct
Hebrew words (298,411)** appear *only* attached on their page and are therefore unretrievable. Breakdown:
- **~75% editorial punctuation** — `⟦⟧` (U+27E6/7), `[ ]`, `.`, `( )`, `:`, `/`.
- **~25% combining diacritics** — U+0307 (Judeo-Arabic upper dot, e.g. `צ̇מאן`), U+0308, geresh/apostrophe
  (`צ'`, `אמ'`). Users type the word *without* the dot or *with* `'` and expect to find `צ̇מאן`.

## When to Surface

**Trigger:** next milestone that touches search quality, Tantivy tokenization/indexing, the LOCAL "My Library"
index, or Hebrew search recall. Independent of the active v8.2.0 Web-Joins-Lab work.

This seed will surface during `/gsd:new-milestone` when the scope matches.

## Scope Estimate

**Large** — spans both apps (web + desktop), both Tantivy indexes, requires index rebuilds + redistribution
(web redeploy + desktop data path), a background-rebuild change to avoid a UI-thread freeze, a golden-query
regression suite, and a cross-AI (Codex) convergence review on the implementation diff. The *code* is roughly
a phase; the **rollout/redistribution** is the heavy part.

## User-confirmed invariants (must all hold — do not regress)

1. `[סגן` (bracket in query) → return **only** `[סגן` (exact).
2. `סגן` (bare) → loose: also finds `[סגן`/prefixed forms **on pages that contain `סגן`**. **No prefix
   expansion** (must NOT start retrieving `בסגן`-only pages from a `סגן` query).
3. `בסגן` → finds `בסגן,`.
4. `צמאן` OR `צ'מאן` → finds the corpus form `צ̇מאן` (U+0307).
5. Results **display original text** (dots/brackets visible).

All five were empirically validated in a throwaway tantivy 0.25.0 index built exactly as the design below.

## Approach (Codex-reviewed — verdict BLOCK on first draft; all findings incorporated)

**Shared tokenizer `hebword`** — regex tokenizer (`tantivy.Tokenizer.regex`, v0.25.0 — matches token spans),
pattern **with no literal whitespace**: a char class of `\w` + Hebrew U+0590-05FF (incl. nikud, maqaf) +
combining marks U+0300-036F + apostrophe + double-quote + `[` `]`, one-or-more. It **keeps** Hebrew/marks/
apostrophe/gershayim/`[ ]` and **splits** space/comma/period/colon/parens/`⟦⟧`/slash. Register it on **every**
Index object after each open/create, **per process** (uvicorn workers; mirror `_ensure_lab_tokenizers`
`genizah_core.py:737`) or `parse_query` raises "unknown tokenizer".

**Stage 1 — punctuation fix (the reported bug): in-place, NO query repoint.**
- Change **only** `content`'s tokenizer `whitespace`→`hebword` in both schemas. Stored value unchanged
  (display intact); queries still target `content`, so all hard-coded `content:` paths keep working
  (line-break `:8371/:8384`, LOCAL `["content",…]:7442`, composition `:9237`, `search_field:8908`).
  `בסגן,`→token `בסגן`. ✅ inv 3.
- **Leave `content_head/content_tail/line_starts/line_ends` on `whitespace`** — they carry `L{n}:word` colon
  position markers (`_extract_position_fields:5542`, lines 5559/5561) that the new tokenizer would shatter.
  `content` itself has no `L:` markers, so splitting colon there is safe.
- **Gate `_add_bracket_variants` on bracket-free terms** (`'[' not in term and ']' not in term`) so `[סגן`
  stays exact (inv 1) while bare `סגן` still expands to bracketed forms (inv 2). Brackets stay in tokens.
- `⟦⟧` (U+27E6/7): split (treat as punctuation; documented minor loss of exact-`⟦⟧` search) — or optionally
  normalize `⟦⟧`→`[]`.

**Stage 2 — diacritic/dot fold (Judeo-Arabic): additive `content_search` fallback.**
- Add a non-stored field `content_search` (both schemas) = `strip_search_diacritics(page_content)` (the SAME
  fold queries already apply — `genizah_core.py:6592`), tokenized `hebword`. `צ̇מאן`→`צמאן`, `אמ'`→`אמ`.
- In `build_tantivy_query`, **add** a lower-weighted `content_search:` OR-clause for the normalized term
  alongside the existing `content` clauses (keep the exact-`content` `^5` boost so original forms rank first).
  Add `content_search` to the parse_query field list at the **word-search retrieval** sites only (`:8434`
  main, `:9237` composition, `:7442` LOCAL); position/line-break and title/shelfmark modes are **not** touched.
  Queries already `strip_search_diacritics`, so `צמאן`/`צ'מאן`→`צמאן` matches the normalized token. ✅ inv 4.
- Filter/highlight stay on stored `content` (mark-tolerant) → display unchanged ✅ inv 5.
- niqqud (U+0591-05C7) is **not** folded by `strip_search_diacritics` — Genizah is largely unvocalized; a
  separate enhancement if a vocalized corpus needs it.

## Rollout

- **LOCAL** — changing `build_local_schema` bumps the source-hash `.schema_version` marker
  (`_compute_schema_marker:705`) → auto-rebuild from `cached_text` in SQLite (**no PDF re-extraction**;
  re-normalize `content_search` there). **Critical:** do **not** rebuild synchronously in
  `LocalIndexer.__init__` (`:1744`, runs on the UI thread `desktop/my_library_tab.py:1082`). Detect
  "needs rebuild" without building the writer, keep `is_searchable=False` (Phase 97 R-01 gate exists), run
  `rebuild_main_index_atomic` (`:3639`) on a background QThread worker with its **own** SQLite connection
  (respect `check_same_thread`), then flip `is_searchable=True`. See `feedback_no_auto_reindex_in_init`.
- **GENIZAH** — no schema marker. Add a main-index version/marker. **Compat gate:** querying `content_search`
  against an old index (no such field) fails → **deploy the rebuilt index before the query code**, or fall
  back to `content`-only when `content_search` is absent. Web: rebuild `create_index` over `Transcriptions.txt`
  server-side + deploy (data-first). Desktop: background `create_index` rebuild gated by the marker (corpus
  already on disk; never UI-thread) or ship a prebuilt index via the sidecar channel.

## Tests / Verification

Assert each invariant + the Codex risks: comma/punctuation retrieval; bracket-exact (`[סגן`→only `[סגן`);
diacritics (`צמאן`/`צ'מאן`/`צ̇מאן` → `צ̇מאן`, not shattered); **position regression** (`L{n}:word` still
works — line_starts/ends untouched); **ranking** (exact-form ranks above the normalized fallback); display
intact; guards that position fields stay whitespace and `_add_bracket_variants` is bracket-gated. Both schemas.
Tests build tiny throwaway indexes (no 2 GB corpus needed) — cloud-friendly. Per `feedback_full_suite_testing_windows`:
GUI tests in the marker split (`GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`); `ruff check .`.

## Breadcrumbs

- `shared/local_indexer.py` — `build_local_schema` content* tokenizer `:682-686`; `_compute_schema_marker`
  `:705`; `__init__` rebuild `:1744`; `rebuild_main_index_atomic` `:3639`.
- `genizah_core.py` — `create_index` content* tokenizer `:5726-5730`, add_document `:5779/:5812/:5925`;
  `_extract_position_fields` `:5542` (L-markers `:5559/5561`); `COMBINING_DIACRITICALS_PATTERN:6589` /
  `strip_search_diacritics:6592`; `_add_bracket_variants:6616`; `_query_has_brackets:6632`;
  `_strip_brackets:6642`; `make_mark_tolerant_pattern:6651`; parse_query sites `:8434`, `:8915-8925`, `:9237`,
  LOCAL `:7442`, line-break `:8371`; `_ensure_lab_tokenizers:737`; `WORD_TOKEN_PATTERN:2404`.
- `desktop/my_library_tab.py:1082→1511` — `LocalIndexer` constructed on the UI thread.
- Plan: `C:\Users\gersh\.claude\plans\linked-beaming-hamster.md`. Codex: `_tmp/codex-hebrew-search-review.md`,
  `_tmp/codex-hebrew-search-tokenizer-brief.md`. Related memory: `feedback_no_auto_reindex_in_init`,
  `feedback_review_workflow`.

## Notes

Decisions left for the executing phase: (1) desktop Genizah rebuild trigger — background `create_index`
(default) vs shipped prebuilt index; (2) `⟦⟧` split (default) vs normalize to `[]`; (3) niqqud folding for
vocalized corpora — out of scope unless needed. Re-run a Codex convergence pass on the implementation diff
(the first plan draft was BLOCKed; a diff-level review is warranted).
