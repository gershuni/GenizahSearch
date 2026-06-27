---
status: shipped
---

# Codex review brief — Hebrew search retrieval fix (GenizahSearch)

You are reviewing a PLAN (not yet implemented). Be critical and specific: flag correctness pitfalls,
production-incident risks, search-quality regressions, simpler/safer alternatives, and anything missed.
Your final critique should be a clear verdict + numbered findings (severity-tagged). The questions are at
the very end.

## System
- Dual app: NiceGUI web + PyQt6 desktop; shared `genizah_core.py`.
- Search is two-phase: **Tantivy (candidate retrieval) → Python regex (filter + highlight)**.
- tantivy-py **v0.25.0** (has `tantivy.Tokenizer.regex(pattern)`; regex tokenizer MATCHES tokens).
- Two Tantivy indexes are in scope:
  - **Main Genizah index** — built offline by `SearchEngine.create_index()` (`genizah_core.py:5709`), schema
    content fields at `:5726-5730`. Desktop builds it locally from a downloaded `Transcriptions.txt`; web
    builds on the server.
  - **LOCAL "My Library" side-index** — `shared/local_indexer.py`, `build_local_schema()` (`:666`), content
    fields `:681-686`.

## Bug report
Desktop My Library user: searching `בסגן` does NOT find a doc whose text is `בסגן,` (word + comma), but
searching `סגן` DOES surface that doc (with `בסגן` highlighted). Re-indexing the library did not help.

## Root cause
Both indexes' searchable `content` field uses `tokenizer_name="whitespace"`, which splits only on spaces, so
punctuation stays attached to the token: `מצותה בסגן,` → tokens `['מצותה','בסגן,']`. A clean `בסגן` query
(tokenized the same way → `בסגן`) never matches the token `בסגן,`. Bare `סגן` still retrieves the page via a
DIFFERENT clean token on it, and the substring/mark-tolerant regex then highlights `בסגן`. So the gap is in
RETRIEVAL (tokenization), not the regex. Re-indexing reproduces the same comma-bearing token.

## Corpus impact (sampled Transcriptions.txt, 35,912 pages / ~120MB)
~3.0% of distinct Hebrew words (298,411) appear ONLY attached on their page → unretrievable. Two classes:
- **Editorial punctuation (~75%)**: `⟦⟧` (U+27E6/7), `[ ]`, `.`, `( )`, `:`, `/`.
- **Combining diacritics (~25%)**: U+0307 (Judeo-Arabic upper dot, e.g. `צ̇מאן`), U+0308, geresh/apostrophe
  (`צ'`, `אמ'`).

## Existing infrastructure (reuse / must preserve)
- `strip_search_diacritics(text)` (`:6592`) / `COMBINING_DIACRITICALS_PATTERN` (`:6589`) =
  `[U+0300-036F, ASCII apostrophe U+0027, geresh U+05F3, gershayim U+05F4, curly quotes U+2018/2019]`. Strips
  marks+apostrophe+geresh; **NOT brackets/comma**. QUERIES already call it (`:1319/:1534/:8681`).
- Regex filter/highlight is mark-tolerant (`make_mark_tolerant_pattern` `:6651`) and bracket-aware
  (`_query_has_brackets` `:6632`, `_strip_brackets` `:6642`) — runs on the STORED original `content`.
- `_add_bracket_variants` (`:6616`) OR-expands `[term`,`term]`,`[term]`,`]term`,`term[` into the Tantivy query
  so bare queries reach bracketed tokens (a workaround for whitespace keeping brackets attached). Used in
  `build_tantivy_query` at `:7790/:7863`.
- `WORD_TOKEN_PATTERN = [\w֐-׿']` (`:2404`).
- LOCAL has a schema-marker auto-rebuild: `_compute_schema_marker` hashes `build_local_schema` SOURCE; on
  mismatch `LocalIndexer.__init__` runs `rebuild_main_index_atomic` (`:1719-1759`) from `cached_text` in
  SQLite (no PDF re-extraction). NOTE: `LocalIndexer.__init__` is called on the **UI thread**
  (`desktop/my_library_tab.py:1082 → 1511`).
- Main Genizah index has **no** schema marker.

## User-confirmed requirements (invariants — must all hold after the fix)
1. Query `[סגן` (bracket in query) → return **ONLY** `[סגן` (exact).
2. Query `סגן` (bare) → loose: also find `[סגן` and prefixed forms ON pages that contain `סגן`. **No true
   prefix expansion** (must NOT start retrieving `בסגן`-only pages from a `סגן` query).
3. Query `בסגן` → find `בסגן,`.
4. Query `צמאן` OR `צ'מאן` → find corpus form `צ̇מאן` (U+0307).
5. Results DISPLAY original text (dots/brackets visible).

## Proposed fix
Add a normalized RETRIEVAL field; leave stored `content` + filter/highlight UNTOUCHED.
1. Register a custom regex tokenizer **"hebword"**: pattern
   `[\w ֐-׿ ̀-ͯ ' " \[ \]]+` — keeps Hebrew (incl. nikud, maqaf U+05BE), combining marks,
   apostrophe, gershayim, AND `[ ]`; SPLITS comma/period/colon/semicolon/parens/`⟦⟧`/slash/space. Register at
   every index create/open/searcher site (else `parse_query` raises "unknown tokenizer").
2. New **non-stored field `content_search`** in BOTH schemas, populated with
   `strip_search_diacritics(page_content)`, tokenized by hebword. Result: `צ̇מאן`→`צמאן`, `אמ'`→`אמ`,
   `בסגן,`→`בסגן`, `(סגן)`→`סגן`, `[סגן`→`[סגן` (bracket kept). Also normalize the position fields
   `content_head/tail/line_starts/line_ends` (all stored=False) the same way.
3. **Repoint candidate retrieval `content`→`content_search`** at `parse_query` sites: `:8434` (main),
   `:8915-8925` (position-filtered map + content fallbacks), `:9237` (composition/parallels), LOCAL responsa
   `:7444/:7455`. Ensure `build_tantivy_query` emits `strip_search_diacritics`-normalized terms. KEEP
   `_add_bracket_variants` / `_query_has_brackets` / `_strip_brackets` / `make_mark_tolerant_pattern`
   unchanged. Do NOT repoint highlight/snippet reads (`doc['content']` at `:8467/:8964/:9250`) — display stays
   original.

### Rollout
- **LOCAL**: schema change bumps the source-hash marker → auto-rebuild from `cached_text` (re-normalize there);
  no manual "Re-index All". Concern: the rebuild runs in `__init__` on the UI thread; a tokenizer change forces
  it for ALL existing users once — large libraries may freeze; consider deferring to the background
  `LocalIndexerWorker` (SQLite is `check_same_thread`-bound).
- **GENIZAH**: rebuild `create_index` server-side (web) + deploy; desktop = background local `create_index`
  rebuild gated by a new version marker, or ship a prebuilt index via the existing sidecar channel.

## Empirical validation (throwaway tantivy 0.25.0 index built EXACTLY as above)
- `צמאן` → finds the `צ̇מאן` doc. `צ'מאן` → finds it. `צ̇מאן` → finds it.
- `בסגן` → finds the `בסגן,` doc.
- `סגן` (bare) → finds the `[סגן` doc AND the `סגן` doc (not the `בסגן,`-only doc — matches "no prefix
  expansion").
- `[סגן` → finds ONLY the `[סגן` doc.
All five invariants held.

## Review questions
1. Correctness pitfalls of the `content_search` + hebword approach. Any tantivy-py 0.25.0 regex-tokenizer
   gotchas (token boundaries, phrase/gap queries, position/offset queries, BM25 scoring over a non-stored
   field, IDF shifts vs the old field)?
2. Is repointing candidate retrieval to `content_search` the right architecture, or is there a simpler/
   lower-risk option (e.g. change `content`'s tokenizer in place for the punctuation class + a separate
   mechanism only for the dot)? Trade-offs.
3. Blast radius of the query repoint across all modes (exact/title/shelfmark/variants/fuzzy/responsa/
   composition/parallels/position). What silently breaks? Any highlight-offset mismatch risk given retrieval
   uses normalized text but highlight runs on original `content`?
4. Bracket invariant: with `[ ]` KEPT in hebword tokens AND queries normalized, are there cases where
   `[סגן`→only-`[סגן` breaks, or where bracketed content becomes UNfindable? Should `⟦⟧` (U+27E6/7) be treated
   like `[ ]` (kept + variant-expanded) rather than split?
5. The LOCAL UI-thread `__init__` rebuild for ALL existing users on first launch after the update: real freeze
   risk for large libraries (rebuild re-adds all pages from cached_text)? Best deferral pattern respecting
   SQLite `check_same_thread`.
6. Genizah rebuild + redistribution: best mechanism. Any multi-worker (uvicorn) concern with registering a
   custom tokenizer per worker / per index handle?
7. Any OTHER place `content` is tokenized or queried that also needs the new field/tokenizer (LAB index? export
   paths? browse?). Data-quality interactions (e.g. the dot attached to the wrong letter from RTL/bidi PDF
   extraction)?
8. Should `content` still be tokenized with hebword (for any path that may still query it) or left whitespace?
   Should `content_search` be stored=False — any downside?
9. Simpler alternatives, or reasons NOT to do this at all.
