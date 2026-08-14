---
id: SEED-033
status: awaiting-decision
planted: 2026-08-12
planted_during: post-v8.5.2 (Phase 136 discovery work active)
trigger_when: owner rules between Design Options A / B / C below — no code until then
scope: Small (Option A) — Medium (Option B: changes PGP precedence app-wide)
apps: both (web `version_selector` + desktop `_auto_select_pgp_edition`) + shared core
origin: owner bug report 2026-08-12 — search "עצים עליו למודה" showed a snippet the
  browse page never displays
related_memory: project_fgp_default_vs_midrash_coverage, reference_local_data_real_paths
---

# SEED-033: the PGP default masks the transcription the search actually matched

## Why This Matters

The reading-view default cascade auto-selects **any** PGP edition with **no coverage or
content check**:

```
PGP edition (unconditional)  →  FGP source (coverage-gated, SEED-030)  →  correction  →  V0.8/HTR
```

SEED-030 closed exactly this gap for FGP and **left it open for PGP**. Consequence: a user
searches a phrase, the result card shows it highlighted, they click through — and the folio
renders a *different* transcription that does not contain the phrase at all. The search result
becomes unfalsifiable from the UI.

### The reported case (reproduced end-to-end 2026-08-12)

Search `עצים עליו למודה` → hit #1 `Ms. P. Heid. Hebr. 18`, folio 1v. Clicking it lands on the
correct folio, which then displays:

```
خدمة تعرض / على مجلس المولا / الفقيه الجليل / جمال الدين مجمل / النعوت والاوصاف
```

That is pgpid **37732** — a 73-character Arabic *khidma* letter address. The V0.8 transcription
of the same folio is **491 characters** and *does* contain the searched phrase. PGP wins
unconditionally, so the matched text is never shown.

## Root Cause (two sites, one policy)

| Site | Code |
|---|---|
| web | `web/components/version_selector.py:157-174` — `if editions: … return` before any check |
| desktop | `genizah_app.py:3059-3064` — `# PGP edition always wins.` loop, returns first `pgp_edition` |

Both bypass `shared/fgp_service.py::choose_default_source`, which today only arbitrates FGP.

## The FGP analogy is imperfect — read this before choosing a fix

SEED-030's model is *"same text, less of it"*: a partial FGP excerpt vs a fuller HTR of the
**same** content, so a length ratio is a valid demotion signal.

**PGP here is a different thing.** This folio carries *both* an Arabic address and Hebrew
literary text (PGP's own description says "on recto there are biblical verses in Hebrew").
PGP transcribed the Arabic **completely**; V0.8 transcribed the Hebrew. Neither is partial —
they cover **different portions of the same leaf**.

So a coverage ratio would mislabel PGP as "worse" when it is simply *about something else*.
The signal that actually matters is **"does this source contain what the user searched for"**,
not "is this source shorter". This is the main argument for Option A over Option B.

## Design Options (owner decision — this is the blocker)

### Option A — search-scoped source selection *(recommended)*
Pass the matched phrase to `/browse` (`highlight` param already exists and is already plumbed
through `create_browse_page(highlight=...)`), and when it is present, default to whichever
available source contains it. PGP-first remains the rule for ordinary browsing.
- **Pro:** fixes the reported complaint exactly; narrowest blast radius; no scholarly-precedence
  change; correct by construction for the different-text case above.
- **Con:** only helps arrivals from search; a user browsing to that folio cold still sees Arabic.
- Note the search card links currently emit **no** `highlight` param — that must be added.

### Option B — extend the coverage gate to PGP
Treat PGP like FGP in `choose_default_source`; low-coverage PGP demotes below V0.8 everywhere.
- **Pro:** one consistent rule; helps cold browsing too.
- **Con:** changes PGP precedence app-wide. A 73-char scholarly edition would stop being the
  default reading text. Mislabels "different portion" as "partial" (see above). Highest risk of
  an unwanted regression for scholars who expect PGP to win.

### Option C — keep the default, improve discoverability
Leave precedence alone; surface a cue that a fuller/other source exists (e.g. "V0.8: 491 chars").
- **Pro:** zero behavioral risk. **Con:** does not fix the reported confusion, only labels it.

A and B are **not** mutually exclusive; A can ship first and stand alone.

## Implementation Sketch (Option A)

1. `web/pages/search_results.py` — add `&highlight=<matched phrase>` to the four browse-link
   sites (URL-encoded; reuse the existing `quote` import).
2. `shared/` — extend the single policy helper rather than adding a second one:
   `choose_default_source(sources, htr_text, *, must_contain: str | None = None, …)`.
   When `must_contain` is set, prefer the first source whose normalized text contains it;
   otherwise fall through to today's exact behavior. Normalize with
   `strip_search_diacritics` on **both** sides (same rule as SEED-030 §1) so a diacritic or
   punctuation difference cannot cause a false miss.
3. Both call sites (`version_selector.py:157`, `genizah_app.py:3059`) **render only** the
   returned decision — no duplicated policy. Preserve PGP-first when `must_contain` is absent.
4. UX: when the default was chosen *because* of the search term, say so (e.g. a small
   "showing the version containing your search" note) — do not switch sources silently.
5. Shared unit tests (no GUI → sidesteps the NiceGUI render-smoke gap): PGP-contains,
   PGP-lacks-but-V0.8-has, neither-has, both-have, diacritic-mismatch.

## Gates

- **Prove the gate can fail:** re-point the test at the real pgpid-37732 / folio-1v pair and
  confirm it *fails* before the fix and passes after. Reading the code is not evidence.
- Codex code-review gate (shared core + both apps).
- Web+desktop parity guard: one shared helper, two render-only call sites.
- No DB change, no reindex.

## Findings that are NOT this seed — do not lose them

Uncovered while diagnosing; both are **upstream data defects**, not app bugs:

1. **V0.8 cross-manuscript misattribution.** `990043940120205171` ("p. Heid. Hebr. 19") has
   **zero** V0.8 records — the whole manuscript is absent from a V0.8-only index. Its folio text
   instead appears inside Hebr. 18's V0.8 record `IE61676826_P000002_FL61676829` (491 chars),
   which decomposes as Hebr. 18's own 53-char near-blank folio-2 stub (byte-identical to
   `IE169327159_P000002`) + Hebr. 19's folio text **appended twice**. So in production
   (V0.8-only) this phrase returns exactly one hit and it is the **wrong manuscript**; the right
   one is unreachable. Worth checking whether the concatenate-and-duplicate pattern recurs
   corpus-wide.
2. **V0.7-only manuscripts are invisible in production.** Sampled 400 of the 232,450
   `browse_map` sys_ids: 93.8% have V0.8, **6.2% are V0.7-only** → roughly **9,000–20,000
   manuscripts** (95% CI, n=400) missing from production search entirely. Assumes production
   ships V0.8 only (owner's statement; not verifiable from a dev box).

## Already fixed while investigating (uncommitted, in `C:\GenizahSearch` only)

Verified live on a warm dev server; `ruff` clean. **Not** yet ported to this branch, and the
main checkout also holds ~15 unrelated modified files — do not blanket-commit.

| Fix | File | Verification |
|---|---|---|
| `_deduplicate` ignored V0.7-vs-V0.7 uid collisions, so one folio rendered twice (same uid arriving as both `scope='system'` and `scope='page'`) | `shared/search_engine.py:2756` | 4 → 3 results |
| Browse deep-links silently degraded to a bare `/browse?sys_id=…` (dropping `fl_id`/`volume_ie`/`page`), which resolves `p_num==1` of the **first** volume — a different folio | `web/pages/search_results.py:668` | now derives each param independently, falling back `raw_header` → `uid`; lands on the phrase (line 6) |
| `fl_id` route dead-ended on `No text available` instead of using the `volume_ie`+`page` it already had | `web/pages/browse.py:783`, `:4693` | previously-erroring URL now loads |
| "Unknown" shelfmark/library on results | — | `csv_bank` startup race; correct on a warm process. **No code change** |

## Breadcrumbs

- Repro URLs (dev): masked case
  `/browse?sys_id=990043939960205171&fl_id=61676829&volume_ie=IE61676826&page=2` (shows Arabic);
  correct ms `/browse?sys_id=990043940120205171&fl_id=169327177&volume_ie=IE169327175&page=1`
  (shows `ך עצים עליו למודה` on line 6).
- `web/components/version_selector.py` — `load_and_apply_latest`, PGP block `:157-174`,
  FGP coverage block `:176-200`.
- `genizah_app.py:3043` `_auto_select_pgp_edition`; callers `desktop/result_dialog.py:1466`,
  `genizah_app.py:7193`.
- `shared/fgp_service.py` — `choose_default_source` (`:608` docstring names both call sites),
  `group_transcription_sources`, `source_relation_kind`.
- `web/browse_bootstrap.py:62-66` — `fl_id` wins outright and returns before `page`/`volume_ie`
  are read. This is why appending `&page=` alone changes nothing when `fl_id` is present.
- `p_num` is **volume-local**, not global: `browse_map` for `990043939960205171` holds p_num 1,2
  under `IE169327159` and 1,2 again under `IE61676826`. A page is only addressable as the pair
  `(ie_id, p_num)` — `shared/browse_map_utils.py:580` uses exactly that key.
- PGP record: `pgp_data/pgp.db` `documents.pgpid=37732`, `document_fragments.document_id=37732`
  (column is `document_id`, not `pgpid`). Repo-root `pgp.db` is a **0-byte placeholder** — see
  `reference_local_data_real_paths`.
- Live index is `C:\Users\gersh\Genizah_Tantivy_Index\tantivy_db` (+ `browse_map.pkl`), **not**
  `C:\GenizahSearch\Genizah_Index`.
- Probe scripts from the session scratchpad (re-runnable): `q1_index.py` (which docs hold the
  phrase), `q7_live_search.py` (real `execute_search` + the href each link site would build),
  plus the V0.8-coverage and blast-radius scans.

## Notes

The search snippet was never wrong — it faithfully quotes the V0.8 text it matched. Everything
downstream (which folio, which manuscript, which transcription is displayed) is where this broke,
in four independent places. Keep them separate when planning: only the PGP default needs a
product decision; the rest were mechanical.
