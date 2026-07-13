---
id: SEED-030
status: ready
planted: 2026-07-13
planted_during: post-v8.4.1 (no active milestone; NEXT was /gsd-new-milestone)
trigger_when: now — user approved implementation via GSD (2026-07-13)
scope: Small–Medium (one shared helper + normalization + tests, two thin call-site swaps, UX hint)
apps: both (web + desktop) + shared core
origin: Gregor Schwarb email 2026-07-13; design approved by Hillel; Codex gpt-5.5 xhigh reviewed
related_memory: project_fgp_default_vs_midrash_coverage
---

# SEED-030: FGP default demotion via coverage ratio (show MiDRASH/HTR when FGP is partial)

## Why This Matters

The reading-view **"Manuscript Text"** default cascade is:

```
PGP edition → FGP source → user correction → V0.8 (MiDRASH/HTR original_text)
```

(web `web/components/version_selector.py::create_version_selector` → inner
`load_and_apply_latest`; desktop `genizah_app.py::_auto_select_pgp_edition`, ~line 2992.)

It auto-selects **any** FGP source over the HTR with **no coverage check**, so *partial /
selected* FGP transcriptions displace the fuller MiDRASH HTR. Reported by Gregor Schwarb: for
Firkovich manuscripts the panel shows only a short selected excerpt, hiding ~90% of the folio's
text that the HTR captures.

### Evidence (validated, not assumed)

Compared FGP `content_length` vs HTR non-whitespace chars per `sys_id` (streamed the whole
`Transcriptions.txt` corpus). "Coverage" = FGP_chars / HTR_chars:

| Collection | n | median coverage | % MSS <25% coverage |
|---|---|---|---|
| CUL | 11,071 | 181% | 2% |
| JTS | 5,676 | 146% | 8% |
| BL / AIU / Mosseri / … | ~2,000 | 120–166% | ~1% |
| Oxford | 3,334 | 81% | 38% |
| **St. Peterburg (Firkovich)** | 1,267 | **9%** | **75%** |
| ALL | 23,516 | 154% | 12% |

Reading: for ~88% of the corpus FGP is *fuller* than the HTR (HTR is riddled with `][`
uncertainty gaps), so FGP-as-default is correct there. The partiality problem is concentrated in
**Firkovich (75% partial)** and part of **Oxford (38%)**. → "demote FGP always" over-corrects;
a **coverage heuristic** is the right fix.

## Chosen Design — runtime coverage ratio

At default-selection time the selector already has both texts for the CURRENT folio:
`original_text = page.text` (HTR) and the folio's FGP section. Compute
`coverage = normalized_len(fgp_folio) / normalized_len(htr_folio)`. If `coverage < THRESHOLD`,
do **not** auto-default to FGP — fall through to HTR/MiDRASH and keep FGP as a secondary menu
option.

### Codex-mandated refinements (must be in the plan)

1. **Normalize both sides symmetrically** — count base Hebrew letters; strip
   diacritics/cantillation/whitespace/punctuation and HTR `][` lacuna markers, but KEEP letters
   inside editorial brackets (still text). Reuse `strip_search_diacritics`
   (`shared/text_normalize` / search tokenizer). **Do NOT use raw `len()`** — raw HTR length
   biases against FGP; raw FGP-with-vowels/apparatus biases for it.
2. **Minimum-HTR-length floor → "coverage unknown"** — when the normalized HTR folio is
   empty/tiny, coverage is undefined, so **KEEP FGP as default** (fail toward FGP; HTR is a
   fullness baseline, NOT ground truth — it can be blank, hallucinated, or wrong-page).
3. **Compare the per-folio FGP text via `get_fgp_section_for_page(source, page_num)`**, NOT
   `source['content']` — multi-section rows would inflate FGP and cause a false-keep.
4. **Threshold = shared constant + env override `FGP_DEFAULT_MIN_COVERAGE`**, default ~0.30–0.35;
   log/debug the computed ratio; **no upper-bound demotion**.
5. **Coverage-only for v1** — do NOT parse `image_id` prefixes in the UI (catalog-500 hard-demote
   deferred; it would need a proper `source_class` field in `fgp_service`, not magic prefixes).
   Coverage already catches most catalog stubs (they're short).
6. **FGP EDITIONS only, never translations** — a translation's length vs the Hebrew HTR is
   meaningless (`source_relation_kind(...) == 'translation'` must be excluded from the ratio).
7. **UX: don't demote silently** — add a version-menu hint/tooltip phrased **"shorter than V0.8"**
   (NOT "partial" — the HTR baseline is imperfect, so don't overclaim).

### Architecture (Codex's strongest point)

Centralize the policy in **one pure shared helper**:

```python
shared/fgp_service.py::choose_default_source(sources, htr_text, page_num)
    -> {source, reason, ratio, eligible}
```

Web (`version_selector`) and desktop (`_auto_select_pgp_edition`) only **render** the returned
decision — no duplicated policy. Unit-test normalization + threshold + Firkovich/edge cases in
**shared tests** (no GUI → sidesteps the NiceGUI render-smoke gap). Preserve the PGP-first rule.
**No DB change, no reindex.**

Open sub-decision for the plan: when FGP is demoted, does the cascade continue to user
corrections or jump straight to V0.8? (Document explicitly.)

## When to Surface

**Trigger:** now. Hillel approved implementation via GSD on 2026-07-13. Ready to `/gsd-plan-phase`
(standalone phase ~133) or fold into the next `/gsd-new-milestone`.

## Scope Estimate

**Small–Medium.** One shared decision helper + Hebrew-letter normalization helper + shared unit
tests; two thin call-site swaps (web `load_and_apply_latest`, desktop `_auto_select_pgp_edition`);
one version-menu hint string (bilingual EN/HE). Warrants the **Codex code-review gate** (touches
shared core + both apps). No data migration.

## Gates

- Codex code-review gate (shared core + both apps).
- Web+desktop parity guard (single shared helper; both render only).
- Pre-implementation nicety: dry-run `choose_default_source` against the shelfmark in Gregor's
  screenshot to confirm it lands correctly before wiring the UI.

## Breadcrumbs

- `web/components/version_selector.py` — `load_and_apply_latest` (~line 150–235), FGP fallback
  picks `fgp_sources[0]` at ~line 173–194 (the bug site).
- `genizah_app.py::_auto_select_pgp_edition` (~line 2992) — desktop default-selection policy.
- `shared/fgp_service.py` — `choose_default_source` (NEW), `get_fgp_section_for_page` (line 232),
  `group_transcription_sources` (175), `source_relation_kind` (144), `filter_sources_for_page`
  (601), `_content_similarity` (528), `_select_fgp_editions_by_similarity` (540).
- `web/pages/browse_enrichment.py:318` + `web/pages/search_results.py:1368` — `all_sources` is
  folio-filtered here (so the selector's sources are already per-folio).
- Corpus: `Transcriptions.txt` header format `==> {sys_id}_{IE}_{Pnnnnnn}_{FL...} <==`.
- FGP sidecar: `fgp_data/fgp_transcriptions.db` (`content_length`, `heb_ratio`, `image_id`
  prefix 500=catalog/600=book/100-151+200+400=teams, `page_info`, `sections`).
- Design review artifacts: `tmp/CODEX-BRIEF-fgp-default-coverage.md`,
  `tmp/CODEX-CRITIQUE-fgp-default-coverage.md`.
- Related memory: `project_fgp_default_vs_midrash_coverage` (+ `project_fgp_transcriptions_sidecar`,
  `project_fgp_xml_rendering`, `project_fgp_image_folio_mapping`).

## Notes

Data validation scripts used to produce the coverage table live in the session scratchpad
(`firk_fgp.json` / `firk_htr.json` / `all_fgp.json` / `all_htr.json`) — regenerate by streaming
`Transcriptions.txt` if needed. Aggregate per-sys_id coverage PROVES the phenomenon; the runtime
helper decides **per-folio**, which is strictly more precise (a folio FGP fully covers stays
default; a snippet demotes; a folio with no FGP never triggered FGP anyway).
