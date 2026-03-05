# Phase 46: Dicta Translation — CONTEXT.md

## Phase Goal
All scholarly data is available in multiple languages via Dicta Translate API, enabling non-Hebrew/non-English speakers to use the platform and improving search completeness across languages.

## Decisions

### 1. Translation Scope

**Direction:** Both — EN→HE (PGP data) AND HE→EN (FJMS data). Full bilingual coverage.

**PGP data (pgp.db) — English→Hebrew:**
- `description` — ALL non-empty descriptions (~35K documents). Full coverage.
- `document_type` — Small fixed taxonomy. Check overlap with existing `pgp_tag_translations.py`. Manual mapping for any gaps.
- `tags` — ALREADY TRANSLATED in `pgp_tag_translations.py` (CATEGORIZED_TAGS). No work needed.

**FJMS catalog (fjms_enrichment.db) — Hebrew→English:**
- **Fill gaps only** — if TitleHeb exists but Title is empty, translate. NEVER overwrite existing human translations.
- `AuthorText` — translate when single-language.
- `catalog_free_desc` — translate Hebrew free descriptions to English.
- Domain classification names — ALREADY BILINGUAL. No work needed.
- Already-bilingual field pairs (Title/TitleHeb, TextualFrameHeb/TextualFrameEng, GenizahTitleOrgTitle/GenizahTitleEngTitle) — fill gaps only.

**Bibliography (542K entries):**
- Not scholarly-critical to translate, BUT Haredi users who don't read English need accessibility.
- Approach: provide general/summarized translation, not word-for-word scholarly citation translation.
- Lower priority than metadata fields — could be a separate sub-phase or later pass.

### 2. Storage & Pipeline

**Pipeline:** Offline batch script (like existing `export_fist_enrichment.py`, `export_pgp_sidecar.py`).
- Run once, re-run for updates.
- Must be defensive: checkpointing, resume capability, configurable throttle.
- Dicta API rate limits unknown — script should handle gracefully.

**Storage:** Claude's discretion. Options considered:
- New columns in existing sidecars (co-located)
- New translations table per sidecar (key-value)
- Separate translations.db sidecar (independent update cycle)
Decision deferred to planning phase — Claude picks based on what works best architecturally.

**Few-shot templates:**
- Test BOTH: Dicta defaults (from translate.dicta.org.il) AND custom scholarly few-shots built from existing bilingual data (Title/TitleHeb pairs, TextualFrame pairs).
- Compare quality, pick best.
- Researcher should investigate translate.dicta.org.il to understand their few-shot format.

### 3. Search Integration

**Scope:** Metadata search only (SQLite queries). NO Tantivy index changes.
- Translated metadata fields are searched alongside originals in metadata/filter queries.
- Included by default, with option to exclude.
- Exclude option UX: Claude's discretion (global setting vs per-search toggle).

**Match indication:** When a search result matched on translated text (not original), show a small "translated match" badge so users understand why the result appeared.

### 4. Display & UX

**Core pattern (user-specified):**
1. **Original match** — leave display as-is, no change.
2. **Translated content** — show translated text, marked with "translated" indicator. Hover reveals original text.
3. **Toggle** — small button to switch between "always show original" / "always translate". Corresponding option in general settings.

**Default:** Show original language by default. Users opt-in to translations.

**Apps:** Both web and desktop — full parity.

**Detail/catalog views:** Replace original with translated text (when user preference is "translate"), hover reveals original.

**Browse tree labels:** Follow user preference — if "translate" is active, show translated category labels.

**Toggle placement:** Needs prototyping during implementation. Many fields have translations, so per-field buttons would be cluttered. Likely a global toggle with clear visual state, but exact UX TBD through trial.

## Code Context

### Dicta Translation API
- **Translation endpoint:** `https://dicta-translation.loadbalancer3.dicta.org.il`
- **No API key required** (`x-no-api-key`)
- **Protocol:** Completions API (NOT chat), OpenAI-compatible
- **Model:** `dicta-il/dictalm2.0`
- **Temperature:** 0, Stop sequence: `\n`
- **Bidirectional:** EN→HE and HE→EN via different few-shot prompt templates
- **Few-shot format:** JSON file with `{prompts: [{English, Hebrew}], he_category, en_category}` — prompt is constructed as alternating category:text pairs
- **Reference implementation:** `TestLLMAPIsProgram.cs` (gitignored — contains API keys for other Dicta models)

### Also available (not for translation, but noted):
- **DictaLM Instruct:** `dictalm2-0-instruct-demo.loadbalancer3.dicta.org.il` (chat API, tool calling)
- **DictaLM Thinking:** `dictalm3-0-api-backend.loadbalancer3.dicta.org.il` (reasoning model)

### Existing bilingual infrastructure
- `pgp_tag_translations.py` — PGP tag taxonomy with Hebrew translations (~300 tags, categorized)
- `genizah_translations.py` — Desktop UI string translations (static dict)
- `web/translations.py` — Web UI translation layer
- FJMS catalog already has bilingual column pairs (Title/TitleHeb, TextualFrame Heb/Eng, GenizahTitle Org/Eng)
- FJMS domain names are already bilingual

### Data volumes (approximate)
- PGP descriptions to translate (EN→HE): ~35K documents
- FJMS catalog gaps to fill: TBD (researcher should count)
- FJMS free descriptions: ~190K entries
- Bibliography: ~542K entries (lower priority)

### Existing batch script patterns
- `scripts/export_fist_enrichment.py` — FJMS sidecar builder
- `scripts/export_pgp_sidecar.py` — PGP sidecar builder
- Both follow: read source → transform → write SQLite sidecar pattern

## Deferred Ideas (captured, not for this phase)
- **Text correction via few-shot:** Use Dicta LM for correcting OCR/transcription text via few-shot examples (user-suggested future feature)
- **On-demand translation fallback:** If new data appears after batch, translate on-demand until next batch (decided against for v1 — batch only)

## Research Priorities
1. Investigate translate.dicta.org.il few-shot format and default templates
2. Count FJMS catalog bilingual gaps (how many Title without TitleHeb, etc.)
3. Test Dicta translation API: latency per request, any rate limits, response format
4. Build sample scholarly few-shots from existing bilingual FJMS pairs
5. Compare translation quality: Dicta defaults vs custom scholarly few-shots
6. Survey PGP document_type distinct values and check coverage in pgp_tag_translations.py
