# Translation QA and Improvement Plan

**Date:** 2026-03-11 (updated 2026-03-13)
**Status:** Completed
**Scope:** Phase 46 translation quality control for PGP, FJMS, library titles, and Oxford metadata

---

## 1. Goal

Reduce hallucinations, mistranslations, terminology drift, and misleading output in the new machine-translated metadata layer without losing the usefulness of broad bilingual coverage.

This plan covers:

- Detection of bad translations already stored in sidecar databases
- Prevention of bad translations in future batch runs
- Human review workflows for high-risk cases
- Product changes that make translation uncertainty visible to users

---

## 2. Current State

The current translation stack is strong operationally but weak on quality assurance:

- Runtime display is read-only via `shared/translation_service.py`
- Translation generation is offline via batch scripts in `scripts/`
- Dicta few-shot prompting exists in `shared/dicta_client.py`
- PGP document types already use a manual controlled mapping
- PGP already has a `--retranslate-nulls` path, which implies some hallucinated rows were manually cleared
- Tests currently verify wiring, schema, and basic API behavior, but not semantic accuracy
- Help text describes translations as machine-generated, but there is no explicit scholarly warning about hallucinations or a user feedback path for bad translations

Main risk areas by source:

- **PGP EN->HE descriptions:** long scholarly prose, names, places, dates, legal terminology
- **FJMS HE->EN free descriptions:** codicological jargon, abbreviations, Hebrew/Aramaic/Arabic transliteration
- **Library title HE->EN / EN->HE:** short labels where one wrong lexical choice propagates to thousands of rows
- **Oxford metadata EN->HE:** compact metadata phrases that are easy to over-interpret

---

## 3. Failure Modes To Target

### 3.1 Hallucination

The model invents content not present in the source:

- added people, places, dates, genres, or legal actions
- expanded abbreviations incorrectly
- inferred narrative details from partial catalog text

### 3.2 Semantic drift

The translation is fluent but wrong in meaning:

- `פיוט` rendered as `Poem` instead of `Piyyut`
- codicological descriptions normalized into generic English
- legal or bibliographic formulas paraphrased too freely

### 3.3 Transliteration inconsistency

The same term is alternately translated, transliterated, or both:

- Saadya / Saadia
- piyyut / poem
- ketubba / marriage contract
- Torah / Bible / Pentateuch in inconsistent contexts

### 3.4 Structural corruption

The output changes the shape of the source too much:

- dropped clauses
- merged list items
- broken punctuation around dates and shelfmarks
- untranslated source copied verbatim into the target language

### 3.5 Direction-specific problems

- **HE->EN:** should often preserve scholarly transliteration rather than plain-language translation
- **EN->HE:** should not invent Hebrew terminology for uncertain transliterations or names

---

## 4. Principles

1. **Prefer precision over fluency** for scholarly metadata.
2. **Treat short repeated strings as high leverage.**
3. **Use humans on risk-ranked samples, not on everything.**
4. **Store review state, not just translated text.**
5. **Never overwrite a reviewed manual correction with a fresh machine run.**

---

## 5. Workstreams

### A. Build a Translation Audit Dataset

Create a reproducible audit export that pulls source text plus translation plus metadata from all stores.

Recommended export columns:

- `dataset` (`pgp`, `fjms`, `titles`, `oxford`)
- `record_id`
- `field_name`
- `direction`
- `source_text`
- `translated_text`
- `model_version`
- `translated_at`
- `source_length`
- `target_length`
- `review_status`
- `review_notes`

Recommended first deliverable:

- `scripts/export_translation_audit_sample.py`
- output CSV files under `reports/translation_audit/`

Sampling strategy:

- 200 random PGP descriptions
- 200 random FJMS free descriptions
- 100 random FJMS short fields
- 100 top-frequency library titles
- 100 EN->HE title backfills (`english_title_he`)
- 50 Oxford metadata phrases

Also include targeted samples:

- longest texts
- shortest texts
- top repeated strings
- rows previously nulled or retried
- rows with mixed Hebrew/Latin script

### B. Add Automatic Quality Heuristics

Introduce a lightweight validator layer that flags suspicious output before or after insertion.

Recommended checks:

- source/target length ratio outliers
- copied-source detection
- script-balance mismatch
- too many numerals added or removed
- bracket/parenthesis mismatch
- named-entity count drift
- suspicious generic words replacing transliterations
- repeated punctuation or truncated endings

Recommended implementation:

- `shared/translation_qc.py`
- `scripts/audit_translations.py`

Each translation should receive:

- `qc_score`
- `qc_flags` as a JSON array or pipe-separated string

High-value rule examples:

- Flag HE->EN output that contains too little Latin text for clearly non-English output
- Flag EN->HE output that still contains most of the original English sentence
- Flag any output that adds question marks, explanatory glosses, or new parentheses not present in the source

### C. Create Controlled Terminology Lists

Build a project glossary for terms that must not drift.

Priority glossary groups:

- genres: `פיוט`, `כתובה`, `שו"ת`, `מדרש`, `תוספתא`
- codicology: folio, bifolium, quire, verso, recto, margin, damage, ruling
- languages and scripts
- personal names with preferred transliterations
- place names
- common legal formulas

Recommended storage:

- `data/translation_glossary.csv`

Columns:

- `term_source`
- `direction`
- `preferred_translation`
- `forbidden_variants`
- `notes`
- `domain`

Use cases:

- post-processing normalization for safe replacements
- validator checks against forbidden variants
- future prompt enrichment

### D. Add Review Status to Translation Tables

Current schemas store translated text but not editorial trust.

Recommended schema additions:

#### `pgp_translations`

- `review_status` (`machine`, `flagged`, `reviewed`, `manual`, `rejected`)
- `qc_score`
- `qc_flags`
- `reviewed_by`
- `reviewed_at`
- `review_notes`
- `source_hash`

#### `fjms_translations`

Add the same fields.

#### `title_translations`

Add equivalent fields for `english_title` and `english_title_he`.

This enables:

- re-running only low-confidence rows
- protecting manually corrected rows
- filtering UI to prefer reviewed output later

### E. Create a Human Review Workflow

Start with a simple offline workflow before building a full UI.

Phase 1:

- export flagged rows to CSV
- review in spreadsheet
- import approved corrections back into SQLite

Phase 2:

- build an admin review page for translations, similar to correction review
- show source and translation side by side
- allow `Approve`, `Edit`, `Reject`, `Mark as misleading`

Reviewer priority order:

1. top-frequency repeated titles
2. flagged high-risk PGP descriptions
3. flagged FJMS free descriptions
4. Oxford metadata

### F. Improve Batch Generation Scripts

Update translation scripts so quality control is part of generation, not only after the fact.

Recommended changes:

- validate every translation before writing
- write flagged rows with `review_status='flagged'`
- optionally skip direct insertion for the worst failures
- log per-batch QC summaries
- preserve source text where not already stored

Script-specific notes:

- `scripts/translate_pgp_descriptions.py`
  - add QC immediately after `desc_he`
  - keep `--retranslate-nulls`
  - add `--retranslate-flagged`
- `scripts/translate_fjms_free_desc.py`
  - add QC for long scholarly descriptions
- `scripts/translate_libraries_titles.py`
  - add high-frequency priority review file
- `scripts/translate_library_titles_en2he.py`
  - add stricter checks because short labels are easy to mistranslate

### G. Improve Prompting Strategy

The current few-shot templates are useful but too generic for all fields.

Recommended next step:

- split prompts by domain, not just by direction

Suggested templates:

- `few_shot_pgp_en2he.json`
- `few_shot_title_he2en.json`
- `few_shot_title_en2he.json`
- `few_shot_codicology_he2en.json`
- `few_shot_oxford_en2he.json`

Prompt rules:

- instruct the model to avoid adding information
- prefer transliteration for titles, names, and technical terms unless a standard translation exists
- preserve dates, punctuation, and bracket structure
- preserve uncertainty markers from the source

### H. Add Product Safeguards

Users should see that these translations are assistive, not authoritative.

Recommended product changes:

- add a visible disclaimer near the translation toggle:
  - "Machine-generated scholarly aid; may contain errors or hallucinations."
- add a `Report translation issue` action in web and desktop
- show original text by default for especially high-risk fields if `review_status != reviewed`
- optionally badge reviewed translations differently from raw machine output

Best first UI targets:

- `web/pages/help.py`
- `web/pages/browse.py`
- `genizah_app.py`

---

## 6. Execution Plan

### Phase 1: Audit Setup

Duration: 1 to 2 days

- export representative samples from all translation stores
- define QC heuristics
- create reviewer spreadsheet template
- review 500 to 750 rows manually

Deliverables:

- audit CSVs
- first error taxonomy
- ranked glossary backlog

### Phase 2: Heuristic Detection

Duration: 2 to 3 days

- implement `shared/translation_qc.py`
- score all existing translations
- classify rows into `machine` vs `flagged`
- produce summary reports by dataset and field

Deliverables:

- QC script
- flagged-row export
- top recurring failure patterns

### Phase 3: High-Leverage Cleanup

Duration: 2 to 4 days

- fix top repeated title mistranslations
- normalize glossary-critical terms
- clear or replace obvious hallucinations
- use targeted retranslation for flagged subsets only

Deliverables:

- corrected glossary pass
- reduced error rate on repeated strings
- updated sidecar rows

### Phase 4: Workflow Integration

Duration: 3 to 5 days

- add review metadata columns
- wire QC into batch scripts
- add issue-report path in UI
- add disclaimer text in product help and translation surfaces

Deliverables:

- review-aware schema
- safer future translation runs
- better user transparency

---

## 7. Success Metrics

Use measurable targets rather than subjective confidence.

### Audit Quality

- at least 750 reviewed samples across datasets
- inter-reviewer agreement tracked on a subset
- each failure assigned a taxonomy label

### Existing Data Cleanup

- 100% of top 100 repeated title strings reviewed
- 100% of previously nulled PGP rows revisited
- at least 90% of high-QC-risk rows reviewed or cleared

### Ongoing Pipeline Quality

- every newly generated translation gets QC metadata
- no reviewed/manual row is overwritten automatically
- flagged rate decreases on later reruns

### Product Safety

- translation disclaimer visible in both apps
- users can report problematic translations
- reviewed translations can be distinguished from raw machine output

---

## 8. Immediate Next Actions

1. Build a cross-dataset audit export script and generate the first sample pack.
2. Manually review the top repeated titles before touching long-tail rows.
3. Implement a first-pass QC module with 5 to 8 simple heuristic rules.
4. Add `review_status`, `qc_score`, and `qc_flags` to translation tables.
5. Add a user-facing disclaimer and issue-report mechanism.
6. Retranslate only flagged subsets after glossary and prompt improvements.

---

## 9. Suggested Order of Attack

If team time is limited, use this order:

1. **Library titles**  
   Reason: one fix can improve thousands of rows.
2. **PGP descriptions**  
   Reason: user-visible scholarly prose and existing evidence of hallucination cleanup.
3. **FJMS short fields**  
   Reason: smaller set, easier to stabilize with glossary rules.
4. **FJMS free descriptions**  
   Reason: highest volume and highest review cost.
5. **Oxford metadata**  
   Reason: useful but lower impact than the others.

---

## 10. Non-Goals

This plan does not attempt to:

- replace Dicta entirely
- make every translation fully publication-grade without human review
- solve full scholarly transliteration standardization in one pass
- build a complex review UI before the audit and validator layers exist

---

## 11. Rationale

The system already has broad coverage and good runtime wiring. The next bottleneck is trust.

The fastest path to better trust is:

- review the most repeated strings first
- add automatic QC flags for obviously bad output
- preserve human decisions in the database
- expose uncertainty clearly in the UI

That combination will produce much more value than re-running the entire corpus with only minor prompt tweaks.
