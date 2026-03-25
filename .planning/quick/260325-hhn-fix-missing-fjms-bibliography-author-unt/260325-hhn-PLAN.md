---
phase: quick-260325-hhn
goal: "Fix missing FJMS bibliography authors + untranslated Penn/Halper catalog entries"
type: quick
---

# Quick Task 260325-hhn: Fix missing FJMS bibliography author + untranslated Penn/Halper catalog entries

## Task 1: Enrich bibliography with book-level authors from FIST.db

**Files:** fist_data/fjms_enrichment.db, FIST_DB_BACKUP/FIST.db
**Action:** Extract title→author mapping from FIST CODE_TitleAuthor + CODE_Author, match to bibliography entries via RunningTitle, update ArticleAuthorEng/Heb
**Verify:** Query bibliography for sys_id 990053965550205171 shows "Ehrlich, Uri" for Weekday Amidah and "Halper, Benzion" for Descriptive Catalogue
**Done:** 398,925 bibliography rows enriched with book-level authors (1,373 title-author mappings)

## Task 2: Add Hebrew translations for catalog source names and acronyms

**Files:** fist_data/fjms_enrichment.db
**Action:** Update SourceNameHeb in catalog + catalog_free_desc tables (53 catalog sources), TitleAcronymHeb in bibliography, RunningTitleHeb
**Verify:** catalog.SourceNameHeb for Penn Catalog = 'קטלוג פן', Halper Catalog = 'קטלוג האלפר'
**Done:** 306,395 catalog rows + 186,319 free_desc rows + 80,000 bibliography rows updated

## Task 3: Fix bibliography dialog author display order + catalog dialog translation support

**Files:** web/components/bibliography_dialog.py, web/components/catalog_dialog.py, shared/translation_service.py
**Action:**
- Bibliography dialog: show Hebrew author first in Hebrew mode (was showing English first)
- Catalog dialog: add en2he free description translation support for Hebrew UI (was only fetching he2en for English UI)
- Translation service: add direction-aware `get_fjms_free_desc_he()` method, add direction filter to existing `get_fjms_free_desc_en()`
- Fix toggle badge directions for bidirectional translations
**Verify:** In Hebrew mode, catalog dialog fetches en2he translations for English catalog descriptions
