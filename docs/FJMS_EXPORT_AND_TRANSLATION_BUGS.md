# FJMS Export & Translation Display Bugs — Technical Report

> **Date:** 2026-03-11
> **Reporter:** User report + investigation
> **Reviewed by:** GPT Codex (corrections incorporated)
> **Branch:** 46-translation-wiring-round2
> **Status:** Open — both bugs confirmed, fixes not yet applied

---

## Bug 1: FJMS Export Drops Catalog Records (MAX(Version) Filter)

### Summary

The FIST.db → fjms_enrichment.db export script (`scripts/export_fist_enrichment.py`) uses a `MAX(Version)` filter when joining `dbo_Signature` to child tables in **6 of its export functions**. This drops catalog records, running titles, sizes, fields, textual frames, and mentions when the latest signature version has no child data but earlier versions do.

Three other export functions (`export_catalog_free_desc`, `export_catalog_full_texts`, `export_bibliography`) have already been fixed — they do NOT use the MAX(Version) filter (see comments at lines 563-566 and 624).

### User-Reported Symptom

ENA 3021.7 shows "FGP Seride Teshuvot Team: Shocken Institute" on the FJMS website but **nothing** from that team in the desktop app's FJMS catalog dialog.

### Root Cause

**Join path in export:**
```
dbo_InventoryAlma → dbo_Inventory → dbo_InventorySignature → dbo_Signature → child table
```

**The problematic filter (present in 6 export functions):**
```sql
JOIN (
    SELECT SetSignatureId, MAX(Version) as MaxVersion
    FROM dbo_Signature GROUP BY SetSignatureId
) lsv ON sig.SetSignatureId = lsv.SetSignatureId
    AND sig.Version = lsv.MaxVersion
```

**For ENA 3021.7 (AlmaId=990053594100205171), Glick team (SourceId=151 = Seride Teshuvot):**

| SetSignatureId | SignatureId | Version | SourceId | Has UnitCatalogRec? |
|----------------|------------|---------|----------|---------------------|
| 63682814 | 101124814 | 1 | 151 (Glick) | **YES** (UnitCatalogRecId=34920814) |
| 63682814 | 101125814 | 1 | 151 (Glick) | No |
| 63682814 | 101126814 | 1 | 151 (Glick) | No |
| 63682814 | 101131814 | 2 | 151 (Glick) | No |
| 63682814 | 101839814 | **3 (MAX)** | 151 (Glick) | **No** ← picked by filter |

The `MAX(Version)` filter picks SignatureId=101839814 (version 3), which has **no** UnitCatalogRec. The actual catalog data is on version 1 (SignatureId=101124814), which gets dropped.

### Scale of Data Loss (catalog table only — primary impact)

| Metric | Value |
|--------|-------|
| Total UnitCatalogRec in FIST.db | 411,022 |
| Currently exported (with MAX filter) | 373,060 |
| **Lost catalog records** | **37,962 (9.2%)** |
| Distinct AlmaIds affected | 33,410 |
| DISTINCT (AlmaId, UnitCatalogRecId) without filter | 730,624 |
| DISTINCT (AlmaId, UnitCatalogRecId) with MAX filter | 685,261 |
| **Recoverable rows** | **45,363** |
| UnitCatalogRecIds appearing on multiple versions | **0** (no duplication risk) |

Note: 730K > 411K because a single UnitCatalogRecId can map to multiple AlmaIds via different inventory paths (range shelfmarks — 26,643 catalog recs serve multiple AlmaIds). This is correct behavior.

### Lost Records by Team/Source (Top 10)

| Source | Lost Records |
|--------|-------------|
| Catalogs (SourceId=500) | 23,675 |
| Institution (SourceId=400) | 3,488 |
| Aggadic Midrashim | 2,483 |
| Firkovitch Collections | 1,065 |
| Nuscha | 982 |
| Inventory | 899 |
| Books | 857 |
| Handlists (SourceId=300) | 810 |
| **Glick (Seride Teshuvot)** | **725** |
| MAGIC | 716 |

### Affected Export Functions (6 — still use MAX(Version))

| Function | Line | Child Table |
|----------|------|-------------|
| `export_catalog()` | 226 (line 278) | dbo_UnitCatalogRec |
| `export_catalog_running_titles()` | 358 (line 383) | dbo_CatalogMultiRunningTitle (via UnitCatalogRec) |
| `export_catalog_sizes()` | 419 (line 448) | Size data via UnitCatalogRec |
| `export_catalog_fields()` | 483 (line 510) | Field data via UnitCatalogRec |
| `export_catalog_textual_frames()` | 677 (line 707) | TextualFrame columns on UnitCatalogRec |
| `export_catalog_mentions()` | 744 (line 776) | Mention data via UnitCatalogRec |

### Already-Fixed Functions (3 — do NOT use MAX(Version))

| Function | Line | Note |
|----------|------|------|
| `export_catalog_free_desc()` | 548 | Comment at line 563: "No latest-version filter here" |
| `export_catalog_full_texts()` | 619 | Comment at line 624: "no version filter" |
| `export_bibliography()` | 813 | No version filter in query |

### Proposed Fix

Remove the `MAX(Version)` filter from the 6 affected functions, matching the pattern already used by `export_catalog_free_desc` and `export_catalog_full_texts`:

```sql
-- BEFORE (drops records):
JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
JOIN (
    SELECT SetSignatureId, MAX(Version) as MaxVersion
    FROM dbo_Signature GROUP BY SetSignatureId
) lsv ON sig.SetSignatureId = lsv.SetSignatureId
    AND sig.Version = lsv.MaxVersion
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId

-- AFTER (keeps all records with data):
JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
```

### Multi-Version Deduplication Policy

**Why removing MAX(Version) is safe — verified per child table:**

All 6 affected functions join through `dbo_Signature → dbo_UnitCatalogRec → child table` via `UnitCatalogRecId`. We verified that `UnitCatalogRecId` never appears on multiple signature versions for **every** child table:

| Child Table | UnitCatalogRecIds on multiple versions |
|-------------|---------------------------------------|
| dbo_UnitCatalogRec (catalog) | **0** |
| dbo_CatalogMultiRunningTitle (running_titles) | **0** |
| dbo_CatalogMultiSize (sizes) | **0** |
| dbo_CatalogMultiField (fields) | **0** |
| dbo_UnitCatalogRec TextualFrame columns (textual_frames) | **0** (same table as catalog) |
| dbo_CatalogMultiMention (mentions) | **0** |

Each UnitCatalogRecId is created on exactly one SignatureId at one version. The MAX filter was filtering out *the entire SetSignatureId* when the latest version lacked child data, not preventing duplicates.

**Additional safety factors:**

1. **`DISTINCT` handles cross-inventory duplication.** A single UnitCatalogRecId can appear via multiple inventory paths (e.g., range shelfmarks like ENA 3021.6-13 map to multiple InventoryIds). The `SELECT DISTINCT` on all columns already dedupes these.

2. **Precedent:** `export_catalog_free_desc` and `export_catalog_full_texts` already successfully use this pattern (no MAX filter + DISTINCT) with explicit comments explaining why (lines 563-566, 624).

3. **Downstream consumers are safe.** `fjms_service.py:get_catalog_records()` groups records into team columns by source_name and deduplicates by UnitCatalogRecId among other fields (lines 2133-2165). Since each UnitCatalogRecId is unique per version, no merge conflicts arise.

### Post-Fix Steps

1. Re-run `export_fist_enrichment.py` (FTS5 index rebuild is automatic via `create_fts5()` at line 1141)
2. Re-run translation scripts for newly exported records (RunningTitle EN→HE, FullText EN→HE for new entries)
3. Verify ENA 3021.7 shows Glick/Seride Teshuvot team

### Verification Query

```sql
-- Should return Glick catalog rec after fix
SELECT c.UnitCatalogRecId, c.SourceName
FROM catalog c
WHERE c.AlmaId = '990053594100205171'
AND c.SourceName = 'Glick';
```

### Suggested Tests

- Export fixture test: create a minimal FIST.db subset with "v1 has child, latest version empty" pattern, run export, verify child data is preserved.

---

## Bug 2: Desktop Translation Toggle Shows Wrong Language by Default

### Summary

The FJMS catalog dialog in the desktop app (`genizah_app.py`) shows translations by default when `show_translations=True`, regardless of UI language or translation direction. This is wrong for `en2he` fields in English UI (shows Hebrew where user expects English) and would also be wrong for `he2en` fields in Hebrew UI.

### User-Reported Symptom

- Running title in Hebrew is **not** translated to English in the English UI
- English running titles correctly translate to Hebrew when Translations are ON

### Root Cause

**Translation directions are mixed across field types:**

| Field | Direction | Count | Source → Translation |
|-------|-----------|-------|---------------------|
| FreeDesc | `he2en` | 254,011 | Hebrew → English |
| RunningTitle | `en2he` | 111,301 | English → Hebrew |
| FullText | `en2he` | 76,857 | English → Hebrew |
| Title | `he2en` | 1,152 | Hebrew → English |
| PersonEngDesc | `he2en` | 1,163 | Hebrew → English |
| AuthorText | `he2en` | 178 | Hebrew → English |
| PersonHebDesc | `en2he` | 702 | English → Hebrew |
| GenizahTitleEngTitle | `he2en` | 626 | Hebrew → English |
| TitleHeb | `en2he` | 8 | English → Hebrew |

**The critical issue:** The desktop display code treats all translations identically — it always shows `translated_text` by default (toggled=False). But `translated_text` is Hebrew for `en2he` fields and English for `he2en` fields. The UI needs to know the direction to choose the correct default.

**Desktop display logic** (3 locations in `genizah_app.py`):

```python
# Line 6784-6792 (RunningTitle — en2he)
orig = str(rt_text).strip()           # catalog value (English for en2he fields)
trans = str(_rt_trans_map.get(rec_id, '')).strip()  # translated_text (Hebrew for en2he)
_should_swap = bool(trans and trans != orig)
toggled = self._cat_toggle_state.get(toggle_key, False)
if _should_swap:
    show_text = orig if toggled else trans    # DEFAULT: shows trans (Hebrew) — WRONG for EN UI
```

Same pattern at lines 6951-6958 (FreeDesc — **he2en**, so trans=English) and 6994-7000 (FullText — en2he).

**Current behavior matrix:**

| Field | Direction | `trans` is | Default shows | EN UI correct? | HE UI correct? |
|-------|-----------|-----------|--------------|----------------|----------------|
| RunningTitle | en2he | Hebrew | Hebrew | NO (wants English) | YES |
| FullText | en2he | Hebrew | Hebrew | NO (wants English) | YES |
| FreeDesc | he2en | English | English | YES | NO (wants Hebrew) |

**The service layer drops direction info.** Both `get_fjms_translations_batch()` (line 360) and `get_fjms_translations_by_signature_ids()` (line 392) return only `translated_text`, not `direction`. The desktop renderer cannot determine which language the translation is in.

### Proposed Fix (Service + UI)

**Step 1: Return direction from translation service.**

Modify `get_fjms_translations_by_signature_ids()` to return `(translated_text, direction)`:

```python
# shared/translation_service.py:392-420
# BEFORE:
rows = self._fjms_conn.execute(
    f"SELECT signature_id, translated_text FROM fjms_translations "
    f"WHERE field_name = ? AND signature_id IN ({placeholders})",
    [field_name] + signature_ids,
).fetchall()
return {row[0]: row[1] for row in rows if row[1]}

# AFTER:
rows = self._fjms_conn.execute(
    f"SELECT signature_id, translated_text, direction FROM fjms_translations "
    f"WHERE field_name = ? AND signature_id IN ({placeholders})",
    [field_name] + signature_ids,
).fetchall()
return {row[0]: (row[1], row[2]) for row in rows if row[1]}
```

**Step 2: Make desktop display direction-aware.**

```python
# genizah_app.py — for each toggle section (RunningTitle, FreeDesc, FullText):
trans_data = _rt_trans_map.get(rec_id)  # now (translated_text, direction) or None
if trans_data:
    trans, direction = trans_data
    trans = str(trans).strip()
    _should_swap = bool(trans and trans != orig)
    if _should_swap:
        # For en2he: trans is Hebrew. EN UI → show orig (English) by default.
        # For he2en: trans is English. EN UI → show trans (English) by default.
        if direction == 'en2he':
            _show_trans_default = is_heb      # Hebrew UI → show Hebrew trans
        else:  # he2en
            _show_trans_default = not is_heb   # English UI → show English trans

        if _show_trans_default:
            show_text = orig if toggled else trans
        else:
            show_text = trans if toggled else orig
```

**Step 3: Update callers.** All 3 desktop sections (RunningTitle, FreeDesc, FullText) and the web's `get_fjms_translations_batch()` need the same direction-aware treatment. The web currently only fetches in English UI (`not is_heb`), which happens to work for `he2en` FreeDesc but would be wrong for `en2he` RunningTitle/FullText.

### Affected Code Locations

| File | Lines | Section | Direction | Bug confirmed? |
|------|-------|---------|-----------|----------------|
| `shared/translation_service.py` | 392-420 | `get_fjms_translations_by_signature_ids()` — add direction to return | Both | Root cause |
| `shared/translation_service.py` | 360-390 | `get_fjms_translations_batch()` — add direction to return | Both | Root cause |
| `genizah_app.py` | 6572-6583 | Translation service init (no language check) | — | — |
| `genizah_app.py` | 6784-6796 | RunningTitle toggle display | en2he | YES — shows HE in EN UI |
| `genizah_app.py` | 6946-6964 | FreeDesc toggle display | he2en | YES — shows EN in HE UI |
| `genizah_app.py` | 6990-7006 | FullText toggle display | en2he | YES — shows HE in EN UI |
| `web/components/catalog_dialog.py` | 274-286 | Web RunningTitle replacement | en2he | YES — replaces EN RT with HE (`_rt_en` is actually Hebrew) |
| `web/components/catalog_dialog.py` | 482-504 | Web FreeDesc replacement | he2en | No — works by coincidence (he2en + `not is_heb` = correct) |

### Web Is Also Affected (Confirmed)

The web dialog has the **same direction-blindness bug**, confirmed via code trace:

**RunningTitle (en2he) in English UI — web:**
- `catalog_dialog.py:49`: `if not is_heb:` fetches translations
- `catalog_dialog.py:274`: `_rt_en = fjms_trans.get('RunningTitle')`
- `get_fjms_translations_batch()` returns `translated_text` which for en2he is **Hebrew**
- Line 286: `rt_vals.append(_rt_en)` — **replaces English RT with Hebrew**
- Variable name `_rt_en` is misleading — value is actually Hebrew

**FreeDesc (he2en) in English UI — web:**
- `catalog_dialog.py:482`: `if not is_heb:` fetches via `get_fjms_free_desc_en()`
- This works correctly because FreeDesc direction is he2en, so `translated_text` is English
- Has proper toggle badges (lines 520-547)

**FullText — web:**
- `catalog_dialog.py:446-468`: `_render_full_texts()` has no translation logic at all
- No direction issue, but also no translation support

**Summary:** The web RunningTitle display is confirmed broken in English UI (shows Hebrew instead of English). The web FreeDesc display works correctly by coincidence (he2en direction matches the `not is_heb` fetch condition). The fix should be applied to both web and desktop.

### Do NOT Copy the Web's Fetch Condition

Desktop should not simply copy the web's `if not is_heb` gate because:
- **Regresses FreeDesc in Hebrew UI** — FreeDesc is he2en, so the English translation would be useful as a toggle in Hebrew UI too.
- **Breaks the toggle paradigm** — Desktop users expect to see both languages and toggle between them.

The correct fix is direction-awareness at the service layer, not UI-only branching.

### Test Cases

1. **RunningTitle (en2he):** Record with English running title (e.g., "Numbers 3:14-7:57"). English UI + translations ON → should show English by default, toggle to Hebrew. Hebrew UI → should show Hebrew by default, toggle to English.
2. **FreeDesc (he2en):** Record with Hebrew free description. English UI + translations ON → should show English translation by default, toggle to Hebrew original. Hebrew UI → should show Hebrew original, toggle to English.
3. **FullText (en2he):** Same as RunningTitle pattern.
4. **ENA 3021.7** — Hebrew running titles from Danzig/Lieberman catalogs. No translation exists → no toggle badge, shows Hebrew regardless of UI. (Correct by definition.)

### Suggested Tests

- Unit test for desktop display-state selection: given `(direction, is_heb, toggled)`, assert correct `(show_text, badge_label)`.
- Integration test: mock `get_fjms_translations_by_signature_ids()` with known direction, verify toggle behavior.

---

## Relationship Between the Two Bugs

For ENA 3021.7 specifically, Bug 1 is the primary issue — the Seride Teshuvot/Glick team data is entirely missing from the export. Bug 2 is a separate UX issue that affects all records with translations.

Both bugs need separate fixes:
1. **Bug 1**: Fix export SQL in 6 functions, re-export FIST.db, re-run translations for new records
2. **Bug 2**: Add direction to translation service return values, make desktop renderer direction-aware (3 toggle sections), fix web RunningTitle replacement logic

---

## Manual Verification Checklist

### Test Records

| Shelfmark | sys_id | Use For |
|-----------|--------|---------|
| ENA 3021.7 | 990053594100205171 | Bug 1 (missing Glick/Seride Teshuvot) + Bug 2 (HE RT, no translation) |
| T-S AS 32.1 | 990051830350205171 | Bug 2 RT: EN RT "Numbers 3:14-7:57" with en2he translation |
| Add. 863, 2 | 990001391750205171 | Bug 2 RT: EN RT "PENTATEUCH (fragment)" with en2he translation |
| ENA 2674.10 | 990053585620205171 | Bug 2 FreeDesc: HE free desc with he2en translation |
| T-S AS 122.285 | 990052082340205171 | Bug 2 FreeDesc: HE free desc with he2en translation |

### Bug 1: Post-Export Verification (desktop app)

Run these checks after removing MAX(Version) from 6 functions and re-exporting:

**A. Reported case fixed:**
- [ ] Open ENA 3021.7 → FJMS Catalog dialog
- [ ] Verify "Glick" (Seride Teshuvot) column appears alongside Danzig and Lieberman
- [ ] Verify Glick column shows: Author field populated, 8 folios, 33-41 rows

**B. Existing data not broken (spot check 3-5 records that worked before):**
- [ ] Open T-S 12.1 → FJMS Catalog dialog → verify same team columns and data as before
- [ ] Open T-S 13J1.1 → FJMS Catalog dialog → verify same data
- [ ] Open any Oxford record with multiple teams → verify no duplicate columns or merged values

**C. Export counts sanity check (run after export):**
```sql
-- Run against new fjms_enrichment.db
SELECT 'catalog' as tbl, count(*) FROM catalog
UNION ALL SELECT 'running_titles', count(*) FROM catalog_running_titles
UNION ALL SELECT 'sizes', count(*) FROM catalog_sizes
UNION ALL SELECT 'fields', count(*) FROM catalog_fields
UNION ALL SELECT 'textual_frames', count(*) FROM catalog_textual_frames
UNION ALL SELECT 'mentions', count(*) FROM catalog_mentions;
-- All counts should be >= previous values. catalog should be ~730K (was 685K).
```

**D. No duplicate teams in UI:**
- [ ] Pick 5 random records → open FJMS Catalog → verify no team column appears twice
- [ ] If a record has range shelfmark data, verify it shows once per team (not duplicated)

**E. FTS5 search still works:**
- [ ] Desktop: Browse tab → text filter → search a known term → verify results appear
- [ ] Web: Browse page → catalog text filter → same search → verify results

**F. Translation coverage for new records:**
- [ ] After re-running RunningTitle translation: verify new English RTs got en2he translations
- [ ] Spot-check 3 newly exported records → open FJMS Catalog → verify running title shows

### Bug 2: Translation Direction Verification (desktop + web)

Run these checks after service layer returns direction and renderers are updated:

**G. Desktop — RunningTitle (en2he) with English UI:**
- [ ] Settings: Language=English, Translations=ON
- [ ] Open T-S AS 32.1 → FJMS Catalog → Running Title row
- [ ] Default should show: "Numbers 3:14-7:57" (English original)
- [ ] Click toggle badge → should switch to Hebrew translation
- [ ] Badge label should read "Translated" initially, "Original" after toggle

**H. Desktop — RunningTitle (en2he) with Hebrew UI:**
- [ ] Settings: Language=Hebrew, Translations=ON
- [ ] Open T-S AS 32.1 → FJMS Catalog → Running Title row
- [ ] Default should show: Hebrew translation (במדבר ג:יד-ז:נז)
- [ ] Click toggle badge → should switch to English original
- [ ] Badge label should read "מקור" initially, "מתורגם" after toggle

**I. Desktop — FreeDesc (he2en) with English UI:**
- [ ] Settings: Language=English, Translations=ON
- [ ] Open ENA 2674.10 → FJMS Catalog → Free Description section
- [ ] Default should show: English translation
- [ ] Click toggle badge → should switch to Hebrew original
- [ ] Verify text direction flips (LTR ↔ RTL) on toggle

**J. Desktop — FreeDesc (he2en) with Hebrew UI:**
- [ ] Settings: Language=Hebrew, Translations=ON
- [ ] Open ENA 2674.10 → FJMS Catalog → Free Description section
- [ ] Default should show: Hebrew original
- [ ] Click toggle badge → should switch to English translation

**K. Desktop — FullText (en2he):**
- [ ] Find a record with English scholarly description and en2he translation
- [ ] English UI: default should show English, toggle to Hebrew
- [ ] Hebrew UI: default should show Hebrew, toggle to English

**L. Desktop — No translation available (no regression):**
- [ ] Open ENA 3021.7 → FJMS Catalog → Running Title row
- [ ] Running titles are Hebrew with no translation → should show Hebrew, NO toggle badge
- [ ] Same behavior in both English and Hebrew UI

**M. Desktop — Translations OFF (no regression):**
- [ ] Settings: Translations=OFF
- [ ] Open any record → FJMS Catalog → verify NO toggle badges appear anywhere
- [ ] All fields show original catalog values only

**N. Web — RunningTitle (en2he) with English UI:**
- [ ] Set language to English
- [ ] Open T-S AS 32.1 → FJMS Catalog dialog → Running Title row
- [ ] Should show English original (NOT Hebrew translation)
- [ ] (Web RT has no toggle — just verify correct language displayed)

**O. Web — FreeDesc (he2en) with English UI (no regression):**
- [ ] Open ENA 2674.10 → FJMS Catalog → Free Description section
- [ ] Should show English translation (this already works — verify not broken)
- [ ] Toggle badge should work: click to see Hebrew original, click again to see English

**P. Web — Hebrew UI (no regression):**
- [ ] Set language to Hebrew
- [ ] Open same records → FJMS Catalog
- [ ] Running titles should show Hebrew catalog value (no translation logic in HE UI)
- [ ] Free descriptions should show Hebrew original (no translation fetched in HE UI)

### Regression Checklist

- [ ] `pytest tests/` — all existing tests pass
- [ ] Desktop: open 5 diverse records (CUL, JTS, Oxford, ENA, Manchester) → FJMS Catalog → no crashes, no empty dialogs, no duplicates
- [ ] Web: same 5 records → catalog dialog → same checks
- [ ] Desktop: search for a common term → verify search results still show, no translation artifacts
- [ ] Web: same search → same check

---

## Files Referenced

| File | Role |
|------|------|
| `scripts/export_fist_enrichment.py` | FIST.db → fjms_enrichment.db export (Bug 1) |
| `genizah_app.py:6565-7010` | Desktop FJMS catalog dialog (Bug 2) |
| `web/components/catalog_dialog.py:47-290` | Web FJMS catalog dialog (reference) |
| `shared/translation_service.py:360-420` | Translation batch lookup methods (Bug 2 — needs direction) |
| `shared/fjms_service.py:110-210` | Team name mappings (Glick = Seride Teshuvot) |
| `fist_data/FIST.db` | Source database |
| `fist_data/fjms_enrichment.db` | Exported sidecar database |
