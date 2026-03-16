# Translation Round 3 — Gap-Closing Batch

> Created: 2026-03-12
> Status: SERVER BATCH RUNNING
> Context: Phase 46 (Dicta Translation), v6.5.0 milestone
> Branch: `46-translation-wiring-round2`

---

## 1. Background

After Rounds 1 and 2 completed (~498K translations in fjms_translations), a thorough
gap analysis revealed ~206K untranslated rows across FJMS catalog tables. This Round 3
closes those gaps using a lightweight extract-translate-merge approach.

### What triggered this round

A database-level investigation of every FJMS catalog table compared existing translations
against source data, classifying each gap by language and direction. Several "gaps"
previously reported turned out to be non-gaps:

| Reported Gap | Actual | Why |
|-------------|--------|-----|
| Library title gaps (32K) | 0 | All blank titles — nothing to translate |
| Library EN->HE backfill (102K) | N/A | `hebrew_title` already serves users; not needed |
| TextualFrame (179K) | 0 | Source table already has both `TextualFrameHeb` and `TextualFrameEng` |
| PGP 885 stubs | 0 | 1-2 word placeholders (e.g., "Accounts.", "Receipt.") |
| PGP 3,686 missing types | 0 | Source `documents` table has NULL/empty `document_type` |

---

## 2. What Was Done Locally (2026-03-12)

### 2.1 RunningTitle EN->HE (295 unique titles, 8,098 rows)

English-only RunningTitles in AlmaIds with zero existing translations. These are short
scholarly terms ("Rabbinica", "Zohar", "Amidah", "Leviticus", etc.) that appear across
9,112 catalog rows but deduplicate to only 295 unique strings.

**Method:** Manual Hebrew mapping for all 295 titles (no Dicta API needed). Scholarly
accuracy verified — these are standard Genizah/Judaic studies terms.

**Script:** `scripts/translate_rt_en2he_local.py`
**Result:** 8,098 inserted, 1,014 skipped (already existed from Round 2 overlap)
**Backup:** `fist_data/fjms_enrichment_pre_round3.db` (created before any inserts)

### 2.2 Server Batch CSVs Extracted

Extracted 5 CSV files containing untranslated rows, classified by source table,
language, and required direction:

| File | Rows | Field | Direction | Source |
|------|------|-------|-----------|--------|
| `freedesc_en2he.csv` | 23,389 | FreeDesc | EN->HE | English free descriptions with no Hebrew translation |
| `freedesc_he2en.csv` | 3,048 | FreeDesc | HE->EN | Hebrew free descriptions with no English translation |
| `fulltext_en2he.csv` | 4,971 | FullText | EN->HE | English full texts with no Hebrew translation |
| `fulltext_he2en.csv` | 14,680 | FullText | HE->EN | Hebrew full texts with no English translation |
| `rt_he2en.csv` | 159,910 | RunningTitle | HE->EN | Hebrew running titles with no English translation |
| **Total** | **205,998** | | | |

**Extraction script:** `scripts/extract_translation_gaps.py`
**Output directory:** `scripts/translation_gaps/`
**Total CSV size:** ~22 MB

### 2.3 Gap Detection Method

The extraction uses a per-AlmaId subquery approach to avoid many-to-many join inflation:

```sql
SELECT r.AlmaId, r.RunningTitle
FROM catalog_running_titles r
LEFT JOIN (SELECT DISTINCT alma_id FROM fjms_translations
           WHERE field_name = 'RunningTitle' AND direction = 'he2en') t
ON r.AlmaId = t.alma_id
WHERE t.alma_id IS NULL
AND r.RunningTitle IS NOT NULL AND LENGTH(r.RunningTitle) > 3
```

Language classification uses regex: `has_latin()` and `has_hebrew()` to determine direction.

---

## 3. Server Batch (RUNNING)

### 3.1 Server Details

| Item | Value |
|------|-------|
| Server | `ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| Screen session | `translate-r3` |
| Started | 2026-03-12 ~05:50 UTC |
| Script | `scripts/translate_gaps_server.py --workers 5` |
| Log file | `translate_gaps_log.txt` |
| God mode | Yes (`DICTA_GOD_MODE=bagatz` in `.env`) |
| Workers | 5 parallel (ThreadPoolExecutor waves) |
| Rate | ~3 items/sec observed |

### 3.2 Batch Order and Estimated Times

| # | Batch | Rows | Direction | Est. Time |
|---|-------|------|-----------|-----------|
| 1 | `freedesc_en` | 23,389 | EN->HE | ~2h |
| 2 | `freedesc_he` | 3,048 | HE->EN | ~17min |
| 3 | `fulltext_en` | 4,971 | EN->HE | ~28min |
| 4 | `fulltext_he` | 14,680 | HE->EN | ~1.4h |
| 5 | `rt` | 159,910 | HE->EN | ~15h |
| | **Total** | **205,998** | | **~19h** |

### 3.3 Checkpointing

- JSON checkpoint files in `scripts/translation_gaps/checkpoint_{batch_name}.json`
- Checkpoints saved every 100 items (set of completed `alma_id` values)
- Atomic writes via `tempfile` + `os.replace`
- SIGINT handler saves checkpoint before exit
- Script resumes automatically from checkpoint on restart

### 3.4 Results Output

- CSV files in `scripts/translation_results/results_{batch_name}.csv`
- Schema: `alma_id, field_name, original_text, translated_text, direction`
- Appended row-by-row (survives crashes — no data loss)

### 3.5 How to Monitor

```bash
# SSH to server
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com

# Check live log
tail -f ~/GenizahSearch/translate_gaps_log.txt

# Check results progress
wc -l ~/GenizahSearch/scripts/translation_results/results_*.csv

# Check which batch is active
grep "Batch " ~/GenizahSearch/translate_gaps_log.txt | tail -5

# Check checkpoint state
cat ~/GenizahSearch/scripts/translation_gaps/checkpoint_*.json | python3 -m json.tool | grep -E '"batch"|"completed".*count|"stats"'
```

### 3.6 How to Resume After Interruption

```bash
screen -r translate-r3   # reattach if still running
# OR restart if screen died:
cd ~/GenizahSearch
screen -dmS translate-r3 bash -c 'python3 scripts/translate_gaps_server.py --workers 5 2>&1 | tee -a translate_gaps_log.txt; echo DONE'
```

The script automatically skips completed items from checkpoint files.

---

## 4. Post-Batch Steps (After Server Finishes)

### Step 1: Download Results

```bash
# From local machine
SERVER="ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com"
scp -r $SERVER:~/GenizahSearch/scripts/translation_results/ scripts/translation_results/
```

### Step 2: Verify Results

```bash
# Check row counts
wc -l scripts/translation_results/results_*.csv

# Sample a few translations
head -5 scripts/translation_results/results_freedesc_en.csv
head -5 scripts/translation_results/results_rt.csv
```

### Step 3: Merge into Local DB

```bash
# Dry run first
python scripts/merge_translation_results.py --dry-run

# Real merge (creates backup automatically)
python scripts/merge_translation_results.py
```

The merge script:
- Creates backup: `fist_data/fjms_enrichment_pre_merge_YYYYMMDD_HHMMSS.db`
- Deduplicates: skips rows where (alma_id, field_name, direction, original_text) already exists
- Checkpoints every 5,000 inserts
- Reports final counts

### Step 4: QC Pass (CRITICAL)

Run QC on the new Round 3 translations using the existing infrastructure:

```bash
# Score all new translations
python -c "
import sqlite3
from shared.translation_qc import score_translation

conn = sqlite3.connect('fist_data/fjms_enrichment.db')
c = conn.cursor()
c.execute(\"SELECT id, original_text, translated_text, direction FROM fjms_translations WHERE model_version = 'dictalm2.0-round3'\")
bad = 0
for row_id, src, tgt, direction in c.fetchall():
    score, flags = score_translation(src, tgt, direction)
    if score < 0.5 or 'copied_source' in flags or 'script_mismatch' in flags:
        bad += 1
        c.execute('DELETE FROM fjms_translations WHERE id = ?', (row_id,))
conn.commit()
print(f'Deleted {bad} bad translations')
conn.close()
"
```

Follow the full QA process from `docs/plans/TRANSLATION_QA_IMPROVEMENT_PLAN.md`:
1. **Workstream A**: Export audit samples from Round 3 translations
2. **Workstream B**: Run heuristic QC (score_translation on all new rows)
3. Delete rows with: copied_source, script_mismatch, score < 0.5
4. Review flagged rows (7-20% typical)

### Step 5: Update Stats

```bash
# Recompile TRANSLATION_STATS.md with updated numbers
python -c "
import sqlite3
conn = sqlite3.connect('fist_data/fjms_enrichment.db')
c = conn.cursor()
c.execute('SELECT field_name, direction, COUNT(*) FROM fjms_translations GROUP BY field_name, direction ORDER BY COUNT(*) DESC')
for row in c.fetchall():
    print(f'{row[0]:30s} {row[1]:6s} {row[2]:>8,}')
c.execute('SELECT COUNT(*) FROM fjms_translations')
print(f\"{'TOTAL':30s} {'':6s} {c.fetchone()[0]:>8,}\")
conn.close()
"
```

Update `docs/TRANSLATION_STATS.md` with:
- Round 3 batch results (per-batch success/failure counts)
- Updated total translation counts
- Post-QC cleanup numbers
- Revised coverage percentages
- New gap analysis (what remains after Round 3)

### Step 6: Upload Cleaned DB to Server

After QC cleanup, the local DB becomes the authoritative copy:

```bash
SERVER="ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com"
# Backup server DB first
ssh $SERVER "cp ~/GenizahSearch/fist_data/fjms_enrichment.db ~/GenizahSearch/fist_data/fjms_enrichment_pre_round3_merge.db"
# Upload
scp fist_data/fjms_enrichment.db $SERVER:~/GenizahSearch/fist_data/fjms_enrichment.db
```

---

## 5. Files Created/Modified in Round 3

| File | Purpose |
|------|---------|
| `scripts/translate_rt_en2he_local.py` | Local translation of 295 EN RunningTitles (manual mapping) |
| `scripts/extract_translation_gaps.py` | Extract untranslated rows as CSVs |
| `scripts/translate_gaps_server.py` | Server batch translation with parallel workers + checkpoints |
| `scripts/merge_translation_results.py` | Merge result CSVs back into fjms_enrichment.db |
| `scripts/translation_gaps/*.csv` | 5 extracted gap CSVs (uploaded to server) |
| `scripts/translation_results/*.csv` | Translation results (created by server script) |
| `docs/TRANSLATION_STATS.md` | Updated with Round 3 status and gap analysis |
| `.planning/phases/46-dicta-translation/ROUND3_GAP_CLOSING.md` | This document |
| `.planning/phases/46-dicta-translation/.continue-here.md` | Session checkpoint |

## 6. Backups

| Backup | Contents | Created |
|--------|----------|---------|
| `fist_data/fjms_enrichment_pre_round3.db` | DB before any Round 3 inserts (498,475 translations) | 2026-03-12 |
| `fist_data/fjms_enrichment_pre_qc_cleanup.db` | DB before Round 2 QC cleanup (556,282 translations) | 2026-03-12 |
| Server checkpoint JSONs | Completed alma_ids per batch (for resume) | Ongoing |

---

## 7. Expected Outcome

After Round 3 completes and QC passes:

| Metric | Before Round 3 | After Round 3 (est.) |
|--------|---------------|---------------------|
| fjms_translations total | 498,475 | ~700K |
| RunningTitle coverage | 133,767 (en2he only) | ~300K (both directions) |
| FreeDesc coverage | 196,314 (he2en only) | ~220K (both directions) |
| FullText coverage | 70,797 (en2he only) | ~90K (both directions) |
| Overall FJMS coverage | 48.5% | ~70% est. |

The remaining untranslated content after Round 3 will be:
- Mixed-language texts (5K) — need per-row language detection
- Very short texts (<20 chars) — below min_length threshold
- Texts in neither Hebrew nor English (Arabic, etc.)
