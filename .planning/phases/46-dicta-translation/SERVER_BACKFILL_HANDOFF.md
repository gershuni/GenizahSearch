# FJMS Translation Backfill — Server Handoff

> Created: 2026-03-11
> Status: READY TO EXECUTE
> Context: Phase 46 (Dicta Translation), v6.5.0 milestone
> Reviewed: GPT-Codex audit applied (6 findings addressed)

## Background

GenizahSearch has ~730K FJMS catalog records with multilingual scholarly text. We translate between Hebrew and English using the Dicta LM 2.0 Translation API so users can search and browse in either language.

**Round 1** (completed 2026-03-07): Translated ~478K items across all categories — libraries titles, PGP descriptions, FJMS catalog fields, and FJMS free descriptions. All stored in three SQLite sidecar databases.

**Round 2** (started 2026-03-08): Added RunningTitle EN→HE (107K) and FullText EN→HE (46K) translations, plus a new TextualFrame mode. RunningTitle and FullText completed successfully. TextualFrame crashed at 62% (50K/81K) due to **SQLite database corruption** on the server.

**Bug fixes** (2026-03-11, commit `565446ae` on branch `46-translation-wiring-round2`):
1. **MAX(Version) export fix**: Removed `MAX(Version)` filter from `scripts/export_fist_enrichment.py` — recovered 45K catalog rows (685K→730K). The local `fjms_enrichment.db` was rebuilt with the full dataset.
2. **Translation direction fix**: `shared/translation_service.py` now returns `(text, direction)` tuples so the UI shows the correct default language. All callers updated (desktop + web).

Because the local DB was rebuilt from scratch with the export fix, it has the correct catalog data (730K rows) but needs the Round 2+ translations backfilled via Dicta API.

## Current State

### Server (`ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com`)

- **Git branch**: `master-main` (does NOT have the latest bug fix scripts or textualframe mode)
- **fjms_enrichment.db**: CORRUPTED (schema pages unreadable, `database disk image is malformed`)
- **Backup**: `fist_data/fjms_enrichment_corrupt_backup.db` (1.1GB, same corruption)
- **Recovery attempted**: `.recover`, `iterdump`, immutable mode, chunked rowid reads — all fail. Corruption is at the B-tree level, not just WAL.
- **Screen sessions**: 3 detached (`fjms-translate`, `en2he-titles`, `tf-translate`) — all finished or crashed

**Checkpoint files on server** (track completed IDs only, NOT the translated text):

| File | IDs Tracked | Translations Done | Date |
|------|-------------|-------------------|------|
| `translate_fjms_catalog_checkpoint.json` | 4,158 | (catalog fields) | 2026-03-05 |
| `translate_fjms_freedesc_checkpoint.json` | 145,074 | 254,835 | 2026-03-07 |
| `translate_fjms_fulltext_checkpoint.json` | 77,772 | 31,554 | 2026-03-10 |
| `translate_fjms_runningtitle_checkpoint.json` | 111,313 | 3,648 | 2026-03-09 |
| `translate_fjms_textualframe_checkpoint.json` | 47,200 | 47,200 | 2026-03-11 |

These checkpoints are **NOT useful for recovery** — they only store which IDs were processed (to skip on resume), not the actual translated text. The translated text was written directly to the now-corrupted DB.

### Local machine (`C:\GenizahSearch`)

- **Git branch**: `46-translation-wiring-round2` (has all bug fixes, committed `565446ae`)
- **fjms_enrichment.db**: CLEAN, rebuilt with MAX(Version) fix
  - 730,624 catalog rows (was 685K before fix)
  - 447,514 existing translations in `fjms_translations` table

**Local translations present (from Round 1 + Round 2 pre-corruption):**

| field_name | direction | count | notes |
|------------|-----------|-------|-------|
| FreeDesc | he2en | 254,011 | Round 1 complete |
| RunningTitle | en2he | 111,501 | Round 2 complete |
| FullText | en2he | 76,857 | Round 2 complete |
| Title | he2en | 2,408 | Round 1 catalog fields |
| PersonEngDesc | he2en | 1,163 | Round 1 catalog fields |
| PersonHebDesc | en2he | 702 | Round 1 catalog fields |
| GenizahTitleEngTitle | he2en | 626 | Round 1 catalog fields |
| AuthorText | he2en | 228 | Round 1 catalog fields |
| TitleHeb | en2he | 18 | Round 1 catalog fields |

**Translation gaps (need backfill):**

These counts were obtained by running each script's actual candidate-fetching and already-translated functions against the local DB on 2026-03-11, using each script's actual default `min_length`. They reflect the exact row-level units each script processes.

| Category | Script | min_length | Total Candidates | Already Done (DB) | Pending | ID Unit | Est. Time |
|----------|--------|-----------|-----------------|-------------------|---------|---------|-----------|
| TextualFrame HE→EN | `translate_fjms_catalog_text.py --mode textualframe` | 10 | 84,425 | 0 | **84,425** | rowid | ~6h |
| RunningTitle EN→HE | `translate_fjms_catalog_text.py --mode runningtitle` | 10 | 134,113 | 111,493 | **22,617** | AlmaId:UnitCatalogRecId | ~2h |
| Catalog fields | `translate_fjms_catalog.py --category all` | n/a | 3,966 | checkpoint-only (see warning) | **up to 3,966** | AlmaId | ~15min |
| FullText EN→HE | `translate_fjms_catalog_text.py --mode fulltext` | 10 | 77,470 | 76,857 | **915** | rowid | ~5min |
| FreeDesc HE→EN | `translate_fjms_free_desc.py` | **20** | 254,835 | 144,511 | **791** | SignatureId | ~3min |
| **TOTAL** | | | | | **~112,714** | | **~9h** |

**FreeDesc duplicate rows explained:** The `fjms_translations` table contains 254,011 FreeDesc rows total, but only 144,511 unique `signature_id` values. The remaining 109,500 rows are duplicates (11,484 signature_ids with 2+ entries from earlier runs that inserted without checking). The `get_already_translated_freedesc()` function correctly deduplicates via `SELECT signature_id ... WHERE field_name = 'FreeDesc'` returning a set, so the DB-based dedup works correctly and the checkpoint is not needed for FreeDesc.

**How each script determines "already done":**

| Script | Dedup Method | Checkpoint Role |
|--------|-------------|-----------------|
| `translate_fjms_catalog_text.py` | Queries `fjms_translations` table (`get_already_translated_rt/ft/tf`) **then** unions with checkpoint IDs | Checkpoint is supplementary; DB-based dedup is primary |
| `translate_fjms_free_desc.py` | Queries `fjms_translations` table (`get_already_translated_freedesc`) **then** unions with checkpoint IDs | Same as above |
| `translate_fjms_catalog.py` | Does **NOT** query `fjms_translations`; selects from source-table gaps (e.g., `WHERE Title IS NULL`) and skips **only** checkpoint IDs | Checkpoint is the **sole** dedup mechanism |

> **WARNING (Finding 1):** `translate_fjms_catalog.py` finds candidates by checking if the *source table column* is empty (e.g., `TitleHeb IS NULL`), NOT by checking if a translation already exists in `fjms_translations`. It skips items *only* if they appear in the checkpoint file. If you delete the checkpoint, it will retranslate all ~3,966 source-table gaps, creating duplicate rows in `fjms_translations` for the ~5,145 items already translated in Round 1. See Step 3 for the correct handling.

## Execution Plan

### Step 1: Merge bug fixes into master-main and deploy

The server needs the updated scripts (textualframe mode, direction tuple fix) AND the bug-fix code for the web app. Two options:

**Option A (recommended): Merge branch into master-main, then deploy**
```bash
# On local machine
cd C:\GenizahSearch
git checkout master-main
git merge 46-translation-wiring-round2
git push origin master-main

# On server
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com
cd /home/ubuntu/GenizahSearch
git pull origin master-main
```

**Option B: Temporary branch checkout for backfill only**
```bash
# On local machine
git push origin 46-translation-wiring-round2

# On server
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com
cd /home/ubuntu/GenizahSearch
git fetch origin
git checkout 46-translation-wiring-round2
```
> **Note (Finding 5):** If you use Option B, switching back to `master-main` after backfill will revert the Bug 2 (translation direction) fix on the production web app. You would need to merge the branch into `master-main` separately before restoring production.

### Step 2: Upload clean local DB to server

Replace the corrupted DB with the clean local one (1.1GB, ~5-10 min upload):

```bash
# On local machine (Git Bash / WSL)
scp fist_data/fjms_enrichment.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/fist_data/fjms_enrichment.db
```

### Step 3: Handle checkpoint files

> **CRITICAL (Finding 1):** The three scripts handle dedup differently. You cannot blindly delete all checkpoints.

**Safe to delete** — these scripts check `fjms_translations` table for already-done items:
```bash
# On server
cd /home/ubuntu/GenizahSearch
rm -f translate_fjms_fulltext_checkpoint.json
rm -f translate_fjms_runningtitle_checkpoint.json
rm -f translate_fjms_textualframe_checkpoint.json
rm -f translate_fjms_freedesc_checkpoint.json
```

**DO NOT delete** — this script relies solely on the checkpoint for dedup:
```
translate_fjms_catalog_checkpoint.json   ← KEEP THIS FILE
```

If `translate_fjms_catalog_checkpoint.json` does not exist on the server (or was already deleted), you must reconstruct it from the local DB before running `translate_fjms_catalog.py`:

```bash
python3 -c "
import sqlite3, json
from datetime import datetime, timezone

conn = sqlite3.connect('fist_data/fjms_enrichment.db')
checkpoint = {}

# Reconstruct from fjms_translations: each field_name maps to a category
field_to_cat = {
    'Title': 'titles_he2en',
    'TitleHeb': 'titles_en2he',
    'AuthorText': 'authors',
    'GenizahTitleEngTitle': 'genizah_titles',
    'PersonEngDesc': 'persons_he2en',
    'PersonHebDesc': 'persons_en2he',
}

for field, cat in field_to_cat.items():
    rows = conn.execute(
        'SELECT DISTINCT alma_id FROM fjms_translations WHERE field_name = ?',
        (field,)
    ).fetchall()
    if rows:
        checkpoint[cat] = [r[0] for r in rows]

with open('translate_fjms_catalog_checkpoint.json', 'w') as f:
    json.dump({
        'completed': {k: v for k, v in checkpoint.items()},
        'counts': {k: len(v) for k, v in checkpoint.items()},
        'saved_at': datetime.now(timezone.utc).isoformat(),
    }, f, indent=2)

total = sum(len(v) for v in checkpoint.values())
print(f'Reconstructed checkpoint: {total} IDs across {len(checkpoint)} categories')
conn.close()
"
```

### Step 4: Verify DB integrity on server

```bash
cd /home/ubuntu/GenizahSearch
python3 -c "
import sqlite3
conn = sqlite3.connect('fist_data/fjms_enrichment.db')
cur = conn.cursor()
print(cur.execute('PRAGMA integrity_check').fetchone())
cur.execute('SELECT COUNT(*) FROM catalog')
print(f'catalog: {cur.fetchone()[0]:,}')
cur.execute('SELECT COUNT(*) FROM fjms_translations')
print(f'fjms_translations: {cur.fetchone()[0]:,}')
cur.execute('SELECT field_name, direction, COUNT(*) FROM fjms_translations GROUP BY field_name, direction ORDER BY field_name')
for r in cur.fetchall():
    print(f'  {r[0]:25s} {r[1]:8s} {r[2]:>8,}')
conn.close()
"
```

Expected: `('ok',)`, 730,624 catalog rows, 447,514 translations.

### Step 5: Verify pending counts match expectations

Run the actual candidate functions to confirm gap sizes before committing to hours of API calls. This is more reliable than `--dry-run` which only shows raw candidate counts without subtracting already-done items (Finding 4).

```bash
cd /home/ubuntu/GenizahSearch
python3 -c "
import sqlite3, sys
sys.path.insert(0, '.')
conn = sqlite3.connect('fist_data/fjms_enrichment.db')

# Catalog fields (source-table gaps, deduped by checkpoint)
from scripts.translate_fjms_catalog import (
    get_title_gaps_he2en, get_title_gaps_en2he, get_author_gaps,
    get_genizah_title_gaps, get_person_gaps_he2en, get_person_gaps_en2he,
    load_checkpoint,
)
cats = [
    ('titles_he2en', get_title_gaps_he2en),
    ('titles_en2he', get_title_gaps_en2he),
    ('authors', get_author_gaps),
    ('genizah_titles', get_genizah_title_gaps),
    ('persons_he2en', get_person_gaps_he2en),
    ('persons_en2he', get_person_gaps_en2he),
]
import os
ckpt_path = os.path.join(os.getcwd(), 'translate_fjms_catalog_checkpoint.json')
ckpt = load_checkpoint(ckpt_path)
ckpt_total = sum(len(v) for v in ckpt.values())
print(f'=== translate_fjms_catalog.py (checkpoint: {ckpt_total} IDs from {ckpt_path}) ===')
total_gaps = 0
total_pending = 0
for name, getter in cats:
    gaps = getter(conn)
    completed = ckpt.get(name, set())
    pending = [g for g in gaps if g[0] not in completed]
    total_gaps += len(gaps)
    total_pending += len(pending)
    print(f'  {name:20s} {len(gaps):>6,} gaps, {len(completed):>6,} in checkpoint, {len(pending):>6,} pending')
print(f'  {"TOTAL":20s} {total_gaps:>6,} gaps, {ckpt_total:>6,} in checkpoint, {total_pending:>6,} pending')
if ckpt_total == 0:
    print('  WARNING: Checkpoint is empty! All source-table gaps will be retranslated.')

# Catalog text (DB-based dedup)
from scripts.translate_fjms_catalog_text import (
    get_runningtitle_candidates, get_already_translated_rt,
    get_fulltext_candidates, get_already_translated_ft,
    get_textualframe_candidates, get_already_translated_tf,
)
rt_c = get_runningtitle_candidates(conn, 10)
rt_d = get_already_translated_rt(conn)
rt_p = [c for c in rt_c if f'{c[1]}:{c[0]}' not in rt_d]
ft_c = get_fulltext_candidates(conn, 10)
ft_d = get_already_translated_ft(conn)
ft_p = [c for c in ft_c if c[0] not in ft_d]
tf_c = get_textualframe_candidates(conn, 10)
tf_d = get_already_translated_tf(conn)
tf_p = [c for c in tf_c if c[0] not in tf_d]
print()
print('=== translate_fjms_catalog_text.py (DB-based dedup) ===')
print(f'  RunningTitle: {len(rt_c):>8,} cand, {len(rt_d):>8,} done, {len(rt_p):>8,} pending')
print(f'  FullText:     {len(ft_c):>8,} cand, {len(ft_d):>8,} done, {len(ft_p):>8,} pending')
print(f'  TextualFrame: {len(tf_c):>8,} cand, {len(tf_d):>8,} done, {len(tf_p):>8,} pending')

# Free desc (DB-based dedup, default min_length=20)
from scripts.translate_fjms_free_desc import get_freedesc_candidates, get_already_translated_freedesc
fd_c = get_freedesc_candidates(conn, 20)  # script default is --min-length 20
fd_d = get_already_translated_freedesc(conn)
fd_p = [c for c in fd_c if str(c[0]) not in fd_d]
print()
print('=== translate_fjms_free_desc.py (DB-based dedup, min_length=20) ===')
print(f'  FreeDesc: {len(fd_c):>8,} cand, {len(fd_d):>8,} done, {len(fd_p):>8,} pending')
conn.close()
"
```

**Expected pending counts** (from local DB, 2026-03-11):

| Category | Pending | Note |
|----------|---------|------|
| TextualFrame | 84,425 | New field, none translated yet |
| RunningTitle | 22,617 | DB-based dedup |
| Catalog fields (source gaps) | ~3,966 minus checkpoint | Checkpoint is sole dedup |
| FullText | 915 | DB-based dedup |
| FreeDesc | 791 | DB-based dedup, min_length=20 |

If counts differ significantly from these, investigate before proceeding.

### Step 6: Run translation backfill

Run in a screen session. Execute sequentially (SQLite single-writer constraint).

```bash
screen -S fjms-backfill
cd /home/ubuntu/GenizahSearch

# 1. Catalog field gaps (source-table gaps, uses checkpoint for dedup)
#    ~3,966 candidates minus checkpoint; ~15 min
#    Workers: hardcoded internally (min(MAX_WORKERS, 5)), no --workers flag
python3 scripts/translate_fjms_catalog.py --category all

# 2. FullText EN→HE gaps (~915 pending, ~5 min)
python3 scripts/translate_fjms_catalog_text.py --mode fulltext --workers 5

# 3. FreeDesc HE→EN gaps (~791 pending, ~3 min)
python3 scripts/translate_fjms_free_desc.py --workers 5

# 4. RunningTitle EN→HE gaps (~22,617 pending, ~2h)
python3 scripts/translate_fjms_catalog_text.py --mode runningtitle --workers 5

# 5. TextualFrame HE→EN (NEW, ~84,425 pending, ~6h)
python3 scripts/translate_fjms_catalog_text.py --mode textualframe --workers 5
```

Detach screen: `Ctrl+A, D`

### Step 7: Monitor progress

```bash
# Reattach to screen
screen -r fjms-backfill

# Or check from outside (last few log lines)
screen -S fjms-backfill -X hardcopy /tmp/backfill_status.txt && tail -20 /tmp/backfill_status.txt
```

### Step 8: Download completed DB

After all jobs finish (~9h total):

```bash
# On local machine
scp ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/fist_data/fjms_enrichment.db fist_data/fjms_enrichment.db
```

### Step 9: Verify locally

```bash
cd C:\GenizahSearch
python -c "
import sqlite3
conn = sqlite3.connect('fist_data/fjms_enrichment.db')
cur = conn.cursor()
cur.execute('SELECT field_name, direction, COUNT(*) FROM fjms_translations GROUP BY field_name, direction ORDER BY field_name')
total = 0
for r in cur.fetchall():
    print(f'  {r[0]:25s} {r[1]:8s} {r[2]:>8,}')
    total += r[2]
print(f'  TOTAL: {total:>8,}')
conn.close()
"
```

Expected total: ~560K+ translations (447K existing + ~113K new).

### Step 10: Restart production web service

If you used Option A in Step 1 (merged to master-main), production already has the bug fixes:
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com
sudo systemctl restart genizah-web
```

If you used Option B (temporary branch checkout), you must merge first:
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com
cd /home/ubuntu/GenizahSearch
git checkout master-main
git merge 46-translation-wiring-round2
sudo systemctl restart genizah-web
```

> **Why this matters (Finding 5):** The Bug 2 fix (translation direction tuples) lives on `46-translation-wiring-round2`. If you switch back to `master-main` without merging, production will show incorrect translation toggle defaults (e.g., showing English text to Hebrew UI users by default).

## Script Reference

### `scripts/translate_fjms_catalog.py`

- **Purpose**: Catalog field gap-fill (Title, Author, GenizahTitle, Person — 6 categories)
- **Key args**: `--category {all|titles|titles_he2en|titles_en2he|authors|genizah_titles|persons_he2en|persons_en2he}`, `--dry-run`, `--limit N`, `--batch-size N`
- **No `--workers` flag**: Worker count is hardcoded as `min(MAX_WORKERS, 5)` (line 397)
- **Source tables**: `catalog` (Title/TitleHeb/AuthorText columns), `genizah_titles`, `genizah_persons`
- **Dedup**: Source-table gaps only (e.g., `WHERE TitleHeb IS NULL`) + checkpoint file. Does **NOT** query `fjms_translations` to skip reruns.
- **Checkpoint**: Category-based dict `{cat_name: [alma_id, ...]}` in `translate_fjms_catalog_checkpoint.json`
- **Direction**: Mixed HE↔EN depending on category
- **No periodic progress logging or SQLite reconnect loop** (unlike the other two scripts)

### `scripts/translate_fjms_catalog_text.py`

- **Purpose**: RunningTitle, FullText, TextualFrame batch translation
- **Key args**: `--mode {runningtitle|fulltext|textualframe|both}`, `--workers N`, `--dry-run`, `--limit N`, `--batch-size N`, `--min-length N`, `--fjms-db PATH`
- **Source tables**: `catalog_running_titles`, `catalog_full_texts`, `catalog_textual_frames`
- **Dedup**: Queries `fjms_translations` table via `get_already_translated_rt/ft/tf()` + unions with checkpoint IDs
- **ID units**: RunningTitle uses `AlmaId:UnitCatalogRecId` composite key; FullText and TextualFrame use `rowid`
- **Checkpoint**: Flat `completed_ids` set in mode-specific JSON files
- **Direction**: EN→HE (RunningTitle, FullText), HE→EN (TextualFrame)
- **Features**: SIGINT handler, progress logging every 1,000 items, SQLite reconnect every 10,000 items, exponential backoff (1s/2s/4s, max 30s, 3 retries)

### `scripts/translate_fjms_free_desc.py`

- **Purpose**: Free descriptions (HE→EN) and bibliography (deferred)
- **Key args**: `--mode {freedesc|bibliography}`, `--workers N`, `--dry-run`, `--limit N`, `--batch-size N`, `--min-length N`
- **Source table**: `catalog_free_desc`
- **Dedup**: Queries `fjms_translations` table via `get_already_translated_freedesc()` + unions with checkpoint IDs
- **ID unit**: `SignatureId`
- **Checkpoint**: Flat `completed_ids` set in `translate_fjms_freedesc_checkpoint.json`
- **Direction**: HE→EN
- **Features**: Same as catalog_text (SIGINT, progress logging, SQLite reconnect, backoff)
- **Note**: Bibliography mode (`--mode bibliography`) exists but is deferred (~542K items, requires `--force` flag)

## Server Connection

| Field | Value |
|-------|-------|
| Host | `ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| User | `ubuntu` |
| SSH | `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| App dir | `/home/ubuntu/GenizahSearch/` |
| DB path | `/home/ubuntu/GenizahSearch/fist_data/fjms_enrichment.db` |
| Service | `genizah-web` (systemd) |
| Web port | 8081 (proxied via Nginx 80/443) |

## Risk Notes

- **SQLite single-writer**: All scripts must run sequentially, not in parallel. Concurrent writes likely caused the original corruption.
- **Dicta API rate limits**: Scripts handle 429 with retry+backoff. 5 workers is the safe default.
- **Disk space**: The DB is ~1.1GB. Server has sufficient space (checked).
- **Web service**: The translation scripts write to the same DB the web app reads. The web app uses read-only connections, so no conflict. But if you need to restart the web service, do it after backfill completes.
- **Branch / production coherence**: Ensure the bug-fix code is on whatever branch the server runs for production. See Step 1 options and Step 10 for details.
- **Catalog checkpoint is critical**: Do not delete `translate_fjms_catalog_checkpoint.json` without reconstructing it first (Step 3). Unlike the other scripts, `translate_fjms_catalog.py` has no DB-based dedup.
- **FreeDesc duplicate rows**: The `fjms_translations` table has 254,011 FreeDesc rows but only 144,511 unique `signature_id`s (109,500 are duplicates from earlier runs). DB-based dedup handles this correctly — only 791 items are actually pending. Consider deduplicating the table post-backfill: `DELETE FROM fjms_translations WHERE rowid NOT IN (SELECT MIN(rowid) FROM fjms_translations WHERE field_name = 'FreeDesc' GROUP BY signature_id)`.
