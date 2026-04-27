# Release Handoff: v7.3.0 (or v7.2.5)

**Date:** 2026-03-26
**Status:** Ready for release
**Scope:** Web + Desktop

---

## What Changed Since v7.2.4

### Data Changes (in local DBs, NOT YET on server)

#### 1. FJMS Bibliography Dedup (`fist_data/fjms_enrichment.db`)
- **828,105 → 427,051 rows** in `bibliography` table (48.4% reduction)
- Pass 1: exact dedup (Comment-only diffs) removed 277,802
- Pass 2: near-dupe merge (catalog/batch twins, page subsets, article name normalization) removed 123,252
- Backup: `fist_data/fjms_enrichment_backup_2026-03-26.db`
- Export script fixed: `scripts/export_fist_enrichment.py` uses GROUP BY to prevent re-occurrence
- Dedup script: `scripts/dedup_bibliography.py`

#### 2. New FreeDesc EN→HE Translations (`fist_data/fjms_enrichment.db`)
- **55,174 new rows** inserted into `fjms_translations` (field_name='FreeDesc', direction='en2he')
- **6,366 overlap rows** updated (840 got signature_id populated from NULL)
- **105 hallucinations excluded** (single-word collapsed outputs)
- Post-merge: 78,546 total en2he FreeDesc (60,700 runtime-visible with signature_id)
- 17,846 legacy NULL-sig rows remain (not served by runtime lookup, cleanup optional)

#### 3. Measurement Data (Phase 54)
- `computed_measurements`: 434,369 rows (new table)
- `manuscript_measurements`: 231,490 rows (new table)
- New service methods: `FjmsService.get_measurements()`, `has_measurements()`
- New UI: Measurements dialog (web + desktop) with browse button

### Code Changes (already committed)

```
feat(54-01): FIST measurement import script and test infrastructure
feat(54-01): FjmsService.get_measurements() and has_measurements()
feat(54-02): web measurements dialog with browse button and translations
feat(54-02): desktop FjmsMeasurementsDialog with browse button wiring
fix(54):     various import safety patches, FGP key format, AlmaId precision
fix(54):     add Measurements button to Browse by Shelfmark tab
fix(desktop): browse tab crash fix — navigation debounce + generation guard
fix(desktop): proper QThread lifecycle for browse image loaders
fix(desktop): wait-or-terminate for ResultDialog image threads
feat:        persistent NLI FL-ID cache to survive service restarts
fix:         bump NLI concurrent fetches default from 4 to 8
fix:         revert broken profiles FK join, use batch profile lookup
```

---

## Release Steps

### Step 1: Upload corrected DB to server

The local `fist_data/fjms_enrichment.db` (1.5GB) has all changes (bib dedup + translations + measurements). The server's copy is stale (still has 828K bib dupes, missing new translations, missing measurements).

```bash
# Compress locally (saves ~70% transfer)
gzip -k fist_data/fjms_enrichment.db

# Upload to server (will take a few minutes)
scp fist_data/fjms_enrichment.db.gz ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/tmp/

# On server: backup old, replace, restart
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com << 'EOF'
cd /home/ubuntu/GenizahSearch
cp fist_data/fjms_enrichment.db fist_data/fjms_enrichment_backup_pre_v73.db
gunzip -c /tmp/fjms_enrichment.db.gz > fist_data/fjms_enrichment.db
sudo systemctl restart genizah-web
EOF
```

**IMPORTANT:** Do NOT copy the server's DB back to local — the server copy lacks the bibliography dedup and QA fixes.

### Step 2: Version bump

```bash
python scripts/bump_version.py 7.3.0   # or 7.2.5 — user to decide
```

Then manually update:
- `CHANGELOG.md` — add `## [X.Y.Z]` section
- `README.md` "What's New" section

### Step 3: Use `/release web+desktop`

This will handle: code review, build, deploy, GitHub release.

Key release notes for What's New:
- **Manuscript Measurements**: physical dimensions (height, width, material, writing surface) now visible via Measurements button in browse (web + desktop)
- **Bibliography Cleanup**: removed ~400K duplicate bibliography entries (48% reduction) for cleaner scholarly references
- **55K New Hebrew Translations**: English free descriptions now available in Hebrew
- **Desktop Stability**: fixed browse tab crash on rapid navigation, image thread lifecycle fixes
- **Performance**: persistent NLI FL-ID cache, bumped concurrent fetches

### Step 4: Post-release verification

After deploy, verify on the live site:
1. Browse a manuscript → check bibliography count is sane (not doubled)
2. Browse a manuscript with measurements → verify Measurements button works
3. Toggle Hebrew → check FreeDesc translations appear
4. Navigate rapidly in browse tab → confirm no crash

---

## DB Files Summary

| DB | Path | Size | Changed? |
|---|---|---|---|
| fjms_enrichment.db | `fist_data/` | 1.5GB | **YES — must upload** |
| pgp.db | `pgp_data/` | 165MB | No |
| libraries_translations.db | root | 84MB | No |
| nli_crossref.db | `nli_data/` | 261MB | No |

Only `fjms_enrichment.db` needs to be uploaded to the server.

---

## Risks & Cautions

1. **DB upload replaces server copy entirely** — the server's fjms_enrichment.db will lose its own translation data, but that's OK because the local copy is the authoritative version (has QA fixes + dedup + all translations merged)
2. **1.5GB upload** — may take a few minutes over SCP. Compress first.
3. **Service restart required** after DB replacement (SQLite file handles)
4. **Backup the server DB first** before replacing (command included above)
5. **17,846 legacy NULL-sig FreeDesc en2he rows** exist but are harmless — they're not served by the runtime lookup path. Can clean up in a future session.
