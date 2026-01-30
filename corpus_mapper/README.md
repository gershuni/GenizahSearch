# Corpus Mapper - מיפוי קורפוסים לגניזה

Maps external text corpora (Judeo-Arabic, Maagarim) to Genizah manuscripts, discovering textual parallels and connections.

## Quick Start / התחלה מהירה

```bash
# Step 1: Discover symbols in your corpus (scan and report)
python -m corpus_mapper discover --corpus ja --limit 10

# Step 2: Configure cleaning rules interactively
python -m corpus_mapper configure

# Step 3: Test on a few files
python -m corpus_mapper test --corpus ja --limit 3

# Step 4: Run full mapping (can run overnight!)
python -m corpus_mapper run --corpus all
```

## Commands / פקודות

### `discover` - גילוי סימנים מיוחדים
Scans corpus files and creates a report of all special symbols and patterns found.

```bash
python -m corpus_mapper discover --corpus ja --limit 10
python -m corpus_mapper discover --corpus maagarim --limit 50
python -m corpus_mapper discover --corpus all
```

Output: `corpus_mapper_output/symbol_report.json`

### `configure` - הגדרת כללי ניקוי
Interactive wizard that asks you what to do with each type of symbol.

```bash
python -m corpus_mapper configure
```

For each symbol type (e.g., `{...}`, `[...]`, `##...##`), you'll be asked:
- **Keep as-is** - לשמור כמו שהוא
- **Remove entirely** - להסיר לחלוטין
- **Remove markers, keep content** - להסיר סימנים, לשמור תוכן
- **Extract as metadata** - לחלץ כמטא-דאטא
- **Prefer bracketed content** - להעדיף תוכן בסוגריים (לתיקונים)

Output: `corpus_mapper/cleaning_rules.json`

### `test` - בדיקה מהירה
Quick test run on a few files to verify everything works.

```bash
python -m corpus_mapper test --corpus ja --limit 3
```

### `run` - הרצה מלאה
Full corpus mapping - can run overnight!

```bash
# Run both corpora
python -m corpus_mapper run --corpus all

# Run only Judeo-Arabic
python -m corpus_mapper run --corpus ja

# Run with custom settings
python -m corpus_mapper run --corpus maagarim --min-score 400 --limit 1000

# Start fresh (ignore checkpoint)
python -m corpus_mapper run --corpus ja --no-resume
```

Features:
- **Checkpointing**: Saves progress every 100 files, can resume if interrupted
- **Background-friendly**: Low CPU usage, safe to run overnight
- **Ctrl+C safe**: Saves current state before exiting

### `stats` - סטטיסטיקות
Show current statistics from the results database.

```bash
python -m corpus_mapper stats
```

### `export` - ייצוא תוצאות
Export results to JSON or CSV.

```bash
python -m corpus_mapper export --format json
python -m corpus_mapper export --format csv --limit 10000
```

## Supported Corpora / קורפוסים נתמכים

### Judeo-Arabic (Friedberg) - ערבית יהודית
- Path: `C:\GenizahSearch\big_data_files\JA`
- Format: JSON files (8.JSON - 141.JSON)
- Size: ~57MB
- Contains: Works by Saadia Gaon, Maimonides, etc.

### Maagarim (Academy) - מאגרים
- Path: `C:\GenizahSearch\big_data_files\Maagarim`
- Format: TXT files with special markup
- Size: ~206MB, 8,233 files
- Contains: Historical Hebrew texts from Academy corpus

## Output / פלט

Results are stored in SQLite database:
`corpus_mapper_output/corpus_connections.sqlite`

Schema:
```sql
corpus_matches (
    source_corpus,   -- 'ja' or 'maagarim'
    source_file,     -- filename
    source_author,   -- author name
    source_title,    -- work title
    source_ref,      -- page/line reference
    source_text,     -- matched text
    ms_id,           -- Genizah manuscript ID
    ms_shelfmark,    -- manuscript shelfmark
    ms_snippet,      -- matching snippet
    score,           -- match score
    match_type       -- 'parallel'
)
```

## Time Estimates / הערכות זמן

| Corpus | Files | Estimated Time |
|--------|-------|----------------|
| Judeo-Arabic | ~134 | 2-4 hours |
| Maagarim | 8,233 | 8-16 hours |
| **Total** | ~8,370 | **10-20 hours** |

## Architecture / ארכיטקטורה

```
corpus_mapper/
├── __init__.py
├── __main__.py         # Entry point
├── main.py             # CLI commands
├── config.py           # Paths and settings
├── symbol_discovery.py # Discover special patterns
├── interactive_config.py # Configure cleaning rules
├── text_cleaner.py     # Apply cleaning rules
├── runner.py           # Batch processing
└── parsers/
    ├── ja_parser.py    # Judeo-Arabic parser
    └── maagarim_parser.py # Maagarim parser
```

## Troubleshooting / פתרון בעיות

### "Transcriptions file not found"
Ensure `Transcriptions.txt` is in the project root.

### "Search engine initialization failed"
The Genizah indexes need to be built first. Run:
```bash
python build_index.py
```

### Resuming after crash
The runner automatically saves checkpoints. Just run the same command again:
```bash
python -m corpus_mapper run --corpus ja
```

### Changing cleaning rules mid-run
1. Stop the current run (Ctrl+C)
2. Run `python -m corpus_mapper configure` to update rules
3. Resume with `python -m corpus_mapper run --corpus ja --no-resume`

## Development / פיתוח

To add a new corpus:

1. Create a new parser in `parsers/`
2. Add corpus config to `config.py`
3. Update `runner.py` to use the new parser
