# Corpus Mapper - מיפוי קורפוסים לגניזה

Maps external text corpora (Judeo-Arabic, Maagarim) to Cairo Genizah manuscripts (~600,000 fragments), discovering textual parallels and connections using the Shmidman-Koppel-Porat fingerprinting algorithm.

## Overview / סקירה כללית

This module searches external corpora against the Genizah manuscript database to find textual parallels. It includes:

- **Pre-filtering**: Skips common biblical/talmudic texts to focus on unique parallels
- **Chunked search**: Divides texts into 5-word overlapping chunks for thorough matching
- **Variant matching**: Handles spelling variations common in medieval Hebrew texts
- **MS-count filtering**: Distinguishes unique parallels from common quotations

## Quick Start / התחלה מהירה

```bash
# Step 1: Build canonical filter (Bible/Mishnah/Talmud fingerprints)
python -c "from corpus_mapper.canonical_filter import build_canonical_fingerprints; build_canonical_fingerprints()"

# Step 2: Discover symbols in your corpus
python -m corpus_mapper discover --corpus ja --limit 10

# Step 3: Test on a few files
python -m corpus_mapper test --corpus ja --limit 1

# Step 4: Export unique parallels (filtering common texts)
python -m corpus_mapper unique --max-ms 5 --min-score 10000

# Step 5: Run full mapping (can run overnight!)
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

**Patterns detected:**
- `{...}` - Curly braces (Hebrew words in JA, editorial additions)
- `[...]` - Square brackets (reconstructed text, corrections)
- `(...)` - Parentheses (abbreviations, notes)
- `##...##` - Headers with metadata (Maagarim)
- `>>` - Content line markers (Maagarim)
- `$...$` - Section markers
- Nikud, taamim, geresh marks

### `configure` - הגדרת כללי ניקוי
Interactive wizard that asks you what to do with each type of symbol.

```bash
python -m corpus_mapper configure
```

For each symbol type, you'll be asked:
- **Keep as-is** - לשמור כמו שהוא
- **Remove entirely** - להסיר לחלוטין
- **Remove markers, keep content** - להסיר סימנים, לשמור תוכן
- **Extract as metadata** - לחלץ כמטא-דאטא

Output: `corpus_mapper/cleaning_rules.json`

### `test` - בדיקה מהירה
Quick test run on a few files (single-threaded for faster startup).

```bash
python -m corpus_mapper test --corpus ja --limit 1
python -m corpus_mapper test --corpus maagarim --limit 3
```

### `run` - הרצה מלאה
Full corpus mapping with multiprocessing support.

```bash
# Run both corpora
python -m corpus_mapper run --corpus all

# Run only Judeo-Arabic
python -m corpus_mapper run --corpus ja

# Run with custom settings
python -m corpus_mapper run --corpus ja --min-score 500 --limit 100

# Start fresh (ignore checkpoint)
python -m corpus_mapper run --corpus ja --no-resume
```

Features:
- **Checkpointing**: Saves progress every 50 files, can resume if interrupted
- **Multiprocessing**: Uses 4 parallel workers for faster processing
- **Ctrl+C safe**: Saves current state before exiting

### `stats` - סטטיסטיקות
Show current statistics from the results database.

```bash
python -m corpus_mapper stats
```

### `unique` - מקבילות ייחודיות
Export unique parallels, filtering out common biblical/talmudic texts.

```bash
# Default: max 10 MS matches, min score 5000
python -m corpus_mapper unique

# Stricter filtering
python -m corpus_mapper unique --max-ms 5 --min-score 10000
```

This is the **key command** for finding interesting results. It filters:
- Chunks matching many manuscripts (100+ = likely biblical verse)
- Keeps chunks matching few manuscripts (1-5 = unique parallel)

### `export` - ייצוא תוצאות
Export all results to JSON or CSV.

```bash
python -m corpus_mapper export --format json
python -m corpus_mapper export --format csv --limit 10000
```

## Filtering Strategy / אסטרטגיית סינון

The module uses two complementary filtering strategies:

### 1. Pre-screening (Canonical Filter)
Before searching, each chunk is checked against a database of canonical texts:
- **Bible (מקרא)** - Full Hebrew Bible
- **Mishnah (משנה)** - Complete Mishnah
- **Talmud Bavli (תלמוד בבלי)** - All tractates

**2.8 million fingerprints** from 94 canonical text files.

If a chunk matches canonical texts, it's skipped (no expensive search needed).

### 2. Post-filtering (MS Count)
After searching, results are filtered by how many manuscripts they match:
- **1-5 MSs**: Unique parallels (interesting!)
- **6-20 MSs**: Moderately common
- **100+ MSs**: Very common (biblical verses, etc.)

**Example results from test run:**
| Category | Chunks | Notes |
|----------|--------|-------|
| 1 MS (unique) | 554 | Most interesting |
| 2-5 MSs | 1,838 | Likely unique |
| 100+ MSs | 3,255 | Common texts (filter out) |

## Supported Corpora / קורפוסים נתמכים

### Judeo-Arabic (Friedberg) - ערבית יהודית
- **Path**: `big_data_files/JA/`
- **Format**: JSON files with structure:
  ```json
  {
    "AuthorName": "רמב\"ע",
    "TitleName": "העיונים והדיונים",
    "Content": [{"PageNumber": 1, "rows": [...]}]
  }
  ```
- **Files**: ~91 JSON files
- **Contains**: Medieval Judeo-Arabic philosophical and religious works
- **Markup**: `{Hebrew words}` embedded in Arabic text

### Maagarim (Academy) - מאגרים
- **Path**: `big_data_files/Maagarim/`
- **Format**: TXT files with filename metadata:
  ```
  author--composition--date--genre--id-OnlyText.txt
  ```
- **Files**: ~8,233 files (~206MB)
- **Contains**: Historical Hebrew texts from the Academy corpus
- **Markup**:
  - `##header|source##` - Section headers with manuscript source
  - `>> content` - Text content lines
  - `$section$` - Section markers

## Output / פלט

### Database Schema
Results stored in: `corpus_mapper_output/corpus_connections.sqlite`

```sql
CREATE TABLE corpus_matches (
    id INTEGER PRIMARY KEY,
    source_corpus TEXT,      -- 'ja' or 'maagarim'
    source_file TEXT,        -- Original filename
    source_author TEXT,      -- Author name
    source_title TEXT,       -- Work title
    source_ref TEXT,         -- Chunk reference (e.g., "chunk 5075")
    source_text TEXT,        -- Matched text (up to 500 chars)
    ms_id TEXT,              -- Genizah manuscript system ID
    ms_shelfmark TEXT,       -- Manuscript shelfmark
    ms_snippet TEXT,         -- Matching snippet from manuscript
    ms_title TEXT,           -- Title from libraries.csv
    score REAL,              -- Match score
    title_match_score REAL,  -- Title similarity (0-1)
    match_type TEXT,         -- 'parallel'
    created_at TIMESTAMP
);

CREATE TABLE checkpoints (
    corpus_id TEXT PRIMARY KEY,
    last_file TEXT,
    files_processed INTEGER,
    total_matches INTEGER,
    config_json TEXT,
    updated_at TIMESTAMP
);
```

### Output Files
```
corpus_mapper_output/
├── corpus_connections.sqlite    # Main results database
├── canonical_fingerprints.pkl   # Cached canonical text fingerprints
├── symbol_report.json           # Symbol discovery report
├── unique_parallels.txt         # Exported unique parallels
├── top_matches.txt              # Top scoring matches
└── logs/
    └── runner_YYYYMMDD_HHMMSS.log
```

## Architecture / ארכיטקטורה

```
corpus_mapper/
├── __init__.py              # Package init
├── __main__.py              # Entry point (python -m corpus_mapper)
├── main.py                  # CLI commands and argument parsing
├── config.py                # Paths, defaults, corpus definitions
├── symbol_discovery.py      # Discover markup patterns in corpora
├── interactive_config.py    # Interactive cleaning rules wizard
├── text_cleaner.py          # Apply cleaning rules to normalize text
├── canonical_filter.py      # Pre-screen against Bible/Mishnah/Talmud
├── runner.py                # Main batch processor with:
│   ├── LibrariesDB          # Interface to libraries.csv
│   ├── ResultsDatabase      # SQLite storage with checkpointing
│   ├── CorpusRunner         # Multiprocess batch runner
│   └── run_test()           # Single-threaded test function
└── parsers/
    ├── __init__.py
    ├── ja_parser.py         # Judeo-Arabic JSON parser
    └── maagarim_parser.py   # Maagarim TXT parser
```

## Configuration / הגדרות

Default search settings in `runner.py`:

```python
SEARCH_CONFIG = {
    'chunk_size': 5,           # Words per chunk (smaller = more matches)
    'chunk_overlap': 2,        # Overlap between chunks
    'min_score': 500,          # Minimum match score
    'mode': 'variants',        # Variant matching mode
    'num_workers': 4,          # Parallel processes (for full run)
    'batch_size': 50,          # Files per checkpoint
    'max_ms_matches': 20,      # Filter threshold for unique parallels
}
```

## Performance Notes / הערות ביצועים

### Lab Engine Loading
The Genizah Lab Index is ~5.8GB and takes 2-5 minutes to load. This is a one-time cost per session.

### Search Speed
- **With canonical filter**: Skips ~50% of chunks (biblical content)
- **Per chunk search**: ~0.1-0.5 seconds
- **Estimated total time**:
  - JA corpus (91 files): 2-4 hours
  - Maagarim corpus (8,233 files): 8-16 hours

### Memory Usage
- Lab Index: ~1-2GB RAM
- Canonical fingerprints: ~200MB RAM
- Recommended: 8GB+ RAM

## Troubleshooting / פתרון בעיות

### "Lab Index not found"
Ensure the Genizah indexes are built:
```bash
python build_index.py
```

### "Canonical filter not loaded"
Build the fingerprints cache:
```bash
python -c "from corpus_mapper.canonical_filter import build_canonical_fingerprints; build_canonical_fingerprints()"
```

### Resuming after crash
The runner saves checkpoints automatically. Just run the same command:
```bash
python -m corpus_mapper run --corpus ja
```

### Too many results
Use the `unique` command with stricter filters:
```bash
python -m corpus_mapper unique --max-ms 3 --min-score 50000
```

## Example Results / דוגמאות לתוצאות

From test run on 1 JA file ("העיונים והדיונים" by רמב"ע):

**Raw results**: 1,211,828 matches
**After filtering (max 5 MSs)**: 2,392 unique parallels

**Sample unique parallel:**
```
Score: 2,048,808 (1 MS match)
Source: העיונים והדיונים, chunk 5137
Text: אלמעבר עאקלא צאלחא צאדקא באלטבע
MS: 990001535220205171_IE37932181_P000051
```

This Judeo-Arabic philosophical fragment matches exactly one Genizah manuscript - a truly unique parallel worth investigating.

## Development / פיתוח

### Adding a new corpus

1. Create parser in `parsers/new_parser.py`:
   ```python
   class NewParser:
       def __init__(self, corpus_path):
           self.path = corpus_path

       def iter_documents(self, limit=None):
           # Yield document objects
           pass
   ```

2. Add corpus config to `config.py`:
   ```python
   CORPORA = {
       'new_corpus': {
           'name': 'New Corpus',
           'path': os.path.join(BASE_DIR, 'path/to/files'),
           'pattern': '*.txt',
           'format': 'txt',
       }
   }
   ```

3. Update `runner.py` to use the new parser.

### Running tests
```bash
python -m corpus_mapper test --corpus ja --limit 1
```

## License

Part of the GenizahSearch project.
