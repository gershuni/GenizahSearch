# Phase 46: Dicta Translation - Research

**Researched:** 2026-03-04
**Domain:** Machine translation via Dicta LM 2.0 API, batch processing, bilingual metadata
**Confidence:** HIGH

## Summary

Phase 46 adds bilingual translations to all scholarly metadata in GenizahSearch. The Dicta Translation API (DictaLM 2.0, a Mistral-7B derivative trained on 190B+ Hebrew/English tokens) provides high-quality bidirectional translation via an OpenAI-compatible Completions endpoint. The API requires no authentication, handles concurrent requests well (tested at 10 concurrent with no errors), and produces scholarly-grade translations of Genizah manuscript metadata.

The translation volume is substantial: ~35K PGP descriptions (EN->HE), ~303K FJMS free descriptions (HE->EN), plus ~2,900 smaller catalog gap-fill items. At 10 concurrent workers, the PGP batch takes ~2.6 hours; the FJMS free description batch takes ~22 hours. The batch script must include checkpointing and resume capability to handle network interruptions across such long runs.

**Primary recommendation:** Use the REST Completions API (`/whatcanthisbe/completions`) with scholarly few-shot prompts. Store translations in new translation tables within existing sidecars (co-located, one-pass export). Build a `shared/translation_service.py` following the same pattern as `fjms_service.py` and `document_service.py`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
1. **Translation Scope:** Both EN->HE (PGP data) and HE->EN (FJMS data). Full bilingual coverage.
   - PGP: ALL non-empty descriptions (~35K), document_type taxonomy, tags already translated
   - FJMS: Fill gaps only in bilingual field pairs (Title/TitleHeb, TextualFrame, etc.), AuthorText, catalog_free_desc
   - Bibliography: Lower priority, general/summarized translation
2. **Pipeline:** Offline batch script (like existing export scripts). Defensive: checkpointing, resume, configurable throttle.
3. **Search Integration:** Metadata search only (SQLite queries). NO Tantivy index changes. "Translated match" badge.
4. **Display/UX:** Original by default, user opt-in toggle. Hover reveals original when translated. Both web and desktop.
5. **Few-shot testing:** Compare Dicta defaults vs custom scholarly few-shots.

### Claude's Discretion
- Storage architecture (new columns vs new tables vs separate sidecar)
- Exclude option UX (global setting vs per-search toggle)

### Deferred Ideas (OUT OF SCOPE)
- Text correction via few-shot (Dicta LM for OCR/transcription correction)
- On-demand translation fallback (no live API calls at runtime)
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| requests | 2.31+ | HTTP client for Dicta Completions API | Already in project, simple synchronous calls |
| sqlite3 | stdlib | Read/write translation tables in sidecars | Already used everywhere |
| concurrent.futures | stdlib | ThreadPoolExecutor for parallel API calls | Simple concurrency, tested at 10 concurrent |
| json | stdlib | Parse API responses, serialize few-shot templates | Standard |
| tqdm | any | Progress bars for batch processing | Already used in export_fist_enrichment.py |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| time | stdlib | Rate limiting, latency tracking | Always in batch scripts |
| pathlib | stdlib | File path handling | Already used in export scripts |
| logging | stdlib | Consistent with project logging pattern | Always |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| REST API | WebSocket API (translate.loadbalancer.dicta.org.il) | WebSocket is sentence-level streaming, harder to batch; REST is simpler and tested |
| requests | aiohttp + asyncio | Would enable higher concurrency but ThreadPoolExecutor at 10 workers already achieves 228 req/min |
| Separate translations.db | New columns in existing sidecars | Separate file allows independent update cycle but adds deployment complexity; new tables in existing sidecars is simpler |

## Architecture Patterns

### Recommended Project Structure
```
scripts/
  translate_pgp_descriptions.py      # Batch: PGP EN->HE (35K descriptions)
  translate_fjms_catalog.py           # Batch: FJMS HE->EN gap-fill (2,900 items)
  translate_fjms_free_desc.py         # Batch: FJMS free desc HE->EN (303K)
shared/
  translation_service.py              # Read-only service for both apps
data/
  few_shot_en2he_scholarly.json       # EN->HE few-shot template
  few_shot_he2en_scholarly.json       # HE->EN few-shot template
```

### Pattern 1: Dicta Translation API Client
**What:** Wrapper around the REST Completions API with few-shot prompt construction.
**When to use:** All translation calls.
**Example:**
```python
# Verified via live API testing (2026-03-04)
import requests
import json

DICTA_BASE = "https://dicta-translation.loadbalancer3.dicta.org.il"
DICTA_ENDPOINT = f"{DICTA_BASE}/whatcanthisbe/completions"
DICTA_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer x-no-api-key",
}

def translate_text(text: str, few_shot_prompt: str, direction: str = "en2he") -> str:
    """Translate text using Dicta LM 2.0 Completions API.

    Args:
        text: Text to translate.
        few_shot_prompt: Pre-built few-shot prefix (category:example pairs).
        direction: "en2he" or "he2en".

    Returns:
        Translated text string.
    """
    if direction == "en2he":
        src_cat, tgt_cat = "English", "Hebrew"
    else:
        src_cat, tgt_cat = "Hebrew", "English"

    prompt = f"{few_shot_prompt}\n\n{src_cat}: {text.strip()}\n{tgt_cat}:"

    payload = {
        "model": "dicta-il/dictalm2.0",
        "prompt": prompt,
        "temperature": 0,
        "stop": ["\n\n"],  # Double newline for multi-sentence text
        "max_tokens": 1024,
    }

    resp = requests.post(DICTA_ENDPOINT, json=payload, headers=DICTA_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["text"].strip()
```

### Pattern 2: Few-Shot Template Format
**What:** JSON file with bilingual example pairs and category labels.
**When to use:** Building prompts for translation API calls.
**Example:**
```python
# Few-shot template format (from TestLLMAPIsProgram.cs reference)
# File: data/few_shot_en2he_scholarly.json
{
    "prompts": [
        {
            "English": "Letter from a merchant requesting payment for goods shipped to Alexandria.",
            "Hebrew": "מכתב מסוחר המבקש תשלום עבור סחורה שנשלחה לאלכסנדריה."
        },
        {
            "English": "Legal document concerning a debt between two parties, dated 1150 CE.",
            "Hebrew": "מסמך משפטי בנושא חוב בין שני צדדים, מתוארך לשנת 1150 לספירה."
        },
        {
            "English": "Court record regarding the appointment of a guardian for orphans.",
            "Hebrew": "רישום בית דין בנושא מינוי אפוטרופוס ליתומים."
        }
    ],
    "en_category": "English",
    "he_category": "Hebrew"
}

# Prompt construction (from C# reference implementation):
def build_few_shot_prompt(template: dict, direction: str = "en2he") -> str:
    """Build the few-shot prefix from a template file."""
    pairs = []
    for p in template["prompts"]:
        if direction == "en2he":
            pairs.append(
                f"{template['en_category']}: {p['English'].strip()}\n"
                f"{template['he_category']}: {p['Hebrew'].strip()}"
            )
        else:
            pairs.append(
                f"{template['he_category']}: {p['Hebrew'].strip()}\n"
                f"{template['en_category']}: {p['English'].strip()}"
            )
    return "\n\n".join(pairs)
```

### Pattern 3: Batch Translation with Checkpointing
**What:** Process large volumes with resume capability.
**When to use:** All batch translation scripts.
**Example:**
```python
# Follows pattern from export_fist_enrichment.py and export_pgp_sidecar.py
import sqlite3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CHECKPOINT_FILE = "translate_checkpoint.json"
CONCURRENCY = 10
BATCH_SIZE = 100

def load_checkpoint() -> dict:
    if Path(CHECKPOINT_FILE).exists():
        return json.loads(Path(CHECKPOINT_FILE).read_text())
    return {"completed_ids": [], "last_id": None}

def save_checkpoint(state: dict):
    Path(CHECKPOINT_FILE).write_text(json.dumps(state))

def batch_translate(db_path, table, id_col, text_col, direction, few_shot):
    """Translate all rows in a table with checkpointing."""
    state = load_checkpoint()
    completed = set(state["completed_ids"])

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        f"SELECT {id_col}, {text_col} FROM {table} "
        f"WHERE {text_col} IS NOT NULL AND {text_col} != ''"
    ).fetchall()

    pending = [(rid, text) for rid, text in rows if rid not in completed]

    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {}
        for rid, text in pending:
            f = pool.submit(translate_text, text, few_shot, direction)
            futures[f] = rid

        for i, f in enumerate(as_completed(futures)):
            rid = futures[f]
            try:
                translation = f.result()
                results.append((rid, translation))
            except Exception as e:
                logger.error(f"Failed {rid}: {e}")

            # Checkpoint every BATCH_SIZE
            if len(results) % BATCH_SIZE == 0:
                flush_results(conn, results)
                results.clear()
                completed.update(futures[f2] for f2 in list(futures)[:i+1] if f2.done())
                save_checkpoint({"completed_ids": list(completed)})

    if results:
        flush_results(conn, results)

    conn.close()
```

### Pattern 4: Translation Service (Read-Only)
**What:** Shared service for accessing translations at runtime.
**When to use:** Both web and desktop apps querying translated metadata.
**Example:**
```python
# Follows pattern from shared/fjms_service.py and shared/document_service.py
class TranslationService:
    """Service for accessing pre-computed translations from sidecar databases."""

    def __init__(self, pgp_db_path=None, fjms_db_path=None, thread_safe=False):
        self._pgp_conn = None
        self._fjms_conn = None
        # Connect to sidecars...

    def get_pgp_description_he(self, pgpid: int) -> Optional[str]:
        """Get Hebrew translation of PGP description."""
        row = self._pgp_conn.execute(
            "SELECT description_he FROM pgp_translations WHERE pgpid = ?",
            (pgpid,)
        ).fetchone()
        return row[0] if row else None

    def get_fjms_free_desc_en(self, alma_id: str, signature_id: int) -> Optional[str]:
        """Get English translation of FJMS free description."""
        row = self._fjms_conn.execute(
            "SELECT free_desc_en FROM fjms_translations "
            "WHERE alma_id = ? AND signature_id = ?",
            (alma_id, signature_id)
        ).fetchone()
        return row[0] if row else None
```

### Pattern 5: Storage Architecture (Recommended: New Tables in Existing Sidecars)
**What:** Add translation tables to pgp.db and fjms_enrichment.db.
**Why this over alternatives:** Co-located data means single sidecar to deploy per domain. Export scripts can be extended to include translations. No new file to manage.
```sql
-- In pgp.db:
CREATE TABLE pgp_translations (
    pgpid INTEGER PRIMARY KEY,
    description_he TEXT,
    document_type_he TEXT,
    translated_at TEXT,
    model_version TEXT DEFAULT 'dictalm2.0'
);

-- In fjms_enrichment.db:
CREATE TABLE fjms_translations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alma_id TEXT NOT NULL,
    field_name TEXT NOT NULL,     -- 'Title', 'TitleHeb', 'AuthorText', 'FreeDesc', etc.
    signature_id INTEGER,         -- For catalog_free_desc (NULL for catalog fields)
    original_text TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    direction TEXT NOT NULL,      -- 'he2en' or 'en2he'
    translated_at TEXT,
    model_version TEXT DEFAULT 'dictalm2.0'
);
CREATE INDEX idx_fjms_trans_alma ON fjms_translations(alma_id);
CREATE INDEX idx_fjms_trans_field ON fjms_translations(alma_id, field_name);
```

### Anti-Patterns to Avoid
- **Inline API calls at runtime:** Never call Dicta API during user search/browse. All translations are pre-computed batch.
- **Overwriting human translations:** FJMS data has human-curated bilingual pairs. NEVER overwrite existing values -- only fill gaps where one language is missing.
- **Single newline as stop sequence for multi-sentence text:** Use `\n\n` (double newline) as stop, not `\n`, when translating descriptions that may contain multiple sentences.
- **Unbounded concurrent requests:** While API handled 10 concurrent without errors, use a configurable throttle (default 10) to be respectful of the free service.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Translation API | Custom LLM inference | Dicta Translation REST API | Free, hosted, high quality for Hebrew/English, no GPU needed |
| Concurrency control | asyncio event loops | ThreadPoolExecutor | Simpler, proven in this codebase, adequate throughput |
| Checkpointing | Custom file locking | JSON checkpoint file + atomic writes | Export scripts already use this pattern |
| Document type translations | API translation of 9 fixed values | Manual mapping dict in code | Only 9 values, scholarly precision needed, cheaper than API |
| Tag translations | Any work at all | pgp_tag_translations.py already complete | CATEGORIZED_TAGS already has ~300 tags translated |
| Domain name translations | Any work at all | Already bilingual in FJMS data | Domain/DomainHeb pairs exist |

**Key insight:** The Dicta API is excellent for free-text descriptions. For small fixed taxonomies (document types, tags), manual translation is more reliable and doesn't require API calls.

## Common Pitfalls

### Pitfall 1: Wrong API Endpoint Path
**What goes wrong:** Using `/v1/completions` (standard OpenAI path) returns 404.
**Why it happens:** The Dicta server uses a non-standard path segment. The C# reference sets `api.ApiVersion = "whatcanthisbe"` which becomes the URL path.
**How to avoid:** Use `/whatcanthisbe/completions` as the endpoint path.
**Warning signs:** HTTP 404 "Cannot POST /v1/completions"

### Pitfall 2: Single Newline Stop Sequence Truncates Multi-Sentence Output
**What goes wrong:** Translation of a description like "Fragment of a letter. Mentions trade." gets cut after the first sentence.
**Why it happens:** `stop: ["\n"]` stops at ANY newline, including sentence boundaries in the output.
**How to avoid:** Use `stop: ["\n\n"]` (double newline) for multi-sentence text. For single-phrase items (document types, titles), `stop: ["\n"]` is fine.
**Warning signs:** Translations consistently shorter than input, missing trailing sentences.

### Pitfall 3: Overwriting Human-Curated FJMS Bilingual Data
**What goes wrong:** Machine translation overwrites high-quality human translations in Title/TitleHeb, TextualFrame Heb/Eng pairs.
**Why it happens:** Batch script doesn't check for existing translations before writing.
**How to avoid:** Gap-fill only: translate where target column IS NULL or empty, source column IS NOT NULL and non-empty. Never overwrite existing data.
**Warning signs:** `UPDATE ... SET Title = ? WHERE AlmaId = ?` without `AND (Title IS NULL OR Title = '')` guard.

### Pitfall 4: Empty or Trivial Descriptions
**What goes wrong:** Translating descriptions like "הערות:" (just "Notes:") or 5-character fragments wastes API calls.
**Why it happens:** Not filtering out trivially short text.
**How to avoid:** Set minimum character threshold (e.g., 20 chars) for translation candidates. Skip descriptions that are just labels or section headers.
**Warning signs:** Many translations that are just 1-2 words, low signal-to-noise ratio.

### Pitfall 5: No Resume After Network Interruption
**What goes wrong:** A 22-hour batch run fails at hour 15, and all progress is lost.
**Why it happens:** No checkpointing mechanism.
**How to avoid:** Save progress to checkpoint file every N items. On restart, skip already-translated IDs.
**Warning signs:** Batch script has no checkpoint/resume logic, uses only in-memory state.

### Pitfall 6: Prompt Too Long for Descriptions with Many Few-Shot Examples
**What goes wrong:** API returns error or truncated output for long descriptions combined with verbose few-shot prompts.
**Why it happens:** DictaLM 2.0 is a 7B parameter model with limited context window (likely 4K-8K tokens from Mistral base).
**How to avoid:** Keep few-shot prompts concise (3-5 examples). For very long descriptions (>2000 chars), use minimal few-shot (1-2 examples). Monitor token counts from usage response.
**Warning signs:** `prompt_tokens` exceeding 2000, completion quality degrading for long inputs.

## Code Examples

### Verified API Call Pattern
```python
# Source: Live API testing 2026-03-04
import requests

DICTA_ENDPOINT = "https://dicta-translation.loadbalancer3.dicta.org.il/whatcanthisbe/completions"
DICTA_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer x-no-api-key",
}

payload = {
    "model": "dicta-il/dictalm2.0",
    "prompt": "English: Letter from a merchant\nHebrew: מכתב מסוחר\n\nEnglish: Legal document\nHebrew: מסמך משפטי\n\nEnglish: Court record regarding a marital dispute\nHebrew:",
    "temperature": 0,
    "stop": ["\n\n"],
    "max_tokens": 512,
}

resp = requests.post(DICTA_ENDPOINT, json=payload, headers=DICTA_HEADERS, timeout=30)
data = resp.json()
translation = data["choices"][0]["text"].strip()
# Returns: "רישום בית דין בנושא סכסוך אישות"

usage = data["usage"]
# Returns: {"prompt_tokens": N, "total_tokens": N, "completion_tokens": N}
```

### PGP Document Type Manual Translations
```python
# Source: PGP document_type survey (2026-03-04)
# Only 9 distinct values - manual translation is more reliable than API
PGP_DOCUMENT_TYPE_HE = {
    "Letter": "מכתב",
    "Legal document": "מסמך משפטי",
    "List or table": "רשימה או טבלה",
    "Literary text": "טקסט ספרותי",
    "State document": "מסמך מדינה",
    "Paraliterary text": "טקסט פרה-ספרותי",
    "Credit instrument or private receipt": "שטר אשראי או קבלה פרטית",
    "Legal query or responsum": "שאלה משפטית או תשובה",
    "Inscription": "כתובת",
}
```

## Data Volume Analysis

### PGP Data (pgp.db) -- EN->HE
| Field | Count | Avg Length | Notes |
|-------|-------|------------|-------|
| description | 35,838 | 282 chars | Primary translation target |
| document_type | 9 distinct | N/A | Manual mapping, NOT API |
| tags | ~300 | N/A | ALREADY translated (pgp_tag_translations.py) |

### FJMS Catalog (fjms_enrichment.db) -- HE->EN Gap-Fill
| Field | Gap Count | Total | Notes |
|-------|-----------|-------|-------|
| TitleHeb without Title (needs EN) | 1,156 | of 685K catalog rows | Fill EN from HE |
| Title without TitleHeb (needs HE) | 1,720 | of 685K catalog rows | Fill HE from EN |
| TextualFrameHeb without Eng | 0 | of 286K frames | No gaps! |
| TextualFrameEng without Heb | 0 | of 286K frames | No gaps! |
| AuthorText | 1,435 non-empty | mostly Hebrew | Translate to EN |
| genizah_titles OrgTitle w/o EngTitle | 626 | of 775 titles | HE->EN |
| genizah_persons EngDesc w/o HebDesc | 703 | of 2,286 persons | EN->HE |
| genizah_persons HebDesc w/o EngDesc | 1,163 | of 2,286 persons | HE->EN |
| catalog_free_desc | 303,392 total | 170K distinct AlmaIds | HE->EN, largest volume |
| bibliography | 542,487 | N/A | Lower priority |

### Time Estimates (at 10 concurrent workers, ~228 req/min)
| Dataset | Items | Estimated Time |
|---------|-------|----------------|
| PGP descriptions (35K) | 35,838 | ~2.6 hours |
| FJMS catalog gaps (~5K) | ~5,600 | ~25 minutes |
| FJMS free descriptions (303K) | 303,392 | ~22 hours |
| Bibliography (542K) | 542,487 | ~40 hours (deferred) |

## API Reference

### Dicta Translation REST API
| Property | Value |
|----------|-------|
| Base URL | `https://dicta-translation.loadbalancer3.dicta.org.il` |
| Endpoint | `/whatcanthisbe/completions` |
| Auth | `Authorization: Bearer x-no-api-key` |
| Model | `dicta-il/dictalm2.0` |
| Method | POST |
| Content-Type | application/json |

### Request Format
```json
{
    "model": "dicta-il/dictalm2.0",
    "prompt": "<few-shot prefix>\n\n<src_category>: <text>\n<tgt_category>:",
    "temperature": 0,
    "stop": ["\n\n"],
    "max_tokens": 1024
}
```

### Response Format
```json
{
    "id": "cmpl-...",
    "object": "text_completion",
    "created": 1772604037,
    "model": "dicta-il/dictalm2.0",
    "choices": [
        {
            "index": 0,
            "text": " translated text here",
            "finish_reason": "stop"
        }
    ],
    "usage": {
        "prompt_tokens": 109,
        "total_tokens": 151,
        "completion_tokens": 42
    }
}
```

### Performance Characteristics (Tested 2026-03-04)
| Metric | Value |
|--------|-------|
| Avg latency (sequential) | 1.85s per request |
| Avg latency (10 concurrent) | 2.49s per request |
| Throughput (sequential) | 32 req/min |
| Throughput (3 concurrent) | 73 req/min |
| Throughput (5 concurrent) | 138 req/min |
| Throughput (10 concurrent) | 228 req/min |
| Rate limit errors | None observed at 10 concurrent |
| Max input tested | 519 chars (worked fine) |
| Multi-paragraph | Works with stop=["\n\n"] |

### Few-Shot Template Format
```json
{
    "prompts": [
        {"English": "...", "Hebrew": "..."},
        {"English": "...", "Hebrew": "..."}
    ],
    "en_category": "English",
    "he_category": "Hebrew"
}
```

Prompt is constructed as:
```
{en_category}: {example1_en}
{he_category}: {example1_he}

{en_category}: {example2_en}
{he_category}: {example2_he}

{en_category}: {text_to_translate}
{he_category}:
```

Direction reversal: swap order within each pair (source category first, target category second).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Manual translation only | DictaLM 2.0 (190B token Hebrew/English model) | 2024 | Automates bulk translation with scholarly quality |
| OpenAI/GPT for Hebrew | DictaLM 2.0 (free, specialized for Hebrew) | 2024 | No cost, better Hebrew quality, runs on Dicta infrastructure |
| Single request processing | Concurrent requests (10 workers) | Tested 2026-03-04 | 7x throughput improvement |

## Open Questions

1. **Rate limits at scale**
   - What we know: 10 concurrent requests work fine in short bursts (10 requests)
   - What's unclear: Whether sustained 10-concurrent over 22 hours triggers rate limiting
   - Recommendation: Start with 5 concurrent, monitor for errors, increase if stable. Add exponential backoff.

2. **Translation quality for long descriptions (>2000 chars)**
   - What we know: 519-char descriptions translate perfectly. Model context window is likely 4K-8K tokens.
   - What's unclear: Quality for the 174 descriptions >2000 chars
   - Recommendation: Test the longest descriptions. If quality degrades, chunk at sentence boundaries.

3. **Bibliography translation strategy**
   - What we know: 542K entries, mostly English scholarly references with mixed Hebrew/English author names
   - What's unclear: Whether translating bibliography references adds value vs confusion
   - Recommendation: Defer to separate sub-phase. Test with samples first.

4. **Optimal few-shot example count**
   - What we know: 2-3 examples produce good results. More examples increase prompt tokens.
   - What's unclear: Diminishing returns curve. Whether domain-specific few-shots (legal vs letter vs catalog) improve quality.
   - Recommendation: Start with 3 general scholarly examples. Test quality. Only add domain-specific if quality is insufficient.

## Validation Architecture

> `workflow.nyquist_validation` not explicitly set to false in config.json, so including this section.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | None (default pytest discovery) |
| Quick run command | `pytest tests/ -x -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| N/A-TRANS-01 | Dicta API client translates EN->HE correctly | unit | `pytest tests/test_translation_service.py::test_en2he -x` | Wave 0 |
| N/A-TRANS-02 | Dicta API client translates HE->EN correctly | unit | `pytest tests/test_translation_service.py::test_he2en -x` | Wave 0 |
| N/A-TRANS-03 | Few-shot prompt construction matches template format | unit | `pytest tests/test_translation_service.py::test_prompt_build -x` | Wave 0 |
| N/A-TRANS-04 | Batch script checkpoints and resumes | unit | `pytest tests/test_translation_service.py::test_checkpoint -x` | Wave 0 |
| N/A-TRANS-05 | Gap-fill never overwrites existing translations | unit | `pytest tests/test_translation_service.py::test_no_overwrite -x` | Wave 0 |
| N/A-TRANS-06 | Translation service reads from sidecar tables | unit | `pytest tests/test_translation_service.py::test_read_service -x` | Wave 0 |
| N/A-TRANS-07 | Document type manual translations cover all 9 values | unit | `pytest tests/test_translation_service.py::test_doc_types -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_translation_service.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_translation_service.py` -- translation service unit tests
- [ ] `shared/translation_service.py` -- read-only translation service
- [ ] `data/few_shot_en2he_scholarly.json` -- EN->HE few-shot template
- [ ] `data/few_shot_he2en_scholarly.json` -- HE->EN few-shot template

## Sources

### Primary (HIGH confidence)
- **Dicta Translation REST API** -- Live tested 2026-03-04, endpoint `/whatcanthisbe/completions`, all response formats verified
- **TestLLMAPIsProgram.cs** -- C# reference implementation at project root, shows API key handling, model name, few-shot format, completions vs chat distinction
- **pgp.db / fjms_enrichment.db** -- Direct database queries for volume analysis, gap counts, sample data
- **HuggingFace dicta-il/dictalm2.0** -- Model card confirms 7B params, Mistral-7B base, 190B+ token training, 1000 injected Hebrew tokens

### Secondary (MEDIUM confidence)
- **Dicta translate frontend** (translate.dicta.org.il) -- JS bundle confirms `genre: "modern-fancy"`, `direction: "he-en"/"en-he"` parameters, WebSocket API at `wss://translate.loadbalancer.dicta.org.il/api/ws`

### Tertiary (LOW confidence)
- **Rate limit behavior at sustained load** -- Only tested 10-request bursts, not sustained multi-hour runs. Flag for validation during batch execution.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All libraries already in project, API verified with live tests
- Architecture: HIGH - Follows established sidecar + service + export script pattern
- API behavior: HIGH - Tested endpoint, response format, concurrency, latency
- Pitfalls: HIGH - All based on actual testing (wrong endpoint, stop sequence, etc.)
- Rate limits at scale: LOW - Only burst-tested, sustained load untested

**Research date:** 2026-03-04
**Valid until:** 2026-04-04 (API endpoint is stable, model is versioned)
