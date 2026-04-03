# Phase 62: Investigation & Validation - Research

**Researched:** 2026-04-03
**Domain:** NLI IIIF image caching feasibility -- rate limits, storage, filesystem, TOS
**Confidence:** MEDIUM (investigation phase -- many findings require empirical validation)

## Summary

Phase 62 is a pure investigation phase: no product code ships. The output is validated data, documented decisions, and reusable test scripts. The phase must answer five questions: (1) what sustained fetch rate can NLI's IIIF API tolerate from a residential IP, (2) how much storage does the NLI-only image corpus require, (3) can EC2's ext4 filesystem handle 300K+ files efficiently, (4) does NLI's TOS permit academic caching, and (5) what image resolution balances quality and storage.

The NLI-only subset -- manuscripts with NLI images but no alternative provider (Cambridge, Manchester, JTS, Oxford) -- is approximately **44,700 manuscripts comprising ~347,400 image rows** in `nli_crossref.db`. However, actual FL IDs must come from IIIF manifest fetches (FGPImageNumberId in the crossref DB is a Friedberg photo number, NOT an NLI FL ID). The estimated total cached images is roughly 300K-350K individual JPEG files.

**Primary recommendation:** Execute in the order specified in CONTEXT.md (TOS first, then subset scoping, rate testing, storage sampling, filesystem validation, report). The NLI API rate limit for authenticated keys is 1,000 requests/hour; the IIIF image endpoint (unauthenticated, different domain) likely has separate limits that the rate test must discover empirically.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **Execution order:** TOS review -> NLI-only subset -> rate test -> storage analysis -> filesystem validation -> report (D-09 through D-16)
- **TOS gate (INV-04):** Documented TOS determination within 5 business days; conditional-go if terms are silent; supersedes hard-gate wording (D-09/D-10/D-11)
- **Ingest topology (D-17):** Residential fetch + rsync to EC2. NLI blocks datacenter IPs (verified 2026-03-17). Home PC fetches -> local staging -> rsync/scp to EC2 -> atomic promotion
- **Rate test execution (D-01 through D-04):** Home PC, residential IP. Conservative ramp: 1->2->4->8 req/sec over 15+ min. Abort on 429/403/3+ consecutive timeouts. Two resolutions: 800px and 1200px
- **Storage sampling (D-07/D-07a):** 1000+ images total, same 500+ manuscripts at both resolutions. Human quality review for resolution decision
- **Filesystem validation (D-15):** Practical test on EC2 with ~50-100K dummy files, measure ls/stat/find performance
- **Deliverables (D-12 through D-14, D-16):** Report in 62-REPORT.md + docs/specs/image-cache-investigation.md. Scripts in scripts/ directory. Monthly EBS cost projection required
- **NLI-only subset (D-05/D-06):** Query nli_crossref.db cross-referencing 4 provider tables + oxford_full_db.json. library_code is NOT safe proxy

### Claude's Discretion
- EC2 filesystem directory structure details (D-08, guided by test results)
- Sample selection algorithm (random vs stratified within NLI-only subset)
- Report structure and sections

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INV-01 | Rate limit testing from residential IP confirms safe NLI fetch rate | NLI API rate limit is 1,000 req/hour for keyed endpoints; IIIF image endpoint limits unknown -- must test empirically. Existing code uses 8-connection pool with semaphore. Rate test script pattern from build_ie_volume_map.py |
| INV-02 | Storage validation: 1000+ image sample at target resolution; NLI-only subset determination | NLI-only subset is ~44,700 manuscripts / ~347K image rows. FGPImageNumberId != FL ID -- manifest fetch needed. Cross-reference 4 tables + oxford_full_db.json |
| INV-03 | EC2 filesystem verified for 815K+ files; hierarchical directory structure | ext4 handles millions of files with htree indexing. Optimal: 100-500 files/directory. 2-level hex hash gives 65K dirs. Default inode ratio supports ~65M inodes on 1TB |
| INV-04 | NLI contacted about TOS -- go/no-go gate | TOS allows "private study, scholarship or research." IIIF API is public, no auth required. API rate limit page exists. Formal outreach email needed per D-10 |
| INV-05 | Target image resolution decided based on storage/quality tradeoff | Rate test at 800px and 1200px produces paired comparison data. Human quality review per D-07a. Size difference drives cost projection |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| requests | 2.32.x | HTTP client for IIIF image fetching | Already used throughout project (web/api.py, genizah_core.py) |
| sqlite3 | stdlib | Query nli_crossref.db for subset determination | Standard for all sidecar DB access in project |
| json | stdlib | Load oxford_full_db.json, write report data | Standard |
| time/statistics | stdlib | Rate measurement, statistics on image sizes | Standard |
| pathlib | stdlib | File path handling for scripts | Project convention in scripts/ |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tqdm | 4.x | Progress bars for batch operations | Already in requirements.txt; use for rate test and sampling progress |
| paramiko or subprocess+ssh | -- | rsync/scp for filesystem test on EC2 | Only needed for D-15 filesystem test; subprocess is fine |
| argparse | stdlib | CLI argument parsing for scripts | Project convention in scripts/ |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| requests | aiohttp | Async would complicate for no gain at 1-8 req/sec test rates; stick with requests |
| Manual rate control | ratelimit library | Overkill for investigation scripts; simple time.sleep() suffices |

**Installation:**
No new packages needed. All dependencies already in project.

## Architecture Patterns

### Script Organization
```
scripts/
  nli_rate_test.py         # INV-01: Rate limit testing with ramp-up
  nli_storage_sample.py    # INV-02 + INV-05: Storage sampling at two resolutions
  nli_only_subset.py       # INV-02: NLI-only subset determination query
  ec2_fs_test.sh           # INV-03: Filesystem benchmark on EC2 (or .py)
```

### Pattern 1: Rate Test Script Structure
**What:** Conservative ramp-up rate tester with abort conditions
**When to use:** INV-01
**Example:**
```python
# Pattern from existing project scripts (build_ie_volume_map.py style)
import requests
import time
import statistics
from pathlib import Path

RATES = [1, 2, 4, 8]  # req/sec ramp-up
PLATEAU_DURATION = 300  # 5 minutes per rate
ABORT_CODES = {429, 403}
CONSECUTIVE_TIMEOUT_LIMIT = 3
TIMEOUT_THRESHOLD = 30  # seconds

def test_rate(session, fl_ids, rate, width, output_dir):
    """Test a specific request rate. Returns (success_count, error_count, sizes)."""
    interval = 1.0 / rate
    consecutive_timeouts = 0
    successes, errors, sizes = 0, 0, []
    start = time.time()
    
    for fl_id in fl_ids:
        if time.time() - start > PLATEAU_DURATION:
            break
        
        url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg"
        try:
            resp = session.get(url, timeout=TIMEOUT_THRESHOLD)
            if resp.status_code in ABORT_CODES:
                return successes, errors, sizes, "ABORT"
            if resp.status_code == 200:
                # Save image for storage sampling
                path = output_dir / f"FL{fl_id}_{width}px.jpg"
                path.write_bytes(resp.content)
                sizes.append(len(resp.content))
                successes += 1
                consecutive_timeouts = 0
            else:
                errors += 1
        except requests.Timeout:
            consecutive_timeouts += 1
            errors += 1
            if consecutive_timeouts >= CONSECUTIVE_TIMEOUT_LIMIT:
                return successes, errors, sizes, "TIMEOUT_ABORT"
        
        time.sleep(max(0, interval - (time.time() - start) % interval))
    
    return successes, errors, sizes, "OK"
```

### Pattern 2: NLI-Only Subset Query
**What:** Cross-reference 4 provider tables + Oxford JSON to determine NLI-only manuscripts
**When to use:** INV-02 (D-05/D-06)
**Example:**
```python
import sqlite3
import json

def get_nli_only_subset(crossref_db_path, oxford_json_path):
    """Return set of sys_ids that have NLI images but NO alternative provider."""
    conn = sqlite3.connect(crossref_db_path)
    
    # All NLI sys_ids
    nli_sysids = set(r[0] for r in conn.execute(
        "SELECT DISTINCT NLI_AlmaId FROM nli_images"
    ).fetchall())
    
    # Cambridge: match by normalized_shelfmark
    # Note: Cambridge match is shelfmark-based, not sys_id-based
    # Need to join through Shelfmark field
    cambridge_covered = set(r[0] for r in conn.execute("""
        SELECT DISTINCT i.NLI_AlmaId 
        FROM nli_images i
        JOIN cambridge_manifests c 
          ON c.normalized_shelfmark = i.Shelfmark
    """).fetchall())
    
    # Manchester: match through ImageSourceName
    manchester_covered = set(r[0] for r in conn.execute("""
        SELECT DISTINCT i.NLI_AlmaId
        FROM nli_images i
        JOIN manchester_luna m 
          ON LOWER(i.ImageSourceName) = m.image_source_name
    """).fetchall())
    
    # JTS: match by Shelfmark
    jts_covered = set(r[0] for r in conn.execute("""
        SELECT DISTINCT i.NLI_AlmaId
        FROM nli_images i
        JOIN jts_dpul j ON j.shelfmark = i.Shelfmark
    """).fetchall())
    
    conn.close()
    
    # Oxford: load JSON and extract sys_ids
    oxford_covered = set()
    with open(oxford_json_path) as f:
        oxford_data = json.load(f)
        # Extract sys_ids from Oxford records
        for key in oxford_data:
            oxford_covered.add(key)  # verify key format matches sys_id
    
    # NLI-only = NLI minus all alternative providers
    nli_only = nli_sysids - cambridge_covered - manchester_covered - jts_covered - oxford_covered
    return nli_only
```

**Important note on the subset query:** The rough estimate of ~44,700 NLI-only manuscripts (based on library_code exclusion) may differ from the precise cross-reference result. The actual query must join through the same paths that `nli_crossref_service.get_image_sources()` uses. Cambridge matches via `normalized_shelfmark`, Manchester via `ImageSourceName`, JTS via `Shelfmark`. Mosseri manuscripts that happen to match Cambridge shelfmarks will be correctly excluded.

### Pattern 3: Filesystem Benchmark
**What:** Create representative directory structure on EC2 and measure performance
**When to use:** INV-03 (D-15)
```bash
#!/bin/bash
# Create 2-level hash directories with dummy files
# Simulates: /cache/{xx}/{yy}/{fl_id}.jpg
BASE="/tmp/cache_test"
mkdir -p "$BASE"

echo "Creating 50,000 dummy files in 2-level hash structure..."
for i in $(seq 1 50000); do
    hex=$(printf '%06x' $i)
    dir="$BASE/${hex:0:2}/${hex:2:2}"
    mkdir -p "$dir"
    dd if=/dev/urandom of="$dir/${hex}.jpg" bs=100K count=1 2>/dev/null
done

echo "Testing ls performance..."
time ls "$BASE/00/00/" > /dev/null
echo "Testing stat performance..."
time stat "$BASE/00/00/000001.jpg" > /dev/null
echo "Testing find count..."
time find "$BASE" -name "*.jpg" | wc -l
echo "Testing du..."
time du -sh "$BASE"
```

### Anti-Patterns to Avoid
- **Fetching from EC2 directly:** NLI blocks datacenter IPs. All fetches must originate from residential IP.
- **Using FGPImageNumberId as FL ID:** These are Friedberg photo numbers, NOT NLI IIIF FL IDs. Must fetch IIIF manifest to resolve actual FL IDs (lesson from Phase 30).
- **Using library_code as proxy for image availability:** Mosseri has partial Cambridge coverage; must cross-reference per-manuscript.
- **Over-engineering test scripts:** These are investigation tools, not production code. Phase 63 builds the real batch fetcher.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HTTP session management | Custom connection pool | `requests.Session` with `HTTPAdapter` | Already proven in web/api.py with NLI |
| Progress bars | Custom progress output | `tqdm` | Already in requirements.txt |
| NLI-only subset logic | New SQL queries from scratch | Extend/reference `nli_crossref_service.get_image_sources()` patterns | Service already knows the join paths |
| Rate limiting | Complex token bucket | Simple `time.sleep(interval)` | Investigation rate (1-8 req/sec) doesn't need sophistication |

## Common Pitfalls

### Pitfall 1: FGPImageNumberId vs FL ID Confusion
**What goes wrong:** Attempting to construct IIIF image URLs from FGPImageNumberId values in nli_crossref.db
**Why it happens:** The field name suggests it's a FL ID, but it's actually a Friedberg photo number (different numbering system)
**How to avoid:** Always resolve FL IDs by fetching IIIF manifests and extracting FL numbers from canvas service URLs
**Warning signs:** Images return 404 or wrong content when using FGP numbers directly

### Pitfall 2: NLI API Rate Limit vs IIIF Image Endpoint
**What goes wrong:** Assuming the documented 1,000 req/hour API limit applies to the IIIF image endpoint
**Why it happens:** NLI has two domains: `api.nli.org.il` (documented rate limits, requires API key) and `iiif.nli.org.il` (public IIIF, no documented limits)
**How to avoid:** Treat IIIF image endpoint limits as unknown until empirically tested. The 1,000/hour limit is for the API key-based endpoints, not necessarily for IIIF image delivery
**Warning signs:** Rate test shows much higher or lower tolerance than expected

### Pitfall 3: Manifest Fetch as Rate Bottleneck
**What goes wrong:** The rate test plan calls for testing image fetches, but forgets that each manuscript requires a manifest fetch first to resolve FL IDs
**Why it happens:** Manifest fetches are separate HTTP requests to the same iiif.nli.org.il domain
**How to avoid:** For the rate test, pre-resolve a batch of FL IDs from manifests before starting the timed image-fetch rate test. Keep manifest resolution and image fetching as separate measured operations
**Warning signs:** Rate measurements conflate manifest fetch time with image fetch time

### Pitfall 4: St. Petersburg Manuscripts Skewing Storage Estimates
**What goes wrong:** RNL/St. Petersburg manuscripts average 16.6 images per manuscript vs 1.7 for BL/Mosseri
**Why it happens:** RNL has large multi-folio codices scanned page-by-page
**How to avoid:** Use stratified sampling across libraries for INV-02, or at minimum report per-library statistics
**Warning signs:** Storage estimate varies wildly depending on which manuscripts were sampled

### Pitfall 5: Oxford Cross-Reference Format Mismatch
**What goes wrong:** Oxford data is in `oxford_full_db.json`, NOT in nli_crossref.db. The JSON key format may not match NLI_AlmaId directly
**Why it happens:** Oxford integration uses a separate data path (web/api.py `/api/oxford_image/`)
**How to avoid:** Inspect oxford_full_db.json key format before building the subset query. May need to match via shelfmark rather than sys_id
**Warning signs:** Zero Oxford exclusions, or exclusion count doesn't match expected ~12,900

### Pitfall 6: Empty FGPImageNumberId for Some Libraries
**What goes wrong:** Some NLI-only libraries (notably St. Petersburg) have empty FGPImageNumberId in crossref, meaning FL IDs are ONLY available via manifest fetch
**Why it happens:** Not all NLI images were indexed with FGP numbers in the crossref database
**How to avoid:** The rate test script must include manifest-based FL resolution. About 7.7% of NLI-only image rows lack FGP IDs, but 32% of NLI-only manuscripts have no FGP IDs at all (14,436 of 44,705). All 16,332 RNL manuscripts fall in this category
**Warning signs:** Script skips manuscripts with no FGPImageNumberId, producing a biased sample

## Code Examples

### Existing NLI IIIF URL Construction (from web/api.py)
```python
# Source: web/api.py line 384 (manifest URL)
url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"

# Source: web/api.py line 535 (image URL with configurable width)
iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg"

# Source: web/api.py line 465 (max resolution image)
iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/2000,/0/default.jpg"
```

### Existing Session Configuration Pattern (from web/api.py)
```python
# Source: web/api.py lines 36-44
_nli_session = requests.Session()
_nli_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
})
_nli_adapter = requests.adapters.HTTPAdapter(
    pool_connections=8,
    pool_maxsize=16,
)
_nli_session.mount('https://iiif.nli.org.il', _nli_adapter)
```

### FL ID Extraction from Manifest (from web/api.py)
```python
# Source: web/api.py lines 392-399
if 'sequences' in data and data['sequences']:
    for canvas in data['sequences'][0].get('canvases', []):
        images = canvas.get('images', [])
        if images:
            resource = images[0].get('resource', {})
            service = resource.get('service', {})
            service_id = service.get('@id', '')
            # Extract FL number from service ID URL
```

### Existing Script Pattern (from scripts/build_ie_volume_map.py)
```python
# Source: scripts/build_ie_volume_map.py lines 26-39
import argparse
import json
import os
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_FILE = PROJECT_DIR / "ie_volume_map.json"
# Uses argparse for CLI, tqdm for progress, json for output
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Live IIIF fetch per page view | Planned: server-side image cache | Phase 62-64 (this milestone) | Eliminates NLI dependency for cached images |
| FGP number for image lookup | FL ID from IIIF manifest | Phase 30 (2026-02) | FGP != FL; manifest fetch required |
| Single NLI image source | 4 alternative providers (CUL/Manchester/JTS/Oxford) | v5.9.0 (2026-02) | NLI-only subset is ~44K not ~253K |
| EC2 direct NLI fetch | Residential fetch + rsync | Verified 2026-03-17 | NLI blocks datacenter IPs |

## Open Questions

1. **IIIF Image Endpoint Rate Limit**
   - What we know: NLI's API key endpoints have 1,000 req/hour limit. The IIIF image endpoint at iiif.nli.org.il is unauthenticated and has no documented rate limit.
   - What's unclear: Whether the IIIF endpoint shares the same limit, has its own limit, or has no limit at all
   - Recommendation: The rate test (INV-01) answers this empirically. Start conservative (1 req/sec = 3,600/hour) and ramp up.

2. **Actual Image File Sizes**
   - What we know: Previous "86GB" estimate was based on a single test image. No validated average.
   - What's unclear: Average JPEG size at 800px and 1200px widths across the corpus (varies by scan quality, content, library)
   - Recommendation: INV-02 sampling of 1000+ images answers this. Expect 50-200KB at 800px, 100-400KB at 1200px based on typical IIIF JPEG delivery.

3. **Oxford JSON Key Format**
   - What we know: `oxford_full_db.json` has ~13K records. Used in `web/api.py` for Oxford image delivery.
   - What's unclear: Whether JSON keys are NLI sys_ids (NLI_AlmaId format) or Oxford-specific identifiers
   - Recommendation: Inspect the JSON file during implementation. If keys don't match sys_ids, need a shelfmark-based cross-reference.

4. **Cambridge Shelfmark Match Coverage for Mosseri**
   - What we know: Rough library_code exclusion puts Mosseri in NLI-only (8,054 manuscripts). An exact SQL join on Shelfmark found 0 matches.
   - What's unclear: Whether the Cambridge match for Mosseri uses a different path (label construction in genizah_core.py) that can't be replicated in a simple SQL join
   - Recommendation: Inspect the Mosseri-to-Cambridge label construction logic in genizah_core.py. May need a Python-side matching function rather than pure SQL.

5. **EC2 Current Disk Space**
   - What we know: EC2 runs Ubuntu at `/home/ubuntu/GenizahSearch`. Uses EBS storage.
   - What's unclear: Current disk usage, available space, EBS volume type and size
   - Recommendation: SSH to EC2 and run `df -h`, `lsblk`, `tune2fs -l /dev/xvda1 | grep -i inode` during filesystem validation step.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | All scripts | Yes | 3.10+ (project req) | -- |
| requests | HTTP fetching | Yes | In requirements.txt | -- |
| tqdm | Progress bars | Yes | In requirements.txt | -- |
| sqlite3 | nli_crossref.db queries | Yes | stdlib | -- |
| SSH/rsync to EC2 | D-15 filesystem test | Yes | Via SSH key | Cockpit web terminal |
| nli_crossref.db | Subset determination | Yes | Local file | -- |
| oxford_full_db.json | Oxford exclusion | Yes | Local file | -- |
| Residential IP | NLI rate testing | Yes | Home PC (per D-01) | -- |
| pytest | Test validation | Yes | 9.0.2 | -- |

**Missing dependencies with no fallback:** None

**Missing dependencies with fallback:** None

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | `tests/conftest.py` |
| Quick run command | `pytest tests/ -x -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INV-01 | Rate test confirms sustainable fetch rate | manual + script | `python scripts/nli_rate_test.py --dry-run` (validate script logic) | -- Wave 0 |
| INV-02 | NLI-only subset count; 1000+ image sample | unit (subset query) + manual (sampling) | `pytest tests/test_nli_subset.py -x` | -- Wave 0 |
| INV-03 | EC2 filesystem handles 300K+ files | manual (EC2 SSH test) | N/A (remote execution) | -- manual-only |
| INV-04 | TOS review documented | manual-only | N/A (human judgment) | -- manual-only |
| INV-05 | Resolution decided with data | manual (human review of samples) | N/A | -- manual-only |

### Sampling Rate
- **Per task commit:** `pytest tests/test_nli_subset.py -x` (if subset query test exists)
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Investigation report complete with all 5 INV requirements addressed

### Wave 0 Gaps
- [ ] `tests/test_nli_subset.py` -- unit tests for NLI-only subset determination query (covers INV-02 subset logic)
- [ ] Script dry-run modes for `nli_rate_test.py` and `nli_storage_sample.py` (validates script logic without network)

Note: Most INV requirements are empirical/manual. Automated testing is limited to validating script logic and subset query correctness.

## NLI TOS and Rate Limit Analysis

### Known Facts (HIGH confidence)
- NLI API key-based endpoints: **1,000 requests/hour** per API key (source: api2.nli.org.il/docs/rate-limits)
- Exceeding limits returns HTTP 429 (Too Many Requests)
- Block lifts automatically after the hourly window rolls over
- NLI general TOS: use limited to "private study, scholarship or research" (source: nli.org.il terms of use)
- IIIF image endpoint (iiif.nli.org.il) is public, no API key required, no documented rate limit
- NLI blocks datacenter IPs (verified 2026-03-17 by project team)

### Inferred (MEDIUM confidence)
- The IIIF image endpoint likely has its own rate limiting (separate from API key limits) but thresholds are undocumented
- Academic caching of manuscript images for research platforms is likely within "scholarship or research" fair use, but NLI has not been asked directly
- Genizah manuscript images are generally not under copyright (medieval manuscripts), but NLI may claim rights over the digitization

### Unknown (requires empirical testing)
- IIIF image endpoint actual rate limit from residential IP
- Whether sustained 4-8 req/sec triggers any blocking
- NLI's institutional position on bulk academic caching
- Whether NLI would grant explicit permission or higher rate limits for academic projects

## NLI-Only Subset Data Summary

### Estimated Counts
| Metric | Value | Source |
|--------|-------|--------|
| Total NLI sys_ids | 253,103 | nli_crossref.db query |
| NLI-only manuscripts (by library_code exclusion) | ~44,705 | Excluding CUL/Manchester/JTS/Oxford library codes |
| NLI-only image rows | ~347,435 | Same exclusion |
| Avg images per NLI-only manuscript | 7.8 | Skewed by RNL (16.6 avg) |
| Distinct FGP IDs (NLI-only, where present) | ~297,655 | NOT usable as FL IDs |
| NLI-only sys_ids with zero FGP IDs | ~14,436 | 32% of NLI-only -- all RNL |

### Library Breakdown (NLI-only, top 5)
| Library | Manuscripts | Image Rows | Avg/Manuscript |
|---------|-------------|------------|----------------|
| St. Petersburg (RNL) | 16,332 | 271,476 | 16.6 |
| British Library | 9,813 | 17,223 | 1.8 |
| Mosseri | 8,054 | 14,090 | 1.7 |
| AIU | 4,086 | 14,821 | 3.6 |
| Lewis-Gibson | 1,811 | 3,895 | 2.2 |

**Key insight:** St. Petersburg dominates image count (78% of NLI-only images) due to large multi-folio codices. Storage estimate is heavily influenced by RNL coverage.

### Cross-Reference Nuances
- Cambridge match path: `nli_images.Shelfmark` = `cambridge_manifests.normalized_shelfmark` (exact match found 0 Mosseri -- may need label construction logic from genizah_core.py)
- Manchester match path: `LOWER(nli_images.ImageSourceName)` = `manchester_luna.image_source_name`
- JTS match path: `nli_images.Shelfmark` = `jts_dpul.shelfmark`
- Oxford: separate JSON file, key format needs verification

## EBS Cost Reference

| Volume Type | Price (us-west-2) | Baseline IOPS | Baseline Throughput |
|-------------|-------------------|---------------|---------------------|
| gp3 | $0.08/GB-month | 3,000 IOPS free | 125 MB/s free |
| gp2 | $0.10/GB-month | 3 IOPS/GB | burst to 3,000 |

**Quick cost projection (to be validated by INV-02):**
- If ~300K images at avg 150KB (800px) = ~45GB -> ~$3.60/month on gp3
- If ~300K images at avg 300KB (1200px) = ~90GB -> ~$7.20/month on gp3
- Additional provisioned IOPS/throughput unlikely needed for static file serving

## Project Constraints (from CLAUDE.md)

- Scripts go in `scripts/` directory (consistent with existing convention)
- Python 3.10+
- Use `requests` library for HTTP (already standard in project)
- Hebrew RTL awareness for any user-facing output
- Test with `pytest tests/`
- Investigation report in `.planning/phases/62-investigation-validation/62-REPORT.md` + `docs/specs/image-cache-investigation.md`

## Sources

### Primary (HIGH confidence)
- `nli_crossref.db` -- Direct queries for subset counts, schema, library breakdown
- `web/api.py` -- NLI IIIF URL patterns, session configuration, FL ID resolution logic
- `shared/nli_crossref_service.py` -- Image source cross-reference join patterns
- [NLI API Rate Limits](https://api2.nli.org.il/docs/rate-limits/) -- 1,000 req/hour per API key

### Secondary (MEDIUM confidence)
- [NLI Terms of Use](https://www.nli.org.il/en/at-your-service/terms-of-use) -- "private study, scholarship or research" language (page returned 403 on direct fetch; content from WebSearch summary)
- [NLI Open Library / Developer Portal](https://www.nli.org.il/en/research-and-teach/open-library) -- IIIF API documentation
- [AWS EBS gp3 Pricing](https://aws.amazon.com/ebs/pricing/) -- $0.08/GB-month in US regions
- [ext4 Performance](https://www.funwithlinux.net/blog/optimal-number-of-files-per-directory-vs-number-of-directories-for-ext4/) -- 100-500 files/directory optimal, htree indexing

### Tertiary (LOW confidence)
- NLI download gist by ttv20 -- Uses 10 concurrent threads, no deliberate throttling, "not working anymore" note suggests NLI has anti-scraping measures
- IIIF image endpoint rate limits -- No documentation found; all claims about IIIF-specific limits are speculative

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in use in the project
- Architecture: HIGH -- patterns directly derived from existing codebase
- NLI rate limits: LOW for IIIF endpoint (undocumented), HIGH for API key endpoints (documented)
- NLI TOS: MEDIUM -- general terms found, specific caching guidance requires outreach
- Storage estimates: LOW -- rough extrapolation; INV-02 sampling is the whole point
- Filesystem: HIGH -- ext4 htree is well-documented for this scale
- Pitfalls: HIGH -- derived from project history and verified data analysis

**Research date:** 2026-04-03
**Valid until:** 2026-05-03 (stable domain; NLI TOS/API unlikely to change within 30 days)
