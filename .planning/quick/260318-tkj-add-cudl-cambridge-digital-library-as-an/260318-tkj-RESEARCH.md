# Quick Task 260318-tkj: Add CUDL as Image Source for CUL-hosted Private Collections - Research

**Researched:** 2026-03-18
**Domain:** Image resolution pipeline, shelfmark normalization, IIIF manifests
**Confidence:** HIGH

## Summary

The Mosseri collection (3,194 records, `lib_code='Mosseri'`) has 3,883 IIIF manifests on CUDL, already imported into the `cambridge_manifests` table of `nli_crossref.db`. The manifests are never discovered because the crossref lookup in `enrich_metadata` uses `normalized_shelfmark` matching, and Mosseri shelfmarks normalize differently than their crossref counterparts.

**Root cause:** The csv_bank stores the **shortest** call_number variant for each record. For Mosseri, the shortest variant is typically `Ms. {SERIES} {NUM}` (e.g., "Ms. VI 108"), which `normalize_shelfmark()` converts to `vi108` (the "ms" prefix is stripped). The crossref sidecar stores the normalized form as `mosserivi108`. The "mosseri" prefix is missing, so the lookup always returns NULL.

**Primary recommendation:** Add a Mosseri-specific CUDL label construction function that converts shelfmark variants into CUDL manifest labels (e.g., `MS-MOSSERI-VI-00108`), then use the existing `get_cambridge_manifest_by_label()` to look up the manifest URL. This achieves 98.3% coverage when iterating over all call_number variants.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Research phase will determine exactly which library_codes are on CUDL using existing docs and the CUDL collection manifest
- User confirmed: use existing documentation in /docs/ as primary research source

### Claude's Discretion
- Classmark conversion rules (shelfmark to CUDL classmark format)
- Manifest verification strategy (construct URL vs verify first)
- Priority placement within enrich_metadata's image source chain

### Deferred Ideas (OUT OF SCOPE)
None specified.
</user_constraints>

## Findings

### 1. Which CUL-hosted Collections Are on CUDL

CUDL manifests in `cambridge_manifests` have exactly 4 collection prefixes:

| CUDL Prefix | Count | Mapped library_code | Status |
|-------------|-------|---------------------|--------|
| `MS-TS` | 135,946 | CUL | Already works (crossref lookup matches) |
| `MS-MOSSERI` | 3,883 | Mosseri | **BROKEN** - crossref lookup fails |
| `MS-OR` | 1,426 | CUL | Already works (CUL Oriental collection) |
| `MS-ADD` | 113 | CUL | Already works (CUL Additional MSS) |

No other private collections (Gaster, Westminster, Halper) appear on CUDL. Gaster was sold to Manchester. Westminster has only 5 records in libraries.csv and no CUDL presence.

**Conclusion:** Only `Mosseri` (lib_code) needs fixing. All other CUDL collections already work.

**Confidence: HIGH** -- verified by direct SQL query on `nli_crossref.db`.

### 2. Why the Crossref Lookup Fails for Mosseri

The image resolution chain in `enrich_metadata` (line 3287-3297):

```python
# 2a-supplement: if MARC didn't provide a CUDL link, try crossref sidecar
shelfmark = current_meta.get('shelfmark', '')  # from csv_bank (shortest variant)
norm_sm = normalize_shelfmark(shelfmark)
cam_manifest_url = crossref_svc.get_cambridge_manifest(norm_sm)
```

The `get_cambridge_manifest()` does an exact match on `normalized_shelfmark` column.

**Mismatch demonstration (test shelfmark Mosseri VI 108, sys_id 990053803330205171):**

| Step | Value |
|------|-------|
| csv_bank shelfmark (shortest) | `Ms. VI 108` |
| `normalize_shelfmark('Ms. VI 108')` | `vi108` |
| Crossref `normalized_shelfmark` for `MS-MOSSERI-VI-00108` | `mosserivi108` |
| **Match?** | **NO** -- "mosseri" prefix missing |

The `normalize_shelfmark()` function strips the "ms" prefix (line 160: `if cleaned.startswith("ms")`), so `Ms. VI 108` becomes `vi108`. The crossref import preserved the full collection name in the normalized form (`mosserivi108`).

**Confidence: HIGH** -- verified by running normalize_shelfmark on real data and comparing with crossref DB.

### 3. Coverage Analysis of Fix Approaches

**Approach A: Prefix "mosseri" to normalized shortest shelfmark**
- Hit rate: 1,954/3,194 = **61.2%**
- Fails for 2nd-series shelfmarks (Ms. L, Ms. P, Ms. C, Ms. A, etc.) where shortest variant doesn't contain a Roman numeral series

**Approach B: Construct CUDL label from shortest variant only**
- Hit rate: 1,957/3,194 = **61.3%**
- Same limitation as Approach A

**Approach C: Construct CUDL label from ALL call_number variants** (RECOMMENDED)
- Hit rate: 3,141/3,194 = **98.3%**
- 53 remaining misses are sub-fragment edge cases that genuinely don't have CUDL entries

The 39% gap between Approaches A/B and C comes from records where the shortest call_number variant is a "2nd Series" designation (e.g., "Ms. L 241", "Ms. P 59") that uses a single letter instead of a Roman numeral. These records also have a Roman numeral variant (e.g., "Mosseri, Jacques Ms. VII 173.3") that maps correctly to CUDL labels. We need access to all variants.

**Confidence: HIGH** -- verified by testing all 3,194 Mosseri records against the crossref DB.

### 4. Mosseri CUDL Label Format

CUDL labels follow the pattern: `MS-MOSSERI-{SERIES}-{PADDED_NUM}[-{PADDED_SUB}][-{LETTER}]`

| Shelfmark | CUDL Label | Notes |
|-----------|------------|-------|
| `Ms. VI 108` | `MS-MOSSERI-VI-00108` | Basic |
| `Moss. VI,129.3` | `MS-MOSSERI-VI-00129-00003` | Sub-fragment |
| `Ms. III 27O` | `MS-MOSSERI-III-00027-O` | Letter suffix |
| `Ms. III 145.3C` | `MS-MOSSERI-III-00145-00003-C` | Sub + letter |
| `Ms. IIIa 15` | `MS-MOSSERI-IIIA-00015` | Series with 'a' suffix |
| `Ms. VIII 179.2B` | `MS-MOSSERI-VIII-00179-00002-B` | Full complex |

Mosseri CUDL series: I, IA, II, III, IIIA, IV, V, VI, VII, VIII, IX, X.

**Conversion regex:** `(?:Ms\.?|Moss\.?)\s*([IVXL]+[a-z]?)\s*,?\s*(\d+)(?:\.(\d+))?([A-Z])?$`
- Group 1: Series (e.g., "VI", "IIIa" -> uppercase to "IIIA")
- Group 2: Number -> zero-pad to 5 digits
- Group 3: Sub-fragment number (optional) -> zero-pad to 5 digits
- Group 4: Letter suffix (optional) -> uppercase

**Confidence: HIGH** -- all 6 test cases verified against actual crossref entries.

### 5. Current enrich_metadata Image Source Chain

Priority order (lines 3269-3392):

1. **MARC external_iiif_link** (e.g., CUDL link from MARC record) -- line 3274
2. **Crossref Cambridge manifest** (normalized shelfmark lookup) -- line 3287
3. **Manchester LUNA canvases** (crossref multi-image) -- line 3300
4. **JTS Figgy manifest** (crossref) -- line 3312
5. **External IIIF fetch** (for any ext_link found above) -- line 3323
6. **Oxford Part images** (if no ext_link at all) -- line 3332
7. **NLI IIIF manifest** (always fetched in parallel) -- line 3367

**Where Mosseri CUDL should slot in:** Between step 2 and step 3. When the normal crossref lookup fails AND `lib_code == 'Mosseri'`, try constructing the CUDL label from the shelfmark. This keeps the existing crossref path as primary and adds Mosseri as a targeted fallback.

**Confidence: HIGH** -- read directly from genizah_core.py.

### 6. Implementation Approach

**Recommended: Two changes to genizah_core.py**

**Change 1: Add `construct_mosseri_cudl_label()` function**
A helper that takes a Mosseri shelfmark variant and returns the CUDL label string, or None if it doesn't match the expected pattern.

**Change 2: Add Mosseri CUDL fallback in `enrich_metadata`**
After the crossref Cambridge lookup fails (line 3297), if `lib_code == 'Mosseri'`:
1. Get all call_number variants (requires storing `call_numbers_raw` in csv_bank for Mosseri records)
2. For each variant, try `construct_mosseri_cudl_label()`
3. If a label is produced, use `get_cambridge_manifest_by_label()` to look up the manifest URL
4. If found, set `ext_link`, `external_url`, and `external_provider = 'cambridge'`

**Change 3: Store full call_numbers for Mosseri in csv_bank**
In `_load_csv_bank()` (line 2847), when `library_code == 'Mosseri'`, also store the raw call_numbers string. This adds ~3,194 strings (negligible memory vs. storing for all 217K records at +18.6MB).

**Alternative: Construct CUDL URL directly without crossref verification**
Instead of looking up the label in the crossref, construct the IIIF manifest URL directly: `http://cudl.lib.cam.ac.uk/iiif/{label}`. Pro: no crossref lookup needed. Con: may produce 404s for the 1.7% of records that don't have CUDL manifests. Since `fetch_external_iiif_data()` already handles failed fetches gracefully, this is acceptable.

**Recommendation: Use the direct URL construction approach.** It is simpler (no crossref query needed), covers the same cases, and the existing error handling in `fetch_external_iiif_data()` deals with invalid URLs. The crossref lookup can remain as the primary path for T-S shelfmarks.

### 7. Test Verification

| sys_id | Shelfmark | Expected CUDL Label | Expected Manifest URL |
|--------|-----------|--------------------|-----------------------|
| 990053803330205171 | Ms. VI 108 | MS-MOSSERI-VI-00108 | `http://cudl.lib.cam.ac.uk/iiif/MS-MOSSERI-VI-00108` |
| 990053803470205171 | Ms. VI 129.3 / Moss. VI,129.3 | MS-MOSSERI-VI-00129-00003 | `http://cudl.lib.cam.ac.uk/iiif/MS-MOSSERI-VI-00129-00003` |

Both verified as existing in the crossref DB.

## Common Pitfalls

### Pitfall 1: Shortest shelfmark loses collection identity
**What goes wrong:** csv_bank stores the shortest call_number variant. For Mosseri, the shortest is often `Ms. {SERIES} {NUM}` which loses the "Mosseri" prefix entirely.
**How to avoid:** Use `library_code` from csv_bank to identify Mosseri records, not the shelfmark text.

### Pitfall 2: 2nd-Series designators don't map to CUDL series
**What goes wrong:** Mosseri has a "2nd Series" naming system (L, A, C, P, Ch, Ph, T, S, G, V) that is NOT a CUDL series. Constructing a label from `Ms. L 241` produces `MS-MOSSERI-L-00241` which doesn't exist.
**How to avoid:** The regex for label construction should only match Roman numeral series ([IVXL]+). Single-letter 2nd-series designators naturally fail the regex and fall through to try the next call_number variant.

### Pitfall 3: The "ms" prefix stripping in normalize_shelfmark
**What goes wrong:** `normalize_shelfmark()` strips a leading "ms" from the cleaned string. This is correct for Oxford shelfmarks but destructive for Mosseri when the full form like "Mosseri, Jacques Ms." is used.
**How to avoid:** Don't modify normalize_shelfmark. Use direct CUDL label construction instead, which bypasses normalization entirely.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | `pytest.ini` |
| Quick run command | `pytest tests/ -x -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CUDL-01 | construct_mosseri_cudl_label converts standard patterns correctly | unit | `pytest tests/test_genizah_core.py -x -k mosseri_cudl` | No - Wave 0 |
| CUDL-02 | enrich_metadata discovers CUDL images for Mosseri records | integration | Manual - requires network call | No |
| CUDL-03 | Non-Mosseri records are unaffected by the change | unit | `pytest tests/test_genizah_core.py -x -k normalize_shelfmark` | Yes (existing) |

### Wave 0 Gaps
- [ ] `tests/test_genizah_core.py::test_construct_mosseri_cudl_label_*` -- unit tests for label construction
- [ ] No new fixtures needed -- tests are pure function tests

## Sources

### Primary (HIGH confidence)
- `nli_data/nli_crossref.db` cambridge_manifests table -- direct SQL queries for all statistics
- `genizah_core.py` lines 125-163 (normalize_shelfmark), 2803-2853 (_load_csv_bank), 3239-3438 (enrich_metadata)
- `shared/nli_crossref_service.py` lines 290-336 (get_cambridge_manifest, get_cambridge_manifest_by_label)
- `libraries.csv` -- raw shelfmark data for all 3,194 Mosseri records
- `docs/plans/EXTERNAL_DATA_INTEGRATION_EXPLORATION.md` -- CUDL collection statistics and label format documentation

## Metadata

**Confidence breakdown:**
- Root cause diagnosis: HIGH -- verified with SQL queries and normalize_shelfmark execution
- Coverage statistics: HIGH -- tested all 3,194 records programmatically
- Label construction rules: HIGH -- verified against crossref DB for all pattern variants
- Implementation approach: HIGH -- based on reading actual enrich_metadata code

**Research date:** 2026-03-18
**Valid until:** Indefinitely (stable infrastructure, no external API changes expected)
