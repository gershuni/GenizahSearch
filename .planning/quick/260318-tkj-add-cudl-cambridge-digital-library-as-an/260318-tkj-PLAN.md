---
phase: quick-260318-tkj
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_core.py
  - tests/test_mosseri_cudl.py
  - docs/OPEN_ISSUES.md
autonomous: true
requirements: [CUDL-01, CUDL-02, CUDL-03]
must_haves:
  truths:
    - "Mosseri manuscripts show CUDL high-res images in browse/ResultDialog/puzzle"
    - "construct_mosseri_cudl_label correctly maps all 6 shelfmark patterns to CUDL labels"
    - "Non-Mosseri records are completely unaffected by the change"
    - "98%+ of Mosseri records resolve to CUDL manifests (3,141/3,194)"
  artifacts:
    - path: "genizah_core.py"
      provides: "construct_mosseri_cudl_label() function + Mosseri CUDL fallback in enrich_metadata + call_numbers_raw in csv_bank for Mosseri"
      contains: "construct_mosseri_cudl_label"
    - path: "tests/test_mosseri_cudl.py"
      provides: "Unit tests for CUDL label construction"
      contains: "test_construct_mosseri_cudl_label"
  key_links:
    - from: "genizah_core.py enrich_metadata"
      to: "construct_mosseri_cudl_label"
      via: "Mosseri fallback after crossref lookup fails"
      pattern: "lib_code.*Mosseri.*construct_mosseri_cudl_label"
    - from: "genizah_core.py enrich_metadata"
      to: "nli_crossref_service.get_cambridge_manifest_by_label"
      via: "label lookup in crossref sidecar"
      pattern: "get_cambridge_manifest_by_label"
---

<objective>
Add CUDL (Cambridge Digital Library) as an image source for the Mosseri collection in enrich_metadata, so that Mosseri manuscripts display high-res IIIF images from CUDL instead of only low-res NLI Rosetta thumbnails.

Purpose: 3,194 Mosseri records currently have no CUDL images despite CUDL hosting 3,883 manifests for them. The crossref lookup fails because the shortest shelfmark variant loses the "Mosseri" prefix during normalization. This fix adds a targeted fallback that constructs the CUDL label from all call_number variants.

Output: Updated genizah_core.py with construct_mosseri_cudl_label() and Mosseri CUDL fallback in enrich_metadata, unit tests, closed open issue.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/quick/260318-tkj-add-cudl-cambridge-digital-library-as-an/260318-tkj-CONTEXT.md
@.planning/quick/260318-tkj-add-cudl-cambridge-digital-library-as-an/260318-tkj-RESEARCH.md
@genizah_core.py (lines 125-163: normalize_shelfmark, lines 2803-2853: _load_csv_bank, lines 3239-3400: enrich_metadata)
@shared/nli_crossref_service.py (lines 290-336: get_cambridge_manifest, get_cambridge_manifest_by_label)

<interfaces>
<!-- Key types and contracts the executor needs. Extracted from codebase. -->

From genizah_core.py line 125:
```python
def normalize_shelfmark(shelfmark: str) -> str:
    """Normalize shelfmarks for consistent matching across the codebase."""
    # Strips "ms" prefix, removes non-alphanumeric, preserves dots between digits
```

From genizah_core.py line 2847 (csv_bank entry structure):
```python
self.csv_bank[sys_id] = {
    'shelfmark': shelf,       # shortest call_number variant
    'title': title,
    'oxford_part_id': oxford_part_id,
    'library_code': library_code,
}
```

From shared/nli_crossref_service.py line 314:
```python
def get_cambridge_manifest_by_label(self, label: str) -> Optional[str]:
    """Get Cambridge IIIF manifest URL by CUDL label (e.g., 'MS-TS-00006-F-00001')."""
    # SELECT manifest_url FROM cambridge_manifests WHERE label = ?
```

From genizah_core.py line 3287-3297 (crossref Cambridge fallback — Mosseri insert point):
```python
# 2a-supplement: if MARC didn't provide a CUDL link, try crossref sidecar
if not ext_link and crossref_svc and crossref_svc.is_available():
    shelfmark = current_meta.get('shelfmark', '')
    if shelfmark:
        norm_sm = normalize_shelfmark(shelfmark)
        cam_manifest_url = crossref_svc.get_cambridge_manifest(norm_sm)
        if cam_manifest_url:
            ext_link = cam_manifest_url
            current_meta['external_url'] = ext_link
            current_meta['external_provider'] = 'cambridge'
            LOGGER.info(f"Using local Cambridge manifest for {system_id} from crossref sidecar")
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add construct_mosseri_cudl_label() and unit tests</name>
  <files>genizah_core.py, tests/test_mosseri_cudl.py</files>
  <behavior>
    - construct_mosseri_cudl_label("Ms. VI 108") == "MS-MOSSERI-VI-00108"
    - construct_mosseri_cudl_label("Moss. VI,129.3") == "MS-MOSSERI-VI-00129-00003"
    - construct_mosseri_cudl_label("Ms. III 27O") == "MS-MOSSERI-III-00027-O"
    - construct_mosseri_cudl_label("Ms. III 145.3C") == "MS-MOSSERI-III-00145-00003-C"
    - construct_mosseri_cudl_label("Ms. IIIa 15") == "MS-MOSSERI-IIIA-00015"
    - construct_mosseri_cudl_label("Ms. VIII 179.2B") == "MS-MOSSERI-VIII-00179-00002-B"
    - construct_mosseri_cudl_label("Mosseri, Jacques Ms. VII 173.3") == "MS-MOSSERI-VII-00173-00003"
    - construct_mosseri_cudl_label("T-S 12.123") returns None (not Mosseri)
    - construct_mosseri_cudl_label("Ms. L 241") returns None (single letter = 2nd-series, not Roman numeral)
    - construct_mosseri_cudl_label("") returns None
    - construct_mosseri_cudl_label(None) returns None
  </behavior>
  <action>
    1. Create `tests/test_mosseri_cudl.py` with a `TestConstructMosseriCudlLabel` class containing tests for all behaviors above. Import `construct_mosseri_cudl_label` from `genizah_core`.

    2. Add `construct_mosseri_cudl_label(shelfmark: str) -> Optional[str]` as a module-level function in `genizah_core.py`, placed right after `normalize_shelfmark()` (after line 163).

    The function logic:
    - Return None if input is None or empty
    - Use regex: `(?:Mosseri.*?)?(?:Ms\.?|Moss\.?)\s*([IVXL]+[a-z]?)\s*,?\s*(\d+)(?:\.(\d+))?([A-Z])?$` (case-insensitive match, but capture groups retain case)
    - The regex MUST match Roman numeral series only (I, V, X, L and combos, optionally followed by a single lowercase letter like 'a'). Single standalone letters (L, A, C, P, etc. from 2nd-series) will NOT match because they appear alone without preceding I/V/X context — EXCEPT "L" which IS a valid Roman numeral. To disambiguate: "L" followed by just a number means 2nd-series (e.g., "Ms. L 241"). Roman numeral series in Mosseri CUDL are specifically: I, IA, II, III, IIIA, IV, V, VI, VII, VIII, IX, X. Use a whitelist check on the captured series instead of purely regex.
    - Valid CUDL series whitelist: {"I", "IA", "II", "III", "IIIA", "IV", "V", "VI", "VII", "VIII", "IX", "X"}
    - Series group: uppercase, e.g., "IIIa" -> "IIIA"
    - Number group: zero-pad to 5 digits
    - Sub-fragment group (optional): zero-pad to 5 digits
    - Letter suffix group (optional): uppercase
    - Construct: `MS-MOSSERI-{SERIES}-{PADDED_NUM}[-{PADDED_SUB}][-{LETTER}]`

    3. Run tests: `pytest tests/test_mosseri_cudl.py -x -v`
  </action>
  <verify>
    <automated>pytest tests/test_mosseri_cudl.py -x -v</automated>
  </verify>
  <done>construct_mosseri_cudl_label handles all 6 documented CUDL label patterns plus the "Mosseri, Jacques" long form, rejects non-Mosseri shelfmarks and 2nd-series designators, all tests pass.</done>
</task>

<task type="auto">
  <name>Task 2: Wire Mosseri CUDL fallback into enrich_metadata and store call_numbers_raw</name>
  <files>genizah_core.py, docs/OPEN_ISSUES.md</files>
  <action>
    **Change 1: Store raw call_numbers for Mosseri in csv_bank (line ~2847)**

    In `_load_csv_bank()`, after the shelf-picking loop (line 2836) and before the `self.csv_bank[sys_id] = {` assignment (line 2847), add:

    ```python
    # Store all call_number variants for Mosseri (needed for CUDL label construction)
    call_numbers_raw = None
    if library_code == 'Mosseri':
        call_numbers_raw = [s.strip() for s in raw_shelves if s.strip()]
    ```

    Then add `'call_numbers_raw': call_numbers_raw` to the csv_bank dict assignment. This adds ~3,194 lists (negligible memory vs 217K records).

    **Change 2: Add Mosseri CUDL fallback in enrich_metadata (after line 3297)**

    After the existing crossref Cambridge manifest lookup (line 3297), and BEFORE the Manchester block (line 3299), add a new block:

    ```python
    # 2a-mosseri: if crossref didn't find Cambridge manifest and this is Mosseri, try CUDL label construction
    if not ext_link and crossref_svc and crossref_svc.is_available():
        lib_code = current_meta.get('lib_code') or self.csv_bank.get(system_id, {}).get('library_code', '')
        if lib_code == 'Mosseri':
            # Try all call_number variants for best CUDL match
            variants = self.csv_bank.get(system_id, {}).get('call_numbers_raw') or [current_meta.get('shelfmark', '')]
            for variant in variants:
                label = construct_mosseri_cudl_label(variant)
                if label:
                    cam_url = crossref_svc.get_cambridge_manifest_by_label(label)
                    if cam_url:
                        ext_link = cam_url
                        current_meta['external_url'] = ext_link
                        current_meta['external_provider'] = 'cambridge'
                        LOGGER.info(f"Using Mosseri CUDL manifest for {system_id}: {label}")
                        break
    ```

    This iterates all call_number variants, tries to construct a CUDL label from each, and looks it up in the crossref sidecar. It stops at the first successful match.

    **Change 3: Mark open issue as fixed in docs/OPEN_ISSUES.md (line 88)**

    Change the Mosseri CUDL open issue status from `❌ Open` to `✅ Fixed (2026-03-18)` and update the notes to explain the fix: "Added construct_mosseri_cudl_label() that converts Mosseri shelfmark variants to CUDL labels (MS-MOSSERI-{SERIES}-{NUM}), wired as fallback in enrich_metadata after crossref normalized-shelfmark lookup fails. Covers 98.3% of 3,194 Mosseri records."
  </action>
  <verify>
    <automated>pytest tests/test_mosseri_cudl.py -x -v && pytest tests/ -x -q --timeout=30 2>/dev/null; echo "Exit: $?"</automated>
  </verify>
  <done>Mosseri records in enrich_metadata now discover CUDL manifests via label construction fallback. csv_bank stores all call_number variants for Mosseri records. Open issue marked as fixed. All existing tests still pass.</done>
</task>

</tasks>

<verification>
1. Unit tests pass: `pytest tests/test_mosseri_cudl.py -x -v`
2. Existing test suite unaffected: `pytest tests/ -x -q`
3. Manual spot-check (optional): In web app, browse to a Mosseri manuscript (e.g., sys_id 990053803330205171 "Ms. VI 108") and verify CUDL high-res images appear instead of only NLI thumbnails
</verification>

<success_criteria>
- construct_mosseri_cudl_label() correctly handles all documented pattern variants
- enrich_metadata discovers CUDL images for Mosseri records that previously had none
- Non-Mosseri records are completely unaffected (no code path changes for them)
- Open issue in docs/OPEN_ISSUES.md marked as fixed
- All tests pass (new + existing)
</success_criteria>

<output>
After completion, create `.planning/quick/260318-tkj-add-cudl-cambridge-digital-library-as-an/260318-tkj-SUMMARY.md`
</output>
