---
phase: quick
plan: 260318-jyz
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_core.py
  - web/services.py
  - web/pages/browse.py
autonomous: true
requirements: [QUICK-FIX]
---

<objective>
Fix library attribution credit lines: currently all non-Oxford manuscripts show "מאוסף הספרייה הלאומית"
(NLI default), even for Manchester, Cambridge, BL, RNL, etc. Each library should show proper
attribution acknowledging the holding institution. Manchester in particular shows NLI credit because
the new get_manchester_canvases() bypasses IIIF manifest fetch where attribution normally comes from.

Output: Correct per-library attribution text in both web and desktop, with correct credit links in web.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_core.py (lines 3299-3308: Manchester canvases block where attribution should be set; lines 3380-3383: attribution fallback chain; lines 1531-1594: LIBRARY_CODES dict)
@web/services.py (lines 271-284: get_browse_page attribution; lines 457-470: get_browse_page_by_fl attribution — TWO identical code paths)
@web/pages/browse.py (lines 4168-4186: credit footer rendering with link)
@.planning/quick/260318-jyz-fix-library-attribution-credit-lines-per/260318-jyz-RESEARCH.md (Full research findings)

<interfaces>
BrowsePage fields available for routing:
- page.attribution: str — the credit text to display
- page.external_provider: str — 'manchester', 'jts', or '' (Cambridge uses page.is_cambridge)
- page.library_code: str — library abbreviation from libraries.csv (e.g., 'CUL', 'Manchester', 'BL')
- page.is_oxford: bool — True for Oxford/Bodleian manuscripts
- page.is_cambridge: bool — True for Cambridge manuscripts
- page.sys_id: str — NLI system ID

LIBRARY_CODES dict (genizah_core.py:1531) maps library_code → English name.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Set Manchester attribution in genizah_core.py and add library-aware attribution in web/services.py</name>
  <files>genizah_core.py, web/services.py</files>
  <action>
**1a. genizah_core.py — Manchester attribution (line ~3305)**

Inside the Manchester canvases block (after `current_meta['external_provider'] = 'manchester'`), add:
```python
current_meta['attribution'] = 'The University of Manchester Library · CC BY-NC-SA 4.0'
```

**1b. web/services.py — Library-aware attribution fallback**

Add an `ATTRIBUTION_BY_LIBRARY` dict near the top of the file (after imports). This maps library_code
to a proper attribution string. `None` means "use IIIF manifest attribution" (already working).
Missing keys fall through to NLI default.

```python
# Library-specific attribution text for image credit lines.
# None = attribution comes from IIIF manifest (don't override).
# Missing key = NLI default (manuscript digitized by NLI, no other source).
ATTRIBUTION_BY_LIBRARY = {
    'CUL': None,        # Cambridge IIIF manifest provides attribution
    'JTS': None,        # JTS/Princeton Figgy manifest provides attribution
    'Manchester': 'The University of Manchester Library · CC BY-NC-SA 4.0',
    'Oxford': 'Bodleian Libraries, University of Oxford · CC BY-NC 4.0',
    'BL': 'British Library · image: הספרייה הלאומית',
    'RNL': 'National Library of Russia · image: הספרייה הלאומית',
    'AIU': 'Alliance Israélite Universelle · image: הספרייה הלאומית',
    'Mosseri': 'Mosseri Collection · image: הספרייה הלאומית',
    'Gaster': 'Gaster Collection · image: הספרייה הלאומית',
    'Halper': 'Halper Collection · image: הספרייה הלאומית',
    'Westminster': 'Westminster College · image: הספרייה הלאומית',
    'Freer': 'Freer Gallery of Art · image: הספרייה הלאומית',
    'HUC': 'Hebrew Union College · image: הספרייה הלאומית',
}
```

Then replace BOTH attribution fallback blocks (lines 271-284 AND 457-470) with:

```python
# Determine attribution
attribution = ''
is_oxford = is_oxford_manuscript(shelfmark, library_code)

# 1. Try IIIF manifest attribution from cache
if actual_sys_id and hasattr(state.meta_mgr, 'nli_cache'):
    cached_meta = state.meta_mgr.nli_cache.get(actual_sys_id, {})
    attribution = cached_meta.get('attribution', '')

# 2. Library-specific override (if no IIIF attribution or library has hardcoded text)
if library_code in ATTRIBUTION_BY_LIBRARY:
    lib_attr = ATTRIBUTION_BY_LIBRARY[library_code]
    if lib_attr is not None:  # None means "keep IIIF manifest attribution"
        attribution = lib_attr
elif is_oxford:
    attribution = 'Bodleian Libraries, University of Oxford · CC BY-NC 4.0'

# 3. Default: NLI
if not attribution:
    attribution = 'הספרייה הלאומית / National Library of Israel'
```

NOTE: The Oxford special case (`is_oxford`) was previously at the TOP of the chain. Move it into the
library lookup for consistency, but keep the `is_oxford` detection since Oxford manuscripts use a
different code path (parts, not standard library_code). The `is_oxford_manuscript()` function detects
Oxford regardless of library_code.

IMPORTANT: Both `get_browse_page()` (line ~271) and `get_browse_page_by_fl()` (line ~457) have
identical attribution blocks. Update BOTH.
  </action>
  <verify>
    <automated>cd C:/genizahsearch && python -c "from web.services import ATTRIBUTION_BY_LIBRARY; print(f'Dict has {len(ATTRIBUTION_BY_LIBRARY)} entries'); assert ATTRIBUTION_BY_LIBRARY['Manchester'] is not None; assert ATTRIBUTION_BY_LIBRARY['CUL'] is None; print('OK')"</automated>
  </verify>
  <done>
    Manchester manuscripts show "The University of Manchester Library · CC BY-NC-SA 4.0" instead of
    NLI default. Other libraries show their proper attribution. Cambridge/JTS still use IIIF manifest.
    Desktop automatically fixed via nli_cache.
  </done>
</task>

<task type="auto">
  <name>Task 2: Per-provider credit link in web browse.py</name>
  <files>web/pages/browse.py</files>
  <action>
Replace the credit footer block at lines 4168-4186 to route links based on library source.

Current code only has two branches: Oxford → Bodleian link, else → NLI ktiv link.

New logic should route based on `page.external_provider`, `page.is_oxford`, `page.is_cambridge`,
and `page.library_code`:

```python
# === Image Credit/Attribution Footer ===
if page.attribution:
    with ui.row().classes('w-full items-center justify-center gap-2 py-2').style(
        'background: #2a2a2a; border-radius: 0 0 8px 8px; border-top: 1px solid #333;'
    ):
        ui.icon('photo_library', size='xs').style('color: #888; font-size: 14px;')
        credit_text = page.attribution

        # Determine credit link based on image source
        if page.is_oxford:
            credit_link = 'https://digital.bodleian.ox.ac.uk/'
        elif page.external_provider == 'manchester':
            credit_link = 'https://luna.manchester.ac.uk/'
        elif page.is_cambridge:
            credit_link = 'https://cudl.lib.cam.ac.uk/'
        elif page.external_provider == 'jts':
            credit_link = 'https://dpul.princeton.edu/cairo_geniza'
        elif page.library_code == 'BL':
            credit_link = 'https://searcharchives.bl.uk/'
        else:
            # Default: NLI ktiv
            credit_link = f'https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}'

        with ui.link(target=credit_link, new_tab=True).style('text-decoration: none;'):
            ui.label(credit_text).classes('text-xs').style(
                'color: #aaa; font-style: italic;'
            )
```

This replaces the existing if/else block with a multi-branch routing.
  </action>
  <verify>
    <automated>cd C:/genizahsearch && python -c "
import ast, inspect
# Verify browse.py parses without errors
with open('web/pages/browse.py', 'r', encoding='utf-8') as f:
    ast.parse(f.read())
print('browse.py parses OK')
"</automated>
  </verify>
  <done>
    Web credit footer links to the correct library website based on image source.
    Manchester → luna.manchester.ac.uk, Cambridge → cudl.lib.cam.ac.uk, etc.
  </done>
</task>

</tasks>

<verification>
1. Verify web/services.py imports and dict: `python -c "from web.services import ATTRIBUTION_BY_LIBRARY; print(ATTRIBUTION_BY_LIBRARY)"`
2. Verify no Python syntax errors: `python -m py_compile genizah_core.py && python -m py_compile web/services.py && python -m py_compile web/pages/browse.py`
3. Run existing tests: `python -m pytest tests/ -x --timeout=60 -q`
4. Manual: Browse to Manchester manuscript (sys_id 990002081410205171) — should show "The University of Manchester Library · CC BY-NC-SA 4.0" with link to luna.manchester.ac.uk
</verification>

<output>
After completion, create `.planning/quick/260318-jyz-fix-library-attribution-credit-lines-per/260318-jyz-SUMMARY.md`
</output>
