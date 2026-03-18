# Research: Library Attribution Credit Lines

## Root Cause

Attribution flows through three layers:
1. **genizah_core.py `enrich_metadata()`** — sets `current_meta['attribution']`
2. **web/services.py `get_browse_page()`** / `get_browse_page_by_fl()` — reads from nli_cache, applies defaults
3. **web/pages/browse.py** (line 4168-4186) / **genizah_app.py** (line 1728-1733) — renders the text

### The Bug

The attribution fallback chain always ends at NLI:

```
genizah_core.py line 3380-3383:
    if marc_attribution:
        current_meta['attribution'] = marc_attribution      # From NLI MARC
    elif not current_meta.get('attribution'):
        current_meta['attribution'] = nli_iiif_data.get('attribution', '')  # "מאוסף הספרייה הלאומית"

web/services.py line 282-284:
    if not attribution:
        attribution = 'הספרייה הלאומית / National Library of Israel'  # Hard default
```

For **Manchester**: The new `get_manchester_canvases()` sets `external_provider='manchester'` but never sets `attribution`. Since `ext_link = '__manchester_direct__'` skips `fetch_external_iiif_data()`, the Manchester IIIF manifest attribution is never fetched.

For **Cambridge**: `fetch_external_iiif_data()` extracts the `attribution` field from the Cambridge IIIF manifest. This works correctly — Cambridge manifests return "Reproduced by kind permission of the Syndics of Cambridge University Library" or similar.

For **JTS**: Same as Cambridge — `fetch_external_iiif_data()` extracts attribution from Figgy manifest.

For **Oxford**: Special-cased at web/services.py line 274-275: `'From the collections of the Bodleian Libraries, Oxford'`.

For **NLI-hosted images of other libraries (RNL, AIU, BL, etc.)**: All show NLI attribution because images come from NLI's IIIF. The physical collections belong to other libraries but NLI digitized them via the Friedberg Genizah Project. The NLI attribution is technically correct for the *digital images*, but the credit should ideally acknowledge the holding institution too.

## Fix Strategy

### Layer 1: Set attribution in genizah_core.py for Manchester

In the Manchester block (line 3299-3308), after setting `images_ext` and `external_provider`, also set:
```python
current_meta['attribution'] = 'The University of Manchester Library. CC BY-NC-SA 4.0'
```

### Layer 2: Add library-aware attribution in web/services.py

Replace the hard NLI default (lines 282-284, 468-470) with a library-code-aware lookup. Create an `ATTRIBUTION_BY_LIBRARY` dict:

```python
ATTRIBUTION_BY_LIBRARY = {
    'Manchester': 'The University of Manchester Library. CC BY-NC-SA 4.0',
    'Oxford': 'Bodleian Libraries, University of Oxford. CC BY-NC 4.0',
    'CUL': '',  # Comes from IIIF manifest — don't override
    'JTS': '',  # Comes from IIIF manifest — don't override
    'BL': 'British Library',
    'RNL': 'National Library of Russia',
    'AIU': 'Alliance Israélite Universelle',
}
```

Fallback chain: IIIF manifest attribution → library-specific → NLI default.

### Layer 3: Fix web display link per provider

browse.py line 4175-4186 only special-cases Oxford for linking. Should also link:
- Manchester → luna.manchester.ac.uk
- Cambridge → cudl.lib.cam.ac.uk
- JTS → dpul.princeton.edu
- All others → NLI ktiv link

The `page.external_provider` field already carries 'manchester', 'jts', or '' (Cambridge).

### Layer 4: Desktop — no code change needed
Desktop just displays `meta['attribution']` from the cache. Fixing Layer 1 fixes desktop.

## Library Attribution Text (Researched)

| Library | English | Hebrew | License | Source |
|---------|---------|--------|---------|--------|
| NLI | National Library of Israel | הספרייה הלאומית | Public domain (pre-1900 manuscripts) | IIIF manifest |
| CUL | Cambridge University Library | ספריית אוניברסיטת קיימברידג׳ | CC BY-NC 3.0 | IIIF manifest returns it |
| Manchester | The University of Manchester Library | ספריית אוניברסיטת מנצ׳סטר | CC BY-NC-SA 4.0 | User confirmed from website |
| Oxford | Bodleian Libraries, University of Oxford | ספריות בודלי, אוניברסיטת אוקספורד | CC BY-NC 4.0 | Verified on digital.bodleian.ox.ac.uk |
| JTS | Jewish Theological Seminary | בית המדרש לרבנים של אמריקה | Varies | IIIF manifest returns it |
| BL | British Library | הספרייה הבריטית | Varies | No IIIF — NLI digitized |
| RNL | National Library of Russia | הספרייה הלאומית של רוסיה | Varies | No IIIF — NLI digitized |
| AIU | Alliance Israélite Universelle | אליאנס ישראלית אוניברסלית | Varies | No IIIF — NLI digitized |

## Key Insight

For libraries whose images come via NLI (BL, RNL, AIU, Gaster, Mosseri, etc.), the credit should say:
**"[Library Name] / image: National Library of Israel"** — acknowledging both the holding institution and the digitization source.

## Files to Modify

1. **genizah_core.py** ~line 3305 — set `current_meta['attribution']` for Manchester
2. **web/services.py** lines 271-284 and 457-470 — library-aware attribution lookup
3. **web/pages/browse.py** lines 4168-4186 — per-provider credit link
4. **genizah_translations.py** — add Hebrew translations for attribution strings (if needed)

## Existing Infrastructure

- `LIBRARY_CODES` dict (genizah_core.py:1531-1594) — maps library_code → English name
- `BrowsePage.external_provider` — 'manchester', 'jts', '' (Cambridge)
- `BrowsePage.library_code` — the library abbreviation
- `page.is_oxford` — Oxford detection flag
