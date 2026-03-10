# Code Review: Branch `46-translation-wiring-round2` vs `master-main`

**Date:** 2026-03-10
**Reviewer:** Claude Opus 4.6
**Branch:** 46-translation-wiring-round2 (22 commits, ~4,769 additions / 1,621 deletions, 46 files)

## Summary

Major themes:
1. **Translation wiring** — language-aware title/description display across all pages (web + desktop)
2. **Removal of on-demand translate** — replaced `create_translatable_text()` with pre-computed batch translations
3. **Catalog dialog translations** — FJMS RunningTitle, FreeDesc, FullText with toggle badges
4. **Citation reminder popup** — one-time dialog for both apps
5. **Help page updates** — new sections for filters and translations
6. **Batch translation scripts** — FJMS, Oxford, library titles EN→HE
7. **Bug fixes** — Supabase URL fallback, HTML stripping from IIIF, shelfmark mismatch fix

---

## Critical Issues

### 1. HTML Injection in desktop catalog dialog (`genizah_app.py`)
**Severity: High**

In `FjmsCatalogDialog._build_html()`, translated text from the FJMS database is inserted directly into HTML without escaping:

```python
# ~line 6835+ (free descriptions section)
display = (
    f'{show_text} '
    f'<a href="cat-toggle:{toggle_key}" style="{_badge_style}">{badge_label}</a>'
)
```

`show_text` is raw translation text from SQLite — never passed through `html.escape()`. Same pattern for RunningTitle and FullText sections. If any translation contains `<script>` or HTML tags, it would be rendered by QTextBrowser.

**Fix:** Wrap `show_text` (and `orig` where used as display text) with `html.escape()` before insertion. The original code before this branch also had this issue for `str(text).strip()` in the same sections, but this branch adds new paths that also skip escaping.

### 2. `_md_show_trans` referenced but never set in parallels metadata (`web/pages/parallels.py`)

At the parallels metadata dialog PGP description section:
```python
if _md_show_trans and get_language() == 'he' and sys_id and p_state.translation_data:
```

But the variable `_md_show_trans` was deleted earlier in the same function (the `try/except` block reading `app.storage.user.get('show_translations')` was removed). This will raise a `NameError` at runtime when a user opens the metadata dialog in parallels.

**Fix:** Either re-add the `_md_show_trans` assignment or use `_par_show_trans` which is set at the outer scope.

---

## Moderate Issues

### 3. Repeated `TranslationService` instantiation pattern
Throughout the web codebase (browse.py, search.py, parallels.py, catalog_dialog.py), there's a pattern of:
```python
from shared.translation_service import TranslationService
svc = TranslationService(thread_safe=True)
# ... use svc ...
svc.close()
```

This appears 15+ times across the diff. Each instantiation opens new SQLite connections. While `thread_safe=True` makes it safe, this is wasteful. Consider a cached singleton per-thread or a connection pool.

### 4. Toggle factory functions defined inside loops without proper closure
In `browse.py` and `catalog_dialog.py`, toggle handler factories like `_make_ox_toggle`, `_make_ol_toggle`, `_make_mt_toggle` are defined inside loops or conditional blocks. While the factory pattern (returning `handler`) correctly captures variables, there's a consistency issue — some use the factory pattern correctly while the Oxford metadata loop in browse.py defines `_make_ox_toggle` inside a `for` loop but captures loop variables `_ox_eng`, `_ox_heb` via the factory parameters (correct). Just flagging that this pattern is fragile if anyone refactors without understanding the closure semantics.

### 5. Notes combined-translation approach loses individual note context (`genizah_app.py`)
```python
notes_combined = '\n'.join(notes)
notes_result = _trans_or_badge('rd_notes', notes_combined, tr('Notes'))
```

Combining all notes into one string for translation means:
- Individual notes lose their `<li>` formatting if the badge is applied
- The `chr(10)` → `<br/>` replacement is a rough approximation
- If translation fails for the combined string but would succeed for individual notes, all are lost

This is a pragmatic tradeoff (fewer API calls) but worth noting.

### 6. `_rt_en` mutation pattern in catalog_dialog.py (web)
```python
_rt_en = fjms_trans.get('RunningTitle') if not is_heb else None
for team in teams:
    ...
    if _rt_en and titles:
        rt_vals.append(_rt_en)
        _rt_en = None  # Only use for the first team with data
    else:
        rt_vals.append('; '.join(titles) if titles else None)
```

Setting `_rt_en = None` after first use means only the first team gets the English translation. If multiple teams have running titles, subsequent ones won't show translations. This may be intentional (one translation covers all) but could surprise maintainers.

---

## Minor Issues

### 7. Inconsistent direction handling
Some places use `'ltr'` / `'rtl'` string, others use empty string `''` to mean LTR. For example in `catalog_dialog.py`:
```python
display_dir = ''  # English is LTR
```
vs. in `browse.py`:
```python
_ox_dir = 'direction: rtl; text-align: right;' if (_ox_is_heb and _ox_heb) else ''
```

Not a bug, but inconsistent style.

### 8. `_has_english` threshold magic number
```python
def _has_english(t, min_latin=10):
    return sum(1 for c in t if ('A' <= c <= 'Z') or ('a' <= c <= 'z')) >= min_latin
```

The default of 10 Latin characters is reasonable but arbitrary. Very short English texts (e.g., "Psalms") would not trigger. A comment explaining the choice would help.

### 9. Dead import removal incomplete
`create_translatable_text` is removed from search.py and browse.py imports, but the file `web/components/translate_button.py` presumably still exists. If it's now completely unused, consider deleting it.

### 10. `import re as _re_meta` inside function body (`genizah_core.py`)
```python
if 'metadata' in data:
    import re as _re_meta
```
The `re` module is almost certainly imported at the top of `genizah_core.py` already. This inner import with aliasing is unnecessary.

### 11. `get_fjms_free_desc_en` called per-item instead of batch
In `web/components/catalog_dialog.py`, free descriptions are translated by calling `get_fjms_free_desc_en()` in a loop:
```python
for sid in sig_ids:
    en = _tsvc_fd.get_fjms_free_desc_en(alma_id, sid)
```
The desktop version uses batch `get_fjms_translations_by_signature_ids()`. The web version should do the same for consistency and performance.

### 12. `english_title_he` column guard is fragile
```python
self._titles_has_en_he = "english_title_he" in cols
```
Later:
```python
if self._titles_has_en_he and len(row) > 5:
    entry["english_title_he"] = row[5]
```
Relying on column position (index 5) after dynamically including the column in SELECT is correct but fragile. Using `sqlite3.Row` dict access would be safer.

---

## Positive Observations

- **Good removal of `translated_match` feature** — the "translated match" badge approach was removed cleanly from both web and desktop, eliminating a confusing UX pattern
- **Proper factory pattern for NiceGUI toggles** — the `_make_*_toggle` factories correctly capture variables by parameter, avoiding the classic loop-closure bug
- **Thread-safety awareness** — `app.storage.user` reads are correctly moved to the main thread before entering `run.io_bound()`
- **Batch translation lookups** — shifting from per-item to batch SQLite queries is a solid performance improvement
- **Citation reminder** — clean implementation with per-machine persistence (localStorage on web, app config on desktop)
- **bump_version.py** is well-structured with dry-run support and manual step reminders

---

## Recommendations

1. **Fix the `_md_show_trans` NameError in parallels.py** — this will crash at runtime
2. **HTML-escape translated text in desktop catalog dialog** before inserting into QTextBrowser HTML
3. **Consider deleting `web/components/translate_button.py`** if `create_translatable_text` is no longer imported anywhere
4. **Use batch lookup for web free description translations** in `catalog_dialog.py` instead of per-item loop
5. **Remove the inner `import re as _re_meta`** in genizah_core.py — use the existing top-level import
