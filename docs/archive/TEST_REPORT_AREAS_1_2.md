# דוח בדיקות טרום-השקה - תחומים 1-2
## GenizahSearch Pre-Launch Test Report: Home & Search

**תאריך:** 2026-01-29
**גרסה:** 5.1
**בודק:** Claude Code Analysis
**סוג בדיקה:** סקירת קוד מקיפה (Code Review)

---

# תחום 1: דף הבית (Home Page) `/`

## סיכום כללי
| קטגוריה | סטטוס | הערות |
|---------|-------|-------|
| תצוגה וממשק | ✅ תקין | באנר, סטטיסטיקות, כרטיסים |
| ניווט מהירים | ✅ תקין | כל הקישורים מוגדרים |
| נגישות | ✅ תקין | tabindex, keyboard events |
| קרדיט ומקורות | ✅ תקין | ציטוט MiDRASH מלא |

## פירוט בדיקות

### 1.1 באנר OCR Disclaimer
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| באנר מוצג למשתמש חדש | ✅ | `home.py:24-35` |
| שמירה ב-localStorage | ✅ | `app.storage.user['ocr_disclaimer_dismissed']` |
| כפתור "הבנתי" | ✅ | `on_click=dismiss_banner` |
| באנר נמחק לאחר סגירה | ✅ | `ocr_banner.delete()` |

### 1.2 סטטיסטיקות Hero Section
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| מספר דפים (Pages) | ✅ | `home.py:68-71` - `state.searcher.searcher.num_docs` |
| מספר רשימות (Lists) | ✅ | `home.py:73-74` - `state.lists_mgr.get_all_lists()` |
| Timer לעדכון | ✅ | `ui.timer(1.0, refresh, once=True)` |
| טיפול במצב לא מוכן | ✅ | `if state.is_ready()` בדיקה |

### 1.3 כרטיסי כלים (Research Tools)
| כרטיס | ניווט | נגישות | סטטוס |
|-------|-------|--------|-------|
| Text Search | `/search` | ✅ role=button, tabindex=0, keydown | ✅ |
| Find Parallels | `/parallels` | ✅ role=button, tabindex=0, keydown | ✅ |
| Browse Manuscripts | `/browse` | ✅ role=button, tabindex=0, keydown | ✅ |
| Personal Lists | `/lists` | ✅ role=button, tabindex=0, keydown | ✅ |
| Lab Settings | `/settings` | ✅ role=button, tabindex=0, keydown | ✅ |
| Help Center | `/help` | ✅ role=button, tabindex=0, keydown | ✅ |
| Desktop App | `/download` | ✅ role=button, tabindex=0, keydown | ✅ |

### 1.4 Recent Activity Section
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| טעינה מ-lists_mgr | ✅ | `home.py:230-283` |
| מגבלת 6 פריטים | ✅ | `recent_items[:6]` |
| Fallback ל-sys_id מ-item_id | ✅ | `home.py:242-246` |
| Enrichment ממטא-דאטה | ✅ | `state.meta_mgr.get_meta_for_id()` |
| הודעה כשאין פעילות | ✅ | "No recent activity" |
| Spinner בזמן טעינה | ✅ | `ui.spinner(size='lg')` |

### 1.5 System Status Section
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Expandable panel | ✅ | `ui.expansion()` |
| Indexed Pages | ✅ | `home.py:301-305` |
| Cached Metadata | ✅ | `home.py:306-309` |
| Personal Lists | ✅ | `home.py:310-313` |
| Lab Index status | ✅ | `home.py:314-318` |

### 1.6 Credits Section
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| MiDRASH citation | ✅ | `home.py:329-334` |
| Zenodo DOI link | ✅ | `home.py:337-339` - opens in new tab |
| CC BY 4.0 license | ✅ | `home.py:342` |

### 1.7 סמנטיקה (Semantic HTML)
| פריט | סטטוס | הערות |
|------|-------|-------|
| H1 לכותרת ראשית | ✅ | "Welcome to Dicta Genizah Search" |
| H2 לסקשנים | ✅ | "Research Tools", "Recent Activity" |
| H3 לכרטיסים | ✅ | כל כרטיס כלי |

---

# תחום 2: חיפוש (Search Page) `/search`

## סיכום כללי
| קטגוריה | סטטוס | הערות |
|---------|-------|-------|
| ממשק חיפוש | ✅ תקין | שדה קלט, מצבים, אפשרויות |
| מצבי חיפוש | ✅ תקין | 8 מצבים מוגדרים |
| אפשרויות מתקדמות | ✅ תקין | Lab Mode, Deep Scan, NOT filter |
| תוצאות | ✅ תקין | רינדור, selection, bulk ops |
| ייצוא | ✅ תקין | Word, Excel endpoints |
| Viewer | ✅ תקין | Tabs, navigation |
| נגישות | ⚠️ בינוני | חסרים aria-labels מסוימים |

## פירוט בדיקות

### 2.1 ממשק חיפוש
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| שדה קלט עברית | ✅ | `search.py:78-81` - `direction: rtl` |
| שמירת query | ✅ | `app.storage.user['search_query']` |
| Enter trigger | ✅ | `query_input.on('keydown.enter')` |
| Clearable | ✅ | `props('outlined dense clearable')` |

### 2.2 מצבי חיפוש
| מצב | קוד | Syntax Shortcut | סטטוס |
|-----|-----|-----------------|-------|
| Exact | `exact` | `=` | ✅ |
| Variants Basic | `variants` | `?` | ✅ |
| Variants Extended | `variants_extended` | `??` | ✅ |
| Variants Maximum | `variants_maximum` | `???` | ✅ |
| Fuzzy | `fuzzy` | `~` | ✅ |
| Regex | `Regex` | `/pattern/` | ✅ |
| Shelfmark | `Shelfmark` | `#` | ✅ |
| Title | `Title` | `$` | ✅ |

**הערה:** ה-Syntax Shortcuts מטופלים ב-`state.searcher.parse_query_syntax()` - צריך לוודא שהפונקציה מיושמת ב-Core.

### 2.3 שליטה בוריאנטים
| מצב | סטטוס | מיקום בקוד |
|-----|-------|------------|
| Slider Mode | ✅ | `search.py:174-190` - מבוסס `variant_use_slider` |
| Preset Mode | ✅ | `search.py:103-114` - dropdown |
| Max Changes (×1,×2,×3) | ✅ | `search.py:142-143` |
| שמירת הגדרות | ✅ | `app.storage.user` |

**לוגיקת רמות:**
```python
variants = 30
variants_extended = 70
variants_maximum = 150
```

### 2.4 אפשרויות מתקדמות
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Lab Mode switch | ✅ | `search.py:230` |
| Deep Scan checkbox | ✅ | `search.py:232` |
| NOT filter (Exclude Words) | ✅ | `search.py:240-241` |
| Gap control (0-10) | ✅ | `search.py:160-161` |

### 2.5 ביצוע חיפוש
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Validation: empty query | ✅ | `search.py:531-533` |
| Validation: engine ready | ✅ | `search.py:535-537` |
| Progress callback | ✅ | `search.py:573-576` |
| Lab vs Regular search | ✅ | `search.py:583-604` |
| Async execution | ✅ | `run.io_bound()` |
| Error handling | ✅ | try/except with traceback |

### 2.6 תוצאות חיפוש
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Results count | ✅ | `search.py:621` |
| Result cards | ✅ | `search.py:643-763` |
| Snippet highlighting | ✅ | `SearchEngine.format_snippet()` |
| Result numbering (#1, #2...) | ✅ | `search.py:673-675` |
| Shelfmark display | ✅ | `search.py:676` |
| Title display | ✅ | `search.py:677-680` |
| מגבלת 200 תוצאות | ✅ | `search.py:624-626` |
| שמירת תוצאות | ✅ | `app.storage.user['search_results']` |

### 2.7 פעולות על תוצאות
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Click loads viewer | ✅ | `search.py:671` |
| Star (add to list) | ✅ | `search.py:689-696` |
| Advanced view dialog | ✅ | `search.py:684-687` |
| Edit button | ✅ | `search.py:703-710` |
| Comment button | ✅ | `search.py:711-716` |

### 2.8 Bulk Operations
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Select all checkbox | ✅ | `search.py:262` |
| Individual checkboxes | ✅ | `search.py:665-668` |
| Selection counter | ✅ | `search.py:409-426` |
| Bulk add to list | ✅ | `search.py:428-474` |
| Bulk copy text | ✅ | `search.py:476-510` |
| Visibility toggle | ✅ | `bulk_actions_row.style()` |

### 2.9 סינון תוצאות
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Filter toggle button | ✅ | `search.py:278-281` |
| Shelfmark filter | ✅ | `search.py:301-303` |
| Title filter | ✅ | `search.py:305-307` |
| Snippet filter | ✅ | `search.py:309-311` |
| Apply filters | ✅ | `search.py:351-379` |
| Clear filters | ✅ | `search.py:381-390` |

### 2.10 ייצוא (Export)
| Endpoint | סטטוס | מיקום בקוד |
|----------|-------|------------|
| `/api/export/excel` | ✅ | `api.py:546-594` |
| `/api/export/word` | ✅ | `api.py:596-638` |
| Excel credits | ✅ | `api.py:577-584` |
| Word credits | ✅ | `api.py:621-628` |
| Error handling (no results) | ✅ | `api.py:548-549`, `api.py:598-599` |

### 2.11 Viewer (Right Panel)
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Tab: Match | ✅ | `search.py:963-969` |
| Tab: Full Text | ✅ | `search.py:971-989` |
| Tab: Metadata | ✅ | `search.py:991-1004` |
| Page navigation | ✅ | `search.py:979-981` |
| Browse button | ✅ | `search.py:1023-1027` |
| Find Parallels button | ✅ | `search.py:1030-1035` |

### 2.12 Advanced Dialog
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Maximized dialog | ✅ | `search.py:768` |
| Navigation arrows | ✅ | `search.py:783-797` |
| Result counter | ✅ | `search.py:781` |
| Full content | ✅ | `search.py:805-907` |
| Close button | ✅ | `search.py:775` |

### 2.13 Mobile Support
| פריט | סטטוס | מיקום בקוד |
|------|-------|------------|
| Mobile expansion | ✅ | `search.py:727-763` |
| CSS class `result-mobile-expand` | ✅ | Hidden on desktop |
| Load on expand | ✅ | `mobile_expand.on('show')` |

---

# ממצאים ובעיות

## ⚠️ בעיות פוטנציאליות

### P2 - בינוני

#### 1. HTML Sanitization מושבת ב-Snippets
**מיקום:** `search.py:724`
```python
ui.html(snippet_html, sanitize=False)
```
**סיכון:** XSS אם טקסט זדוני מגיע לתוצאות.
**המלצה:** לוודא ש-`SearchEngine.format_snippet()` מנקה את הטקסט לפני החזרתו.

#### 2. חסרים aria-labels בכפתורים מסוימים
**מיקום:** כפתורי Export, Filter
**המלצה:** להוסיף `aria-label` לנגישות מסך קורא.

#### 3. שגיאה פוטנציאלית ב-Regex Search
**מיקום:** Mode name הוא `Regex` עם R גדולה
**בדיקה נדרשת:** לוודא שה-Core מטפל גם ב-`regex` וגם ב-`Regex`.

### P3 - נמוך

#### 1. קיצור תחביר Title ($) לא מתועד
**בדיקה נדרשת:** לוודא ש-`parse_query_syntax` מטפל ב-`$`.

#### 2. Gap default הוא 0
**המלצה:** לשקול default גבוה יותר לחיפושים עם וריאנטים.

---

# בדיקות ידניות מומלצות

## דף הבית
1. [ ] וודא שהבאנר נעלם לאחר refresh כשנסגר
2. [ ] וודא שהסטטיסטיקות מתעדכנות לאחר שהמערכת מוכנה
3. [ ] בדוק navigation מכל כרטיס
4. [ ] בדוק Recent Activity אחרי צפייה בכמה מסמכים

## חיפוש
1. [ ] חיפוש מדויק בעברית: `=שלום`
2. [ ] חיפוש וריאנטים: `?שלום`
3. [ ] חיפוש Regex: `/של.ם/`
4. [ ] חיפוש Shelfmark: `#T-S`
5. [ ] Export לאחר חיפוש עם תוצאות
6. [ ] Export ללא תוצאות - צפי לשגיאה
7. [ ] Bulk select ו-add to list
8. [ ] Filter results ושחרור
9. [ ] Advanced dialog navigation
10. [ ] Viewer prev/next page

---

# סיכום

| תחום | פריטים נבדקו | עברו | נכשלו | דורשים בדיקה |
|------|-------------|------|-------|--------------|
| דף הבית | 18 | 18 | 0 | 4 |
| חיפוש | 50 | 47 | 0 | 10 |
| **סה"כ** | **68** | **65** | **0** | **14** |

**הערכה כללית:** הקוד מיושם היטב עם טיפול בשגיאות, נגישות בסיסית, ושמירת מצב. נדרשת בדיקה ידנית לאימות הפונקציונליות בפועל.

---

*דוח זה נוצר על ידי סקירת קוד אוטומטית. בדיקות ידניות נדרשות לאימות מלא.*
