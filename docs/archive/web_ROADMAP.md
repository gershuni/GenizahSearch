# מפת דרכים - ממשק Web לחיפוש גניזה

## סקירת מצב נוכחי

### מה כבר מומש:
- [x] תשתית NiceGUI בסיסית
- [x] Service Layer (GenizahService) עם thread safety
- [x] דף בית עם 3 כרטיסים
- [x] חיפוש טקסט בסיסי (execute_search)
- [x] חיפוש מקבילות בסיסי (search_composition_logic)
- [x] דפדוף בכתבי יד (get_browse_page)
- [x] צפייה במסמך בודד
- [x] תמיכה בעברית/אנגלית (i18n)
- [x] כתובות תמונות IIIF

### מה חסר (לפי עדיפות):

---

## שלב 1: השלמת פונקציונליות בסיסית (עדיפות גבוהה)

### 1.1 שיפור חיפוש טקסט
**פונקציות מקור:** `SearchEngine.execute_search()`, `build_tantivy_query()`, `build_regex_pattern()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| הדגשת תוצאות | המרת `*text*` ל-HTML עם `<mark>` | קל |
| תמיכה ב-gap | הוספת פרמטר word gap לחיפוש ביטויים | קל |
| מצבי חיפוש נוספים | Fuzzy (~), Regex (/), Title ($), Shelfmark (#) | בינוני |
| תוצאות חוצות עמודים | טיפול ב-`page_highlights` ו-`scope` | בינוני |
| דה-דופליקציה | העדפת V0.8 על V0.7 | קל |

**קוד לייבא מ-genizah_core.py:**
```python
# שורות 3500-3700 - execute_search
# שורות 3200-3350 - build_tantivy_query
# שורות 3350-3450 - build_regex_pattern
# שורות 3450-3500 - highlight
```

### 1.2 שיפור תצוגת מסמך
**פונקציות מקור:** `get_full_text_by_id()`, `get_browse_page()`, `get_full_manuscript()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| הדגשת מונחי חיפוש | הצגת highlight_pattern בטקסט | קל |
| ניווט בין עמודים | כפתורי קודם/הבא עם absolute_index | בינוני |
| תצוגת תמונה | טעינת תמונות IIIF עם zoom/pan | בינוני |
| מעבר בין כתבי יד | allow_cross=True לניווט רציף | קל |

### 1.3 שיפור דפדוף
**פונקציות מקור:** `resolve_system_by_shelfmark()`, `get_browse_page()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| השלמה אוטומטית | Autocomplete לשדה shelfmark | בינוני |
| תצוגת מטא-דאטא | הצגת title, author, date | קל |
| בחירת עמוד | קפיצה לעמוד ספציפי | קל |

---

## שלב 2: פיצ'רים מתקדמים (עדיפות בינונית)

### 2.1 חיפוש Lab Mode (אופציונלי)
**פונקציות מקור:** `LabEngine.lab_search()`, `lab_composition_search()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| אתחול LabEngine | יצירת אינדקס fingerprint | גבוה |
| חיפוש Lab | ממשק לחיפוש מבוסס תדירות | גבוה |
| הגדרות scoring | פרמטרים לציון התאמה | בינוני |

**הערה:** Lab Mode הוא אופציונלי ומורכב. מומלץ להשאיר לשלב מאוחר יותר.

### 2.2 מטא-דאטא מורחב
**פונקציות מקור:** `MetadataManager.fetch_nli_data()`, `fetch_marc_data()`, `enrich_metadata()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| שליפת NLI API | קבלת מטא-דאטא מהספרייה הלאומית | בינוני |
| שליפת MARC | קבלת רשומות MARC מ-Aleph | בינוני |
| קאשינג | שמירת מטא-דאטא לשימוש חוזר | קל |
| תצוגה מורחבת | פאנל מטא-דאטא מפורט | קל |

### 2.3 תמונות IIIF מתקדמות
**פונקציות מקור:** `fetch_iiif_manifest()`, `fetch_external_iiif_data()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| טעינת manifests | קבלת רשימת תמונות | בינוני |
| ספקים חיצוניים | Cambridge, British Library | בינוני |
| Zoom/Pan | ממשק צפייה בתמונות | גבוה |
| טעינה אסינכרונית | טעינה ברקע ללא חסימת UI | בינוני |

---

## שלב 3: פיצ'רים מלאים (עדיפות נמוכה)

### 3.1 ייצוא תוצאות
**פונקציות מקור:** `export_results()`, `export_comp_report()`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| ייצוא Excel | תוצאות חיפוש ל-XLSX | בינוני |
| ייצוא Word | דו"ח עם עיצוב RTL | בינוני |
| ייצוא CSV | פורמט פשוט | קל |

### 3.2 רשימות אישיות
**פונקציות מקור:** `ListsManager` (CRUD operations)

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| יצירת רשימה | CRUD לרשימות | בינוני |
| הוספת פריטים | שמירת תוצאות לרשימה | קל |
| ייצוא/ייבוא | JSON import/export | קל |
| תיוגים והערות | metadata לפריטים | בינוני |

**הערה:** דורש אימות משתמשים (auth) או session storage.

### 3.3 קבוצות קודיקולוגיות (Neubauer Parts)
**פונקציות מקור:** `CodicologicalPartsManager`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| טעינת parts | קריאת NeubParts.csv | קל |
| דפדוף לפי part | בחירת חלק קודיקולוגי | בינוני |
| קיבוץ תוצאות | איחוד תוצאות לפי part | בינוני |

### 3.4 וריאנטים מתקדמים
**פונקציות מקור:** `VariantManager`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| וריאנטים מותאמים | מיפויי תווים מותאמים אישית | בינוני |
| Hamming distance | וריאנטים לפי מרחק עריכה | גבוה |
| קאש וריאנטים | LRU cache לביצועים | קל |

---

## שלב 4: AI ותכונות עתידיות

### 4.1 עוזר AI
**פונקציות מקור:** `AIManager`, `AIDialog`

| משימה | תיאור | מורכבות |
|--------|--------|----------|
| יצירת regex | AI ליצירת ביטויים רגולריים | בינוני |
| חיבור ל-Gemini | Google AI API | בינוני |

---

## ארכיטקטורה מומלצת

### מבנה קבצים מוצע:
```
web/
├── main.py              # נקודת כניסה + routing
├── services.py          # Service Layer (GenizahService)
├── translations.py      # תרגומים
├── pages/
│   ├── home.py          # דף בית
│   ├── search.py        # חיפוש טקסט
│   ├── parallels.py     # מציאת מקבילות
│   ├── browse.py        # דפדוף בכתבי יד
│   └── document.py      # צפייה במסמך
├── components/
│   ├── result_card.py   # כרטיס תוצאה
│   ├── image_viewer.py  # צפייה בתמונות
│   ├── metadata_panel.py # פאנל מטא-דאטא
│   └── pagination.py    # ניווט עמודים
└── utils/
    ├── highlight.py     # המרת *text* ל-HTML
    └── cache.py         # קאשינג מטא-דאטא
```

### הנחיות לייבוא פונקציות:

1. **לעולם לא לייבא ישירות מ-genizah_core** לקבצי UI
2. **כל הגישה דרך GenizahService** ב-services.py
3. **Thread safety** - כל פעולת חיפוש עוברת דרך `_search_lock`
4. **Async handling** - פעולות ארוכות עם spinner ו-async/await

### דוגמה להוספת פונקציה:

```python
# ב-services.py - הוספת פונקציה חדשה
class GenizahService:
    def get_metadata(self, sys_id: str) -> dict:
        """Fetch enriched metadata for a manuscript."""
        if not self.is_ready:
            return {}

        with self._search_lock:
            try:
                return self._metadata_manager.enrich_metadata(sys_id)
            except Exception as e:
                logger.error(f"Metadata error: {e}")
                return {}

# ב-pages/document.py - שימוש בפונקציה
from web.services import get_service

def create_document_page(uid: str):
    service = get_service()
    metadata = service.get_metadata(sys_id)
    # הצגת metadata בממשק
```

---

## סדר עבודה מומלץ

### Sprint 1 (בסיסי):
1. שיפור הדגשת תוצאות חיפוש
2. ניווט בין עמודים במסמך
3. תצוגת מטא-דאטא בסיסית
4. השלמה אוטומטית ל-shelfmark

### Sprint 2 (מתקדם):
1. תצוגת תמונות IIIF
2. שליפת מטא-דאטא מ-NLI
3. מצבי חיפוש נוספים (fuzzy, regex)
4. תוצאות חוצות עמודים

### Sprint 3 (מלא):
1. ייצוא תוצאות
2. Codicological Parts
3. Lab Mode (אם נדרש)

---

## הגדרת סוכנים (Agents) לפיתוח

לפיתוח מהיר, ניתן להשתמש בסוכנים מקבילים:

### סוכן 1: חיפוש מתקדם
```
Task: Implement advanced search features in web interface
- Add word gap parameter
- Add fuzzy and regex modes
- Implement result highlighting with <mark> tags
- Handle cross-page results
```

### סוכן 2: תמונות IIIF
```
Task: Implement IIIF image viewing
- Fetch IIIF manifests
- Create image viewer component with zoom/pan
- Support external providers (Cambridge, BL)
- Async image loading
```

### סוכן 3: מטא-דאטא
```
Task: Implement metadata fetching and display
- Add NLI API integration
- Add MARC data fetching
- Create metadata panel component
- Implement caching
```

### איך להפעיל סוכן:
```
בקש ממני: "הפעל סוכן לפיתוח [תחום]"
או: "עבוד במקביל על חיפוש ותמונות"
```

---

## סיכום עדיפויות

| עדיפות | תכונה | זמן משוער |
|--------|--------|-----------|
| 🔴 גבוהה | הדגשת תוצאות | שעה |
| 🔴 גבוהה | ניווט עמודים | שעתיים |
| 🔴 גבוהה | מטא-דאטא בסיסי | שעה |
| 🟡 בינונית | תמונות IIIF | 3-4 שעות |
| 🟡 בינונית | מצבי חיפוש נוספים | 2-3 שעות |
| 🟡 בינונית | NLI/MARC API | 2-3 שעות |
| 🟢 נמוכה | ייצוא | 2-3 שעות |
| 🟢 נמוכה | רשימות אישיות | 4-5 שעות |
| 🟢 נמוכה | Lab Mode | 6-8 שעות |

---

*מסמך זה נוצר אוטומטית על בסיס ניתוח מעמיק של genizah_core.py, genizah_app.py ו-gui_threads.py*
