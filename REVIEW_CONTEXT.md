# הקשר לסקירת הצ'קליסט
## Context for Checklist Review

**מטרה:** בדיקת Pre-Launch לאתר חיפוש גניזה (Genizah Search)
**טכנולוגיות:** Python, NiceGUI (Vue.js/Quasar), FastAPI, SQLite
**שפת ממשק:** עברית (RTL)

---

## קבצים לסקירה

### 1. הצ'קליסט הראשי
- `PRE_LAUNCH_CHECKLIST.md` - 440 פריטי בדיקה ב-20 תחומים

### 2. דוחות בדיקה מפורטים
| קובץ | תחומים |
|------|--------|
| `TEST_REPORT_AREAS_1_2.md` | דף הבית, חיפוש |
| `TEST_REPORT_AREAS_3_4.md` | דפדוף (Browse), מקבילות |
| `TEST_REPORT_AREAS_5_6.md` | רשימות, משתמשים/אימות |
| `TEST_REPORT_AREAS_7_8.md` | תיקונים, תגובות |
| `TEST_REPORT_AREAS_9_10.md` | גילויים, אדמין |
| `TEST_REPORT_AREAS_11_14.md` | הגדרות, עזרה, ניווט, ערכות נושא |
| `TEST_REPORT_AREAS_17_20.md` | שילובים, ביצועים, שגיאות, אבטחה |

### 3. קבצי קוד מרכזיים (לרפרנס)
```
web/
├── main.py              # App entry, routing, themes, responsive
├── auth_state.py        # Authentication, API calls, tokens
├── state.py             # Global state management
├── api.py               # Image proxy, exports, IIIF
├── services.py          # Search services
├── translations.py      # i18n
├── pages/
│   ├── home.py          # Landing page
│   ├── search.py        # Search interface
│   ├── browse.py        # Document viewer (2300+ lines)
│   ├── parallels.py     # Parallel text finder
│   ├── lists.py         # Personal lists
│   ├── corrections.py   # Corrections system
│   ├── discoveries.py   # Community discoveries
│   ├── admin.py         # Admin panel
│   ├── profile.py       # User profile
│   ├── settings.py      # Settings page
│   ├── help.py          # Help center
│   └── accessibility.py # Accessibility statement
└── components/
    ├── comment_dialog.py   # Comment creation
    ├── notes_display.py    # Comment display
    ├── joins_panel.py      # Fragment joins
    ├── text_editor.py      # Text editing
    └── typography.py       # H1/H2/H3 components

backend/
├── api/routes/          # FastAPI endpoints
│   ├── auth.py, users.py, comments.py, corrections.py,
│   ├── discoveries.py, versions.py, joins.py, documents.py
├── services/            # Business logic
└── models/              # SQLAlchemy models
```

---

## מבנה האפליקציה

### דפים ציבוריים (ללא התחברות)
- `/` - דף הבית
- `/search` - חיפוש טקסט
- `/browse` - צפייה בכתבי יד
- `/parallels` - חיפוש מקבילות
- `/help` - מרכז עזרה
- `/accessibility` - הצהרת נגישות
- `/download` - הורדת אפליקציית דסקטופ

### דפים למשתמשים מחוברים
- `/lists` - רשימות אישיות
- `/profile` - פרופיל משתמש
- `/corrections` - התיקונים שלי
- `/discoveries` - גילויים קהילתיים
- `/settings` - הגדרות

### דפים לאדמין
- `/admin` - פאנל ניהול

---

## תפקידי משתמשים (Role Hierarchy)
```
GUEST        → צפייה בלבד
CONTRIBUTOR  → תגובות, הגשת תיקונים
REVIEWER     → סקירת תיקונים
EDITOR       → אישור/דחיית תיקונים
ADMIN        → גישה מלאה + ניהול
```

---

## שאלות לסוקר

1. **שלמות:** האם יש פונקציונליות שלא נבדקה?
2. **עדיפויות:** האם סדר העדיפויות (P0-P3) נכון?
3. **אבטחה:** האם זיהיתי את כל סיכוני האבטחה?
4. **מקרי קצה:** אילו edge cases חסרים?
5. **אינטגרציה:** האם יש זרימות E2E שחסרות?
6. **תאימות:** האם יש בעיות RTL/Hebrew שלא נבדקו?

---

## ממצאים עיקריים מהסקירה

### נמצאו כבעיות:
- 17 מופעי `sanitize=False` (XSS risk)
- אין Rate Limiting
- אין CSRF protection
- Path Traversal potential בקובץ parallels.py
- ~60 DEBUG prints בקוד
- תגובות לא מוצגות כראוי ב-Browse

### לא נבדקו:
- Backend DB queries לעומק
- Production server config
- SSL/TLS configuration
- Browser compatibility (רק code review)
- E2E integration tests

---

**נוצר:** 2026-01-29
**מטרה:** סקירת עמיתים (Peer Review) של הצ'קליסט
