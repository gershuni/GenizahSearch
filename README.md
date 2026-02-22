# Genizah Search Pro 6.0.0

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 6.0.0?

### v6.0.0: Local Data Architecture

All scholarly reference data migrated to local SQLite sidecars for offline-capable, sub-millisecond browsing. Plus desktop stability fixes, paginated search, and performance optimizations.

* **Offline PGP browsing:** All PGP data (35,839 documents, transcriptions, footnotes, fragments) served from local pgp.db sidecar — no internet required for metadata browsing
* **FJMS catalog descriptions:** Expanded scholarly catalog with dedicated dialog showing content identification, physical metadata, running titles, free descriptions, and genizah titles
* **Paginated search:** PAGE_SIZE=50 replaces the 200-result cap, with prev/next navigation
* **Desktop stability:** All known crash-on-navigate bugs fixed with Qt lifecycle guards
* **Performance:** Parallel NLI fetch, browse crossref parallelization, FL ID O(1) index, variant cache unification
* **PostHog analytics:** Privacy-first analytics alongside Google Analytics (env-var gated)
* **Sidecar updates:** Desktop app checks for newer sidecar versions at startup, downloads to AppData

### Previous Features (v5.0–v5.9)

* **Multi-Source Images (v5.9):** NLI, Cambridge, Manchester LUNA, JTS/Princeton Figgy with folio navigation, bibliography (542K), catalog cross-references (64K)
* **FJMS Integration (v5.8):** Domain classifications, scientific joins, and catalog enrichment from FIST.db via SQLite sidecar
* **Responsa Search (v5.7):** Advanced search with Responsa-Project style syntax, grammatical expansion, Judeo-Arabic support, and tabular query builder
* **Princeton Geniza Project (PGP):** 35,839 curated documents with transcriptions, translations, and metadata
* **Virtual Reading Desk:** Multi-manuscript synchronized viewer for related fragments
* **PGP Tag Search:** 251 tags in 16 categories for thematic browsing
* **Web Platform:** [genizahsearch.com](https://genizahsearch.com) — full-featured web access from any browser
* **Community Features:** Discoveries, comments, corrections — collaborate with researchers worldwide
* **Cross-Paragraph Search:** Find text spanning paragraph boundaries, filtering out common citations
* **Cloud Sync:** Automatic list sync across devices
* **In-App Updates:** Desktop self-updates from GitHub Releases

---

## Core Features

### Integrated Visual Analysis (IIIF)

* **In-App Viewer:** High-resolution images from NLI, Cambridge, Manchester LUNA, and JTS/Princeton Figgy
* **Multi-Source Toggle:** Switch between image sources with colored source chips
* **Folio Navigation:** Page-level navigation with scholarly recto/verso notation
* **Image Tools:** Zoom and rotation controls

### Oxford Bodleian Integration

* Full support with **Neubauer catalog** integration
* Part-based and folio-based navigation
* Rich metadata display

### Lab Mode (Experimental)

Parallel detection based on **Shmidman, Koppel, and Porat (2016)**.

* Rare letter encoding for spelling variation tolerance
* Deep scan for complex queries

### Personal Lists

* Create and organize manuscript collections
* Browse and filter by custom lists

---

## Additional Capabilities

* **Cross-Page Search:** Results span page boundaries
* **Enhanced Export:** Excel, CSV, DOCX with selection support
* **Find in Text:** Quick search with highlighting
* **Composition Search:** Detect parallels using chunk analysis

---

## Getting Started

### Web (Recommended)

Visit [genizahsearch.com](https://genizahsearch.com) to start using Genizah Search Pro immediately.

### Desktop Installation

1. **Download:** Get `GenizahSearchPro_V6.0.0_Setup.exe` from the **Assets** section
2. **Install:** Run the installer and follow instructions
3. **Data Setup:** The software requires the **MiDRASH** dataset (`Transcriptions.txt`)

> **Antivirus Note:** Some antivirus software (Avast, AVG, Windows Defender) may flag the installer as suspicious. These are **false positives** caused by PyInstaller packaging. See [ANTIVIRUS_INFO.txt](ANTIVIRUS_INFO.txt) for details and solutions.

---

## Documentation

For detailed documentation, see the [docs/](docs/DOCUMENTATION_INDEX.md) directory:

* **[Documentation Index](docs/DOCUMENTATION_INDEX.md)** - Overview of all documentation
* **[Admin Guide](docs/guides/WEBSITE_ADMIN_GUIDE.md)** - Website management for administrators
* **[Code Index](docs/CODE_INDEX.md)** - Code structure and architecture
* **[Plans](docs/plans/)** - Implementation plans and roadmaps

---

## Credits & Data

* **Development:** Hillel Gershuni
* **Data Sources:**
  - Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*. ([doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473))
  - [Princeton Geniza Project (PGP)](https://geniza.princeton.edu/) — Curated transcriptions, translations, and metadata for Cairo Genizah documents
  - Fragment of the Jewish Manuscript Studies (FJMS) — Domain classifications, scientific joins, and catalog records
* **Lab Mode Algorithm:** Based on [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**Acknowledgments:**
Developed with the support of **DICTA**.
Assisted by **Claude**, **Gemini**, and **GPT**.
Special thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.

---

# Hebrew (עברית)

# Genizah Search Pro 6.0.0 | אתר הגניזה של דיקטה

**פלטפורמת מחקר שיתופית לגניזה הקהירית**

גרסה 6.0 כוללת **ארכיטקטורת נתונים מקומית** — כל נתוני PGP הועברו לבסיס נתונים מקומי, עיון במטא-דאטה ללא אינטרנט, תיקוני יציבות, ואופטימיזציית ביצועים.

> **גישה מהאינטרנט:** [genizahsearch.com](https://genizahsearch.com) - חיפוש, עיון ושיתוף פעולה מכל דפדפן

---

## מה חדש בגרסה 6.0?

### ארכיטקטורת נתונים מקומית

כל נתוני ההפניה המדעיים הועברו לבסיסי נתונים מקומיים (SQLite) לעיון אופליין עם זמני תגובה של פחות מאלפית שנייה.

* **עיון PGP אופליין:** כל נתוני PGP (35,839 מסמכים, תעתיקים, הערות שוליים, קטעים) מוגשים מבסיס נתונים מקומי — אין צורך באינטרנט לעיון במטא-דאטה
* **תיאורי קטלוג FJMS:** קטלוג מדעי מורחב עם דיאלוג ייעודי המציג זיהוי תוכן, מטא-דאטה פיזי, כותרות ריצה, תיאורים חופשיים וכותרות גניזה
* **חיפוש עם דפדוף:** 50 תוצאות בעמוד מחליפים את מגבלת 200 התוצאות
* **יציבות שולחן העבודה:** כל התרסקויות הניווט הידועות תוקנו
* **ביצועים:** שליפת NLI מקבילית, אינדקס FL ID, איחוד מטמון וריאנטים
* **עדכוני סיידקר:** האפליקציה בודקת גרסאות חדשות של בסיסי נתונים בהפעלה

### תכונות מגרסאות קודמות (5.0–5.9)

* **שילוב תמונות ממספר מקורות (5.9):** NLI, קיימברידג', מנצ'סטר LUNA ו-JTS/פרינסטון Figgy עם ניווט דפים, ביבליוגרפיה (542K), הפניות קטלוגיות (64K)
* **שילוב FJMS (5.8):** סיווגי תחום, צירופים מדעיים ורשומות קטלוג מ-FIST.db דרך בסיס נתונים SQLite
* **חיפוש פרויקט השו"ת (5.7):** חיפוש מתקדם עם תחביר ייעודי, הרחבה דקדוקית, ערבית-יהודית, ובונה שאילתות טבלאי
* **פרויקט הגניזה של פרינסטון (PGP):** 35,839 מסמכים אצורים עם תעתיקים, תרגומים ומטא-דאטה
* **שולחן קריאה וירטואלי:** צפיין מסונכרן לקטעים קשורים זה לצד זה
* **חיפוש תגיות PGP:** 251 תגיות ב-16 קטגוריות לעיון נושאי
* **פלטפורמת אינטרנט:** [genizahsearch.com](https://genizahsearch.com) — גישה מלאה מכל דפדפן
* **תכונות קהילה:** גילויים, הערות, תיקונים — שיתוף פעולה עם חוקרים ברחבי העולם
* **חיפוש חוצה-פסקאות:** איתור טקסט החוצה גבולות פסקאות, סינון ציטוטים נפוצים
* **סנכרון ענן:** סנכרון רשימות אוטומטי בין מכשירים
* **עדכונים אוטומטיים:** אפליקציית שולחן העבודה מתעדכנת מ-GitHub Releases

---

## תכונות ליבה

### ניתוח חזותי משולב (IIIF)

* **צפיין מובנה:** תמונות ברזולוציה גבוהה מ-NLI, קיימברידג', מנצ'סטר LUNA ו-JTS/פרינסטון Figgy
* **מתג מקורות:** מעבר בין מקורות תמונות עם כפתורי מקור צבעוניים
* **ניווט דפים:** ניווט ברמת דף עם סימון recto/verso מדעי
* **כלי תמונה:** שליטה בזום וסיבוב

### אינטגרציית אוקספורד-בודלי

* תמיכה מלאה עם שילוב **קטלוג נויבאואר**
* ניווט לפי יחידות קודיקולוגיות ודפים
* תצוגת מטא-דאטה עשירה

### מצב מעבדה (ניסיוני)

איתור מקבילות מבוסס על **שמידמן, קופל ופורת (2016)**.

* קידוד אותיות נדירות לסבילות לשינויי כתיב
* סריקה עמוקה לשאילתות מורכבות

### רשימות אישיות

* יצירה וארגון של אוספי כתבי יד
* עיון וסינון לפי רשימות מותאמות אישית

---

## יכולות נוספות

* **חיפוש חוצה-דפים:** תוצאות משתרעות מעבר לגבולות הדף
* **ייצוא משופר:** Excel, CSV, DOCX עם תמיכה בבחירה
* **חיפוש בטקסט:** חיפוש מהיר עם הדגשה
* **חיפוש מקבילות:** איתור מקבילות באמצעות ניתוח מקטעים

---

## תחילת עבודה

### אינטרנט (מומלץ)

בקרו ב-[genizahsearch.com](https://genizahsearch.com) כדי להתחיל להשתמש ב-Genizah Search Pro מיד.

### התקנה לשולחן העבודה

1. **הורדה:** הורידו את `GenizahSearchPro_V6.0.0_Setup.exe` מאזור ה-**Assets**
2. **התקנה:** הריצו את קובץ ההתקנה ועקבו אחר ההוראות
3. **הגדרת נתונים:** התוכנה דורשת את מאגר **MiDRASH** (`Transcriptions.txt`)

> **הערה לגבי אנטיוירוס:** חלק מתוכנות האנטיוירוס (Avast, AVG, Windows Defender) עשויות לסמן את קובץ ההתקנה כחשוד. אלה **זיהויים שגויים** הנגרמים מאריזת PyInstaller. ראו [ANTIVIRUS_INFO.txt](ANTIVIRUS_INFO.txt) לפרטים ופתרונות.

---

## קרדיטים

* **פיתוח:** הלל גרשוני
* **מקורות נתונים:**
  - Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
  - [פרויקט הגניזה של פרינסטון (PGP)](https://geniza.princeton.edu/) — תעתיקים, תרגומים ומטא-דאטה אצורים למסמכי הגניזה הקהירית
  - Fragment of the Jewish Manuscript Studies (FJMS) — סיווגי תחום, צירופים מדעיים ורשומות קטלוג
* **אלגוריתם מצב מעבדה:** מבוסס על [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**תודות:**
פותח בתמיכת **דיקטה (DICTA)**.
בסיוע **Claude**, **Gemini** ו-**GPT**.
תודה מיוחדת לאבי שמידמן, אלישע רוזנצוייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר.
