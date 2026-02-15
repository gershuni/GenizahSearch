# Genizah Search Pro 5.8.0

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 5.8.0?

### v5.8.0: FJMS Integration

Integration of scholarly metadata from the **Fragment of the Jewish Manuscript Studies (FJMS)** database — domain classifications, scientific joins, and catalog records — into both web and desktop apps.

* **Domain classifications:** Subject badges (Piyyut, Bible, Letters, etc.) on browse pages with hierarchical search filtering
* **Scientific joins:** FJMS scholarly join groups with scholar attribution in Related Fragments panel
* **Catalog enrichment:** FJMS titles, authors, dates, and content identifications alongside PGP metadata
* **Domain filtering:** Post-search dynamic filtering with hierarchical checkbox tree dialog
* **SQLite sidecar:** 762K rows from FIST.db with FTS5 full-text index for future catalog search

### Previous Features (v5.0–v5.7)

* **Responsa Search:** Advanced search with Responsa-Project style syntax, grammatical expansion, Judeo-Arabic support, and tabular query builder
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

* **In-App Viewer:** High-resolution images from NLI and Cambridge
* **Sequential Navigation:** Browse pages and manuscripts continuously
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

1. **Download:** Get `GenizahSearchPro_V5.8.0_Setup.exe` from the **Assets** section
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

# Genizah Search Pro 5.8.0 | אתר הגניזה של דיקטה

**פלטפורמת מחקר שיתופית לגניזה הקהירית**

גרסה 5.8 כוללת **שילוב FJMS** — מידע מפרויקט הגניזה של פרידברג: סיווגי תחום, צירופים מדעיים ורשומות קטלוג.

> **גישה מהאינטרנט:** [genizahsearch.com](https://genizahsearch.com) - חיפוש, עיון ושיתוף פעולה מכל דפדפן

---

## מה חדש בגרסה 5.8?

### שילוב FJMS

שילוב מטא-דאטה מדעי מבסיס הנתונים **Fragment of the Jewish Manuscript Studies (FJMS)** — סיווגי תחום, צירופים מדעיים ורשומות קטלוג — בווב ובאפליקציית שולחן העבודה.

* **סיווגי תחום:** תגיות נושא (פיוט, מקרא, מכתבים ועוד) בדפי עיון עם סינון היררכי בחיפוש
* **צירופים מדעיים:** קבוצות צירוף מדעיות מ-FJMS עם ייחוס לחוקר בפאנל הקטעים הקשורים
* **העשרת קטלוג:** כותרות, מחברים, תאריכים וזיהויי תוכן מ-FJMS לצד מטא-דאטה של PGP
* **סינון תחומים:** סינון דינמי לאחר חיפוש עם דיאלוג עץ היררכי
* **בסיס נתונים SQLite:** 762 אלף שורות מ-FIST.db עם אינדקס FTS5 לחיפוש קטלוגי עתידי

### תכונות מגרסאות קודמות (5.0–5.7)

* **חיפוש פרויקט השו"ת:** חיפוש מתקדם עם תחביר ייעודי, הרחבה דקדוקית, ערבית-יהודית, ובונה שאילתות טבלאי
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

* **צפיין מובנה:** תמונות ברזולוציה גבוהה מהספרייה הלאומית וקיימברידג'
* **ניווט רציף:** דפדוף בין עמודים וכתבי יד
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

1. **הורדה:** הורידו את `GenizahSearchPro_V5.8.0_Setup.exe` מאזור ה-**Assets**
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
