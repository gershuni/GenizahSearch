# Genizah Search Pro 5.4

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 5.4?

### Cross-Paragraph Search

A powerful new search mode that finds manuscripts with text spanning paragraph boundaries. This helps filter out common citations (Mishnah, Talmud, known phrases) that typically appear within paragraphs.

* **Three search modes:** Full search, Cross-paragraph only, Combined (with boost)
* **Customizable delimiters:** Line break, blank line (paragraph), period, colon
* **Visual indicators:** 🔗 emoji prefix on scores, tooltips showing match count
* **Advanced settings:** Boost factor, minimum boundary matches, minimum delimiter distance

### Cloud Sync with Supabase

* **Automatic list sync** across devices when logged in
* **Direct cloud connection** for faster performance
* **Offline support** - changes sync when reconnected

### Web Platform: Dicta Genizah Search (אתר הגניזה של דיקטה)

Access from any device with a web browser at [genizahsearch.com](https://genizahsearch.com).

* **Public Web Application:** Full-featured interface accessible anywhere
* **Mobile Responsive:** Optimized for tablets and phones
* **User Accounts:** Register, login, and manage your profile
* **Offline Support:** Community features sync when reconnected

### Community Features

Collaborate with researchers worldwide.

* **Discovery Center:** Share and explore research discoveries
  - Vote on discoveries
  - Pin important findings
  - Mark as answered/resolved
  - Reference multiple shelfmarks
* **Comments System:** Add page-specific notes to manuscripts
  - Public and private comments
  - Draft support for work in progress
* **Corrections:** Contribute transcription improvements
  - Submit corrections for review
  - Track your contributions

### Desktop Integration

* Seamless sync between desktop and web
* Comments and text editing dialogs
* Full offline community mode

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

1. **Download:** Get `GenizahSearchPro_V5.4.1_Setup.exe` from the **Assets** section
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
* **Data Source:** Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*. ([doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473))
* **Lab Mode Algorithm:** Based on [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**Acknowledgments:**
Developed with the support of **DICTA**.
Assisted by **Claude**, **Gemini**, and **GPT**.
Special thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.

---

# Hebrew (עברית)

# Genizah Search Pro 5.4 | אתר הגניזה של דיקטה

**פלטפורמת מחקר שיתופית לגניזה הקהירית**

גרסה 5.3 כוללת **חיפוש חוצה-פסקאות**, **סנכרון ענן**, ושיפורים רבים בממשק.

> **גישה מהאינטרנט:** [genizahsearch.com](https://genizahsearch.com) - חיפוש, עיון ושיתוף פעולה מכל דפדפן

---

## מה חדש בגרסה 5.4?

### חיפוש חוצה-פסקאות

מצב חיפוש חדש שמוצא כתבי יד עם טקסט החוצה גבולות פסקאות. זה עוזר לסנן ציטוטים נפוצים (משנה, תלמוד, ביטויים מוכרים) שבדרך כלל מופיעים בתוך פסקאות.

* **שלושה מצבי חיפוש:** חיפוש מלא, חוצה-פסקאות בלבד, משולב (עם דירוג מוגבר)
* **מפרידים מותאמים אישית:** שבירת שורה, שורה ריקה (פסקה), נקודה, נקודתיים
* **חיווי חזותי:** סימן 🔗 לפני הציון, טולטיפים עם מספר התאמות
* **הגדרות מתקדמות:** מקדם דירוג, מינימום גבולות, מרחק מפריד מינימלי

### סנכרון ענן עם Supabase

* **סנכרון רשימות אוטומטי** בין מכשירים כשמחוברים
* **חיבור ישיר לענן** לביצועים מהירים יותר
* **תמיכה אופליין** - שינויים מסתנכרנים בחיבור מחדש

### פלטפורמת אינטרנט

גישה ל-Genizah Search Pro מכל מכשיר עם דפדפן.

* **אפליקציית אינטרנט ציבורית:** ממשק מלא נגיש מכל מקום
* **תצוגה מותאמת לנייד:** אופטימיזציה לטאבלטים וטלפונים
* **חשבונות משתמשים:** הרשמה, התחברות וניהול פרופיל
* **תמיכה אופליין:** תכונות הקהילה מסתנכרנות בחיבור מחדש

### תכונות קהילה

שיתוף פעולה עם חוקרים ברחבי העולם.

* **מרכז גילויים:** שיתוף וחקירת גילויי מחקר
  - הצבעה על גילויים
  - נעיצת ממצאים חשובים
  - סימון כנענה/נפתר
  - הפניה למספר סימוני מדף
* **מערכת הערות:** הוספת הערות ספציפיות לעמוד לכתבי יד
  - הערות ציבוריות ופרטיות
  - תמיכה בטיוטות לעבודה בתהליך
* **תיקונים:** תרומת שיפורי תמלול
  - הגשת תיקונים לבדיקה
  - מעקב אחר התרומות שלך

### אינטגרציה עם שולחן העבודה

* סנכרון חלק בין שולחן העבודה לאינטרנט
* דיאלוגים להערות ועריכת טקסט
* מצב קהילה אופליין מלא

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

1. **הורדה:** הורידו את `GenizahSearchPro_V5.4.1_Setup.exe` מאזור ה-**Assets**
2. **התקנה:** הריצו את קובץ ההתקנה ועקבו אחר ההוראות
3. **הגדרת נתונים:** התוכנה דורשת את מאגר **MiDRASH** (`Transcriptions.txt`)

> **הערה לגבי אנטיוירוס:** חלק מתוכנות האנטיוירוס (Avast, AVG, Windows Defender) עשויות לסמן את קובץ ההתקנה כחשוד. אלה **זיהויים שגויים** הנגרמים מאריזת PyInstaller. ראו [ANTIVIRUS_INFO.txt](ANTIVIRUS_INFO.txt) לפרטים ופתרונות.

---

## קרדיטים

* **פיתוח:** הלל גרשוני
* **מקור הנתונים:** Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
* **אלגוריתם מצב מעבדה:** מבוסס על [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**תודות:**
פותח בתמיכת **דיקטה (DICTA)**.
בסיוע **Claude**, **Gemini** ו-**GPT**.
תודה מיוחדת לאבי שמידמן, אלישע רוזנצוייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר.
