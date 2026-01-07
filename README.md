## 📜 Genizah Search Pro 4.0

**Research Tool for the Cairo Genizah**

Genizah Search Pro is a software for searching and researching Cairo Genizah manuscripts, based on the automatic transcriptions from the MiDRASH Project.
**Version 4.0** adds support for Oxford Bodleian Library's codicological units (Neubauer catalog Parts), enhancing manuscript browsing with proper manuscript identification.

---

## 🚀 What's New in Version 4.0?

* **📖 Codicological Parts (Neubauer):**
  Full support for Oxford Bodleian Library manuscripts with proper Neubauer catalog integration.
  * **Part-Based Browsing:** View manuscripts as complete codicological units, not just individual folios.
  * **Oxford Metadata:** Display catalog information directly from the Bodleian Hebrew manuscripts database.
  * **Dual Navigation:** Toggle between navigating by Parts (codicological units) or by Folios (individual pages).
  * **Autocomplete:** Search for Parts using the "(neubauer)" suffix in the shelfmark field.
  * **Image Integration:** Direct access to high-resolution Oxford manuscript images.

* **📚 Integrated Manuscript Viewer:**
  Allows viewing manuscript images directly within the software, independent of an external browser.
  * **Image Viewing:** Loads high-resolution images from the National Library of Israel and Cambridge University Library services.
  * **Sequential Navigation:** Continuous browsing between pages and sequential transition between manuscripts in the corpus (by file order).
  * **Display Tools:** Zoom and Rotation options to adjust the view.

* **🧪 Lab Mode:**
  An experimental search engine for detecting parallels, based on the algorithm by **Shmidman, Koppel, and Porat (2016)**.
  * **Proximity Detection:** Uses "rare letter" encoding to detect matches even in cases of spelling variations or OCR errors.
  * **Deep Scan:** Comprehensive scan for complex queries.
  * **Advanced Settings:** Control over scoring weights (density, word order, rarity).

* **🔎 Cross-Page Search:**
  Search now spans across page boundaries and manuscript sequences, keeping context even when matches cross folio edges.

* **🔍 Find in Text:**
  Quick in-pane search with highlighting in manuscript, source, and browse text views.

* **⚡ Interface & Data Management:**
  * **Custom Export:** Option to export only selected rows from results (Excel/CSV/TXT/DOCX).
  * **Filtering & Sorting:** Column-based filters and improved sorting in result tables.
  * **Quick Actions:** Unified "Actions" column allowing view or browse of a result with a single click.

---

## ✨ Key Features

### 🔍 Search
* **Fast Engine:** Based on the Tantivy library.
* **Search Modes:**
  * **Exact**
  * **Variants** (common spelling changes)
  * **Fuzzy** (Levenshtein distance)
  * **Regex** (Regular Expressions)
  * **Metadata** (Search by Title or Shelfmark)

### 🧩 Composition / Parallels Search
A tool for detecting parallels between a source text and the corpus, especially effective for finding new manuscripts or new citations of known works. The tool divides the text into chunks and searches for each chunk individually across the corpus, increasing the chance of finding parallels even in corrupted or slightly different texts.
* **Analysis:** Configure chunk size and filter by frequency.
* **Recursive Search:** Ability to run an additional search based on found results to uncover textual chains.
* **Grouping:** Automatic grouping of matches and filtering common titles to an Appendix.

### 📚 Manuscript Browse
* **Direct Access:** Load a manuscript by System ID or Shelfmark.
* **Offline Work:** Display text and metadata even without connection to image services.
* **External Links:** Quick link to the "Ktiv" catalog.

---

## 🛠 Installation

1. **Download:** Download the latest ZIP file from the Releases page.
2. **Extract:** Unzip the files to a new folder.
3. **Data File:**
   * Download `Transcriptions.txt` (MiDRASH dataset).
   * Save it next to the executable or select its location on first run.
4. **Run:** Run `GenizahSearchPro.exe`.
5. **Index Building:** On first run, build the index (takes a few minutes). Lab Mode requires building its own special index.

---

## 🎓 Credits

**Development:** Hillel Gershuni

**Data Source – MiDRASH Project:**
Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
[https://doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)

**Please cite this database in any use of the search results from this software.**

**Lab Mode Algorithm:**
Based on the research and algorithms of **Avi Shmidman, Moshe Koppel, and Eli Porat** (2016) for detecting parallels in Hebrew manuscripts, [https://arxiv.org/abs/1602.08715](https://arxiv.org/abs/1602.08715).

**Acknowledgments:**
This tool was developed with the assistance of **Gemini 3.0** and **GPT 5.1/2**. My thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer for their advice and support.

---

## 📄 License
The software is distributed under the **MIT** License.
The dataset is distributed under **CC BY 4.0**.
---

# 📜 Genizah Search Pro 4.0 (עברית)

**כלי מחקר לגניזה הקהירית**

Genizah Search Pro היא תוכנה לחיפוש ולמחקר בכתבי היד של הגניזה הקהירית, המבוססת על התעתיקים האוטומטיים של פרויקט MiDRASH.
**גרסה 4.0** מוסיפה תמיכה ביחידות קודיקולוגיות (Parts לפי קטלוג נויבאואר) של ספריית הבודליאנה באוקספורד, ומשפרת את העיון בכתבי היד עם זיהוי נכון של כתבי יד שלמים.

---

## 🚀 מה חדש בגרסה 4.0?

* **📖 חלקים קודיקולוגיים (נויבאואר):**
  תמיכה מלאה בכתבי היד של ספריית הבודליאנה באוקספורד עם שילוב קטלוג נויבאואר.
  * **עיון לפי Part:** צפייה בכתבי יד כיחידות קודיקולוגיות שלמות, לא רק לפי דפים בודדים.
  * **מטא-דאטה מאוקספורד:** הצגת מידע קטלוגי ישירות ממאגר כתבי היד העבריים של הבודליאנה.
  * **ניווט כפול:** מעבר בין ניווט לפי Parts (יחידות קודיקולוגיות) לבין ניווט לפי פוליואים (דפים בודדים).
  * **השלמה אוטומטית:** חיפוש Parts באמצעות הסיומת "(neubauer)" בשדה מספר המדף.
  * **שילוב תמונות:** גישה ישירה לתמונות ברזולוציה גבוהה מאוקספורד.

* **📚 צפיין כתבי יד מובנה:**
  מאפשר עיון בתמונות כתבי היד ישירות בתוך התוכנה, ללא תלות בדפדפן חיצוני.
  * **צפייה בתמונות** טעינת תמונות ברזולוציה גבוהה משירותי הספרייה הלאומית ומספריית האוניברסיטה של קימברידג'.
  * **ניווט סדרתי:** דפדוף רציף בין עמודים ומעבר סדרתי בין כתבי יד בקורפוס (לפי סדר הקבצים).
  * **כלי תצוגה:** אפשרויות זום וסיבוב (Rotation) להתאמת התצוגה.

* **🧪 מצב מעבדה (Lab Mode):**
  מנוע חיפוש ניסיוני לאיתור מקבילות, המבוסס על האלגוריתם של **שמידמן, קופל ופורת, 2016**.
  * **זיהוי קרבה:** שימוש בקידוד "אותיות נדירות" לאיתור התאמות גם במקרים של שיבושי כתיב או OCR.
  * **סריקה עמוקה (Deep Scan):** סריקה מקיפה לשאילתות מורכבות.
  * **הגדרות מתקדמות:** שליטה במשקלי הניקוד (צפיפות, סדר מילים, נדירות).

* **🔎 חיפוש חוצה-דפים:**
  החיפוש עובר כעת בין גבולות דפים ורצפים של כתבי יד, כך שהקשר נשמר גם כשיש התאמה החוצה את קצה הדף.

* **🔍 חיפוש בתוך הטקסט:**
  חיפוש מהיר עם הדגשה בחלונות הטקסט של כתב היד, המקור וחלון העיון.

* **⚡ ממשק וניהול נתונים:**
  * **ייצוא מותאם:** אפשרות לייצוא שורות נבחרות בלבד מתוך התוצאות (Excel/CSV/TXT/DOCX).
  * **סינון ומיון:** סינון לפי עמודות ושיפור כלי המיון בטבלאות התוצאות.
  * **פעולות מהירות:** עמודת "פעולות" מאוחדת המאפשרת צפייה בתוצאה או מעבר לעיון בכתב היד בלחיצה אחת.

---

## ✨ תכונות עיקריות

### 🔍 חיפוש
* **מנוע מהיר:** מבוסס על ספריית Tantivy.
* **מצבי חיפוש:**
  * **מדויק**
  * **וריאנטים** (שינויי כתיב נפוצים)
  * **עמום** (מרחק לווינשטיין)
  * **ביטוי רגולרי**
  * **מטא-דאטה** (חיפוש לפי כותרת או מספר מדף)

### 🧩 חיפוש חיבורים/מקבילות 
כלי לאיתור מקבילות בין טקסט מקור לבין הקורפוס, יעיל במיוחד למציאת כתבי יד חדשים או ציטוטים חדשים של חיבורים ידועים. הכלי מחלק את הטקסט למקטעים, ומחפש כל מקטע בפני עצמו בכל הקורפוס, וכך מעלה את הסיכוי למצוא מקבילות גם בטקסטים משובשים או שונים במידת מה מן הטקסט שלנו.
* **ניתוח:** הגדרת גודל מקטע וסינון לפי תדירות.
* **חיפוש רקורסיבי:** אפשרות להריץ חיפוש נוסף על בסיס התוצאות שנמצאו לאיתור שרשראות טקסט.
* **קיבוץ:** קיבוץ אוטומטי של התאמות וסינון כותרות נפוצות לנספח.

### 📚 עיון בכתב יד
* **גישה ישירה:** טעינת כתב יד לפי מספר מערכת או מספר מדף.
* **עבודה באוף-ליין:** הצגת הטקסט והמטא-דאטה גם ללא חיבור לשירותי התמונות.
* **קישורים חיצוניים:** מעבר מהיר לקטלוג "כתיב".

---

## 🛠 התקנה

1. **הורדה:** הורידו את קובץ ה-ZIP העדכני מדף השחרורים.
2. **חילוץ:** חלצו את הקבצים לתיקייה חדשה.
3. **קובץ נתונים:**
   * הורידו את `Transcriptions.txt` (מאגר MiDRASH).
   * שמרו אותו לצד קובץ ההפעלה או בחרו את מיקומו בהפעלה הראשונה.
4. **הפעלה:** הריצו את `GenizahSearchPro.exe`.
5. **בניית אינדקס:** בהפעלה ראשונה בנו את האינדקס (לוקח כמה דקות). למצב המעבדה יש צורך לבנות אינדקס מיוחד משלו.

---

## 🎓 קרדיטים

**פיתוח:** הלל גרשוני

**מקור הנתונים – פרויקט MiDRASH:**
Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
[https://doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)

**אנא צטטו מסד נתונים זה בכל שימוש בתוצאות החיפוש בתוכנה**

**אלגוריתם מצב מעבדה:**
מבוסס על המחקר והאלגוריתמים של **אבי שמידמן, משה קופל ואלי פורת** (2016) לזיהוי מקבילות בכתבי יד עבריים, [https://arxiv.org/abs/1602.08715](https://arxiv.org/abs/1602.08715).

**תודות:**

כלי זה פותח בסיוע **Gemini 3.0** ו**GPT 5.1/2**. תודתי נתונה לאבי שמידמן, אלישע רוזנצוייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר על עצותיהם ותמיכתם.</p>

---

## 📄 רישיון
התוכנה מופצת תחת רישיון **MIT**.
מאגר הנתונים מופץ תחת **CC BY 4.0**.
