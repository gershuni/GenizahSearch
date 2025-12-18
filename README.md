## 📜 Genizah Search Pro 3.2.1

**The Ultimate Search & Analysis Tool for the Cairo Genizah Corpus**

Genizah Search Pro is a powerful desktop application designed for researchers working with Cairo Genizah manuscripts, especially with the transcriptions made available by the MiDRASH Project.
**Version 3** introduces a major redesign focused on speed, instant metadata access, and continuous manuscript reading.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey) ![License](https://img.shields.io/badge/License-MIT-green)

---

> **⚠️ IMPORTANT – FIRST RUN / AFTER UPGRADES**
> After installing or upgrading to version 3, go to **Settings & About** and click **Build / Rebuild Index**.
> Rebuilding the index is required to enable line-by-line display and metadata-based search.

---

## 🚀 What’s New in Version 3.2.1

* **🧪 Improved reliability:** Clearer startup diagnostics and logging for missing dependencies and configuration issues.
* **🏷️ Version consistency:** Application title, About panel, and Windows App ID are now synchronized from a single version source.

## 🚀 What’s New in Version 3.2

* **📊 Expanded export options:** Export search results and composition reports to **Excel (.xlsx)** and **CSV**, in addition to plain text.

## 🚀 What’s New in Version 3.1

* **🇮🇱 Full Hebrew UI:** Complete Hebrew localization, including optimized Right-to-Left layout.
* **🔠 Improved Variants Search:** Better handling of OCR noise and character substitutions.
* **📄 Professional reports:** All exports include standardized citation headers for MiDRASH.
* **🛠 Stability improvements:** General bug fixes and performance tuning.

## 🚀 What’s New in Version 3.0

* **⚡ Instant offline metadata:** Includes an internal metadata database (`libraries.csv`) with over **216,000 records**, showing **Shelfmarks and Titles instantly** in search results.
* **🔎 Metadata search:** Search directly by **Shelfmark** or **Composition Title** from the main search bar.
* **📐 Original line breaks:** Manuscript text is displayed line-by-line as in the original transcription.
* **📊 Sortable results:** Sort by Shelfmark, Title, or relevance.
* **🖼️ Image previews:** Thumbnails are retrieved from NLI services and cached locally.
* **📜 Continuous manuscript view:** Read all pages of a manuscript in a single scrolling view.
* **💾 Save full manuscripts:** Export multi-page manuscripts into one text file.

---

## ✨ Key Features

### 🔍 Advanced Search Engine

* **Ultra-fast search:** Powered by [Tantivy](https://github.com/quickwit-oss/tantivy).
* **Search modes:**

  * **Exact**
  * **Variants** (Hebrew-optimized OCR correction)
  * **Fuzzy**
  * **Regex**
  * **Shelfmark / Title** (metadata search)
* **Rich results table:** Sortable columns, instant metadata, snippets, and image previews.

### 🧩 Composition Analysis (Source Matching)

* **Find parallels:** Paste a source text to locate all its occurrences in the corpus.
* **Smart grouping:** Automatically groups likely matches and pushes noisy results to an Appendix.
* **Advanced filtering:** Filter results before or after analysis.
* **Split-screen comparison:** Source text vs. Genizah fragment with synchronized highlighting.

### 📚 Manuscript Browser

* **Direct access:** Enter a **System ID (`99…`)** to open a manuscript.
* **Smart image engine:** Retrieves the best available image from NLI IIIF/Rosetta services and caches it locally.
* **Deep links:** One-click access to the Ktiv catalog and high-resolution viewer.

> ⚠️ **Note:** Direct navigation by FL ID is **not currently supported**.

---

## 🛠 Installation (Windows)

1. **Download**
   Download the latest `GenizahSearchPro-3.2.1.zip` from the Releases page.

2. **Extract**
   Unzip to a **new folder**.
   **Keep all files together. Do not run the EXE from inside the ZIP.**

3. **Required data file**
   Place the following file **in the same folder as `GenizahSearchPro.exe`**:

   * `Transcriptions.txt`
     MiDRASH dataset: [https://doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)

4. **Bundled files (included in the ZIP)**

   * `libraries.csv`
   * `README.txt`
   * `LICENSE`
   * `PLACE THE PROGRAM NEXT TO TRANSCRIPTIONS FILE.txt`

5. **Run**
   Double-click `GenizahSearchPro.exe`.

6. **First run**
   Go to **Settings & About → Build / Rebuild Index**
   (Indexing takes a few minutes.)

---

## 📖 Usage Guide

### 1. Standard Search

* **Query:** Enter words, shelfmarks, or titles.
* **Mode:**

  * **Exact** – exact word match (supports Gap).
  * **Variants** – recommended for OCR-noisy text.
  * **Fuzzy**
  * **Regex**
  * **Shelfmark / Title** – metadata lookup.
* **Sort:** Click column headers.
* **View:** Double-click a result to open the full viewer.

### 2. Composition Search (Finding Parallels)

1. Paste source text or load a text file.
2. Configure:

   * **Chunk size** (recommended: 4–7 words)
   * **Max frequency**
   * **Appendix threshold**
3. *(Optional)* Exclude known manuscripts.
4. *(Optional)* Filter known texts (Bible, Mishna, Talmud).
5. Run analysis and explore grouped results.
6. Export a detailed report.

### 3. Browsing & Reading

* Go to **Browse Manuscript**.
* Enter a **System ID (`99…`)**.
* Use **View All** to read the entire manuscript.
* Save the full manuscript text locally.

---

## 🎓 Credits & Acknowledgments

**Developed by:** Hillel Gershuni

**Data source – MiDRASH Project:**
Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
[https://doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)

**Libraries used:**

* PyQt6
* Tantivy
* Requests / urllib3

---

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.


---

# 📜 Genizah Search Pro 3.2.1 (עברית)

**כלי חיפוש וניתוח מתקדם לכתבי היד של הגניזה הקהירית**

Genizah Search Pro היא אפליקציית שולחן עבודה למחקר בקורפוס הגניזה הקהירית, המבוססת על תעתיקי **פרויקט MiDRASH**.
גרסה 3 מתמקדת במהירות, בנגישות למטא־דאטה ובחוויית קריאה רציפה וברורה.

---

## ⚠️ חשוב: הפעלה ראשונה / לאחר עדכון

לאחר התקנה ראשונה **או עדכון גרסה**, יש לגשת ללשונית
**הגדרות ואודות** → **בנה / בנייה מחדש של אינדקס**.

פעולה זו נדרשת כדי לאפשר חיפוש, תצוגה שורה־אחר־שורה ושימוש במטא־דאטה.

---

## 🚀 מה חדש בגרסה 3.2.1

* **בדיקות אמינות משופרות:** איתור שגיאות חבילה ותלויות חסרות לפני יצירת קובץ ההפעלה.
* **סנכרון גרסה:** כותרת התוכנה, מסך האודות ומזהה האפליקציה ב-Windows משתמשים בערך גרסה אחיד.

## 🚀 מה חדש בגרסה 3.2

* **אפשרויות ייצוא מורחבות:** ייצוא תוצאות חיפוש ודוחות חיפוש חיבורים לפורמטים
  **Excel (.xlsx)** ו-**CSV**, בנוסף לקובצי טקסט.

## 🚀 מה חדש בגרסה 3.1

* **תמיכה מלאה בעברית:** ממשק מתורגם, כולל כיווניות מימין לשמאל.
* **חיפוש וריאנטים משופר:** טיפול מתקדם בשגיאות OCR וחילופי אותיות.
* **דוחות מקצועיים:** כל קובצי הייצוא כוללים כותרת ציטוט סטנדרטית.
* **שיפורי יציבות ותיקוני תצוגת מטא־דאטה.**

---

## 🚀 חידושי גרסה 3.0

* **מטא־דאטה אופליין מיידי:** הצגת **מספר מדף** ו-**כותרת** מתוך `libraries.csv`
  (מעל 216,000 רשומות) ישירות בטבלת התוצאות.
* **חיפוש במטא־דאטה:** חיפוש לפי **כותרת** או **מספר מדף** משורת החיפוש הראשית.
* **תצוגה שורה־אחר־שורה:** הטקסט מוצג לפי שבירת השורות המקורית של התעתיק.
* **תוצאות ניתנות למיון:** מיון לפי מספר מדף, כותרת או רלוונטיות.
* **תצוגה רציפה:** קריאת כתב יד שלם בגלילה אחת באמצעות **הכל**.
* **שמירת כתב יד:** ייצוא הטקסט המלא של כתב יד לקובץ.

---

## ✨ תכונות עיקריות

### 🔍 חיפוש

* **חיפוש מהיר במיוחד** באמצעות Tantivy.
* **מצבי חיפוש** (לפי שמות הממשק):

  * **מדויק**
  * **וריאנטים (?)**
  * **מורחב (??)**
  * **מקסימלי (???)**
  * **עמום (~)**
  * **ביטוי רגולרי**
  * **כותרת**
  * **מספר מדף**
* **טבלת תוצאות עשירה:** מספר מערכת, מספר מדף, כותרת, קטע, תמונה ומקור.

---

### 🧩 חיפוש חיבורים

כלי לאיתור מקבילות בין טקסט מקור לבין הקורפוס.

1. **כותרת החיבור** – אופציונלי.
2. הדבקת טקסט בתיבה **הדבק טקסט מקור...** או לחיצה על **טען קובץ טקסט**.
3. הגדרות:

   * **גוש** – מספר מילים בכל יחידת חיפוש.
   * **תדירות מקס'** – סינון ביטויים נפוצים.
   * **סינון >** – העברת כותרות נפוצות ל-**נספח**.
4. אופציונלי:

   * **החרג כתבי יד**
   * **סנן טקסט**
5. לחיצה על **נתח**.
6. ייצוא באמצעות **שמור דוח**.

---

### 📚 עיון בכתב יד

* מעבר ללשונית **עיון בכתב יד**.
* הזנת **מספר מערכת** ולחיצה על **עבור**.
* אפשרויות:

  * **הכל** – תצוגה רציפה של כל כתב היד.
  * **שמור** – שמירת כתב היד לקובץ.
  * **פתח באתר 'כתיב'** – צפייה בקטלוג הספרייה הלאומית.

> ⚠️ נכון לגרסה זו, העיון מתבצע **לפי מספר מערכת בלבד**.

---

## 🛠 התקנה

1. **הורדה:** הורידו את קובץ ה-ZIP העדכני מדף השחרורים.
2. **חילוץ:** חלצו את כל הקבצים **לתיקייה אחת חדשה**.
3. **קובץ נדרש:**

   * `Transcriptions.txt` – חייב להימצא **באותה תיקייה** כמו `GenizahSearchPro.exe`.
4. **הפעלה:** לחצו פעמיים על `GenizahSearchPro.exe`.
5. **בהפעלה הראשונה:** בנו אינדקס דרך **הגדרות ואודות**.

---

## 🎓 קרדיטים ותודות

פותח ע"י **הלל גרשוני**.
מבוסס על תעתיקי **פרויקט MiDRASH** (Zenodo, 2025).

---

## 📄 רישיון

התוכנה מופצת תחת רישיון **MIT**.
מאגר התעתיקים מופץ תחת **CC BY 4.0**.

