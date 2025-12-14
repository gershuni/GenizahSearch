**עברית למטה**

# 📜 Genizah Search Pro 3.2

**The Ultimate Search & Analysis Tool for the Cairo Genizah Corpus**

Genizah Search Pro is a powerful desktop application designed for researchers working with Cairo Genizah manuscripts, especially with the transcriptions made available by the MiDRASH Project. **Version 3** introduces a complete overhaul of the user experience, focusing on speed, metadata accessibility, and reading continuity.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey) ![License](https://img.shields.io/badge/License-MIT-green)

> **⚠️ IMPORTANT: FIRST RUN**  
> After installing or upgrading to version 3, you must go to the **Settings & About** tab and click **"Build / Rebuild Index"**.  
> This is required to support the new line-by-line display and metadata search features.

---

## 🚀 What's New in Version 3.2?
* **📊 Expanded Export Options:** You can now export search results and composition reports directly to **Excel (.xlsx)** and **CSV** formats, in addition to standard Text files.

---

##🚀 What's New in Version 3.1?
* **🇮🇱 Full Hebrew Support:** The software is now fully localized! The interface can be switched to Hebrew (including optimized Right-to-Left layout).
* **🔠 Enhanced Variants Search:** The search algorithms have been significantly refined to better handle complex OCR errors and character swaps, providing more accurate and comprehensive results in "Variants" modes.
* **📄 Professional Reports:** All exported files (Search Results, Manuscripts, Composition Reports) now automatically include standardized citation headers, making it easier to credit the MiDRASH project in your research.
* **🛠️ Stability & Bug Fixes:** Includes general performance improvements and fixes for metadata display issues.

---

## 🚀 What's New in Version 3.0?

*   **⚡ Instant Offline Metadata:** The software now includes an internal database (`libraries.csv`) with over 216,000 records, displaying Shelfmarks and Titles **instantly** alongside search results.
*   **🔎 Metadata Search:** You can now search directly for a **Shelfmark** (e.g., "T-S NS 306.15") or a **Composition Title** directly from the main search bar.
*   **📐 Original Line Breaks:** Manuscript text is now displayed line-by-line, exactly as it appears in the original transcription, rather than as a continuous block of text.
*   **📊 Sortable Results:** Click on table headers to sort search results by Shelfmark, Title, or Relevance.
*   **🖼️ Visual Preview:** Manuscript images are now displayed directly in the Search Results and Browse tabs (with smart caching for instant loading).
*   **📜 Continuous Manuscript View:** Read a manuscript naturally. The new **"View All"** feature loads all pages of a manuscript into a single, continuous scrolling view.
*   **💾 Save Entire Manuscript:** Export the full text of a multi-page manuscript into a single text file for offline study.

---

## ✨ Key Features

### 🔍 Advanced Search Engine
*   **Ultra-fast Search:** Powered by [Tantivy](https://github.com/quickwit-oss/tantivy) (Rust-based) for sub-second results.
*   **Search Modes:**
    *   **Text Search:** Exact, Fuzzy (Levenshtein), Regex, and **Variants** (Hebrew-optimized OCR correction).
    *   **Metadata Search:** Dedicated modes for **Title** and **Shelfmark** lookup.
*   **Rich Result Table:** Sortable columns, instant metadata, text snippets, and image previews.

### 🧩 Composition Analysis (Source Matching)
*   **Find Parallels:** Paste a source text (e.g., a known poem or Halakhic text) to find all its occurrences in the Genizah.
*   **Smart Grouping:** Automatically groups results by manuscript title (e.g., "Mishneh Torah") and other results that are probably less relevant to the user, to the Appendix.
*   **Advanced Filtering:** Filter results by specific words before or after the analysis.
*   **Split-Screen View:** Compare the Genizah fragment against your source text side-by-side with synchronized red highlighting.

### 📚 Manuscript Browser
*   **Unified Search:** Enter a System ID (`99...`) or a File ID (`FL...`) to jump directly to a specific page.
*   **Smart Image Engine:** Automatically retrieves the best available image (using NLI's IIIF/Rosetta servers) and caches it locally.
*   **Deep Links:** One-click access to the Ktiv catalog and high-res viewer.

---

## 🛠 Installation

1.  **Download:** Get the latest `GenizahSearchPro_v3.2.zip` from the releases page.
2.  **Extract:** Unzip to the folder where the Transcriptions.txt file exists (see below).
3.  **Required Data Files:**
    Ensure the following files are inside the folder next to `GenizahPro.exe`:
    *   `Transcriptions.txt` (The MiDRASH dataset, https://doi.org/10.5281/zenodo.17734473).
    *   `libraries.csv` (The metadata mapping file - **New in v3**).
4.  **Run:** Double-click `GenizahSearchPro.exe`.
5.	**On the first run, build the index. It will take a few minutes.

---

## 📖 Usage Guide

### 1. Standard Search
*   **Query:** Type words, shelfmarks, or titles.
*   **Mode:**
	*	**Exact:** Search for words as they are. You can set **Gap** between words.
    *   **Variants:** Best for general text search (handles OCR errors).
	*	**Fuzzy:** Other method to overcome OCR errors.
	*	**Regex:** Search with sophisticated Regular Expressions.
    *   **Shelfmark / Title:** Specific metadata lookup.
*   **Sort:** Click the "Shelfmark" or "Title" headers to organize results lexicographically.
*   **View:** Double-click a result to open the full Viewer, showing the manuscript image, text, and metadata.

### 2. Composition Search (Finding Parallels)
This tool breaks your source text into small "chunks" and searches for them in the corpus.
1.  **Input:** Paste your source text into the large text box, or click **Load Text File**.
2.  **Settings:**
    *   **Chunk:** Number of words per search block (Recommended: 4-7).
    *   **Max Freq:** Ignore common phrases that appear more than X times (reduces noise).
    *   **Filter >:** Move titles that appear many times to the "Appendix" group.
3.	**Exclude Manuscripts (Optional):** Enter known system numbers or shelfmarks of manuscripts that you want to filter.
3.  **Filter Text (Optional):** Click **Filter Text** to sort out known texts such as Bible or Mishna and Talmud.
4.  **Analyze:** Click the button to start. Results will appear in a tree structure.
5.  **Export:** Click **Save Report** to generate a detailed text file with all matches.

### 3. Browsing & Reading
*   Go to the **Browse Manuscript** tab.
*   Enter a **System ID** to load the manuscript cover, or an **FL ID** to jump to a specific image.
*   **View All:** Loads the entire manuscript text (all pages) into one scrollable window.
*   **Save:** Downloads the full text of the manuscript to your computer.

---

## 🎓 Credits & Acknowledgments

**Developed by:** Hillel Gershuni.

**Data Source:**
This tool relies on the **MiDRASH** project dataset:
> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17734473

**Libraries Used:**
*   PyQt6 (GUI Framework)
*   Tantivy (High-performance Search)
*   Requests & Urllib3 (Networking)

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---
# 📜 Genizah Search Pro 3.2

**כלי החיפוש והניתוח האולטימטיבי לקורפוס הגניזה הקהירית**

Genizah Search Pro היא אפליקציית שולחן עבודה עוצמתית שתוכננה עבור חוקרים העובדים עם כתבי יד מהגניזה הקהירית, ובמיוחד עם התעתיקים שהונגשו על ידי פרויקט MiDRASH. **גרסה 3** מציגה שיפור מקיף של חווית המשתמש, עם התמקדות במהירות, נגישות לנתונים (metadata) ורצף קריאה.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey) ![License](https://img.shields.io/badge/License-MIT-green)

> **⚠️ חשוב: הפעלה ראשונה**
> לאחר ההתקנה או השדרוג לגרסה 3, חובה לגשת ללשונית **Settings & About** (הגדרות ואודות) וללחוץ על **"Build / Rebuild Index"** (בניית אינדקס).
> פעולה זו נדרשת כדי לתמוך בתצוגה החדשה שורה-אחר-שורה ובאפשרויות החיפוש במטא-דאטה.

---

## 🚀 מה חדש בגרסה 3.2?
* **📊 אפשרויות ייצוא מורחבות:** כעת ניתן לייצא את תוצאות החיפוש ודוחות חיפוש החיבורים ישירות לפורמטים **Excel (.xlsx)** ו-**CSV**, בנוסף לקבצי טקסט רגילים.

---

## 🚀 מה חדש בגרסה 3.1?
* **🇮🇱 תמיכה מלאה בעברית:** התוכנה כעת עברה לוקליזציה מלאה! ניתן להעביר את הממשק לעברית (כולל התאמה אופטימלית לכיווניות מימין-לשמאל).
* **🔠 חיפוש וריאנטים משופר:** אלגוריתמי החיפוש שוכללו משמעותית כדי להתמודד טוב יותר עם שגיאות OCR מורכבות וחילופי תווים, מה שמספק תוצאות מדויקות ומקיפות יותר במצבי "Variants".
* **📄 דוחות מקצועיים:** כל הקבצים המיוצאים (תוצאות חיפוש, כתבי יד, דוחות חיפוש מקבילות) כוללים כעת אוטומטית כותרות ציטוט סטנדרטיות, מה שמקל על מתן קרדיט לפרויקט MiDRASH במחקר שלכם.
* **🛠️ יציבות ותיקוני באגים:** כולל שיפורי ביצועים כלליים ותיקונים לבעיות תצוגת מטא-דאטה.

---

## 🚀 מה חדש בגרסה 3.0?

* **⚡ מטא-דאטה אופליין מיידי:** התוכנה כוללת כעת מסד נתונים פנימי (`libraries.csv`) עם מעל 216,000 רשומות, המציג מספרי מדף (Shelfmarks) וכותרות **באופן מיידי** לצד תוצאות החיפוש.
* **🔎 חיפוש מטא-דאטה:** ניתן לחפש ישירות **מספר מדף** (למשל, "T-S NS 306.15") או **כותרת חיבור** ישירות משורת החיפוש הראשית.
* **📐 שבירת שורות מקורית:** טקסט כתב היד מוצג כעת שורה-אחר-שורה, בדיוק כפי שהוא מופיע בתעתיק המקורי, במקום כגוש טקסט רציף.
* **📊 תוצאות ניתנות למיון:** לחצו על כותרות הטבלה כדי למיין את התוצאות לפי מספר מדף, כותרת או רלוונטיות.
* **🖼️ תצוגה מקדימה ויזואלית:** תמונות כתב היד מוצגות כעת ישירות בתוצאות החיפוש ובלשונית העיון (עם מטמון חכם לטעינה מיידית).
* **📜 תצוגת כתב יד רציפה:** קראו את כתב היד בצורה טבעית. התכונה החדשה **"View All"** טוענת את כל עמודי כתב היד לתצוגת גלילה רציפה אחת.
* **💾 שמירת כתב יד שלם:** ייצוא הטקסט המלא של כתב יד מרובה-עמודים לקובץ טקסט יחיד ללמידה אופליין.

---

## ✨ תכונות עיקריות

### 🔍 מנוע חיפוש מתקדם
* **חיפוש מהיר במיוחד:** מופעל על ידי [Tantivy](https://github.com/quickwit-oss/tantivy) (מבוסס Rust) לתוצאות בפחות משנייה.
* **מצבי חיפוש:**
    * **חיפוש טקסט:** מדויק (Exact), עמום (Fuzzy/Levenshtein), ביטוי רגולרי (Regex), ו-**Variants** (תיקון OCR מותאם לעברית).
    * **חיפוש מטא-דאטה:** מצבים ייעודיים לחיפוש **כותרת** ו**מספר מדף**.
* **טבלת תוצאות עשירה:** עמודות ניתנות למיון, מטא-דאטה מיידי, קטעי טקסט (Snippets) ותצוגה מקדימה של תמונות.

### 🧩 חיפוש חיבורים (Composition Analysis)
* **מציאת חיבורים:** הדביקו טקסט מקור (למשל, פיוט ידוע או טקסט תלמודי) כדי למצוא את כל המופעים שלו בגניזה.
* **קיבוץ חכם:** מקבץ תוצאות אוטומטית לפי כותרת כתב היד (למשל, "משנה תורה") ומעביר לנספח התאמות שכנראה פחות יעניינו את המשתמש.
* **סינון מתקדם:** סינון תוצאות לפי מילים ספציפיות לפני או אחרי הניתוח.
* **תצוגת מסך מפוצל:** השוואת קטע הגניזה מול טקסט המקור שלכם זה לצד זה עם הדגשה אדומה מסונכרנת.

### 📚 דפדפן כתבי יד
* **חיפוש אחוד:** הזינו מספר מערכת (`...99`) או מזהה קובץ (`...FL`) כדי לקפוץ ישירות לעמוד ספציפי.
* **מנוע תמונות חכם:** מאחזר אוטומטית את התמונה הטובה ביותר הזמינה (באמצעות שרתי IIIF/Rosetta של הספרייה הלאומית) ושומר אותה במטמון מקומי.
* **קישורי עומק:** גישה בלחיצה אחת לקטלוג "כתיב" ולצפיין ברזולוציה גבוהה.

---

## 🛠 התקנה

1.  **הורדה:** הורידו את `GenizahSearchPro_v3.2.zip` העדכני מדף השחרורים (Releases).
2.  **חילוץ:** חלצו את ה-zip לתיקייה שבה נמצא הקובץ Transcriptions.txt (ראו למטה).
3.  **קבצי נתונים נדרשים:**
    ודאו שהקבצים הבאים נמצאים בתיקייה לצד `GenizahPro.exe`:
    * `Transcriptions.txt` (מערך הנתונים של MiDRASH, https://doi.org/10.5281/zenodo.17734473).
    * `libraries.csv` (קובץ מיפוי המטא-דאטה - **חדש בגרסה 3**).
4.  **הפעלה:** לחצו פעמיים על `GenizahSearchPro.exe`.
5.  **בהפעלה הראשונה, בצעו בניית אינדקס. זה ייקח מספר דקות.**

---

## 📖 מדריך שימוש

### 1. חיפוש רגיל (Standard Search)
* **Query (שאילתה):** הקלידו מילים, מספרי מדף או כותרות.
* **Mode (מצב):**
    * **Exact:** חיפוש מילים כפי שהן. ניתן להגדיר **Gap** (מרחק) בין מילים.
    * **Variants:** הטוב ביותר לחיפוש טקסט כללי (מתמודד עם שגיאות OCR).
    * **Fuzzy:** שיטה נוספת להתגברות על שגיאות OCR.
    * **Regex:** חיפוש באמצעות ביטויים רגולריים מתוחכמים.
    * **Shelfmark / Title:** חיפוש מטא-דאטה ספציפי.
* **Sort (מיון):** לחצו על כותרות "Shelfmark" או "Title" כדי לארגן את התוצאות בצורה לקסיקוגרפית.
* **View (צפייה):** לחצו פעמיים על תוצאה כדי לפתוח את הצפיין המלא, המציג את תמונת כתב היד, הטקסט והמטא-דאטה.

### 2. חיפוש חיבורים (מציאת מקבילות)
כלי זה מפרק את טקסט המקור שלכם ל"גושים" קטנים ומחפש אותם בקורפוס.
1.  **קלט:** הדביקו את טקסט המקור בתיבת הטקסט הגדולה, או לחצו על **Load Text File** (טען קובץ טקסט).
2.  **הגדרות:**
    * **Chunk:** מספר המילים בכל גוש חיפוש (מומלץ: 4-7).
    * **Max Freq:** התעלמות מביטויים נפוצים המופיעים יותר מ-X פעמים (מפחית רעש).
    * **Filter >:** העברת כותרות המופיעות פעמים רבות לקבוצת "Appendix" (נספח).
3.  **Exclude Manuscripts (אופציונלי):** הזינו מספרי מערכת או מספרי מדף ידועים שברצונכם לסנן.
4.  **Filter Text (אופציונלי):** לחצו על **Filter Text** כדי לסנן טקסטים ידועים כגון תנ"ך או משנה ותלמוד.
5.  **Analyze (נתח):** לחצו על הכפתור להתחלה. התוצאות יופיעו במבנה עץ.
6.  **Export (ייצוא):** לחצו על **Save Report** כדי לייצר קובץ טקסט מפורט עם כל ההתאמות.

### 3. עיון וקריאה
* עברו ללשונית **Browse Manuscript**.
* הזינו **System ID** כדי לטעון את כריכת כתב היד, או **FL ID** כדי לקפוץ לתמונה ספציפית.
* **View All:** טוען את כל טקסט כתב היד (כל העמודים) לחלון גלילה רציף אחד.
* **Save:** מוריד את הטקסט המלא של כתב היד למחשב שלכם.

---

## 🎓 קרדיטים ותודות

**פותח ע"י:** הלל גרשוני.

**מקור הנתונים:**
כלי זה מסתמך על מערך הנתונים של פרויקט **MiDRASH**:
> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17734473

**ספריות בשימוש:**
* PyQt6 (GUI Framework)
* Tantivy (High-performance Search)
* Requests & Urllib3 (Networking)

---

## 📄 רישיון

פרויקט זה מופץ תחת רישיון **MIT License** - ראו קובץ [LICENSE](LICENSE) לפרטים.