שילבתי את ההערות החדשות של גרסה 4.1 (רשימות אישיות וכו') לתוך המבנה המקצועי והמורחב של הערות גרסה 4.0 שסיפקת, כולל עדכון הוראות ההתקנה לשימוש ב-Installer החדש.

הנה קובץ ה-`README.md` המעודכן והמלוטש:

---

# Genizah Search Pro 4.1.1

**From Search Engine to Research Suite**

Genizah Search Pro has evolved. Version 4.0 marked the transformation from a text retrieval utility into a **comprehensive research environment** for the Cairo Genizah.
**Version 4.1** builds on this foundation by introducing **Personal Lists** management, allowing researchers to curate, organize, and filter their own collections of manuscripts.

> **⚠️ Important: Rebuild the index after upgrading to this version!**

---

## 🆕 What's New in Version 4.1?

### 📋 Personal Lists Management

A complete workflow for managing your research corpus.

* **Create & Curate:** New tab for creating and organizing personal lists of manuscripts.
* **Browse by List:** A new side panel in the Browse tab allows for easy navigation through your custom lists.
* **List Filtering:** Filter search results directly based on your personal lists to focus on specific sub-corpora.

### 🛠️ Stability & Interface Refinements (v4.0.1 - v4.1.1)

* **Compact Context View:** The source context view is now a compact panel at the bottom of the interface, providing clearer highlighting without cluttering the main results.
* **Visual Fixes:** Corrected list preview image loading and star icon alignment in search results.
* **Bug Fixes:** Resolved issues causing duplicate search results.
* **Reports:** Generated reports are now consistently saved to the user’s *Documents* directory.

---

## 🌟 Core Features (Major v4.0 Updates)

### 🖼️ Integrated Visual Analysis (IIIF)

Research workflow is unified, allowing users to consult manuscript images without leaving the software.

* **In-App Viewer:** Direct fetching of high-resolution images from the National Library of Israel and Cambridge University Library.
* **Sequential Navigation:** Browse through pages and manuscripts continuously.
* **Image Tools:** Built-in Zoom and Rotation controls.

### 📖 Oxford Bodleian Integration

Full support for Oxford Bodleian Library manuscripts with proper **Neubauer catalog** integration.

* **Part-Based Browsing:** View manuscripts as complete codicological units, not just individual folios.
* **Rich Metadata:** Display catalog information directly from the Bodleian Hebrew manuscripts database.
* **Dual Navigation:** Toggle between navigating by Parts (codicological units) or by Folios (individual pages).

### 🧪 Lab Mode (Experimental)

A powerful search engine for detecting parallels, based on the algorithm by **Shmidman, Koppel, and Porat (2016)**.

* **Proximity Detection:** Uses "rare letter" encoding to detect matches even in cases of spelling variations or OCR errors.
* **Deep Scan:** Comprehensive scanning for complex queries.

---

## 🚀 Additional Capabilities

* **🔎 Cross-Page Search:** Search results span across page boundaries, preserving context even when matches cross folio edges.
* **⚡ Enhanced Export:** Option to export only selected rows to Excel, CSV, or DOCX.
* **🔍 Find in Text:** Quick in-pane search with highlighting across all text views.
* **🧩 Composition Search:** Detect parallels between a source text and the entire corpus using chunk analysis.

---

## 🛠 Installation Guide

**Simplified Deployment:** The software is now distributed via a standard Windows Installer.

1. **Download:** Get the `GenizahSearchPro_V4.1.1_Setup.exe` file from the **Assets** section below.
2. **Install:** Run the installer and follow the on-screen instructions.
3. **Data Setup:**
* The software requires the **MiDRASH** project dataset (`Transcriptions.txt`).
* Upon first launch, if the file is not detected, you will be prompted to select its location. We recommend placing it in your `Documents/GenizahSearchPro` folder.



---

## 🎓 Credits & Data

* **Development:** Hillel Gershuni
* **Data Source:** Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*. ([doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473))
* **Lab Mode Algorithm:** Based on [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715).

**Acknowledgments:**
This software was developed with the support of **DICTA**.
Assisted by **Gemini 3.0** and **GPT 5.1-5.2**.
Special thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.

---

### Hebrew (עברית)

# Genizah Search Pro 4.1.1

**ממנוע חיפוש לכלי מחקר: עדכון משמעותי**

גרסה 4.0 סימנה את המעבר של Genizah Search Pro מכלי לאחזור טקסט ל**סביבת מחקר כוללת** לגניזה הקהירית.
**גרסה 4.1** ממשיכה מגמה זו ומוסיפה ניהול **רשימות אישיות**, המאפשר לחוקרים לארגן, לשמור ולסנן אוספים מותאמים אישית של כתבי יד.

> **⚠️ חשוב: יש לבנות מחדש את האינדקס לאחר העדכון לגרסה זו!**

---

## 🆕 מה חדש בגרסה 4.1?

### 📋 ניהול רשימות אישיות

זרימת עבודה מלאה לניהול קורפוס המחקר האישי שלך.

* **יצירה וניהול:** לשונית חדשה ליצירה וארגון של רשימות כתבי יד אישיות.
* **עיון לפי רשימה:** פאנל צד חדש בלשונית העיון ("Browse") מאפשר ניווט נוח ומהיר בתוך הרשימות שיצרת.
* **סינון בחיפוש:** אפשרות לסינון תוצאות החיפוש אך ורק מתוך הרשימות האישיות, להתמקדות בתת-קורפוס ספציפי.

### 🛠️ שיפורי יציבות וממשק (v4.0.1 - v4.1.1)

* **תצוגת הקשר (Context) קומפקטית:** תצוגת ההקשר אורגנה מחדש לפאנל בתחתית הממשק, להדגשה ברורה ומניעת עומס בחלון התוצאות.
* **תיקונים ויזואליים:** תוקן היישור של אייקון הכוכב ותצוגת התמונות המקדימה ברשימות.
* **תיקוני באגים:** נפתרה בעיה של כפילות בתוצאות החיפוש.
* **דוחות:** דוחות נשמרים כעת באופן עקבי בתיקיית המסמכים (Documents) של המשתמש.

---

## 🌟 תכונות ליבה (עדכוני v4.0)

### 🖼️ ניתוח חזותי משולב (IIIF)

זרימת העבודה המחקרית מאוחדת כעת, ומאפשרת עיון בתמונות כתבי היד מבלי לצאת מהתוכנה.

* **צפיין מובנה:** טעינה ישירה של תמונות ברזולוציה גבוהה מהספרייה הלאומית וספריית אוניברסיטת קיימברידג'.
* **ניווט רציף:** דפדוף בין עמודים ומעבר סדרתי בין כתבי יד בקורפוס.
* **כלי תצוגה:** אפשרויות זום וסיבוב (Rotation) מובנות.

### 📖 אינטגרציית אוקספורד-בודלי

תמיכה מלאה בכתבי היד של ספריית הבודליאנה באוקספורד עם שילוב נכון של **קטלוג נויבאואר**.

* **עיון לפי יחידות קודיקולוגיות (Parts):** צפייה בכתבי יד כיחידות שלמות, ולא רק כדפים בודדים.
* **מטא-דאטה עשיר:** הצגת מידע קטלוגי ישירות ממאגר כתבי היד העבריים של הבודליאנה.
* **ניווט כפול:** אפשרות לעבור בין ניווט לפי יחידות קודיקולוגיות (Parts) לבין ניווט לפי דפים בודדים (Folios).

### 🧪 מצב מעבדה (Lab Mode) - ניסיוני

מנוע חיפוש עוצמתי לאיתור מקבילות, המבוסס על האלגוריתם של **שמידמן, קופל ופורת (2016)**.

* **זיהוי קרבה:** שימוש בקידוד "אותיות נדירות" לאיתור התאמות גם במקרים של שינויי כתיב או שגיאות OCR.
* **סריקה עמוקה:** סריקה מקיפה לשאילתות מורכבות.

---

## 🚀 יכולות נוספות

* **🔎 חיפוש חוצה-דפים:** תוצאות החיפוש משתרעות מעבר לגבולות הדף ושומרות על ההקשר.
* **⚡ ייצוא משופר:** אפשרות לייצא רק שורות נבחרות לאקסל, CSV או Word.
* **🔍 חיפוש בתוך הטקסט:** חיפוש מהיר עם הדגשה בתוך חלונית הטקסט.
* **🧩 חיפוש מקבילות:** איתור מקבילות בין טקסט מקור לקורפוס הגניזה באמצעות חלוקה למקטעים.

---

## 🛠 הוראות התקנה

**התקנה פשוטה:** התוכנה מופצת כעת באמצעות קובץ התקנה סטנדרטי (Installer).

1. **הורדה:** הורידו את הקובץ `GenizahSearchPro_V4.1.1_Setup.exe` מאזור ה-**Assets** למטה.
2. **התקנה:** הריצו את קובץ ההתקנה ועקבו אחר ההוראות.
3. **הגדרת נתונים:**
* התוכנה דורשת את קובץ הנתונים של פרויקט **MiDRASH** (`Transcriptions.txt`).
* בהפעלה הראשונה, אם הקובץ לא יזוהה, תתבקשו לבחור את מיקומו. אנו ממליצים לשמור אותו בתיקייה `Documents/GenizahSearchPro`.



---

## 🎓 קרדיטים

* **פיתוח:** הלל גרשוני
* **מקור הנתונים:** Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*.
* **אלגוריתם מצב מעבדה:** מבוסס על [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715).

**תודות:**
התוכנה פותחה בתמיכת **דיקטה (DICTA)**.
בסיוע **Gemini 3.0** ו-**GPT 5.1-5.2**.
תודה מיוחדת לאבי שמידמן, אלישע רוזנצוייג, אפרים מאירי, אלעזר גרשוני, איתי קגן, אלנתן חן ועדיאל ברויאר.