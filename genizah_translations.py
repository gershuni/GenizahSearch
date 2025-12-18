# -*- coding: utf-8 -*-
from version import APP_VERSION

TRANSLATIONS = {
    # --- General ---
    f"Genizah Search Pro V{APP_VERSION}": f"Genizah Search Pro V{APP_VERSION}",
    "Initializing components... Please wait.": "מאתחל רכיבים... אנא המתן.",
    "Fatal Error": "שגיאה קריטית",
    "Failed to initialize:\n{}": "אתחול נכשל:\n{}",
    "Close": "סגור",
    "Cancel": "ביטול",
    "OK": "אישור",
    "Apply": "החל",
    "Yes": "כן",
    "No": "לא",
    "Error": "שגיאה",
    "Warning": "אזהרה",
    "Information": "מידע",
    "Ready.": "מוכן.",
    "Loading...": "טוען...",
    "Please wait while components load...": "אנא המתן בעת טעינת הרכיבים...",
    "Stop": "עצור",
    
    # --- Connectivity / Status ---
    "Online": "מקוון",
    "Offline": "לא מקוון",
    "Degraded": "זמין חלקית",
    "Checking connectivity...": "בודק קישוריות...",
    "All external services responding.": "כל השירותים החיצוניים מגיבים.",
    "All services healthy": "כל השירותים תקינים",
    "No internet connection": "אין חיבור לאינטרנט",
    "NLI service unavailable": "שירות הספרייה הלאומית אינו זמין",
    "AI provider unavailable": "ספק הבינה המלאכותית אינו זמין",
    "Connectivity Error": "שגיאת קישוריות",
    "Components loaded. Ready.": "הרכיבים נטענו. המערכת מוכנה.",

    # --- Tabs ---
    "Search": "חיפוש",
    "Composition Search": "חיפוש חיבורים", # Updated
    "Browse Manuscript": "עיון בכתב יד",
    "Settings & About": "הגדרות ואודות",

    # --- Main Search Tab ---
    "Search terms, title or shelfmark...": "מילות חיפוש, כותרת או מספר מדף...",
    "Query:": "שאילתא:",
    "Mode:": "מצב:",
    "Gap:": "מרחק:",
    "Gap": "מרחק",
    "Maximum word distance (0 = Exact phrase)": "מרחק מקסימלי בין מילים (0 = ביטוי מדויק)",
    "Search": "חפש",
    "🤖 AI Assistant": "🤖 עוזר AI",
    "Generate Regex with Gemini AI": "צור ביטוי רגולרי באמצעות AI",
    "Search Help": "עזרה בחיפוש",
    "System ID": "מספר מערכת",
    "System ID:": "מספר מערכת:",
    "Shelfmark": "מספר מדף",
    "Title": "כותרת",
    "Snippet": "קטע",
    "Img": "תמונה",
    "Src": "מקור",
    "Export Results": "יצא תוצאות",
    "Reload shelfmark/title metadata": "רענן נתוני מספר מדף/כותרת",
    "Stop metadata loading": "עצור טעינת נתונים",
    "No results found.": "לא נמצאו תוצאות.",
    "Found {}. Loading metadata...": "נמצאו {}. טוען נתונים...",
    "Metadata already loaded for {} items.": "נתונים נטענו עבור {} פריטים.",
    "Stopping metadata load...": "עוצר טעינת נתונים...",
    "Metadata loaded: {}/{}": "נתונים נטענו: {}/{}",
    "Metadata Error": "שגיאת נתונים",
    "Metadata load cancelled. Loaded {}/{}.": "טעינת נתונים בוטלה. נטענו {}/{}.",
    "Loaded {} items.": "נטענו {} פריטים.",

    # --- Search Modes & Tooltips ---
    "Exact": "מדויק",
    "Variants (?)": "וריאנטים (?)",
    "Extended (??)": "מורחב (??)",
    "Maximum (???)": "מקסימלי (???)",
    "Fuzzy (~)": "עמום (~)",
    "Regex": "ביטוי רגולרי",
    "Exact match": "התאמה מדויקת",
    "Basic variants: ד/ר, ה/ח, ו/י/ן etc.": "וריאנטים בסיסיים: ד/ר, ה/ח, ו/י/ן וכו'",
    "Extended variants: Adds more swaps (א/ע, ק/כ etc.)": "וריאנטים מורחבים: מוסיף עוד חילופים (א/ע, ק/כ וכו')",
    "Maximum variants: Very broad search": "וריאנטים מקסימליים: חיפוש רחב מאוד",
    "Fuzzy search: Levenshtein distance": "חיפוש עמום: מרחק לווינשטיין",
    "Regex: Use AI Assistant for complex patterns": "ביטוי רגולרי: השתמש בעוזר ה-AI לתבניות מורכבות",
    "Search in Title metadata": "חיפוש בכותרות (Metadata)",
    "Search in Shelfmark metadata": "חיפוש במספרי מדף (Metadata)",

    # --- Composition Tab ---
    "Composition Title": "כותרת החיבור",
    "Paste source text...": "הדבק טקסט מקור...",
    "Load Text File": "טען קובץ טקסט",
    "Exclude Manuscripts": "החרג כתבי יד",
    "Filter Text": "סנן טקסט",
    "Excluded: {}": "הוחרגו: {}",
    "Chunk: ": "גוש: ",
    "Words per search block (Rec: 5-7)": "מילים בכל גוש חיפוש (מומלץ: 5-7)",
    "Max Freq: ": "תדירות מקס': ",
    "Ignore phrases appearing > X times (filters common phrases)": "התעלם מביטויים המופיעים מעל X פעמים (מסנן ביטויים נפוצים)",
    "Filter > ": "סינון > ",
    "Move titles appearing > X times to Appendix": "העבר כותרות המופיעות מעל X פעמים לנספח",
    "Analyze Composition": "נתח חיבור",
    "Analyze": "נתח",
    "Context": "הקשר",
    "Save Report": "שמור דוח",
    "Composition Help": "עזרה בחיפוש מקבילות",
    "Scanning chunks...": "סורק...", 
    "No composition matches found.": "לא נמצאו התאמות.",
    "Group Results?": "לקבץ תוצאות?",
    "Grouping may take longer and relies on NLI metadata. Group now?": "הקיבוץ עשוי להימשך זמן מה ומתבסס על נתוני הספרייה הלאומית. לקבץ כעת?",
    "Grouping compositions...": "מקבץ חיבורים...",
    "Grouping Error": "שגיאה בקיבוץ",
    "Grouping stopped. Showing ungrouped results.": "הקיבוץ נעצר. מציג תוצאות לא מקובצות.",
    "Main ({})": "ראשי ({})",
    "Appendix ({})": "נספח ({})",
    "Filtered by Text ({})": "סונן לפי טקסט ({})",
    "Known Manuscripts ({})": "כתבי יד ידועים ({})",
    "Filtered Main ({})": "ראשי מסונן ({})",
    "Filtered Appendix ({})": "נספח מסונן ({})",
    "Untitled Composition": "חיבור ללא כותרת",
    "Metadata Missing": "חסרים נתונים",
    "Shelfmark/Title/Page info missing for some items.\nContinue using system IDs? Choose No to load metadata first.": "חסרים פרטי מספר מדף/כותרת/עמוד לחלק מהפריטים.\nלהמשיך עם מספרי מערכת? בחר 'לא' כדי לטעון נתונים תחילה.",
    "Loading shelfmarks and titles...": "טוען מספרי מדף וכותרות...",
    "Loading metadata was cancelled.": "טעינת הנתונים בוטלה.",
    "Fetching metadata before export...": "טוען נתונים לפני ייצוא...",
    "Loading missing metadata...": "טוען נתונים חסרים...",
    "No items.": "אין פריטים.",

    # --- Browse Tab ---
    "Enter System ID...": "הכנס מספר מערכת...",
    "Go": "עבור",
    "Enter ID to browse.": "הכנס מספר לדפדוף.",
    "Ktiv": "כתיב",
    "Open in Ktiv Website": "פתח באתר 'כתיב'",
    "View All": "הכל",
    "Show full text continuously (Infinite Scroll)": "הצג טקסט מלא ברצף (גלילה אינסופית)",
    "Save": "שמור",
    "Save full manuscript to file": "שמור כתב יד מלא לקובץ",
    "No Preview": "אין תצוגה",
    "Loading Meta...": "טוען מידע...",
    "No Image": "אין תמונה",
    "Waiting...": "ממתין...",
    "Not found or end.": "לא נמצא או סוף הקובץ.",
    "Nav": "ניווט",
    "Loading full manuscript...": "טוען כתב יד מלא...",
    "Could not load full text.": "לא ניתן לטעון טקסט מלא.",
    "Continuous View": "תצוגה רציפה",
    "Save Manuscript": "שמור כתב יד",
    "Saved": "נשמר",
    "Manuscript saved to:\n{}": "כתב היד נשמר אל:\n{}",
    "<< Prev": ">> הקודם", 
    "Next >>": "הבא <<",
    "Page / Image": "עמוד / תמונה", 
    "Image": "תמונה",

    # --- Settings Tab ---
    "Data & Index": "נתונים ואינדקס",
    "Download Transcriptions (Zenodo)": "הורד תעתיקים (Zenodo)",
    "Build / Rebuild Index": "בנה / בנייה מחדש של אינדקס",
    "AI Configuration": "הגדרות AI",
    "Provider:": "ספק:",
    "Model:": "מודל:",
    "API Key:": "מפתח API:",
    "Save Settings": "שמור הגדרות",
    "About": "אודות",
    "Citation:": "ציטוט:",
    "Copy": "העתק",
    "Copy Citation": "העתק ציטוט",
    "Citation copied to clipboard!": "הציטוט הועתק ללוח!",
    "Copied": "הועתק",
    "Missing Key": "מפתח חסר",
    "Please configure your AI Provider & Key in Settings.": "אנא הגדר ספק AI ומפתח בהגדרות.",
    "Saved to {}": "נשמר אל {}",

    # --- Indexing ---
    "Index not found.\nWould you like to build it now?\n(Requires 'Transcriptions.txt' next to this app)": "האינדקס לא נמצא.\nהאם ברצונך לבנות אותו כעת?\n(דורש את הקובץ 'Transcriptions.txt' לצד התוכנה)",
    "Index Missing": "אינדקס חסר",
    "Start indexing?": "להתחיל ביצירת אינדקס?",
    "Index": "אינדקס",
    "Indexing... %p%": "ממפתח... %p%",
    "Indexing complete": "הבנייה הושלמה",
    "Indexing failed": "הבנייה נכשלה",
    "Indexing Error": "שגיאה בבנייה",
    "Done": "בוצע",
    "Indexing complete. Documents indexed: {}": "בניית האינדקס הושלמה. מסמכים שמופתחו: {}",

    # --- Result Dialog ---
    "Manuscript Viewer": "צפייה בכתב יד",
    "Result {} of {}": "תוצאה {} מתוך {}", 
    "Go to Ktiv": "עיון בכתיב", 
    "Sys": "מס' מערכת", 
    "FL": "מס' קובץ", 
    "Image:": "תמונה:", 
    "Manuscript Text": "טקסט כתב היד", 

    "◀ Prev Result": "▶ לתוצאה קודמת",
    "Next Result ▶": "לתוצאה הבאה ◀",

    "Match Context (Source)": "הקשר (מקור)",

    # --- AI Dialog ---
    "AI Regex Assistant ({})": "עוזר ביטויים רגולריים - AI ({})",
    "Describe pattern (e.g. 'Word starting with Aleph')...": "תאר תבנית (למשל 'מילה המתחילה באות א')...",
    "Send": "שלח",
    "Generated Regex will appear here.": "הביטוי הרגולרי שיופק יופיע כאן.",
    "Thinking...": "חושב...",
    "Use this Regex": "השתמש בביטוי זה",
    "Hello! I can help you build Regex for Hebrew manuscripts.": "שלום! אני יכול לעזור לך לבנות ביטויים רגולריים לכתבי יד עבריים.",
    "System": "מערכת",
    "You": "אתה",
    "Gemini": "ג'מיני",

    # --- Exclude Dialog ---
    "Enter system IDs or shelfmarks to exclude (one per line).": "הכנס מספרי מערכת או מספרי מדף להחרגה (אחד בשורה).",
    "Load from File": "טען מקובץ",

    # --- Filter Text Dialog ---
    "Enter text to filter results (results found in this text will be moved to a separate list):": "הכנס טקסט לסינון תוצאות (תוצאות שיימצאו בטקסט זה יועברו לרשימה נפרדת):",
    "Paste text here...": "הדבק טקסט כאן...",
    "Load Text": "טען טקסט",
    "Load": "טען",

    # --- Export Report Headers ---
    "Composition Search": "חיפוש חיבורים", 
    "COMPOSITION REPORT SUMMARY": "סיכום דוח חיפוש חיבורים", 
    "Total Results": "סך כל התוצאות",
    "Main Manuscripts": "כתבי יד ראשיים",
    "Main Appendix": "נספח ראשי",
    "MAIN APPENDIX SUMMARY": "סיכום נספח ראשי", 
    "FILTERED APPENDIX SUMMARY": "סיכום נספח מסונן", 
    "KNOWN MANUSCRIPTS SUMMARY": "סיכום כתבי יד ידועים", 
    "No known manuscripts were excluded.": "לא הוחרגו כתבי יד ידועים.", 
    "MAIN MANUSCRIPTS": "כתבי יד ראשיים", 
    "Known Manuscripts": "כתבי יד ידועים",
    "Excluded": "הוחרגו", 
    "Filtered by Text": "סונן לפי טקסט",
    "Search_Results": "תוצאות_חיפוש",
    "Composition_Report": "דוח_חיפוש_חיבורים",
    "Source Context": "טקסט מקור",
    "Manuscript": "כתב יד",
    "Source": "מקור",
    "MS": "כ\"י",

    # --- HTML Help Content (Search) ---
    "SEARCH_HELP_HTML": """<div dir='rtl'><h3>מצבי חיפוש</h3>
    <ul>
    <li><b>מדויק:</b> מוצא התאמות מדויקות בלבד.</li>
    <li><b>וריאנטים (?):</b> שגיאות OCR בסיסיות.</li>
    <li><b>מורחב (??):</b> וריאנטים נוספים.</li>
    <li><b>מקסימלי (???):</b> חילופים אגרסיביים (השתמש בזהירות).</li>
    <li><b>עמום (~):</b> מרחק לווינשטיין (1-2 שגיאות).</li>
    <li><b>ביטוי רגולרי:</b> תבניות מורכבות (היעזר ב-AI).</li>
    <li><b>כותרת:</b> חיפוש בכותרות החיבורים.</li>
    <li><b>מספר מדף:</b> חיפוש במספרי מדף.</li>
    </ul>
    <hr>
    <b>מרחק (Gap):</b> מרחק מרבי בין מילים (לא רלוונטי לכותרת/מספר מדף).</div>""",

    # --- HTML Help Content (Composition) ---
    "COMP_HELP_HTML": """<div dir='rtl'><h3>חיפוש מקבילות</h3>
    <p>מוצא מקבילות בין טקסט המקור לגניזה.</p>
    <ul>
    <li><b>גוש (Chunk):</b> מספר מילים בכל יחידת חיפוש (מומלץ 5-7).</li>
    <li><b>תדירות מקס':</b> סנן ביטויים המופיעים מעל X פעמים.</li>
    <li><b>סינון >:</b> קבץ תוצאות אם כותרת מופיעה בתדירות גבוהה (העבר לנספח).</li>
    </ul></div>""",

    # --- HTML About ---
    "ABOUT_HTML": f"""
        <style>
            h3 {{ margin-bottom: 0px; margin-top: 10px; }}
            p {{ margin-top: 5px; margin-bottom: 5px; line-height: 1.4; }}
            a {{ color: #2980b9; text-decoration: none; }}
        </style>
        <div style='font-family: Arial; font-size: 13px;' dir='rtl'>
            <div style='text-align:center;'>
                <h2 style='margin-bottom:5px;'>Genizah Search Pro {APP_VERSION}</h2>
                <p style='color: #7f8c8d;'>פותח על ידי הלל גרשוני (<a href='mailto:gershuni@gmail.com'>gershuni@gmail.com</a>)</p>
            </div>
            <hr>

            <h3>מוקדש לזכרו של מורנו האהוב, פרופ' מנחם כהנא ז\"ל</h3>
            <h3>קרדיטים</h3>
            <p>כלי זה פותח בסיוע <b>Gemini 3.0</b> ו-<b>GPT 5.1</b>. תודתי נתונה לאבי שמידמן, אלישע רוזנצוייג, אפרים מאירי, אלעזר גרשוני, איתי קגן ואלנתן חן על עצותיהם ותמיכתם.</p>

            <h3>מקור הנתונים ותודות</h3>
            <p>תוכנה זו מבוססת על מאגר התעתיקים שנוצר במסגרת <b>פרויקט MiDRASH</b>. אני מודה לראשי הפרויקט – דניאל שטוקל בן עזרא, מרינה רוסטוב, נחום דרשוביץ, אבי שמידמן ויהודית שלנגר – ולצפרה זיו וליצחק גילה מהספרייה הלאומית.תודות רבות גם לשאר חברי הצוות: לואיג'י במבאצ'י, שרווה גוגאוולה, דריה וסיוטינסקי שפירא, משה לביא, אלנה לולי, חיים לפין, נורית עזר, בראת קוראר-ברכאת, בנימין קיסלינג וורד רזיאל קרצ'מר.</p>
            <p>הנגשת מאגר כה מורכב וערך לציבור היא צעד משמעותי למדע הפתוח, ואני מעריך מאוד את נדיבותם באפשרם לכל אחד לגשת לטקסטים אלו.</p>
            <h3>רישיון</h3>

            <p>המאגר הבסיסי מופץ תחת רישיון Creative Commons Attribution 4.0 International (<a href='https://creativecommons.org/licenses/by/4.0/'>CC BY 4.0</a>)</p>

            <h3>ציטוט</h3>
            <p>אם אתם משתמשים בתוצאות אלו למחקרכם, אנא צטטו את יוצרי המאגר: Stoekl Ben Ezra, Daniel, Luigi Bambaci, Benjamin Kiessling, Hayim Lapin, Nurit Ezer, Elena Lolli, Marina Rustow, et al. MiDRASH Automatic Transcriptions. Data set. Zenodo, 2025. <a href='https://doi.org/10.5281/zenodo.17734473'>https://doi.org/10.5281/zenodo.17734473</a>. ניתן לציין גם שהשתמשתם בתוכנה זו: Genizah Search Pro של הלל גרשוני.</p>
        </div>
        """,

    "Restart Required": "נדרש אתחול מחדש",
    "Please restart the application for the language change to take effect.": "אנא הפעילו מחדש את התוכנה כדי שהשינוי בשפה ייכנס לתוקף.",
    "Language": "שפה",
    
    # --- Report Headers ---
    "REPORT_CREDIT_TXT": """הופק באמצעות Genizah Search Pro
מקור הנתונים: תעתיקי MiDRASH (שטוקל בן עזרא ואח', 2025)
המאגר זמין בכתובת: https://doi.org/10.5281/zenodo.17734473
================================================================================
"""
}
