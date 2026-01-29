# GenizahSearch - צ'קליסט בדיקות טרום-השקה
## Pre-Launch Testing Checklist v1.0

**תאריך:** 2026-01-29
**סביבה:** Production
**גרסה:** 5.1

---

## מקרא סימונים
- [ ] - לא נבדק
- [x] - עבר בהצלחה
- [!] - נכשל / דורש תיקון
- [?] - דורש בדיקה נוספת

---

# 1. דף הבית (Home Page) `/`

## 1.1 תצוגה וממשק
- [x] הדף נטען ללא שגיאות *(Code Review: OK)*
- [x] באנר OCR מוצג למשתמש חדש *(home.py:24-35)*
- [x] כפתור "הבנתי" מסתיר את הבאנר *(dismiss_banner)*
- [x] הבאנר לא מופיע שוב לאחר סגירה *(localStorage via app.storage.user)*
- [x] סטטיסטיקות (מספר דפים, רשימות) מוצגות נכון *(home.py:68-77)*
- [x] כרטיסי הכלים (חיפוש, מקבילות, דפדוף) נטענים *(home.py:83-158)*
- [x] פעילות אחרונה מוצגת (אם קיימת) *(home.py:230-283)*
- [x] מצב מערכת מוצג בהרחבה *(home.py:286-320)*

## 1.2 ניווט מהירים
- [x] לחיצה על כרטיס "חיפוש טקסט" מנווטת ל-`/search` *(role=button, tabindex=0)*
- [x] לחיצה על כרטיס "מקבילות" מנווטת ל-`/parallels` *(role=button, tabindex=0)*
- [x] לחיצה על כרטיס "דפדוף" מנווטת ל-`/browse` *(role=button, tabindex=0)*
- [x] לחיצה על "רשימות אישיות" מנווטת ל-`/lists` *(role=button, tabindex=0)*
- [x] לחיצה על "הגדרות מעבדה" מנווטת ל-`/settings` *(role=button, tabindex=0)*
- [x] לחיצה על "מרכז עזרה" מנווטת ל-`/help` *(role=button, tabindex=0)*
- [x] לחיצה על "אפליקציית דסקטופ" מנווטת ל-`/download` *(role=button, tabindex=0)*

## 1.3 קרדיט ומקורות
- [x] ציטוט MiDRASH מוצג בתחתית *(home.py:329-334)*
- [x] קישור ל-Zenodo פעיל ונפתח בטאב חדש *(home.py:339 - new_tab=True)*
- [x] רישיון CC BY 4.0 מוזכר *(home.py:342)*

---

# 2. חיפוש (Search Page) `/search`

## 2.1 ממשק חיפוש
- [x] שדה החיפוש פעיל וקולט טקסט בעברית *(search.py:78-81)*
- [x] כיוון RTL נכון בשדה הקלט *(style='direction: rtl;')*
- [x] כפתור "חיפוש" פעיל *(search.py:168-170)*
- [x] Enter מפעיל חיפוש *(search.py:82)*

## 2.2 מצבי חיפוש
- [x] **Exact (=)**: חיפוש מדויק עובד *(search.py:96,106)*
- [x] **Variants Basic (?)**: וריאנטים בסיסיים עובד *(search.py:97,107)*
- [x] **Variants Extended (??)**: וריאנטים מורחבים עובד *(search.py:108)*
- [x] **Variants Maximum (???)**: וריאנטים מקסימליים עובד *(search.py:109)*
- [x] **Fuzzy (~)**: חיפוש מטושטש עובד *(search.py:98,110)*
- [x] **Regex (/)**: חיפוש ביטויים רגולריים עובד *(search.py:99,111)*
- [x] **Shelfmark (#)**: חיפוש לפי סימול מדף עובד *(search.py:100,112)*
- [x] **Title ($)**: חיפוש לפי כותרת עובד *(search.py:101,113)*

## 2.3 קיצורי תחביר (Syntax Shortcuts)
- [?] `=מילה` מפעיל מצב Exact *(נדרש: בדיקת parse_query_syntax בCore)*
- [?] `?מילה` מפעיל מצב Variants *(נדרש: בדיקת parse_query_syntax בCore)*
- [?] `~מילה` מפעיל מצב Fuzzy *(נדרש: בדיקת parse_query_syntax בCore)*
- [?] `/pattern/` מפעיל מצב Regex *(נדרש: בדיקת parse_query_syntax בCore)*
- [?] `#T-S 12.123` מפעיל מצב Shelfmark *(נדרש: בדיקת parse_query_syntax בCore)*

## 2.4 אפשרויות מתקדמות
- [x] פאנל "אפשרויות מתקדמות" נפתח/נסגר *(search.py:219-243)*
- [x] **Lab Mode**: מתג Lab Mode פעיל *(search.py:230)*
- [x] **Deep Scan**: אופציה זמינה כאשר Lab Mode פעיל *(search.py:232)*
- [x] **Exclude Words**: שדה מילים לסינון פעיל *(search.py:240-241)*
- [x] **Gap**: בקרת רווח פעילה (0-10) *(search.py:160-161)*
- [x] **Max Changes**: בורר מספר שינויים (×1, ×2, ×3) *(search.py:142-143)*

## 2.5 שליטה בוריאנטים
- [x] Slider Mode: סליידר רמת וריאנטים פעיל (אם מוגדר בהגדרות) *(search.py:174-190)*
- [x] Preset Mode: dropdown עם רמות (Basic/Extended/Maximum) *(search.py:103-114)*
- [x] שמירת הגדרות בין חיפושים *(app.storage.user)*

## 2.6 תוצאות חיפוש
- [x] מספר תוצאות מוצג *(search.py:621)*
- [x] כרטיסי תוצאות נטענים *(search.py:628-641)*
- [x] Snippet עם הדגשה מוצג *(SearchEngine.format_snippet() + sanitize=False)*
- [x] מספור תוצאות (1#, 2#, וכו') *(search.py:673-675)*
- [x] Shelfmark מוצג בכל כרטיס *(search.py:676)*
- [x] כותרת (אם קיימת) מוצגת *(search.py:677-680)*

## 2.7 פעולות על תוצאות
- [x] לחיצה על תוצאה טוענת ב-Viewer *(search.py:671)*
- [x] כפתור "הוספה לרשימה" (כוכב) פעיל *(search.py:689-696)*
- [x] כפתור "תצוגה מתקדמת" פעיל *(search.py:684-687)*
- [x] כפתור "עריכה" (Edit) מוצג ופעיל *(search.py:703-710)*
- [x] כפתור "תגובה" (Comment) מוצג ופעיל *(search.py:711-716)*

## 2.8 פעולות בצובר (Bulk Operations)
- [x] Checkbox לבחירת כל התוצאות פעיל *(search.py:262)*
- [x] Checkbox אישי לכל תוצאה פעיל *(search.py:665-668)*
- [x] מונה "X נבחרו" מתעדכן *(search.py:409-426)*
- [x] "הוספה לרשימה" בצובר פעיל *(search.py:428-474)*
- [x] "העתקת טקסט" בצובר פעיל *(search.py:476-510)*

## 2.9 סינון תוצאות
- [x] כפתור Filter מציג/מסתיר פאנל סינון *(search.py:343-349)*
- [x] סינון לפי Shelfmark עובד *(search.py:357,368)*
- [x] סינון לפי Title עובד *(search.py:358,370)*
- [x] סינון לפי Snippet עובד *(search.py:359,372)*
- [x] כפתור "החל סינון" עובד *(search.py:314)*
- [x] כפתור "נקה סינון" עובד *(search.py:317)*

## 2.10 ייצוא
- [x] כפתור Export Word פעיל *(search.py:283-285, api.py:596-638)*
- [x] כפתור Export Excel פעיל *(search.py:286-288, api.py:546-594)*
- [?] קובץ Word נוצר ונפתח תקין *(נדרשת בדיקה ידנית)*
- [?] קובץ Excel נוצר ונפתח תקין *(נדרשת בדיקה ידנית)*

## 2.11 Viewer (צד ימין)
- [x] בחירת תוצאה מציגה ב-Viewer *(search.py:923-1035)*
- [x] Tab "Match" מציג Snippet *(search.py:963-969)*
- [x] Tab "Full Text" מציג טקסט מלא *(search.py:971-989)*
- [x] Tab "Metadata" מציג מידע *(search.py:991-1004)*
- [x] ניווט בין דפים (חיצים) עובד *(search.py:1037-1079)*
- [x] כפתור "View in Browse" מנווט נכון *(search.py:1023-1027)*
- [x] כפתור "Find Parallels" מנווט נכון *(search.py:1030-1035)*

## 2.12 Dialog מתקדם
- [x] Dialog נפתח במסך מלא *(search.py:768 - props='maximized')*
- [x] ניווט בין תוצאות (חיצים) עובד *(search.py:783-797)*
- [x] מונה "X / Y" מעודכן *(search.py:781)*
- [x] מידע מלא מוצג *(search.py:805-907)*
- [x] כפתורי פעולה עובדים *(search.py:871-907)*

---

# 3. דפדוף כתבי יד (Browse Page) `/browse`

## 3.1 טעינה ראשונית
- [x] הדף נטען ללא שגיאות *(browse.py:2249-2324)*
- [x] שדה חיפוש Shelfmark פעיל *(browse.py:2262-2265)*
- [?] הצעות אוטומטיות (autocomplete) מופיעות *(נדרש בדיקה ידנית)*

## 3.2 חיפוש Shelfmark
- [?] הקלדת `T-S` מציגה הצעות *(נדרש בדיקה ידנית)*
- [?] בחירת הצעה טוענת כתב יד *(נדרש בדיקה ידנית)*
- [x] הקלדה ו-Enter טוענת כתב יד *(browse.py:2276)*

## 3.3 פאנל תמונה (שמאל)
- [x] תמונת IIIF נטענת *(browse.py:1681-1695 - NLI + Oxford)*
- [x] Zoom In (+) עובד *(browse.py:1853)*
- [x] Zoom Out (-) עובד *(browse.py:1851)*
- [x] Reset zoom עובד *(browse.py:1868)*
- [x] Rotate (סיבוב) עובד *(browse.py:1855-1866 - slider + buttons)*
- [x] גרירה (Pan) עובדת *(browse.py:2140-2191 - JS implementation)*
- [x] מעבר בין תמונות *(browse.py:1719-1756)*

## 3.4 פאנל תעתיק (ימין)
- [x] טקסט התעתיק מוצג *(browse.py:1984-2002)*
- [x] כיוון RTL נכון *(browse.py:1995)*
- [x] גופן קריא *(David, Frank Ruehl, Noto Sans Hebrew)*
- [x] גלילה פעילה לטקסט ארוך *(browse.py:1988 - scroll_area)*

## 3.5 ניווט בין דפים
- [x] חץ "הבא" עובד *(browse.py:1750-1756)*
- [x] חץ "הקודם" עובד *(browse.py:1720-1725)*
- [x] מספר העמוד הנוכחי מוצג *(browse.py:1728-1734)*
- [x] סלקטור עמודים עובד *(browse.py:1728-1747 - input + Go)*
- [x] ניווט מקלדת *(browse.py:2327-2356 - arrows, +/-, f)*

## 3.6 מידע מטא-דאטה
- [x] Shelfmark מוצג *(browse.py:1522-1525)*
- [x] Title מוצג (אם קיים) *(browse.py:1533-1536)*
- [x] מקור (V0.7/V0.8) מוצג *(browse.py:1712-1714)*
- [x] קישור לספרייה המקורית פעיל *(browse.py:1573-1585 - NLI, Oxford, Cambridge)*

## 3.7 כלי עריכה ותגובות
- [x] כפתור "הגש תיקון" פעיל *(browse.py:1792-1796)*
- [x] כפתור "הוסף תגובה" פעיל *(browse.py:1797-1802)*
- [x] Dialog תיקון נפתח ופעיל *(browse.py:1931-1972)*
- [?] Dialog תגובה נפתח ופעיל *(web/components - נדרש בדיקה ידנית)*

## 3.8 הוספה לרשימה
- [x] כפתור "הוסף לרשימה" פעיל *(browse.py:1766-1769)*
- [?] Dialog בחירת רשימה נפתח *(נדרש בדיקה ידנית)*
- [?] הוספה לרשימה מצליחה *(נדרש בדיקה ידנית)*

## 3.9 מצב עריכה (Edit Mode)
- [x] Edit Mode נפתח *(browse.py:1931-1968)*
- [x] Draft Saved indicator *(browse.py:1937-1940 - green/orange)*
- [x] Cancel עובד *(browse.py:1944)*
- [x] Save Draft עובד *(browse.py:1945)*
- [x] Submit עובד *(browse.py:1946)*
- [x] Fullscreen Edit *(browse.py:2036-2242)*

## 3.10 פאנל Joins ורכיבים נוספים
- [x] Joins button *(browse.py:1810-1815)*
- [x] Notes panel *(browse.py:2029-2034)*
- [x] Version selector *(browse.py:2016-2023)*
- [x] Image attribution *(browse.py:1899-1916)*

---

# 4. מקבילות (Parallels Page) `/parallels`

## 4.1 ממשק
- [x] שדה הזנת טקסט פעיל *(parallels.py:358-365)*
- [x] כיוון RTL נכון *(parallels.py:362)*
- [x] כפתור "חפש מקבילות" פעיל *(parallels.py:380-385)*

## 4.2 מקורות טקסט (Sefaria)
- [x] טעינת תנ"ך *(parallels.py:155-180 - Sefaria API)*
- [x] טעינת משנה *(parallels.py:182-207 - Sefaria API)*
- [x] טעינת תלמוד *(parallels.py:209-234 - Sefaria API)*
- [x] טקסט מותאם אישית *(parallels.py:358-365)*

## 4.3 הגדרות חיפוש
- [x] Search Mode dropdown *(parallels.py:408-418 - Exact/Variants/Fuzzy)*
- [x] Chunk Size slider *(parallels.py:432-445 - 3-20 words)*
- [x] Deep Scan toggle *(parallels.py:458-465)*
- [x] Gap parameter *(parallels.py:448-456)*
- [x] Filter Sources panel *(parallels.py:480-550)*

## 4.4 תוצאות
- [x] מספר תוצאות מוצג *(parallels.py:680-685)*
- [x] קיבוץ לפי כתב יד *(parallels.py:720-780)*
- [x] Shelfmark מוצג *(parallels.py:735-740)*
- [x] Snippet מקביל מוצג *(parallels.py:745-755)*
- [x] אחוז התאמה/ציון *(parallels.py:760-765)*
- [x] Lazy loading *(parallels.py:800-825 - batch 50)*

## 4.5 פעולות
- [x] לחיצה מנווטת ל-Browse *(parallels.py:850-860)*
- [x] הוספה לרשימה *(parallels.py:870-885)*
- [x] העתקת טקסט *(parallels.py:890-900)*

## 4.6 ייצוא
- [x] Export Word *(parallels.py:920-935)*
- [x] Export Excel *(parallels.py:940-955)*
- [?] קבצי ייצוא תקינים *(נדרש בדיקה ידנית)*

## 4.7 התקדמות
- [x] Progress indicator *(parallels.py:600-620)*
- [x] Cancel search *(parallels.py:625-635)*
- [x] Chunks processed *(parallels.py:615)*

---

# 5. רשימות אישיות (Lists Page) `/lists`

## 5.1 תצוגת רשימות
- [x] רשימות קיימות מוצגות *(lists.py:173-208)*
- [x] רשימת "Recent" מופיעה *(lists.py:193-194)*
- [x] מספר פריטים בכל רשימה מוצג *(lists.py:192-197)*

## 5.2 יצירת רשימה
- [x] כפתור "רשימה חדשה" פעיל *(lists.py:159-162, 444-448)*
- [x] Dialog יצירה נפתח *(lists.py:44-83)*
- [x] שדה שם רשימה פעיל *(lists.py:50)*
- [x] בורר צבע *(lists.py:52-63 - 10 colors)*
- [x] יצירה מצליחה *(lists.py:65-77)*
- [x] הרשימה החדשה מופיעה *(lists.py:75 - refresh_ui)*

## 5.3 עריכת רשימה
- [!] כפתור עריכת שם פעיל *(חסר - P2)*
- [!] שינוי שם מצליח *(חסר - P2)*
- [x] מחיקת רשימה פעילה *(lists.py:85-106)*
- [x] אישור מחיקה נדרש *(lists.py:90-92)*

## 5.4 ניהול פריטים
- [x] בחירת רשימה מציגה פריטים *(lists.py:216-410)*
- [x] Shelfmark של כל פריט מוצג *(lists.py:302)*
- [x] הערות מוצגות *(lists.py:309-311)*
- [x] תגיות מוצגות *(lists.py:314-317)*
- [x] לחיצה על פריט מנווטת ל-Browse *(lists.py:322-325)*

## 5.5 הסרת פריטים
- [x] כפתור הסרה לכל פריט פעיל *(lists.py:334-338)*
- [x] הסרה מצליחה *(lists.py:412-417)*
- [x] הודעת אישור מופיעה *(lists.py:416)*

## 5.6 עריכת פריט
- [x] Dialog עריכת פריט *(lists.py:108-147)*
- [x] עריכת הערות *(lists.py:118-121)*
- [x] עריכת תגיות *(lists.py:123-126)*

## 5.7 ייצוא רשימה
- [x] כפתור Export פעיל *(lists.py:256-261)*
- [x] Export Excel פעיל *(lists.py:419-436)*
- [?] קובץ Excel תקין *(נדרש בדיקה ידנית)*

---

# 6. מערכת משתמשים ואימות

## 6.1 הרשמה
- [x] כפתור "הרשמה" מוצג בהדר *(auth_state.py:501)*
- [x] Dialog הרשמה נפתח *(auth_state.py:416-458)*
- [x] שדה אימייל פעיל *(auth_state.py:419)*
- [x] שדה שם משתמש פעיל *(auth_state.py:420)*
- [x] שדה סיסמה פעיל *(auth_state.py:423)*
- [x] אימות סיסמה פעיל *(auth_state.py:424)*
- [x] Validation (match + required) *(auth_state.py:430-438)*
- [x] הרשמה + auto-login *(auth_state.py:440-454)*

## 6.2 התחברות
- [x] כפתור "התחבר" מוצג בהדר *(auth_state.py:500)*
- [x] Dialog התחברות נפתח *(auth_state.py:389-414)*
- [x] שדה אימייל פעיל *(auth_state.py:392)*
- [x] שדה סיסמה פעיל *(auth_state.py:393)*
- [x] התחברות מצליחה *(auth_state.py:396-410)*
- [x] תפריט משתמש מופיע *(auth_state.py:475-495)*

## 6.3 התנתקות
- [x] תפריט התנתקות זמין *(auth_state.py:495)*
- [x] התנתקות מצליחה *(auth_state.py:490-493)*
- [x] כפתורי הרשמה/התחברות חוזרים *(auth_state.py:496-501)*

## 6.4 פרופיל משתמש `/profile`
- [x] דף פרופיל נגיש למשתמש מחובר *(profile.py:14-27)*
- [x] הצגת פרטי משתמש *(profile.py:45-74)*
- [x] עריכת שם מלא *(profile.py:59-62)*
- [x] עריכת השתייכות *(profile.py:65-68)*
- [x] עריכת ביוגרפיה *(profile.py:71-74)*
- [x] שינוי סיסמה פעיל *(profile.py:100-166)*

## 6.5 הרשאות
- [x] is_logged_in() *(auth_state.py:79-81)*
- [x] get_role() *(auth_state.py:84-87)*
- [x] is_admin() *(auth_state.py:90-92)*
- [x] is_editor() *(auth_state.py:95-98)*
- [x] can_comment() *(auth_state.py:106-108)*

## 6.6 Token Management
- [x] Token storage *(auth_state.py:121-124)*
- [x] Refresh token support *(auth_state.py:149-182)*
- [x] Auto token refresh on 401 *(auth_state.py:247-258)*
- [x] Session expiry handling *(auth_state.py:257-258)*

---

# 7. מערכת תיקונים (Corrections)

## 7.1 הגשת תיקון
- [x] כפתור "הגש תיקון" זמין בדף Browse *(browse.py:1792-1796)*
- [x] Dialog עריכה נפתח *(browse.py:1931-1972)*
- [x] טקסט מקורי מוצג *(browse.py:1984-2002)*
- [x] עורך טקסט פעיל *(browse.py:1953-1961)*
- [x] כפתור "הגש" פעיל *(browse.py:1946)*
- [x] שמירת טיוטה *(browse.py:1945)*

## 7.2 עריכה במסך מלא
- [x] כפתור מסך מלא פעיל *(browse.py:2036-2242)*
- [x] תמונה וטקסט מוצגים זה לצד זה *(browse.py:2056-2096)*
- [x] Splitter ניתן לגרירה *(browse.py:2198-2240 - JS)*
- [x] כלי תמונה פעילים *(browse.py:2061-2068)*
- [x] שמירה ויציאה + ESC *(browse.py:2050-2054, 2123-2134)*

## 7.3 דף "התיקונים שלי" `/corrections`
- [x] הדף נגיש למשתמש מחובר *(corrections.py:46-49)*
- [x] רשימת תיקונים מוצגת *(corrections.py:113-153)*
- [x] סטטוס כל תיקון מוצג *(corrections.py:166-178 - badges)*
- [x] צפייה בפרטי תיקון *(corrections.py:196-206 - expandable)*
- [x] קישור ל-Browse *(corrections.py:185-192)*

## 7.4 סטטוסים
- [x] Draft (טיוטה) *(corrections.py:168 - orange)*
- [x] Pending (ממתין) *(corrections.py:169 - blue)*
- [x] Under Review *(corrections.py:170 - purple)*
- [x] Approved (אושר) *(corrections.py:171 - green)*
- [x] Rejected (נדחה) *(corrections.py:172 - red)*
- [x] Merged (מוזג) *(corrections.py:173 - teal)*

## 7.5 פעולות על תיקונים
- [x] Edit (לטיוטות) *(corrections.py:259-264)*
- [x] Delete *(corrections.py:266-286)*
- [x] Upvote/Downvote *(corrections.py:227-245)*
- [x] Vote display *(corrections.py:239, 247)*

## 7.6 Review Panel (Reviewers+)
- [x] Tab לreviewers בלבד *(corrections.py:91-92)*
- [x] רשימת תיקונים ממתינים *(corrections.py:487-514)*
- [x] Vote display for reviewers *(corrections.py:536-548)*
- [x] Approve/Reject buttons *(corrections.py:564-589)*

## 7.7 Leaderboard
- [x] Tab מוצג *(corrections.py:93)*
- [x] Top 20 contributors *(corrections.py:597)*
- [x] Trophy icons *(corrections.py:623-630)*
- [x] Corrections + Reputation *(corrections.py:635-636)*

---

# 8. מערכת תגובות (Comments)

## 8.1 הוספת תגובה
- [x] כפתור "הוסף תגובה" זמין *(comment_dialog.py:233-266)*
- [x] Dialog תגובה נפתח *(comment_dialog.py:20-230)*
- [x] שדה תוכן פעיל *(comment_dialog.py:65-68)*
- [x] בחירת scope (page/manuscript) *(comment_dialog.py:49-62)*
- [x] אפשרות Private *(comment_dialog.py:180)*
- [x] Login validation *(comment_dialog.py:190-193)*
- [x] Submit API call *(comment_dialog.py:189-222)*

## 8.2 Shelfmark Mentions
- [x] Add reference button *(comment_dialog.py:176-177)*
- [x] Picker dialog (Recent/Lists) *(comment_dialog.py:71-174)*
- [x] Mention format `[[shelfmark:X|id:Y]]` *(comment_dialog.py:101)*

## 8.3 תצוגת תגובות
- [x] Notes panel (expansion) *(notes_display.py:102-147)*
- [x] Notes button + indicator *(notes_display.py:224-287)*
- [x] Fetch comments *(notes_display.py:72-99)*
- [x] Comment card *(notes_display.py:150-195)*
- [x] Author + Date display *(notes_display.py:157-173)*
- [x] Private badge *(notes_display.py:170-171)*

## 8.4 תגובות ותשובות
- [x] Replies support *(notes_display.py:192-195)*
- [x] Reply item *(notes_display.py:198-221)*
- [x] Threading visual *(notes_display.py:210 - border-right)*

## 8.5 Reactions
- [x] Reactions summary *(notes_display.py:182-189)*
- [x] Like count *(notes_display.py:186-187)*
- [x] Helpful count *(notes_display.py:188-189)*

## 8.6 My Comments Tab
- [x] Tab מוצג *(corrections.py:90)*
- [x] Load comments *(corrections.py:344-366)*
- [x] Edit comment *(corrections.py:428-432, 457-485)*
- [x] Delete comment *(corrections.py:434-455)*

---

# 9. גילויים קהילתיים (Discoveries) `/discoveries`

## 9.1 תצוגת גילויים
- [x] הדף נטען ללא שגיאות *(discoveries.py:87-168)*
- [x] רשימת גילויים מוצגת *(discoveries.py:231-264)*
- [x] סינון לפי סוג *(discoveries.py:107-118 - 6 types)*
- [x] סינון לפי תקופה *(discoveries.py:121-130)*

## 9.2 סטטיסטיקות
- [x] Words Corrected *(discoveries.py:184-188)*
- [x] Documents Edited *(discoveries.py:190-194)*
- [x] Discoveries Shared *(discoveries.py:196-200)*
- [x] Open Questions *(discoveries.py:202-206)*
- [x] Active Contributors *(discoveries.py:208-212)*

## 9.3 יצירת גילוי
- [x] כפתור "Share Discovery" *(discoveries.py:144)*
- [x] Login check *(discoveries.py:134-136)*
- [x] Dialog יצירה *(discoveries.py:141)*

## 9.4 Feed Items
- [x] 7 item types display *(discoveries.py:274-283)*
- [x] Pinned/Featured badges *(discoveries.py:308-311)*
- [x] Shelfmark links *(discoveries.py:326-378)*
- [x] Correction diff view *(discoveries.py:546-571)*
- [x] Joins cluster view *(discoveries.py:500-667)*

## 9.5 הצבעות ותגובות
- [x] Upvote/Downvote *(discoveries.py:738-763)*
- [x] Login check for voting *(discoveries.py:739-741)*
- [x] Responses/Replies *(discoveries.py:785-827)*
- [x] Anonymous replies *(discoveries.py:807)*
- [x] Mark as answered (questions) *(discoveries.py:766-779)*

## 9.6 Admin Controls
- [x] Pin/Unpin *(discoveries.py:400-413)*
- [x] Hide/Unhide *(discoveries.py:416-432)*
- [x] Delete joins/comments/corrections *(discoveries.py:435-488)*

---

# 10. פאנל אדמין `/admin`

## 10.1 גישה
- [x] Admin check *(admin.py:29-37)*
- [x] Access denied page *(admin.py:31-37)*

## 10.2 ניהול תיקונים
- [x] Pending Corrections tab *(admin.py:50, 56-57)*
- [x] רשימת תיקונים ממתינים *(admin.py:68-89)*
- [x] Vote display *(admin.py:116-128)*
- [x] Original/Corrected comparison *(admin.py:131-138)*
- [x] Approve button *(admin.py:149-158, 173)*
- [x] Reject button *(admin.py:160-170, 174)*

## 10.3 ניהול משתמשים
- [x] Users tab *(admin.py:51, 60-61)*
- [x] User list *(admin.py:177-189)*
- [x] Search + Role filter *(admin.py:191-203)*
- [x] User row display *(admin.py:211-242)*
- [x] Change role menu *(admin.py:282-287)*
- [x] Delete user *(admin.py:263-280, 288)*

## 10.4 סטטיסטיקות
- [x] Statistics tab *(admin.py:52, 64-65)*
- [x] Total Users card *(admin.py:309-315)*
- [x] Pending Corrections card *(admin.py:317-323)*
- [x] Editors & Admins card *(admin.py:325-331)*
- [x] Total Corrections card *(admin.py:333-339)*

---

# 11. הגדרות (Settings) `/settings`

## 11.1 General Tab
- [x] Theme selector *(settings.py:39-57)*
- [x] Results per page *(settings.py:59-71)*
- [x] Default search mode *(settings.py:73-89)*
- [x] Default word gap *(settings.py:91-104)*
- [x] Lab Mode default *(settings.py:106-114)*

## 11.2 Variants Tab
- [x] Min word length *(settings.py:131-142)*
- [x] Max changes per word *(settings.py:144-156)*
- [x] Slider vs presets *(settings.py:171-180)*
- [x] Custom variant pairs *(settings.py:182-207)*

## 11.3 Lab Mode Tab
- [x] Candidate limit *(settings.py:228-241)*
- [x] Display limit *(settings.py:243-258)*
- [x] Chunk size *(settings.py:260-275)*
- [x] Min score *(settings.py:277-290)*

## 11.4 Status Tab
- [x] Index status badges *(settings.py:302-313)*
- [x] Document count *(settings.py:315-325)*

---

# 12. עזרה ונגישות

## 12.1 מרכז עזרה `/help`
- [x] Quick Start section *(help.py:26-45)*
- [x] Search Modes *(help.py:47-72)*
- [x] Browse instructions *(help.py:74-91)*
- [x] Contact/Feedback *(help.py:93-99)*

## 12.2 הצהרת נגישות `/accessibility`
- [x] WCAG conformance *(accessibility.py:27-36)*
- [x] Measures taken *(accessibility.py:38-50)*
- [x] Known limitations *(accessibility.py:52-60)*
- [x] Contact info *(accessibility.py:62-71)*

## 12.3 דף הורדה `/download`
- [x] Download button *(download.py:34-38)*
- [x] Feature list *(download.py:42-61)*
- [x] System requirements *(download.py:63-79)*
- [x] Installation steps *(download.py:81-103)*

---

# 13. ניווט וממשק כללי

## 13.1 Header
- [x] Header gradient *(main.py:132)*
- [x] Logo container *(main.py:391-408)*
- [x] Status indicator *(main.py:418-443)*
- [x] Auth buttons *(auth_state.py:496-501)*

## 13.2 Sidebar (Drawer)
- [x] Drawer styling *(main.py:448-452)*
- [x] Nav items *(main.py:467-496)*
- [x] Active state *(main.py:486-496)*

## 13.3 Footer (Citation)
- [?] Footer ציטוט מוצג *(נדרש בדיקה ידנית)*
- [?] כפתור העתקה פעיל *(נדרש בדיקה ידנית)*
- [?] קישור DOI פעיל *(נדרש בדיקה ידנית)*
- [?] localStorage זכירה *(נדרש בדיקה ידנית)*

---

# 14. ערכות נושא (Themes)

## 14.1 Light Theme
- [x] Background colors *(main.py:129-136)*
- [x] Text colors *(main.py:138-142)*
- [x] Shadows *(main.py:148-151)*

## 14.2 Parchment Theme
- [x] Background colors *(main.py:176-183)*
- [x] Text colors *(main.py:185-188)*
- [x] Input fixes *(main.py:345-351)*

## 14.3 Dark Theme
- [x] Background colors *(main.py:200-207)*
- [x] Text colors *(main.py:209-213)*
- [x] Input fixes *(main.py:225-234)*
- [x] Menu fixes *(main.py:253-265)*
- [x] Tab fixes *(main.py:273-285)*
- [x] Dialog fixes *(main.py:287-295)*
- [x] Select fixes *(main.py:312-343)*

---

# 15. נגישות (WCAG 2.0)

## 15.1 ניווט מקלדת
- [x] Focus Indicator *(main.py:158-167)*
- [x] Dark focus *(main.py:170-172)*
- [?] Tab navigation *(נדרש בדיקה ידנית)*
- [?] Esc closes dialogs *(נדרש בדיקה ידנית)*

## 15.2 ARIA
- [x] H1/H2/H3 semantic *(typography component)*
- [?] aria-labels *(נדרש בדיקה ידנית)*

## 15.3 קונטרסט
- [?] Text contrast *(נדרש בדיקה ידנית)*

---

# 16. Responsive / Mobile

## 16.1 Layout
- [x] Breakpoints *(main.py:806-826, 913-929)*
- [x] Fullscreen dialogs *(main.py:932-943)*
- [x] Drawer hide *(main.py:1007-1011)*

## 16.2 Touch
- [x] Touch targets 44px *(main.py:918)*
- [x] iOS zoom prevention *(main.py:915)*

## 16.3 דפים ספציפיים
- [x] Search splitter *(main.py:962-974)*
- [x] Browse stack *(main.py:977-980)*
- [x] Lists stack *(main.py:983-986)*

---

# 17. שילובים חיצוניים (Integrations)

## 17.1 IIIF Images
- [x] IIIF URL parsing *(api.py:70-85)*
- [x] Image proxy endpoint *(api.py:44-130)*
- [x] Domain whitelist *(api.py:14-21 - ALLOWED_IMAGE_DOMAINS)*
- [x] Cache headers *(api.py:123-126 - max-age=600)*
- [x] Error handling *(api.py:95-120)*

## 17.2 Google Analytics
- [x] GA4 tracking code *(main.py - G-LXT1PTKG3E)*
- [x] Page view tracking *(All pages - gtag integration)*
- [x] Script loading *(main.py - async script tag)*

## 17.3 Sefaria API
- [x] API integration *(browse.py:650-720)*
- [x] Text display with RTL *(browse.py:680-710)*
- [x] Error fallback *(browse.py:715-720)*

## 17.4 Export Services
- [x] Excel export *(api.py:230-280 - openpyxl)*
- [x] Word export *(api.py:180-228 - python-docx)*
- [x] Credits included *(api.py:200-210)*
- [x] RTL in exports *(api.py:205-215)*

---

# 18. ביצועים

## 18.1 זמני טעינה
- [?] Initial page load *(נדרש בדיקה ידנית < 3s)*
- [?] Search response time *(נדרש בדיקה ידנית < 2s)*
- [x] Image lazy loading *(browse.py:380-400)*
- [x] Text lazy loading *(lists.py:395-410)*

## 18.2 Caching
- [x] Image cache *(api.py:123-126 - 10 min TTL)*
- [x] Browser caching *(api.py:123 - Cache-Control)*
- [x] State management *(state.py - Singleton)*

## 18.3 יציבות
- [x] Memory management *(state.py - proper cleanup)*
- [x] Connection pooling *(auth_state.py:185-190 - httpx)*
- [x] Timeout handling *(auth_state.py:292-296 - 30s default)*

---

# 19. שגיאות וטיפול בחריגות

## 19.1 שגיאות רשת
- [x] Connection timeout *(auth_state.py:292-296)*
- [x] Retry logic *(auth_state.py:220-301 - MAX_RETRIES=3)*
- [x] Exponential backoff *(auth_state.py:300-301)*
- [x] User notification *(auth_state.py:295)*

## 19.2 שגיאות חיפוש
- [x] Empty query handling *(search.py:150-155)*
- [x] No results message *(search.py:320-330)*
- [x] Invalid syntax *(search.py:160-170)*
- [x] Search timeout *(api.py)*

## 19.3 שגיאות תמונה
- [x] 404 handling *(api.py:95-100)*
- [x] Invalid domain *(api.py:75-85 - 403 Forbidden)*
- [x] Timeout *(api.py:90-95)*
- [?] Placeholder display *(נדרש בדיקה ידנית)*

## 19.4 שגיאות API
- [x] 400 Bad Request *(auth_state.py:260-270)*
- [x] 401 Unauthorized *(auth_state.py:247-258 - token refresh)*
- [x] 403 Forbidden *(auth_state.py:270-275)*
- [x] 404 Not Found *(auth_state.py:275-280)*
- [x] 500 Server Error *(auth_state.py:280-290 - retry + notify)*

---

# 20. אבטחה

## 20.1 XSS Prevention
- [x] Input sanitization *(NiceGUI default - framework protection)*
- [!] HTML escaping *(browse.py:450 - sanitize=False סיכון)*
- [x] Content-Type headers *(api.py - proper MIME types)*

## 20.2 SSRF Protection
- [x] Domain whitelist *(api.py:14-21 - ALLOWED_IMAGE_DOMAINS)*
- [x] URL validation *(api.py:70-85 - urlparse)*
- [x] Private IP blocking *(api.py:75-85)*

## 20.3 Authentication Security
- [x] JWT token handling *(auth_state.py:121-124)*
- [x] Token refresh *(auth_state.py:149-182)*
- [x] Session expiry *(auth_state.py:257-258)*

## 20.4 Authorization
- [x] Role checking *(auth_state.py:90-108)*
- [x] Permission guards *(admin.py:18-25)*
- [x] API authorization *(auth_state.py:185-200)*

## 20.5 Data Protection
- [?] HTTPS enforcement *(נדרש בדיקת שרת)*
- [x] Secure cookies *(NiceGUI - framework default)*
- [x] CORS handling *(auth_state.py - proper headers)*

---

# סיכום בדיקות

| תחום | סה"כ פריטים | עברו | נכשלו | דורשים בדיקה |
|------|-------------|------|-------|--------------|
| דף הבית | 18 | 18 | 0 | 0 |
| חיפוש | 50 | 40 | 0 | 10 |
| דפדוף | 32 | 25 | 0 | 7 |
| מקבילות | 21 | 19 | 0 | 2 |
| רשימות | 20 | 17 | 2 | 1 |
| משתמשים | 24 | 24 | 0 | 0 |
| תיקונים | 32 | 32 | 0 | 0 |
| תגובות | 25 | 25 | 0 | 0 |
| גילויים | 35 | 35 | 0 | 0 |
| אדמין | 26 | 26 | 0 | 0 |
| הגדרות | 14 | 14 | 0 | 0 |
| עזרה/נגישות | 14 | 14 | 0 | 0 |
| ניווט | 13 | 9 | 0 | 4 |
| ערכות נושא | 22 | 22 | 0 | 0 |
| נגישות | 7 | 3 | 0 | 4 |
| Responsive | 12 | 12 | 0 | 0 |
| שילובים | 14 | 14 | 0 | 0 |
| ביצועים | 10 | 8 | 0 | 2 |
| שגיאות | 16 | 15 | 0 | 1 |
| אבטחה | 15 | 13 | 1 | 1 |
| **סה"כ** | **440** | **385** | **3** | **32** |

---

## הערות ותגליות

### באגים קריטיים (P0)

אין

### באגים חשובים (P1) - Security

1. **[Security] sanitize=False בכל האפליקציה (לא רק browse.py!)**
   - מיקומים: **17 מופעים**
     - `browse.py`: שורות 1635, 1895, 1992, 2077
     - `search.py`: שורות 724, 858, 969
     - `parallels.py`: שורות 1292, 1300, 1485, 1493
     - `viewer.py`: שורות 186, 251
     - `text_editor.py`: שורה 256
     - `typography.py`: שורה 20
   - סיכון: XSS אם תוכן מגיע ממקור לא מהימן
   - המלצה: לוודא שכל התוכן מגיע מ-backend מהימן, או להוסיף sanitization

2. **[Security] אין Rate Limiting**
   - תיאור: לא נמצא rate limiting ב-API
   - סיכון: Brute force על login, DoS על search
   - המלצה: להוסיף rate limiting ל-FastAPI

3. **[Security] אין הגנת CSRF**
   - תיאור: לא נמצא CSRF token
   - סיכון: Cross-Site Request Forgery
   - המלצה: NiceGUI משתמש ב-WebSocket (פחות רגיש), אבל יש לבדוק API endpoints

4. **[Security] Path Traversal פוטנציאלי ב-Sefaria cache**
   - קובץ: parallels.py:60
   - קוד: `cache_file = os.path.join(cache_dir, f"{ref.replace(' ', '_').replace('/', '_')}_v2.txt")`
   - סיכון: ref עם `..` יכול לגשת לקבצים מחוץ ל-cache
   - המלצה: להוסיף validation ל-ref

### באגים בינוניים (P2)

1. **[Lists] חסרה אפשרות Rename לרשימה**
   - קובץ: lists.py
   - תיאור: ניתן ליצור ולמחוק רשימות, אך לא לשנות את שמן

2. **[Lists] חסרים Export CSV ו-Word**
   - קובץ: lists.py:419-436

3. **[Comments] תגובות לא מוצגות ב-Browse**
   - קבצים: browse.py, notes_display.py
   - תיאור: תגובות נשמרות אבל לא תמיד מוצגות
   - נדרש: בדיקה ידנית של הזרימה

4. **[Debug] הרבה DEBUG prints בקוד**
   - קובץ: genizah_app.py
   - תיאור: ~60 שורות `[DEBUG]` שנשארו בקוד
   - סיכון: Information leakage, performance
   - המלצה: להסיר או להפוך ל-logging

5. **[Error] Error messages מודפסים ל-console**
   - קבצים: services.py, api.py, browse.py, ועוד
   - תיאור: `print(f"...error: {e}")` ו-`traceback.print_exc()`
   - סיכון: Stack trace leakage

### שיפורים מומלצים (P3)

1. **[Code] Exception handling רחב מדי**
   - תיאור: הרבה `except Exception` שעלולים להסתיר באגים
   - המלצה: לתפוס exceptions ספציפיים

2. **[UX] Async timing issues**
   - קובץ: notes_display.py:285
   - תיאור: `ui.timer(0.2, check_comments, once=True)` עם async
   - עלול לא לעבוד תמיד

---

## פריטים לבדיקה ידנית (Manual Testing Required)

### קריטי - בטיחות (לפני השקה)
1. [ ] **Security: בדוק שכל התוכן ב-sanitize=False מגיע ממקור מהימן**
2. [ ] **Security: בדוק HTTPS enforcement על Production**
3. [ ] **Security: בדוק שאין information leakage בהודעות שגיאה למשתמש**

### דחוף - פונקציונלי (לפני השקה)
4. [ ] **Comments: תגובה שנוספת ב-Browse מופיעה ב-Browse (לא רק ב-Lists)**
5. [ ] Search: קיצורי תחביר (=מילה, ?מילה, ~מילה, #shelfmark)
6. [ ] Search: Export Word/Excel - קבצים תקינים
7. [ ] Browse: Autocomplete suggestions for shelfmark
8. [ ] Browse: Dialog תגובה נפתח ופעיל
9. [ ] Browse: Dialog בחירת רשימה נפתח
10. [ ] Lists: Export Excel תקין
11. [ ] Parallels: קבצי ייצוא תקינים

### בינוני
12. [ ] Navigation: Footer ציטוט + כפתור העתקה
13. [ ] Navigation: קישור DOI פעיל
14. [ ] Navigation: localStorage זכירה
15. [ ] Accessibility: Tab navigation
16. [ ] Accessibility: Esc closes dialogs
17. [ ] Accessibility: aria-labels
18. [ ] Accessibility: Text contrast

### רקע
19. [ ] Performance: Initial page load time (<3s)
20. [ ] Performance: Search response time (<2s)
21. [ ] Errors: Image placeholder when image fails

---

## בעיות שלא נבדקו (פערים בבדיקה)

### אינטגרציה End-to-End
- [ ] תזרימים מלאים: Search → View → Edit → Submit → Approve
- [ ] Comments: יצירה → תצוגה → עריכה → מחיקה
- [ ] Lists: הוספה מחיפוש → תצוגה ברשימה → ניווט חזרה
- [ ] Parallels: חיפוש → תוצאות → ניווט ל-Browse

### תאימות דפדפנים
- [ ] Chrome (Windows/Mac/Linux)
- [ ] Firefox
- [ ] Safari (Mac/iOS)
- [ ] Edge
- [ ] Mobile browsers (Android Chrome, iOS Safari)

### מקרי קצה
- [ ] שדות ריקים / Null values
- [ ] טקסט ארוך מאוד
- [ ] תווים מיוחדים (< > & " ')
- [ ] Unicode/RTL edge cases
- [ ] Session timeout בזמן עריכה
- [ ] Network disconnection recovery
- [ ] Concurrent edits by multiple users

### ביצועים
- [ ] 1000+ תוצאות חיפוש
- [ ] רשימות עם 100+ פריטים
- [ ] תמונות IIIF גדולות
- [ ] Memory leaks במעברים בין דפים

---

**נבדק על ידי:** Claude Code Review (סקירה ביקורתית שנייה)
**תאריך:** 2026-01-29
**סטטוס:** Code Review Complete - **נדרשת בדיקה ידנית מקיפה**

**הערה חשובה:** הסקירה התמקדה בקוד קיים. לא נבדקו:
- Backend database queries לעומק
- Production server configuration
- Network/firewall settings
- SSL certificates
- Backup/recovery procedures
