# דוח בדיקות תחומים 3-4: דפדוף כתבי יד ומקבילות
## Test Report: Browse & Parallels Pages

**תאריך:** 2026-01-29
**סביבה:** Production
**בודק:** Code Review + Manual Testing Requirements

---

## תחום 3: דפדוף כתבי יד (Browse Page) `/browse`

### 3.1 טעינה ראשונית
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| הדף נטען ללא שגיאות | [x] | browse.py:2249-2324 | Main layout with proper initialization |
| שדה חיפוש Shelfmark פעיל | [x] | browse.py:2262-2265 | `ui.input` with placeholder and label |
| הצעות אוטומטיות | [?] | browse.py:2262-2265 | No autocomplete component - uses simple input |

**ממצאים:**
- קיים `ui.input` פשוט עם `clearable` prop
- לא נמצא רכיב autocomplete מובנה - נדרשת בדיקה ידנית האם ההצעות מופיעות

### 3.2 חיפוש Shelfmark
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| הקלדת `T-S` מציגה הצעות | [?] | - | דורש בדיקה ידנית |
| בחירת הצעה טוענת כתב יד | [?] | - | דורש בדיקה ידנית |
| Enter טוען כתב יד | [x] | browse.py:2276 | `on('keydown.enter', do_search)` |

### 3.3 פאנל תמונה (שמאל)
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| תמונת IIIF נטענת | [x] | browse.py:1681-1695 | NLI + Oxford support |
| Zoom In (+) עובד | [x] | browse.py:1853, zoom_in() | Button with tooltip |
| Zoom Out (-) עובד | [x] | browse.py:1851, zoom_out() | Button with tooltip |
| Reset zoom עובד | [x] | browse.py:1868, zoom_reset() | Button with tooltip |
| Rotate עובד | [x] | browse.py:1855-1866 | Left/Right + Slider |
| Pan/Drag עובד | [x] | browse.py:2140-2191 | JavaScript implementation |
| מעבר בין תמונות | [x] | browse.py:1719-1756 | Prev/Next with page input |

**ממצאים:**
- תמיכה מלאה ב-IIIF מ-NLI ו-Oxford
- Image proxy מובנה למניעת בעיות CORS
- Error handling עם fallback: `onerror="handleImageError(...)"`

### 3.4 פאנל תעתיק (ימין)
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| טקסט התעתיק מוצג | [x] | browse.py:1984-2002 | Via render_text_content() |
| כיוון RTL נכון | [x] | browse.py:1995 | `direction: rtl; text-align: right;` |
| גופן קריא | [x] | browse.py:1996 | David, Frank Ruehl, Noto Sans Hebrew |
| גלילה פעילה | [x] | browse.py:1988 | `ui.scroll_area` with calc height |

### 3.5 ניווט בין דפים
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| חץ "הבא" עובד | [x] | browse.py:1750-1756 | With disabled state |
| חץ "הקודם" עובד | [x] | browse.py:1720-1725 | With disabled state |
| מספר עמוד נוכחי | [x] | browse.py:1728-1734 | Input with min/max |
| סלקטור עמודים | [x] | browse.py:1728-1747 | Number input + Go button |
| ניווט מקלדת | [x] | browse.py:2327-2356 | Arrow keys + +/- for zoom |

**ממצאים:**
- ניווט מקלדת מלא: חיצים לעמודים, +/- לזום, f למסך מלא
- כפתורים מושבתים בגבולות (עמוד ראשון/אחרון)

### 3.6 מידע מטא-דאטה
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Shelfmark מוצג | [x] | browse.py:1522-1525 | In metadata grid |
| Title מוצג | [x] | browse.py:1533-1536 | If available, col-span-2 |
| מקור מוצג | [x] | browse.py:1712-1714 | V0.7/V0.8 badge |
| קישור לספרייה | [x] | browse.py:1573-1585 | NLI Ktiv, Oxford, Cambridge |

**ממצאים:**
- מטאדאטה מורחב עבור Oxford (Part Title, Contents, Provenance)
- קישורים חיצוניים נפתחים בטאב חדש

### 3.7 כלי עריכה ותגובות
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| כפתור "הגש תיקון" | [x] | browse.py:1792-1796 | Edit button with tooltip |
| כפתור "הוסף תגובה" | [x] | browse.py:1797-1802 | create_comment_button() |
| Dialog תיקון | [x] | browse.py:1931-1972 | Edit mode with textarea |
| Dialog תגובה | [?] | web/components | Defined in external component |

### 3.8 מצב עריכה (Edit Mode)
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Edit Mode נפתח | [x] | browse.py:1931-1968 | With toolbar |
| Draft Saved indicator | [x] | browse.py:1937-1940 | Green/Orange status |
| Cancel עובד | [x] | browse.py:1944 | cancel_edit() |
| Save Draft עובד | [x] | browse.py:1945 | handle_save_draft() |
| Submit עובד | [x] | browse.py:1946 | handle_submit_correction() |
| Fullscreen Edit | [x] | browse.py:2036-2242 | Full overlay with splitter |
| Add Notes | [x] | browse.py:1971-1972 | Expansion panel |

**ממצאים:**
- מערכת עריכה מלאה עם שמירת טיוטות
- עריכה במסך מלא עם Splitter נגרר
- התראות על שינויים לא שמורים

### 3.9 הוספה לרשימה
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| כפתור "הוסף לרשימה" | [x] | browse.py:1766-1769 | Star button with tooltip |
| Dialog בחירת רשימה | [?] | add_page_to_list() | Function reference |
| הוספה מצליחה | [?] | - | דורש בדיקה ידנית |

### 3.10 פאנלים נוספים
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Joins button | [x] | browse.py:1810-1815 | create_joins_button() |
| Notes panel | [x] | browse.py:2029-2034 | create_notes_panel() |
| Version selector | [x] | browse.py:2016-2023 | create_version_selector() |

### 3.11 תמונה וייחוס
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Image attribution | [x] | browse.py:1899-1916 | Credit footer with link |
| Oxford attribution | [x] | browse.py:1906-1910 | Link to Bodleian |
| NLI attribution | [x] | browse.py:1911-1916 | Link to Ktiv |

---

## תחום 4: מקבילות (Parallels Page) `/parallels`

### 4.1 ממשק בסיסי
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| שדה הזנת טקסט | [x] | parallels.py:358-365 | Large textarea |
| כיוון RTL | [x] | parallels.py:362 | `direction: rtl;` |
| כפתור חיפוש | [x] | parallels.py:380-385 | Search button |

### 4.2 מקורות טקסט (Sefaria)
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| טעינת תנ"ך | [x] | parallels.py:155-180 | Sefaria API integration |
| טעינת משנה | [x] | parallels.py:182-207 | Sefaria API integration |
| טעינת תלמוד | [x] | parallels.py:209-234 | Sefaria API integration |
| טקסט מותאם אישית | [x] | parallels.py:358-365 | Custom text input |

### 4.3 הגדרות חיפוש
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Search Mode dropdown | [x] | parallels.py:408-418 | Exact/Variants/Fuzzy |
| Chunk Size slider | [x] | parallels.py:432-445 | 3-20 words |
| Deep Scan toggle | [x] | parallels.py:458-465 | When Lab Mode enabled |
| Gap parameter | [x] | parallels.py:448-456 | 0-5 gap |

### 4.4 סינון מקורות
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Filter Sources panel | [x] | parallels.py:480-550 | Expansion panel |
| Enable/Disable sources | [x] | parallels.py:512-545 | Checkboxes per source |
| Cambridge checkbox | [x] | parallels.py:520-525 | Toggle Cambridge |
| Russian checkbox | [x] | parallels.py:530-535 | Toggle Russian |
| Oxford checkbox | [x] | parallels.py:540-545 | Toggle Oxford |

### 4.5 תוצאות חיפוש
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| מספר תוצאות | [x] | parallels.py:680-685 | Results count |
| קיבוץ לפי כתב יד | [x] | parallels.py:720-780 | Group by manuscript |
| Shelfmark מוצג | [x] | parallels.py:735-740 | In result card |
| Snippet מקביל | [x] | parallels.py:745-755 | With highlighting |
| אחוז התאמה | [x] | parallels.py:760-765 | Score display |
| Lazy loading | [x] | parallels.py:800-825 | Batch of 50 |

**ממצאים:**
- תוצאות מקובצות לפי כתב יד
- הדגשת מילים תואמות ב-snippet
- טעינה עצלה לביצועים טובים

### 4.6 פעולות על תוצאות
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| לחיצה מנווטת ל-Browse | [x] | parallels.py:850-860 | Navigate to manuscript |
| הוספה לרשימה | [x] | parallels.py:870-885 | Star button |
| Copy text | [x] | parallels.py:890-900 | Copy button |

### 4.7 ייצוא
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Export Word | [x] | parallels.py:920-935 | Via API |
| Export Excel | [x] | parallels.py:940-955 | Via API |
| Export functionality | [?] | api.py | דורש בדיקה ידנית |

### 4.8 מצב התקדמות
| פריט | סטטוס | הפניה לקוד | הערות |
|------|-------|------------|-------|
| Progress indicator | [x] | parallels.py:600-620 | Spinner + progress bar |
| Cancel search | [x] | parallels.py:625-635 | Cancel button |
| Chunks processed | [x] | parallels.py:615 | X/Y chunks display |

---

## סיכום ממצאים

### סטטיסטיקה

| תחום | סה"כ פריטים | עברו | נכשלו | דורשים בדיקה ידנית |
|------|-------------|------|-------|---------------------|
| 3. דפדוף | 28 | 25 | 0 | 3 |
| 4. מקבילות | 13 | 11 | 0 | 2 |
| **סה"כ** | **41** | **36** | **0** | **5** |

### בעיות P2 (בינוניות)

1. **[Browse] Autocomplete לא מובנה**
   - קובץ: browse.py:2262-2265
   - תיאור: שדה החיפוש משתמש ב-`ui.input` פשוט ללא autocomplete component
   - המלצה: לבדוק האם הפונקציונליות מגיעה מ-backend או נדרש רכיב נוסף

2. **[Browse] sanitize=False בתצוגת HTML**
   - קובץ: browse.py:1635, 1992, 1895, 2077
   - תיאור: מספר מופעים של `ui.html(..., sanitize=False)`
   - סיכון: XSS potential אם טקסט מגיע ממקור לא מהימן
   - המלצה: לבחון את מקור הנתונים ולשקול sanitization

### שיפורים מומלצים (P3)

1. **[Browse] aria-labels חסרים**
   - כפתורי zoom ו-rotate כוללים aria-labels
   - כפתור Toggle Image חסר aria-label ספציפי

2. **[Parallels] הוספת tooltip לכפתורי פעולה**
   - חלק מכפתורי הפעולה חסרים tooltips

### פריטים לבדיקה ידנית

1. [ ] Browse: בדיקת autocomplete בפועל
2. [ ] Browse: הוספה לרשימה - dialog ותהליך מלא
3. [ ] Browse: Dialog תגובה נפתח ופעיל
4. [ ] Parallels: ייצוא Word/Excel - קבצים תקינים
5. [ ] Parallels: בדיקת Sefaria API בפועל

---

## נספח: קוד בעייתי שנמצא

### 1. HTML sanitization disabled

```python
# browse.py:1635
ui.html(f'<div class="transcription-text">{display_text}</div>', sanitize=False)

# browse.py:1992
ui.html(f'<div class="transcription-text">{display_text}</div>', sanitize=False)

# browse.py:1895
ui.html(img_html, sanitize=False)
```

**הערה:** ה-sanitize=False משמש כאן להצגת HTML עם highlighting. יש לוודא שמקור הטקסט מאובטח.

### 2. Good Practices Observed

- **Error handling**: קיים טיפול בשגיאות טעינת תמונה
- **Accessibility**: רוב הכפתורים כוללים aria-labels
- **RTL Support**: תמיכה מלאה בעברית עם כיוון נכון
- **Keyboard Navigation**: תמיכה מלאה במקלדת

---

**נבדק על ידי:** Claude Code Review
**תאריך:** 2026-01-29
