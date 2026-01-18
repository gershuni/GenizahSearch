# דוח סקירת קוד - GenizahSearch
## חידושים, פערים, באגים ושדות מיותרים

**תאריך:** 2026-01-18

---

## 1. חידושים בקוד (Features)

### 1.1 מערכת תיקונים מקיפה
- **Workflow מלא:** DRAFT → PENDING → UNDER_REVIEW → APPROVED/REJECTED/NEEDS_REVISION
- **הצבעות קהילתיות:** upvotes/downvotes עם חישוב quality_score
- **מנגנון רפוטציה:** נקודות למשתמשים לפי תרומות

### 1.2 מערכת תגליות (Discoveries)
- **תמיכה במספר Shelfmarks:** שדה `additional_shelfmarks` מאפשר קישור למספר כתבי יד
- **קישורי כתבי יד קשורים:** שדה `related_manuscripts` עם סוגי קשר (parallel, continuation, fragment)
- **סימון שאלות כ"נענו":** `is_answered` לשאלות קהילתיות

### 1.3 מערכת תגובות מתקדמת
- **Threading:** תגובות מקוננות עם `parent_id`
- **תגובות רגשיות (Reactions):** like, helpful, insightful, thanks, disagree
- **תגובות פרטיות:** `is_public` flag לתגובות אישיות

### 1.4 עורך טקסט משופר (Web)
- **Auto-save:** שמירה אוטומטית ל-localStorage כל 30 שניות
- **טיוטות מקומיות:** ניהול טיוטות לפני שליחה לשרת
- **ספירת שינויים:** מילים שנוספו/הוסרו בזמן אמת

---

## 2. בעיות חמורות (Critical Issues) 🔴

### 2.1 חוסר עקביות בין document_id ל-system_id

**קובץ:** `backend/services/correction_service.py:48-56`

```python
doc_id = data.document_id or data.system_id  # אחד מהם יכול להיות None
```

**בעיה:** הקוד משתמש ב-`document_id` ו-`system_id` בצורה לא עקבית. בחלק מהמקומות נבדק רק `document_id`, בחלק רק `system_id`.

**השפעה:** תיקונים עלולים לא להתחבר נכון לדף המסמך.

---

### 2.2 בדיקת תיקון קיים לא מלאה

**קובץ:** `backend/services/correction_service.py:51-59`

```python
existing = db.query(Correction).filter(
    Correction.author_id == user.id,
    or_(
        Correction.document_id == doc_id,
        Correction.system_id == doc_id
    ),
    Correction.page_number == page_num,
    Correction.status.in_([CorrectionStatus.DRAFT, CorrectionStatus.PENDING, CorrectionStatus.APPROVED])
).first()
```

**בעיה:** אם `page_num` הוא `None` (כשלא מסופק), הבדיקה לא תעבוד נכון - תמיד יחזור None גם אם יש תיקון קיים.

**תיקון מוצע:** להוסיף טיפול ב-`page_number=None`:
```python
if page_num:
    q = q.filter(Correction.page_number == page_num)
else:
    q = q.filter(Correction.page_number.is_(None))
```

---

### 2.3 אין ולידציה של shelfmark mentions בתגובות

**קובץ:** `web/components/comment_dialog.py:100-102`

```python
mention = f"[[shelfmark:{sm}|id:{did}]]"
current = comment_text.value or ''
comment_text.value = current + ' ' + mention + ' '
```

**בעיה:** ה-mentions נשמרים כטקסט פשוט אבל אין קוד בצד השרת שמפרש אותם או מאמת שה-IDs קיימים. זה יכול להוביל ל-broken links.

---

### 2.4 page_number vs line_number - בלבול סמנטי

**קבצים מרובים:**
- `backend/models/correction.py:88,91` - יש גם `line_number` וגם `page_number`
- `web/components/comment_dialog.py:209-210` - משתמש ב-`line_number` לציין מספר דף

**בעיה:** השדות משמשים בצורה לא עקבית:
- לפעמים `line_number` = שורה בתוך טקסט
- לפעמים `line_number` = מספר תמונה/דף
- `page_number` קיים בנפרד

**השפעה:** בלבול בממשק המשתמש ובקישורים לדפים ספציפיים.

---

## 3. באגים קטנים יותר (Minor Bugs) 🟡

### 3.1 Auto-save לא עובד בפועל

**קובץ:** `web/components/text_editor.py:374`

```python
# Start auto-save in background
ui.timer(AUTO_SAVE_INTERVAL, lambda: None)  # Placeholder - NiceGUI handles async differently
```

**בעיה:** ה-auto-save מוגדר אבל ה-timer בפועל לא עושה כלום (`lambda: None`). הקוד ב-`auto_save()` אסינכרוני ולעולם לא נקרא.

---

### 3.2 חוסר טיפול ב-None בתאריכים

**קובץ:** `corrections_ui.py:539,846`

```python
date_str = correction.created_at[:10] if correction.created_at else ""
```

**בעיה:** אם `created_at` הוא אובייקט datetime ולא string, הגישה ב-slicing תיכשל.

---

### 3.3 Disconnect לא בטוח

**קובץ:** `corrections_ui.py:748-751`

```python
try:
    self.login_btn.clicked.disconnect()
except:
    pass
```

**בעיה:** `except:` תופס את כל החריגות כולל SystemExit ו-KeyboardInterrupt. צריך להיות `except TypeError:` או `except RuntimeError:`.

---

### 3.4 חוסר אימות rejection_reason

**קובץ:** `web/pages/corrections.py:509-518`

```python
async def reject(c=corr, notes=review_notes):
    result = await api_call("POST", f"/corrections/{c['id']}/review", {
        "action": "reject",
        "notes": notes.value or None  # שולח notes במקום rejection_reason
    })
```

**בעיה:** השרת מצפה ל-`rejection_reason` כאשר action="reject", אבל הווב שולח `notes`. הבאקאנד יחזיר שגיאה: "Rejection reason is required".

---

### 3.5 חסר await ב-click handlers

**קובץ:** `web/pages/discoveries.py:91-92`

```python
type_filter.on('update:model-value', lambda: refresh_feed())
```

**בעיה:** `refresh_feed()` היא async function, אבל היא נקראת ללא `await`. יכול להוביל להתנהגות לא צפויה.

**תיקון:** `lambda e: asyncio.create_task(refresh_feed())`

---

## 4. פערים בין Web ל-Desktop 📊

| תכונה | Web | Desktop | הערות |
|--------|-----|---------|--------|
| **עורך טקסט מתקדם** | ✅ Side-by-side, auto-save | ⚠️ פשוט יותר | Desktop חסר auto-save וטיוטות מקומיות |
| **תגובות פרטיות** | ✅ `is_public` checkbox | ❌ לא קיים | Desktop לא מאפשר תגובות פרטיות |
| **Shelfmark mentions** | ✅ בתגובות | ❌ לא קיים | Desktop לא תומך ב-[[shelfmark:X]] |
| **לוח מובילים** | ✅ tab ייעודי | ⚠️ חלקי | Web מציג אך מסתיר (designers prefer anonymity) |
| **Admin UI** | ✅ hide/unhide, pin | ✅ pin/hide | שווים |
| **Additional Shelfmarks** | ✅ תצוגה + יצירה | ✅ תצוגה בלבד | Desktop לא מאפשר הוספת shelfmarks נוספים ביצירת discovery |
| **Related Manuscripts** | ⚠️ תצוגה | ❌ לא קיים | אף אחד מהם לא מאפשר הגדרת קשרים |
| **Comment Reactions** | ❌ לא ממומש | ❌ לא ממומש | קיים ב-backend אבל לא ב-UI |
| **Draft Management** | ✅ localStorage | ⚠️ חלקי | Desktop שומר ב-API בלבד |
| **Review Panel** | ✅ מלא | ⚠️ חסר | Desktop חסר UI לreview |

### פירוט הפערים:

#### 4.1 Desktop חסר תגובות פרטיות
**קובץ:** `corrections_ui.py` - `CommentDialog`

אין checkbox ל-"Private comment" כמו בווב. כל התגובות ב-Desktop הן ציבוריות.

#### 4.2 Desktop חסר עורך טקסט מתקדם
**קובץ:** `corrections_ui.py:226-417` - `CorrectionSubmitDialog`

הדיאלוג פשוט יותר:
- אין תצוגה side-by-side
- אין auto-save
- אין טיוטות מקומיות
- אין keyboard shortcuts

#### 4.3 Web חסר Reactions
**קובץ:** `web/components/comment_dialog.py`

Backend תומך ב-5 סוגי reactions אבל UI לא מציג אותם.

---

## 5. שדות מיותרים או כפולים 🗑️

### 5.1 Correction Model

| שדה | סיבה להיות מיותר |
|------|------------------|
| `relevance_score` | מוגדר אבל לעולם לא מחושב או משתמשים בו |
| `applied_at` | כפול - יש גם `reviewed_at` שמציין אותו דבר ל-approved |
| `char_start`, `char_end` | נדיר שמשתמשים בהם - רוב התיקונים מבוססי טקסט |

**קובץ:** `backend/models/correction.py:89-90,129,135`

```python
char_start = Column(Integer, nullable=True)  # נדיר בשימוש
char_end = Column(Integer, nullable=True)    # נדיר בשימוש
relevance_score = Column(Float, default=0.0) # לעולם לא מחושב
applied_at = Column(DateTime, nullable=True) # כפול עם reviewed_at
```

### 5.2 Comment Model

| שדה | סיבה להיות מיותר |
|------|------------------|
| `resolved_at` | תאריך רזולוציה - לעולם לא מוצג ב-UI |
| `reaction_count` | cached count שלא מתעדכן כראוי |

**קובץ:** `backend/models/comment.py:85,89`

### 5.3 Discovery Model

| שדה | סיבה להיות מיותר |
|------|------------------|
| `view_count` | מוגדר אבל לא מעודכן בשום מקום |
| `is_featured` | כפול עם status=FEATURED |
| `downvotes` | בפועל לא משתמשים - רק upvotes בשימוש |

**קובץ:** `backend/models/discovery.py:109,104,112`

### 5.4 User Model

| שדה | סיבה להיות מיותר |
|------|------------------|
| `avatar_url` | מוגדר אבל אין UI שמציג אותו |
| `settings` | מוגדר כ-Text אבל לא משתמשים |
| `api_key` | מוגדר אבל אין endpoint ל-API key authentication |

**קובץ:** `backend/models/user.py:56,77,74`

---

## 6. המלצות לתיקון

### עדיפות גבוהה 🔴

1. **תקן את הבדיקה של תיקון קיים** - הוסף טיפול ב-`page_number=None`
2. **תקן rejection_reason בווב** - שלח את השדה הנכון
3. **הפעל את ה-auto-save** - יישם את הטיימר בצורה נכונה

### עדיפות בינונית 🟡

4. **תאם page_number/line_number** - בחר שם אחיד ושמור עליו
5. **הוסף תגובות פרטיות ל-Desktop**
6. **הוסף Comment Reactions ל-UI**

### עדיפות נמוכה 🟢

7. **הסר שדות מיותרים** - נקה את המודלים
8. **הוסף ולידציה ל-shelfmark mentions**
9. **שפר את עורך הטקסט ב-Desktop**

---

## 7. סיכום סטטיסטי

| קטגוריה | כמות |
|---------|------|
| בעיות חמורות | 4 |
| באגים קטנים | 5 |
| פערים web/desktop | 10 |
| שדות מיותרים | 11 |
| **סה"כ ממצאים** | **30** |

---

*דוח זה נוצר על ידי סקירת קוד אוטומטית*
