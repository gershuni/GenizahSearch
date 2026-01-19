# תוכנית התאמה לניידים - GenizahSearch

## מצב נוכחי

### טכנולוגיות
- **Framework**: NiceGUI (Python-based, Vue 3 + Quasar)
- **סגנונות**: CSS Variables + Tailwind-like utility classes
- **Breakpoints קיימים**: 768px (tablet), 1024px (desktop קטן)
- **תמיכה ב-RTL**: מלאה לעברית

### מה עובד
- תפריט המבורגר קיים להסתרת הסרגל הצדדי
- כמה Media Queries בסיסיים
- Flex-wrap בפריסת כרטיסים
- תמיכה מובנית ב-RTL

### מה חסר
- אין breakpoints לניידים קטנים (320px-480px)
- אזורי לחיצה קטנים מדי למגע
- פריסות גריד לא מותאמות
- רוחב sidebar גדול מדי (280px)
- חיפוש מהיר מוסתר בניידים
- תמונות וצפיינים לא מותאמים

---

## שלב 1: תשתית CSS גלובלית

### 1.1 הוספת Breakpoints חדשים

**קובץ:** `web/main.py` (COMMON_STYLES)

```css
/* Mobile breakpoints להוספה */
@media (max-width: 320px) { /* iPhone SE, small phones */ }
@media (max-width: 375px) { /* iPhone 12/13 mini */ }
@media (max-width: 480px) { /* Standard mobile */ }
@media (max-width: 640px) { /* Large mobile / phablet */ }
@media (max-width: 768px) { /* Tablet - קיים */ }
@media (max-width: 1024px) { /* Small desktop - קיים */ }
```

### 1.2 CSS Variables לניידים

**להוסיף ל-`:root`:**

```css
/* Mobile-specific spacing */
--spacing-mobile-xs: 4px;
--spacing-mobile-sm: 8px;
--spacing-mobile-md: 12px;
--spacing-mobile-lg: 16px;

/* Mobile touch targets (מינימום 44px לפי Apple) */
--touch-target-min: 44px;
--touch-target-comfortable: 48px;

/* Mobile font sizes */
--font-size-mobile-xs: 0.75rem;   /* 12px */
--font-size-mobile-sm: 0.875rem;  /* 14px */
--font-size-mobile-base: 1rem;    /* 16px - מונע zoom באייפון */
--font-size-mobile-lg: 1.125rem;  /* 18px */
--font-size-mobile-xl: 1.25rem;   /* 20px */

/* Mobile sidebar width */
--sidebar-width-mobile: 260px;
--sidebar-width-tablet: 280px;
```

### 1.3 Global Mobile Styles

```css
/* Touch-friendly elements */
@media (max-width: 768px) {
    /* מניעת zoom על input */
    input, select, textarea {
        font-size: 16px !important;
    }

    /* אזורי לחיצה גדולים יותר */
    button, .clickable, a {
        min-height: var(--touch-target-min);
        min-width: var(--touch-target-min);
    }

    /* מניעת horizontal scroll */
    body {
        overflow-x: hidden;
    }

    /* שיפור scroll behavior */
    * {
        -webkit-overflow-scrolling: touch;
    }
}
```

---

## שלב 2: Header (כותרת עליונה)

### 2.1 בעיות נוכחיות
- חיפוש מהיר מוסתר לגמרי בניידים
- לוגו ואלמנטים דחוסים
- כפתורים קטנים מדי למגע

### 2.2 שינויים נדרשים

**קובץ:** `web/main.py` (שורות 696-738)

```css
@media (max-width: 480px) {
    .app-header {
        padding: 0 8px !important;
        height: 56px !important; /* קטן יותר בניידים */
    }

    .header-logo {
        font-size: 1rem !important;
    }

    /* הסתרת טקסט הלוגו, השארת אייקון */
    .logo-text {
        display: none;
    }

    /* כפתורי header גדולים יותר */
    .header-button {
        padding: 12px !important;
        min-width: 44px !important;
        min-height: 44px !important;
    }
}

@media (max-width: 640px) {
    /* חיפוש מהיר - מעבר לשורה נפרדת או popup */
    .quick-search-container {
        position: fixed;
        top: 56px;
        left: 0;
        right: 0;
        padding: 8px 16px;
        background: var(--bg-header);
        display: none; /* נפתח בלחיצה */
        z-index: 100;
    }

    .quick-search-container.open {
        display: block;
    }

    .quick-search-toggle {
        display: flex !important;
    }
}
```

### 2.3 משימות יישום

- [ ] הוסף כפתור חיפוש באייקון שפותח שדה חיפוש
- [ ] הקטן את הלוגו והסתר טקסט בניידים קטנים
- [ ] הגדל את כל כפתורי ה-Header ל-44px מינימום
- [ ] בדוק מרווחים בין אלמנטים

---

## שלב 3: Sidebar (תפריט צדדי)

### 3.1 בעיות נוכחיות
- רוחב 280px - כמעט כל המסך בניידים קטנים
- אין overlay כהה מאחורי התפריט
- אין gesture לסגירה (swipe)

### 3.2 שינויים נדרשים

**קובץ:** `web/main.py` (שורות 740-809)

```css
@media (max-width: 480px) {
    .q-drawer {
        width: 85vw !important; /* 85% מרוחב המסך */
        max-width: 280px !important;
    }

    .nav-item {
        padding: 14px 20px !important;
        min-height: 48px !important;
        font-size: 1rem !important;
    }

    .nav-icon {
        font-size: 1.25rem !important;
    }

    /* Footer actions */
    .drawer-footer {
        padding: 12px !important;
    }

    .theme-switcher button {
        width: 40px !important;
        height: 40px !important;
    }
}

/* Overlay לסגירת התפריט */
.drawer-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.5);
    z-index: 999;
    display: none;
}

.drawer-overlay.visible {
    display: block;
}
```

### 3.3 משימות יישום

- [ ] שנה רוחב sidebar ל-85vw (מקסימום 280px)
- [ ] הוסף overlay כהה שסוגר את התפריט בלחיצה
- [ ] הגדל פריטי ניווט ל-48px מינימום
- [ ] הוסף תמיכה ב-swipe לסגירה (אם NiceGUI תומך)
- [ ] בדוק שה-footer נשאר בתחתית

---

## שלב 4: דף הבית (Home)

### 4.1 בעיות נוכחיות
- כרטיסי סטטיסטיקות בפריסה אופקית
- כרטיסי כלים עם min-width קבוע
- גופנים גדולים מדי

### 4.2 שינויים נדרשים

**קובץ:** `web/pages/home.py`

```css
@media (max-width: 480px) {
    /* כרטיסי סטטיסטיקות */
    .stats-grid {
        grid-template-columns: 1fr !important;
        gap: 12px !important;
    }

    .stat-card {
        padding: 12px !important;
    }

    .stat-value {
        font-size: 1.5rem !important;
    }

    .stat-label {
        font-size: 0.875rem !important;
    }

    /* כרטיסי כלים */
    .tools-grid {
        grid-template-columns: 1fr !important;
    }

    .tool-card {
        min-width: unset !important;
        padding: 16px !important;
    }

    .tool-card-title {
        font-size: 1.125rem !important;
    }

    .tool-card-description {
        font-size: 0.875rem !important;
    }

    /* כותרת הדף */
    .page-title {
        font-size: 1.25rem !important;
    }

    .page-subtitle {
        font-size: 0.875rem !important;
    }
}

@media (max-width: 640px) {
    .stats-grid {
        grid-template-columns: repeat(2, 1fr) !important;
    }
}
```

### 4.3 משימות יישום

- [ ] שנה grid לעמודה בודדת בניידים קטנים
- [ ] הסר min-width מכרטיסים
- [ ] הקטן גופנים
- [ ] צמצם padding
- [ ] בדוק שכל התוכן נראה ללא scroll אופקי

---

## שלב 5: דף חיפוש (Search)

### 5.1 בעיות נוכחיות
- Splitter לא מתאים לניידים
- פילטרים בגריד 3 עמודות
- כפתורי חיפוש קטנים
- תוצאות ופרטים זה לצד זה

### 5.2 שינויים נדרשים

**קובץ:** `web/pages/search.py`

```css
@media (max-width: 768px) {
    /* ביטול splitter - מעבר לפריסה אנכית */
    .search-splitter {
        flex-direction: column !important;
    }

    .search-results-panel,
    .search-details-panel {
        width: 100% !important;
        max-height: none !important;
    }

    /* תוצאות בחצי עליון */
    .search-results-panel {
        height: 40vh !important;
        min-height: 200px !important;
    }

    /* פרטים בחצי תחתון */
    .search-details-panel {
        flex: 1 !important;
    }
}

@media (max-width: 480px) {
    /* פילטרים בעמודה בודדת */
    .search-filters-grid {
        grid-template-columns: 1fr !important;
        gap: 8px !important;
    }

    /* שדה חיפוש */
    .search-input {
        font-size: 16px !important;
        padding: 12px !important;
    }

    /* כפתורי חיפוש */
    .search-buttons {
        flex-direction: column !important;
        gap: 8px !important;
    }

    .search-button {
        width: 100% !important;
        min-height: 48px !important;
    }

    /* כרטיסי תוצאות */
    .result-card {
        padding: 12px !important;
    }

    .result-title {
        font-size: 1rem !important;
    }

    .result-meta {
        font-size: 0.75rem !important;
    }

    /* Advanced options */
    .advanced-options {
        padding: 12px !important;
    }
}
```

### 5.3 משימות יישום

- [ ] החלף Splitter בפריסה אנכית בניידים
- [ ] שנה פילטרים לעמודה בודדת
- [ ] הגדל שדות קלט ל-16px מינימום
- [ ] הפוך כפתורים לרוחב מלא
- [ ] הוסף כפתור "חזרה לתוצאות" בתצוגת פרטים
- [ ] שקול tabs לתוצאות/פרטים במקום split

---

## שלב 6: דף צפייה במסמכים (Browse)

### 6.1 בעיות נוכחיות
- תמונה וטקסט בפאנלים זה לצד זה
- גובה תמונה 70vh - יותר מדי בניידים
- פקדי zoom קטנים
- transcription panel לא מותאם

### 6.2 שינויים נדרשים

**קובץ:** `web/pages/browse.py`

```css
@media (max-width: 768px) {
    /* כבר קיים חלקית - לשפר */
    .viewer-panels {
        flex-direction: column !important;
    }

    .image-panel {
        width: 100% !important;
        height: 40vh !important;
        min-height: 250px !important;
    }

    .transcription-panel-wrapper {
        width: 100% !important;
        height: auto !important;
        max-height: 50vh !important;
    }
}

@media (max-width: 480px) {
    .image-panel {
        height: 35vh !important;
        min-height: 200px !important;
    }

    /* פקדי zoom */
    .zoom-controls {
        position: fixed !important;
        bottom: 70px !important;
        right: 16px !important;
        flex-direction: column !important;
        gap: 8px !important;
    }

    .zoom-button {
        width: 48px !important;
        height: 48px !important;
        border-radius: 50% !important;
        box-shadow: var(--shadow-lg) !important;
    }

    /* Navigation buttons */
    .nav-button {
        width: 44px !important;
        height: 44px !important;
    }

    /* Metadata panel */
    .metadata-section {
        padding: 12px !important;
    }

    .metadata-label {
        font-size: 0.75rem !important;
    }

    .metadata-value {
        font-size: 0.875rem !important;
    }

    /* טאבים */
    .viewer-tabs .q-tab {
        padding: 8px 12px !important;
        min-height: 44px !important;
    }
}
```

### 6.3 משימות יישום

- [ ] ערום פאנלים אנכית בטאבלטים
- [ ] הקטן גובה תמונה ל-35vh בניידים
- [ ] הזז פקדי zoom לפינה תחתונה כצפים
- [ ] הגדל כפתורי ניווט
- [ ] הוסף swipe לניווט בין דפים
- [ ] בדוק pinch-to-zoom בתמונה
- [ ] שקול מצב "focus" שמציג רק תמונה או רק טקסט

---

## שלב 7: דף מקבילות (Parallels)

### 7.1 בעיות נוכחיות
- טבלת תוצאות רחבה מדי
- עמודות רבות
- גלילה אופקית בעייתית

### 7.2 שינויים נדרשים

**קובץ:** `web/pages/parallels.py`

```css
@media (max-width: 768px) {
    /* המרת טבלה לכרטיסים */
    .parallels-table {
        display: none !important;
    }

    .parallels-cards {
        display: flex !important;
        flex-direction: column !important;
        gap: 12px !important;
    }

    .parallel-card {
        background: var(--bg-card);
        border-radius: 12px;
        padding: 16px;
        border: 1px solid var(--border-light);
    }

    .parallel-card-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 12px;
    }

    .parallel-card-content {
        font-size: 0.875rem;
        line-height: 1.6;
    }

    .parallel-card-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 12px;
        font-size: 0.75rem;
        color: var(--text-secondary);
    }
}

@media (max-width: 480px) {
    .parallel-card {
        padding: 12px !important;
    }

    /* שדה הזנת טקסט */
    .parallel-input {
        min-height: 120px !important;
        font-size: 16px !important;
    }
}
```

### 7.3 משימות יישום

- [ ] צור תצוגת כרטיסים כחלופה לטבלה בניידים
- [ ] הסתר עמודות לא חיוניות
- [ ] הוסף כפתור "הצג עוד" לפרטים נוספים
- [ ] הגדל שדה הזנת טקסט
- [ ] בדוק שטקסט עברי נקרא נכון

---

## שלב 8: דף גילויים (Discoveries)

### 8.1 בעיות נוכחיות
- כרטיסי גילויים בגריד
- תמונות קטנות
- מידע רב בכל כרטיס

### 8.2 שינויים נדרשים

**קובץ:** `web/pages/discoveries.py`

```css
@media (max-width: 640px) {
    .discoveries-grid {
        grid-template-columns: 1fr !important;
        gap: 16px !important;
    }
}

@media (max-width: 480px) {
    .discovery-card {
        padding: 0 !important;
        overflow: hidden !important;
    }

    .discovery-image {
        width: 100% !important;
        height: 180px !important;
        object-fit: cover !important;
    }

    .discovery-content {
        padding: 12px !important;
    }

    .discovery-title {
        font-size: 1rem !important;
        margin-bottom: 8px !important;
    }

    .discovery-description {
        font-size: 0.875rem !important;
        /* הגבלת שורות */
        display: -webkit-box;
        -webkit-line-clamp: 3;
        -webkit-box-orient: vertical;
        overflow: hidden;
    }

    .discovery-meta {
        font-size: 0.75rem !important;
        margin-top: 8px !important;
    }

    /* Filters */
    .discovery-filters {
        flex-direction: column !important;
        gap: 8px !important;
    }

    .filter-chip {
        width: 100% !important;
        justify-content: center !important;
    }
}
```

### 8.3 משימות יישום

- [ ] שנה גריד לעמודה בודדת בניידים
- [ ] הגדל תמונות כדי למלא רוחב
- [ ] הגבל תיאור ל-3 שורות עם "..."
- [ ] ערום פילטרים אנכית
- [ ] הוסף infinite scroll או pagination

---

## שלב 9: דף רשימות (Lists)

### 9.1 בעיות נוכחיות
- רשימות וכרטיסים בפריסה אופקית
- פעולות רשימה קטנות
- drag & drop לא עובד במגע

### 9.2 שינויים נדרשים

**קובץ:** `web/pages/lists.py`

```css
@media (max-width: 768px) {
    .lists-layout {
        flex-direction: column !important;
    }

    .lists-sidebar {
        width: 100% !important;
        max-height: 200px !important;
        border-bottom: 1px solid var(--border-light) !important;
        border-right: none !important;
    }

    .list-content {
        width: 100% !important;
    }
}

@media (max-width: 480px) {
    .list-item {
        padding: 12px !important;
    }

    .list-item-title {
        font-size: 0.875rem !important;
    }

    /* פעולות רשימה */
    .list-actions {
        position: fixed !important;
        bottom: 0 !important;
        left: 0 !important;
        right: 0 !important;
        padding: 12px 16px !important;
        background: var(--bg-card) !important;
        border-top: 1px solid var(--border-light) !important;
        display: flex !important;
        gap: 8px !important;
        z-index: 100 !important;
    }

    .list-action-button {
        flex: 1 !important;
        min-height: 44px !important;
    }

    /* הוספה לרשימה */
    .add-to-list-dialog {
        width: 95vw !important;
        max-width: none !important;
    }
}
```

### 9.3 משימות יישום

- [ ] ערום sidebar ותוכן אנכית
- [ ] העבר פעולות רשימה ל-bottom bar קבוע
- [ ] החלף drag & drop בכפתורי "העלה/הורד"
- [ ] הגדל אזורי לחיצה
- [ ] בדוק דיאלוגים בניידים

---

## שלב 10: דף הגדרות (Settings)

### 10.1 בעיות נוכחיות
- גריד 2 עמודות קבוע
- טפסים צרים

### 10.2 שינויים נדרשים

**קובץ:** `web/pages/settings.py`

```css
@media (max-width: 768px) {
    .settings-grid {
        grid-template-columns: 1fr !important;
        gap: 16px !important;
    }
}

@media (max-width: 480px) {
    .settings-section {
        padding: 16px !important;
    }

    .settings-title {
        font-size: 1rem !important;
    }

    .settings-input {
        font-size: 16px !important; /* מניעת zoom */
    }

    .settings-button {
        width: 100% !important;
        min-height: 48px !important;
    }

    /* Toggle switches */
    .settings-toggle {
        min-height: 44px !important;
    }
}
```

### 10.3 משימות יישום

- [ ] שנה גריד לעמודה בודדת
- [ ] הגדל שדות קלט
- [ ] הפוך כפתורים לרוחב מלא
- [ ] וודא שכל הטוגלים גדולים מספיק

---

## שלב 11: דף עזרה (Help)

### 11.1 בעיות נוכחיות
- תוכן עם sidebar
- קישורים קטנים
- טקסט ארוך

### 11.2 שינויים נדרשים

**קובץ:** `web/pages/help.py`

```css
@media (max-width: 768px) {
    .help-layout {
        flex-direction: column !important;
    }

    .help-sidebar {
        width: 100% !important;
        position: sticky !important;
        top: 56px !important;
        background: var(--bg-primary) !important;
        z-index: 10 !important;
        border-bottom: 1px solid var(--border-light) !important;
    }

    .help-nav {
        display: flex !important;
        overflow-x: auto !important;
        white-space: nowrap !important;
        padding: 12px !important;
        gap: 8px !important;
    }

    .help-nav-item {
        flex-shrink: 0 !important;
        padding: 8px 16px !important;
        border-radius: 20px !important;
        background: var(--bg-secondary) !important;
    }

    .help-content {
        width: 100% !important;
        padding: 16px !important;
    }
}

@media (max-width: 480px) {
    .help-content h2 {
        font-size: 1.25rem !important;
    }

    .help-content h3 {
        font-size: 1.125rem !important;
    }

    .help-content p,
    .help-content li {
        font-size: 0.9375rem !important;
        line-height: 1.7 !important;
    }

    .help-code {
        font-size: 0.8125rem !important;
        overflow-x: auto !important;
    }
}
```

### 11.3 משימות יישום

- [ ] המר sidebar לניווט אופקי sticky
- [ ] הוסף גלילה אופקית לניווט
- [ ] הגדל גופנים לקריאות
- [ ] בדוק קוד וטבלאות

---

## שלב 12: דיאלוגים ו-Modals

### 12.1 שינויים גלובליים

```css
@media (max-width: 480px) {
    .q-dialog {
        padding: 0 !important;
    }

    .q-dialog__inner {
        width: 100% !important;
        max-width: 100% !important;
        height: 100% !important;
        max-height: 100% !important;
    }

    .q-card {
        border-radius: 0 !important;
        height: 100% !important;
    }

    /* או bottom sheet style */
    .q-dialog__inner--bottom-sheet {
        width: 100% !important;
        max-height: 85vh !important;
        border-radius: 16px 16px 0 0 !important;
    }

    .dialog-header {
        position: sticky !important;
        top: 0 !important;
        background: var(--bg-card) !important;
        padding: 16px !important;
        border-bottom: 1px solid var(--border-light) !important;
        z-index: 1 !important;
    }

    .dialog-content {
        padding: 16px !important;
        overflow-y: auto !important;
    }

    .dialog-actions {
        position: sticky !important;
        bottom: 0 !important;
        background: var(--bg-card) !important;
        padding: 16px !important;
        border-top: 1px solid var(--border-light) !important;
        display: flex !important;
        gap: 12px !important;
    }

    .dialog-button {
        flex: 1 !important;
        min-height: 48px !important;
    }
}
```

### 12.2 משימות יישום

- [ ] הפוך דיאלוגים ל-full-screen בניידים
- [ ] או השתמש ב-bottom sheet pattern
- [ ] הוסף header קבוע עם כפתור סגירה
- [ ] הוסף actions קבועים בתחתית
- [ ] בדוק כל דיאלוג בנפרד

---

## שלב 13: טעינה ומצבי שגיאה

### 13.1 שינויים נדרשים

```css
@media (max-width: 480px) {
    /* Loading skeleton */
    .skeleton-card {
        height: 120px !important;
    }

    /* Error state */
    .error-container {
        padding: 24px 16px !important;
        text-align: center !important;
    }

    .error-icon {
        font-size: 48px !important;
    }

    .error-message {
        font-size: 1rem !important;
    }

    .error-action {
        width: 100% !important;
        min-height: 48px !important;
    }

    /* Empty state */
    .empty-state {
        padding: 32px 16px !important;
    }

    .empty-icon {
        font-size: 64px !important;
    }

    .empty-title {
        font-size: 1.125rem !important;
    }

    .empty-description {
        font-size: 0.875rem !important;
    }

    /* Pull to refresh indicator */
    .pull-to-refresh {
        position: fixed;
        top: 56px;
        left: 50%;
        transform: translateX(-50%);
        padding: 8px 16px;
        background: var(--bg-card);
        border-radius: 20px;
        box-shadow: var(--shadow-md);
        z-index: 100;
    }
}
```

### 13.2 משימות יישום

- [ ] התאם skeleton loaders לניידים
- [ ] מרכז הודעות שגיאה
- [ ] הפוך כפתורי action לרוחב מלא
- [ ] שקול הוספת pull-to-refresh

---

## שלב 14: ביצועים (Performance)

### 14.1 אופטימיזציות נדרשות

1. **תמונות:**
   - [ ] השתמש ב-`loading="lazy"` לתמונות
   - [ ] הגדר `srcset` לגדלים שונים
   - [ ] דחס תמונות לניידים

2. **גופנים:**
   - [ ] הגבל משקלי גופן (`font-display: swap`)
   - [ ] טען רק תווים נדרשים לעברית

3. **CSS:**
   - [ ] הפרד CSS קריטי
   - [ ] טען CSS לא קריטי באופן אסינכרוני

4. **JavaScript:**
   - [ ] צמצם bundle size
   - [ ] השתמש ב-code splitting

5. **רשת:**
   - [ ] הפעל gzip/brotli compression
   - [ ] השתמש ב-HTTP/2
   - [ ] הגדר caching headers

### 14.2 בדיקות ביצועים

- [ ] בדוק ב-Lighthouse (יעד: 90+ Mobile)
- [ ] בדוק ב-WebPageTest
- [ ] בדוק עם throttling (3G איטי)
- [ ] מדוד First Contentful Paint < 2s
- [ ] מדוד Time to Interactive < 5s

---

## שלב 15: בדיקות ו-QA

### 15.1 מכשירים לבדיקה

| מכשיר | רוחב | גובה | Pixel Ratio |
|-------|------|------|-------------|
| iPhone SE | 375px | 667px | 2 |
| iPhone 12/13 | 390px | 844px | 3 |
| iPhone 14 Pro Max | 430px | 932px | 3 |
| Samsung Galaxy S21 | 360px | 800px | 3 |
| iPad Mini | 768px | 1024px | 2 |
| iPad Pro 11" | 834px | 1194px | 2 |

### 15.2 רשימת בדיקות

**פונקציונליות:**
- [ ] ניווט ראשי עובד
- [ ] חיפוש עובד
- [ ] צפייה במסמכים עובדת
- [ ] רשימות עובדות
- [ ] הגדרות עובדות
- [ ] החלפת שפה עובדת
- [ ] החלפת ערכת נושא עובדת
- [ ] התחברות/התנתקות עובדת

**UI/UX:**
- [ ] אין scroll אופקי
- [ ] כל הטקסט קריא
- [ ] כל הכפתורים לחיצים
- [ ] דיאלוגים נפתחים ונסגרים
- [ ] תמונות נטענות
- [ ] RTL תקין
- [ ] גופנים נטענים

**ביצועים:**
- [ ] זמן טעינה < 3 שניות ב-4G
- [ ] אינטראקציה חלקה (60fps)
- [ ] אין "jumps" בטעינה

---

## שלב 16: יישום בפועל - סדר עדיפויות

### עדיפות גבוהה (שבוע 1):
1. תשתית CSS גלובלית ו-breakpoints
2. Header התאמה
3. Sidebar התאמה
4. דף הבית התאמה

### עדיפות בינונית (שבוע 2):
5. דף חיפוש התאמה
6. דף צפייה במסמכים התאמה
7. דיאלוגים התאמה

### עדיפות רגילה (שבוע 3):
8. דף מקבילות התאמה
9. דף גילויים התאמה
10. דף רשימות התאמה
11. דף הגדרות התאמה
12. דף עזרה התאמה

### עדיפות נמוכה (שבוע 4):
13. מצבי טעינה ושגיאה
14. אופטימיזציות ביצועים
15. בדיקות QA מקיפות

---

## קבצים לעריכה

| קובץ | תיאור | עדיפות |
|------|-------|--------|
| `web/main.py` | תשתית, Header, Sidebar | גבוהה |
| `web/pages/home.py` | דף הבית | גבוהה |
| `web/pages/search.py` | חיפוש | גבוהה |
| `web/pages/browse.py` | צפייה במסמכים | גבוהה |
| `web/pages/parallels.py` | מקבילות | בינונית |
| `web/pages/discoveries.py` | גילויים | בינונית |
| `web/pages/lists.py` | רשימות | בינונית |
| `web/pages/settings.py` | הגדרות | נמוכה |
| `web/pages/help.py` | עזרה | נמוכה |
| `web/components/*.py` | קומפוננטות | בינונית |

---

## סיכום

מסמך זה מתאר 16 שלבים להתאמת האתר לניידים. היישום דורש:

1. **שינויים ב-CSS**: הוספת media queries, התאמת גדלים
2. **שינויים במבנה**: פריסות אנכיות במקום אופקיות
3. **שינויים ב-UX**: אזורי מגע גדולים, ניווט מותאם
4. **בדיקות**: במגוון מכשירים ותרחישים

המלצה: להתחיל מהתשתית הגלובלית ולהתקדם לפי סדר העדיפויות.
