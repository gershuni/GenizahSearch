# Mobile Responsive Design Plan - GenizahSearch

> **Status:** Planned (not yet started)
> **Date:** 2025 (updated 2026-03-13)
>
> **UX/UI Expert Review**: This document has been enhanced with 2025 best practices from leading UX/UI guidelines including Apple HIG, Google Material Design, and WCAG accessibility standards.

---

## Executive Summary

This document outlines a comprehensive plan to make GenizahSearch mobile-friendly. The implementation follows a **mobile-first approach** with progressive enhancement, adhering to current industry standards for touch targets, accessibility, and responsive design.

### Key Standards Applied
- **Touch targets**: Minimum 44×44px (Apple HIG) / 48×48dp (Material Design)
- **Typography**: Minimum 16px for inputs (prevents iOS zoom), 14-17pt for body text
- **Contrast**: WCAG 4.5:1 for normal text, 3:1 for large text
- **Performance**: Target 60fps animations, <3s load time on 4G
- **Breakpoints**: 320px, 375px, 480px, 640px, 768px, 1024px

---

## Current State Analysis

### Technology Stack
- **Framework**: NiceGUI (Python-based, Vue 3 + Quasar)
- **Styling**: CSS Variables + Tailwind-like utility classes
- **Existing Breakpoints**: 768px (tablet), 1024px (small desktop)
- **RTL Support**: Full Hebrew language support

### What Works
- Hamburger menu exists for sidebar toggle
- Basic media queries at 768px and 1024px
- Flex-wrap on card layouts
- Built-in RTL support

### Critical Gaps
- No breakpoints for small mobile devices (320px-480px)
- Touch targets too small for comfortable interaction
- Grid layouts not responsive
- Sidebar width (280px) too wide for mobile
- Quick search hidden on mobile devices
- Images and viewers not optimized for small screens

---

## Phase 1: Global CSS Infrastructure

### 1.1 Add Mobile Breakpoints

**File:** `web/main.py` (COMMON_STYLES)

```css
/* Mobile-first breakpoints */
@media (max-width: 320px) { /* iPhone SE, small phones */ }
@media (max-width: 375px) { /* iPhone 12/13 mini */ }
@media (max-width: 480px) { /* Standard mobile */ }
@media (max-width: 640px) { /* Large mobile / phablet */ }
@media (max-width: 768px) { /* Tablet - existing */ }
@media (max-width: 1024px) { /* Small desktop - existing */ }
```

### 1.2 Mobile CSS Variables

**Add to `:root`:**

```css
/* Mobile-specific spacing */
--spacing-mobile-xs: 4px;
--spacing-mobile-sm: 8px;
--spacing-mobile-md: 12px;
--spacing-mobile-lg: 16px;

/* Touch targets (Apple HIG: 44px min, Material: 48dp recommended) */
--touch-target-min: 44px;
--touch-target-comfortable: 48px;

/* Mobile font sizes */
--font-size-mobile-xs: 0.75rem;   /* 12px */
--font-size-mobile-sm: 0.875rem;  /* 14px */
--font-size-mobile-base: 1rem;    /* 16px - prevents iOS zoom */
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
    /* Prevent iOS zoom on input focus */
    input, select, textarea {
        font-size: 16px !important;
    }

    /* Larger touch targets */
    button, .clickable, a {
        min-height: var(--touch-target-min);
        min-width: var(--touch-target-min);
    }

    /* Prevent horizontal scroll */
    body {
        overflow-x: hidden;
    }

    /* Smooth scrolling for iOS */
    * {
        -webkit-overflow-scrolling: touch;
    }

    /* Respect reduced motion preference */
    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
            animation-duration: 0.01ms !important;
            transition-duration: 0.01ms !important;
        }
    }
}
```

### 1.4 Implementation Checklist

- [ ] Add all mobile breakpoints to COMMON_STYLES
- [ ] Add CSS variables for mobile spacing and touch targets
- [ ] Add global touch-friendly styles
- [ ] Add `prefers-reduced-motion` support for accessibility
- [ ] Test viewport meta tag is correctly set

---

## Phase 2: Header

### 2.1 Current Issues
- Quick search completely hidden on mobile
- Logo and elements cramped
- Buttons too small for touch

### 2.2 Required Changes

**File:** `web/main.py` (lines 696-738)

```css
@media (max-width: 480px) {
    .app-header {
        padding: 0 8px !important;
        height: 56px !important;
    }

    .header-logo {
        font-size: 1rem !important;
    }

    /* Hide logo text, keep icon */
    .logo-text {
        display: none;
    }

    /* Larger header buttons for touch */
    .header-button {
        padding: 12px !important;
        min-width: 44px !important;
        min-height: 44px !important;
    }
}

@media (max-width: 640px) {
    /* Quick search - move to expandable overlay */
    .quick-search-container {
        position: fixed;
        top: 56px;
        left: 0;
        right: 0;
        padding: 8px 16px;
        background: var(--bg-header);
        display: none;
        z-index: 100;
        box-shadow: var(--shadow-md);
    }

    .quick-search-container.open {
        display: block;
    }

    .quick-search-toggle {
        display: flex !important;
    }
}
```

### 2.3 UX Recommendations

> **Expert Tip**: Position important actions in the "thumb zone" - the lower portion of the screen where users can easily reach with one hand. Consider moving the search icon to an easily accessible location.

### 2.4 Implementation Checklist

- [ ] Add search icon button that opens search field
- [ ] Reduce logo size, hide text on small screens
- [ ] Increase all header buttons to 44px minimum
- [ ] Verify spacing between elements
- [ ] Add visual feedback (ripple/highlight) on button tap

---

## Phase 3: Sidebar Navigation

### 3.1 Current Issues
- 280px width - takes almost full screen on small phones
- No dark overlay behind menu
- No swipe gesture to close

### 3.2 Required Changes

**File:** `web/main.py` (lines 740-809)

```css
@media (max-width: 480px) {
    .q-drawer {
        width: 85vw !important;
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

    .drawer-footer {
        padding: 12px !important;
    }

    .theme-switcher button {
        width: 40px !important;
        height: 40px !important;
    }
}

/* Overlay to close menu on tap */
.drawer-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.5);
    z-index: 999;
    display: none;
    backdrop-filter: blur(2px);
}

.drawer-overlay.visible {
    display: block;
}
```

### 3.3 UX Recommendations

> **Expert Tip**: According to the Baymard Institute, 84% of mobile apps struggle with touch interfaces. Ensure adequate spacing between nav items (at least 8px) to prevent accidental taps on adjacent items.

### 3.4 Implementation Checklist

- [ ] Change sidebar width to 85vw (max 280px)
- [ ] Add dark overlay that closes menu on tap
- [ ] Increase nav items to 48px minimum height
- [ ] Add swipe-to-close support (if NiceGUI supports)
- [ ] Verify footer stays at bottom
- [ ] Add focus states for keyboard navigation

---

## Phase 4: Home Page

### 4.1 Current Issues
- Statistics cards in horizontal layout
- Tool cards with fixed min-width
- Font sizes too large

### 4.2 Required Changes

**File:** `web/pages/home.py`

```css
@media (max-width: 480px) {
    /* Statistics cards */
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

    /* Tool cards */
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

    /* Page header */
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

### 4.3 Implementation Checklist

- [ ] Change grid to single column on small mobile
- [ ] Remove min-width from cards
- [ ] Reduce font sizes
- [ ] Reduce padding
- [ ] Verify no horizontal scroll

---

## Phase 5: Search Page

### 5.1 Current Issues
- Splitter not suitable for mobile
- Filters in 3-column grid
- Small search buttons
- Results and details side by side

### 5.2 Required Changes

**File:** `web/pages/search.py`

```css
@media (max-width: 768px) {
    /* Disable splitter - vertical layout */
    .search-splitter {
        flex-direction: column !important;
    }

    .search-results-panel,
    .search-details-panel {
        width: 100% !important;
        max-height: none !important;
    }

    .search-results-panel {
        height: 40vh !important;
        min-height: 200px !important;
    }

    .search-details-panel {
        flex: 1 !important;
    }
}

@media (max-width: 480px) {
    /* Filters in single column */
    .search-filters-grid {
        grid-template-columns: 1fr !important;
        gap: 8px !important;
    }

    .search-input {
        font-size: 16px !important;
        padding: 12px !important;
    }

    .search-buttons {
        flex-direction: column !important;
        gap: 8px !important;
    }

    .search-button {
        width: 100% !important;
        min-height: 48px !important;
    }

    .result-card {
        padding: 12px !important;
    }

    .result-title {
        font-size: 1rem !important;
    }

    .result-meta {
        font-size: 0.75rem !important;
    }

    .advanced-options {
        padding: 12px !important;
    }
}
```

### 5.3 UX Recommendations

> **Expert Tip**: Consider using tabs to toggle between "Results" and "Details" views instead of showing both simultaneously. This pattern is more familiar to mobile users and provides more screen real estate for each view.

### 5.4 Implementation Checklist

- [ ] Replace Splitter with vertical layout on mobile
- [ ] Change filters to single column
- [ ] Increase input fields to 16px minimum
- [ ] Make buttons full-width
- [ ] Add "Back to Results" button in details view
- [ ] Consider tabs for results/details toggle

---

## Phase 6: Document Viewer (Browse)

### 6.1 Current Issues
- Image and text panels side by side
- Image height 70vh - too much on mobile
- Small zoom controls
- Transcription panel not optimized

### 6.2 Required Changes

**File:** `web/pages/browse.py`

```css
@media (max-width: 768px) {
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

    /* Floating zoom controls */
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
        background: var(--bg-card) !important;
    }

    .nav-button {
        width: 44px !important;
        height: 44px !important;
    }

    .metadata-section {
        padding: 12px !important;
    }

    .metadata-label {
        font-size: 0.75rem !important;
    }

    .metadata-value {
        font-size: 0.875rem !important;
    }

    .viewer-tabs .q-tab {
        padding: 8px 12px !important;
        min-height: 44px !important;
    }
}
```

### 6.3 UX Recommendations

> **Expert Tip**: For image-heavy views, consider a "focus mode" toggle that maximizes either the image or transcription. This is especially valuable for detailed manuscript examination on small screens.

### 6.4 Implementation Checklist

- [ ] Stack panels vertically on tablets
- [ ] Reduce image height to 35vh on mobile
- [ ] Move zoom controls to floating position (bottom-right)
- [ ] Increase navigation buttons
- [ ] Add swipe gestures for page navigation
- [ ] Verify pinch-to-zoom works on images
- [ ] Consider "focus mode" for image-only or text-only view

---

## Phase 7: Parallels Page

### 7.1 Current Issues
- Results table too wide
- Many columns
- Horizontal scrolling problematic

### 7.2 Required Changes

**File:** `web/pages/parallels.py`

```css
@media (max-width: 768px) {
    /* Convert table to cards */
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

    .parallel-input {
        min-height: 120px !important;
        font-size: 16px !important;
    }
}
```

### 7.3 Implementation Checklist

- [ ] Create card view as alternative to table on mobile
- [ ] Hide non-essential columns
- [ ] Add "Show more" button for additional details
- [ ] Increase text input field
- [ ] Verify Hebrew text renders correctly in RTL

---

## Phase 8: Discoveries Page

### 8.1 Current Issues
- Discovery cards in grid
- Small images
- Too much information per card

### 8.2 Required Changes

**File:** `web/pages/discoveries.py`

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
        display: -webkit-box;
        -webkit-line-clamp: 3;
        -webkit-box-orient: vertical;
        overflow: hidden;
    }

    .discovery-meta {
        font-size: 0.75rem !important;
        margin-top: 8px !important;
    }

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

### 8.3 Implementation Checklist

- [ ] Change grid to single column on mobile
- [ ] Expand images to full width
- [ ] Limit description to 3 lines with ellipsis
- [ ] Stack filters vertically
- [ ] Add infinite scroll or pagination

---

## Phase 9: Lists Page

### 9.1 Current Issues
- Lists and cards in horizontal layout
- Small list actions
- Drag & drop doesn't work with touch

### 9.2 Required Changes

**File:** `web/pages/lists.py`

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

    /* Fixed bottom action bar */
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

    .add-to-list-dialog {
        width: 95vw !important;
        max-width: none !important;
    }
}
```

### 9.3 Implementation Checklist

- [ ] Stack sidebar and content vertically
- [ ] Move list actions to fixed bottom bar
- [ ] Replace drag & drop with "Move up/down" buttons
- [ ] Increase touch targets
- [ ] Test dialogs on mobile

---

## Phase 10: Settings Page

### 10.1 Current Issues
- Fixed 2-column grid
- Narrow forms

### 10.2 Required Changes

**File:** `web/pages/settings.py`

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
        font-size: 16px !important;
    }

    .settings-button {
        width: 100% !important;
        min-height: 48px !important;
    }

    .settings-toggle {
        min-height: 44px !important;
    }
}
```

### 10.3 Implementation Checklist

- [ ] Change grid to single column
- [ ] Increase input fields
- [ ] Make buttons full-width
- [ ] Ensure all toggles are large enough

---

## Phase 11: Help Page

### 11.1 Current Issues
- Content with sidebar
- Small links
- Long text

### 11.2 Required Changes

**File:** `web/pages/help.py`

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
        -webkit-overflow-scrolling: touch;
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

### 11.3 Implementation Checklist

- [ ] Convert sidebar to horizontal sticky nav
- [ ] Add horizontal scroll to navigation
- [ ] Increase font sizes for readability
- [ ] Test code blocks and tables

---

## Phase 12: Dialogs & Modals

### 12.1 Global Changes

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

    /* Or bottom sheet style */
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

### 12.2 UX Recommendations

> **Expert Tip**: For mobile, prefer the "bottom sheet" pattern over centered modals. Users find them more natural to dismiss (swipe down) and they don't obstruct as much content.

### 12.3 Implementation Checklist

- [ ] Make dialogs full-screen on mobile
- [ ] Or use bottom sheet pattern
- [ ] Add fixed header with close button
- [ ] Add fixed actions at bottom
- [ ] Test each dialog individually

---

## Phase 13: Loading & Error States

### 13.1 Required Changes

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

### 13.2 Implementation Checklist

- [ ] Adapt skeleton loaders for mobile
- [ ] Center error messages
- [ ] Make action buttons full-width
- [ ] Consider adding pull-to-refresh

---

## Phase 14: Performance Optimization

### 14.1 Required Optimizations

1. **Images:**
   - [ ] Use `loading="lazy"` for images
   - [ ] Define `srcset` for different sizes
   - [ ] Compress images for mobile

2. **Fonts:**
   - [ ] Limit font weights (`font-display: swap`)
   - [ ] Load only required Hebrew characters (subsetting)

3. **CSS:**
   - [ ] Extract critical CSS
   - [ ] Load non-critical CSS asynchronously

4. **JavaScript:**
   - [ ] Reduce bundle size
   - [ ] Use code splitting

5. **Network:**
   - [ ] Enable gzip/brotli compression
   - [ ] Use HTTP/2
   - [ ] Set caching headers

### 14.2 Performance Targets

| Metric | Target |
|--------|--------|
| First Contentful Paint | < 2s |
| Time to Interactive | < 5s |
| Largest Contentful Paint | < 2.5s |
| Cumulative Layout Shift | < 0.1 |
| Lighthouse Mobile Score | 90+ |

### 14.3 Performance Testing Checklist

- [ ] Test with Lighthouse (target: 90+ Mobile)
- [ ] Test with WebPageTest
- [ ] Test with throttling (slow 3G)
- [ ] Measure FCP < 2s
- [ ] Measure TTI < 5s
- [ ] Test on actual devices

---

## Phase 15: Accessibility Compliance

### 15.1 WCAG 2.1 Requirements

> **Expert Note**: In 2025, accessibility is a core design pillar, not an optional feature. Both iOS and Android guidelines emphasize designing for users with disabilities.

| Requirement | Standard | Implementation |
|-------------|----------|----------------|
| Color Contrast | 4.5:1 (normal), 3:1 (large) | Verify all text/background combinations |
| Touch Targets | 44×44px minimum | Apply to all interactive elements |
| Focus States | Visible focus indicators | Add outline styles for keyboard navigation |
| Screen Readers | ARIA labels | Add aria-label to icons and images |
| Reduced Motion | Respect preference | Add `prefers-reduced-motion` support |

### 15.2 Accessibility Checklist

- [ ] Verify color contrast ratios (WCAG AA minimum)
- [ ] Add visible focus states to all interactive elements
- [ ] Add ARIA labels to icon-only buttons
- [ ] Add alt text to all images
- [ ] Test with VoiceOver (iOS) and TalkBack (Android)
- [ ] Respect `prefers-reduced-motion` setting
- [ ] Ensure logical tab order
- [ ] Test keyboard navigation

---

## Phase 16: Testing & QA

### 16.1 Device Testing Matrix

| Device | Width | Height | Pixel Ratio | Priority |
|--------|-------|--------|-------------|----------|
| iPhone SE | 375px | 667px | 2 | High |
| iPhone 12/13 | 390px | 844px | 3 | High |
| iPhone 14 Pro Max | 430px | 932px | 3 | Medium |
| Samsung Galaxy S21 | 360px | 800px | 3 | High |
| iPad Mini | 768px | 1024px | 2 | Medium |
| iPad Pro 11" | 834px | 1194px | 2 | Low |

### 16.2 Functional Testing Checklist

**Core Features:**
- [ ] Main navigation works
- [ ] Search works
- [ ] Document viewing works
- [ ] Lists work
- [ ] Settings work
- [ ] Language switching works
- [ ] Theme switching works
- [ ] Login/logout works

**UI/UX:**
- [ ] No horizontal scroll
- [ ] All text is readable
- [ ] All buttons are tappable
- [ ] Dialogs open and close
- [ ] Images load
- [ ] RTL is correct
- [ ] Fonts load

**Performance:**
- [ ] Load time < 3 seconds on 4G
- [ ] Smooth interactions (60fps)
- [ ] No layout jumps during loading

---

## Implementation Priority

### High Priority (Week 1):
1. Global CSS infrastructure and breakpoints
2. Header adaptation
3. Sidebar adaptation
4. Home page adaptation

### Medium Priority (Week 2):
5. Search page adaptation
6. Document viewer adaptation
7. Dialogs adaptation

### Normal Priority (Week 3):
8. Parallels page adaptation
9. Discoveries page adaptation
10. Lists page adaptation
11. Settings page adaptation
12. Help page adaptation

### Lower Priority (Week 4):
13. Loading and error states
14. Performance optimizations
15. Accessibility compliance
16. Comprehensive QA testing

---

## Files to Edit

| File | Description | Priority |
|------|-------------|----------|
| `web/main.py` | Infrastructure, Header, Sidebar | High |
| `web/pages/home.py` | Home page | High |
| `web/pages/search.py` | Search | High |
| `web/pages/browse.py` | Document viewer | High |
| `web/pages/parallels.py` | Parallels | Medium |
| `web/pages/discoveries.py` | Discoveries | Medium |
| `web/pages/lists.py` | Lists | Medium |
| `web/pages/settings.py` | Settings | Low |
| `web/pages/help.py` | Help | Low |
| `web/components/*.py` | Components | Medium |

---

## Summary

This document outlines 16 phases to make the website mobile-responsive. Implementation requires:

1. **CSS Changes**: Adding media queries, adapting sizes
2. **Structural Changes**: Vertical layouts instead of horizontal
3. **UX Changes**: Larger touch areas, adapted navigation
4. **Accessibility**: WCAG compliance, screen reader support
5. **Testing**: Across various devices and scenarios

**Recommendation**: Start with global infrastructure and proceed according to priority order.

---

## References & Resources

- [Best Practices for Mobile UI/UX Design in 2025](https://medium.com/@CarlosSmith24/best-practices-for-mobile-ui-ux-design-in-2025-b5e35310c4e9)
- [Responsive Design: Best Practices & Examples | UXPin](https://www.uxpin.com/studio/blog/best-practices-examples-of-excellent-responsive-design/)
- [Mobile UX Design Guide: Best Practices for 2025](https://www.webstacks.com/blog/mobile-ux-design)
- [Mobile App UI Design: Best Practices and Trends for 2025](https://www.thedroidsonroids.com/blog/mobile-app-ui-design-guide)
- [App Designer's Guide to Responsive Mobile UX - LogRocket](https://blog.logrocket.com/ux-design/app-designers-guide-responsive-mobile-ux/)
- [Top 10 UI/UX Design Principles for 2025](https://www.eymockup.com/ui-ux-design-principles-2025/)
