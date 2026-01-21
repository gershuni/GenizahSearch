# Accessibility Compliance Report - Genizah Search Pro

**Date:** February 2025
**Target Standard:** Israeli Standard 5568 / WCAG 2.0 Level AA

## Executive Summary
Genizah Search Pro has undergone a targeted accessibility remediation to comply with Israeli regulations. The application now supports keyboard navigation, screen reader access via semantic structure and ARIA labels, and visual accessibility requirements.

## 1. Compliance Status
**Status:** Partially Conformant (Level AA)

The web interface largely meets WCAG 2.0 AA requirements. Exceptions related to the inherent nature of the content (historical manuscript images) are noted below.

## 2. Key Remediations Implemented

### 2.1 Keyboard Accessibility (P0-A, P0-C, P0-D)
- **Refactored Clickable Elements:** Non-semantic clickable cards (Sidebar, Home Page tools, Search Results) now use `role="button"`, `tabindex="0"`, and include `Enter`/`Space` keyboard handlers.
- **Focus Visibility:** A global, high-contrast focus indicator (`outline: 2px solid var(--primary-600)`) has been applied to all interactive elements.
- **Skip Link:** A "Skip to main content" link is the first focusable element on every page.
- **Viewer Controls:** Manuscript viewer now supports keyboard zooming (`+`/`-`) and basic navigation arrows.

### 2.2 Screen Reader Support (P0-B, P0-E, P1-A)
- **Accessible Names:** All icon-only buttons (Search, Edit, Delete, Zoom controls) now have explicit `aria-label` attributes using localized strings.
- **Semantic Headings:** The application uses a strict `h1`-`h6` hierarchy implemented via custom semantic components.
- **Landmarks:** The page structure includes proper header, nav, and main regions.
- **Language Attributes:** The `<html>` tag dynamically updates `lang` (he/en) and `dir` (rtl/ltr) attributes based on the active language.

### 2.3 Forms & Input (P0-E)
- **Labels:** Search inputs and settings controls use visible labels or `aria-label` where visual design precludes a label.

## 3. Known Limitations & Exclusions

### 3.1 Manuscript Images
**Issue:** The core content consists of digital images of historical manuscripts.
**Reason:** By definition, image-only content of this nature cannot be fully accessible to blind users without text alternatives.
**Mitigation:** We provide machine-generated (OCR) transcriptions for a significant portion of the corpus, displayed in a readable, resizable format next to the images.

### 3.2 Complex Viewer Interactions
**Issue:** Advanced image manipulation (rotation, fine panning) is primarily mouse/touch-driven.
**Mitigation:** Basic zoom and page navigation are keyboard accessible.

## 4. Contact Information
For accessibility inquiries or assistance, please contact:
**Email:** `gershuni [at] gmail [dot] com`
