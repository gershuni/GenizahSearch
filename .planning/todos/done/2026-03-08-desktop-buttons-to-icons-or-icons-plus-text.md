---
created: 2026-03-08T14:35:52.364Z
title: Desktop buttons to icons or icons+text
area: desktop, ui
files:
  - genizah_app.py
---

## Problem

Some buttons in the desktop PyQt6 app are text-only and take up too much horizontal space or look cluttered. Converting them to icons or icons+text would improve the UI density and visual clarity.

## Decision (2026-03-15): Option B — Icon+text compact

All action buttons get icons paired with short labels. Buttons shrink ~40%. Still readable without tooltips.

### Priority 1: ResultDialog (most cluttered)

**Action row** (9 buttons → icon+short text):
| Current | Icon | Short label |
|---------|------|-------------|
| עיון בכתב יד (Browse manuscript) | 📖 | עיון / Browse |
| חפש מקבילות (Search for parallels) | 🔍 | מקבילות / Parallels |
| הוסף לרשימה (Add/Remove from List) | ⭐ | רשימה / List |
| הצג מידע מורחב (Show Extended Info) | ℹ️ | מידע / Info |
| ביבליוגרפיה פרידברג (FJMS Bib) | 📚 | Bib(N) |
| ביבליוגרפיה כתיב (NLI Bib) | 📚 | Bib(N) |
| מידע קטלוגי (Catalog Records) | 📋 | Cat(N) |
| 🖼️ (Toggle Image) | 🖼️ | (icon-only, already compact) |
| תרגומים כבויים/פעילים (Translations) | 🌐 | Trans ON/OFF |

**Community row** (already partly icon'd, just standardize):
| Current | Icon | Short label |
|---------|------|-------------|
| ✏️ עריכה (Edit) | ✏️ | Edit (keep as-is) |
| 💬 הערה (Comment) | 💬 | Comment (keep) |
| צפה בתיקונים (View Corrections) | 📝 | Corr (shorten) |
| 🔗 (Joins) | 🔗 | (keep as-is) |

**Image toolbar** (already mostly symbols, standardize):
| Current | Icon | Short label |
|---------|------|-------------|
| + / - | 🔍+/🔍- or keep +/- | (keep compact) |
| אפס (Reset) | ↩️ | Reset |
| ↺ / ↻ | keep | (already icons) |
| עיון בכתיב (View on Ktiv) | 🔗 | Ktiv |

### Priority 2: Search tab controls
### Priority 3: Browse tab controls
### Priority 4: Composition tab controls
### Priority 5: Lists tab actions

## Implementation Notes

- Use emoji for icons (no external icon library dependency)
- All buttons keep tooltips with full Hebrew/English text
- Badge counts (N) stay visible on Bib/Cat buttons
- Checkable buttons (Info, Trans, Image) use color change on checked state
- Compact mode bar follows same icon scheme
