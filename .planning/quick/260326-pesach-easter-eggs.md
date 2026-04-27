# Pesach Easter Eggs — Handoff

**Date:** 2026-03-26
**Deadline:** Active from now until end of Pesach (April 20, 2026)
**Scope:** Web + Desktop

---

## Three Features

### Feature 1: "Ma Nishtana" Easter Egg (30 min)
- Intercept search for "מה נשתנה" and variants ("מה נשתנה הלילה", "ma nishtana")
- Also trigger on "הגדה", "סדר פסח", "אפיקומן"
- Show a special result card with a real Genizah Haggadah fragment shelfmark + Pesach greeting
- Minimal code — check in search handler
- Both apps

### Feature 2: Pesach Genizah Collection (2-3 hours)
- **3,237 manuscripts** with Pesach terms in libraries.csv titles (פסח, הגדה, Passover, Haggadah, Pesach piyyutim)
- 18 manuscripts in FJMS "Passover Haggadah" domain
- Create a special banner during Pesach period linking to a filtered browse view
- "פסח בגניזה / Pesach in the Genizah" — curated collection
- Categories: Haggadot, Piyyutim, Legal texts, Letters mentioning Pesach
- Could be a filtered browse query or dedicated mini-page
- Time-limited: show banner from now through April 20

### Feature 3: Four Sons of the Genizah (2-3 hours)
- Track session behavior via existing PostHog events or local counters:
  - Search count, filter usage, Responsa syntax usage
  - Correction reports
  - Browse-only sessions
  - Empty searches, typos
- Classify user as one of four sons:
  - **The Wise Scholar (חכם)** — uses Responsa syntax + filters + parallels
  - **The Wicked Skeptic (רשע)** — reports many corrections, challenges data
  - **The Simple Browser (תם)** — only browses, never searches
  - **The One Who Doesn't Know (שאינו יודע לשאול)** — empty searches, few actions
- Show classification with playful Hebrew toast after 10+ actions in session
- Time-limited: active through April 20

---

## Technical Notes

- All three should have a date check: `datetime.now() <= datetime(2026, 4, 20)`
- Pesach 2026 starts evening of April 12, ends evening of April 20
- Features should be fun but respectful — this is a scholarly tool used by researchers
- Hebrew quality matters — user is native Hebrew speaker, will review all text
- Both apps where possible (desktop may skip #2 banner)

## Data Available

- `libraries.csv` title column (index 7): 3,237 matches for פסח/הגדה/Passover/Haggadah
- FJMS `domains` table: "Passover Haggadah" domain (18 manuscripts)
- FJMS `domains` table: "Piyyut" domain (51,228 — subset are Pesach piyyutim, identifiable by "Pesah" in title)
- libraries.csv titles containing "Piyyut (Pesah)": good signal for Pesach-specific piyyutim

## Session Context

- v7.3.0 just released and deployed
- Web app running on EC2, desktop installer on GitHub releases
- All measurement/bib-dedup/translation work is shipped
- Phase 54 complete, Phase 55 (search within results) is next in roadmap but Pesach features are a fun detour
