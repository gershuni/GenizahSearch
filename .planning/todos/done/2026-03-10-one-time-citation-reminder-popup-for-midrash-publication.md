---
created: 2026-03-10T09:09:41.953Z
title: One-time citation reminder popup for MiDRASH publication
area: ui, web, desktop
files:
  - web/main.py
  - web/pages/search.py
  - genizah_app.py
---

## Problem

Users should be reminded to cite the MiDRASH publication when using GenizahSearch results in their research. Currently there is no citation prompt anywhere in the app.

## Solution

Add a one-time popup dialog (shown once per user/installation) on app start for both:
- **Web app**: NiceGUI dialog on first visit (track via localStorage or Supabase user preference)
- **Desktop app**: PyQt6 QMessageBox on first launch (track via QSettings)

Content should include the proper citation format for the MiDRASH publication and a "Don't show again" checkbox or similar dismissal mechanism.
