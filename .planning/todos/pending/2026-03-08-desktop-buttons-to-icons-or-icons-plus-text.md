---
created: 2026-03-08T14:35:52.364Z
title: Desktop buttons to icons or icons+text
area: desktop, ui
files:
  - genizah_app.py
---

## Problem

Some buttons in the desktop PyQt6 app are text-only and take up too much horizontal space or look cluttered. Converting them to icons or icons+text would improve the UI density and visual clarity.

## Solution

Identify button-heavy areas in genizah_app.py (toolbar, search controls, result actions) and replace appropriate text-only buttons with icon buttons or icon+text buttons using PyQt6's QIcon/setIcon. Choose clear, standard icons that are self-explanatory.
