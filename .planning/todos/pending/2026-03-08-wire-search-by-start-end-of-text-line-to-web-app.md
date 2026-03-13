---
created: 2026-03-08T14:35:52.364Z
title: Wire search-by-start/end-of-text/line to web app
area: web, search
files:
  - genizah_app.py
  - web/pages/search.py
  - genizah_core.py
---

## Problem

The desktop app already has a search-by-start/end-of-text/line feature (anchoring search to beginning or end of a document or line). This feature is not yet available in the web app. Users on the web should have parity with this desktop search capability.

## Solution

Wire the existing desktop start/end anchoring logic into the web search UI. The core search engine support likely already exists in genizah_core.py — need to expose the UI controls in web/pages/search.py and pass the options through to the search pipeline.
