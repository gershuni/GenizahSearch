---
created: 2026-02-11T16:15:29.361Z
title: Migrate desktop corrections fetch to shared corrections_service
area: desktop
files:
  - genizah_app.py:6229-6297
  - genizah_app.py:3070-3155
  - shared/corrections_service.py
  - supabase_corrections_client.py:815
---

## Problem

The desktop app and web app use different code paths for fetching pending corrections:

- **Web:** Uses `shared.corrections_service.get_pending_corrections_for_page(client, sys_id, page_number, user_id)` — server-side filtering by page + user, returns only draft/pending/under_review statuses.
- **Desktop:** Uses `SupabaseCorrectionsClient.get_corrections_for_document(doc_id, include_drafts=True)` — fetches ALL corrections for the entire document (all pages, all users, all statuses), then filters client-side by page number and permissions.

This inconsistency means:
1. Desktop makes a heavier query than necessary (all corrections vs. just pending for one page)
2. Two separate code paths to maintain for the same feature
3. Permission filtering logic is duplicated in genizah_app.py instead of being centralized

Discovered during Phase 24 planning when analyzing desktop version selector code.

## Solution

Refactor the Browse tab (~6229) and Reading Desk (~3070) correction-fetching sections to call `get_pending_corrections_for_page()` from the shared service for pending corrections. Keep `get_corrections_for_document` only for fetching approved corrections from other users (which the shared service doesn't cover). This aligns both apps on the same data path for pending corrections.
