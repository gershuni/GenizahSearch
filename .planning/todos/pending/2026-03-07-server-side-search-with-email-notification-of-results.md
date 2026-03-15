---
created: 2026-03-07T21:30:04.145Z
title: Server-side search with email notification of results
area: search, infrastructure
files:
  - genizah_core.py
  - web/pages/search.py
---

## Problem

Long-running searches (especially composition/parallel searches) can take minutes to hours. Users must keep the browser tab open and wait. There is no way to submit a search and come back later to see the results.

The user wants the ability to run searches on the server in the background and receive results by email when the search completes. This would allow users to:
- Submit complex searches and close their browser
- Queue multiple searches
- Get notified when results are ready without keeping the app open

## Solution

Milestone-level feature (v7.0+), not a single phase. Requires multiple new subsystems:

1. **Background search worker** — decouple search from web session (Celery/RQ/asyncio task queue)
2. **Results persistence** — store completed results in Supabase or sidecar DB with retrieval endpoint
3. **Email notification** — transactional email service (Resend/SendGrid), results summary + link template
4. **Async search UI** — submit form, "my searches" status/results page, cancel option
5. **Rate limiting & auth guards** — per-user quotas to prevent resource abuse

### Notes (2026-03-15)
- v6.2.0 already mitigated the worst pain: ETA display, cancel with partial results, desktop notifications, sleep prevention
- No existing background task system or email infrastructure — this is greenfield
- Composition searches are the primary use case (minutes to hours)
- Related to power user feedback items ב (ETA) and ג (partial results on cancel), both already shipped
- Reuse existing SearchEngine + composition logic from genizah_core.py
- Desktop app could also benefit from a "search on server" mode
