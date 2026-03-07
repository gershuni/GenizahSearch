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

TBD — high-level considerations:
- Server-side search worker (background task/queue) that runs searches independently of the web session
- Email integration (SMTP or transactional email service like SendGrid/Resend) to notify users
- Results storage: save search results to DB or file, provide a link to view them
- Authentication: tie searches to user accounts (Supabase auth already exists)
- Rate limiting: prevent abuse of server resources
- Could reuse existing SearchEngine + composition search logic from genizah_core.py
- Desktop app could also benefit from a "search on server" mode for very large searches
