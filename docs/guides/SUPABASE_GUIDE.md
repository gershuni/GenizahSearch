# GenizahSearch Supabase Guide

> Guide for working with Supabase in the GenizahSearch project
> Last updated: 2026-05-14

---

## Overview

GenizahSearch uses [Supabase](https://supabase.com) as its backend for:
- **Authentication** - User registration, login, password reset
- **Database** - PostgreSQL for user data
- **Row Level Security** - Data isolation between users

---

## Setup

### 1. Get Credentials

Get `SUPABASE_URL` and `SUPABASE_ANON_KEY` from:
- Project admin, or
- Supabase Dashboard → Project → Settings → API

### 2. Configure Environment

Add to `.env`:
```bash
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

---

## Database Schema

### Data API Grants for New Tables

Supabase is changing the default Data API exposure model for tables in the `public` schema:

- 2026-05-30: new Supabase projects no longer auto-expose new `public` tables to the Data API by default.
- 2026-10-30: the behavior is enforced for existing projects.
- Existing tables keep their current grants, but newly created tables need explicit grants before `supabase-js`, PostgREST, or GraphQL can access them.

For every new `public` table that should be reachable through the Supabase client, include grants in the same migration as the table, RLS, and policies:

```sql
-- Grant access per role. Keep anon read-only unless the table is intentionally public-write.
GRANT SELECT
ON TABLE public.your_table
TO anon;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE public.your_table
TO authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE public.your_table
TO service_role;

-- SERIAL/BIGSERIAL inserts also need sequence privileges.
GRANT USAGE, SELECT
ON SEQUENCE public.your_table_id_seq
TO authenticated, service_role;

-- RLS and policies are still required. Grants only make the table visible to the role.
ALTER TABLE public.your_table ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read their own rows"
ON public.your_table
FOR SELECT TO authenticated
USING (auth.uid() = user_id);
```

For public-read system tables, grant only the operations the app needs. For private/user-owned tables, grant role-level access broadly enough for the Data API to reach the table, then constrain rows with RLS policies.

### Tables

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    profiles     │     │   user_lists    │     │   list_items    │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ id (PK, FK)     │──┐  │ id (PK)         │──┐  │ id (PK)         │
│ email           │  │  │ user_id (FK)    │◀─┘  │ list_id (FK)    │◀─┐
│ full_name       │  │  │ name            │     │ sys_id          │  │
│ role            │  └─▶│ color           │     │ shelfmark       │  │
│ reputation      │     │ project_id (FK) │     │ title           │  │
│ created_at      │     │ created_at      │     │ note            │  │
└─────────────────┘     └─────────────────┘     │ tags            │  │
                                                │ added_at        │  │
                                                └─────────────────┘  │
                                                                     │
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐  │
│   corrections   │     │    comments     │     │   discoveries   │  │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤  │
│ id (PK)         │     │ id (PK)         │     │ id (PK)         │  │
│ user_id (FK)    │     │ user_id (FK)    │     │ user_id (FK)    │  │
│ sys_id          │     │ sys_id          │     │ type            │  │
│ fl_id           │     │ fl_id           │     │ title           │  │
│ original_text   │     │ content         │     │ content         │  │
│ corrected_text  │     │ is_private      │     │ shelfmarks      │  │
│ status          │     │ created_at      │     │ votes           │  │
│ created_at      │     └─────────────────┘     │ is_pinned       │  │
└─────────────────┘                             └─────────────────┘  │
                                                                     │
┌─────────────────┐     ┌─────────────────┐                          │
│     joins       │     │    projects     │◀─────────────────────────┘
├─────────────────┤     ├─────────────────┤
│ id (PK)         │     │ id (PK)         │
│ user_id (FK)    │     │ user_id (FK)    │
│ shelfmarks      │     │ name            │
│ join_type       │     │ color           │
│ confidence      │     │ created_at      │
│ created_at      │     └─────────────────┘
└─────────────────┘
```

### profiles

User profiles (extends `auth.users`):

| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Primary key (= auth.users.id) |
| `email` | text | User's email |
| `full_name` | text | Display name |
| `role` | text | user / editor / reviewer / admin |
| `affiliation` | text | Institution/organization |
| `bio` | text | User biography |
| `reputation` | int | Contribution score |
| `avatar_url` | text | Profile picture URL |
| `created_at` | timestamp | Registration date |

### user_lists

Personal manuscript lists:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Primary key |
| `user_id` | uuid | Owner |
| `name` | text | List name (Hebrew) |
| `name_en` | text | List name (English) |
| `color` | text | Display color (#hex) |
| `project_id` | int | Parent project (optional) |
| `is_default` | bool | Default list flag |
| `is_system` | bool | System list (e.g., Recent) |

### list_items

Items in lists:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Primary key |
| `list_id` | int | Parent list |
| `sys_id` | text | Manuscript system ID |
| `shelfmark` | text | Manuscript shelfmark |
| `title` | text | Manuscript title |
| `fl_id` | text | Specific folio/page |
| `note` | text | User notes |
| `tags` | jsonb | Tags array |
| `added_at` | timestamp | When added |

### corrections

Transcription corrections:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Primary key |
| `author_id` | uuid | Submitter (**Note:** uses `author_id`, not `user_id`) |
| `sys_id` | text | Manuscript system ID |
| `fl_id` | text | Specific page |
| `original_text` | text | Original transcription |
| `corrected_text` | text | Corrected version |
| `status` | text | draft / pending / approved / rejected |
| `votes_up` | int | Upvote count |
| `votes_down` | int | Downvote count |
| `reviewed_by` | uuid | Reviewer (if reviewed) |
| `reviewed_at` | timestamp | Review date |
| `ie_id` | text | IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness |

### documents (PGP Data)

PGP (Princeton Geniza Project) document metadata and transcriptions:

| Column | Type | Description |
|--------|------|-------------|
| `pgpid` | int | Primary key - PGP document ID |
| `shelfmark_combined` | text | Raw combined shelfmark from PGP (e.g., "T-S 13J35.3 + AIU VII.A.23") |
| `document_type` | text | Document type (Letter, Legal document, List, etc.) |
| `tags` | jsonb | Subject tags array (e.g., ["communal", "marriage"]) |
| `doc_date_original` | text | Original date notation from source |
| `doc_date_standard` | text | Standardized date range |
| `inferred_date_display` | text | Human-readable inferred date |
| `description` | text | English scholarly description |
| `transcription` | text | Full transcription content |
| `transcription_source` | text | Attribution (e.g., "Amir Ashur, PGP") |
| `pgp_url` | text | Generated URL to PGP website (computed) |
| `created_at` | timestamp | Import timestamp |

**Note:** This table stores system data imported from PGP, NOT user-generated content. There is no `user_id` column. RLS allows public read access; writes happen via service role during data import.

### document_fragments (PGP Linkages)

Links PGP documents to GenizahSearch fragments:

| Column | Type | Description |
|--------|------|-------------|
| `id` | serial | Primary key |
| `document_id` | int | FK to documents.pgpid |
| `sys_id` | text | GenizahSearch system_number |
| `shelfmark` | text | Denormalized shelfmark for display |
| `page_info` | text | Page/folio info (recto, verso, recto and verso) |
| `sequence_order` | int | Order within multi-fragment document |
| `created_at` | timestamp | Import timestamp |

**Note:** Single-fragment manuscripts do NOT have entries in these tables (DOC-02 requirement). Only multi-fragment documents that are part of PGP joins are stored here.

---

## Using the Python Client

### Import

```python
from web.supabase_client import (
    get_client,
    sign_in, sign_up, sign_out,
    get_user_lists, create_list, add_list_item,
    get_corrections, submit_correction
)
```

### Authentication

```python
# Sign up
result = sign_up('user@example.com', 'password123', {
    'full_name': 'John Doe',
    'affiliation': 'University'
})

# Sign in
result = sign_in('user@example.com', 'password123')
if result.get('success'):
    user = result['user']
    session = result['session']

# Sign out
sign_out()

# Get current user
user = get_current_user()
```

### Lists

```python
# Get all lists
lists = get_user_lists(user_id)

# Create a list
result = create_list(
    user_id=user_id,
    name='רשימה חדשה',
    name_en='New List',
    color='#FF5733'
)

# Add item to list
result = add_list_item(
    list_id=123,
    sys_id='MS-12345',
    shelfmark='T-S 12.123',
    title='Fragment Title',
    note='My notes'
)

# Get items in list
items = get_list_items(list_id=123)

# Delete list
delete_list(list_id=123)
```

### Corrections

```python
# Submit correction
result = submit_correction(
    user_id=user_id,
    sys_id='MS-12345',
    fl_id='T-S 12.123.1r',
    original_text='original text here',
    corrected_text='corrected text here'
)

# Get user's corrections
corrections = get_user_corrections(user_id)

# Get pending corrections (for reviewers)
pending = get_pending_corrections()
```

### Direct Queries

For operations not covered by helper functions:

```python
client = get_client()

# Select
result = client.table('profiles').select('*').eq('role', 'admin').execute()

# Insert (note: comments uses author_id, not user_id)
result = client.table('comments').insert({
    'author_id': user_id,  # NOT user_id!
    'sys_id': 'MS-12345',
    'content': 'My comment'
}).execute()

# Update
result = client.table('profiles').update({
    'full_name': 'New Name'
}).eq('id', user_id).execute()

# Delete
result = client.table('list_items').delete().eq('id', item_id).execute()
```

### ⚠️ Avoid Profile Joins

**Do NOT use profile joins** in queries unless the FK relationship is configured:

```python
# WRONG - will fail without FK relationship:
result = client.table('corrections').select('*, profiles(full_name)').execute()

# CORRECT - simple select:
result = client.table('corrections').select('*').execute()
```

If you need user info, fetch profiles separately after getting the author_id/user_id.

---

## Row Level Security (RLS)

RLS policies ensure users can only access their own data.

### Important: Policy Configuration

**All INSERT/UPDATE/DELETE policies must use the `authenticated` role**, not `public`. Using `public` role causes `auth.uid()` to return null for anonymous users, breaking the policy.

To fix RLS policies in bulk, run: `scripts/fix_rls_policies.sql`

### Column Naming (Important!)

The database has inconsistent column naming for user references:

| Table | User Column |
|-------|-------------|
| `comments` | `author_id` |
| `corrections` | `author_id` |
| `discoveries` | `user_id` |
| `fragment_joins` | `user_id` |
| `user_lists` | `user_id` |
| `list_items` | (via list_id → user_lists.user_id) |

**Always check the actual column name** when writing queries or RLS policies.

### Policy Examples

```sql
-- Correct: Uses authenticated role
CREATE POLICY "Users can create comments" ON comments
FOR INSERT TO authenticated
WITH CHECK (auth.uid() = author_id);

-- Wrong: Uses public role (will fail)
CREATE POLICY "Users can create comments" ON comments
FOR INSERT TO public
WITH CHECK (auth.uid() = author_id);
```

### profiles
- Users can read all profiles (public info)
- Users can only update their own profile

### user_lists, list_items
- Users can only CRUD their own lists and items

### corrections
- Anyone can read approved corrections
- Users can CRUD their own corrections
- Reviewers/admins can update any correction status

### comments
- Anyone can read public comments
- Users can only see their own private comments
- Users can CRUD their own comments
- `ie_id` (text, nullable): IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness

### Detailed Policy SQL Examples

```sql
-- ============================================
-- profiles table
-- ============================================
-- Anyone can read profiles (public info)
CREATE POLICY "Public profiles are viewable by everyone"
ON profiles FOR SELECT
TO public
USING (true);

-- Users can update only their own profile
CREATE POLICY "Users can update own profile"
ON profiles FOR UPDATE
TO authenticated
USING (auth.uid() = id)
WITH CHECK (auth.uid() = id);

-- ============================================
-- user_lists table
-- ============================================
-- Users can only see their own lists
CREATE POLICY "Users can view own lists"
ON user_lists FOR SELECT
TO authenticated
USING (auth.uid() = user_id);

-- Users can create lists for themselves
CREATE POLICY "Users can create own lists"
ON user_lists FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = user_id);

-- Users can update their own lists
CREATE POLICY "Users can update own lists"
ON user_lists FOR UPDATE
TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- Users can delete their own lists
CREATE POLICY "Users can delete own lists"
ON user_lists FOR DELETE
TO authenticated
USING (auth.uid() = user_id);

-- ============================================
-- corrections table
-- ============================================
-- Anyone can read approved corrections
CREATE POLICY "Approved corrections are public"
ON corrections FOR SELECT
TO public
USING (status = 'approved');

-- Users can read their own corrections (any status)
CREATE POLICY "Users can view own corrections"
ON corrections FOR SELECT
TO authenticated
USING (auth.uid() = author_id);

-- Users can create corrections
CREATE POLICY "Users can create corrections"
ON corrections FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = author_id);

-- ============================================
-- comments table
-- ============================================
-- Public comments are readable by all
CREATE POLICY "Public comments are viewable"
ON comments FOR SELECT
TO public
USING (is_public = true);

-- Users can see their own private comments
CREATE POLICY "Users can view own comments"
ON comments FOR SELECT
TO authenticated
USING (auth.uid() = author_id);

-- Users can create comments
CREATE POLICY "Users can create comments"
ON comments FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = author_id);

-- ============================================
-- discoveries table
-- ============================================
-- Non-hidden discoveries are public
CREATE POLICY "Visible discoveries are public"
ON discoveries FOR SELECT
TO public
USING (is_hidden = false);

-- Users can create discoveries
CREATE POLICY "Users can create discoveries"
ON discoveries FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = user_id);

-- Users can update their own discoveries
CREATE POLICY "Users can update own discoveries"
ON discoveries FOR UPDATE
TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- fragment_joins table
-- ============================================
-- All joins are publicly readable
CREATE POLICY "Joins are public"
ON fragment_joins FOR SELECT
TO public
USING (true);

-- Users can create joins
CREATE POLICY "Users can create joins"
ON fragment_joins FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = user_id);

-- Users can delete their own joins
CREATE POLICY "Users can delete own joins"
ON fragment_joins FOR DELETE
TO authenticated
USING (auth.uid() = user_id);
```

### BANNED_TABLES extension protocol (Phase 92.1)

When adding a new user-scoped table to the schema (one whose RLS SELECT policy is `TO authenticated USING (auth.uid() = user_id)` or any user-private filter), also add the table name to `BANNED_TABLES` in `tests/test_no_anonymous_reads_on_authenticated_tables.py`. This ensures the AST scanner CI guard catches any future `get_client().table('<new_table>')` regression that would silently return 0 rows for logged-in users (the post-Phase-90 anon singleton invariant means anonymous-role reads never see RLS-`TO authenticated` rows).

The scanner's known blind spots — module-level `table = get_client().table` aliases and wrapper functions like `def _q(name): return get_client().table(name)` — are documented in the scanner's seed-trap section with `xfail-strict` placeholders; do not rely on the scanner alone for those patterns. Code review remains the second line of defense.

Partial-auth tables (`corrections`, `comments`) — those with both a `TO public USING (<filter>)` AND a `TO authenticated USING (auth.uid()=<owner>)` SELECT branch — are deliberately NOT in `BANNED_TABLES` because their `get_client()` reads remain valid for the public branch. The per-function migration in `web/supabase_client.py` switches to `get_user_client()` for those readers so the authenticated branch becomes reachable. If a future schema change extends such a table to a user-private-only filter (no public branch), add it to `BANNED_TABLES`.

Note: `discoveries` is NOT in this partial-auth list. Its only SELECT policy is `TO public USING (is_hidden=false)` (see `scripts/fix_rls_policies.sql:60-95`) — there is no `TO authenticated` branch, so the anon client and authenticated client return identical rows and `get_client()` reads are correct. Do not add `discoveries` to `BANNED_TABLES`.

---

## OAuth Authentication

### OAuth Callback Flow

GenizahSearch uses Supabase OAuth for Google login. The OAuth flow works as follows:

1. **User clicks "Login with Google"**
   - Frontend calls `get_oauth_url('google', redirect_url)`
   - Supabase generates OAuth URL with state parameter

2. **User authenticates with Google**
   - Redirected to Google login
   - After consent, Google redirects back to Supabase

3. **Supabase redirects to callback**
   - URL: `https://genizahsearch.com/auth/callback`
   - Tokens in URL hash: `#access_token=...&refresh_token=...` (implicit flow)

4. **Frontend extracts tokens**
   - JavaScript reads URL hash fragment
   - Tokens are passed to `set_session_from_url(access_token, refresh_token)`

### Token Extraction (Implicit Flow)

```javascript
// The callback page extracts tokens from URL hash
const hash = window.location.hash.substring(1);
const params = new URLSearchParams(hash);
const access_token = params.get('access_token');
const refresh_token = params.get('refresh_token');
```

### Setting Up Session

```python
from web.supabase_client import set_session_from_url

# After extracting tokens from URL
result = set_session_from_url(access_token, refresh_token)
if result.get('success'):
    user = result['user']
    session = result['session']
```

### Environment Variables for OAuth

```bash
# Required in .env for OAuth redirect
SITE_URL=https://genizahsearch.com

# Supabase Dashboard → Authentication → URL Configuration
# Site URL: https://genizahsearch.com
# Redirect URLs: https://genizahsearch.com/auth/callback
```

### Desktop App Users & OAuth

Users who sign up via Google OAuth don't have a password set. To use the desktop app:
1. Go to Profile → Set Password, or
2. Use "Forgot Password" to receive a password reset email

---

## Supabase Dashboard

### Accessing the Dashboard

1. Go to https://supabase.com/dashboard
2. Select the GenizahSearch project

### Common Tasks

**View Data:**
- Table Editor → Select table → Browse rows

**Run SQL:**
- SQL Editor → New query → Execute

**View Users:**
- Authentication → Users

**Check Logs:**
- Logs → API logs / Auth logs

**Database Backups:**
- Settings → Database → Backups

---

## Common Queries

### Get user with their lists and items

```sql
SELECT
    p.id,
    p.email,
    p.full_name,
    l.name as list_name,
    COUNT(i.id) as item_count
FROM profiles p
LEFT JOIN user_lists l ON l.user_id = p.id
LEFT JOIN list_items i ON i.list_id = l.id
GROUP BY p.id, l.id
ORDER BY p.email;
```

### Get correction statistics

```sql
SELECT
    status,
    COUNT(*) as count
FROM corrections
GROUP BY status;
```

### Get top contributors

```sql
SELECT
    p.full_name,
    p.reputation,
    COUNT(c.id) as corrections_count
FROM profiles p
LEFT JOIN corrections c ON c.author_id = p.id AND c.status = 'approved'
GROUP BY p.id
ORDER BY p.reputation DESC
LIMIT 10;
```

### Find items by shelfmark

```sql
SELECT
    li.*,
    ul.name as list_name,
    p.email as owner_email
FROM list_items li
JOIN user_lists ul ON ul.id = li.list_id
JOIN profiles p ON p.id = ul.user_id
WHERE li.shelfmark ILIKE '%T-S 12%'
ORDER BY li.added_at DESC;
```

---

## Troubleshooting

### "SUPABASE_ANON_KEY not set"

Make sure `.env` file exists and contains valid credentials.

### "Permission denied"

Either table grants or RLS may be blocking the operation. Check:
1. The table has explicit `GRANT` statements for the current role (`anon`, `authenticated`, or `service_role`)
2. User is authenticated
3. User owns the resource
4. User has required role
5. RLS policy matches the operation

### "Connection failed"

1. Check Supabase status: https://status.supabase.com
2. Verify URL and key are correct
3. Check network connectivity

### "Foreign key violation"

You're trying to reference a non-existent record. Check that:
- `user_id` exists in `profiles`
- `list_id` exists in `user_lists`
- etc.

---

## Best Practices

1. **Always use helper functions** when available (in `supabase_client.py`)
2. **Handle errors** - all functions return `{'error': ...}` on failure
3. **Add explicit Data API grants and check RLS** when adding new `public` tables
4. **Use transactions** for multi-step operations
5. **Index frequently queried columns**

---

## Published Joins (Phase 52)

Community puzzle join publishing allows users to share their fragment arrangements.

### Tables

#### published_joins

```sql
CREATE TABLE published_joins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id),
    local_doc_id TEXT,
    title TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    join_type TEXT NOT NULL DEFAULT 'physical',
    fragments_json JSONB NOT NULL,
    shelfmarks TEXT[] NOT NULL DEFAULT '{}',
    image_path TEXT,
    thumbnail_path TEXT,
    is_published BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX idx_published_joins_user ON published_joins(user_id);
CREATE INDEX idx_published_joins_published ON published_joins(is_published, created_at DESC);
CREATE INDEX idx_published_joins_shelfmarks ON published_joins USING GIN(shelfmarks);
```

#### published_join_fragments

Fragment index for reverse lookups (find published joins containing a given sys_id).

```sql
CREATE TABLE published_join_fragments (
    join_id UUID NOT NULL REFERENCES published_joins(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT NOT NULL
);

CREATE INDEX idx_pjf_sys_id ON published_join_fragments(sys_id);
```

### RLS Policies

#### published_joins

```sql
ALTER TABLE published_joins ENABLE ROW LEVEL SECURITY;

-- SELECT: anyone can read published joins
CREATE POLICY "Anyone can read published joins"
ON published_joins FOR SELECT
USING (is_published = true);

-- SELECT: users can see their own joins (even unpublished)
CREATE POLICY "Users can read own joins"
ON published_joins FOR SELECT TO authenticated
USING (auth.uid() = user_id);

-- INSERT: authenticated users can create joins
CREATE POLICY "Users can insert own joins"
ON published_joins FOR INSERT TO authenticated
WITH CHECK (auth.uid() = user_id);

-- UPDATE: users can update their own joins
CREATE POLICY "Users can update own joins"
ON published_joins FOR UPDATE TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- DELETE: users can delete their own joins
CREATE POLICY "Users can delete own joins"
ON published_joins FOR DELETE TO authenticated
USING (auth.uid() = user_id);

-- UPDATE: admins can update any published join (for soft-delete/hide)
CREATE POLICY "Admins can update any published join"
ON published_joins FOR UPDATE TO authenticated
USING (EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND role = 'admin'))
WITH CHECK (true);
```

#### published_join_fragments

```sql
ALTER TABLE published_join_fragments ENABLE ROW LEVEL SECURITY;

-- SELECT: anyone can read fragments of published joins
CREATE POLICY "Anyone can read published join fragments"
ON published_join_fragments FOR SELECT
USING (EXISTS (SELECT 1 FROM published_joins pj WHERE pj.id = join_id AND pj.is_published = true));

-- INSERT: users can insert fragments for their own joins
CREATE POLICY "Users can insert own join fragments"
ON published_join_fragments FOR INSERT TO authenticated
WITH CHECK (EXISTS (SELECT 1 FROM published_joins pj WHERE pj.id = join_id AND pj.user_id = auth.uid()));

-- DELETE: users can delete fragments for their own joins
CREATE POLICY "Users can delete own join fragments"
ON published_join_fragments FOR DELETE TO authenticated
USING (EXISTS (SELECT 1 FROM published_joins pj WHERE pj.id = join_id AND pj.user_id = auth.uid()));
```

### Storage Bucket

**Bucket name:** `puzzle-images` (public read access)

```sql
-- Storage policies for puzzle-images bucket
-- Users can upload to their own folder (folder name = auth.uid())
CREATE POLICY "Users can upload own puzzle images"
ON storage.objects FOR INSERT TO authenticated
WITH CHECK (bucket_id = 'puzzle-images' AND (storage.foldername(name))[1] = auth.uid()::text);

-- Users can update/overwrite their own images
CREATE POLICY "Users can update own puzzle images"
ON storage.objects FOR UPDATE TO authenticated
USING (bucket_id = 'puzzle-images' AND (storage.foldername(name))[1] = auth.uid()::text);

-- Users can delete their own images
CREATE POLICY "Users can delete own puzzle images"
ON storage.objects FOR DELETE TO authenticated
USING (bucket_id = 'puzzle-images' AND (storage.foldername(name))[1] = auth.uid()::text);

-- Anyone can read puzzle images (public bucket)
CREATE POLICY "Anyone can read puzzle images"
ON storage.objects FOR SELECT
USING (bucket_id = 'puzzle-images');
```

---

## Pending Migrations

### Phase 61: Add ie_id columns (volume-aware community data)

Run in Supabase SQL Editor before testing multi-IE corrections/comments:

```sql
ALTER TABLE corrections ADD COLUMN IF NOT EXISTS ie_id TEXT;
ALTER TABLE comments ADD COLUMN IF NOT EXISTS ie_id TEXT;
COMMENT ON COLUMN corrections.ie_id IS 'IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness.';
COMMENT ON COLUMN comments.ie_id IS 'IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness.';
```

## Resources

- [Supabase Documentation](https://supabase.com/docs)
- [Supabase Python Client](https://supabase.com/docs/reference/python)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Row Level Security](https://supabase.com/docs/guides/auth/row-level-security)
