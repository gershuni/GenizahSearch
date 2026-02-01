# GenizahSearch Supabase Guide

> Guide for working with Supabase in the GenizahSearch project

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
| `user_id` | uuid | Submitter |
| `sys_id` | text | Manuscript system ID |
| `fl_id` | text | Specific page |
| `original_text` | text | Original transcription |
| `corrected_text` | text | Corrected version |
| `status` | text | draft / pending / approved / rejected |
| `votes_up` | int | Upvote count |
| `votes_down` | int | Downvote count |
| `reviewed_by` | uuid | Reviewer (if reviewed) |
| `reviewed_at` | timestamp | Review date |

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

# Insert
result = client.table('comments').insert({
    'user_id': user_id,
    'sys_id': 'MS-12345',
    'content': 'My comment'
}).execute()

# Update
result = client.table('profiles').update({
    'full_name': 'New Name'
}).eq('id', user_id).execute()

# Delete
result = client.table('list_items').delete().eq('id', item_id).execute()

# Complex queries
result = client.table('corrections') \
    .select('*, profiles(full_name)') \
    .eq('status', 'pending') \
    .order('created_at', desc=True) \
    .limit(10) \
    .execute()
```

---

## Row Level Security (RLS)

RLS policies ensure users can only access their own data:

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
LEFT JOIN corrections c ON c.user_id = p.id AND c.status = 'approved'
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

RLS is blocking the operation. Check:
1. User is authenticated
2. User owns the resource
3. User has required role

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
3. **Check RLS** when adding new tables
4. **Use transactions** for multi-step operations
5. **Index frequently queried columns**

---

## Resources

- [Supabase Documentation](https://supabase.com/docs)
- [Supabase Python Client](https://supabase.com/docs/reference/python)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Row Level Security](https://supabase.com/docs/guides/auth/row-level-security)
