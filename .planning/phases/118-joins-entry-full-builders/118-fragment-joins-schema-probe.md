# Fragment Joins Schema Probe — Wave 0 (2026-06-18)

## Open Question 1: Does `fragment_joins.status` exist in the live Supabase deployment?

**Answer: YES — confirmed via static schema evidence (two independent live-code sources).**

---

## Method Used

**Static fallback** (canonical SQL + live function signature). No Supabase environment
variables were present in the execution environment (SUPABASE_URL / SUPABASE_ANON_KEY
not set), so a live `limit(1)` probe could not be run. The static evidence is treated as
authoritative because two independent sources agree:

---

## Evidence

### Source 1: `supabase_setup.sql:162` (canonical table DDL)

```sql
CREATE TABLE public.fragment_joins (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    fragment_a_sys_id TEXT NOT NULL,
    fragment_a_shelfmark TEXT,
    fragment_b_sys_id TEXT NOT NULL,
    fragment_b_shelfmark TEXT,
    join_type TEXT DEFAULT 'uncertain' CHECK (join_type IN ('physical', 'content', 'uncertain')),
    confidence TEXT DEFAULT 'possible' CHECK (confidence IN ('certain', 'probable', 'possible')),
    notes TEXT,
    evidence TEXT,
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed', 'rejected')),  -- line 162
    created_at TIMESTAMPTZ DEFAULT NOW(),
    confirmed_by UUID REFERENCES auth.users(id),   -- line 164
    confirmed_at TIMESTAMPTZ
);
```

The `status` column:
- Type: `TEXT`
- Default: `'proposed'`
- Constraint: `CHECK (status IN ('proposed', 'confirmed', 'rejected'))`
- The value `'confirmed'` is the canonical CHECK value for confirmed joins.

### Source 2: `web/supabase_client.py:1574-1594` (live function, already supports the filter)

```python
def get_fragment_joins(user_id: str = None, fragment_sys_id: str = None,
                       status: str = None) -> List[Dict]:
    """Get fragment joins with optional filters."""
    # docstring references status='confirmed' explicitly
    ...
    if status:
        query = query.eq('status', status)   # line 1594 — applied when truthy
```

The function ALREADY accepts a `status` kwarg and applies `.eq('status', status)` when
truthy. The docstring (lines 1578-1579) explicitly references `status='confirmed'`.

### Note on `docs/guides/SUPABASE_GUIDE.md`

The SUPABASE_GUIDE.md schema diagram OMITS the `status` column. This diagram is STALE.
Do NOT treat its omission as evidence of absence. The canonical SQL + the live client
function are the authoritative sources.

---

## RLS Context

The RLS policy on `fragment_joins` is:

```sql
CREATE POLICY "Anyone can view joins" ON fragment_joins FOR SELECT
    USING (true);
```

`USING (true)` means **all rows are publicly readable** — the DB does NOT enforce
confirmed-only filtering at the SQL layer. This is critical to understanding D-17:

> A user's own `proposed` joins are readable by anyone via RLS. The application-layer
> `status='confirmed'` filter is therefore the ONLY mechanism that excludes unconfirmed
> joins from the process-global Lab known-joins group.

---

## Directive for Plan 02 — UNAMBIGUOUS

### PRIMARY ANC-05 fix (use this):

On the Lab / `confirmed_only=True` path in `fetch_connected_fragments`, call:

```python
joins = get_fragment_joins(
    fragment_sys_id=document_id,
    status='confirmed',   # EXACT value — matches CHECK constraint
)
```

AND use a separate cache key:

```python
cache_key = f"doc:{document_id}:pgp:{pgpid}:confirmed"
```

This prevents the confirmed-only cache from poisoning the unconfirmed browse-dialog
cache (keyed `doc:{document_id}:pgp:{pgpid}`). Because RLS is `USING(true)`, the
application-layer `status='confirmed'` filter is what realizes D-17: User A's `proposed`
joins are excluded from the process-global Lab group seen by all users.

### CONDITIONAL FALLBACK (use ONLY if live probe proves status column absent):

If a future live probe of the deployed Supabase instance demonstrates that the `status`
column is absent from `fragment_joins` (contradicting both `supabase_setup.sql:162` and
`web/supabase_client.py:1593-1594`), fall back to an application-layer filter:

```python
# Fallback ONLY if status column absent in live DB:
joins = get_fragment_joins(fragment_sys_id=document_id)
joins = [j for j in joins if j.get('status') != 'proposed']
# OR: exclude source=='user' rows that are not confirmed
joins = [j for j in joins if j.get('source') != 'user']
```

AND still use the `:confirmed` cache key to prevent cache poisoning.

This contingency is NOT the default path. The static evidence is clear.

---

## Summary Decision Table

| Path | status kwarg | cache key | Purpose |
|------|-------------|-----------|---------|
| Lab / `confirmed_only=True` | `status='confirmed'` | `doc:{id}:pgp:{pgpid}:confirmed` | D-17: exclude proposed joins from global Lab group |
| Browse dialog / default (`confirmed_only=False`) | No status filter | `doc:{id}:pgp:{pgpid}` | Show all joins (proposed + confirmed) to the owner |

---

## Conclusion

`fragment_joins.status` EXISTS with values `proposed` / `confirmed` / `rejected`
(default `proposed`). The ANC-05 primary fix is `get_fragment_joins(fragment_sys_id=…,
status='confirmed')` on the Lab path, plus the `:confirmed` cache key. Plan 02 has no
remaining ambiguity about the schema — proceed with the primary fix.
