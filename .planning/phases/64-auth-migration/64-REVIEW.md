---
phase: 64-auth-migration
reviewed: 2026-04-14T00:00:00Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - web/supabase_client.py
  - supabase_corrections_client.py
  - web/main.py
  - web/api.py
findings:
  critical: 2
  warning: 6
  info: 4
  total: 12
status: issues_found
---

# Phase 64: Code Review Report

**Reviewed:** 2026-04-14
**Depth:** standard
**Files Reviewed:** 4
**Status:** issues_found

## Summary

Four files were reviewed covering the Supabase auth migration layer (web and desktop clients), the web application entry point, and the API route module. The auth token handling and per-user client caching logic in `web/supabase_client.py` is generally sound. Two critical issues were identified: a hardcoded Supabase anon key committed directly in source in `supabase_corrections_client.py`, and an unguarded ilike injection in search queries in the same file. Several warnings cover in-memory caches without eviction bounds, unsafe `float()`/`int()` conversions that crash on malformed input, a `get_connected_fragments` `timeout` parameter that is accepted but never enforced, and a `get_all_corrections` search path that injects user-controlled text without escaping. Four informational items address code duplication, dead parameters, unbounded rate-limit maps, and a side-effecting patch to a library's installed template file.

---

## Critical Issues

### CR-01: Hardcoded Supabase credentials committed to source

**File:** `supabase_corrections_client.py:62-63`
**Issue:** Both `SUPABASE_URL` and `SUPABASE_ANON_KEY` are hardcoded as default values in `os.environ.get()` calls. The anon key is a fully-formed JWT that is now in version control. While Supabase anon keys are considered public-facing, hardcoding them in source creates several problems: it leaks the project reference ID (`ylcpglwxompwjcufdemz`), it means rotating the key requires a code change rather than an env-var update, and it can end up in log output or error messages. The web client (`web/supabase_client.py`) correctly delegates to `shared/supabase_provider.py` and has no hardcoded fallback — the desktop client should follow the same pattern.

```python
# Current (lines 62-63)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://ylcpglwxompwjcufdemz.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...')

# Fix: remove hardcoded defaults, raise on missing config (same as web client)
from shared.supabase_provider import get_url, get_anon_key
SUPABASE_URL = get_url()
SUPABASE_ANON_KEY = get_anon_key()
```

### CR-02: PostHog filter string injection in `web/supabase_client.py` `get_all_corrections` / ilike patterns in desktop client

**File:** `supabase_corrections_client.py:950-953` and `1384-1385`, `1494-1496`, `1528-1530`
**Issue:** User-controlled strings (search_text, shelfmark, query) are interpolated directly into Supabase PostgREST filter strings passed to `.or_()` and `.ilike()`. PostgREST filter syntax uses `,` as an OR separator and special characters like `(`, `)`, `.` have meaning in the query syntax. A shelfmark containing `%` or `,` can corrupt the filter expression, causing incorrect query results (data leakage) or a 400 error. The `get_all_corrections` search_text path is particularly sensitive because it is an admin-facing search across all users' corrections text.

```python
# Vulnerable (line 951-953)
query = query.or_(
    f'original_text.ilike.%{search_text}%,'
    f'corrected_text.ilike.%{search_text}%,'
    f'notes.ilike.%{search_text}%'
)

# Fix: escape the user value before interpolation
import re as _re
safe = _re.sub(r'[%_,()]', lambda m: f'\\{m.group()}', search_text)
query = query.or_(
    f'original_text.ilike.%{safe}%,'
    f'corrected_text.ilike.%{safe}%,'
    f'notes.ilike.%{safe}%'
)
# Same fix needed for shelfmark/query in get_connected_fragments (line 1384),
# search_joins (line 1494), and get_my_joins (line 1528).
```

---

## Warnings

### WR-01: In-memory image caches are unbounded — potential memory exhaustion

**File:** `web/api.py:495`, `571`, `574`, `637`, `694`
**Issue:** Five separate in-process image caches (`_image_cache`, `_oxford_image_cache`, `_cambridge_image_cache`, `_manchester_image_cache`, `_jts_image_cache`) are plain dicts with no eviction policy. Each entry stores full binary image content (up to 2 MB each at `width=2000`). On a long-running server with many unique sys_id/page combinations, these caches grow without bound and will eventually exhaust available memory. The NLI cache already has TTL checks via `IMAGE_CACHE_TTL`; the check is present but the old entries are never removed — they remain in memory even after expiry.

**Fix:** Use `collections.OrderedDict` with a fixed max size (e.g., 500 entries) and pop the oldest on insert, or use the existing `NLI_DISK_CACHE_TTL` mechanism that already handles pruning for the FL ID cache.

```python
from collections import OrderedDict
_MAX_IMAGE_CACHE = 500
_image_cache: OrderedDict = OrderedDict()

# On insert:
if len(_image_cache) >= _MAX_IMAGE_CACHE:
    _image_cache.popitem(last=False)  # evict oldest
_image_cache[cache_key] = (resp.content, content_type, _time.time())
```

### WR-02: `get_user_lists` retry loop is infinite on JWT expiry

**File:** `web/supabase_client.py:460-465`
**Issue:** When a JWT expiry is detected, the code calls `reset_client()` and then recursively calls `get_user_lists()` with the same arguments. If `reset_client()` does not result in a newly valid token (which it won't — it only clears the singleton, it does not refresh tokens), the retry will hit the same expired JWT, detect it again, and recurse indefinitely — or until Python's call stack limit is reached. The same pattern appears in `get_comments` (line 880) and `get_projects` (line 698).

**Fix:** The retry should not be recursive; add a `_retry` boolean guard:

```python
def get_user_lists(user_id: str, include_deleted: bool = False, _retry: bool = True) -> List[Dict]:
    ...
    except Exception as e:
        if _is_jwt_expired(e) and _retry:
            reset_client()
            return get_user_lists(user_id=user_id, include_deleted=include_deleted, _retry=False)
        ...
```

### WR-03: Unsafe `float()` / `int()` conversions on untrusted query params crash with 500

**File:** `web/api.py:1115`, `1118`, `1213`, `1215`
**Issue:** In `puzzle_process` and `puzzle_upload_derivative`, query parameters are converted with bare `float()` and `int()` calls:
```python
threshold = float(request.query_params.get('threshold', 30))
size = int(request.query_params.get('size', 800))
```
A request with `?threshold=abc&size=xyz` will raise `ValueError` and return an unhandled 500 to the caller instead of a clean 400. These endpoints accept uploads from the browser extension, so malformed parameters are plausible.

**Fix:**
```python
try:
    threshold = float(request.query_params.get('threshold', 30))
    size = int(request.query_params.get('size', 800))
except (ValueError, TypeError):
    return Response(content="Invalid threshold or size parameter", status_code=400)
```

### WR-04: `get_connected_fragments` timeout parameter is accepted but never enforced

**File:** `supabase_corrections_client.py:1375`
**Issue:** The method signature is `def get_connected_fragments(self, shelfmark: str, timeout: int = 30)` and the timeout parameter is docstring'd as a timeout, but the Supabase client call has no timeout — it uses whatever the underlying httpx default is. When a caller uses `get_connected_fragments_quick(shelfmark)` expecting a 3-second timeout (line 1433), no such limit is actually applied. This can block the desktop app for an extended period.

**Fix:** Either enforce the timeout via Supabase client options, or document that it is unused:
```python
# Option A: document the limitation
# timeout: int = 30  # NOTE: not currently enforced; Supabase client uses its own default
# Option B: pass timeout to supabase client (supabase-py supports options= dict)
response = client.table('fragment_joins').select('*').or_(...).execute(options={'timeout': timeout})
```

### WR-05: `_puzzle_rate_limits` dict also grows without bound (memory exhaustion + rate limit bypass after eviction)

**File:** `web/api.py:1064`
**Issue:** The per-IP rate limit map `_puzzle_rate_limits` is a plain dict and never has its stale entries cleared. With many unique IPs (real traffic or a distributed attack), it grows indefinitely. There is also a subtler issue: because old windows are never pruned, a high-cardinality IP set leaves the dict large, but once an IP's window expires (`now - window_start >= 60`), their counter is reset to 1 — so the dict size keeps growing but effectively acts as if no rate limit applies for IPs not seen recently.

**Fix:** Add periodic eviction (or use a TTL-aware structure) when the dict grows large:
```python
# Prune stale entries when dict exceeds threshold
if len(_puzzle_rate_limits) > 10000:
    cutoff = now - 60
    _puzzle_rate_limits = {ip: v for ip, v in _puzzle_rate_limits.items() if v[1] > cutoff}
```

### WR-06: `_session_to_dict` returns `None` typed as `Dict` — breaks type contract

**File:** `web/supabase_client.py:384`
**Issue:** The function is annotated `-> Dict` but explicitly returns `None` when the session argument is falsy. Callers that type-check the return value will see unexpected `None` values. More practically, callers that do `session['access_token']` immediately on the return value from `sign_up` or `sign_in` will crash with `TypeError: 'NoneType' object is not subscriptable` for the case where a session is not returned (e.g., sign_up before email confirmation).

```python
# Current (lines 383-391)
def _session_to_dict(session) -> Dict:
    if not session:
        return None  # type: ignore — violates return type
    ...

# Fix: return Optional[Dict]
def _session_to_dict(session) -> Optional[Dict]:
    if not session:
        return None
    ...
```

---

## Info

### IN-01: `_patch_html_lang_attribute` mutates NiceGUI's installed template on every boot

**File:** `web/main.py:77-87`
**Issue:** The patch opens and rewrites `index.html` inside the NiceGUI package directory on every application startup. While guarded by an idempotency check (`if '<html>' in original`), this writes to the installed package's file tree on every first boot after upgrade, and it mutates shared infrastructure that other NiceGUI processes on the same host would see. It also silently succeeds or fails, and the warning on failure (`'html lang patch failed: %s'`) is the only signal. This is a code smell — a brittle dependency on package internals.

**Fix:** Prefer injecting the lang attribute via a custom `head_html` or NiceGUI's `app.on_startup` mechanism if the framework supports it, or at minimum add a startup check that verifies the patch took effect.

### IN-02: Unused `source` parameter in desktop `create_join`

**File:** `supabase_corrections_client.py:1351-1359`
**Issue:** `FragmentJoin.source` is documented as a field (defaulting to `'user'`), but `create_join()` never writes it to the DB dict. The caller never has a way to set source to anything other than the hardcoded `'user'` default in `_parse_join`. This is dead interface surface.

### IN-03: `vote_correction` and `vote_discovery` in desktop client recount votes from scratch

**File:** `supabase_corrections_client.py:989-1011`, `1271-1299`
**Issue:** Both vote methods fetch all votes for an item after casting a vote and recount upvotes/downvotes from scratch (`sum(1 for v in votes.data ...)`), then write the aggregated counts back. This is a TOCTOU race: between the `upsert` and the `update`, another user's vote may arrive, and the count overwrite will lose that concurrent write. The preferred pattern is a database-side increment/decrement (SQL `UPDATE ... SET upvotes = upvotes + 1`).

### IN-04: Duplicate profile-enrichment pattern across four methods

**File:** `supabase_corrections_client.py: ~854-872, ~902-919, ~959-979`; `web/supabase_client.py:838-855`
**Issue:** The pattern of batch-fetching profiles and merging them into a result list is copy-pasted into at least four methods in the desktop client (`get_corrections_for_document`, `get_my_corrections`, `get_all_corrections`) and once extracted to the `_enrich_with_profiles` helper in the web client. The desktop client should adopt the same helper pattern to reduce duplication and the risk of divergence.

---

_Reviewed: 2026-04-14_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
