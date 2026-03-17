# Phase 52: Community + Integration - Research

**Researched:** 2026-03-17
**Domain:** Supabase community publishing (tables + storage), NiceGUI/PyQt6 UI integration
**Confidence:** HIGH

## Summary

Phase 52 adds the ability to publish puzzle join documents for community review and browse published joins from other researchers. The core technical work is: (1) a new Supabase `published_joins` table for metadata, (2) Supabase Storage bucket for composite PNG images, (3) publish/unpublish actions from both web and desktop puzzle canvases, (4) a new feed item type in the Discoveries Center, and (5) a "Community Puzzle Joins" section in the joins panel for browse/ResultDialog contexts.

The project already has all the building blocks: `PuzzleDocument.to_json()` for arrangement serialization, `compose_puzzle_export()` for full-res PNG generation, `get_feed_items()` for multi-table feed aggregation, `get_user_client()` for per-user RLS-authenticated Supabase operations, and `storage3` v2.28.0 already installed. No new dependencies are needed.

**Primary recommendation:** Create a `published_joins` Supabase table with user_id, arrangement JSON, metadata, and image URLs. Use a `puzzle-images` Supabase Storage bucket for composite PNGs and thumbnails. Follow the existing `create_discovery` / `get_feed_items` pattern exactly for publishing and feed integration.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **One-click publish** -- user clicks Publish, join immediately visible to all. No moderation queue.
- Requires **logged-in Supabase account** (consistent with corrections/lists auth model)
- User can **edit and unpublish anytime** -- full control. Re-publish updates the existing published record. Unpublish makes it private again.
- Attribution shown as **display name from Supabase profile** (e.g. "Published by Dr. Sarah Cohen")
- **Full arrangement JSON** published (fragment positions, rotations, scales, flips, shelfmarks, folio labels). Enables forking.
- **Full-resolution composite PNG** uploaded to server storage at publish time (~170GB available).
- **Thumbnail** -- smaller composite for browsing display
- **Metadata** -- title, notes, fragment shelfmarks list, author (Supabase user), publish date
- Published puzzle joins appear **mixed into the activity feed** alongside existing discoveries and questions
- Feed item shows: composite thumbnail, title, author, fragment shelfmarks, date
- Click to view details: full-res image download, notes, fragment list, "Open in Puzzle" (fork) button
- **Separate section below FJMS scientific joins** -- "Community Puzzle Joins" section in joins panel
- Clear distinction from authoritative FJMS joins
- Desktop parity: Publish button, Discoveries tab, joins dialog

### Claude's Discretion
- Supabase table schema design for published joins
- Supabase storage bucket configuration for composite images
- Fork behavior details (copy to local joins.db, rename convention)
- Feed item card design and layout
- Thumbnail size and compression for upload
- Desktop Discoveries tab rendering approach

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CANV-02 | User can add a fragment from personal lists or browse/search results | Already implemented in Phase 49-50 per REQUIREMENTS.md (marked complete). Verify integration points exist. |
| COMM-01 | Join documents are saved to personal workspace by default | Already implemented via joins.db local SQLite (Phase 50). Published_joins table adds optional cloud visibility. |
| COMM-02 | User can publish a join document for community review | New `published_joins` table + Storage bucket + publish UI in both apps |
| COMM-03 | Published joins are browsable by other users | Feed integration in Discoveries Center + joins panel section in browse/ResultDialog |
</phase_requirements>

## Standard Stack

### Core (already installed, no new dependencies)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase | 2.28.0 | Database client + Storage API | Already used for all community features |
| storage3 | 2.28.0 | Supabase Storage file uploads/downloads | Already installed as supabase dependency |
| Pillow | (installed) | PNG composite generation | Already used by puzzle_export.py |
| NiceGUI | (installed) | Web UI components | Project web framework |
| PyQt6 | (installed) | Desktop UI components | Project desktop framework |

### No New Dependencies Required
The entire phase uses existing libraries. Supabase Storage is accessed via the already-installed `supabase` client (`client.storage.from_("bucket")`).

**Installation:**
```bash
# Nothing to install -- all dependencies already present
```

## Architecture Patterns

### Recommended Project Structure
```
shared/
  puzzle_publish_service.py  # NEW: Supabase publish/unpublish/list operations
  puzzle_model.py            # EXISTING: PuzzleDocument/PuzzleFragment (no changes)
  puzzle_service.py          # EXISTING: Local joins.db CRUD (no changes)
  puzzle_export.py           # EXISTING: Composite PNG generation (no changes)
web/
  supabase_client.py         # EXTEND: add published_joins CRUD + Storage upload
  pages/
    puzzle.py                # EXTEND: add Publish button + status indicator
    discoveries.py           # EXTEND: add 'puzzle_join' feed item type
  components/
    joins_panel.py           # EXTEND: add "Community Puzzle Joins" section
genizah_app.py               # EXTEND: PuzzleCanvasWindow publish button, ResultDialog joins
```

### Pattern 1: Supabase Table + Storage for Published Joins

**What:** A `published_joins` Postgres table holds metadata + arrangement JSON. Composite PNG and thumbnail are uploaded to a `puzzle-images` Supabase Storage bucket. The table stores the storage path (not URL) for the image.

**Recommended schema:**
```sql
CREATE TABLE published_joins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id),
    local_doc_id TEXT,              -- local joins.db ID for re-linking
    title TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    join_type TEXT NOT NULL DEFAULT 'physical',
    fragments_json JSONB NOT NULL,  -- full PuzzleDocument arrangement
    shelfmarks TEXT[] NOT NULL DEFAULT '{}',  -- for search/filtering
    image_path TEXT,                -- Storage path: puzzle-images/{user_id}/{id}.png
    thumbnail_path TEXT,            -- Storage path: puzzle-images/{user_id}/{id}_thumb.png
    is_published BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX idx_published_joins_user ON published_joins(user_id);
CREATE INDEX idx_published_joins_published ON published_joins(is_published, created_at DESC);
CREATE INDEX idx_published_joins_shelfmarks ON published_joins USING GIN(shelfmarks);

-- Fragment index for reverse lookups (which published joins contain this sys_id)
CREATE TABLE published_join_fragments (
    join_id UUID NOT NULL REFERENCES published_joins(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT NOT NULL
);
CREATE INDEX idx_pjf_sys_id ON published_join_fragments(sys_id);
```

**RLS policies:**
```sql
-- Published joins are publicly readable
CREATE POLICY "Published joins are public"
ON published_joins FOR SELECT TO public
USING (is_published = true);

-- Users can see their own unpublished joins
CREATE POLICY "Users see own joins"
ON published_joins FOR SELECT TO authenticated
USING (auth.uid() = user_id);

-- Users can insert their own joins
CREATE POLICY "Users can publish"
ON published_joins FOR INSERT TO authenticated
WITH CHECK (auth.uid() = user_id);

-- Users can update their own joins
CREATE POLICY "Users can update own"
ON published_joins FOR UPDATE TO authenticated
USING (auth.uid() = user_id)
WITH CHECK (auth.uid() = user_id);

-- Users can delete their own joins
CREATE POLICY "Users can delete own"
ON published_joins FOR DELETE TO authenticated
USING (auth.uid() = user_id);

-- Fragment index follows parent visibility
CREATE POLICY "Published join fragments public"
ON published_join_fragments FOR SELECT TO public
USING (EXISTS (
    SELECT 1 FROM published_joins pj
    WHERE pj.id = join_id AND (pj.is_published = true OR pj.user_id = auth.uid())
));
```

**Storage bucket:**
```sql
-- Create bucket (via Supabase dashboard or SQL)
INSERT INTO storage.buckets (id, name, public) VALUES ('puzzle-images', 'puzzle-images', true);

-- RLS: Authenticated users can upload to their own folder
CREATE POLICY "Users upload own images"
ON storage.objects FOR INSERT TO authenticated
WITH CHECK (bucket_id = 'puzzle-images' AND (storage.foldername(name))[1] = auth.uid()::text);

-- Public read
CREATE POLICY "Public read puzzle images"
ON storage.objects FOR SELECT TO public
USING (bucket_id = 'puzzle-images');

-- Users can delete own images
CREATE POLICY "Users delete own images"
ON storage.objects FOR DELETE TO authenticated
USING (bucket_id = 'puzzle-images' AND (storage.foldername(name))[1] = auth.uid()::text);
```

### Pattern 2: Publish Flow (follows create_discovery pattern)

**What:** Publishing reuses `get_user_client()` for per-user auth, generates composite PNG + thumbnail, uploads to Storage, inserts metadata row.

**Example (web publish):**
```python
async def publish_join(doc: PuzzleDocument, image_service):
    """Publish a join document to Supabase."""
    from web.supabase_client import get_user_client
    from web.auth_state import GlobalAuthState
    from shared.puzzle_export import compose_puzzle_export, generate_thumbnail

    auth = GlobalAuthState.get_instance()
    if not auth.is_authenticated:
        raise ValueError("Must be logged in to publish")

    client = get_user_client()
    user_id = auth.user_id

    # Generate full-res composite PNG
    composite = compose_puzzle_export(doc.fragments, image_service, export_size=3000)
    if composite is None:
        raise ValueError("No fragments to export")

    import io
    buf = io.BytesIO()
    composite.save(buf, format='PNG', optimize=True)
    png_bytes = buf.getvalue()

    # Generate thumbnail (300px for feed display)
    thumb_buf = io.BytesIO()
    composite.thumbnail((300, 300))
    composite.save(thumb_buf, format='PNG', optimize=True)
    thumb_bytes = thumb_buf.getvalue()

    # Upload to Storage
    join_id = str(uuid.uuid4())
    image_path = f"{user_id}/{join_id}.png"
    thumb_path = f"{user_id}/{join_id}_thumb.png"

    client.storage.from_("puzzle-images").upload(
        path=image_path,
        file=png_bytes,
        file_options={"content-type": "image/png", "upsert": "true"}
    )
    client.storage.from_("puzzle-images").upload(
        path=thumb_path,
        file=thumb_bytes,
        file_options={"content-type": "image/png", "upsert": "true"}
    )

    # Extract shelfmarks list
    shelfmarks = list(dict.fromkeys(f.shelfmark for f in doc.fragments if f.shelfmark))

    # Insert metadata row
    import json
    data = {
        'id': join_id,
        'user_id': user_id,
        'local_doc_id': doc.id,
        'title': doc.title,
        'notes': doc.notes,
        'join_type': doc.join_type,
        'fragments_json': json.loads(doc.to_json()),  # JSONB
        'shelfmarks': shelfmarks,
        'image_path': image_path,
        'thumbnail_path': thumb_path,
        'is_published': True,
    }
    client.table('published_joins').upsert(data).execute()
    return join_id
```

### Pattern 3: Feed Integration (follows get_feed_items pattern)

**What:** Extend `get_feed_items()` to query `published_joins` table alongside discoveries/corrections/comments. Add a new item_type `'puzzle_join'`.

**Example:**
```python
# In get_feed_items(), add after the joins section:
if not item_type or item_type == 'puzzle_join':
    try:
        pj_query = client.table('published_joins').select(
            'id, user_id, title, notes, shelfmarks, thumbnail_path, created_at'
        ).eq('is_published', True).order('created_at', desc=True).limit(limit)
        pj_resp = pj_query.execute()
        for pj in (pj_resp.data or []):
            # Get public URL for thumbnail
            thumb_url = client.storage.from_("puzzle-images").get_public_url(pj['thumbnail_path'])
            items.append({
                'id': f"puzzle_join_{pj['id']}",
                'item_type': 'puzzle_join',
                'title': pj.get('title', ''),
                'shelfmarks': pj.get('shelfmarks', []),
                'thumbnail_url': thumb_url,
                'created_at': pj.get('created_at'),
                'author': {'id': pj.get('user_id')},
            })
    except Exception as e:
        logger.error(f"Error loading puzzle joins: {e}")
```

### Pattern 4: Joins Panel Community Section

**What:** In `joins_panel.py`, after rendering FJMS scientific joins, add a "Community Puzzle Joins" section that queries `published_join_fragments` by sys_id.

**Key:** This section queries Supabase (not local joins.db) for published joins containing the current manuscript's sys_id. Clear visual separation from FJMS joins.

### Pattern 5: Fork to Local Workspace

**What:** "Open in Puzzle" downloads the arrangement JSON from Supabase, creates a new local PuzzleDocument in joins.db with a modified title (e.g., "Fork of: Original Title"), and opens it in the puzzle canvas.

**Convention:** Forked documents get `title = f"Fork of: {original_title}"` and a new UUID. The original published join ID is not tracked (simple copy, not git-style forking).

### Anti-Patterns to Avoid
- **Don't store base64 PNG in Postgres columns:** Use Supabase Storage for binary files. Base64 in JSONB would bloat the table and slow queries.
- **Don't re-composite on the server:** Store the pre-rendered PNG at publish time. Viewers download the stored image.
- **Don't create a separate publish service module if small:** The publish/unpublish operations are simple CRUD -- they belong in `web/supabase_client.py` alongside existing community operations. If the code grows beyond ~100 lines, extract to `shared/puzzle_publish_service.py`.
- **Don't modify PuzzleDocument or PuzzleService:** Local persistence is separate from community publishing. The publish flow reads from the local model but writes to Supabase independently.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| File storage | Custom file server | Supabase Storage bucket | CDN, RLS, public URLs, already installed |
| User authentication | Custom auth checks | `get_user_client()` + `GlobalAuthState` | Already handles per-user RLS, token refresh |
| Feed aggregation | New feed system | Extend existing `get_feed_items()` | Unified feed already works with multiple types |
| Profile display names | Custom user lookup | Existing profiles batch-resolve pattern | See `get_fragment_joins()` profiles_map pattern |

**Key insight:** Every piece of infrastructure needed for publishing already exists in the codebase. The work is integration, not invention.

## Common Pitfalls

### Pitfall 1: Blocking the UI During PNG Generation + Upload
**What goes wrong:** `compose_puzzle_export()` is CPU-intensive (fetches images, composites them). Calling it synchronously freezes the web UI.
**Why it happens:** NiceGUI runs on a single event loop. Heavy I/O + CPU blocks all users.
**How to avoid:** Use `await run.io_bound(publish_join, doc, image_service)` in the web app. Desktop already runs in threads.
**Warning signs:** UI freezes during publish, no progress indicator.

### Pitfall 2: Storage Path Collisions on Re-publish
**What goes wrong:** User publishes, unpublishes, re-publishes -- storage paths must handle updates.
**Why it happens:** If using the same join_id, the old image is still there.
**How to avoid:** Use `upsert: "true"` in file_options so re-uploads overwrite. The join_id stays constant across re-publishes.

### Pitfall 3: RLS Policy Conflict (Own Unpublished vs Public Published)
**What goes wrong:** A user can't see their own unpublished joins because the RLS policy only allows `is_published = true`.
**Why it happens:** Missing the "users see own joins" policy.
**How to avoid:** Two SELECT policies: one for public (`is_published = true`), one for owner (`auth.uid() = user_id`). PostgreSQL ORs multiple permissive policies.

### Pitfall 4: Desktop Supabase Client Differences
**What goes wrong:** Desktop uses `supabase_corrections_client.py` which creates its own client, not `web/supabase_client.py`.
**Why it happens:** Desktop doesn't have NiceGUI's `app.storage.user` for per-request auth.
**How to avoid:** Desktop publish should use the desktop client's authenticated session (already has `self.client` from login). Storage upload API is the same.

### Pitfall 5: Large PNG Upload Size
**What goes wrong:** Full-res composites at 3000px can be 5-20MB. Upload may time out or hit Supabase free-tier limits.
**Why it happens:** RGBA PNG with multiple high-res fragments.
**How to avoid:** (1) Use PNG optimize=True. (2) Consider JPEG for the full-res version (no transparency needed for the published view). (3) Supabase Pro plan has 100MB upload limit -- well within range. (4) Show upload progress in UI.

### Pitfall 6: Thumbnail Already Exists Locally But Not Suitable for Feed
**What goes wrong:** Local `thumbnail_b64` in joins.db is 150x150px -- too small for feed cards.
**Why it happens:** Local thumbnails are for document list sidebar, feed cards need larger images.
**How to avoid:** Generate a separate 300px thumbnail for the published version. Upload both full-res and 300px thumbnail to Storage.

## Code Examples

### Supabase Storage Upload (Python)
```python
# Source: supabase.com/docs/reference/python/storage-from-upload
from supabase import create_client

client = create_client(url, key)

# Upload bytes directly (no need to write to disk first)
response = client.storage.from_("puzzle-images").upload(
    path="user-uuid/join-uuid.png",
    file=png_bytes,  # bytes object
    file_options={"content-type": "image/png", "upsert": "true"}
)

# Get public URL
public_url = client.storage.from_("puzzle-images").get_public_url("user-uuid/join-uuid.png")
```

### Existing Create Discovery Pattern (for reference)
```python
# Source: web/supabase_client.py:824
# Published joins follow this identical pattern:
client = get_user_client()  # Per-user auth for RLS
data = {
    'user_id': user_id,
    'title': title,
    'content': content,
    # ...
}
response = client.table('discoveries').insert(data).execute()
```

### Existing Feed Item Pattern (for reference)
```python
# Source: web/supabase_client.py:1130
# get_feed_items() queries multiple tables and merges into unified feed.
# Add published_joins query after the existing joins section.
# Each item gets: id, item_type, title, created_at, author
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No community joins | Local joins.db only (Phase 50) | 2026-03-17 | Private workspace only |
| N/A | Supabase Storage for images | Phase 52 (new) | First use of Storage in project |
| Feed: discoveries+corrections+comments+joins | Add puzzle_join type | Phase 52 (new) | 5th feed item type |

**New in this phase:**
- First use of Supabase Storage bucket in the project (for composite PNGs)
- First feed item type with an associated image (thumbnail display in cards)

## Open Questions

1. **Thumbnail size for feed cards**
   - What we know: Local thumbnails are 150px. Feed cards in discoveries.py show text-only items currently.
   - What's unclear: Optimal thumbnail size for visual feed cards with fragment images.
   - Recommendation: 300px max dimension. Balances quality vs upload size (~20-50KB per thumbnail).

2. **JPEG vs PNG for published full-res image**
   - What we know: Composites have transparent backgrounds (RGBA PNG). Viewers download for inspection.
   - What's unclear: Whether transparency matters for the published download.
   - Recommendation: Use PNG for fidelity. Researchers may overlay or further compose. File size is acceptable for download.

3. **Desktop Discoveries tab existence**
   - What we know: CONTEXT.md mentions "Desktop Discoveries tab shows published puzzle joins in feed." The desktop app has community features in ResultDialog but no dedicated Discoveries tab was found in code search.
   - What's unclear: Whether a desktop Discoveries tab exists or needs to be created.
   - Recommendation: Research during planning. If no tab exists, the simplest approach is adding published joins to the existing desktop joins dialog (accessible from ResultDialog community_row). A full Discoveries tab is a larger scope item.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini (project root) |
| Quick run command | `pytest tests/test_puzzle_service.py -x -q` |
| Full suite command | `pytest tests/ -x -q` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CANV-02 | Add fragment from lists/browse/search | manual-only | N/A (UI integration, already complete) | N/A |
| COMM-01 | Join docs saved to personal workspace by default | unit | `pytest tests/test_puzzle_service.py -x` | Exists (tests local save) |
| COMM-02 | User can publish a join document | unit + integration | `pytest tests/test_puzzle_publish.py -x` | Wave 0 |
| COMM-03 | Published joins are browsable | unit + integration | `pytest tests/test_puzzle_publish.py -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_puzzle_publish.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_puzzle_publish.py` -- covers COMM-02, COMM-03 (mock Supabase client for publish/list/fork operations)
- [ ] Supabase table + RLS creation (manual via dashboard, verified by publish integration test)

## Sources

### Primary (HIGH confidence)
- `web/supabase_client.py` -- existing CRUD patterns for discoveries, fragment_joins, corrections
- `shared/puzzle_service.py` -- local PuzzleDocument persistence patterns
- `shared/puzzle_export.py` -- composite PNG generation API
- `docs/guides/SUPABASE_GUIDE.md` -- existing table schemas and RLS policies
- [Supabase Python Storage docs](https://supabase.com/docs/reference/python/storage-from-upload) -- upload API

### Secondary (MEDIUM confidence)
- `storage3` v2.28.0 (PyPI) -- already installed, confirmed via `pip show`
- Supabase Storage bucket RLS patterns -- standard patterns from official docs

### Tertiary (LOW confidence)
- Desktop Discoveries tab scope -- unclear if tab exists or needs creation (needs verification during planning)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already installed and in use
- Architecture: HIGH -- follows exact patterns from existing community features
- Pitfalls: HIGH -- based on direct code reading of existing integration points
- Desktop parity: MEDIUM -- desktop Discoveries tab may need scoping during planning

**Research date:** 2026-03-17
**Valid until:** 2026-04-17 (stable domain, no external library changes expected)
