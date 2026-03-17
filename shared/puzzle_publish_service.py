# -*- coding: utf-8 -*-
"""
Puzzle Publish Service for community join publishing.

Provides CRUD operations for publishing puzzle joins to Supabase so they
are visible to the community. Each function accepts a `client` parameter
(a Supabase Client object) so it works with both web's get_user_client()
and desktop's CorrectionsClient._get_client().

Tables:
- published_joins: metadata, fragments JSON, image paths
- published_join_fragments: fragment index for reverse lookups by sys_id

Storage bucket: puzzle-images (public read)
"""

import io
import json
import logging
import uuid
from typing import Dict, List, Optional

from shared.puzzle_model import PuzzleDocument, PuzzleFragment
from shared.puzzle_export import compose_puzzle_export

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

STORAGE_BUCKET = 'puzzle-images'
THUMBNAIL_SIZE = 300   # px, for feed display
EXPORT_SIZE = 3000     # px, for full-res published image


# ── Public API ───────────────────────────────────────────────────────

def publish_join(client, user_id: str, doc: PuzzleDocument, image_service) -> str:
    """
    Publish (or re-publish) a puzzle join to Supabase.

    Generates composite PNG and thumbnail, uploads to storage, and upserts
    metadata into the published_joins table. Also rebuilds the fragment
    index in published_join_fragments.

    Args:
        client: Authenticated Supabase client.
        user_id: The publishing user's UUID.
        doc: The PuzzleDocument to publish.
        image_service: PuzzleImageService for rendering the composite.

    Returns:
        The join_id (same as doc.id).
    """
    from PIL import Image

    # Generate composite PNG
    composite = compose_puzzle_export(
        doc.fragments, image_service, export_size=EXPORT_SIZE
    )

    # Generate thumbnail from composite
    thumb_bytes = b''
    png_bytes = b''
    if composite is not None:
        # Full-res PNG
        buf = io.BytesIO()
        composite.save(buf, format='PNG')
        png_bytes = buf.getvalue()

        # Thumbnail
        thumb = composite.copy()
        thumb.thumbnail((THUMBNAIL_SIZE, THUMBNAIL_SIZE), Image.LANCZOS)
        tbuf = io.BytesIO()
        thumb.save(tbuf, format='PNG')
        thumb_bytes = tbuf.getvalue()

    # Upload to storage
    image_path = f'{user_id}/{doc.id}.png'
    thumbnail_path = f'{user_id}/{doc.id}_thumb.png'

    bucket = client.storage.from_(STORAGE_BUCKET)
    bucket.upload(
        image_path, png_bytes,
        file_options={'content-type': 'image/png', 'upsert': 'true'}
    )
    bucket.upload(
        thumbnail_path, thumb_bytes,
        file_options={'content-type': 'image/png', 'upsert': 'true'}
    )

    # Extract unique shelfmarks (preserve order)
    shelfmarks = list(dict.fromkeys(
        f.shelfmark for f in doc.fragments if f.shelfmark
    ))

    # Upsert metadata row
    fragments_json = json.loads(doc.to_json())
    row_data = {
        'id': doc.id,
        'user_id': user_id,
        'local_doc_id': doc.id,
        'title': doc.title,
        'notes': doc.notes,
        'join_type': doc.join_type,
        'fragments_json': fragments_json,
        'shelfmarks': shelfmarks,
        'image_path': image_path,
        'thumbnail_path': thumbnail_path,
        'is_published': True,
    }
    client.table('published_joins').upsert(row_data).execute()

    # Rebuild fragment index: delete old, insert new
    client.table('published_join_fragments').delete().eq(
        'join_id', doc.id
    ).execute()

    frag_rows = [
        {'join_id': doc.id, 'sys_id': f.sys_id, 'shelfmark': f.shelfmark}
        for f in doc.fragments
    ]
    if frag_rows:
        client.table('published_join_fragments').insert(frag_rows).execute()

    return doc.id


def unpublish_join(client, user_id: str, join_id: str) -> None:
    """
    Unpublish a join: set is_published=False and remove storage files.

    Args:
        client: Authenticated Supabase client.
        user_id: The owning user's UUID.
        join_id: The published join ID to unpublish.
    """
    # Soft-delete: mark as unpublished
    client.table('published_joins').update(
        {'is_published': False}
    ).eq('id', join_id).execute()

    # Remove storage files
    bucket = client.storage.from_(STORAGE_BUCKET)
    bucket.remove([
        f'{user_id}/{join_id}.png',
        f'{user_id}/{join_id}_thumb.png',
    ])


def list_published_joins(client, limit: int = 50, offset: int = 0) -> List[Dict]:
    """
    List published joins with thumbnail URLs and author names.

    Args:
        client: Supabase client (anon or authenticated).
        limit: Maximum number of results.
        offset: Pagination offset.

    Returns:
        List of dicts with id, title, notes, shelfmarks, thumbnail_url,
        user_id, author_name, created_at.
    """
    response = (
        client.table('published_joins')
        .select('id, title, notes, shelfmarks, thumbnail_path, user_id, created_at')
        .eq('is_published', True)
        .order('created_at', desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )

    rows = response.data or []
    if not rows:
        return []

    # Resolve author display names
    user_ids = list(set(r['user_id'] for r in rows))
    profiles = resolve_author_profiles(client, user_ids)

    # Build result with thumbnail URLs
    bucket = client.storage.from_(STORAGE_BUCKET)
    result = []
    for r in rows:
        thumbnail_url = ''
        if r.get('thumbnail_path'):
            thumbnail_url = bucket.get_public_url(r['thumbnail_path'])
        result.append({
            'id': r['id'],
            'title': r['title'],
            'notes': r.get('notes', ''),
            'shelfmarks': r.get('shelfmarks', []),
            'thumbnail_url': thumbnail_url,
            'user_id': r['user_id'],
            'author_name': profiles.get(r['user_id'], 'Anonymous'),
            'created_at': r['created_at'],
        })

    return result


def get_published_joins_for_fragment(client, sys_id: str) -> List[Dict]:
    """
    Find published joins containing a given fragment (by sys_id).

    Uses batch query to avoid N+1: fetches all matching join_ids from the
    fragment index, then fetches parent rows in a single .in_() query.

    Args:
        client: Supabase client.
        sys_id: The system ID of the fragment to look up.

    Returns:
        List of dicts with id, title, shelfmarks, thumbnail_url,
        user_id, author_name, created_at.
    """
    # Step 1: Find join_ids from fragment index
    frag_response = (
        client.table('published_join_fragments')
        .select('join_id')
        .eq('sys_id', sys_id)
        .execute()
    )
    frag_rows = frag_response.data or []
    if not frag_rows:
        return []

    join_ids = list(set(r['join_id'] for r in frag_rows))

    # Step 2: Batch-fetch parent rows (no N+1)
    pj_response = (
        client.table('published_joins')
        .select('id, title, shelfmarks, thumbnail_path, user_id, created_at')
        .in_('id', join_ids)
        .eq('is_published', True)
        .execute()
    )
    parent_rows = pj_response.data or []
    if not parent_rows:
        return []

    # Resolve author profiles
    user_ids = list(set(r['user_id'] for r in parent_rows))
    profiles = resolve_author_profiles(client, user_ids)

    bucket = client.storage.from_(STORAGE_BUCKET)
    result = []
    for r in parent_rows:
        thumbnail_url = ''
        if r.get('thumbnail_path'):
            thumbnail_url = bucket.get_public_url(r['thumbnail_path'])
        result.append({
            'id': r['id'],
            'title': r['title'],
            'shelfmarks': r.get('shelfmarks', []),
            'thumbnail_url': thumbnail_url,
            'user_id': r['user_id'],
            'author_name': profiles.get(r['user_id'], 'Anonymous'),
            'created_at': r['created_at'],
        })

    return result


def get_published_join_detail(client, join_id: str) -> Optional[Dict]:
    """
    Get full detail for a single published join.

    Args:
        client: Supabase client.
        join_id: The published join ID.

    Returns:
        Dict with id, title, notes, shelfmarks, fragments_json, image_url,
        thumbnail_url, user_id, author_name, created_at. Or None if not found.
    """
    response = (
        client.table('published_joins')
        .select('*')
        .eq('id', join_id)
        .execute()
    )
    rows = response.data or []
    if not rows:
        return None

    r = rows[0]
    profiles = resolve_author_profiles(client, [r['user_id']])

    bucket = client.storage.from_(STORAGE_BUCKET)
    image_url = ''
    thumbnail_url = ''
    if r.get('image_path'):
        image_url = bucket.get_public_url(r['image_path'])
    if r.get('thumbnail_path'):
        thumbnail_url = bucket.get_public_url(r['thumbnail_path'])

    return {
        'id': r['id'],
        'title': r['title'],
        'notes': r.get('notes', ''),
        'shelfmarks': r.get('shelfmarks', []),
        'fragments_json': r.get('fragments_json', {}),
        'image_url': image_url,
        'thumbnail_url': thumbnail_url,
        'user_id': r['user_id'],
        'author_name': profiles.get(r['user_id'], 'Anonymous'),
        'created_at': r['created_at'],
    }


def fork_published_join(client, join_id: str, puzzle_service) -> Optional[str]:
    """
    Fork a published join into a local PuzzleDocument.

    Creates a new local document with a fresh UUID and "Fork of:" title prefix.
    Saves it to local joins.db via the provided puzzle_service.

    Args:
        client: Supabase client.
        join_id: The published join ID to fork.
        puzzle_service: PuzzleService instance for local persistence.

    Returns:
        The new local document ID, or None on failure.
    """
    detail = get_published_join_detail(client, join_id)
    if detail is None:
        return None

    # Reconstruct PuzzleDocument from fragments_json
    fj = detail['fragments_json']

    # fragments_json is the full doc JSON (from doc.to_json())
    # It may have 'fragments' key with the list of fragment dicts
    if isinstance(fj, dict) and 'fragments' in fj:
        frag_list = fj['fragments']
    elif isinstance(fj, list):
        frag_list = fj
    else:
        frag_list = []

    fragments = []
    for fd in frag_list:
        try:
            fragments.append(PuzzleFragment(**fd))
        except Exception as e:
            logger.warning("fork_published_join: skipping fragment: %s", e)

    new_doc = PuzzleDocument(
        id=str(uuid.uuid4()),
        title=f"Fork of: {detail['title']}",
        notes=detail.get('notes', ''),
        join_type=fj.get('join_type', 'physical') if isinstance(fj, dict) else 'physical',
        fragments=fragments,
    )

    saved_id = puzzle_service.save_document(new_doc)
    return saved_id if saved_id else new_doc.id


def resolve_author_profiles(client, user_ids: List[str]) -> Dict[str, str]:
    """
    Batch-fetch display names from the profiles table.

    Args:
        client: Supabase client.
        user_ids: List of user UUIDs to resolve.

    Returns:
        Dict mapping user_id -> full_name. Missing users get "Anonymous".
    """
    if not user_ids:
        return {}

    try:
        response = (
            client.table('profiles')
            .select('id, full_name')
            .in_('id', user_ids)
            .execute()
        )
        profiles = {r['id']: r['full_name'] for r in (response.data or [])}
    except Exception as e:
        logger.error("resolve_author_profiles failed: %s", e)
        profiles = {}

    # Fill in "Anonymous" for missing users
    return {uid: profiles.get(uid, 'Anonymous') for uid in user_ids}
