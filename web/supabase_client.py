# -*- coding: utf-8 -*-
"""
Supabase Client for GenizahSearch

This module provides the Supabase client and helper functions for:
- Authentication (sign up, sign in, sign out)
- User profile management
- Lists and items CRUD operations
- Corrections, comments, discoveries, and joins

Replaces the FastAPI backend for data operations.
"""

import logging
import threading
import time
from typing import Optional, Dict, List, Tuple
from supabase import create_client, Client
from supabase_auth.errors import AuthApiError
from shared.supabase_provider import get_url, get_anon_key

logger = logging.getLogger(__name__)

# Per-user session lock to prevent concurrent token refresh (race condition fix)
_session_locks: Dict[int, threading.Lock] = {}
_locks_guard = threading.Lock()
# Cached authenticated clients: storage_id -> (client, expiry_timestamp)
_client_cache: Dict[int, Tuple[Client, float]] = {}
_CLIENT_CACHE_TTL = 50  # seconds (Supabase access tokens last 60s)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load from centralized provider (reads from environment variables)
SUPABASE_URL = get_url()
SUPABASE_ANON_KEY = get_anon_key()

# Singleton client instance
_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL:
            raise ValueError(
                "SUPABASE_URL not set! "
                "Set it in environment variables or .env file."
            )
        if not SUPABASE_ANON_KEY:
            raise ValueError(
                "SUPABASE_ANON_KEY not set! "
                "Set it in environment variables or .env file."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client


def reset_client():
    """Reset the singleton client (e.g., after JWT expiry)."""
    global _client
    _client = None


def _is_jwt_expired(error) -> bool:
    """Check if a Supabase error is a JWT expiry."""
    msg = str(error)
    return 'JWT expired' in msg or 'PGRST303' in msg


def _clear_stale_auth(storage):
    """Clear stale auth tokens from user storage to break the retry loop.

    When a refresh token is already consumed (e.g., concurrent tab usage),
    continuing to retry with the same token produces an infinite error loop.
    Clearing the tokens forces the UI to show the user as logged out so they
    can re-authenticate with fresh credentials.
    """
    storage.pop('auth_session', None)
    storage.pop('auth_user', None)
    storage.pop('auth_profile', None)
    logger.warning("[get_user_client] Cleared stale auth — user must re-login")


def get_user_client() -> Client:
    """Get a per-user Supabase client authenticated with the current user's session tokens.

    Uses a per-session lock + short-lived cache to prevent the race condition where
    concurrent requests all try to consume the same one-time-use refresh token.
    The first request refreshes and caches; subsequent requests reuse the cached client.

    Falls back to the singleton client if no user session tokens are available.
    """
    try:
        from nicegui import app as _app
        storage = _app.storage.user
        storage_id = id(storage)

        # Fast path: return cached client if still valid
        now = time.time()
        cached = _client_cache.get(storage_id)
        if cached and cached[1] > now:
            return cached[0]

        # Get or create a lock for this user session
        with _locks_guard:
            if storage_id not in _session_locks:
                _session_locks[storage_id] = threading.Lock()
            lock = _session_locks[storage_id]

        with lock:
            # Re-check cache after acquiring lock (another thread may have refreshed)
            cached = _client_cache.get(storage_id)
            if cached and cached[1] > now:
                return cached[0]

            # Read tokens (may have been updated by a prior request that held the lock)
            auth_session = storage.get('auth_session', {})
            access_token = auth_session.get('access_token')
            refresh_token = auth_session.get('refresh_token')

            if access_token and refresh_token:
                try:
                    user_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
                    resp = user_client.auth.set_session(access_token, refresh_token)
                    # Update stored tokens — set_session may have refreshed them
                    if resp and resp.session:
                        storage['auth_session'] = {
                            'access_token': resp.session.access_token,
                            'refresh_token': resp.session.refresh_token,
                        }
                    _client_cache[storage_id] = (user_client, now + _CLIENT_CACHE_TTL)
                    return user_client
                except Exception as e:
                    logger.error(f"[get_user_client] set_session failed: {e}")
                    _client_cache.pop(storage_id, None)
                    # Clear auth only for terminal token errors (consumed/expired/invalid);
                    # transient network or Supabase outages should NOT log the user out.
                    err_msg = str(e).lower()
                    if 'refresh token' in err_msg or 'invalid' in err_msg or 'expired' in err_msg:
                        _clear_stale_auth(storage)
                    return get_client()
            else:
                logger.info("[get_user_client] No auth_session tokens in user storage — user should re-login")
                return get_client()
    except Exception as e:
        logger.error(f"[get_user_client] Error creating per-user client: {e}")
        return get_client()


# ============================================================================
# AUTHENTICATION
# ============================================================================

def sign_up(email: str, password: str, metadata: Dict = None) -> Dict:
    """
    Register a new user.

    Args:
        email: User's email
        password: User's password
        metadata: Optional user metadata (full_name, affiliation, etc.)

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        client = get_client()
        options = {'data': metadata} if metadata else {}

        response = client.auth.sign_up({
            'email': email,
            'password': password,
            'options': options if options else None
        })

        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session) if response.session else None
            }
        return {'error': 'Registration failed - no user returned'}

    except AuthApiError as e:
        return {'error': str(e)}
    except Exception as e:
        return {'error': f'Registration error: {str(e)}'}


def sign_in(email: str, password: str) -> Dict:
    """
    Sign in an existing user.

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        client = get_client()
        response = client.auth.sign_in_with_password({
            'email': email,
            'password': password
        })

        if response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Login failed'}

    except AuthApiError as e:
        return {
            'error': str(e),
            'error_code': getattr(e, 'code', ''),
            'status_code': getattr(e, 'status', ''),
        }
    except Exception as e:
        return {'error': f'Login error: {str(e)}'}


def sign_out() -> Dict:
    """Sign out the current user."""
    try:
        # Clear cached authenticated client for this session
        try:
            from nicegui import app as _app
            storage_id = id(_app.storage.user)
            _client_cache.pop(storage_id, None)
            _session_locks.pop(storage_id, None)
        except Exception:
            pass
        client = get_client()
        client.auth.sign_out()
        return {'success': True}
    except Exception as e:
        return {'error': f'Logout error: {str(e)}'}


def get_current_user() -> Optional[Dict]:
    """Get the currently authenticated user."""
    try:
        client = get_client()
        response = client.auth.get_user()
        if response and response.user:
            return _user_to_dict(response.user)
        return None
    except Exception:
        return None


def get_session() -> Optional[Dict]:
    """Get the current session."""
    try:
        client = get_client()
        session = client.auth.get_session()
        if session:
            return _session_to_dict(session)
        return None
    except Exception:
        return None


def refresh_session() -> Dict:
    """Refresh the current session."""
    try:
        client = get_client()
        response = client.auth.refresh_session()
        if response.session:
            return {
                'success': True,
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Session refresh failed'}
    except Exception as e:
        return {'error': f'Refresh error: {str(e)}'}


def get_oauth_url(provider: str = 'google', redirect_to: str = None) -> Dict:
    """
    Get OAuth URL for social login using Supabase's built-in OAuth method.

    Uses the Supabase client's sign_in_with_oauth which handles state parameter
    generation and PKCE flow automatically.

    Args:
        provider: OAuth provider ('google', 'github', etc.)
        redirect_to: URL to redirect after auth

    Returns:
        Dict with 'url' on success, or 'error' on failure
    """
    try:
        client = get_client()

        # Use Supabase's built-in OAuth method which handles state/PKCE
        options = {}
        if redirect_to:
            options['redirect_to'] = redirect_to

        response = client.auth.sign_in_with_oauth({
            'provider': provider,
            'options': options
        })

        if response and response.url:
            return {'success': True, 'url': response.url}

        return {'error': 'Failed to generate OAuth URL'}

    except Exception as e:
        return {'error': f'OAuth error: {str(e)}'}


def set_session_from_url(access_token: str, refresh_token: str) -> Dict:
    """
    Set session from OAuth callback tokens.

    Args:
        access_token: The access token from URL
        refresh_token: The refresh token from URL

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        client = get_client()
        response = client.auth.set_session(access_token, refresh_token)

        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Failed to set session'}

    except Exception as e:
        return {'error': f'Session error: {str(e)}'}


def exchange_code_for_session(code: str) -> Dict:
    """
    Exchange OAuth code for session (PKCE flow fallback).

    Args:
        code: The authorization code from URL query parameter

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        client = get_client()
        response = client.auth.exchange_code_for_session({'auth_code': code})

        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Failed to exchange code for session'}

    except Exception as e:
        return {'error': f'Code exchange error: {str(e)}'}


def _user_to_dict(user) -> Dict:
    """Convert Supabase user object to dictionary."""
    return {
        'id': str(user.id),
        'email': user.email,
        'created_at': user.created_at,
        'updated_at': user.updated_at,
        'user_metadata': user.user_metadata or {}
    }


def _session_to_dict(session) -> Dict:
    """Convert Supabase session object to dictionary."""
    if not session:
        return None
    return {
        'access_token': session.access_token,
        'refresh_token': session.refresh_token,
        'expires_at': session.expires_at,
        'token_type': session.token_type
    }


# ============================================================================
# PROFILE OPERATIONS
# ============================================================================

def get_profile(user_id: str) -> Optional[Dict]:
    """Get a user's profile."""
    try:
        client = get_client()
        response = client.table('profiles').select('*').eq('id', user_id).single().execute()
        return response.data
    except Exception:
        return None


def get_user_corrections_count(user_id: str) -> int:
    """Get count of approved corrections for a user."""
    try:
        client = get_client()
        response = client.table('corrections').select('id', count='exact').eq('author_id', user_id).eq('status', 'approved').execute()
        return response.count if response.count is not None else 0
    except Exception:
        return 0


def update_profile(user_id: str, data: Dict) -> Dict:
    """Update a user's profile."""
    try:
        client = get_user_client()
        response = client.table('profiles').update(data).eq('id', user_id).execute()
        if response.data:
            return {'success': True, 'profile': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# LISTS OPERATIONS
# ============================================================================

def get_user_lists(user_id: str, include_deleted: bool = False) -> List[Dict]:
    """Get all lists for a user.

    Args:
        user_id: The user's UUID
        include_deleted: If True, include soft-deleted lists (for trash view)
    """
    try:
        client = get_client()
        query = client.table('user_lists').select('*').eq('user_id', user_id)
        if not include_deleted:
            query = query.is_('deleted_at', 'null')
        response = query.order('created_at').execute()
        return response.data or []
    except Exception as e:
        # Fallback if deleted_at column doesn't exist yet
        if 'deleted_at' in str(e):
            try:
                client = get_client()
                response = client.table('user_lists').select('*').eq('user_id', user_id).order('created_at').execute()
                return response.data or []
            except Exception as e2:
                logger.error(f"Error getting lists (fallback): {e2}")
                return []
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_user_lists, resetting client and retrying")
            reset_client()
            try:
                return get_user_lists(user_id=user_id, include_deleted=include_deleted)
            except Exception as e2:
                logger.error(f"Error getting lists (retry): {e2}")
                return []
        logger.error(f"Error getting lists: {e}")
        return []


def get_deleted_lists(user_id: str) -> List[Dict]:
    """Get soft-deleted lists for a user (trash view)."""
    try:
        client = get_client()
        response = client.table('user_lists').select('*').eq('user_id', user_id).not_.is_('deleted_at', 'null').order('deleted_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        # Return empty if deleted_at column doesn't exist yet
        if 'deleted_at' in str(e):
            return []  # No trash feature until migration is run
        logger.error(f"Error getting deleted lists: {e}")
        return []


def create_list(user_id: str, name: str, name_en: str = None, color: str = '#FFD700',
                project_id: int = None, is_default: bool = False) -> Dict:
    """Create a new list."""
    try:
        client = get_user_client()
        data = {
            'user_id': user_id,
            'name': name,
            'name_en': name_en or name,
            'color': color,
            'is_default': is_default,
            'is_system': False
        }
        if project_id:
            data['project_id'] = project_id

        response = client.table('user_lists').insert(data).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Failed to create list'}
    except Exception as e:
        return {'error': str(e)}


def update_list(list_id: int, data: Dict) -> Dict:
    """Update a list."""
    try:
        client = get_user_client()
        response = client.table('user_lists').update(data).eq('id', list_id).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_list(list_id: int, permanent: bool = False) -> Dict:
    """Soft-delete a list (move to trash).

    Args:
        list_id: The list ID
        permanent: If True, permanently delete (no recovery). Default is soft delete.
    """
    try:
        client = get_user_client()
        if permanent:
            # Permanent delete - also deletes items via CASCADE
            client.table('user_lists').delete().eq('id', list_id).execute()
        else:
            # Soft delete - set deleted_at timestamp
            from datetime import datetime, timezone
            try:
                client.table('user_lists').update({
                    'deleted_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', list_id).execute()
            except Exception as soft_err:
                # Fallback to hard delete if deleted_at column doesn't exist
                if 'deleted_at' in str(soft_err):
                    client.table('user_lists').delete().eq('id', list_id).execute()
                else:
                    raise soft_err
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def restore_list(list_id: int) -> Dict:
    """Restore a soft-deleted list from trash."""
    try:
        client = get_user_client()
        response = client.table('user_lists').update({
            'deleted_at': None
        }).eq('id', list_id).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Restore failed'}
    except Exception as e:
        # If deleted_at column doesn't exist, trash feature is not available
        if 'deleted_at' in str(e):
            return {'error': 'Trash feature not available - run migration first'}
        return {'error': str(e)}


def empty_trash(user_id: str) -> Dict:
    """Permanently delete all soft-deleted lists for a user."""
    try:
        client = get_user_client()
        # Get all deleted lists
        deleted = get_deleted_lists(user_id)
        count = len(deleted)
        # Permanently delete each one
        for lst in deleted:
            client.table('user_lists').delete().eq('id', lst['id']).execute()
        return {'success': True, 'deleted_count': count}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# LIST ITEMS OPERATIONS
# ============================================================================

def get_list_items(list_id: int) -> List[Dict]:
    """Get all items in a list."""
    try:
        client = get_client()
        response = client.table('list_items').select('*').eq('list_id', list_id).order('added_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting list items: {e}")
        return []


def add_list_item(list_id: int, sys_id: str, shelfmark: str = None, title: str = None,
                  fl_id: str = None, note: str = '', tags: List[str] = None) -> Dict:
    """Add an item to a list."""
    try:
        client = get_user_client()
        data = {
            'list_id': list_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'title': title,
            'fl_id': fl_id,
            'note': note or '',
            'tags': tags or []
        }
        response = client.table('list_items').insert(data).execute()
        if response.data:
            return {'success': True, 'item': response.data[0]}
        return {'error': 'Failed to add item'}
    except Exception as e:
        return {'error': str(e)}


def update_list_item(item_id: int, data: Dict) -> Dict:
    """Update a list item."""
    try:
        client = get_user_client()
        response = client.table('list_items').update(data).eq('id', item_id).execute()
        if response.data:
            return {'success': True, 'item': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_list_item(item_id: int) -> Dict:
    """Delete a list item."""
    try:
        client = get_user_client()
        client.table('list_items').delete().eq('id', item_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# RECENT ITEMS OPERATIONS
# ============================================================================

def get_recent_items(user_id: str, limit: int = 50) -> List[Dict]:
    """Get recent items for a user."""
    try:
        client = get_client()
        response = client.table('recent_items').select('*').eq('user_id', user_id).order('viewed_at', desc=True).limit(limit).execute()
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting recent items: {e}")
        return []


def add_recent_item(user_id: str, sys_id: str, shelfmark: str = None,
                    title: str = None, fl_id: str = None) -> Dict:
    """Add an item to recent history."""
    try:
        client = get_user_client()

        # Remove existing entry for this sys_id (to move it to top)
        client.table('recent_items').delete().eq('user_id', user_id).eq('sys_id', sys_id).execute()

        # Add new entry
        data = {
            'user_id': user_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'title': title,
            'fl_id': fl_id
        }
        response = client.table('recent_items').insert(data).execute()

        # Cleanup: keep only last 50 items
        all_recent = client.table('recent_items').select('id').eq('user_id', user_id).order('viewed_at', desc=True).execute()
        if all_recent.data and len(all_recent.data) > 50:
            ids_to_delete = [item['id'] for item in all_recent.data[50:]]
            if ids_to_delete:
                client.table('recent_items').delete().in_('id', ids_to_delete).execute()

        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# PROJECTS OPERATIONS
# ============================================================================

def get_projects(user_id: str) -> List[Dict]:
    """Get all projects for a user."""
    try:
        client = get_client()
        response = client.table('projects').select('*').eq('user_id', user_id).order('created_at').execute()
        return response.data or []
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_projects, resetting client and retrying")
            reset_client()
            try:
                client = get_client()
                return client.table('projects').select('*').eq('user_id', user_id).order('created_at').execute().data or []
            except Exception as e2:
                logger.error(f"Error getting projects (retry): {e2}")
                return []
        logger.error(f"Error getting projects: {e}")
        return []


def create_project(user_id: str, name: str, color: str = '#4CAF50') -> Dict:
    """Create a new project."""
    try:
        client = get_user_client()
        data = {
            'user_id': user_id,
            'name': name,
            'color': color
        }
        response = client.table('projects').insert(data).execute()
        if response.data:
            return {'success': True, 'project': response.data[0]}
        return {'error': 'Failed to create project'}
    except Exception as e:
        return {'error': str(e)}


def update_project(project_id: int, data: Dict) -> Dict:
    """Update a project."""
    try:
        client = get_user_client()
        response = client.table('projects').update(data).eq('id', project_id).execute()
        if response.data:
            return {'success': True, 'project': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_project(project_id: int) -> Dict:
    """Delete a project."""
    try:
        client = get_user_client()
        client.table('projects').delete().eq('id', project_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# CORRECTIONS OPERATIONS
# ============================================================================

def get_corrections(sys_id: str = None, author_id: str = None, status: str = None, ie_id: str = None) -> List[Dict]:
    """Get corrections with optional filters, including author profile data."""
    try:
        client = get_client()
        # Fetch corrections (profile data is fetched separately below)
        query = client.table('corrections').select('*')

        if sys_id:
            query = query.eq('sys_id', sys_id)
        if author_id:
            query = query.eq('author_id', author_id)
        if status:
            query = query.eq('status', status)
        if ie_id:
            query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")

        response = query.order('created_at', desc=True).execute()
        corrections = response.data or []

        # Fetch profile data for authors
        if corrections:
            user_ids = set(c.get('author_id') for c in corrections if c.get('author_id'))
            if user_ids:
                profiles_response = client.table('profiles').select(
                    'id, full_name, username'
                ).in_('id', list(user_ids)).execute()
                profiles_map = {p['id']: p for p in (profiles_response.data or [])}

                # Merge profile data into corrections
                for c in corrections:
                    aid = c.get('author_id')
                    if aid and aid in profiles_map:
                        c['profiles'] = profiles_map[aid]
                    else:
                        c['profiles'] = {}

        return corrections
    except Exception as e:
        logger.error(f"Error getting corrections: {e}")
        return []


def create_correction(author_id: str, sys_id: str, shelfmark: str, page_number: int,
                      original_text: str, corrected_text: str, notes: str = '',
                      status: str = 'pending', ie_id: str = None) -> Dict:
    """Create a new correction."""
    try:
        client = get_user_client()
        data = {
            'author_id': author_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'page_number': page_number,
            'original_text': original_text,
            'corrected_text': corrected_text,
            'notes': notes,
            'status': status
        }
        if ie_id:
            data['ie_id'] = ie_id
        response = client.table('corrections').insert(data).execute()
        if response.data:
            return {'success': True, 'correction': response.data[0]}
        return {'error': 'Failed to create correction'}
    except Exception as e:
        return {'error': str(e)}


def update_correction(correction_id: int, data: Dict) -> Dict:
    """Update a correction."""
    try:
        client = get_user_client()
        response = client.table('corrections').update(data).eq('id', correction_id).execute()
        if response.data:
            return {'success': True, 'correction': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# COMMENTS OPERATIONS
# ============================================================================

def _enrich_with_profiles(client, rows: List[Dict], id_field: str = 'author_id') -> List[Dict]:
    """Batch-resolve user IDs to profile data and attach as 'profiles' key."""
    if not rows:
        return rows
    user_ids = set(r.get(id_field) for r in rows if r.get(id_field))
    if not user_ids:
        return rows
    try:
        profiles_response = client.table('profiles').select(
            'id, full_name, username'
        ).in_('id', list(user_ids)).execute()
        profiles_map = {p['id']: p for p in (profiles_response.data or [])}
    except Exception:
        profiles_map = {}
    for r in rows:
        aid = r.get(id_field)
        r['profiles'] = profiles_map.get(aid, {}) if aid else {}
    return rows


def get_comments(sys_id: str = None, author_id: str = None, is_public: bool = True, ie_id: str = None) -> List[Dict]:
    """Get comments with optional filters."""
    try:
        client = get_client()
        query = client.table('comments').select('*')

        if sys_id:
            query = query.eq('sys_id', sys_id)
        if author_id:
            query = query.eq('author_id', author_id)
        if is_public is not None:
            query = query.eq('is_public', is_public)
        if ie_id:
            query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")

        response = query.order('created_at', desc=True).execute()
        comments = response.data or []
        return _enrich_with_profiles(client, comments)
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_comments, resetting client and retrying")
            reset_client()
            try:
                client = get_client()
                query = client.table('comments').select('*')
                if sys_id:
                    query = query.eq('sys_id', sys_id)
                if author_id:
                    query = query.eq('author_id', author_id)
                if is_public is not None:
                    query = query.eq('is_public', is_public)
                if ie_id:
                    query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")
                comments = query.order('created_at', desc=True).execute().data or []
                return _enrich_with_profiles(client, comments)
            except Exception as e2:
                logger.error(f"Error getting comments (retry): {e2}")
                return []
        logger.error(f"Error getting comments: {e}")
        return []


def create_comment(author_id: str, sys_id: str, content: str, shelfmark: str = None,
                   page_number: int = None, scope: str = 'page', is_public: bool = True,
                   parent_id: int = None, ie_id: str = None) -> Dict:
    """Create a new comment."""
    try:
        client = get_user_client()
        data = {
            'author_id': author_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'page_number': page_number,
            'content': content,
            'scope': scope,
            'is_public': is_public,
            'parent_id': parent_id
        }
        if ie_id:
            data['ie_id'] = ie_id
        response = client.table('comments').insert(data).execute()
        if response.data:
            return {'success': True, 'comment': response.data[0]}
        return {'error': 'Failed to create comment'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERIES OPERATIONS
# ============================================================================

def get_discoveries(user_id: str = None, type: str = None, status: str = None) -> List[Dict]:
    """Get discoveries with optional filters."""
    try:
        client = get_client()
        query = client.table('discoveries').select('*')

        if user_id:
            query = query.eq('user_id', user_id)
        if type:
            query = query.eq('type', type)
        if status:
            query = query.eq('status', status)

        response = query.order('created_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting discoveries: {e}")
        return []


def create_discovery(user_id: str, title: str, content: str, type: str = 'discovery',
                     document_id: str = None, shelfmark: str = None, page_number: int = None,
                     is_anonymous: bool = False, additional_shelfmarks: List[Dict] = None,
                     related_manuscripts: List[Dict] = None) -> Dict:
    """Create a new discovery.

    Note: The database schema uses a 'shelfmarks' JSONB array field.
    The document_id, shelfmark, page_number and additional entries are
    stored in this array.
    """
    try:
        client = get_user_client()

        # Build shelfmarks array from the various inputs
        shelfmarks = []

        # Add primary shelfmark/document if provided
        if document_id or shelfmark:
            primary = {}
            if document_id:
                primary['document_id'] = document_id
            if shelfmark:
                primary['shelfmark'] = shelfmark
            if page_number is not None:
                primary['page_number'] = page_number
            if primary:
                shelfmarks.append(primary)

        # Add additional shelfmarks
        if additional_shelfmarks:
            shelfmarks.extend(additional_shelfmarks)

        # Add related manuscripts
        if related_manuscripts:
            shelfmarks.extend(related_manuscripts)

        data = {
            'user_id': user_id,
            'title': title,
            'content': content,
            'type': type,
            'is_anonymous': is_anonymous,
            'shelfmarks': shelfmarks
        }

        response = client.table('discoveries').insert(data).execute()
        if response.data:
            return {'success': True, 'discovery': response.data[0]}
        return {'error': 'Failed to create discovery'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# FRAGMENT JOINS OPERATIONS
# ============================================================================

def get_fragment_joins(user_id: str = None, fragment_sys_id: str = None,
                       status: str = None) -> List[Dict]:
    """Get fragment joins with optional filters."""
    try:
        # Use authenticated client when available — RLS only shows
        # proposed joins to their creator (status='confirmed' OR auth.uid()=user_id)
        try:
            client = get_user_client()
        except Exception:
            client = get_client()
        query = client.table('fragment_joins').select('*')

        if user_id:
            query = query.eq('user_id', user_id)
        if fragment_sys_id:
            query = query.or_(f'fragment_a_sys_id.eq.{fragment_sys_id},fragment_b_sys_id.eq.{fragment_sys_id}')
        if status:
            query = query.eq('status', status)

        response = query.order('created_at', desc=True).execute()
        rows = response.data or []

        # Batch-resolve user_ids to display names via profiles table
        uid_set = {r.get('user_id') for r in rows if r.get('user_id')}
        profiles_map = {}
        if uid_set:
            try:
                profiles_resp = client.table('profiles').select('id, full_name, username').in_('id', list(uid_set)).execute()
                for p in (profiles_resp.data or []):
                    profiles_map[p['id']] = p.get('full_name') or p.get('username') or ''
            except Exception:
                pass
        for row in rows:
            row['created_by_username'] = profiles_map.get(row.get('user_id'), '')
        return rows
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_fragment_joins, resetting client and retrying")
            reset_client()
            try:
                return get_fragment_joins(user_id=user_id, fragment_sys_id=fragment_sys_id, status=status)
            except Exception as e2:
                logger.error(f"Error getting joins (retry): {e2}")
                return []
        logger.error(f"Error getting joins: {e}")
        return []


def create_fragment_join(user_id: str, fragment_a_sys_id: str, fragment_a_shelfmark: str,
                         fragment_b_sys_id: str, fragment_b_shelfmark: str,
                         join_type: str = 'uncertain', confidence: str = 'possible',
                         notes: str = '', evidence: str = '') -> Dict:
    """Create a new fragment join."""
    try:
        client = get_user_client()
        # Map UI join type values to DB CHECK constraint values
        # DB allows: 'physical', 'content', 'uncertain'
        join_type_map = {
            'physical_join': 'physical',
            'same_composition': 'content',
        }
        db_join_type = join_type_map.get(join_type, join_type) or 'uncertain'
        data = {
            'user_id': user_id,
            'fragment_a_sys_id': fragment_a_sys_id,
            'fragment_a_shelfmark': fragment_a_shelfmark,
            'fragment_b_sys_id': fragment_b_sys_id,
            'fragment_b_shelfmark': fragment_b_shelfmark,
            'join_type': db_join_type,
            'confidence': confidence,
            'notes': notes,
            'evidence': evidence
        }
        response = client.table('fragment_joins').insert(data).execute()
        if response.data:
            return {'success': True, 'join': response.data[0]}
        return {'error': 'Failed to create join'}
    except Exception as e:
        return {'error': str(e)}


def delete_fragment_join(join_id: int) -> Dict:
    """Delete a fragment join."""
    try:
        client = get_user_client()
        client.table('fragment_joins').delete().eq('id', join_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DELETE OPERATIONS
# ============================================================================

def delete_comment(comment_id: int) -> Dict:
    """Delete a comment."""
    try:
        client = get_user_client()
        client.table('comments').delete().eq('id', comment_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def delete_correction(correction_id: int) -> Dict:
    """Delete a correction."""
    try:
        client = get_user_client()
        client.table('corrections').delete().eq('id', correction_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def delete_discovery(discovery_id: int) -> Dict:
    """Delete a discovery."""
    try:
        client = get_user_client()
        client.table('discoveries').delete().eq('id', discovery_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def update_discovery(discovery_id: int, data: Dict) -> Dict:
    """Update a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update(data).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True, 'discovery': response.data[0]}
        return {'error': 'Failed to update discovery'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERY RESPONSES
# ============================================================================

def get_discovery_responses(discovery_id: int) -> List[Dict]:
    """Get responses for a discovery."""
    try:
        client = get_client()
        response = client.table('discovery_responses').select('*').eq(
            'discovery_id', discovery_id
        ).order('created_at', desc=False).execute()
        rows = response.data or []
        return _enrich_with_profiles(client, rows, id_field='user_id')
    except Exception as e:
        logger.error(f"Error getting discovery responses: {e}")
        return []


def create_discovery_response(discovery_id: int, user_id: str, content: str,
                               is_anonymous: bool = False) -> Dict:
    """Create a response to a discovery."""
    try:
        client = get_user_client()
        data = {
            'discovery_id': discovery_id,
            'user_id': user_id,
            'content': content,
            'is_anonymous': is_anonymous
        }
        response = client.table('discovery_responses').insert(data).execute()
        if response.data:
            return {'success': True, 'response': response.data[0]}
        return {'error': 'Failed to create response'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERY VOTING AND STATUS
# ============================================================================

def vote_discovery(discovery_id: int, user_id: str, vote_type: str) -> Dict:
    """Vote on a discovery (up or down)."""
    try:
        client = get_user_client()
        # Check if user already voted
        existing = client.table('discovery_votes').select('*').eq(
            'discovery_id', discovery_id
        ).eq('user_id', user_id).execute()

        if existing.data:
            # Update existing vote
            vote = existing.data[0]
            if vote.get('vote_type') == vote_type:
                # Remove vote if same type
                client.table('discovery_votes').delete().eq('id', vote['id']).execute()
                return {'success': True, 'action': 'removed'}
            else:
                # Change vote
                client.table('discovery_votes').update({'vote_type': vote_type}).eq(
                    'id', vote['id']
                ).execute()
                return {'success': True, 'action': 'changed'}
        else:
            # Create new vote
            client.table('discovery_votes').insert({
                'discovery_id': discovery_id,
                'user_id': user_id,
                'vote_type': vote_type
            }).execute()
            return {'success': True, 'action': 'created'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_answered(discovery_id: int, answered: bool) -> Dict:
    """Toggle answered status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_answered': answered
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_pin(discovery_id: int, pinned: bool) -> Dict:
    """Toggle pinned status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_pinned': pinned
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_hidden(discovery_id: int, hidden: bool) -> Dict:
    """Toggle hidden status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_hidden': hidden
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# FEED OPERATIONS
# ============================================================================

def get_feed_items(item_type: str = None, period: str = None,
                   limit: int = 50, offset: int = 0, include_hidden: bool = False) -> Dict:
    """Get activity feed items combining discoveries, corrections, comments, and joins."""
    try:
        client = get_client()
        items = []

        # Get discoveries
        if not item_type or item_type in ('discovery', 'question', 'identification', 'note'):
            try:
                query = client.table('discoveries').select('*')
                if not include_hidden:
                    query = query.eq('is_hidden', False)
                if item_type:
                    query = query.eq('type', item_type)
                discoveries = query.order('created_at', desc=True).limit(limit).execute()
                for d in (discoveries.data or []):
                    items.append({
                        'id': f"{d.get('type', 'discovery')}_{d.get('id')}",
                        'item_type': d.get('type', 'discovery'),
                        'title': d.get('title', ''),
                        'content_preview': d.get('content', '')[:500] if d.get('content') else '',
                        'document_id': d.get('document_id'),
                        'shelfmark': d.get('shelfmark'),
                        'page_number': d.get('page_number'),
                        'created_at': d.get('created_at'),
                        'is_pinned': d.get('is_pinned', False),
                        'is_featured': d.get('is_featured', False),
                        'is_answered': d.get('is_answered', False),
                        'is_hidden': d.get('is_hidden', False),
                        'upvotes': d.get('upvotes', 0),
                        'downvotes': d.get('downvotes', 0),
                        'response_count': d.get('response_count', 0),
                        'additional_shelfmarks': d.get('additional_shelfmarks'),
                        'related_manuscripts': d.get('related_manuscripts'),
                        'author': {
                            'id': d.get('user_id'),
                            'is_anonymous': d.get('is_anonymous', False)
                        }
                    })
            except Exception as e:
                logger.error(f"Error loading discoveries: {e}")

        # Get corrections
        if not item_type or item_type == 'correction':
            try:
                corrections = client.table('corrections').select('*').eq(
                    'status', 'approved'
                ).order('created_at', desc=True).limit(limit).execute()
                for c in (corrections.data or []):
                    items.append({
                        'id': f"correction_{c.get('id')}",
                        'item_type': 'correction',
                        'title': '',
                        'original_text': c.get('original_text', ''),
                        'corrected_text': c.get('corrected_text', ''),
                        'document_id': c.get('sys_id'),
                        'shelfmark': c.get('shelfmark'),
                        'page_number': c.get('page_number'),
                        'created_at': c.get('created_at'),
                        'author': {'id': c.get('author_id')}  # corrections uses author_id
                    })
            except Exception as e:
                logger.error(f"Error loading corrections: {e}")

        # Get comments
        if not item_type or item_type == 'comment':
            try:
                comments = client.table('comments').select('*').eq(
                    'is_public', True
                ).order('created_at', desc=True).limit(limit).execute()
                for c in (comments.data or []):
                    items.append({
                        'id': f"comment_{c.get('id')}",
                        'item_type': 'comment',
                        'title': '',
                        'content_preview': c.get('content', '')[:500] if c.get('content') else '',
                        'document_id': c.get('sys_id'),
                        'shelfmark': c.get('shelfmark'),
                        'page_number': c.get('page_number'),
                        'created_at': c.get('created_at'),
                        'author': {'id': c.get('author_id')}  # comments uses author_id
                    })
            except Exception as e:
                logger.error(f"Error loading comments: {e}")

        # Get joins
        if not item_type or item_type == 'join':
            try:
                joins = client.table('fragment_joins').select('*').order(
                    'created_at', desc=True
                ).limit(limit).execute()
                for j in (joins.data or []):
                    # Normalize column names for consumers (author resolved in second pass below)
                    normalized_join = {
                        'id': j.get('id'),
                        'fragment_a': j.get('fragment_a_shelfmark', ''),
                        'fragment_b': j.get('fragment_b_shelfmark', ''),
                        'document_id_a': j.get('fragment_a_sys_id'),
                        'document_id_b': j.get('fragment_b_sys_id'),
                        'relationship_type': j.get('join_type'),
                        'notes': j.get('notes', ''),
                        'created_by_username': '',  # Resolved in second-pass profile lookup
                        'created_at': j.get('created_at'),
                    }
                    items.append({
                        'id': f"join_{j.get('id')}",
                        'item_type': 'join',
                        'title': f"{j.get('fragment_a_shelfmark', '')} ↔ {j.get('fragment_b_shelfmark', '')}",
                        'content_preview': j.get('notes', ''),
                        'document_id': j.get('fragment_a_sys_id'),
                        'shelfmark': j.get('fragment_a_shelfmark'),
                        'created_at': j.get('created_at'),
                        'cluster_fragments': [j.get('fragment_a_shelfmark'), j.get('fragment_b_shelfmark')],
                        'cluster_joins': [normalized_join],
                        'author': {'id': j.get('user_id')}
                    })
            except Exception as e:
                logger.error(f"Error loading joins: {e}")

        # Get published puzzle joins
        if not item_type or item_type == 'puzzle_join':
            try:
                pj_resp = client.table('published_joins').select(
                    'id, user_id, title, notes, shelfmarks, thumbnail_path, created_at'
                ).eq('is_published', True).order('created_at', desc=True).limit(limit).execute()
                for pj in (pj_resp.data or []):
                    thumb_url = ''
                    if pj.get('thumbnail_path'):
                        thumb_url = client.storage.from_('puzzle-images').get_public_url(pj['thumbnail_path'])
                    items.append({
                        'id': f"puzzle_join_{pj['id']}",
                        'item_type': 'puzzle_join',
                        'title': pj.get('title', ''),
                        'content_preview': pj.get('notes', '')[:500] if pj.get('notes') else '',
                        'shelfmarks': pj.get('shelfmarks', []),
                        'thumbnail_url': thumb_url,
                        'created_at': pj.get('created_at'),
                        'author': {'id': pj.get('user_id')},
                    })
            except Exception as e:
                logger.error(f"Error loading published puzzle joins: {e}")

        # Sort by created_at descending
        items.sort(key=lambda x: x.get('created_at', '') or '', reverse=True)

        # Apply offset and limit
        total = len(items)
        items = items[offset:offset + limit]

        # Fetch profile data for authors (to get full_name and username)
        try:
            user_ids = set()
            for item in items:
                author_id = item.get('author', {}).get('id')
                if author_id:
                    user_ids.add(author_id)

            if user_ids:
                profiles_response = client.table('profiles').select('id, full_name, username, affiliation').in_('id', list(user_ids)).execute()
                profiles_map = {p['id']: p for p in (profiles_response.data or [])}

                # Merge profile data into author dicts and cluster_joins
                for item in items:
                    author = item.get('author', {})
                    author_id = author.get('id')
                    if author_id and author_id in profiles_map:
                        profile = profiles_map[author_id]
                        author['full_name'] = profile.get('full_name')
                        author['username'] = profile.get('username')
                        author['affiliation'] = profile.get('affiliation')
                        # Propagate into cluster_joins for join feed items
                        display_name = profile.get('full_name') or profile.get('username') or ''
                        for cj in item.get('cluster_joins', []):
                            if isinstance(cj, dict) and not cj.get('created_by_username'):
                                cj['created_by_username'] = display_name
        except Exception as e:
            logger.error(f"Error fetching profiles for feed: {e}")

        return {'items': items, 'total': total}
    except Exception as e:
        logger.exception(f"Error getting feed items: {e}")
        return {'items': [], 'total': 0, 'error': str(e)}
