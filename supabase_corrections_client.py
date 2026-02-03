# -*- coding: utf-8 -*-
"""
Supabase-based Corrections Client for Desktop App

This module provides a drop-in replacement for corrections_client.py that uses
Supabase directly instead of the REST API backend. It maintains the same interface
for backward compatibility.

Part of Phase 5: Desktop App Supabase Migration
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime

# Import from web's supabase_client for shared Supabase connection
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'web'))

try:
    from supabase import create_client, Client
    from gotrue.errors import AuthApiError
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None
    AuthApiError = Exception

try:
    import keyring
    KEYRING_AVAILABLE = True
except ImportError:
    keyring = None
    KEYRING_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Load from environment variables or use defaults
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://ylcpglwxompwjcufdemz.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlsY3BnbHd4b21wd2pjdWZkZW16Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk3Njc0NzUsImV4cCI6MjA4NTM0MzQ3NX0.xKzlyKrBV0MxADYHqD0lyyymoVxTX91hyI4T6TGchpE')

# Try to load from .env file if not in environment
if not SUPABASE_ANON_KEY:
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('SUPABASE_ANON_KEY='):
                        SUPABASE_ANON_KEY = line.split('=', 1)[1].strip().strip('"\'')
                    elif line.startswith('SUPABASE_URL='):
                        SUPABASE_URL = line.split('=', 1)[1].strip().strip('"\'')
        except Exception as e:
            logger.warning(f"Failed to load .env file: {e}")


# ============================================================================
# DATA CLASSES (same as corrections_client.py for compatibility)
# ============================================================================

@dataclass
class User:
    """User data class"""
    id: int  # For compatibility - will store UUID as string in _uuid
    email: str
    username: str
    full_name: Optional[str] = None
    affiliation: Optional[str] = None
    role: str = "contributor"
    reputation_score: int = 0
    contribution_count: int = 0
    approved_corrections_count: int = 0
    _uuid: Optional[str] = None  # Actual Supabase UUID


@dataclass
class Correction:
    """Correction data class"""
    id: int
    document_id: str
    original_text: str
    corrected_text: str
    status: str
    correction_type: str = "text_correction"
    author_id: Optional[int] = None
    author_username: Optional[str] = None
    line_number: Optional[int] = None
    char_start: Optional[int] = None
    char_end: Optional[int] = None
    page_number: Optional[int] = None
    confidence_score: float = 0.8
    source_reference: Optional[str] = None
    notes: Optional[str] = None
    upvotes: int = 0
    downvotes: int = 0
    created_at: Optional[str] = None
    shelfmark: Optional[str] = None
    system_id: Optional[str] = None


@dataclass
class Comment:
    """Comment data class"""
    id: int
    content: str
    author_id: int
    author_username: Optional[str] = None
    document_id: Optional[str] = None
    correction_id: Optional[int] = None
    comment_type: str = "general"
    created_at: Optional[str] = None
    is_pinned: bool = False
    is_resolved: bool = False
    is_public: bool = True
    is_anonymous: bool = False
    reply_count: int = 0
    line_number: Optional[int] = None
    page_number: Optional[int] = None


@dataclass
class Discovery:
    """Discovery/Question data class"""
    id: int
    title: str
    content: str
    discovery_type: str = "discovery"
    status: str = "published"
    author_id: Optional[int] = None
    author_username: Optional[str] = None
    author_full_name: Optional[str] = None
    is_anonymous: bool = False
    document_id: Optional[str] = None
    shelfmark: Optional[str] = None
    page_number: Optional[int] = None
    additional_shelfmarks: Optional[List[Dict]] = None
    related_manuscripts: Optional[List[Dict]] = None
    is_featured: bool = False
    is_pinned: bool = False
    is_hidden: bool = False
    is_answered: bool = False
    view_count: int = 0
    upvotes: int = 0
    downvotes: int = 0
    response_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class FragmentJoin:
    """Fragment join data class"""
    id: int
    fragment_a: str
    fragment_b: str
    document_id_a: Optional[str] = None
    document_id_b: Optional[str] = None
    relationship_type: Optional[str] = None
    notes: Optional[str] = None
    source: str = "user"
    source_url: Optional[str] = None
    created_by_username: Optional[str] = None
    created_at: Optional[str] = None


@dataclass
class JoinedFragmentDetail:
    """A fragment in the connected component with relationship info"""
    shelfmark: str
    document_id: Optional[str] = None
    is_current: bool = False
    relationship_type: Optional[str] = None
    join_id: Optional[int] = None
    join_source: Optional[str] = None


@dataclass
class ConnectedFragments:
    """Connected fragments response"""
    shelfmark: str
    shelfmark_normalized: str
    fragments: List[str] = field(default_factory=list)
    fragment_details: List[JoinedFragmentDetail] = field(default_factory=list)
    joins: List[FragmentJoin] = field(default_factory=list)
    total_fragments: int = 0
    total_joins: int = 0


@dataclass
class DiscoveryResponse:
    """Response to a discovery"""
    id: int
    discovery_id: int
    content: str
    author_id: Optional[int] = None
    author_username: Optional[str] = None
    is_anonymous: bool = False
    created_at: Optional[str] = None


@dataclass
class FeedItem:
    """Activity feed item"""
    id: str
    item_type: str
    title: str
    content_preview: str
    author_username: Optional[str] = None
    author_full_name: Optional[str] = None
    is_anonymous: bool = False
    document_id: Optional[str] = None
    shelfmark: Optional[str] = None
    page_number: Optional[int] = None
    created_at: Optional[str] = None
    response_count: int = 0
    is_featured: bool = False
    is_pinned: bool = False
    is_hidden: bool = False
    is_answered: bool = False
    upvotes: int = 0
    downvotes: int = 0
    original_text: Optional[str] = None
    corrected_text: Optional[str] = None
    fragment_a: Optional[str] = None
    fragment_b: Optional[str] = None
    document_id_a: Optional[str] = None
    document_id_b: Optional[str] = None
    relationship_type: Optional[str] = None
    join_source: Optional[str] = None


# ============================================================================
# SUPABASE CORRECTIONS CLIENT
# ============================================================================

class SupabaseCorrectionsClient:
    """
    Supabase-based client for corrections, comments, discoveries, and joins.

    This is a drop-in replacement for CorrectionsClient that uses Supabase
    directly instead of the REST API backend.
    """

    def __init__(self, config_path: Path = None):
        """Initialize the Supabase corrections client."""
        self.config_path = config_path or Path.home() / '.genizah_corrections'
        self.config_path.mkdir(exist_ok=True)

        self.credentials_file = self.config_path / 'supabase_credentials.json'
        self.cache_file = self.config_path / 'community_cache.json'

        self._client: Optional[Client] = None
        self.current_user: Optional[User] = None

        # Cache
        self._cache: Dict[str, Any] = {}
        self._cache_loaded = False

        # Offline mode tracking
        self._is_offline = False
        self._last_connectivity_check = 0
        self._connectivity_check_interval = 5

        # Load saved session
        self._load_credentials()

    def _get_client(self) -> Optional[Client]:
        """Get or create Supabase client."""
        if not SUPABASE_AVAILABLE:
            logger.warning("Supabase library not available")
            return None

        if self._client is None:
            if not SUPABASE_ANON_KEY:
                logger.warning("SUPABASE_ANON_KEY not set")
                return None
            try:
                self._client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
            except Exception as e:
                logger.error(f"Failed to create Supabase client: {e}")
                return None
        return self._client

    def _load_credentials(self):
        """Load saved Supabase session from disk."""
        if self.credentials_file.exists():
            try:
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)

                # Restore session if we have tokens
                access_token = data.get('access_token')
                refresh_token = data.get('refresh_token')

                if access_token and refresh_token:
                    client = self._get_client()
                    if client:
                        try:
                            # Try to restore the session
                            client.auth.set_session(access_token, refresh_token)

                            # Load cached user data
                            cached_user = data.get('cached_user')
                            if cached_user:
                                self.current_user = User(
                                    id=cached_user.get('id', 0),
                                    email=cached_user.get('email', ''),
                                    username=cached_user.get('username', ''),
                                    full_name=cached_user.get('full_name'),
                                    affiliation=cached_user.get('affiliation'),
                                    role=cached_user.get('role', 'contributor'),
                                    reputation_score=cached_user.get('reputation_score', 0),
                                    _uuid=cached_user.get('uuid')
                                )
                        except Exception as e:
                            logger.debug(f"Could not restore session: {e}")

            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")

    def _save_credentials(self):
        """Save Supabase session to disk."""
        try:
            client = self._get_client()
            if not client:
                return

            session = client.auth.get_session()
            data = {}

            if session:
                data['access_token'] = session.access_token
                data['refresh_token'] = session.refresh_token

            if self.current_user:
                data['cached_user'] = {
                    'id': self.current_user.id,
                    'email': self.current_user.email,
                    'username': self.current_user.username,
                    'full_name': self.current_user.full_name,
                    'affiliation': self.current_user.affiliation,
                    'role': self.current_user.role,
                    'reputation_score': self.current_user.reputation_score,
                    'uuid': self.current_user._uuid
                }

            with open(self.credentials_file, 'w') as f:
                json.dump(data, f)

        except Exception as e:
            logger.warning(f"Failed to save credentials: {e}")

    # =========================================================================
    # LOGIN CREDENTIALS STORAGE (for "Remember Me" feature)
    # =========================================================================
    # Service name for keyring - used to identify the application
    KEYRING_SERVICE = "GenizahSearch"
    KEYRING_EMAIL_KEY = "saved_email"

    def save_login_credentials(self, email: str, password: str) -> bool:
        """
        Save login credentials securely using keyring.
        Email is saved to credentials file, password to system keyring.
        Returns True if successful, False otherwise.
        """
        try:
            # Save email to credentials file
            if self.credentials_file.exists():
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {}

            data['saved_email'] = email
            data['remember_me'] = True

            with open(self.credentials_file, 'w') as f:
                json.dump(data, f)

            # Save password to system keyring (secure storage)
            if KEYRING_AVAILABLE:
                keyring.set_password(self.KEYRING_SERVICE, email, password)
                logger.info(f"Login credentials saved for {email}")
                return True
            else:
                logger.warning("keyring not available, password not saved")
                return False

        except Exception as e:
            logger.warning(f"Failed to save login credentials: {e}")
            return False

    def get_saved_login_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get saved login credentials.
        Returns (email, password) tuple, or (None, None) if not saved.
        """
        try:
            if not self.credentials_file.exists():
                return None, None

            with open(self.credentials_file, 'r') as f:
                data = json.load(f)

            email = data.get('saved_email')
            remember_me = data.get('remember_me', False)

            if not email or not remember_me:
                return None, None

            # Get password from system keyring
            password = None
            if KEYRING_AVAILABLE:
                try:
                    password = keyring.get_password(self.KEYRING_SERVICE, email)
                except Exception as e:
                    logger.debug(f"Could not retrieve password from keyring: {e}")

            return email, password

        except Exception as e:
            logger.warning(f"Failed to get saved credentials: {e}")
            return None, None

    def clear_saved_login_credentials(self):
        """Clear saved login credentials."""
        try:
            # Remove from credentials file
            if self.credentials_file.exists():
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)

                email = data.pop('saved_email', None)
                data.pop('remember_me', None)

                with open(self.credentials_file, 'w') as f:
                    json.dump(data, f)

                # Remove from keyring
                if KEYRING_AVAILABLE and email:
                    try:
                        keyring.delete_password(self.KEYRING_SERVICE, email)
                    except Exception:
                        pass  # Password may not exist in keyring

            logger.info("Saved login credentials cleared")

        except Exception as e:
            logger.warning(f"Failed to clear saved credentials: {e}")

    def _load_cache(self):
        """Load cached community data from disk."""
        if self._cache_loaded:
            return
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                    self._cache_loaded = True
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
                self._cache = {}

    def _save_cache(self):
        """Save community data cache to disk."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, default=str)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def get_cached_data(self, key: str) -> Optional[Any]:
        """Get cached data if available."""
        self._load_cache()
        return self._cache.get(key)

    def set_cached_data(self, key: str, data: Any):
        """Set cache data and save to disk."""
        self._cache[key] = data
        self._save_cache()

    def clear_cache(self):
        """Clear the community data cache."""
        self._cache = {}
        if self.cache_file.exists():
            try:
                self.cache_file.unlink()
            except:
                pass

    def is_server_available(self, force_check: bool = False) -> bool:
        """Check if Supabase is reachable."""
        import time
        import socket
        from urllib.parse import urlparse

        current_time = time.time()

        if not force_check and (current_time - self._last_connectivity_check) < self._connectivity_check_interval:
            return not self._is_offline

        self._last_connectivity_check = current_time

        try:
            parsed = urlparse(SUPABASE_URL)
            host = parsed.hostname or 'supabase.co'
            port = parsed.port or 443

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            result = sock.connect_ex((host, port))
            sock.close()
            self._is_offline = (result != 0)
        except Exception:
            self._is_offline = True

        return not self._is_offline

    def reset_offline_status(self):
        """Reset offline status to force a fresh connectivity check."""
        self._last_connectivity_check = 0
        self._is_offline = False

    # ==================== Authentication ====================

    def is_logged_in(self) -> bool:
        """Check if user is logged in."""
        client = self._get_client()
        if not client:
            return self.current_user is not None
        try:
            user = client.auth.get_user()
            return user is not None and user.user is not None
        except:
            return self.current_user is not None

    def login(self, email: str, password: str) -> Tuple[bool, str]:
        """Login with email and password."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            response = client.auth.sign_in_with_password({
                'email': email,
                'password': password
            })

            if response.user:
                # Get or create profile - convert UUID to string
                self._load_user_profile(str(response.user.id))
                self._save_credentials()
                return True, "Login successful"

            return False, "Login failed"

        except AuthApiError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Login error: {str(e)}"

    def register(
        self,
        email: str,
        username: str,
        password: str,
        full_name: str = None,
        affiliation: str = None
    ) -> Tuple[bool, str]:
        """Register a new user."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            # Sign up with Supabase auth
            response = client.auth.sign_up({
                'email': email,
                'password': password,
                'options': {
                    'data': {
                        'username': username,
                        'full_name': full_name,
                        'affiliation': affiliation
                    }
                }
            })

            if response.user:
                # Update profile with additional info
                try:
                    client.table('profiles').upsert({
                        'id': str(response.user.id),
                        'username': username,
                        'full_name': full_name,
                        'affiliation': affiliation,
                        'role': 'user'
                    }).execute()
                except Exception as e:
                    logger.warning(f"Failed to update profile: {e}")

                return True, "Registration successful"

            return False, "Registration failed"

        except AuthApiError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Registration error: {str(e)}"

    def logout(self):
        """Logout and clear credentials."""
        client = self._get_client()
        if client:
            try:
                client.auth.sign_out()
            except:
                pass

        self.current_user = None
        if self.credentials_file.exists():
            try:
                self.credentials_file.unlink()
            except:
                pass

    def request_password_reset(self, email: str) -> Dict[str, Any]:
        """
        Request a password reset email.

        This is useful for users who signed up with Google OAuth and need
        to set a password for desktop app login.

        Args:
            email: User's email address

        Returns:
            Dict with 'success': True on success, or 'error': message on failure
        """
        client = self._get_client()
        if not client:
            return {'error': 'Supabase client not available'}

        try:
            # Supabase's reset_password_for_email sends a reset link
            client.auth.reset_password_for_email(email)
            return {'success': True}
        except AuthApiError as e:
            return {'error': str(e)}
        except Exception as e:
            return {'error': f'Failed to send reset email: {str(e)}'}

    def _load_user_profile(self, user_id: str):
        """Load user profile from Supabase."""
        client = self._get_client()
        if not client:
            return

        try:
            response = client.table('profiles').select('*').eq('id', user_id).single().execute()

            if response.data:
                profile = response.data
                self.current_user = User(
                    id=hash(user_id) % (10**9),  # Create int ID from UUID for compatibility
                    email=client.auth.get_user().user.email if client.auth.get_user() else '',
                    username=profile.get('username', ''),
                    full_name=profile.get('full_name'),
                    affiliation=profile.get('affiliation'),
                    role=profile.get('role', 'user'),
                    reputation_score=profile.get('reputation', 0),
                    _uuid=user_id
                )
            else:
                # Create minimal user from auth data
                auth_user = client.auth.get_user()
                if auth_user and auth_user.user:
                    self.current_user = User(
                        id=hash(user_id) % (10**9),
                        email=auth_user.user.email,
                        username=auth_user.user.email.split('@')[0],
                        _uuid=user_id
                    )

        except Exception as e:
            logger.warning(f"Failed to load profile: {e}")

    def get_current_user(self, skip_if_cached: bool = False) -> Optional[User]:
        """Get current logged-in user."""
        if skip_if_cached and self.current_user:
            return self.current_user

        client = self._get_client()
        if not client:
            return self.current_user

        try:
            auth_user = client.auth.get_user()
            if auth_user and auth_user.user:
                self._load_user_profile(str(auth_user.user.id))
                self._save_credentials()
                return self.current_user
        except:
            pass

        return self.current_user

    # ==================== Corrections ====================

    def create_correction(
        self,
        document_id: str,
        original_text: str,
        corrected_text: str,
        correction_type: str = "text_correction",
        line_number: int = None,
        char_start: int = None,
        char_end: int = None,
        page_number: int = None,
        confidence_score: float = None,
        source_reference: str = None,
        notes: str = None,
        shelfmark: str = None,
        system_id: str = None,
        context_before: str = None,
        context_after: str = None,
        status: str = None,
        save_as_draft: bool = False
    ) -> Tuple[Optional[Correction], str]:
        """Create a new correction."""
        client = self._get_client()
        if not client:
            return None, "Supabase client not available"

        if not self.current_user or not self.current_user._uuid:
            return None, "Must be logged in to create corrections"

        try:
            data = {
                'author_id': self.current_user._uuid,
                'sys_id': document_id,
                'shelfmark': shelfmark,
                'page_number': page_number,
                'original_text': original_text,
                'corrected_text': corrected_text,
                'notes': notes or '',
                'status': 'draft' if save_as_draft else (status or 'pending')
            }

            response = client.table('corrections').insert(data).execute()

            if response.data:
                return self._parse_correction(response.data[0]), "Correction created"
            return None, "Failed to create correction"

        except Exception as e:
            return None, str(e)

    def get_correction(self, correction_id: int) -> Optional[Correction]:
        """Get a specific correction."""
        client = self._get_client()
        if not client:
            return None

        try:
            response = client.table('corrections').select('*').eq('id', correction_id).single().execute()

            if response.data:
                return self._parse_correction(response.data)
            return None
        except Exception as e:
            logger.warning(f"Failed to get correction: {e}")
            return None

    def get_corrections_for_document(
        self,
        document_id: str,
        include_drafts: bool = False
    ) -> List[Correction]:
        """Get all corrections for a document."""
        client = self._get_client()
        if not client:
            return []

        try:
            query = client.table('corrections').select('*').eq('sys_id', document_id)

            if not include_drafts:
                query = query.neq('status', 'draft')

            response = query.order('created_at', desc=True).execute()
            return [self._parse_correction(c) for c in response.data or []]

        except Exception as e:
            logger.warning(f"Failed to get document corrections: {e}")
            return []

    def get_my_corrections(
        self,
        status: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Get current user's corrections."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return [], 0

        try:
            query = client.table('corrections').select('*', count='exact').eq('author_id', self.current_user._uuid)

            if status:
                query = query.eq('status', status)

            offset = (page - 1) * page_size
            response = query.order('created_at', desc=True).range(offset, offset + page_size - 1).execute()

            corrections = [self._parse_correction(c) for c in response.data or []]
            total = response.count or len(corrections)
            return corrections, total

        except Exception as e:
            logger.warning(f"Failed to get my corrections: {e}")
            return [], 0

    def get_all_corrections(
        self,
        status: str = None,
        document_id: str = None,
        search_text: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Get all corrections."""
        client = self._get_client()
        if not client:
            return [], 0

        try:
            query = client.table('corrections').select('*', count='exact')

            if status:
                query = query.eq('status', status)
            if document_id:
                query = query.eq('sys_id', document_id)
            if search_text:
                query = query.or_(
                    f'original_text.ilike.%{search_text}%,'
                    f'corrected_text.ilike.%{search_text}%,'
                    f'notes.ilike.%{search_text}%'
                )

            offset = (page - 1) * page_size
            response = query.order('created_at', desc=True).range(offset, offset + page_size - 1).execute()

            corrections = [self._parse_correction(c) for c in response.data or []]
            total = response.count or len(corrections)
            return corrections, total

        except Exception as e:
            logger.warning(f"Failed to get all corrections: {e}")
            return [], 0

    def vote_correction(self, correction_id: int, vote_value: int) -> Tuple[bool, str]:
        """Vote on a correction (+1 or -1)."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return False, "Must be logged in to vote"

        try:
            # Upsert vote
            client.table('correction_votes').upsert({
                'correction_id': correction_id,
                'user_id': self.current_user._uuid,
                'vote': vote_value
            }).execute()

            # Update correction vote counts
            votes = client.table('correction_votes').select('vote').eq('correction_id', correction_id).execute()
            upvotes = sum(1 for v in votes.data if v['vote'] > 0)
            downvotes = sum(1 for v in votes.data if v['vote'] < 0)

            client.table('corrections').update({
                'upvotes': upvotes,
                'downvotes': downvotes
            }).eq('id', correction_id).execute()

            return True, "Vote recorded"

        except Exception as e:
            return False, str(e)

    def _parse_correction(self, data: Dict) -> Correction:
        """Parse correction data into Correction object."""
        profiles = data.get('profiles', {}) or {}
        return Correction(
            id=data['id'],
            document_id=data.get('sys_id', ''),
            original_text=data.get('original_text', ''),
            corrected_text=data.get('corrected_text', ''),
            status=data.get('status', 'pending'),
            author_username=profiles.get('username'),
            page_number=data.get('page_number'),
            notes=data.get('notes'),
            upvotes=data.get('upvotes', 0),
            downvotes=data.get('downvotes', 0),
            created_at=data.get('created_at'),
            shelfmark=data.get('shelfmark'),
            system_id=data.get('sys_id')
        )

    # ==================== Comments ====================

    def create_comment(
        self,
        content: str,
        document_id: str = None,
        correction_id: int = None,
        parent_id: int = None,
        comment_type: str = "general",
        line_number: int = None,
        page_number: int = None,
        is_public: bool = True,
        is_anonymous: bool = False
    ) -> Tuple[Optional[Comment], str]:
        """Create a new comment."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return None, "Must be logged in to comment"

        try:
            data = {
                'author_id': self.current_user._uuid,
                'sys_id': document_id,
                'content': content,
                'scope': comment_type,
                'page_number': page_number,
                'is_public': is_public,
                'parent_id': parent_id
            }

            response = client.table('comments').insert(data).execute()

            if response.data:
                return self._parse_comment(response.data[0]), "Comment created"
            return None, "Failed to create comment"

        except Exception as e:
            return None, str(e)

    def get_document_comments(
        self,
        document_id: str,
        page: int = 1,
        page_size: int = 50
    ) -> List[Comment]:
        """Get comments for a document."""
        client = self._get_client()
        if not client:
            return []

        try:
            offset = (page - 1) * page_size
            response = client.table('comments').select('*').eq('sys_id', document_id).eq(
                'is_public', True
            ).order('created_at', desc=True).range(offset, offset + page_size - 1).execute()

            return [self._parse_comment(c) for c in response.data or []]

        except Exception as e:
            logger.warning(f"Failed to get comments: {e}")
            return []

    def get_comments_for_document(self, document_id: str, page: int = 1, page_size: int = 50) -> List[Comment]:
        """Alias for get_document_comments."""
        return self.get_document_comments(document_id, page, page_size)

    def get_my_comments(self, page: int = 1, page_size: int = 20) -> Tuple[List[Comment], int]:
        """Get current user's comments."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return [], 0

        try:
            offset = (page - 1) * page_size
            response = client.table('comments').select('*', count='exact').eq('author_id', self.current_user._uuid).order(
                'created_at', desc=True
            ).range(offset, offset + page_size - 1).execute()

            comments = [self._parse_comment(c) for c in response.data or []]
            total = response.count or len(comments)
            return comments, total

        except Exception as e:
            logger.warning(f"Failed to get my comments: {e}")
            return [], 0

    def _parse_comment(self, data: Dict) -> Comment:
        """Parse comment data into Comment object."""
        profiles = data.get('profiles', {}) or {}
        return Comment(
            id=data['id'],
            content=data.get('content', ''),
            author_id=hash(data.get('author_id', '')) % (10**9),
            author_username=profiles.get('username'),
            document_id=data.get('sys_id'),
            comment_type=data.get('scope', 'general'),
            created_at=data.get('created_at'),
            is_public=data.get('is_public', True),
            reply_count=data.get('reply_count', 0),
            page_number=data.get('page_number')
        )

    # ==================== Discoveries ====================

    def create_discovery(
        self,
        title: str,
        content: str,
        discovery_type: str = "discovery",
        document_id: str = None,
        page_number: int = None,
        shelfmark: str = None,
        is_anonymous: bool = False,
        additional_shelfmarks: List[Dict] = None,
        related_manuscripts: List[Dict] = None
    ) -> Tuple[Optional[Discovery], str]:
        """Create a new discovery/question."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return None, "Must be logged in to create discoveries"

        try:
            shelfmarks_list = [shelfmark] if shelfmark else []
            if additional_shelfmarks:
                for sm in additional_shelfmarks:
                    if isinstance(sm, dict) and sm.get('shelfmark'):
                        shelfmarks_list.append(sm['shelfmark'])
                    elif isinstance(sm, str):
                        shelfmarks_list.append(sm)

            data = {
                'user_id': self.current_user._uuid,
                'type': discovery_type,
                'title': title,
                'content': content,
                'shelfmarks': shelfmarks_list,
                'is_anonymous': is_anonymous
            }

            response = client.table('discoveries').insert(data).execute()

            if response.data:
                return self._parse_discovery(response.data[0]), "Discovery created"
            return None, "Failed to create discovery"

        except Exception as e:
            return None, str(e)

    def get_discovery(self, discovery_id: int) -> Optional[Discovery]:
        """Get a single discovery."""
        client = self._get_client()
        if not client:
            return None

        try:
            response = client.table('discoveries').select('*').eq('id', discovery_id).single().execute()

            if response.data:
                # Fetch profile data for the author
                user_id = response.data.get('user_id')
                if user_id:
                    try:
                        profile_response = client.table('profiles').select('id, full_name, username').eq('id', user_id).single().execute()
                        if profile_response.data:
                            response.data['profiles'] = profile_response.data
                    except Exception as e:
                        logger.warning(f"Failed to fetch profile for discovery: {e}")
                return self._parse_discovery(response.data)
            return None

        except Exception as e:
            logger.warning(f"Failed to get discovery: {e}")
            return None

    def get_discoveries(
        self,
        discovery_type: str = None,
        featured_only: bool = False,
        include_hidden: bool = False,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Discovery], int]:
        """Get list of discoveries."""
        client = self._get_client()
        if not client:
            return [], 0

        try:
            query = client.table('discoveries').select('*', count='exact')

            if discovery_type:
                query = query.eq('type', discovery_type)
            if featured_only:
                query = query.eq('status', 'featured')
            if not include_hidden:
                query = query.eq('is_hidden', False)

            offset = (page - 1) * page_size
            response = query.order('created_at', desc=True).range(offset, offset + page_size - 1).execute()

            # Fetch profile data for authors (to get full_name and username)
            discovery_data = response.data or []
            user_ids = set()
            for d in discovery_data:
                user_id = d.get('user_id')
                if user_id:
                    user_ids.add(user_id)

            profiles_map = {}
            if user_ids:
                try:
                    profiles_response = client.table('profiles').select('id, full_name, username').in_('id', list(user_ids)).execute()
                    profiles_map = {p['id']: p for p in (profiles_response.data or [])}
                except Exception as e:
                    logger.warning(f"Failed to fetch profiles for discoveries: {e}")

            # Merge profile data into discovery dicts
            for d in discovery_data:
                user_id = d.get('user_id')
                if user_id and user_id in profiles_map:
                    d['profiles'] = profiles_map[user_id]

            discoveries = [self._parse_discovery(d) for d in discovery_data]
            total = response.count or len(discoveries)
            return discoveries, total

        except Exception as e:
            logger.warning(f"Failed to get discoveries: {e}")
            return [], 0

    def vote_discovery(self, discovery_id: int, vote_type: str) -> Tuple[bool, str]:
        """Vote on a discovery ('up', 'down', or 'none')."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return False, "Must be logged in to vote"

        try:
            if vote_type == 'none':
                # Remove vote
                client.table('discovery_votes').delete().eq(
                    'discovery_id', discovery_id
                ).eq('user_id', self.current_user._uuid).execute()
            else:
                vote_value = 1 if vote_type == 'up' else -1
                client.table('discovery_votes').upsert({
                    'discovery_id': discovery_id,
                    'user_id': self.current_user._uuid,
                    'vote': vote_value
                }).execute()

            # Update discovery vote counts
            votes = client.table('discovery_votes').select('vote').eq('discovery_id', discovery_id).execute()
            upvotes = sum(1 for v in votes.data if v['vote'] > 0)
            downvotes = sum(1 for v in votes.data if v['vote'] < 0)

            client.table('discoveries').update({
                'upvotes': upvotes,
                'downvotes': downvotes
            }).eq('id', discovery_id).execute()

            return True, "Vote recorded"

        except Exception as e:
            return False, str(e)

    def _parse_discovery(self, data: Dict) -> Discovery:
        """Parse discovery data into Discovery object."""
        profiles = data.get('profiles', {}) or {}
        shelfmarks = data.get('shelfmarks', []) or []

        return Discovery(
            id=data['id'],
            title=data.get('title', ''),
            content=data.get('content', ''),
            discovery_type=data.get('type', 'discovery'),
            status=data.get('status', 'active'),
            author_username=profiles.get('username'),
            author_full_name=profiles.get('full_name'),
            is_anonymous=data.get('is_anonymous', False),
            shelfmark=shelfmarks[0] if shelfmarks else None,
            is_featured=data.get('status') == 'featured',
            is_pinned=data.get('is_pinned', False),
            is_hidden=data.get('is_hidden', False),
            is_answered=data.get('status') == 'answered',
            view_count=data.get('view_count', 0),
            upvotes=data.get('upvotes', 0),
            downvotes=data.get('downvotes', 0),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )

    # ==================== Fragment Joins ====================

    def create_join(
        self,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
        document_id_a: Optional[str] = None,
        document_id_b: Optional[str] = None
    ) -> Tuple[Optional[FragmentJoin], str]:
        """Create a join between two fragments."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return None, "Must be logged in to create joins"

        try:
            data = {
                'user_id': self.current_user._uuid,
                'fragment_a_sys_id': document_id_a or '',
                'fragment_a_shelfmark': fragment_a,
                'fragment_b_sys_id': document_id_b or '',
                'fragment_b_shelfmark': fragment_b,
                'join_type': relationship_type or 'uncertain',
                'notes': notes or '',
                'status': 'proposed'
            }

            response = client.table('fragment_joins').insert(data).execute()

            if response.data:
                return self._parse_join(response.data[0]), "Join created"
            return None, "Failed to create join"

        except Exception as e:
            return None, str(e)

    def get_connected_fragments(self, shelfmark: str, timeout: int = 30) -> Optional[ConnectedFragments]:
        """Get all fragments connected to the given shelfmark."""
        client = self._get_client()
        if not client:
            return None

        try:
            # Find all joins involving this shelfmark
            response = client.table('fragment_joins').select('*').or_(
                f'fragment_a_shelfmark.ilike.%{shelfmark}%,'
                f'fragment_b_shelfmark.ilike.%{shelfmark}%'
            ).execute()

            if not response.data:
                return ConnectedFragments(
                    shelfmark=shelfmark,
                    shelfmark_normalized=shelfmark.lower().replace(' ', ''),
                    fragments=[shelfmark],
                    total_fragments=1,
                    total_joins=0
                )

            # Build connected component
            fragments = set([shelfmark])
            joins = []

            for join_data in response.data:
                joins.append(self._parse_join(join_data))
                fragments.add(join_data.get('fragment_a_shelfmark', ''))
                fragments.add(join_data.get('fragment_b_shelfmark', ''))

            fragments.discard('')
            fragment_list = sorted(list(fragments))

            fragment_details = [
                JoinedFragmentDetail(
                    shelfmark=f,
                    is_current=(f.lower() == shelfmark.lower())
                ) for f in fragment_list
            ]

            return ConnectedFragments(
                shelfmark=shelfmark,
                shelfmark_normalized=shelfmark.lower().replace(' ', ''),
                fragments=fragment_list,
                fragment_details=fragment_details,
                joins=joins,
                total_fragments=len(fragment_list),
                total_joins=len(joins)
            )

        except Exception as e:
            logger.warning(f"Failed to get connected fragments: {e}")
            return None

    def get_connected_fragments_quick(self, shelfmark: str) -> Optional[ConnectedFragments]:
        """Get connected fragments with a short timeout."""
        return self.get_connected_fragments(shelfmark, timeout=3)

    def get_connected_fragments_by_id(self, document_id: str, timeout: int = 30) -> Optional[ConnectedFragments]:
        """Get all fragments connected to the given document_id."""
        client = self._get_client()
        if not client:
            return None

        try:
            response = client.table('fragment_joins').select('*').or_(
                f'fragment_a_sys_id.eq.{document_id},'
                f'fragment_b_sys_id.eq.{document_id}'
            ).execute()

            if not response.data:
                return None

            fragments = set()
            joins = []

            for join_data in response.data:
                joins.append(self._parse_join(join_data))
                fragments.add(join_data.get('fragment_a_shelfmark', ''))
                fragments.add(join_data.get('fragment_b_shelfmark', ''))

            fragments.discard('')
            fragment_list = sorted(list(fragments))

            return ConnectedFragments(
                shelfmark=fragment_list[0] if fragment_list else '',
                shelfmark_normalized='',
                fragments=fragment_list,
                joins=joins,
                total_fragments=len(fragment_list),
                total_joins=len(joins)
            )

        except Exception as e:
            logger.warning(f"Failed to get connected fragments by id: {e}")
            return None

    def search_joins(
        self,
        query: Optional[str] = None,
        source: Optional[str] = None,
        relationship_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[FragmentJoin], int]:
        """Search joins by shelfmark pattern or filters. Returns (joins, total)."""
        client = self._get_client()
        if not client:
            return [], 0

        try:
            q = client.table('fragment_joins').select('*', count='exact')

            if query:
                q = q.or_(
                    f'fragment_a_shelfmark.ilike.%{query}%,'
                    f'fragment_b_shelfmark.ilike.%{query}%'
                )
            if relationship_type:
                q = q.eq('join_type', relationship_type)

            response = q.order('created_at', desc=True).range(offset, offset + limit - 1).execute()

            joins = [self._parse_join(j) for j in response.data or []]
            total = response.count or len(joins)
            return joins, total

        except Exception as e:
            logger.warning(f"Failed to search joins: {e}")
            return [], 0

    def get_my_joins(
        self,
        query: Optional[str] = None,
        relationship_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[FragmentJoin], int]:
        """Get current user's joins. Returns (joins, total)."""
        client = self._get_client()
        if not client or not self.current_user or not self.current_user._uuid:
            return [], 0

        try:
            q = client.table('fragment_joins').select('*', count='exact').eq('user_id', self.current_user._uuid)

            if query:
                q = q.or_(
                    f'fragment_a_shelfmark.ilike.%{query}%,'
                    f'fragment_b_shelfmark.ilike.%{query}%'
                )
            if relationship_type:
                q = q.eq('join_type', relationship_type)

            response = q.order('created_at', desc=True).range(offset, offset + limit - 1).execute()

            joins = [self._parse_join(j) for j in response.data or []]
            total = response.count or len(joins)
            return joins, total

        except Exception as e:
            logger.warning(f"Failed to get my joins: {e}")
            return [], 0

    def _parse_join(self, data: Dict) -> FragmentJoin:
        """Parse join data into FragmentJoin object."""
        profiles = data.get('profiles', {}) or {}
        return FragmentJoin(
            id=data['id'],
            fragment_a=data.get('fragment_a_shelfmark', ''),
            fragment_b=data.get('fragment_b_shelfmark', ''),
            document_id_a=data.get('fragment_a_sys_id'),
            document_id_b=data.get('fragment_b_sys_id'),
            relationship_type=data.get('join_type'),
            notes=data.get('notes'),
            source='user',
            created_by_username=profiles.get('username'),
            created_at=data.get('created_at')
        )

    # ==================== Feed ====================

    def get_feed(
        self,
        item_type: str = None,
        period: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[FeedItem], int]:
        """Get activity feed - combines discoveries, corrections, comments, joins."""
        client = self._get_client()
        if not client:
            return [], 0

        try:
            # For now, just return discoveries as feed items
            discoveries, total = self.get_discoveries(
                discovery_type=item_type if item_type != 'all' else None,
                page=page,
                page_size=page_size
            )

            feed_items = []
            for d in discoveries:
                feed_items.append(FeedItem(
                    id=f"discovery_{d.id}",
                    item_type=d.discovery_type,
                    title=d.title,
                    content_preview=d.content[:200] if d.content else '',
                    author_username=d.author_username,
                    author_full_name=d.author_full_name,
                    is_anonymous=d.is_anonymous,
                    shelfmark=d.shelfmark,
                    created_at=d.created_at,
                    response_count=d.response_count,
                    is_featured=d.is_featured,
                    is_pinned=d.is_pinned,
                    is_answered=d.is_answered,
                    upvotes=d.upvotes,
                    downvotes=d.downvotes
                ))

            return feed_items, total

        except Exception as e:
            logger.warning(f"Failed to get feed: {e}")
            return [], 0

    # ==================== Stats ====================

    def get_correction_stats(self) -> Dict[str, int]:
        """Get correction statistics."""
        client = self._get_client()
        if not client:
            return {}

        try:
            response = client.table('corrections').select('status', count='exact').execute()
            # Count by status
            stats = {'total': len(response.data or [])}
            for item in response.data or []:
                status = item.get('status', 'unknown')
                stats[status] = stats.get(status, 0) + 1
            return stats
        except:
            return {}

    def get_discovery_stats(self) -> Dict:
        """Get discovery statistics."""
        client = self._get_client()
        if not client:
            return {}

        try:
            response = client.table('discoveries').select('type', count='exact').execute()
            stats = {'total': len(response.data or [])}
            for item in response.data or []:
                dtype = item.get('type', 'discovery')
                stats[dtype] = stats.get(dtype, 0) + 1
            return stats
        except:
            return {}

    # ==================== Additional Methods ====================

    def submit_correction(self, correction_id: int, notes: str = None) -> Tuple[bool, str]:
        """Submit a draft correction for review."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            update_data = {'status': 'pending'}
            if notes:
                update_data['notes'] = notes
            client.table('corrections').update(update_data).eq('id', correction_id).execute()
            return True, "Correction submitted for review"
        except Exception as e:
            return False, str(e)

    def get_all_comments(self, page: int = 1, page_size: int = 20) -> Tuple[List[Comment], int]:
        """Get all public comments."""
        client = self._get_client()
        if not client:
            return [], 0

        try:
            offset = (page - 1) * page_size
            response = client.table('comments').select('*', count='exact').eq(
                'is_public', True
            ).order('created_at', desc=True).range(offset, offset + page_size - 1).execute()

            comments = [self._parse_comment(c) for c in response.data or []]
            total = response.count or len(comments)
            return comments, total
        except Exception as e:
            logger.warning(f"Failed to get all comments: {e}")
            return [], 0

    def update_discovery(
        self,
        discovery_id: int,
        title: str = None,
        content: str = None,
        document_id: str = None,
        page_number: int = None,
        shelfmark: str = None,
        is_anonymous: bool = None
    ) -> Tuple[bool, str]:
        """Update a discovery."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            update_data = {}
            if title is not None:
                update_data['title'] = title
            if content is not None:
                update_data['content'] = content
            if shelfmark is not None:
                update_data['shelfmarks'] = [shelfmark]
            if is_anonymous is not None:
                update_data['is_anonymous'] = is_anonymous

            if update_data:
                client.table('discoveries').update(update_data).eq('id', discovery_id).execute()
            return True, "Discovery updated"
        except Exception as e:
            return False, str(e)

    def delete_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Delete (hide) a discovery."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            client.table('discoveries').update({'is_hidden': True}).eq('id', discovery_id).execute()
            return True, "Discovery deleted"
        except Exception as e:
            return False, str(e)

    def add_discovery_response(
        self,
        discovery_id: int,
        content: str,
        is_anonymous: bool = False
    ) -> Tuple[Optional[DiscoveryResponse], str]:
        """Add a response to a discovery (as a comment)."""
        # Discoveries don't have a separate responses table in Supabase schema
        # Use comments with parent_id relationship
        return None, "Discovery responses not implemented in Supabase schema"

    def get_discovery_responses(self, discovery_id: int) -> List[DiscoveryResponse]:
        """Get responses for a discovery."""
        # Not implemented in current Supabase schema
        return []

    def mark_discovery_answered(self, discovery_id: int, answered: bool = True) -> Tuple[bool, str]:
        """Mark a question as answered."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            status = 'answered' if answered else 'active'
            client.table('discoveries').update({'status': status}).eq('id', discovery_id).execute()
            return True, "Marked as answered" if answered else "Marked as unanswered"
        except Exception as e:
            return False, str(e)

    def get_pending_corrections(self, page: int = 1, page_size: int = 20) -> Tuple[List[Correction], int]:
        """Get pending corrections for review."""
        return self.get_all_corrections(status='pending', page=page, page_size=page_size)

    def review_correction(
        self,
        correction_id: int,
        action: str,
        review_notes: str = None
    ) -> Tuple[bool, str]:
        """Review a correction."""
        client = self._get_client()
        if not client or not self.current_user:
            return False, "Must be logged in to review"

        try:
            status_map = {
                'approve': 'approved',
                'reject': 'rejected',
                'request_revision': 'draft'
            }
            new_status = status_map.get(action)
            if not new_status:
                return False, f"Invalid action: {action}"

            update_data = {
                'status': new_status,
                'reviewed_by': self.current_user._uuid,
                'reviewed_at': datetime.utcnow().isoformat()
            }
            if review_notes:
                update_data['rejection_reason'] = review_notes

            client.table('corrections').update(update_data).eq('id', correction_id).execute()
            return True, f"Correction {action}d"
        except Exception as e:
            return False, str(e)

    def get_document_stats(self, document_id: str) -> Dict:
        """Get statistics for a document."""
        client = self._get_client()
        if not client:
            return {}

        try:
            corrections = client.table('corrections').select('id', count='exact').eq('sys_id', document_id).execute()
            comments = client.table('comments').select('id', count='exact').eq('sys_id', document_id).execute()

            return {
                'corrections_count': corrections.count or 0,
                'comments_count': comments.count or 0
            }
        except:
            return {}

    def get_corrected_text(self, document_id: str, original_text: str) -> str:
        """Apply all approved corrections to document text."""
        # This would need the original corrections to be applied
        # For now, return original text
        return original_text

    def get_page_versions(self, document_id: str, page_num: int = 1) -> Dict:
        """Fetch all versions for a document page."""
        # Versions table not in current Supabase schema
        return {'all_versions': [], 'current_default': None, 'total': 0}

    def get_version_content(self, version_id: int) -> Dict:
        """Fetch content for a specific version."""
        return {}

    def record_document_view(self, document_id: str):
        """Record a document view for statistics."""
        pass  # Not tracking views in current schema

    def get_leaderboard(self, limit: int = 10) -> List[User]:
        """Get top users by reputation."""
        client = self._get_client()
        if not client:
            return []

        try:
            response = client.table('profiles').select('*').order(
                'reputation', desc=True
            ).limit(limit).execute()

            return [User(
                id=hash(u.get('id', '')) % (10**9),
                email='',
                username=u.get('username', ''),
                full_name=u.get('full_name'),
                role=u.get('role', 'user'),
                reputation_score=u.get('reputation', 0),
                _uuid=u.get('id')
            ) for u in response.data or []]
        except:
            return []

    def get_join_by_id(self, join_id: int) -> Optional[FragmentJoin]:
        """Get a specific join by ID."""
        client = self._get_client()
        if not client:
            return None

        try:
            response = client.table('fragment_joins').select('*').eq('id', join_id).single().execute()
            if response.data:
                return self._parse_join(response.data)
            return None
        except:
            return None

    def delete_join(self, join_id: int) -> Tuple[bool, str]:
        """Delete a join."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            client.table('fragment_joins').delete().eq('id', join_id).execute()
            return True, "Join deleted"
        except Exception as e:
            return False, str(e)

    def update_join(
        self,
        join_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Tuple[Optional[FragmentJoin], str]:
        """Update a join's metadata."""
        client = self._get_client()
        if not client:
            return None, "Supabase client not available"

        try:
            update_data = {}
            if relationship_type is not None:
                update_data['join_type'] = relationship_type
            if notes is not None:
                update_data['notes'] = notes

            if update_data:
                response = client.table('fragment_joins').update(update_data).eq('id', join_id).execute()
                if response.data:
                    return self._parse_join(response.data[0]), "Join updated"
            return None, "No changes made"
        except Exception as e:
            return None, str(e)

    def react_to_comment(self, comment_id: int, reaction_type: str) -> Tuple[bool, str]:
        """Add/toggle reaction to a comment."""
        # Reactions not implemented in current schema
        return False, "Comment reactions not implemented"

    def pin_discovery(self, discovery_id: int, pinned: bool = True) -> Tuple[bool, str]:
        """Pin or unpin a discovery."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            client.table('discoveries').update({'is_pinned': pinned}).eq('id', discovery_id).execute()
            return True, "Discovery pinned" if pinned else "Discovery unpinned"
        except Exception as e:
            return False, str(e)

    def hide_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Hide a discovery."""
        return self.delete_discovery(discovery_id)

    def unhide_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Unhide a discovery."""
        client = self._get_client()
        if not client:
            return False, "Supabase client not available"

        try:
            client.table('discoveries').update({'is_hidden': False}).eq('id', discovery_id).execute()
            return True, "Discovery unhidden"
        except Exception as e:
            return False, str(e)


# ============================================================================
# SINGLETON
# ============================================================================

_supabase_client_instance: Optional[SupabaseCorrectionsClient] = None


def get_supabase_corrections_client() -> SupabaseCorrectionsClient:
    """Get or create the Supabase corrections client singleton."""
    global _supabase_client_instance
    if _supabase_client_instance is None:
        _supabase_client_instance = SupabaseCorrectionsClient()
    return _supabase_client_instance
