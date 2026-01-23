"""
Corrections API Client - For Desktop App Integration
Handles communication with the Genizah Corrections backend API
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class User:
    """User data class"""
    id: int
    email: str
    username: str
    full_name: Optional[str] = None
    affiliation: Optional[str] = None
    role: str = "contributor"
    reputation_score: int = 0
    contribution_count: int = 0
    approved_corrections_count: int = 0


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
    line_number: Optional[int] = None  # Line within text (for inline corrections)
    char_start: Optional[int] = None
    char_end: Optional[int] = None
    page_number: Optional[int] = None  # Page/image number in manuscript
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
    line_number: Optional[int] = None  # Line within text (for inline comments)
    page_number: Optional[int] = None  # Page/image number in manuscript


@dataclass
class Discovery:
    """Discovery/Question data class"""
    id: int
    title: str
    content: str
    discovery_type: str = "discovery"  # discovery, question, identification, note
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
    document_id_a: Optional[str] = None  # sys_id for fragment A
    document_id_b: Optional[str] = None  # sys_id for fragment B
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
    item_type: str  # discovery, question, correction, comment, join
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
    # For corrections
    original_text: Optional[str] = None
    corrected_text: Optional[str] = None
    # For joins
    fragment_a: Optional[str] = None
    fragment_b: Optional[str] = None
    document_id_a: Optional[str] = None
    document_id_b: Optional[str] = None
    relationship_type: Optional[str] = None
    join_source: Optional[str] = None


class CorrectionsClient:
    """
    Client for the Genizah Corrections API.
    Used by the desktop application to interact with the corrections system.
    """

    def __init__(self, base_url: str = None, config_path: Path = None):
        """
        Initialize the corrections client.

        Args:
            base_url: API base URL (default: https://genizahsearch.com/api/v1)
            config_path: Path to store credentials
        """
        self.base_url = base_url or os.environ.get(
            'CORRECTIONS_API_URL',
            'https://genizahsearch.com/api/v1'
        )
        self.config_path = config_path or Path.home() / '.genizah_corrections'
        self.config_path.mkdir(exist_ok=True)

        self.credentials_file = self.config_path / 'credentials.json'
        self.cache_file = self.config_path / 'community_cache.json'
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.current_user: Optional[User] = None

        # Community data cache (refreshed on startup)
        self._cache: Dict[str, Any] = {}
        self._cache_loaded = False

        # Offline mode tracking
        self._is_offline = False
        self._last_connectivity_check = 0
        self._connectivity_check_interval = 5  # seconds between checks (reduced for faster offline detection)

        # Setup session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        # Load saved credentials
        self._load_credentials()

    def _load_credentials(self):
        """Load saved credentials from disk (no network calls at startup)"""
        if self.credentials_file.exists():
            try:
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)
                    self.access_token = data.get('access_token')
                    self.refresh_token = data.get('refresh_token')
                    # Load cached user data if available (no network call)
                    cached_user = data.get('cached_user')
                    if cached_user and self.access_token:
                        self.current_user = User(
                            id=cached_user.get('id', 0),
                            email=cached_user.get('email', ''),
                            username=cached_user.get('username', ''),
                            full_name=cached_user.get('full_name'),
                            affiliation=cached_user.get('affiliation'),
                            role=cached_user.get('role', 'contributor'),
                            reputation_score=cached_user.get('reputation_score', 0),
                            contribution_count=cached_user.get('contribution_count', 0),
                            approved_corrections_count=cached_user.get('approved_corrections_count', 0)
                        )
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")

    def _save_credentials(self):
        """Save credentials and cached user data to disk"""
        try:
            data = {
                'access_token': self.access_token,
                'refresh_token': self.refresh_token
            }
            # Cache user data for offline startup
            if self.current_user:
                data['cached_user'] = {
                    'id': self.current_user.id,
                    'email': self.current_user.email,
                    'username': self.current_user.username,
                    'full_name': self.current_user.full_name,
                    'affiliation': self.current_user.affiliation,
                    'role': self.current_user.role,
                    'reputation_score': self.current_user.reputation_score,
                    'contribution_count': self.current_user.contribution_count,
                    'approved_corrections_count': self.current_user.approved_corrections_count
                }
            with open(self.credentials_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to save credentials: {e}")

    def _load_cache(self):
        """Load cached community data from disk"""
        if self._cache_loaded:
            return
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                    self._cache_loaded = True
                    logger.info("Loaded community cache from disk")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
                self._cache = {}

    def _save_cache(self):
        """Save community data cache to disk"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, default=str)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def get_cached_data(self, key: str) -> Optional[Any]:
        """Get cached data if available"""
        self._load_cache()
        return self._cache.get(key)

    def set_cached_data(self, key: str, data: Any):
        """Set cache data and save to disk"""
        self._cache[key] = data
        self._save_cache()

    def clear_cache(self):
        """Clear the community data cache"""
        self._cache = {}
        if self.cache_file.exists():
            try:
                self.cache_file.unlink()
            except:
                pass

    def is_server_available(self, force_check: bool = False) -> bool:
        """
        Quick check if the corrections server is available.

        Uses a fast socket connection test (not HTTP) for near-instant failure detection.
        Uses a cached result to avoid repeated checks.
        Set force_check=True to bypass the cache.

        Returns:
            True if server is reachable, False otherwise
        """
        import time
        import socket
        from urllib.parse import urlparse

        current_time = time.time()

        # Return cached result if within check interval (unless force_check)
        if not force_check and (current_time - self._last_connectivity_check) < self._connectivity_check_interval:
            return not self._is_offline

        self._last_connectivity_check = current_time

        # Parse host and port from base_url
        try:
            parsed = urlparse(self.base_url)
            host = parsed.hostname or 'localhost'
            port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        except Exception:
            host, port = 'localhost', 8000

        # Fast socket connection test - fails almost instantly if no server
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)  # 500ms timeout - very fast
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

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        headers = {'Content-Type': 'application/json'}
        if self.access_token:
            headers['Authorization'] = f'Bearer {self.access_token}'
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Dict = None,
        params: Dict = None,
        timeout: int = 30
    ) -> Dict:
        """
        Make an API request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request body data
            params: Query parameters
            timeout: Request timeout in seconds (default 30)

        Returns:
            Response JSON data

        Raises:
            Exception: On API error
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        try:
            response = self.session.request(
                method,
                url,
                headers=headers,
                json=data,
                params=params,
                timeout=timeout
            )

            # Handle token refresh
            if response.status_code == 401 and self.refresh_token:
                if self._refresh_access_token():
                    headers = self._get_headers()
                    response = self.session.request(
                        method,
                        url,
                        headers=headers,
                        json=data,
                        params=params,
                        timeout=30
                    )

            if response.status_code >= 400:
                error_data = response.json()
                raise Exception(error_data.get('detail', 'Request failed'))

            return response.json()

        except requests.exceptions.ConnectionError:
            raise Exception("Could not connect to corrections server")
        except requests.exceptions.Timeout:
            raise Exception("Request timed out")

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token"""
        try:
            response = self.session.post(
                f"{self.base_url}/auth/refresh",
                json={'refresh_token': self.refresh_token},
                timeout=10
            )

            if response.ok:
                data = response.json()
                self.access_token = data['access_token']
                self.refresh_token = data.get('refresh_token', self.refresh_token)
                self._save_credentials()
                return True

        except Exception as e:
            logger.warning(f"Token refresh failed: {e}")

        self.logout()
        return False

    # ==================== Authentication ====================

    def is_logged_in(self) -> bool:
        """Check if user is logged in"""
        return self.access_token is not None

    def login(self, email: str, password: str) -> Tuple[bool, str]:
        """
        Login with email and password.

        Returns:
            Tuple of (success, message)
        """
        try:
            response = self.session.post(
                f"{self.base_url}/auth/login",
                json={'email': email, 'password': password},
                timeout=10
            )

            if response.ok:
                data = response.json()
                self.access_token = data['access_token']
                self.refresh_token = data.get('refresh_token')
                # get_current_user will also save credentials with user data
                self.get_current_user()
                return True, "Login successful"
            else:
                error = response.json()
                return False, error.get('detail', 'Login failed')

        except Exception as e:
            return False, str(e)

    def register(
        self,
        email: str,
        username: str,
        password: str,
        full_name: str = None,
        affiliation: str = None
    ) -> Tuple[bool, str]:
        """
        Register a new user.

        Returns:
            Tuple of (success, message)
        """
        try:
            response = self.session.post(
                f"{self.base_url}/auth/register",
                json={
                    'email': email,
                    'username': username,
                    'password': password,
                    'confirm_password': password,
                    'full_name': full_name,
                    'affiliation': affiliation
                },
                timeout=10
            )

            if response.ok:
                return True, "Registration successful"
            else:
                error = response.json()
                return False, error.get('detail', 'Registration failed')

        except Exception as e:
            return False, str(e)

    def logout(self):
        """Logout and clear credentials"""
        self.access_token = None
        self.refresh_token = None
        self.current_user = None
        if self.credentials_file.exists():
            self.credentials_file.unlink()

    def get_current_user(self, skip_if_cached: bool = False) -> Optional[User]:
        """Get current logged-in user.

        Args:
            skip_if_cached: If True, return cached user without network call
        """
        if not self.access_token:
            return None

        # Return cached user if requested and available
        if skip_if_cached and self.current_user:
            return self.current_user

        try:
            data = self._request('GET', '/auth/me')
            self.current_user = User(
                id=data['id'],
                email=data['email'],
                username=data['username'],
                full_name=data.get('full_name'),
                affiliation=data.get('affiliation'),
                role=data.get('role', 'contributor'),
                reputation_score=data.get('reputation_score', 0),
                contribution_count=data.get('contribution_count', 0),
                approved_corrections_count=data.get('approved_corrections_count', 0)
            )
            # Cache user data for offline use
            self._save_credentials()
            return self.current_user
        except Exception as e:
            logger.warning(f"Failed to get current user: {e}")
            return self.current_user  # Return cached user if available

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
        """
        Create a new correction.

        Returns:
            Tuple of (Correction or None, message)
        """
        try:
            payload = {
                'document_id': document_id,
                'original_text': original_text,
                'corrected_text': corrected_text,
                'correction_type': correction_type,
            }
            # Only add optional fields if provided
            if line_number is not None:
                payload['line_number'] = line_number
            if char_start is not None:
                payload['char_start'] = char_start
            if char_end is not None:
                payload['char_end'] = char_end
            if page_number is not None:
                payload['page_number'] = page_number
            if confidence_score is not None:
                payload['confidence_score'] = confidence_score
            if source_reference:
                payload['source_reference'] = source_reference
            if notes:
                payload['notes'] = notes
            if shelfmark:
                payload['shelfmark'] = shelfmark
            if system_id:
                payload['system_id'] = system_id
            if context_before:
                payload['context_before'] = context_before
            if context_after:
                payload['context_after'] = context_after
            if status:
                payload['status'] = status

            # Pass save_as_draft as query parameter
            params = {'save_as_draft': str(save_as_draft).lower()} if save_as_draft else None
            data = self._request('POST', '/corrections/', payload, params=params)
            return self._parse_correction(data), "Correction created"
        except Exception as e:
            return None, str(e)

    def get_correction(self, correction_id: int) -> Optional[Correction]:
        """Get a specific correction"""
        try:
            data = self._request('GET', f'/corrections/{correction_id}')
            return self._parse_correction(data)
        except Exception as e:
            logger.warning(f"Failed to get correction: {e}")
            return None

    def submit_correction(self, correction_id: int, notes: str = None) -> Tuple[bool, str]:
        """Submit a draft correction for review"""
        try:
            self._request('POST', f'/corrections/{correction_id}/submit', {'notes': notes})
            return True, "Correction submitted for review"
        except Exception as e:
            return False, str(e)

    def vote_correction(self, correction_id: int, vote_value: int) -> Tuple[bool, str]:
        """Vote on a correction (+1 or -1)"""
        try:
            self._request('POST', f'/corrections/{correction_id}/vote', {'vote_value': vote_value})
            return True, "Vote recorded"
        except Exception as e:
            return False, str(e)

    def get_corrections_for_document(
        self,
        document_id: str,
        include_drafts: bool = False
    ) -> List[Correction]:
        """Get all approved corrections for a document"""
        try:
            data = self._request(
                'GET',
                f'/corrections/document/{document_id}',
                params={'include_drafts': str(include_drafts).lower()}
            )
            return [self._parse_correction(c) for c in data]
        except Exception as e:
            logger.warning(f"Failed to get document corrections: {e}")
            return []

    def get_my_corrections(
        self,
        status: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """
        Get current user's corrections.

        Returns:
            Tuple of (list of corrections, total count)
        """
        try:
            params = {'page': page, 'page_size': page_size}
            if status:
                params['status'] = status
            data = self._request('GET', '/corrections/my', params=params)
            corrections = [self._parse_correction(c) for c in data.get('items', [])]
            return corrections, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get my corrections: {e}")
            return [], 0

    def search_corrections(
        self,
        document_id: str = None,
        status: str = None,
        search_text: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Search corrections with filters"""
        try:
            params = {'page': page, 'page_size': page_size}
            if document_id:
                params['document_id'] = document_id
            if status:
                params['status'] = status
            if search_text:
                params['search_text'] = search_text

            data = self._request('GET', '/corrections/', params=params)
            corrections = [self._parse_correction(c) for c in data.get('items', [])]
            return corrections, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to search corrections: {e}")
            return [], 0

    def get_correction_stats(self) -> Dict[str, int]:
        """Get correction statistics"""
        try:
            return self._request('GET', '/corrections/stats')
        except Exception as e:
            logger.warning(f"Failed to get stats: {e}")
            return {}

    def _parse_correction(self, data: Dict) -> Correction:
        """Parse correction data into Correction object"""
        author = data.get('author', {})
        return Correction(
            id=data['id'],
            document_id=data['document_id'],
            original_text=data['original_text'],
            corrected_text=data['corrected_text'],
            status=data['status'],
            correction_type=data.get('correction_type', 'text_correction'),
            author_id=data.get('author_id'),
            author_username=author.get('username') if author else None,
            line_number=data.get('line_number'),
            char_start=data.get('char_start'),
            char_end=data.get('char_end'),
            page_number=data.get('page_number'),
            confidence_score=data.get('confidence_score', 0.8),
            source_reference=data.get('source_reference'),
            notes=data.get('notes'),
            upvotes=data.get('upvotes', 0),
            downvotes=data.get('downvotes', 0),
            created_at=data.get('created_at'),
            shelfmark=data.get('shelfmark'),
            system_id=data.get('system_id')
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
        """Create a new comment"""
        try:
            data = self._request('POST', '/comments/', {
                'content': content,
                'document_id': document_id,
                'correction_id': correction_id,
                'parent_id': parent_id,
                'comment_type': comment_type,
                'line_number': line_number,
                'page_number': page_number,
                'is_public': is_public,
                'is_anonymous': is_anonymous
            })
            return self._parse_comment(data), "Comment created"
        except Exception as e:
            return None, str(e)

    def get_document_comments(
        self,
        document_id: str,
        page: int = 1,
        page_size: int = 50
    ) -> List[Comment]:
        """Get comments for a document"""
        try:
            data = self._request(
                'GET',
                f'/comments/document/{document_id}',
                params={'page': page, 'page_size': page_size}
            )
            return [self._parse_comment(c) for c in data.get('items', [])]
        except Exception as e:
            logger.warning(f"Failed to get comments: {e}")
            return []

    # Alias for consistency with UI naming
    def get_comments_for_document(
        self,
        document_id: str,
        page: int = 1,
        page_size: int = 50
    ) -> List[Comment]:
        """Alias for get_document_comments"""
        return self.get_document_comments(document_id, page, page_size)

    def react_to_comment(self, comment_id: int, reaction_type: str) -> Tuple[bool, str]:
        """Add/toggle reaction to a comment"""
        try:
            self._request('POST', f'/comments/{comment_id}/react', {
                'reaction_type': reaction_type
            })
            return True, "Reaction added"
        except Exception as e:
            return False, str(e)

    def _parse_comment(self, data: Dict) -> Comment:
        """Parse comment data into Comment object"""
        author = data.get('author', {})
        return Comment(
            id=data['id'],
            content=data['content'],
            author_id=data.get('author_id', 0),
            author_username=author.get('username') if author else None,
            document_id=data.get('document_id'),
            correction_id=data.get('correction_id'),
            comment_type=data.get('comment_type', 'general'),
            created_at=data.get('created_at'),
            is_pinned=data.get('is_pinned', False),
            is_resolved=data.get('is_resolved', False),
            is_public=data.get('is_public', True),
            is_anonymous=data.get('is_anonymous', False),
            reply_count=data.get('reply_count', 0),
            line_number=data.get('line_number'),
            page_number=data.get('page_number')
        )

    def get_my_comments(
        self,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Comment], int]:
        """Get current user's comments"""
        try:
            params = {'page': page, 'page_size': page_size}
            data = self._request('GET', '/comments/my', params=params)
            comments = [self._parse_comment(c) for c in data.get('items', [])]
            return comments, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get my comments: {e}")
            return [], 0

    def get_all_comments(
        self,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Comment], int]:
        """Get all public comments via search (no login required)"""
        try:
            # Use search endpoint with broad query to get recent comments
            params = {'query': '%', 'page': page, 'page_size': page_size}
            data = self._request('GET', '/comments/search/', params=params)
            comments = [self._parse_comment(c) for c in data.get('items', [])]
            return comments, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get all comments: {e}")
            return [], 0

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
        """Create a new discovery/question"""
        try:
            data = self._request('POST', '/discoveries/', {
                'title': title,
                'content': content,
                'discovery_type': discovery_type,
                'document_id': document_id,
                'page_number': page_number,
                'shelfmark': shelfmark,
                'is_anonymous': is_anonymous,
                'additional_shelfmarks': additional_shelfmarks,
                'related_manuscripts': related_manuscripts
            })
            return self._parse_discovery(data), "Discovery created"
        except Exception as e:
            return None, str(e)

    def get_discovery(self, discovery_id: int) -> Optional[Discovery]:
        """Get a single discovery"""
        try:
            data = self._request('GET', f'/discoveries/{discovery_id}')
            return self._parse_discovery(data)
        except Exception as e:
            logger.warning(f"Failed to get discovery: {e}")
            return None

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
        """Update a discovery"""
        try:
            update_data = {}
            if title is not None:
                update_data['title'] = title
            if content is not None:
                update_data['content'] = content
            if document_id is not None:
                update_data['document_id'] = document_id
            if page_number is not None:
                update_data['page_number'] = page_number
            if shelfmark is not None:
                update_data['shelfmark'] = shelfmark
            if is_anonymous is not None:
                update_data['is_anonymous'] = is_anonymous

            self._request('PUT', f'/discoveries/{discovery_id}', update_data)
            return True, "Discovery updated"
        except Exception as e:
            return False, str(e)

    def delete_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Delete (hide) a discovery"""
        try:
            self._request('DELETE', f'/discoveries/{discovery_id}')
            return True, "Discovery deleted"
        except Exception as e:
            return False, str(e)

    def pin_discovery(self, discovery_id: int, pinned: bool = True) -> Tuple[bool, str]:
        """Pin or unpin a discovery (admin only)"""
        try:
            self._request('POST', f'/discoveries/{discovery_id}/pin', params={'pinned': str(pinned).lower()})
            return True, "Discovery pinned" if pinned else "Discovery unpinned"
        except Exception as e:
            return False, str(e)

    def hide_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Hide a discovery (admin only)"""
        try:
            self._request('POST', f'/discoveries/{discovery_id}/hide')
            return True, "Discovery hidden"
        except Exception as e:
            return False, str(e)

    def unhide_discovery(self, discovery_id: int) -> Tuple[bool, str]:
        """Unhide a discovery (admin only)"""
        try:
            self._request('POST', f'/discoveries/{discovery_id}/unhide')
            return True, "Discovery unhidden"
        except Exception as e:
            return False, str(e)

    def get_discoveries(
        self,
        discovery_type: str = None,
        featured_only: bool = False,
        include_hidden: bool = False,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Discovery], int]:
        """Get list of discoveries"""
        try:
            params = {
                'limit': page_size,
                'offset': (page - 1) * page_size
            }
            if discovery_type:
                params['discovery_type'] = discovery_type
            if featured_only:
                params['featured_only'] = True
            if include_hidden:
                params['include_hidden'] = True

            data = self._request('GET', '/discoveries/', params=params)
            discoveries = [self._parse_discovery(d) for d in data.get('items', [])]
            return discoveries, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get discoveries: {e}")
            return [], 0

    def get_feed(
        self,
        item_type: str = None,
        period: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[FeedItem], int]:
        """Get activity feed"""
        try:
            params = {
                'limit': page_size,
                'offset': (page - 1) * page_size
            }
            if item_type and item_type != 'all':
                params['item_type'] = item_type
            if period and period != 'all':
                params['period'] = period

            data = self._request('GET', '/discoveries/feed/items', params=params)
            items = [self._parse_feed_item(i) for i in data.get('items', [])]
            return items, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get feed: {e}")
            return [], 0

    def get_discovery_stats(self) -> Dict:
        """Get discovery statistics"""
        try:
            return self._request('GET', '/discoveries/stats/summary')
        except Exception as e:
            logger.warning(f"Failed to get discovery stats: {e}")
            return {}

    def add_discovery_response(
        self,
        discovery_id: int,
        content: str,
        is_anonymous: bool = False
    ) -> Tuple[Optional[DiscoveryResponse], str]:
        """Add a response to a discovery"""
        try:
            data = self._request('POST', f'/discoveries/{discovery_id}/responses', {
                'content': content,
                'is_anonymous': is_anonymous
            })
            return self._parse_discovery_response(data), "Response added"
        except Exception as e:
            return None, str(e)

    def get_discovery_responses(
        self,
        discovery_id: int
    ) -> List[DiscoveryResponse]:
        """Get responses for a discovery"""
        try:
            data = self._request('GET', f'/discoveries/{discovery_id}/responses')
            return [self._parse_discovery_response(r) for r in data.get('items', [])]
        except Exception as e:
            logger.warning(f"Failed to get discovery responses: {e}")
            return []

    def vote_discovery(
        self,
        discovery_id: int,
        vote_type: str  # 'up', 'down', or 'none'
    ) -> Tuple[bool, str]:
        """Vote on a discovery"""
        try:
            self._request('POST', f'/discoveries/{discovery_id}/vote', params={'vote_type': vote_type})
            return True, "Vote recorded"
        except Exception as e:
            return False, str(e)

    def mark_discovery_answered(
        self,
        discovery_id: int,
        answered: bool = True
    ) -> Tuple[bool, str]:
        """Mark a question as answered"""
        try:
            self._request('POST', f'/discoveries/{discovery_id}/answer', params={'answered': str(answered).lower()})
            return True, "Marked as answered" if answered else "Marked as unanswered"
        except Exception as e:
            return False, str(e)

    def _parse_discovery(self, data: Dict) -> Discovery:
        """Parse discovery data into Discovery object"""
        author = data.get('author', {})
        return Discovery(
            id=data['id'],
            title=data['title'],
            content=data['content'],
            discovery_type=data.get('discovery_type', 'discovery'),
            status=data.get('status', 'published'),
            author_id=author.get('id') if author else None,
            author_username=author.get('username') if author else None,
            author_full_name=author.get('full_name') if author else None,
            is_anonymous=author.get('is_anonymous', False) if author else False,
            document_id=data.get('document_id'),
            shelfmark=data.get('shelfmark'),
            page_number=data.get('page_number'),
            additional_shelfmarks=data.get('additional_shelfmarks'),
            related_manuscripts=data.get('related_manuscripts'),
            is_featured=data.get('is_featured', False),
            is_pinned=data.get('is_pinned', False),
            is_hidden=data.get('is_hidden', False),
            is_answered=data.get('is_answered', False),
            view_count=data.get('view_count', 0),
            response_count=data.get('response_count', 0),
            upvotes=data.get('upvotes', 0),
            downvotes=data.get('downvotes', 0),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )

    def _parse_discovery_response(self, data: Dict) -> DiscoveryResponse:
        """Parse discovery response data"""
        author = data.get('author', {})
        return DiscoveryResponse(
            id=data['id'],
            discovery_id=data.get('discovery_id', 0),
            content=data['content'],
            author_id=author.get('id') if author else None,
            author_username=author.get('username') if author else None,
            is_anonymous=author.get('is_anonymous', False) if author else False,
            created_at=data.get('created_at')
        )

    def _parse_feed_item(self, data: Dict) -> FeedItem:
        """Parse feed item data"""
        author = data.get('author', {})
        return FeedItem(
            id=data['id'],
            item_type=data['item_type'],
            title=data['title'],
            content_preview=data.get('content_preview', ''),
            author_username=author.get('username') if author else None,
            author_full_name=author.get('full_name') if author else None,
            is_anonymous=author.get('is_anonymous', False) if author else False,
            document_id=data.get('document_id'),
            shelfmark=data.get('shelfmark'),
            page_number=data.get('page_number'),
            created_at=data.get('created_at'),
            response_count=data.get('response_count', 0),
            is_featured=data.get('is_featured', False),
            is_pinned=data.get('is_pinned', False),
            is_answered=data.get('is_answered', False),
            upvotes=data.get('upvotes', 0),
            downvotes=data.get('downvotes', 0),
            original_text=data.get('original_text'),
            corrected_text=data.get('corrected_text'),
            # Join fields
            fragment_a=data.get('fragment_a'),
            fragment_b=data.get('fragment_b'),
            document_id_a=data.get('document_id_a'),
            document_id_b=data.get('document_id_b'),
            relationship_type=data.get('relationship_type'),
            join_source=data.get('join_source')
        )

    # ==================== All Users Corrections ====================

    def get_all_corrections(
        self,
        status: str = None,
        document_id: str = None,
        search_text: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Get all corrections (from all users)"""
        try:
            params = {'page': page, 'page_size': page_size}
            if status:
                params['status'] = status
            if document_id:
                params['document_id'] = document_id
            if search_text:
                params['search_text'] = search_text

            data = self._request('GET', '/corrections/', params=params)
            corrections = [self._parse_correction(c) for c in data.get('items', [])]
            return corrections, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get all corrections: {e}")
            return [], 0

    def get_pending_corrections(
        self,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Get pending corrections for review"""
        try:
            params = {'page': page, 'page_size': page_size}
            data = self._request('GET', '/corrections/pending', params=params)
            corrections = [self._parse_correction(c) for c in data.get('items', [])]
            return corrections, data.get('total', 0)
        except Exception as e:
            logger.warning(f"Failed to get pending corrections: {e}")
            return [], 0

    def review_correction(
        self,
        correction_id: int,
        action: str,  # 'approve', 'reject', 'request_revision'
        review_notes: str = None
    ) -> Tuple[bool, str]:
        """Review a correction"""
        try:
            data = {'action': action}
            if review_notes:
                data['review_notes'] = review_notes
            self._request('POST', f'/corrections/{correction_id}/review', data)
            return True, f"Correction {action}d"
        except Exception as e:
            return False, str(e)

    # ==================== Documents ====================

    def get_document_stats(self, document_id: str) -> Dict:
        """Get statistics for a document"""
        try:
            return self._request('GET', f'/documents/{document_id}/stats')
        except Exception as e:
            logger.warning(f"Failed to get document stats: {e}")
            return {}

    def get_corrected_text(self, document_id: str, original_text: str) -> str:
        """
        Apply all approved corrections to document text.

        Args:
            document_id: Document ID
            original_text: Original transcription text

        Returns:
            Text with corrections applied
        """
        try:
            # API expects original_text as query parameter with POST method
            data = self._request('POST', f'/documents/{document_id}/corrected-text', params={
                'original_text': original_text
            })
            return data.get('corrected_text', original_text)
        except Exception as e:
            logger.warning(f"Failed to get corrected text: {e}")
            return original_text

    # ==================== Versions API ====================

    def get_page_versions(self, document_id: str, page_num: int = 1) -> Dict:
        """
        Fetch all versions for a document page.

        Args:
            document_id: System ID of the document
            page_num: Page number

        Returns:
            Dict with 'all_versions', 'current_default', 'total'
        """
        default_response = {'all_versions': [], 'current_default': None, 'total': 0}
        try:
            data = self._request('GET', f'/versions/{document_id}/{page_num}')
            if not isinstance(data, dict):
                return default_response
            # Ensure expected keys exist
            if 'all_versions' not in data or not isinstance(data.get('all_versions'), list):
                data['all_versions'] = []
            if 'current_default' not in data:
                data['current_default'] = None
            return data
        except Exception as e:
            logger.warning(f"Failed to get page versions: {e}")
            return default_response

    def get_version_content(self, version_id: int) -> Dict:
        """
        Fetch content for a specific version.

        Args:
            version_id: Version ID

        Returns:
            Dict with version details including 'content'
        """
        try:
            data = self._request('GET', f'/versions/id/{version_id}')
            return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to get version content: {e}")
            return {}

    def record_document_view(self, document_id: str):
        """Record a document view for statistics"""
        try:
            self._request('POST', f'/documents/{document_id}/view')
        except Exception as e:
            logger.debug(f"Failed to record view: {e}")

    # ==================== Leaderboard ====================

    def get_leaderboard(self, limit: int = 10) -> List[User]:
        """Get top users by reputation"""
        try:
            data = self._request('GET', '/users/leaderboard/top', params={'limit': limit})
            return [User(
                id=u['id'],
                email='',  # Not exposed in public profile
                username=u['username'],
                full_name=u.get('full_name'),
                affiliation=u.get('affiliation'),
                role=u.get('role', 'contributor'),
                reputation_score=u.get('reputation_score', 0),
                approved_corrections_count=u.get('approved_corrections_count', 0)
            ) for u in data]
        except Exception as e:
            logger.warning(f"Failed to get leaderboard: {e}")
            return []

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
        """
        Create a join between two fragments.

        Args:
            fragment_a: First fragment shelfmark
            fragment_b: Second fragment shelfmark
            relationship_type: Optional - 'physical_join' or 'same_composition'
            notes: Optional notes about the join
            document_id_a: System ID (sys_id) for fragment A
            document_id_b: System ID (sys_id) for fragment B

        Returns:
            (FragmentJoin, message) on success, (None, error) on failure
        """
        try:
            payload = {
                'fragment_a': fragment_a,
                'fragment_b': fragment_b,
                'relationship_type': relationship_type,
                'notes': notes
            }
            # Only include document_ids if provided
            if document_id_a:
                payload['document_id_a'] = document_id_a
            if document_id_b:
                payload['document_id_b'] = document_id_b

            data = self._request('POST', '/joins/', payload)
            return self._parse_join(data), "Join created"
        except Exception as e:
            return None, str(e)

    def get_connected_fragments(self, shelfmark: str, timeout: int = 30) -> Optional[ConnectedFragments]:
        """
        Get all fragments connected to the given shelfmark.

        Returns the full connected component - if A joins to B and B joins to C,
        querying any of them returns all three.

        Args:
            shelfmark: The shelfmark to query
            timeout: Request timeout in seconds (default 30)

        Returns:
            ConnectedFragments object or None on error
        """
        try:
            # Use query parameter approach to avoid URL path encoding issues with /
            data = self._request('GET', '/joins/connected', params={'shelfmark': shelfmark}, timeout=timeout)
            return ConnectedFragments(
                shelfmark=data['shelfmark'],
                shelfmark_normalized=data['shelfmark_normalized'],
                fragments=data.get('fragments', []),
                fragment_details=[
                    JoinedFragmentDetail(
                        shelfmark=fd.get('shelfmark', ''),
                        document_id=fd.get('document_id'),
                        is_current=fd.get('is_current', False),
                        relationship_type=fd.get('relationship_type'),
                        join_id=fd.get('join_id'),
                        join_source=fd.get('join_source')
                    ) for fd in data.get('fragment_details', [])
                ],
                joins=[self._parse_join(j) for j in data.get('joins', [])],
                total_fragments=data.get('total_fragments', 0),
                total_joins=data.get('total_joins', 0)
            )
        except Exception as e:
            logger.warning(f"Failed to get connected fragments: {e}")
            return None

    def get_connected_fragments_quick(self, shelfmark: str) -> Optional[ConnectedFragments]:
        """
        Get connected fragments with a short timeout (3 seconds).
        Use this for UI updates where blocking is unacceptable.
        """
        return self.get_connected_fragments(shelfmark, timeout=3)

    def get_connected_fragments_by_id(self, document_id: str, timeout: int = 30) -> Optional[ConnectedFragments]:
        """
        Get all fragments connected to the given document_id (sys_id).

        This is the preferred method as document_id is the stable identifier.

        Args:
            document_id: The system ID (sys_id) to query
            timeout: Request timeout in seconds (default 30)

        Returns:
            ConnectedFragments object or None on error
        """
        try:
            data = self._request('GET', '/joins/connected', params={'document_id': document_id}, timeout=timeout)
            return ConnectedFragments(
                shelfmark=data.get('shelfmark', ''),
                shelfmark_normalized=data.get('shelfmark_normalized', ''),
                fragments=data.get('fragments', []),
                fragment_details=[
                    JoinedFragmentDetail(
                        shelfmark=fd.get('shelfmark', ''),
                        document_id=fd.get('document_id'),
                        is_current=fd.get('is_current', False),
                        relationship_type=fd.get('relationship_type'),
                        join_id=fd.get('join_id'),
                        join_source=fd.get('join_source')
                    ) for fd in data.get('fragment_details', [])
                ],
                joins=[self._parse_join(j) for j in data.get('joins', [])],
                total_fragments=data.get('total_fragments', 0),
                total_joins=data.get('total_joins', 0)
            )
        except Exception as e:
            logger.warning(f"Failed to get connected fragments by id: {e}")
            return None

    def get_join_by_id(self, join_id: int) -> Optional[FragmentJoin]:
        """Get a specific join by ID"""
        try:
            data = self._request('GET', f'/joins/{join_id}')
            return self._parse_join(data)
        except Exception as e:
            logger.warning(f"Failed to get join: {e}")
            return None

    def delete_join(self, join_id: int) -> Tuple[bool, str]:
        """Delete a join"""
        try:
            self._request('DELETE', f'/joins/{join_id}')
            return True, "Join deleted"
        except Exception as e:
            return False, str(e)

    def update_join(
        self,
        join_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Tuple[Optional[FragmentJoin], str]:
        """Update a join's metadata"""
        try:
            data = self._request('PATCH', f'/joins/{join_id}', {
                'relationship_type': relationship_type,
                'notes': notes
            })
            return self._parse_join(data), "Join updated"
        except Exception as e:
            return None, str(e)

    def search_joins(
        self,
        query: Optional[str] = None,
        source: Optional[str] = None,
        relationship_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[FragmentJoin], int]:
        """Search joins by shelfmark pattern or filters. Returns (joins, total)."""
        try:
            params = {'limit': limit, 'offset': offset}
            if query:
                params['q'] = query
            if source:
                params['source'] = source
            if relationship_type:
                params['relationship_type'] = relationship_type

            data = self._request('GET', '/joins/', params=params)
            joins = [self._parse_join(j) for j in data.get('results', [])]
            total = data.get('total', len(joins))
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
        try:
            params = {'limit': limit, 'offset': offset}
            if query:
                params['q'] = query
            if relationship_type:
                params['relationship_type'] = relationship_type

            data = self._request('GET', '/joins/my', params=params)
            joins = [self._parse_join(j) for j in data.get('results', [])]
            total = data.get('total', len(joins))
            return joins, total
        except Exception as e:
            logger.warning(f"Failed to get my joins: {e}")
            return [], 0

    def _parse_join(self, data: Dict) -> FragmentJoin:
        """Parse join data into FragmentJoin object"""
        created_by = data.get('created_by', {})
        return FragmentJoin(
            id=data['id'],
            fragment_a=data['fragment_a'],
            fragment_b=data['fragment_b'],
            document_id_a=data.get('document_id_a'),
            document_id_b=data.get('document_id_b'),
            relationship_type=data.get('relationship_type'),
            notes=data.get('notes'),
            source=data.get('source', 'user'),
            source_url=data.get('source_url'),
            created_by_username=created_by.get('username') if created_by else None,
            created_at=data.get('created_at')
        )


# Singleton instance for easy access
_client_instance: Optional[CorrectionsClient] = None


def get_corrections_client() -> CorrectionsClient:
    """Get or create the corrections client singleton"""
    global _client_instance
    if _client_instance is None:
        _client_instance = CorrectionsClient()
    return _client_instance
