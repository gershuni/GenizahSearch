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
    line_number: Optional[int] = None
    char_start: Optional[int] = None
    char_end: Optional[int] = None
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
    is_answered: bool = False
    view_count: int = 0
    response_count: int = 0
    upvotes: int = 0
    downvotes: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


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
    item_type: str  # discovery, question, correction, comment
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
    is_answered: bool = False
    upvotes: int = 0
    downvotes: int = 0
    # For corrections
    original_text: Optional[str] = None
    corrected_text: Optional[str] = None


class CorrectionsClient:
    """
    Client for the Genizah Corrections API.
    Used by the desktop application to interact with the corrections system.
    """

    def __init__(self, base_url: str = None, config_path: Path = None):
        """
        Initialize the corrections client.

        Args:
            base_url: API base URL (default: http://localhost:8000/api/v1)
            config_path: Path to store credentials
        """
        self.base_url = base_url or os.environ.get(
            'CORRECTIONS_API_URL',
            'http://localhost:8000/api/v1'
        )
        self.config_path = config_path or Path.home() / '.genizah_corrections'
        self.config_path.mkdir(exist_ok=True)

        self.credentials_file = self.config_path / 'credentials.json'
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.current_user: Optional[User] = None

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
        """Load saved credentials from disk"""
        if self.credentials_file.exists():
            try:
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)
                    self.access_token = data.get('access_token')
                    self.refresh_token = data.get('refresh_token')
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")

    def _save_credentials(self):
        """Save credentials to disk"""
        try:
            with open(self.credentials_file, 'w') as f:
                json.dump({
                    'access_token': self.access_token,
                    'refresh_token': self.refresh_token
                }, f)
        except Exception as e:
            logger.warning(f"Failed to save credentials: {e}")

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
        params: Dict = None
    ) -> Dict:
        """
        Make an API request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request body data
            params: Query parameters

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
                timeout=30
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
                self._save_credentials()
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

    def get_current_user(self) -> Optional[User]:
        """Get current logged-in user"""
        if not self.access_token:
            return None

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
            return self.current_user
        except Exception as e:
            logger.warning(f"Failed to get current user: {e}")
            return None

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
        confidence_score: float = 0.8,
        source_reference: str = None,
        notes: str = None,
        shelfmark: str = None,
        system_id: str = None,
        context_before: str = None,
        context_after: str = None
    ) -> Tuple[Optional[Correction], str]:
        """
        Create a new correction.

        Returns:
            Tuple of (Correction or None, message)
        """
        try:
            data = self._request('POST', '/corrections/', {
                'document_id': document_id,
                'original_text': original_text,
                'corrected_text': corrected_text,
                'correction_type': correction_type,
                'line_number': line_number,
                'char_start': char_start,
                'char_end': char_end,
                'confidence_score': confidence_score,
                'source_reference': source_reference,
                'notes': notes,
                'shelfmark': shelfmark,
                'system_id': system_id,
                'context_before': context_before,
                'context_after': context_after
            })
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
                params={'include_drafts': include_drafts}
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
        line_number: int = None
    ) -> Tuple[Optional[Comment], str]:
        """Create a new comment"""
        try:
            data = self._request('POST', '/comments/', {
                'content': content,
                'document_id': document_id,
                'correction_id': correction_id,
                'parent_id': parent_id,
                'comment_type': comment_type,
                'line_number': line_number
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
            reply_count=data.get('reply_count', 0)
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

    def get_discoveries(
        self,
        discovery_type: str = None,
        featured_only: bool = False,
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
            corrected_text=data.get('corrected_text')
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
            data = self._request('POST', f'/documents/{document_id}/corrected-text', {
                'original_text': original_text
            })
            return data.get('corrected_text', original_text)
        except Exception as e:
            logger.warning(f"Failed to get corrected text: {e}")
            return original_text

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


# Singleton instance for easy access
_client_instance: Optional[CorrectionsClient] = None


def get_corrections_client() -> CorrectionsClient:
    """Get or create the corrections client singleton"""
    global _client_instance
    if _client_instance is None:
        _client_instance = CorrectionsClient()
    return _client_instance
