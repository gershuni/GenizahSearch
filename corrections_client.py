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
            is_resolved=data.get('is_resolved', False)
        )

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
