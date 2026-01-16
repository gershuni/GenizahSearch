"""
Tests for the Genizah Corrections API
"""
import pytest
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Test database setup
TEST_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Override database dependency
from backend.models.database import Base, get_db
from backend.main import app

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

# Create tables
Base.metadata.create_all(bind=engine)

client = TestClient(app)


class TestHealth:
    """Test health check endpoint"""

    def test_health_check(self):
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_root_endpoint(self):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data


class TestAuthentication:
    """Test authentication endpoints"""

    def test_register_user(self):
        response = client.post("/api/v1/auth/register", json={
            "email": "test@example.com",
            "username": "testuser",
            "password": "TestPass123",
            "confirm_password": "TestPass123",
            "full_name": "Test User",
            "affiliation": "Test University"
        })
        assert response.status_code == 201
        data = response.json()
        assert data["email"] == "test@example.com"
        assert data["username"] == "testuser"
        assert data["role"] == "contributor"

    def test_register_duplicate_email(self):
        # First registration
        client.post("/api/v1/auth/register", json={
            "email": "dupe@example.com",
            "username": "dupeuser1",
            "password": "TestPass123",
            "confirm_password": "TestPass123"
        })
        # Duplicate
        response = client.post("/api/v1/auth/register", json={
            "email": "dupe@example.com",
            "username": "dupeuser2",
            "password": "TestPass123",
            "confirm_password": "TestPass123"
        })
        assert response.status_code == 400
        assert "already registered" in response.json()["detail"].lower()

    def test_register_weak_password(self):
        response = client.post("/api/v1/auth/register", json={
            "email": "weak@example.com",
            "username": "weakuser",
            "password": "weak",
            "confirm_password": "weak"
        })
        assert response.status_code == 422  # Validation error

    def test_login_success(self):
        # Register first
        client.post("/api/v1/auth/register", json={
            "email": "login@example.com",
            "username": "loginuser",
            "password": "LoginPass123",
            "confirm_password": "LoginPass123"
        })
        # Login
        response = client.post("/api/v1/auth/login", json={
            "email": "login@example.com",
            "password": "LoginPass123"
        })
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_login_wrong_password(self):
        response = client.post("/api/v1/auth/login", json={
            "email": "login@example.com",
            "password": "WrongPass123"
        })
        assert response.status_code == 401

    def test_login_nonexistent_user(self):
        response = client.post("/api/v1/auth/login", json={
            "email": "nobody@example.com",
            "password": "AnyPass123"
        })
        assert response.status_code == 401


class TestCorrections:
    """Test corrections endpoints"""

    @pytest.fixture
    def auth_headers(self):
        """Get authentication headers"""
        # Register and login
        client.post("/api/v1/auth/register", json={
            "email": "corruser@example.com",
            "username": "corruser",
            "password": "CorrPass123",
            "confirm_password": "CorrPass123"
        })
        response = client.post("/api/v1/auth/login", json={
            "email": "corruser@example.com",
            "password": "CorrPass123"
        })
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def test_create_correction(self, auth_headers):
        response = client.post("/api/v1/corrections/", json={
            "document_id": "TEST_DOC_001",
            "original_text": "הכתוב המקורי",
            "corrected_text": "הכתוב המתוקן",
            "correction_type": "text_correction",
            "confidence_score": 0.9,
            "notes": "Test correction"
        }, headers=auth_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["document_id"] == "TEST_DOC_001"
        assert data["status"] == "draft"
        return data["id"]

    def test_create_correction_unauthorized(self):
        response = client.post("/api/v1/corrections/", json={
            "document_id": "TEST_DOC_002",
            "original_text": "original",
            "corrected_text": "corrected"
        })
        assert response.status_code == 401

    def test_get_correction(self, auth_headers):
        # Create first
        create_response = client.post("/api/v1/corrections/", json={
            "document_id": "TEST_DOC_003",
            "original_text": "orig",
            "corrected_text": "corr"
        }, headers=auth_headers)
        correction_id = create_response.json()["id"]

        # Get
        response = client.get(f"/api/v1/corrections/{correction_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == correction_id

    def test_submit_correction(self, auth_headers):
        # Create
        create_response = client.post("/api/v1/corrections/", json={
            "document_id": "TEST_DOC_004",
            "original_text": "orig",
            "corrected_text": "corr"
        }, headers=auth_headers)
        correction_id = create_response.json()["id"]

        # Submit
        response = client.post(
            f"/api/v1/corrections/{correction_id}/submit",
            json={"notes": "Ready for review"},
            headers=auth_headers
        )
        assert response.status_code == 200
        assert response.json()["status"] == "pending"

    def test_list_corrections(self, auth_headers):
        # Create a correction first
        client.post("/api/v1/corrections/", json={
            "document_id": "TEST_LIST_DOC",
            "original_text": "orig",
            "corrected_text": "corr"
        }, headers=auth_headers)

        response = client.get("/api/v1/corrections/")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data

    def test_vote_on_correction(self, auth_headers):
        # Register another user
        client.post("/api/v1/auth/register", json={
            "email": "voter@example.com",
            "username": "voter",
            "password": "VoterPass123",
            "confirm_password": "VoterPass123"
        })
        login_response = client.post("/api/v1/auth/login", json={
            "email": "voter@example.com",
            "password": "VoterPass123"
        })
        voter_headers = {"Authorization": f"Bearer {login_response.json()['access_token']}"}

        # Create correction with first user
        create_response = client.post("/api/v1/corrections/", json={
            "document_id": "TEST_VOTE_DOC",
            "original_text": "orig",
            "corrected_text": "corr"
        }, headers=auth_headers)
        correction_id = create_response.json()["id"]

        # Vote with second user
        response = client.post(
            f"/api/v1/corrections/{correction_id}/vote",
            json={"vote_value": 1},
            headers=voter_headers
        )
        assert response.status_code == 200
        assert response.json()["upvotes"] == 1


class TestComments:
    """Test comments endpoints"""

    @pytest.fixture
    def auth_headers(self):
        """Get authentication headers"""
        client.post("/api/v1/auth/register", json={
            "email": "commenter@example.com",
            "username": "commenter",
            "password": "CommentPass123",
            "confirm_password": "CommentPass123"
        })
        response = client.post("/api/v1/auth/login", json={
            "email": "commenter@example.com",
            "password": "CommentPass123"
        })
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def test_create_comment_on_document(self, auth_headers):
        response = client.post("/api/v1/comments/", json={
            "content": "This is a test comment",
            "document_id": "TEST_COMMENT_DOC",
            "comment_type": "general"
        }, headers=auth_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["content"] == "This is a test comment"

    def test_get_document_comments(self, auth_headers):
        # Create comment first
        client.post("/api/v1/comments/", json={
            "content": "Comment for listing",
            "document_id": "TEST_LIST_COMMENTS"
        }, headers=auth_headers)

        response = client.get("/api/v1/comments/document/TEST_LIST_COMMENTS")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data

    def test_react_to_comment(self, auth_headers):
        # Create comment
        create_response = client.post("/api/v1/comments/", json={
            "content": "Comment for reaction",
            "document_id": "TEST_REACTION_DOC"
        }, headers=auth_headers)
        comment_id = create_response.json()["id"]

        # React
        response = client.post(
            f"/api/v1/comments/{comment_id}/react",
            json={"reaction_type": "helpful"},
            headers=auth_headers
        )
        assert response.status_code == 200


class TestDocuments:
    """Test document endpoints"""

    def test_get_document_stats(self):
        response = client.get("/api/v1/documents/TEST_STATS_DOC/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["document_id"] == "TEST_STATS_DOC"
        assert "correction_count" in data

    def test_get_document_metadata(self):
        response = client.get("/api/v1/documents/TEST_META_DOC/metadata")
        assert response.status_code == 200


class TestUsers:
    """Test user endpoints"""

    def test_get_leaderboard(self):
        response = client.get("/api/v1/users/leaderboard/top?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
