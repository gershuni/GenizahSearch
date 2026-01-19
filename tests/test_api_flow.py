#!/usr/bin/env python3
"""
Quick API flow test to verify corrections system works end-to-end.

Tests the flow:
1. Register user
2. Login
3. Create correction
4. View correction
5. Get versions for page
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from backend.main import app
from backend.models.database import init_db, SessionLocal, Base, engine

# Initialize database
init_db()

# Create test client
client = TestClient(app)

def test_api_flow():
    """Test the complete API flow."""
    print("=" * 60)
    print("API FLOW TEST")
    print("=" * 60)

    # 1. Register user
    print("\n=== 1. Register User ===")
    register_data = {
        "email": "apitest@example.com",
        "username": "apitest",
        "password": "TestPass123!",
        "confirm_password": "TestPass123!",
        "full_name": "API Test User",
        "affiliation": "Test"
    }

    response = client.post("/api/v1/auth/register", json=register_data)
    if response.status_code == 201:
        print(f"  ✓ User registered: {response.json().get('username')}")
    elif response.status_code == 400 and "already" in response.text.lower():
        print(f"  ✓ User already exists (from previous test)")
    else:
        print(f"  ❌ Registration failed: {response.status_code} - {response.text}")
        return False

    # 2. Login
    print("\n=== 2. Login ===")
    login_data = {
        "email": "apitest@example.com",
        "password": "TestPass123!"
    }

    response = client.post("/api/v1/auth/login", json=login_data)
    if response.status_code != 200:
        print(f"  ❌ Login failed: {response.status_code} - {response.text}")
        return False

    token = response.json().get("access_token")
    print(f"  ✓ Login successful, token: {token[:20]}...")

    headers = {"Authorization": f"Bearer {token}"}

    # 3. Create correction
    print("\n=== 3. Create Correction ===")
    correction_data = {
        "document_id": "990000412990205171",
        "page_number": 1,
        "original_text": "טקסט מקורי",
        "corrected_text": "טקסט מתוקן",
        "correction_type": "text_correction",
        "notes": "תיקון בדיקה"
    }

    response = client.post("/api/v1/corrections/", json=correction_data, headers=headers)
    if response.status_code not in [200, 201]:
        print(f"  ❌ Create correction failed: {response.status_code} - {response.text}")
        return False

    correction = response.json()
    correction_id = correction.get("id")
    print(f"  ✓ Correction created: ID {correction_id}")
    print(f"    Status: {correction.get('status')}")

    # 4. Get my corrections
    print("\n=== 4. Get My Corrections ===")
    response = client.get("/api/v1/corrections/my", headers=headers)
    if response.status_code != 200:
        print(f"  ❌ Get corrections failed: {response.status_code} - {response.text}")
        return False

    my_corrections = response.json()
    print(f"  ✓ Found {my_corrections.get('total', 0)} corrections")

    # 5. Get versions for page
    print("\n=== 5. Get Page Versions ===")
    response = client.get("/api/v1/versions/990000412990205171/1")
    if response.status_code != 200:
        print(f"  ❌ Get versions failed: {response.status_code} - {response.text}")
        return False

    versions = response.json()
    print(f"  ✓ Found {versions.get('total', 0)} versions for page")
    if versions.get("current_default"):
        default = versions["current_default"]
        print(f"    Current default: ID {default.get('id')} ({default.get('source')})")

    for v in versions.get("all_versions", []):
        print(f"    - ID {v.get('id')}: {v.get('source')} {'(default)' if v.get('is_current_default') else ''}")

    print("\n" + "=" * 60)
    print("✅ ALL API TESTS PASSED")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_api_flow()
    sys.exit(0 if success else 1)
