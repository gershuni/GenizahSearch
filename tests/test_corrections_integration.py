#!/usr/bin/env python3
"""
Integration tests for the user corrections system.

Tests:
1. User registration
2. User authentication
3. Creating corrections
4. Viewing corrections
5. Creating comments
6. Viewing comments
7. Version management
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.models.database import init_db, SessionLocal, Base, engine
from backend.models import User, UserRole, Correction, CorrectionStatus, Comment
from backend.models import TranscriptionVersion, PageDefault, VersionSource
from backend.services import AuthService, UserService, CorrectionService, CommentService, VersionService
from backend.schemas.user import UserCreate
from backend.schemas.correction import CorrectionCreate
from backend.schemas.comment import CommentCreate


def test_user_registration():
    """Test user registration"""
    print("\n=== Test: User Registration ===")

    db = SessionLocal()
    try:
        # Create a test user
        user_data = UserCreate(
            email="test@example.com",
            username="testuser",
            password="TestPassword123!",
            confirm_password="TestPassword123!",
            full_name="Test User",
            affiliation="Test University"
        )

        user, error = UserService.create_user(db, user_data)

        if error:
            print(f"  ❌ Registration failed: {error}")
            return None

        print(f"  ✓ User registered: {user.username} (ID: {user.id})")
        print(f"  ✓ Role: {user.role}")

        return user

    finally:
        db.close()


def test_user_login(email="test@example.com", password="TestPassword123!"):
    """Test user login"""
    print("\n=== Test: User Login ===")

    db = SessionLocal()
    try:
        user, error = AuthService.authenticate_user(db, email, password)

        if error:
            print(f"  ❌ Login failed: {error}")
            return None, None

        # Generate token
        tokens = AuthService.create_tokens(user)

        print(f"  ✓ Login successful: {user.username}")
        print(f"  ✓ Token: {tokens.access_token[:20]}...")

        return user, tokens.access_token

    finally:
        db.close()


def test_create_correction(user):
    """Test creating a correction"""
    print("\n=== Test: Create Correction ===")

    db = SessionLocal()
    try:
        # Refresh user in this session
        user = db.query(User).filter(User.id == user.id).first()

        correction_data = CorrectionCreate(
            document_id="99123456789",
            original_text="טקסט מקורי עם שגיאה",
            corrected_text="טקסט מתוקן ללא שגיאה",
            notes="תיקון שגיאת כתיב",
            page_number=1
        )

        correction, error = CorrectionService.create_correction(
            db, user, correction_data
        )

        if error:
            print(f"  ❌ Create correction failed: {error}")
            return None

        print(f"  ✓ Correction created: ID {correction.id}")
        print(f"  ✓ Status: {correction.status}")
        print(f"  ✓ Document: {correction.document_id}")

        return correction

    finally:
        db.close()


def test_submit_correction(user, correction_id):
    """Test submitting a correction for review"""
    print("\n=== Test: Submit Correction ===")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user.id).first()

        correction, error = CorrectionService.submit_correction(
            db, user, correction_id
        )

        if error:
            print(f"  ❌ Submit failed: {error}")
            return None

        print(f"  ✓ Correction submitted")
        print(f"  ✓ New status: {correction.status}")

        return correction

    finally:
        db.close()


def test_get_my_corrections(user):
    """Test getting user's corrections"""
    print("\n=== Test: Get My Corrections ===")

    db = SessionLocal()
    try:
        from backend.schemas.correction import CorrectionSearchParams

        params = CorrectionSearchParams(author_id=user.id)
        corrections, total = CorrectionService.search_corrections(db, params)

        print(f"  ✓ Found {total} corrections")
        for c in corrections:
            print(f"    - ID {c.id}: {c.status.value} - {c.document_id}")

        return corrections

    finally:
        db.close()


def test_create_comment(user, document_id="99123456789"):
    """Test creating a comment"""
    print("\n=== Test: Create Comment ===")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user.id).first()

        comment_data = CommentCreate(
            document_id=document_id,
            content="זו הערה לדוגמה על המסמך",
        )

        comment, error = CommentService.create_comment(db, user, comment_data)

        if error:
            print(f"  ❌ Create comment failed: {error}")
            return None

        print(f"  ✓ Comment created: ID {comment.id}")
        print(f"  ✓ Content: {comment.content[:30]}...")
        print(f"  ✓ Type: {comment.comment_type}")

        return comment

    finally:
        db.close()


def test_get_document_comments(document_id="99123456789"):
    """Test getting comments for a document"""
    print("\n=== Test: Get Document Comments ===")

    db = SessionLocal()
    try:
        comments, total = CommentService.get_comments_for_document(db, document_id)

        print(f"  ✓ Found {total} comments")
        for c in comments:
            author = c.author.username if c.author else "Unknown"
            print(f"    - ID {c.id}: {author} - {c.content[:30]}...")

        return comments

    finally:
        db.close()


def test_create_version(user, sys_id="99123456789", page_num=1):
    """Test creating a transcription version"""
    print("\n=== Test: Create Version ===")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user.id).first()

        from backend.schemas.version import VersionCreate

        version_data = VersionCreate(
            sys_id=sys_id,
            page_num=page_num,
            content="זהו טקסט התעתיק המלא של העמוד הראשון",
            change_description="גרסה ראשונית"
        )

        version, error = VersionService.create_user_version(
            db, version_data, user, set_as_default=True
        )

        if error:
            print(f"  ❌ Create version failed: {error}")
            return None

        print(f"  ✓ Version created: ID {version.id}")
        print(f"  ✓ Source: {version.source}")
        print(f"  ✓ Is default: {version.is_current_default}")

        return version

    finally:
        db.close()


def test_get_page_versions(sys_id="99123456789", page_num=1):
    """Test getting versions for a page"""
    print("\n=== Test: Get Page Versions ===")

    db = SessionLocal()
    try:
        default, all_versions = VersionService.get_page_versions(db, sys_id, page_num)

        print(f"  ✓ Found {len(all_versions)} versions")
        if default:
            print(f"  ✓ Current default: ID {default.id} ({default.source})")

        for v in all_versions:
            user_name = v.user.username if v.user else "System"
            print(f"    - ID {v.id}: {v.source.value} by {user_name}")

        return all_versions

    finally:
        db.close()


def test_import_base_version(sys_id="99123456789", page_num=1):
    """Test importing a V0.8 base version"""
    print("\n=== Test: Import Base Version ===")

    db = SessionLocal()
    try:
        from backend.schemas.version import ImportVersionRequest

        import_data = ImportVersionRequest(
            sys_id=sys_id,
            page_num=page_num,
            content="זהו הטקסט המקורי מ-V0.8",
            source=VersionSource.V08
        )

        version, error = VersionService.import_base_version(db, import_data)

        if error:
            print(f"  ❌ Import failed: {error}")
            return None

        print(f"  ✓ Base version imported: ID {version.id}")
        print(f"  ✓ Source: {version.source}")
        print(f"  ✓ Is default: {version.is_current_default}")

        return version

    finally:
        db.close()


def cleanup_database():
    """Clean up test data"""
    print("\n=== Cleanup ===")

    db = SessionLocal()
    try:
        # Disable foreign key checks for SQLite
        from sqlalchemy import text
        db.execute(text("PRAGMA foreign_keys = OFF"))

        # Delete all tables
        db.query(PageDefault).delete()
        db.query(TranscriptionVersion).delete()

        # Import additional models that need cleanup
        from backend.models.correction import CorrectionVote
        from backend.models.comment import CommentReaction
        from backend.models.activity_log import ActivityLog

        db.query(CorrectionVote).delete()
        db.query(CommentReaction).delete()
        db.query(ActivityLog).delete()
        db.query(Comment).delete()
        db.query(Correction).delete()
        db.query(User).delete()

        db.commit()

        # Re-enable foreign key checks
        db.execute(text("PRAGMA foreign_keys = ON"))

        print("  ✓ Test data cleaned up")
    except Exception as e:
        db.rollback()
        print(f"  ⚠ Cleanup warning: {e}")
    finally:
        db.close()


def run_all_tests():
    """Run all integration tests"""
    print("=" * 60)
    print("CORRECTIONS SYSTEM INTEGRATION TESTS")
    print("=" * 60)

    # Initialize database
    init_db()

    # Clean start
    cleanup_database()

    # Test user registration
    user = test_user_registration()
    if not user:
        print("\n❌ FAILED: User registration")
        return False

    # Test login
    user_logged, token = test_user_login()
    if not user_logged:
        print("\n❌ FAILED: User login")
        return False

    # Test correction creation
    correction = test_create_correction(user)
    if not correction:
        print("\n❌ FAILED: Create correction")
        return False

    # Test correction submission
    submitted = test_submit_correction(user, correction.id)
    if not submitted:
        print("\n❌ FAILED: Submit correction")
        return False

    # Test get my corrections
    test_get_my_corrections(user)

    # Test comment creation
    comment = test_create_comment(user)
    if not comment:
        print("\n❌ FAILED: Create comment")
        return False

    # Test get document comments
    test_get_document_comments()

    # Test import base version
    base_version = test_import_base_version()
    if not base_version:
        print("\n❌ FAILED: Import base version")
        return False

    # Test create user version
    user_version = test_create_version(user)
    if not user_version:
        print("\n❌ FAILED: Create user version")
        return False

    # Test get page versions
    test_get_page_versions()

    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED")
    print("=" * 60)

    return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
