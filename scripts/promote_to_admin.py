#!/usr/bin/env python
"""
Promote User to Admin - Utility Script

Usage:
    python scripts/promote_to_admin.py <email>

Example:
    python scripts/promote_to_admin.py user@example.com
"""
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.models.database import SessionLocal
from backend.models.user import User


def promote_user_to_admin(email: str):
    """Promote a user to admin role."""
    db = SessionLocal()

    try:
        # Find user by email
        user = db.query(User).filter(User.email == email).first()

        if not user:
            print(f"❌ Error: User with email '{email}' not found.")
            print("\nAvailable users:")
            all_users = db.query(User).all()
            if not all_users:
                print("  (No users in database)")
            for u in all_users:
                print(f"  - {u.email} ({u.username}) - Role: {u.role}")
            return False

        # Check current role
        print(f"Found user: {user.full_name} ({user.email})")
        print(f"Current role: {user.role}")

        if user.role == "admin":
            print("✓ User is already an admin!")
            return True

        # Promote to admin
        user.role = "admin"
        db.commit()

        print(f"✅ Success! User '{email}' has been promoted to admin.")
        print(f"New role: {user.role}")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python scripts/promote_to_admin.py <email>")
        print("\nExample:")
        print("  python scripts/promote_to_admin.py user@example.com")
        sys.exit(1)

    email = sys.argv[1]

    print("=" * 60)
    print("Promote User to Admin")
    print("=" * 60)
    print()

    success = promote_user_to_admin(email)

    print()
    print("=" * 60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
