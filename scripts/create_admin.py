#!/usr/bin/env python3
"""
Create an admin user for Genizah Search
Usage: python create_admin.py <email> <password> [full_name]
"""
import sys
import os
from pathlib import Path

# Load .env file before importing database modules
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())

sys.path.insert(0, os.path.dirname(__file__))

from backend.models.database import SessionLocal, init_db
from backend.models.user import User, UserRole
import bcrypt

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def create_admin(email: str, password: str, full_name: str = None):
    init_db()
    db = SessionLocal()

    # Check if user exists
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        print(f"User {email} already exists.")
        if existing.role != UserRole.ADMIN:
            existing.role = UserRole.ADMIN
            db.commit()
            print(f"Updated {email} to admin role.")
        else:
            print(f"{email} is already an admin.")
        db.close()
        return

    # Create new admin user
    user = User(
        email=email,
        username=email.split('@')[0],
        full_name=full_name or email.split('@')[0],
        password_hash=hash_password(password),
        role=UserRole.ADMIN,
        is_active=True,
        is_verified=True,
        reputation_score=100
    )
    db.add(user)
    db.commit()
    print(f"Admin user {email} created successfully!")
    db.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python create_admin.py <email> <password> [full_name]")
        sys.exit(1)

    email = sys.argv[1]
    password = sys.argv[2]
    full_name = sys.argv[3] if len(sys.argv) > 3 else None

    create_admin(email, password, full_name)
