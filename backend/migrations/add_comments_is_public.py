#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Migration: Add is_public column to comments table

This script adds:
1. is_public column to comments table (defaults to True/1)

Run from project root: python -m backend.migrations.add_comments_is_public
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text, inspect
from backend.models.database import engine


def column_exists(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    inspector = inspect(conn)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def table_exists(conn, table_name: str) -> bool:
    """Check if a table exists."""
    inspector = inspect(conn)
    return table_name in inspector.get_table_names()


def run_migration():
    """Run the database migration."""
    print("Starting database migration...")

    with engine.connect() as conn:
        # Add is_public to comments table
        if table_exists(conn, 'comments'):
            if not column_exists(conn, 'comments', 'is_public'):
                print("Adding is_public column to comments table...")
                conn.execute(text(
                    "ALTER TABLE comments ADD COLUMN is_public BOOLEAN DEFAULT 1"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("is_public column already exists in comments table.")
        else:
            print("comments table does not exist!")

    print("\nMigration completed successfully!")


if __name__ == "__main__":
    run_migration()
