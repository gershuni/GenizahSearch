#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Migration: Add page_number column to comments table

This script adds:
1. page_number column to comments table (nullable integer)

This separates the semantic confusion between line_number (line within text)
and page_number (page/image number in manuscript).

Run from project root: python -m backend.migrations.add_comments_page_number
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
    print("Starting database migration: add_comments_page_number...")

    with engine.connect() as conn:
        # Add page_number to comments table
        if table_exists(conn, 'comments'):
            if not column_exists(conn, 'comments', 'page_number'):
                print("Adding page_number column to comments table...")
                conn.execute(text(
                    "ALTER TABLE comments ADD COLUMN page_number INTEGER"
                ))
                conn.commit()
                print("  Done.")

                # Migrate existing data: copy line_number to page_number where it seems
                # to be used as a page number (this is a best-effort migration)
                print("Migrating existing line_number values to page_number...")
                conn.execute(text(
                    "UPDATE comments SET page_number = line_number WHERE line_number IS NOT NULL"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("page_number column already exists in comments table.")
        else:
            print("comments table does not exist!")

    print("\nMigration completed successfully!")


if __name__ == "__main__":
    run_migration()
