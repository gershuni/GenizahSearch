#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Migration: Add discovery voting, pinning, and answered status

This script adds:
1. is_pinned, is_answered, upvotes, downvotes columns to discoveries table
2. discovery_votes table for tracking individual votes

Run from project root: python -m backend.migrations.add_discovery_votes_and_pins
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text, inspect
from backend.models.database import engine, Base
from backend.models.discovery import DiscoveryVote


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
        # Add new columns to discoveries table
        if table_exists(conn, 'discoveries'):
            if not column_exists(conn, 'discoveries', 'is_pinned'):
                print("Adding is_pinned column to discoveries table...")
                conn.execute(text(
                    "ALTER TABLE discoveries ADD COLUMN is_pinned BOOLEAN DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("is_pinned column already exists.")

            if not column_exists(conn, 'discoveries', 'is_answered'):
                print("Adding is_answered column to discoveries table...")
                conn.execute(text(
                    "ALTER TABLE discoveries ADD COLUMN is_answered BOOLEAN DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("is_answered column already exists.")

            if not column_exists(conn, 'discoveries', 'upvotes'):
                print("Adding upvotes column to discoveries table...")
                conn.execute(text(
                    "ALTER TABLE discoveries ADD COLUMN upvotes INTEGER DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("upvotes column already exists.")

            if not column_exists(conn, 'discoveries', 'downvotes'):
                print("Adding downvotes column to discoveries table...")
                conn.execute(text(
                    "ALTER TABLE discoveries ADD COLUMN downvotes INTEGER DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("downvotes column already exists.")
        else:
            print("discoveries table does not exist!")

        # Create discovery_votes table
        if not table_exists(conn, 'discovery_votes'):
            print("Creating discovery_votes table...")
            DiscoveryVote.__table__.create(engine)
            print("  Done.")
        else:
            print("discovery_votes table already exists.")

    print("\nMigration completed successfully!")


if __name__ == "__main__":
    run_migration()
