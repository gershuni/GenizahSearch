#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Migration: Add Discoveries and Anonymous Fields

This script adds:
1. is_anonymous column to corrections table
2. is_anonymous column to comments table
3. discoveries table
4. discovery_responses table

Run from project root: python -m backend.migrations.add_discoveries_and_anonymous
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text, inspect
from backend.models.database import engine, Base
from backend.models import Discovery, DiscoveryResponse


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
        # 1. Add is_anonymous to corrections table
        if table_exists(conn, 'corrections'):
            if not column_exists(conn, 'corrections', 'is_anonymous'):
                print("Adding is_anonymous column to corrections table...")
                conn.execute(text(
                    "ALTER TABLE corrections ADD COLUMN is_anonymous BOOLEAN DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("is_anonymous column already exists in corrections table.")

        # 2. Add is_anonymous to comments table
        if table_exists(conn, 'comments'):
            if not column_exists(conn, 'comments', 'is_anonymous'):
                print("Adding is_anonymous column to comments table...")
                conn.execute(text(
                    "ALTER TABLE comments ADD COLUMN is_anonymous BOOLEAN DEFAULT 0"
                ))
                conn.commit()
                print("  Done.")
            else:
                print("is_anonymous column already exists in comments table.")

        # 3. Create discoveries table if it doesn't exist
        if not table_exists(conn, 'discoveries'):
            print("Creating discoveries table...")
            Discovery.__table__.create(engine)
            print("  Done.")
        else:
            print("discoveries table already exists.")

        # 4. Create discovery_responses table if it doesn't exist
        if not table_exists(conn, 'discovery_responses'):
            print("Creating discovery_responses table...")
            DiscoveryResponse.__table__.create(engine)
            print("  Done.")
        else:
            print("discovery_responses table already exists.")

    print("\nMigration completed successfully!")


if __name__ == "__main__":
    run_migration()
