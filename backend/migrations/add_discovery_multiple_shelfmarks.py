"""
Migration: Add multiple shelfmarks and related manuscripts to discoveries

Adds:
- additional_shelfmarks: JSON array for multiple shelfmarks
- related_manuscripts: JSON array for linked manuscripts

Run this migration to update existing databases.
"""
import sqlite3
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_db_path() -> Path:
    """Get the database path"""
    # Check common locations
    paths = [
        Path("backend/genizah_corrections.db"),
        Path("genizah_corrections.db"),
        Path("data/genizah_corrections.db"),
    ]
    for p in paths:
        if p.exists():
            return p
    # Return default
    return Path("backend/genizah_corrections.db")


def migrate():
    """Run the migration"""
    db_path = get_db_path()

    if not db_path.exists():
        logger.warning(f"Database not found at {db_path}, skipping migration")
        return False

    logger.info(f"Running migration on {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if columns exist
        cursor.execute("PRAGMA table_info(discoveries)")
        columns = [col[1] for col in cursor.fetchall()]

        # Add additional_shelfmarks column if not exists
        if 'additional_shelfmarks' not in columns:
            logger.info("Adding additional_shelfmarks column to discoveries")
            cursor.execute("""
                ALTER TABLE discoveries
                ADD COLUMN additional_shelfmarks TEXT DEFAULT '[]'
            """)
            logger.info("Added additional_shelfmarks column")
        else:
            logger.info("additional_shelfmarks column already exists")

        # Add related_manuscripts column if not exists
        if 'related_manuscripts' not in columns:
            logger.info("Adding related_manuscripts column to discoveries")
            cursor.execute("""
                ALTER TABLE discoveries
                ADD COLUMN related_manuscripts TEXT DEFAULT '[]'
            """)
            logger.info("Added related_manuscripts column")
        else:
            logger.info("related_manuscripts column already exists")

        conn.commit()
        logger.info("Migration completed successfully")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def rollback():
    """Rollback the migration (SQLite doesn't support DROP COLUMN easily)"""
    logger.warning("Rollback for this migration requires recreating the table")
    # In SQLite, you'd need to recreate the table without the columns
    # This is left as a manual operation for safety
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    migrate()
