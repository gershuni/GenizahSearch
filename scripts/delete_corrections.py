#!/usr/bin/env python3
"""
Script to list and delete corrections from the database.
Run this from the project root directory.

Usage:
    python scripts/delete_corrections.py list          # List all corrections
    python scripts/delete_corrections.py delete <id>   # Delete a specific correction
    python scripts/delete_corrections.py delete-all    # Delete ALL corrections (careful!)
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.models.database import SessionLocal
from backend.models.correction import Correction
from backend.models.transcription_version import TranscriptionVersion


def list_corrections():
    """List all corrections in the database."""
    db = SessionLocal()
    try:
        corrections = db.query(Correction).order_by(Correction.created_at.desc()).all()

        if not corrections:
            print("No corrections found.")
            return

        print(f"\n{'ID':<5} {'Status':<12} {'Doc ID':<20} {'Page':<5} {'Author':<15} {'Created':<20}")
        print("-" * 80)

        for c in corrections:
            author_name = c.author.username if c.author else "N/A"
            created = c.created_at.strftime("%Y-%m-%d %H:%M") if c.created_at else "N/A"
            doc_id = (c.document_id or c.system_id or "N/A")[:18]

            print(f"{c.id:<5} {c.status.value:<12} {doc_id:<20} {c.page_number or 0:<5} {author_name:<15} {created:<20}")

        print(f"\nTotal: {len(corrections)} corrections")

    finally:
        db.close()


def delete_correction(correction_id: int):
    """Delete a specific correction and its associated version."""
    db = SessionLocal()
    try:
        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            print(f"Correction {correction_id} not found.")
            return False

        # Also delete associated TranscriptionVersion if exists
        version = db.query(TranscriptionVersion).filter(
            TranscriptionVersion.correction_id == correction_id
        ).first()

        if version:
            print(f"Deleting associated version {version.id}...")
            db.delete(version)

        print(f"Deleting correction {correction_id} (status: {correction.status.value})...")
        db.delete(correction)
        db.commit()
        print("Done!")
        return True

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        return False
    finally:
        db.close()


def delete_all_corrections():
    """Delete ALL corrections and associated versions."""
    from backend.models.transcription_version import PageDefault

    db = SessionLocal()
    try:
        # Get version IDs that are linked to corrections
        version_ids = [v.id for v in db.query(TranscriptionVersion).filter(
            TranscriptionVersion.correction_id.isnot(None)
        ).all()]

        # First delete PageDefault entries that reference these versions
        if version_ids:
            page_defaults_deleted = db.query(PageDefault).filter(
                PageDefault.default_version_id.in_(version_ids)
            ).delete(synchronize_session=False)
            print(f"Deleted {page_defaults_deleted} page default entries.")

        # Then delete the versions
        versions_deleted = db.query(TranscriptionVersion).filter(
            TranscriptionVersion.correction_id.isnot(None)
        ).delete(synchronize_session=False)
        print(f"Deleted {versions_deleted} transcription versions.")

        # Finally delete all corrections
        corrections_deleted = db.query(Correction).delete(synchronize_session=False)
        print(f"Deleted {corrections_deleted} corrections.")

        db.commit()
        print("Done!")
        return True

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        return False
    finally:
        db.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "list":
        list_corrections()

    elif command == "delete":
        if len(sys.argv) < 3:
            print("Usage: python scripts/delete_corrections.py delete <correction_id>")
            sys.exit(1)
        try:
            correction_id = int(sys.argv[2])
            delete_correction(correction_id)
        except ValueError:
            print("Invalid correction ID. Must be a number.")
            sys.exit(1)

    elif command == "delete-all":
        confirm = input("Are you sure you want to delete ALL corrections? (yes/no): ")
        if confirm.lower() == "yes":
            delete_all_corrections()
        else:
            print("Cancelled.")

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
