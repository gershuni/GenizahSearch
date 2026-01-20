"""
Migration: Add fragment_joins table
Created: January 2026
"""
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text,
    ForeignKey, Index, MetaData, Table, inspect
)
from datetime import datetime


def upgrade(engine):
    """Create the fragment_joins table"""
    inspector = inspect(engine)

    # Check if table already exists
    if 'fragment_joins' in inspector.get_table_names():
        print("Table 'fragment_joins' already exists, skipping creation")
        return

    metadata = MetaData()

    fragment_joins = Table(
        'fragment_joins',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),

        # The two joined fragments (stored alphabetically for deduplication)
        Column('fragment_a', String(200), nullable=False),
        Column('fragment_b', String(200), nullable=False),

        # Optional: sys_ids for faster lookups
        Column('document_id_a', String(100), nullable=True),
        Column('document_id_b', String(100), nullable=True),

        # Relationship type (optional)
        Column('relationship_type', String(50), nullable=True),

        # Metadata
        Column('notes', Text, nullable=True),
        Column('source', String(50), default='user'),
        Column('source_url', Text, nullable=True),

        # Tracking
        Column('created_by', Integer, ForeignKey('users.id'), nullable=True),
        Column('created_at', DateTime, default=datetime.utcnow),
        Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),

        # Soft delete
        Column('is_active', Boolean, default=True),

        # Indexes
        Index('ix_fragment_joins_a', 'fragment_a'),
        Index('ix_fragment_joins_b', 'fragment_b'),
        Index('ix_fragment_joins_source', 'source'),
        Index('ix_fragment_joins_active', 'is_active'),
        Index('ix_fragment_joins_unique', 'fragment_a', 'fragment_b', unique=True),
    )

    metadata.create_all(engine)
    print("Created table 'fragment_joins' with indexes")


def downgrade(engine):
    """Drop the fragment_joins table"""
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if 'fragment_joins' in metadata.tables:
        metadata.tables['fragment_joins'].drop(engine)
        print("Dropped table 'fragment_joins'")
    else:
        print("Table 'fragment_joins' does not exist")


if __name__ == "__main__":
    # For manual testing
    from ..models.database import engine
    upgrade(engine)
