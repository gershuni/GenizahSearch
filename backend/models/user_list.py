"""
User Lists Model - Personal manuscript lists with per-user storage.

Enables users to save, organize, and sync their personal manuscript lists
across devices. Lists are associated with user accounts and stored in the
database, with support for tags, notes, and project organization.
"""
import enum
from datetime import datetime
from typing import Optional, List as TypeList
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, ForeignKey, Index, JSON
)
from sqlalchemy.orm import relationship
from .database import Base


class UserProject(Base):
    """
    Project for organizing lists.

    Projects group related lists together (e.g., "Research Paper 2024").
    """
    __tablename__ = "user_projects"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    color = Column(String(20), default='#4CAF50')  # Hex color
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("User", backref="projects")
    lists = relationship("UserList", back_populates="project", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_user_projects_user', 'user_id'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'color': self.color,
            'created': self.created_at.timestamp() if self.created_at else None
        }


class UserList(Base):
    """
    Personal manuscript list belonging to a user.

    Supports multiple named lists with colors, optional project grouping,
    and system lists (like "Recently Viewed").
    """
    __tablename__ = "user_lists"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    name_en = Column(String(255), nullable=True)  # English name for translations
    color = Column(String(20), default='#FFD700')  # Hex color (gold default)
    is_default = Column(Boolean, default=False)  # The main "General" list
    is_system = Column(Boolean, default=False)   # System-managed lists (Recently Viewed)
    project_id = Column(Integer, ForeignKey("user_projects.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("User", backref="lists")
    project = relationship("UserProject", back_populates="lists")
    items = relationship("ListItem", back_populates="list", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_user_lists_user', 'user_id'),
        Index('ix_user_lists_project', 'project_id'),
    )

    def to_dict(self, include_items=False):
        result = {
            'id': str(self.id),  # String ID for compatibility with existing code
            'name': self.name,
            'name_en': self.name_en,
            'color': self.color,
            'is_default': self.is_default,
            'is_system': self.is_system,
            'project_id': str(self.project_id) if self.project_id else None,
            'created': self.created_at.timestamp() if self.created_at else None,
            'count': len(self.items) if self.items else 0
        }
        if include_items:
            result['items'] = [item.to_dict() for item in self.items]
        return result


class ListItem(Base):
    """
    Item in a user's list (a saved manuscript).

    Each item links a manuscript (by sys_id) to a list, with optional
    notes and tags for organization.
    """
    __tablename__ = "list_items"

    id = Column(Integer, primary_key=True, index=True)
    list_id = Column(Integer, ForeignKey("user_lists.id", ondelete="CASCADE"), nullable=False, index=True)
    sys_id = Column(String(100), nullable=False)  # Manuscript system ID
    shelfmark = Column(String(255), nullable=True)  # Cached shelfmark
    title = Column(String(500), nullable=True)  # Cached title
    fl_id = Column(String(100), nullable=True)  # Fragment Library ID
    note = Column(Text, nullable=True)  # User's personal note
    tags = Column(JSON, default=list)  # List of tag strings
    added_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    list = relationship("UserList", back_populates="items")

    __table_args__ = (
        Index('ix_list_items_list', 'list_id'),
        Index('ix_list_items_sys_id', 'sys_id'),
        Index('ix_list_items_list_sys', 'list_id', 'sys_id'),  # For duplicate checking
    )

    def to_dict(self):
        return {
            'item_id': str(self.id),
            'sys_id': self.sys_id,
            'shelfmark': self.shelfmark,
            'title': self.title,
            'fl_id': self.fl_id,
            'note': self.note,
            'tags': self.tags or [],
            'added_at': self.added_at.timestamp() if self.added_at else None,
            'lists': [str(self.list_id)]  # For compatibility with existing code
        }


class RecentItem(Base):
    """
    Recently viewed items for a user.

    Automatically populated when user views manuscripts.
    Limited to 50 most recent items per user.
    """
    __tablename__ = "recent_items"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    sys_id = Column(String(100), nullable=False)
    shelfmark = Column(String(255), nullable=True)
    title = Column(String(500), nullable=True)
    fl_id = Column(String(100), nullable=True)
    viewed_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("User", backref="recent_items")

    __table_args__ = (
        Index('ix_recent_items_user', 'user_id'),
        Index('ix_recent_items_user_viewed', 'user_id', 'viewed_at'),
    )

    def to_dict(self):
        return {
            'item_id': str(self.id),
            'sys_id': self.sys_id,
            'shelfmark': self.shelfmark,
            'title': self.title,
            'fl_id': self.fl_id,
            'viewed_at': self.viewed_at.timestamp() if self.viewed_at else None
        }
