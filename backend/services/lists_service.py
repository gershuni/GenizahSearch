"""
Lists Service - Business logic for user lists management.

Handles all CRUD operations for user lists, list items, projects,
and recent items. Includes migration from local storage.
"""
from typing import Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from ..models.user_list import UserList, ListItem, UserProject, RecentItem
from ..models.user import User
from ..schemas.user_list import (
    UserListCreate, UserListUpdate, ListItemCreate, ListItemUpdate,
    ProjectCreate, ProjectUpdate, MigrateListsRequest
)


MAX_RECENT_ITEMS = 50

DEFAULT_COLORS = [
    '#FFD700',  # Gold (default)
    '#4CAF50',  # Green
    '#2196F3',  # Blue
    '#9C27B0',  # Purple
    '#FF5722',  # Deep Orange
    '#00BCD4',  # Cyan
    '#E91E63',  # Pink
    '#795548',  # Brown
    '#607D8B',  # Blue Grey
    '#F44336',  # Red
]


class ListsService:
    """Service class for managing user lists."""

    # === List Operations ===

    @staticmethod
    def get_all_lists(db: Session, user_id: int, include_recent: bool = True) -> List[UserList]:
        """Get all lists for a user."""
        query = db.query(UserList).filter(UserList.user_id == user_id)
        if not include_recent:
            query = query.filter(UserList.is_system == False)
        return query.order_by(UserList.created_at).all()

    @staticmethod
    def get_list(db: Session, user_id: int, list_id: int) -> Optional[UserList]:
        """Get a specific list by ID."""
        return db.query(UserList).filter(
            UserList.id == list_id,
            UserList.user_id == user_id
        ).first()

    @staticmethod
    def create_list(db: Session, user_id: int, data: UserListCreate) -> UserList:
        """Create a new list for a user."""
        # Get next available color
        color = data.color
        if not color:
            used_colors = set(
                db.query(UserList.color)
                .filter(UserList.user_id == user_id)
                .distinct()
                .all()
            )
            used_colors = {c[0] for c in used_colors}
            for c in DEFAULT_COLORS:
                if c not in used_colors:
                    color = c
                    break
            if not color:
                color = DEFAULT_COLORS[0]

        new_list = UserList(
            user_id=user_id,
            name=data.name,
            name_en=data.name_en,
            color=color,
            project_id=data.project_id,
            is_default=False,
            is_system=False
        )
        db.add(new_list)
        db.commit()
        db.refresh(new_list)
        return new_list

    @staticmethod
    def update_list(
        db: Session, user_id: int, list_id: int, data: UserListUpdate
    ) -> Optional[UserList]:
        """Update a list's properties."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list or user_list.is_system:
            return None

        if data.name is not None:
            user_list.name = data.name
        if data.name_en is not None:
            user_list.name_en = data.name_en
        if data.color is not None:
            user_list.color = data.color
        if data.project_id is not None:
            user_list.project_id = data.project_id if data.project_id > 0 else None

        db.commit()
        db.refresh(user_list)
        return user_list

    @staticmethod
    def delete_list(db: Session, user_id: int, list_id: int) -> bool:
        """Delete a list and all its items."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list or user_list.is_default or user_list.is_system:
            return False

        db.delete(user_list)
        db.commit()
        return True

    @staticmethod
    def ensure_default_lists(db: Session, user_id: int) -> Tuple[UserList, UserList]:
        """Ensure user has default and recent lists, creating if needed."""
        # Check for default list
        default_list = db.query(UserList).filter(
            UserList.user_id == user_id,
            UserList.is_default == True
        ).first()

        if not default_list:
            default_list = UserList(
                user_id=user_id,
                name='General',
                name_en='General',
                color='#FFD700',
                is_default=True,
                is_system=False
            )
            db.add(default_list)

        # Check for recent list
        recent_list = db.query(UserList).filter(
            UserList.user_id == user_id,
            UserList.is_system == True
        ).first()

        if not recent_list:
            recent_list = UserList(
                user_id=user_id,
                name='Recently Viewed',
                name_en='Recently Viewed',
                color='#9E9E9E',
                is_default=False,
                is_system=True
            )
            db.add(recent_list)

        db.commit()
        db.refresh(default_list)
        db.refresh(recent_list)
        return default_list, recent_list

    # === Item Operations ===

    @staticmethod
    def get_items_in_list(db: Session, user_id: int, list_id: int) -> List[ListItem]:
        """Get all items in a list."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list:
            return []
        return db.query(ListItem).filter(
            ListItem.list_id == list_id
        ).order_by(desc(ListItem.added_at)).all()

    @staticmethod
    def add_item_to_list(
        db: Session, user_id: int, list_id: int, data: ListItemCreate
    ) -> Optional[ListItem]:
        """Add an item to a list."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list:
            return None

        # Check if item already exists in this list
        existing = db.query(ListItem).filter(
            ListItem.list_id == list_id,
            ListItem.sys_id == data.sys_id
        ).first()

        if existing:
            # Update existing item
            if data.note is not None:
                existing.note = data.note
            if data.tags is not None:
                existing.tags = data.tags
            if data.shelfmark:
                existing.shelfmark = data.shelfmark
            if data.title:
                existing.title = data.title
            db.commit()
            db.refresh(existing)
            return existing

        # Create new item
        new_item = ListItem(
            list_id=list_id,
            sys_id=data.sys_id,
            shelfmark=data.shelfmark,
            title=data.title,
            fl_id=data.fl_id,
            note=data.note,
            tags=data.tags or []
        )
        db.add(new_item)
        db.commit()
        db.refresh(new_item)
        return new_item

    @staticmethod
    def remove_item_from_list(
        db: Session, user_id: int, list_id: int, item_id: int
    ) -> bool:
        """Remove an item from a list."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list:
            return False

        item = db.query(ListItem).filter(
            ListItem.id == item_id,
            ListItem.list_id == list_id
        ).first()

        if not item:
            return False

        db.delete(item)
        db.commit()
        return True

    @staticmethod
    def update_item(
        db: Session, user_id: int, list_id: int, item_id: int, data: ListItemUpdate
    ) -> Optional[ListItem]:
        """Update an item's note or tags."""
        user_list = ListsService.get_list(db, user_id, list_id)
        if not user_list:
            return None

        item = db.query(ListItem).filter(
            ListItem.id == item_id,
            ListItem.list_id == list_id
        ).first()

        if not item:
            return None

        if data.note is not None:
            item.note = data.note
        if data.tags is not None:
            item.tags = data.tags

        db.commit()
        db.refresh(item)
        return item

    @staticmethod
    def is_item_in_any_list(db: Session, user_id: int, sys_id: str) -> List[int]:
        """Check which lists contain a specific item."""
        results = db.query(ListItem.list_id).join(UserList).filter(
            UserList.user_id == user_id,
            ListItem.sys_id == sys_id
        ).all()
        return [r[0] for r in results]

    # === Project Operations ===

    @staticmethod
    def get_projects(db: Session, user_id: int) -> List[UserProject]:
        """Get all projects for a user."""
        return db.query(UserProject).filter(
            UserProject.user_id == user_id
        ).order_by(UserProject.created_at).all()

    @staticmethod
    def create_project(db: Session, user_id: int, data: ProjectCreate) -> UserProject:
        """Create a new project."""
        # Get next available color
        color = data.color
        if not color:
            used_colors = set(
                db.query(UserProject.color)
                .filter(UserProject.user_id == user_id)
                .distinct()
                .all()
            )
            used_colors = {c[0] for c in used_colors}
            for c in DEFAULT_COLORS[1:]:  # Skip gold for projects
                if c not in used_colors:
                    color = c
                    break
            if not color:
                color = DEFAULT_COLORS[1]

        project = UserProject(
            user_id=user_id,
            name=data.name,
            color=color
        )
        db.add(project)
        db.commit()
        db.refresh(project)
        return project

    @staticmethod
    def update_project(
        db: Session, user_id: int, project_id: int, data: ProjectUpdate
    ) -> Optional[UserProject]:
        """Update a project's properties."""
        project = db.query(UserProject).filter(
            UserProject.id == project_id,
            UserProject.user_id == user_id
        ).first()

        if not project:
            return None

        if data.name is not None:
            project.name = data.name
        if data.color is not None:
            project.color = data.color

        db.commit()
        db.refresh(project)
        return project

    @staticmethod
    def delete_project(
        db: Session, user_id: int, project_id: int, delete_lists: bool = False
    ) -> bool:
        """Delete a project, optionally with its lists."""
        project = db.query(UserProject).filter(
            UserProject.id == project_id,
            UserProject.user_id == user_id
        ).first()

        if not project:
            return False

        if delete_lists:
            # Delete all lists in the project
            db.query(UserList).filter(
                UserList.project_id == project_id,
                UserList.user_id == user_id
            ).delete()
        else:
            # Just unassign lists from the project
            db.query(UserList).filter(
                UserList.project_id == project_id,
                UserList.user_id == user_id
            ).update({'project_id': None})

        db.delete(project)
        db.commit()
        return True

    # === Recent Items ===

    @staticmethod
    def get_recent_items(db: Session, user_id: int, limit: int = MAX_RECENT_ITEMS) -> List[RecentItem]:
        """Get recently viewed items for a user."""
        return db.query(RecentItem).filter(
            RecentItem.user_id == user_id
        ).order_by(desc(RecentItem.viewed_at)).limit(limit).all()

    @staticmethod
    def add_recent_item(
        db: Session, user_id: int, sys_id: str,
        shelfmark: str = None, title: str = None, fl_id: str = None
    ) -> RecentItem:
        """Add or update a recent item."""
        # Check if already exists
        existing = db.query(RecentItem).filter(
            RecentItem.user_id == user_id,
            RecentItem.sys_id == sys_id
        ).first()

        if existing:
            # Update viewed time
            existing.viewed_at = datetime.utcnow()
            if shelfmark:
                existing.shelfmark = shelfmark
            if title:
                existing.title = title
            db.commit()
            db.refresh(existing)
            return existing

        # Create new entry
        recent = RecentItem(
            user_id=user_id,
            sys_id=sys_id,
            shelfmark=shelfmark,
            title=title,
            fl_id=fl_id
        )
        db.add(recent)
        db.commit()

        # Clean up old entries (keep only MAX_RECENT_ITEMS)
        count = db.query(func.count(RecentItem.id)).filter(
            RecentItem.user_id == user_id
        ).scalar()

        if count > MAX_RECENT_ITEMS:
            # Delete oldest entries
            oldest = db.query(RecentItem).filter(
                RecentItem.user_id == user_id
            ).order_by(RecentItem.viewed_at).limit(count - MAX_RECENT_ITEMS).all()
            for item in oldest:
                db.delete(item)
            db.commit()

        db.refresh(recent)
        return recent

    # === Migration ===

    @staticmethod
    def migrate_local_lists(
        db: Session, user_id: int, data: MigrateListsRequest
    ) -> Tuple[int, int, int, int]:
        """
        Migrate local lists to user account.
        Returns (lists_migrated, items_migrated, projects_migrated, recent_migrated).
        """
        lists_migrated = 0
        items_migrated = 0
        projects_migrated = 0
        recent_migrated = 0

        # Create project mapping (local id -> db id)
        project_map = {}
        for proj_data in data.projects:
            project = ListsService.create_project(db, user_id, proj_data)
            # Store mapping using name as key since local ids are strings
            project_map[proj_data.name] = project.id
            projects_migrated += 1

        # Ensure default lists exist
        default_list, _ = ListsService.ensure_default_lists(db, user_id)

        # Create lists (skip if list with same name already exists)
        existing_lists = {lst.name: lst for lst in ListsService.get_all_lists(db, user_id)}

        for list_data in data.lists:
            # Map project if specified
            project_id = None
            if list_data.project_id and list_data.project_id in project_map:
                project_id = project_map[list_data.project_id]

            if list_data.is_default:
                # Merge with existing default list
                target_list = default_list
            elif list_data.name in existing_lists:
                # Use existing list with same name (skip creation)
                target_list = existing_lists[list_data.name]
            else:
                # Create new list
                create_data = UserListCreate(
                    name=list_data.name,
                    name_en=list_data.name_en,
                    color=list_data.color,
                    project_id=project_id
                )
                target_list = ListsService.create_list(db, user_id, create_data)
                lists_migrated += 1

            # Add items to the list
            for item_data in list_data.items:
                ListsService.add_item_to_list(db, user_id, target_list.id, item_data)
                items_migrated += 1

        # Migrate recent items
        for recent_data in data.recent_items:
            ListsService.add_recent_item(
                db, user_id, recent_data.sys_id,
                recent_data.shelfmark, recent_data.title, recent_data.fl_id
            )
            recent_migrated += 1

        return lists_migrated, items_migrated, projects_migrated, recent_migrated

    # === Tags ===

    @staticmethod
    def get_all_tags(db: Session, user_id: int) -> List[str]:
        """Get all unique tags used by a user."""
        items = db.query(ListItem.tags).join(UserList).filter(
            UserList.user_id == user_id
        ).all()

        all_tags = set()
        for (tags,) in items:
            if tags:
                all_tags.update(tags)

        return sorted(all_tags)
