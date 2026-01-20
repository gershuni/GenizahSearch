"""
Join Service - Business logic for fragment joins
Handles creating, querying, and managing pairwise fragment joins
"""
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

from ..models.fragment_join import FragmentJoin, normalize_shelfmark, normalize_join_order
from ..models.user import User


class JoinService:
    """Service for managing fragment joins"""

    @staticmethod
    def create_join(
        db: Session,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
        source: str = "user",
        source_url: Optional[str] = None,
        user: Optional[User] = None,
        document_id_a: Optional[str] = None,
        document_id_b: Optional[str] = None
    ) -> Tuple[Optional[FragmentJoin], Optional[str]]:
        """
        Create a join between two fragments.

        Returns (join, None) on success, (None, error_message) on failure.
        """
        # Normalize and order fragments
        norm_a, norm_b = normalize_join_order(fragment_a, fragment_b)

        # Check if same fragment (after normalization)
        if norm_a == norm_b:
            return None, "Cannot join a fragment to itself"

        # Check if join already exists
        existing = db.query(FragmentJoin).filter(
            FragmentJoin.fragment_a == norm_a,
            FragmentJoin.fragment_b == norm_b,
            FragmentJoin.is_active == True
        ).first()

        if existing:
            return None, f"Join already exists (id: {existing.id})"

        # Reorder document_ids if fragments were swapped
        orig_a_norm = normalize_shelfmark(fragment_a)
        if document_id_a and document_id_b and orig_a_norm != norm_a:
            document_id_a, document_id_b = document_id_b, document_id_a

        # Create the join
        join = FragmentJoin(
            fragment_a=norm_a,
            fragment_b=norm_b,
            relationship_type=relationship_type,
            notes=notes,
            source=source,
            source_url=source_url,
            created_by=user.id if user else None,
            document_id_a=document_id_a,
            document_id_b=document_id_b
        )

        db.add(join)
        db.commit()
        db.refresh(join)

        return join, None

    @staticmethod
    def get_join_by_id(db: Session, join_id: int) -> Optional[FragmentJoin]:
        """Get a join by ID"""
        return db.query(FragmentJoin).filter(
            FragmentJoin.id == join_id,
            FragmentJoin.is_active == True
        ).first()

    @staticmethod
    def get_direct_joins(db: Session, shelfmark: str) -> List[FragmentJoin]:
        """Get all direct joins for a fragment"""
        normalized = normalize_shelfmark(shelfmark)

        return db.query(FragmentJoin).filter(
            FragmentJoin.is_active == True,
            or_(
                FragmentJoin.fragment_a == normalized,
                FragmentJoin.fragment_b == normalized
            )
        ).all()

    @staticmethod
    def get_connected_fragments(db: Session, shelfmark: str) -> Dict[str, Any]:
        """
        Get all fragments in the same connected component.
        Uses BFS to traverse the join graph.

        Returns dict with:
        - shelfmark: original shelfmark
        - shelfmark_normalized: normalized shelfmark
        - fragments: list of all connected shelfmarks
        - fragment_details: list with relationship info
        - joins: all joins in the component
        - total_fragments: count
        - total_joins: count
        """
        normalized = normalize_shelfmark(shelfmark)

        # BFS to find all connected fragments
        visited = set()
        to_visit = [normalized]
        joins_found = []
        fragment_to_join = {}  # Track how each fragment is connected

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)

            # Get all direct joins for current fragment
            direct_joins = db.query(FragmentJoin).filter(
                FragmentJoin.is_active == True,
                or_(
                    FragmentJoin.fragment_a == current,
                    FragmentJoin.fragment_b == current
                )
            ).all()

            for join in direct_joins:
                joins_found.append(join)

                # Determine the other fragment
                other = join.fragment_b if join.fragment_a == current else join.fragment_a

                # Track relationship info
                if other not in fragment_to_join:
                    fragment_to_join[other] = {
                        'join_id': join.id,
                        'relationship_type': join.relationship_type,
                        'source': join.source,
                        'document_id': join.document_id_b if join.fragment_a == current else join.document_id_a
                    }

                if other not in visited:
                    to_visit.append(other)

        # Deduplicate joins
        unique_joins = list({join.id: join for join in joins_found}.values())

        # Build fragment details
        fragment_details = []
        for frag in sorted(visited):
            is_current = (frag == normalized)
            detail = {
                'shelfmark': frag,
                'is_current': is_current,
                'document_id': None,
                'relationship_type': None,
                'join_id': None,
                'join_source': None
            }

            if not is_current and frag in fragment_to_join:
                info = fragment_to_join[frag]
                detail['join_id'] = info['join_id']
                detail['relationship_type'] = info['relationship_type']
                detail['join_source'] = info['source']
                detail['document_id'] = info['document_id']

            fragment_details.append(detail)

        return {
            "shelfmark": shelfmark,
            "shelfmark_normalized": normalized,
            "fragments": sorted(list(visited)),
            "fragment_details": fragment_details,
            "joins": unique_joins,
            "total_fragments": len(visited),
            "total_joins": len(unique_joins)
        }

    @staticmethod
    def get_connected_fragments_by_document_id(db: Session, document_id: str) -> Dict[str, Any]:
        """
        Get all fragments in the same connected component using document_id (sys_id).
        Uses BFS to traverse the join graph via document_id fields.

        Returns dict with same structure as get_connected_fragments.
        """
        if not document_id:
            return {
                "shelfmark": None,
                "shelfmark_normalized": None,
                "fragments": [],
                "fragment_details": [],
                "joins": [],
                "total_fragments": 0,
                "total_joins": 0
            }

        # BFS to find all connected fragments via document_id
        visited_doc_ids = set()
        to_visit = [document_id]
        joins_found = []
        doc_id_to_shelfmark = {}  # Map document_id -> shelfmark
        fragment_to_join = {}  # Track how each fragment is connected

        while to_visit:
            current_doc_id = to_visit.pop(0)
            if current_doc_id in visited_doc_ids:
                continue
            visited_doc_ids.add(current_doc_id)

            # Get all direct joins for current document_id
            direct_joins = db.query(FragmentJoin).filter(
                FragmentJoin.is_active == True,
                or_(
                    FragmentJoin.document_id_a == current_doc_id,
                    FragmentJoin.document_id_b == current_doc_id
                )
            ).all()

            for join in direct_joins:
                joins_found.append(join)

                # Map document_ids to shelfmarks
                if join.document_id_a:
                    doc_id_to_shelfmark[join.document_id_a] = join.fragment_a
                if join.document_id_b:
                    doc_id_to_shelfmark[join.document_id_b] = join.fragment_b

                # Determine the other document_id
                if join.document_id_a == current_doc_id:
                    other_doc_id = join.document_id_b
                    other_shelfmark = join.fragment_b
                else:
                    other_doc_id = join.document_id_a
                    other_shelfmark = join.fragment_a

                # Track relationship info using shelfmark as key
                if other_shelfmark and other_shelfmark not in fragment_to_join:
                    fragment_to_join[other_shelfmark] = {
                        'join_id': join.id,
                        'relationship_type': join.relationship_type,
                        'source': join.source,
                        'document_id': other_doc_id
                    }

                if other_doc_id and other_doc_id not in visited_doc_ids:
                    to_visit.append(other_doc_id)

        # Deduplicate joins
        unique_joins = list({join.id: join for join in joins_found}.values())

        # Get the shelfmark for the original document_id
        original_shelfmark = doc_id_to_shelfmark.get(document_id, document_id)

        # Build fragment list from shelfmarks
        visited_shelfmarks = set(doc_id_to_shelfmark.values())

        # Build fragment details
        fragment_details = []
        for frag in sorted(visited_shelfmarks):
            is_current = (frag == original_shelfmark)
            detail = {
                'shelfmark': frag,
                'is_current': is_current,
                'document_id': None,
                'relationship_type': None,
                'join_id': None,
                'join_source': None
            }

            if not is_current and frag in fragment_to_join:
                info = fragment_to_join[frag]
                detail['join_id'] = info['join_id']
                detail['relationship_type'] = info['relationship_type']
                detail['join_source'] = info['source']
                detail['document_id'] = info['document_id']

            fragment_details.append(detail)

        return {
            "shelfmark": original_shelfmark,
            "shelfmark_normalized": normalize_shelfmark(original_shelfmark) if original_shelfmark else None,
            "fragments": sorted(list(visited_shelfmarks)),
            "fragment_details": fragment_details,
            "joins": unique_joins,
            "total_fragments": len(visited_shelfmarks),
            "total_joins": len(unique_joins)
        }

    @staticmethod
    def delete_join(
        db: Session,
        join_id: int,
        user: Optional[User] = None
    ) -> bool:
        """Soft delete a join"""
        join = db.query(FragmentJoin).filter(
            FragmentJoin.id == join_id,
            FragmentJoin.is_active == True
        ).first()

        if not join:
            return False

        join.is_active = False
        join.updated_at = datetime.utcnow()
        db.commit()

        return True

    @staticmethod
    def update_join(
        db: Session,
        join_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Optional[FragmentJoin]:
        """Update join metadata"""
        join = db.query(FragmentJoin).filter(
            FragmentJoin.id == join_id,
            FragmentJoin.is_active == True
        ).first()

        if not join:
            return None

        if relationship_type is not None:
            join.relationship_type = relationship_type
        if notes is not None:
            join.notes = notes

        join.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(join)

        return join

    @staticmethod
    def search_joins(
        db: Session,
        query: Optional[str] = None,
        source: Optional[str] = None,
        relationship_type: Optional[str] = None,
        created_by_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[FragmentJoin], int]:
        """
        Search joins by shelfmark pattern or filters.

        Returns (joins, total_count)
        """
        q = db.query(FragmentJoin).filter(FragmentJoin.is_active == True)

        if query:
            pattern = f"%{query.upper()}%"
            q = q.filter(
                or_(
                    FragmentJoin.fragment_a.ilike(pattern),
                    FragmentJoin.fragment_b.ilike(pattern)
                )
            )

        if source:
            q = q.filter(FragmentJoin.source == source)

        if relationship_type:
            q = q.filter(FragmentJoin.relationship_type == relationship_type)

        if created_by_id:
            q = q.filter(FragmentJoin.created_by == created_by_id)

        total = q.count()
        joins = q.order_by(FragmentJoin.created_at.desc()).offset(offset).limit(limit).all()

        return joins, total

    @staticmethod
    def get_join_between(
        db: Session,
        fragment_a: str,
        fragment_b: str
    ) -> Optional[FragmentJoin]:
        """Get join between two specific fragments if it exists"""
        norm_a, norm_b = normalize_join_order(fragment_a, fragment_b)

        return db.query(FragmentJoin).filter(
            FragmentJoin.fragment_a == norm_a,
            FragmentJoin.fragment_b == norm_b,
            FragmentJoin.is_active == True
        ).first()

    @staticmethod
    def bulk_create_joins(
        db: Session,
        shelfmarks: List[str],
        relationship_type: Optional[str] = None,
        source: str = "import",
        source_url: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create pairwise joins between all provided shelfmarks.
        Used for Princeton imports where multiple fragments form one document.

        Returns dict with created count and any errors.
        """
        created = 0
        skipped = 0
        errors = []

        # Create joins between all pairs
        for i, frag_a in enumerate(shelfmarks):
            for frag_b in shelfmarks[i + 1:]:
                join, error = JoinService.create_join(
                    db=db,
                    fragment_a=frag_a,
                    fragment_b=frag_b,
                    relationship_type=relationship_type,
                    source=source,
                    source_url=source_url,
                    notes=notes
                )

                if join:
                    created += 1
                elif error and "already exists" in error:
                    skipped += 1
                else:
                    errors.append(f"{frag_a} <-> {frag_b}: {error}")

        return {
            "shelfmarks": shelfmarks,
            "joins_created": created,
            "joins_skipped": skipped,
            "errors": errors
        }
