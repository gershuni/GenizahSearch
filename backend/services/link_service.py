"""
Link Service - Business logic for fragment links
Handles creating, querying, and managing pairwise fragment links
"""
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

from ..models.fragment_link import FragmentLink, normalize_shelfmark, normalize_link_order
from ..models.user import User


class LinkService:
    """Service for managing fragment links"""

    @staticmethod
    def create_link(
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
    ) -> Tuple[Optional[FragmentLink], Optional[str]]:
        """
        Create a link between two fragments.

        Returns (link, None) on success, (None, error_message) on failure.
        """
        # Normalize and order fragments
        norm_a, norm_b = normalize_link_order(fragment_a, fragment_b)

        # Check if same fragment (after normalization)
        if norm_a == norm_b:
            return None, "Cannot link a fragment to itself"

        # Check if link already exists
        existing = db.query(FragmentLink).filter(
            FragmentLink.fragment_a == norm_a,
            FragmentLink.fragment_b == norm_b,
            FragmentLink.is_active == True
        ).first()

        if existing:
            return None, f"Link already exists (id: {existing.id})"

        # Reorder document_ids if fragments were swapped
        orig_a_norm = normalize_shelfmark(fragment_a)
        if document_id_a and document_id_b and orig_a_norm != norm_a:
            document_id_a, document_id_b = document_id_b, document_id_a

        # Create the link
        link = FragmentLink(
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

        db.add(link)
        db.commit()
        db.refresh(link)

        return link, None

    @staticmethod
    def get_link_by_id(db: Session, link_id: int) -> Optional[FragmentLink]:
        """Get a link by ID"""
        return db.query(FragmentLink).filter(
            FragmentLink.id == link_id,
            FragmentLink.is_active == True
        ).first()

    @staticmethod
    def get_direct_links(db: Session, shelfmark: str) -> List[FragmentLink]:
        """Get all direct links for a fragment"""
        normalized = normalize_shelfmark(shelfmark)

        return db.query(FragmentLink).filter(
            FragmentLink.is_active == True,
            or_(
                FragmentLink.fragment_a == normalized,
                FragmentLink.fragment_b == normalized
            )
        ).all()

    @staticmethod
    def get_connected_fragments(db: Session, shelfmark: str) -> Dict[str, Any]:
        """
        Get all fragments in the same connected component.
        Uses BFS to traverse the link graph.

        Returns dict with:
        - shelfmark: original shelfmark
        - shelfmark_normalized: normalized shelfmark
        - fragments: list of all connected shelfmarks
        - fragment_details: list with relationship info
        - links: all links in the component
        - total_fragments: count
        - total_links: count
        """
        normalized = normalize_shelfmark(shelfmark)

        # BFS to find all connected fragments
        visited = set()
        to_visit = [normalized]
        links_found = []
        fragment_to_link = {}  # Track how each fragment is connected

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)

            # Get all direct links for current fragment
            direct_links = db.query(FragmentLink).filter(
                FragmentLink.is_active == True,
                or_(
                    FragmentLink.fragment_a == current,
                    FragmentLink.fragment_b == current
                )
            ).all()

            for link in direct_links:
                links_found.append(link)

                # Determine the other fragment
                other = link.fragment_b if link.fragment_a == current else link.fragment_a

                # Track relationship info
                if other not in fragment_to_link:
                    fragment_to_link[other] = {
                        'link_id': link.id,
                        'relationship_type': link.relationship_type,
                        'source': link.source,
                        'document_id': link.document_id_b if link.fragment_a == current else link.document_id_a
                    }

                if other not in visited:
                    to_visit.append(other)

        # Deduplicate links
        unique_links = list({link.id: link for link in links_found}.values())

        # Build fragment details
        fragment_details = []
        for frag in sorted(visited):
            is_current = (frag == normalized)
            detail = {
                'shelfmark': frag,
                'is_current': is_current,
                'document_id': None,
                'relationship_type': None,
                'link_id': None,
                'link_source': None
            }

            if not is_current and frag in fragment_to_link:
                info = fragment_to_link[frag]
                detail['link_id'] = info['link_id']
                detail['relationship_type'] = info['relationship_type']
                detail['link_source'] = info['source']
                detail['document_id'] = info['document_id']

            fragment_details.append(detail)

        return {
            "shelfmark": shelfmark,
            "shelfmark_normalized": normalized,
            "fragments": sorted(list(visited)),
            "fragment_details": fragment_details,
            "links": unique_links,
            "total_fragments": len(visited),
            "total_links": len(unique_links)
        }

    @staticmethod
    def delete_link(
        db: Session,
        link_id: int,
        user: Optional[User] = None
    ) -> bool:
        """Soft delete a link"""
        link = db.query(FragmentLink).filter(
            FragmentLink.id == link_id,
            FragmentLink.is_active == True
        ).first()

        if not link:
            return False

        link.is_active = False
        link.updated_at = datetime.utcnow()
        db.commit()

        return True

    @staticmethod
    def update_link(
        db: Session,
        link_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Optional[FragmentLink]:
        """Update link metadata"""
        link = db.query(FragmentLink).filter(
            FragmentLink.id == link_id,
            FragmentLink.is_active == True
        ).first()

        if not link:
            return None

        if relationship_type is not None:
            link.relationship_type = relationship_type
        if notes is not None:
            link.notes = notes

        link.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(link)

        return link

    @staticmethod
    def search_links(
        db: Session,
        query: Optional[str] = None,
        source: Optional[str] = None,
        relationship_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[FragmentLink], int]:
        """
        Search links by shelfmark pattern or filters.

        Returns (links, total_count)
        """
        q = db.query(FragmentLink).filter(FragmentLink.is_active == True)

        if query:
            pattern = f"%{query.upper()}%"
            q = q.filter(
                or_(
                    FragmentLink.fragment_a.ilike(pattern),
                    FragmentLink.fragment_b.ilike(pattern)
                )
            )

        if source:
            q = q.filter(FragmentLink.source == source)

        if relationship_type:
            q = q.filter(FragmentLink.relationship_type == relationship_type)

        total = q.count()
        links = q.order_by(FragmentLink.created_at.desc()).offset(offset).limit(limit).all()

        return links, total

    @staticmethod
    def get_link_between(
        db: Session,
        fragment_a: str,
        fragment_b: str
    ) -> Optional[FragmentLink]:
        """Get link between two specific fragments if it exists"""
        norm_a, norm_b = normalize_link_order(fragment_a, fragment_b)

        return db.query(FragmentLink).filter(
            FragmentLink.fragment_a == norm_a,
            FragmentLink.fragment_b == norm_b,
            FragmentLink.is_active == True
        ).first()

    @staticmethod
    def bulk_create_links(
        db: Session,
        shelfmarks: List[str],
        relationship_type: Optional[str] = None,
        source: str = "import",
        source_url: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create pairwise links between all provided shelfmarks.
        Used for Princeton imports where multiple fragments form one document.

        Returns dict with created count and any errors.
        """
        created = 0
        skipped = 0
        errors = []

        # Create links between all pairs
        for i, frag_a in enumerate(shelfmarks):
            for frag_b in shelfmarks[i + 1:]:
                link, error = LinkService.create_link(
                    db=db,
                    fragment_a=frag_a,
                    fragment_b=frag_b,
                    relationship_type=relationship_type,
                    source=source,
                    source_url=source_url,
                    notes=notes
                )

                if link:
                    created += 1
                elif error and "already exists" in error:
                    skipped += 1
                else:
                    errors.append(f"{frag_a} <-> {frag_b}: {error}")

        return {
            "shelfmarks": shelfmarks,
            "links_created": created,
            "links_skipped": skipped,
            "errors": errors
        }
