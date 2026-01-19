"""
Version Indexer - Indexes TranscriptionVersions in Tantivy for search integration

Indexes transcription versions (V0.7, V0.8, user corrections) as a separate
searchable field that can be combined with the main document search.

The index allows:
- Searching across all transcription versions
- Filtering by source (V0.7, V0.8, user)
- Finding documents with user corrections
"""
import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from ..models.transcription_version import TranscriptionVersion, VersionSource

logger = logging.getLogger(__name__)

# Try to import Tantivy
try:
    import tantivy
    TANTIVY_AVAILABLE = True
except ImportError:
    TANTIVY_AVAILABLE = False
    logger.warning("Tantivy not available - version search will be limited")


class VersionIndexer:
    """
    Indexes TranscriptionVersions in a Tantivy index for full-text search.

    This allows searching within user corrections and transcription versions
    separately or combined with the main document index.

    Schema:
        - id: Version ID
        - sys_id: Document system ID
        - page_num: Page number
        - content: Full transcription text
        - source: Version source (V0.7, V0.8, user)
        - user_id: User who created (for user versions)
        - user_name: Author name
        - created_at: Timestamp
        - version_number: Sequential version number
    """

    def __init__(self, index_path: Path = None):
        """
        Initialize the version indexer.

        Args:
            index_path: Path for the Tantivy index
        """
        self.index_path = index_path
        self.index = None
        self.schema = None

        if TANTIVY_AVAILABLE and index_path:
            self._init_index()

    def _init_index(self):
        """Initialize or open the Tantivy index"""
        try:
            self.index_path.mkdir(parents=True, exist_ok=True)

            # Build schema
            schema_builder = tantivy.SchemaBuilder()

            # ID field (stored, indexed)
            schema_builder.add_integer_field("id", stored=True, indexed=True)

            # Document reference
            schema_builder.add_text_field("sys_id", stored=True, tokenizer_name="raw")
            schema_builder.add_integer_field("page_num", stored=True, indexed=True)

            # Main content field - full-text searchable
            schema_builder.add_text_field("content", stored=True)

            # Source field for filtering
            schema_builder.add_text_field("source", stored=True, tokenizer_name="raw")

            # User info
            schema_builder.add_integer_field("user_id", stored=True, indexed=True)
            schema_builder.add_text_field("user_name", stored=True, tokenizer_name="raw")

            # Metadata
            schema_builder.add_integer_field("version_number", stored=True, indexed=True)
            schema_builder.add_date_field("created_at", stored=True)

            self.schema = schema_builder.build()

            # Open or create index
            if list(self.index_path.iterdir()):
                self.index = tantivy.Index.open(str(self.index_path))
                logger.info(f"Opened existing versions index at {self.index_path}")
            else:
                self.index = tantivy.Index(self.schema, str(self.index_path))
                logger.info(f"Created new versions index at {self.index_path}")

        except Exception as e:
            logger.error(f"Failed to initialize versions index: {e}")
            self.index = None

    def index_version(self, version: TranscriptionVersion) -> bool:
        """
        Add or update a version in the index.

        Args:
            version: The TranscriptionVersion to index

        Returns:
            True if successful, False otherwise
        """
        if not self.index:
            return False

        try:
            writer = self.index.writer()

            # Delete existing document with same ID
            writer.delete_documents("id", version.id)

            # Add document
            doc = tantivy.Document()
            doc.add_integer("id", version.id)
            doc.add_text("sys_id", version.sys_id)
            doc.add_integer("page_num", version.page_num)
            doc.add_text("content", version.content)
            doc.add_text("source", version.source.value)

            if version.user_id:
                doc.add_integer("user_id", version.user_id)
            if version.user:
                doc.add_text("user_name", version.user.full_name or version.user.username)

            doc.add_integer("version_number", version.version_number)
            if version.created_at:
                doc.add_date("created_at", version.created_at)

            writer.add_document(doc)
            writer.commit()

            logger.debug(f"Indexed version {version.id} for {version.sys_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to index version {version.id}: {e}")
            return False

    def remove_version(self, version_id: int) -> bool:
        """Remove a version from the index"""
        if not self.index:
            return False

        try:
            writer = self.index.writer()
            writer.delete_documents("id", version_id)
            writer.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to remove version {version_id}: {e}")
            return False

    def search_versions(
        self,
        query: str,
        sys_id: str = None,
        source: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search versions.

        Args:
            query: Search query
            sys_id: Optionally filter by document
            source: Optionally filter by source (V0.7, V0.8, user)
            limit: Maximum results

        Returns:
            List of matching versions
        """
        if not self.index:
            return []

        try:
            searcher = self.index.searcher()
            query_parser = tantivy.QueryParser.for_index(
                self.index,
                ["content"]
            )

            # Build query with filters
            query_parts = [f'({query})']
            if sys_id:
                query_parts.append(f'sys_id:"{sys_id}"')
            if source:
                query_parts.append(f'source:"{source}"')

            combined_query = ' AND '.join(query_parts)
            parsed_query = query_parser.parse_query(combined_query)
            search_result = searcher.search(parsed_query, limit)

            results = []
            for score, doc_address in search_result.hits:
                doc = searcher.doc(doc_address)
                results.append({
                    'id': doc.get_first("id"),
                    'sys_id': doc.get_first("sys_id"),
                    'page_num': doc.get_first("page_num"),
                    'content': doc.get_first("content"),
                    'source': doc.get_first("source"),
                    'user_id': doc.get_first("user_id"),
                    'user_name': doc.get_first("user_name"),
                    'version_number': doc.get_first("version_number"),
                    'score': score
                })

            return results

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def search_user_versions(
        self,
        query: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search only user-contributed versions.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of matching user versions
        """
        return self.search_versions(query, source='user', limit=limit)

    def get_versions_for_document(self, sys_id: str) -> List[Dict[str, Any]]:
        """Get all indexed versions for a document"""
        if not self.index:
            return []

        try:
            searcher = self.index.searcher()
            query_parser = tantivy.QueryParser.for_index(
                self.index,
                ["sys_id"]
            )
            query = query_parser.parse_query(f'sys_id:"{sys_id}"')
            search_result = searcher.search(query, 1000)

            results = []
            for score, doc_address in search_result.hits:
                doc = searcher.doc(doc_address)
                results.append({
                    'id': doc.get_first("id"),
                    'sys_id': doc.get_first("sys_id"),
                    'page_num': doc.get_first("page_num"),
                    'content': doc.get_first("content"),
                    'source': doc.get_first("source"),
                    'user_id': doc.get_first("user_id"),
                    'user_name': doc.get_first("user_name"),
                    'version_number': doc.get_first("version_number")
                })

            return results

        except Exception as e:
            logger.error(f"Document query failed: {e}")
            return []

    def rebuild_index(self, db: Session, progress_callback=None) -> int:
        """
        Rebuild the entire versions index from database.

        Args:
            db: Database session
            progress_callback: Optional callback for progress updates

        Returns:
            Number of versions indexed
        """
        if not self.index:
            logger.warning("Index not available")
            return 0

        try:
            # Clear existing index
            writer = self.index.writer()
            writer.delete_all_documents()
            writer.commit()

            # Get all versions
            versions = db.query(TranscriptionVersion).all()

            total = len(versions)
            count = 0

            for version in versions:
                self.index_version(version)

                count += 1
                if progress_callback and count % 100 == 0:
                    progress_callback(count, total)

            # Mark all as indexed
            from .version_service import VersionService
            VersionService.mark_as_indexed(db, [v.id for v in versions])

            logger.info(f"Rebuilt versions index with {count} versions")
            return count

        except Exception as e:
            logger.error(f"Index rebuild failed: {e}")
            return 0

    def index_unindexed_versions(self, db: Session) -> Tuple[int, int]:
        """
        Index versions that haven't been indexed yet.

        Args:
            db: Database session

        Returns:
            Tuple of (indexed_count, failed_count)
        """
        if not self.index:
            return 0, 0

        from .version_service import VersionService

        try:
            unindexed = VersionService.get_unindexed_versions(db, limit=1000)

            indexed = 0
            failed = 0
            indexed_ids = []

            for version in unindexed:
                if self.index_version(version):
                    indexed += 1
                    indexed_ids.append(version.id)
                else:
                    failed += 1

            if indexed_ids:
                VersionService.mark_as_indexed(db, indexed_ids)

            logger.info(f"Indexed {indexed} versions, {failed} failed")
            return indexed, failed

        except Exception as e:
            logger.error(f"Incremental indexing failed: {e}")
            return 0, 0

    @property
    def is_available(self) -> bool:
        """Check if the indexer is available"""
        return TANTIVY_AVAILABLE and self.index is not None

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        if not self.index:
            return {'available': False}

        try:
            searcher = self.index.searcher()
            return {
                'available': True,
                'num_docs': searcher.num_docs,
                'index_path': str(self.index_path)
            }
        except Exception as e:
            return {'available': False, 'error': str(e)}


# Singleton instance
_indexer_instance: Optional[VersionIndexer] = None


def get_version_indexer(index_path: Path = None) -> VersionIndexer:
    """Get or create the version indexer singleton"""
    global _indexer_instance

    if _indexer_instance is None and index_path:
        _indexer_instance = VersionIndexer(index_path)

    return _indexer_instance
