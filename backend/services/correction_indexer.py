"""
Correction Indexer - Indexes corrections in Tantivy for search integration
Allows searching corrections alongside the main document index
"""
import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Try to import Tantivy
try:
    import tantivy
    TANTIVY_AVAILABLE = True
except ImportError:
    TANTIVY_AVAILABLE = False
    logger.warning("Tantivy not available - correction search will be limited")


class CorrectionIndexer:
    """
    Indexes approved corrections in a Tantivy index for fast full-text search.

    Schema:
        - id: Correction ID
        - document_id: Reference to original document
        - original_text: Original text
        - corrected_text: Corrected text
        - author: Author username
        - correction_type: Type of correction
        - created_at: Timestamp
        - shelfmark: Document shelfmark
    """

    def __init__(self, index_path: Path = None):
        """
        Initialize the correction indexer.

        Args:
            index_path: Path for the Tantivy index
        """
        self.index_path = index_path
        self.index = None
        self.schema = None
        self.writer = None

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

            # Document reference (stored, indexed)
            schema_builder.add_text_field("document_id", stored=True, tokenizer_name="raw")

            # Text fields (stored, full-text indexed)
            schema_builder.add_text_field("original_text", stored=True)
            schema_builder.add_text_field("corrected_text", stored=True)

            # Metadata fields
            schema_builder.add_text_field("author", stored=True, tokenizer_name="raw")
            schema_builder.add_text_field("correction_type", stored=True, tokenizer_name="raw")
            schema_builder.add_text_field("shelfmark", stored=True, tokenizer_name="raw")
            schema_builder.add_date_field("created_at", stored=True)

            self.schema = schema_builder.build()

            # Open or create index
            if list(self.index_path.iterdir()):
                self.index = tantivy.Index.open(str(self.index_path))
                logger.info(f"Opened existing corrections index at {self.index_path}")
            else:
                self.index = tantivy.Index(self.schema, str(self.index_path))
                logger.info(f"Created new corrections index at {self.index_path}")

        except Exception as e:
            logger.error(f"Failed to initialize corrections index: {e}")
            self.index = None

    def index_correction(
        self,
        correction_id: int,
        document_id: str,
        original_text: str,
        corrected_text: str,
        author: str = None,
        correction_type: str = None,
        shelfmark: str = None,
        created_at: datetime = None
    ) -> bool:
        """
        Add or update a correction in the index.

        Returns:
            True if successful, False otherwise
        """
        if not self.index:
            return False

        try:
            writer = self.index.writer()

            # Delete existing document with same ID
            writer.delete_documents("id", correction_id)

            # Add document
            doc = tantivy.Document()
            doc.add_integer("id", correction_id)
            doc.add_text("document_id", document_id)
            doc.add_text("original_text", original_text)
            doc.add_text("corrected_text", corrected_text)

            if author:
                doc.add_text("author", author)
            if correction_type:
                doc.add_text("correction_type", correction_type)
            if shelfmark:
                doc.add_text("shelfmark", shelfmark)
            if created_at:
                doc.add_date("created_at", created_at)

            writer.add_document(doc)
            writer.commit()

            logger.debug(f"Indexed correction {correction_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to index correction {correction_id}: {e}")
            return False

    def remove_correction(self, correction_id: int) -> bool:
        """Remove a correction from the index"""
        if not self.index:
            return False

        try:
            writer = self.index.writer()
            writer.delete_documents("id", correction_id)
            writer.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to remove correction {correction_id}: {e}")
            return False

    def search_corrections(
        self,
        query: str,
        document_id: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search corrections.

        Args:
            query: Search query
            document_id: Optionally filter by document
            limit: Maximum results

        Returns:
            List of matching corrections
        """
        if not self.index:
            return []

        try:
            searcher = self.index.searcher()
            query_parser = tantivy.QueryParser.for_index(
                self.index,
                ["original_text", "corrected_text"]
            )

            if document_id:
                # Combined query with document filter
                combined_query = f'({query}) AND document_id:"{document_id}"'
            else:
                combined_query = query

            parsed_query = query_parser.parse_query(combined_query)
            search_result = searcher.search(parsed_query, limit)

            results = []
            for score, doc_address in search_result.hits:
                doc = searcher.doc(doc_address)
                results.append({
                    'id': doc.get_first("id"),
                    'document_id': doc.get_first("document_id"),
                    'original_text': doc.get_first("original_text"),
                    'corrected_text': doc.get_first("corrected_text"),
                    'author': doc.get_first("author"),
                    'correction_type': doc.get_first("correction_type"),
                    'shelfmark': doc.get_first("shelfmark"),
                    'score': score
                })

            return results

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def get_corrections_for_document(self, document_id: str) -> List[Dict[str, Any]]:
        """Get all indexed corrections for a document"""
        if not self.index:
            return []

        try:
            searcher = self.index.searcher()
            query_parser = tantivy.QueryParser.for_index(
                self.index,
                ["document_id"]
            )
            query = query_parser.parse_query(f'document_id:"{document_id}"')
            search_result = searcher.search(query, 1000)

            results = []
            for score, doc_address in search_result.hits:
                doc = searcher.doc(doc_address)
                results.append({
                    'id': doc.get_first("id"),
                    'document_id': doc.get_first("document_id"),
                    'original_text': doc.get_first("original_text"),
                    'corrected_text': doc.get_first("corrected_text"),
                    'author': doc.get_first("author"),
                    'correction_type': doc.get_first("correction_type"),
                    'shelfmark': doc.get_first("shelfmark")
                })

            return results

        except Exception as e:
            logger.error(f"Document query failed: {e}")
            return []

    def rebuild_index(self, db: Session, progress_callback=None) -> int:
        """
        Rebuild the entire corrections index from database.

        Args:
            db: Database session
            progress_callback: Optional callback for progress updates

        Returns:
            Number of corrections indexed
        """
        if not self.index:
            logger.warning("Index not available")
            return 0

        from ..models.correction import Correction, CorrectionStatus

        try:
            # Clear existing index
            writer = self.index.writer()
            writer.delete_all_documents()
            writer.commit()

            # Get all approved corrections
            corrections = db.query(Correction).filter(
                Correction.status == CorrectionStatus.APPROVED
            ).all()

            total = len(corrections)
            count = 0

            for correction in corrections:
                self.index_correction(
                    correction_id=correction.id,
                    document_id=correction.document_id,
                    original_text=correction.original_text,
                    corrected_text=correction.corrected_text,
                    author=correction.author.username if correction.author else None,
                    correction_type=correction.correction_type.value,
                    shelfmark=correction.shelfmark,
                    created_at=correction.created_at
                )

                count += 1
                if progress_callback and count % 100 == 0:
                    progress_callback(count, total)

            logger.info(f"Rebuilt corrections index with {count} corrections")
            return count

        except Exception as e:
            logger.error(f"Index rebuild failed: {e}")
            return 0

    @property
    def is_available(self) -> bool:
        """Check if the indexer is available"""
        return TANTIVY_AVAILABLE and self.index is not None


# Singleton instance
_indexer_instance: Optional[CorrectionIndexer] = None


def get_correction_indexer(index_path: Path = None) -> CorrectionIndexer:
    """Get or create the correction indexer singleton"""
    global _indexer_instance

    if _indexer_instance is None and index_path:
        _indexer_instance = CorrectionIndexer(index_path)

    return _indexer_instance
