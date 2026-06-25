# -*- coding: utf-8 -*-
"""Codicological unit management for Oxford/Neubauer manuscripts.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import CodicologicalManager`` callers continue working.
"""

import json
import logging
import os
import re

from shared.browse_map_utils import natural_sort_key
from shared.config import Config

LOGGER = logging.getLogger(__name__)


class CodicologicalManager:
    """
    Manages codicological units (Parts) for Oxford manuscripts.
    Maps between our folio-based system IDs and Oxford's Neubauer catalog Parts.

    A "Part" is a codicological unit in the Neubauer catalog that may contain
    multiple folios (and thus multiple system IDs in our system).
    """

    def __init__(self):
        # Core mappings
        self.folio_to_part = {}       # sys_id → part_id (e.g., "MS. Heb. d. 29/2")
        self.part_to_folios = {}      # part_id → [sys_ids] (ordered by folio number)
        self.part_metadata = {}       # part_id → {title, contents, provenance, images, ...}
        self.part_to_volume = {}      # part_id → volume_number (for grouping)

        # For autocomplete: [(display_text, normalized, part_id)]
        self.part_autocomplete = []

        # Raw JSON data (loaded once)
        self._oxford_db = {}
        self._loaded = False

        # Volume structure: volume_num → {part_id → folio_range}
        self._volume_parts = {}

    def load(self, csv_bank=None):
        """
        Load Oxford database and build all mappings.

        Args:
            csv_bank: Optional dict of sys_id → {shelfmark, title, oxford_part_id}
                      from MetadataManager for resolving missing parts.
        """
        if self._loaded:
            return True

        if not os.path.exists(Config.OXFORD_DB):
            LOGGER.warning("Oxford database not found at %s", Config.OXFORD_DB)
            return False

        try:
            LOGGER.info("Loading Oxford codicological database from %s", Config.OXFORD_DB)
            with open(Config.OXFORD_DB, 'r', encoding='utf-8') as f:
                self._oxford_db = json.load(f)

            self._build_part_mappings()
            self._build_folio_mappings(csv_bank)
            self._build_autocomplete_list()

            self._loaded = True
            LOGGER.info("Loaded %d codicological parts from Oxford database", len(self.part_metadata))
            return True

        except Exception as e:
            LOGGER.error("Failed to load Oxford database: %s", e)
            return False

    def _build_part_mappings(self):
        """Build part_metadata and volume structure from JSON."""
        for volume_num, parts in self._oxford_db.items():
            self._volume_parts[volume_num] = {}

            for part_id, part_data in parts.items():
                # Store metadata
                self.part_metadata[part_id] = {
                    'title': part_data.get('metadata', {}).get('title', ''),
                    'contents': part_data.get('metadata', {}).get('contents', ''),
                    'provenance': part_data.get('metadata', {}).get('provenance', ''),
                    'languages': part_data.get('metadata', {}).get('languages', ''),
                    'volume_url': part_data.get('volume_url', ''),
                    'direct_link': part_data.get('direct_link', ''),
                    'folio_range': part_data.get('folio_range', []),
                    'images': part_data.get('images', []),
                }

                self.part_to_volume[part_id] = volume_num

                # Store folio range for later resolution
                folio_range = part_data.get('folio_range', [])
                if folio_range and len(folio_range) == 2:
                    self._volume_parts[volume_num][part_id] = folio_range

                # Initialize empty folio list (will be populated from CSV)
                self.part_to_folios[part_id] = []

    def _build_folio_mappings(self, csv_bank=None):
        """
        Build folio→part mappings using CSV data.
        For entries with oxford_part_id, use it directly.
        For entries without, try to resolve using folio_range from JSON.
        """
        if csv_bank is None:
            return

        # First pass: direct mappings from CSV oxford_part_id
        for sys_id, data in csv_bank.items():
            oxford_part_id = data.get('oxford_part_id', '')
            if oxford_part_id and oxford_part_id in self.part_metadata:
                self.folio_to_part[sys_id] = oxford_part_id
                if sys_id not in self.part_to_folios[oxford_part_id]:
                    self.part_to_folios[oxford_part_id].append(sys_id)

        # Second pass: resolve missing parts using folio_range
        for sys_id, data in csv_bank.items():
            if sys_id in self.folio_to_part:
                continue  # Already mapped

            shelfmark = data.get('shelfmark', '')
            resolved_part = self._resolve_part_by_folio_range(shelfmark)
            if resolved_part:
                self.folio_to_part[sys_id] = resolved_part
                if sys_id not in self.part_to_folios[resolved_part]:
                    self.part_to_folios[resolved_part].append(sys_id)

        # Sort folio lists by natural order of shelfmark
        for part_id in self.part_to_folios:
            self.part_to_folios[part_id].sort(
                key=lambda sid: self._get_folio_number(csv_bank.get(sid, {}).get('shelfmark', ''))
            )

    def _resolve_part_by_folio_range(self, shelfmark):
        """
        Resolve a shelfmark to its Part using folio_range from JSON.

        Example: "MS heb. d.29/8" → folio 8 in volume d.29
        We look for a Part in that volume whose folio_range contains 8.
        """
        if not shelfmark:
            return None

        # Parse shelfmark to extract volume and folio
        # Pattern: MS heb. X.Y/Z or MS heb. X. Y/Z
        match = re.match(
            r'(?:MS\.?\s*)?heb\.?\s*([a-z])\.?\s*(\d+)[/.](\d+)',
            shelfmark,
            re.IGNORECASE
        )
        if not match:
            return None

        letter, volume_num, folio_num = match.groups()
        folio_num = int(folio_num)

        # Construct volume identifier pattern to find in our volume_parts
        # The JSON uses volume numbers like "56" which correspond to specific volumes
        # We need to find parts that match "MS. Heb. {letter}. {volume_num}"
        volume_pattern = f"heb. {letter}. {volume_num}"

        for part_id, metadata in self.part_metadata.items():
            # Check if part_id matches the volume pattern
            if volume_pattern.lower() not in part_id.lower():
                continue

            folio_range = metadata.get('folio_range', [])
            if len(folio_range) == 2:
                start, end = folio_range
                if start <= folio_num <= end:
                    return part_id

        return None

    def _get_folio_number(self, shelfmark):
        """Extract folio number from shelfmark for sorting."""
        if not shelfmark:
            return 0
        match = re.search(r'[/.](\d+)\s*$', shelfmark)
        return int(match.group(1)) if match else 0

    def _build_autocomplete_list(self):
        """Build autocomplete entries for Parts with 'part X' suffix."""
        self.part_autocomplete = []

        for part_id, metadata in self.part_metadata.items():
            # Part ID format: "MS. Heb. d. 29/2" -> "heb. d. 29 part 2"
            # Parse the Part ID
            match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)/(\d+)$', part_id, re.IGNORECASE)
            if match:
                letter, volume, part_num = match.groups()
                # Display format: "heb. d. 29 part 2"
                display = f"heb. {letter}. {volume} part {part_num}"
            else:
                # Fallback for non-standard Part IDs
                display = f"{part_id} part"

            # Normalized for matching (lowercase, no dots/spaces)
            normalized = re.sub(r'[\s.]', '', display.lower())

            title = metadata.get('title', '')

            self.part_autocomplete.append({
                'display': display,
                'normalized': normalized,
                'part_id': part_id,
                'title': title,
            })

        # Sort by natural order
        self.part_autocomplete.sort(key=lambda x: natural_sort_key(x['display']))

    # --- Public API ---

    def get_part_for_folio(self, sys_id):
        """Get the Part ID for a given system ID (folio)."""
        return self.folio_to_part.get(sys_id)

    def get_folios_for_part(self, part_id):
        """Get all system IDs (folios) belonging to a Part, in order."""
        return self.part_to_folios.get(part_id, [])

    def get_part_metadata(self, part_id):
        """Get full metadata for a Part."""
        return self.part_metadata.get(part_id, {})

    def get_part_images(self, part_id):
        """Get all images for a Part."""
        metadata = self.part_metadata.get(part_id, {})
        return metadata.get('images', [])

    def get_part_display_name(self, part_id):
        """Get display name for a Part (with 'part X' suffix)."""
        if part_id in self.part_metadata:
            # Convert "MS. Heb. d. 29/2" to "heb. d. 29 part 2"
            # Also handles letter parts like "MS. Heb. d. 25/C" -> "heb. d. 25 part C"
            match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)/([A-Za-z0-9]+)$', part_id, re.IGNORECASE)
            if match:
                letter, volume, part_num = match.groups()
                return f"heb. {letter}. {volume} part {part_num}"
            return f"{part_id} part"
        return part_id

    def get_part_label(self, part_id):
        """Return a short Part label suitable for shelfmark suffixes (e.g., "part 23")."""
        if not part_id:
            return ""

        match = re.search(r'/([A-Za-z0-9]+)$', part_id)
        if match:
            return f"part {match.group(1)}"

        return "part"

    def is_part_id(self, identifier):
        """Check if an identifier is a Part ID (vs a regular shelfmark)."""
        # Check for "part" suffix (new format)
        if re.search(r'\bpart\s*\d*\s*$', identifier, re.IGNORECASE):
            return True
        # Legacy: check for "(neubauer)" suffix
        if "(neubauer)" in identifier.lower():
            return True
        # Also check if it's directly in our metadata
        clean = identifier.strip()
        return clean in self.part_metadata

    def parse_part_identifier(self, identifier):
        """
        Parse an identifier that might be a Part.
        Returns (part_id, is_part) tuple.

        Handles formats:
        - "heb. d. 29 part 2" -> "MS. Heb. d. 29/2"
        - "MS. Heb. d. 29/2 (neubauer)" -> "MS. Heb. d. 29/2"
        - "MS. Heb. d. 29/2" -> "MS. Heb. d. 29/2"
        """
        if not identifier:
            return None, False

        # Check for new format: "heb. d. 29 part 2"
        match = re.match(r'^(?:ms\.?\s*)?heb\.?\s*([a-z])\.?\s*(\d+)\s+part\s*(\d+)\s*$',
                         identifier, re.IGNORECASE)
        if match:
            letter, volume, part_num = match.groups()
            # Convert to canonical Part ID format
            part_id = f"MS. Heb. {letter}. {volume}/{part_num}"
            if part_id in self.part_metadata:
                return part_id, True
            # Try with uppercase letter
            part_id_upper = f"MS. Heb. {letter.upper()}. {volume}/{part_num}"
            if part_id_upper in self.part_metadata:
                return part_id_upper, True

        # Legacy: remove "(neubauer)" suffix if present
        clean = re.sub(r'\s*\(neubauer\)\s*$', '', identifier, flags=re.IGNORECASE).strip()
        # Also remove " part" suffix without number
        clean = re.sub(r'\s+part\s*$', '', clean, flags=re.IGNORECASE).strip()

        if clean in self.part_metadata:
            return clean, True

        # Try to normalize and find
        normalized = re.sub(r'\s+', ' ', clean).strip()
        if normalized in self.part_metadata:
            return normalized, True

        return identifier, False

    def get_image_for_folio(self, sys_id, side='a'):
        """
        Get the specific image URL for a folio within its Part.

        Args:
            sys_id: System ID of the folio
            side: 'a' or 'b' for recto/verso

        Returns:
            dict with 'label', 'full_url', 'thumb_url' or None
        """
        part_id = self.folio_to_part.get(sys_id)
        if not part_id:
            return None

        images = self.get_part_images(part_id)
        if not images:
            return None

        # Find the folio number in the part's folio list
        folios = self.part_to_folios.get(part_id, [])
        if sys_id not in folios:
            return None

        folio_index = folios.index(sys_id)

        # Each folio has 2 images (a and b)
        # folio_index 0 → images[0] (a), images[1] (b)
        # folio_index 1 → images[2] (a), images[3] (b)
        image_offset = 0 if side == 'a' else 1
        image_index = (folio_index * 2) + image_offset

        if image_index < len(images):
            return images[image_index]

        return None

    def get_all_images_for_part(self, part_id):
        """Get all images for a Part with their labels."""
        return self.get_part_images(part_id)

    def get_adjacent_part(self, part_id, direction=1):
        """
        Get the next or previous Part in the same volume.

        Args:
            part_id: Current Part ID
            direction: 1 for next, -1 for previous

        Returns:
            Adjacent Part ID or None
        """
        volume = self.part_to_volume.get(part_id)
        if not volume or volume not in self._oxford_db:
            return None

        # Get parts in this volume, sorted
        volume_parts = list(self._oxford_db[volume].keys())
        volume_parts.sort(key=natural_sort_key)

        try:
            idx = volume_parts.index(part_id)
            new_idx = idx + direction
            if 0 <= new_idx < len(volume_parts):
                return volume_parts[new_idx]
        except ValueError:
            pass

        return None


