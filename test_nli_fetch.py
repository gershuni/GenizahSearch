
import sys
import logging
from genizah_core import MetadataManager, configure_logger

# Setup Logging
configure_logger()
logging.getLogger("genizah").setLevel(logging.DEBUG)

def test_enrichment():
    sys_id = "990051372520205171"
    mgr = MetadataManager()

    print(f"--- Fetching Enriched Metadata for {sys_id} ---")
    data = mgr.enrich_metadata(sys_id)

    print("\n[Parsed MARC Data]")
    marc = data.get('marc', {})
    for k, v in marc.items():
        print(f"{k}: {v}")

    print("\n[IIIF Data]")
    print(f"Physical Desc: {data.get('physical_desc')}")
    print(f"Canvas Map (First 5): {list(data.get('canvas_map', {}).items())[:5]}...")

    if 'date' not in marc:
        print("\n[FAIL] Date field missing from schema!")
        sys.exit(1)

    if 'subjects' not in marc:
        print("\n[FAIL] Subjects field missing from schema!")
        sys.exit(1)

    print("\n[SUCCESS] Metadata schema updated successfully.")

if __name__ == "__main__":
    test_enrichment()
