
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

    if 'physical_medium' not in marc:
        print("\n[FAIL] physical_medium missing!")
        sys.exit(1)

    if 'online_link' not in marc:
        print("\n[FAIL] online_link missing!")
        sys.exit(1)

    print("\n[SUCCESS] New fields integrated.")

if __name__ == "__main__":
    test_enrichment()
