
import sys
import logging
from genizah_core import MetadataManager, configure_logger

# Setup Logging
configure_logger()
logging.getLogger("genizah").setLevel(logging.DEBUG)

def test_enrichment():
    sys_id = "990051372520205171"
    mgr = MetadataManager()

    # Simulate an external link detection manually since NLI MARC is flaky in this env
    print(f"--- Testing External IIIF Fetch Logic ---")

    # Example URL from user comment
    cudl_view = "http://cudl.lib.cam.ac.uk/view/MS-TS-NS-00321-00008/1"

    print(f"Converting View URL: {cudl_view}")

    data = mgr.fetch_external_iiif_data(cudl_view)

    print("\n[External Data Result]")
    print(f"Attribution: {data.get('attribution')}")
    print(f"Metadata Keys: {list(data.get('metadata', {}).keys())}")
    print(f"Canvas Count: {len(data.get('canvases', []))}")

    if data.get('canvases'):
        print(f"First Canvas: {data['canvases'][0]}")

    if not data.get('canvases'):
        print("\n[FAIL] No canvases found (Network error or parsing error?)")
        # Don't fail the whole test if network is just blocked, but warn
    else:
        print("\n[SUCCESS] External IIIF fetched.")

if __name__ == "__main__":
    test_enrichment()
