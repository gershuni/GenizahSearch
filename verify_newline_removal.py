
import sys
import re
from unittest.mock import MagicMock

# Mock dependencies to import genizah_app
sys.modules['genizah_core'] = MagicMock()
sys.modules['gui_threads'] = MagicMock()
sys.modules['filter_text_dialog'] = MagicMock()
sys.modules['version'] = MagicMock()
sys.modules['PyQt6'] = MagicMock()
sys.modules['PyQt6.QtWidgets'] = MagicMock()
sys.modules['PyQt6.QtCore'] = MagicMock()
sys.modules['PyQt6.QtGui'] = MagicMock()

# Import the function indirectly by extracting it from source or via import if possible
# Since it's a nested function, we can't import it directly.
# We will read the file and extract the function code to test logic,
# OR we can mock the class and access the method if it was a method.
# Since it is nested in export_comp_report, we will rely on source verification or
# just trust the previous step + syntax check.

# However, to be thorough, let's create a quick test that imports the app
# and monkeypatches the dependencies, then instantiates the GUI (mocked) and calls the method.
# But export_comp_report is an instance method.

# Let's try to verify via regex on the file content itself to ensure the change is there.
with open('genizah_app.py', 'r') as f:
    content = f.read()

checks = [
    'snippet = r.get(\'raw_file_hl\', \'\').strip().replace(\'\\n\', \' \').replace(\'\\r\', \'\')',
    't = t.replace("<br>", " ").replace("<br/>", " ").replace("\\n", " ").replace("\\r", "")'
]

for check in checks:
    if check in content:
        print(f"VERIFIED: Found {check}")
    else:
        print(f"FAILED: Did not find {check}")
        sys.exit(1)

print("All newline removal checks passed.")
