import sys
import os
import shutil
from unittest.mock import MagicMock
import PyQt6.QtWidgets

# Mock GUI elements
PyQt6.QtWidgets.QMessageBox = MagicMock()
PyQt6.QtWidgets.QFileDialog = MagicMock()
PyQt6.QtWidgets.QFileDialog.getSaveFileName.return_value = ("test_export.csv", "CSV")

# Force headless
os.environ["QT_QPA_PLATFORM"] = "offscreen"

from genizah_app import GenizahGUI
from PyQt6.QtWidgets import QApplication

# Setup
app = QApplication(sys.argv)
gui = GenizahGUI()

# Mock Results
gui.last_results = [
    {
        'display': {'id': '123', 'shelfmark': 'Shelf 1', 'title': 'Title 1', 'img': '1', 'source': 'Src'},
        'snippet': 'Snippet 1',
        'raw_header': '123_Header',
        'raw_file_hl': 'Snippet 1 *Highlight*'
    },
    {
        'display': {'id': '456', 'shelfmark': 'Shelf 2', 'title': 'Title 2', 'img': '2', 'source': 'Src'},
        'snippet': 'Snippet 2',
        'raw_header': '456_Header',
        'raw_file_hl': 'Snippet 2'
    }
]

# Run Export
print("Running export_results('csv')...")
try:
    gui.export_results('csv')
except Exception as e:
    print(f"Export crashed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Verify file exists
if os.path.exists("test_export.csv"):
    print("Export successful: File created.")
    with open("test_export.csv", 'r', encoding='utf-8-sig') as f:
        print("Content:")
        print(f.read())
    os.remove("test_export.csv")
else:
    print("Export failed: File not created.")
    sys.exit(1)
