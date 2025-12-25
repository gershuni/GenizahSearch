
import re
import unittest
from openpyxl.cell.rich_text import TextBlock, CellRichText
from openpyxl.cell.text import InlineFont

# Mocking the functions from genizah_app.py for testing logic

# Recreate the regex from the file
illegal_chars_re = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

def sanitize_for_excel(text):
    """Cleans text to prevent Excel XML corruption."""
    if text is None: return ""
    t = str(text)

    # 1. Remove illegal characters
    t = illegal_chars_re.sub('', t)

    # 2. Handle malicious formulas
    t = t.strip()
    if t.startswith(('=', '+', '-', '@')):
        t = "'" + t

    if len(t) > 32000:
        t = t[:32000] + "..."

    return t

def _clean_and_marker(text):
    """Prepares HTML for export: converts spans to *, removes other tags."""
    t = str(text or "")
    # Emulate the red span logic from the app
    if "<span" in t:
        t = re.sub(r'<span[^>]*>', '*', t)
        t = t.replace('</span>', '*')
    t = t.replace("<br>", "\n").replace("<br/>", "\n")
    t = re.sub(r'<[^>]+>', '', t)
    return t.strip()

def write_rich_cell_logic(text):
    # This emulates the logic inside write_rich_cell
    safe_text = sanitize_for_excel(text)

    if '*' not in safe_text:
        return ["PLAIN", safe_text]

    parts = safe_text.split('*')
    rich_string = []

    # Logic in existing code:
    # for i, part in enumerate(parts):
    #     if i % 2 == 1:
    #         rich_string.append(TextBlock(font_red, part))
    #     else:
    #         rich_string.append(TextBlock(font_normal, part))

    for i, part in enumerate(parts):
        if i % 2 == 1:
            rich_string.append(f"RED({part})")
        else:
            rich_string.append(f"NORMAL({part})")

    return rich_string

class TestExcelExport(unittest.TestCase):
    def test_sanitize(self):
        # Test basic cleaning
        self.assertEqual(sanitize_for_excel("Hello"), "Hello")
        # Test illegal char removal (e.g. vertical tab \x0b)
        self.assertEqual(sanitize_for_excel("Hello\x0bWorld"), "HelloWorld")

    def test_clean_and_marker(self):
        html = "Start <span style='color:red'>match</span> end"
        cleaned = _clean_and_marker(html)
        self.assertEqual(cleaned, "Start *match* end")

        html_complex = "<div>Line 1<br>Line 2 <span class='hl'>highlight</span>.</div>"
        cleaned = _clean_and_marker(html_complex)
        self.assertEqual(cleaned, "Line 1\nLine 2 *highlight*.")

    def test_rich_text_logic_standard(self):
        # Case: "Start *match* end"
        # Split: ["Start ", "match", " end"]
        # i=0 (even): "Start " -> Normal
        # i=1 (odd): "match" -> Red
        # i=2 (even): " end" -> Normal
        res = write_rich_cell_logic("Start *match* end")
        self.assertEqual(res, ["NORMAL(Start )", "RED(match)", "NORMAL( end)"])

    def test_rich_text_logic_start_match(self):
        # Case: "*match* end"
        # Split: ["", "match", " end"]
        # i=0: "" -> Normal
        # i=1: "match" -> Red
        # i=2: " end" -> Normal
        res = write_rich_cell_logic("*match* end")
        self.assertEqual(res, ["NORMAL()", "RED(match)", "NORMAL( end)"])

    def test_rich_text_logic_no_match(self):
        # Case: "No match here"
        res = write_rich_cell_logic("No match here")
        self.assertEqual(res, ["PLAIN", "No match here"])

    def test_rich_text_logic_entire_red_bug_repro(self):
        # User says: "context column is colored entirely red instead of just the relevant part"
        # This implies `*` markers might be missing or misplaced,
        # OR the logic interprets the whole string as i % 2 == 1?
        # If the string starts with `*` and has no closing `*`,
        # e.g., "*whole string"
        # Split: ["", "whole string"]
        # i=0: "" (Normal)
        # i=1: "whole string" (Red)
        # This would effectively color everything red.

        text = "*whole string red"
        res = write_rich_cell_logic(text)
        self.assertEqual(res, ["NORMAL()", "RED(whole string red)"])

        # What if _clean_and_marker fails?
        # If input HTML is `<span ...>whole text</span>`
        # _clean_and_marker -> `*whole text*`
        # split -> ["", "whole text", ""] -> Normal, Red, Normal.
        # This looks correct (it highlights the match).

        # BUT, what if the context logic in `genizah_core` or `genizah_app`
        # wraps the *entire* context string in `*` markers because the match is huge
        # or the context window is small?
        pass

    def test_hebrew_corruption(self):
        # Excel XML doesn't like certain unicode control characters even if valid in Python strings.
        # Specifically RTL markers if not handled right?
        # But `illegal_chars_re` handles C0 controls.
        # What about RLM/LRM? U+200E, U+200F. Valid in XML.
        pass

if __name__ == '__main__':
    unittest.main()
