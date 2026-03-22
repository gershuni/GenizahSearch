# Quick Task 260322-kr9: Close ResultDialog when adding to puzzle — Summary

## Change
Added `self.close()` after `parent.add_to_puzzle()` in `ResultDialog._add_to_puzzle()` (genizah_app.py:6229).

## What it does
When the user clicks the Puzzle button in the search ResultDialog, the dialog now closes automatically after adding the fragment to the puzzle canvas. Previously, the ResultDialog stayed open and sat above the puzzle window.

## Files changed
- `genizah_app.py` — 1 line added in `ResultDialog._add_to_puzzle()`
