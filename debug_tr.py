# -*- coding: utf-8 -*-
import sys
from genizah_translations import TRANSLATIONS

def tr(text):
    return TRANSLATIONS.get(text, "MISSING")

print(f"Checking '◀ Prev Result': {tr('◀ Prev Result')}")
print(f"Checking 'Next Result ▶': {tr('Next Result ▶')}")
