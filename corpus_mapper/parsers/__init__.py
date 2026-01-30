# -*- coding: utf-8 -*-
"""Parsers for various corpus formats."""

from .ja_parser import JAParser, parse_ja_file
from .maagarim_parser import MaagarimParser, parse_maagarim_file

__all__ = ['JAParser', 'MaagarimParser', 'parse_ja_file', 'parse_maagarim_file']
