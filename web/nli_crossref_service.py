# -*- coding: utf-8 -*-
"""
Backward-compatibility shim.

All nli_crossref_service functions have moved to shared.nli_crossref_service.
This shim re-exports them so existing web imports continue working:
  from web.nli_crossref_service import NliCrossrefService  # still works
"""
from shared.nli_crossref_service import NliCrossrefService, get_nli_crossref_service
