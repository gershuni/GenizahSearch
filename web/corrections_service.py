# -*- coding: utf-8 -*-
"""
Backward-compatibility shim.

All corrections_service functions have moved to shared.corrections_service.
This shim re-exports them so existing web imports continue working:
  from web.corrections_service import get_pending_corrections_for_page  # still works
"""
from shared.corrections_service import get_pending_corrections_for_page as get_pending_corrections_for_page
