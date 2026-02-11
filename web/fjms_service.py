# -*- coding: utf-8 -*-
"""
Backward-compatibility shim.

All fjms_service functions have moved to shared.fjms_service.
This shim re-exports them so existing web imports continue working:
  from web.fjms_service import FjmsService  # still works
"""
from shared.fjms_service import FjmsService, get_fjms_service
