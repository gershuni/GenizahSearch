from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable


def _safe_json_size(value: Any) -> int | None:
    """Return a compact JSON byte estimate for a storage payload."""
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
    except Exception:
        return None


def _top_payload_keys(payload: Any, limit: int = 5) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key, value in payload.items():
        rows.append({
            'key': str(key),
            'bytes_estimate': _safe_json_size(value),
            'type': type(value).__name__,
            'length': len(value) if hasattr(value, '__len__') else None,
        })
    rows.sort(
        key=lambda item: item['bytes_estimate'] if item['bytes_estimate'] is not None else -1,
        reverse=True,
    )
    return rows[:limit]


def _format_timestamp(timestamp: float) -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timestamp))


def summarize_nicegui_storage(storage: Any, clients: Iterable[Any]) -> dict[str, Any]:
    """Build diagnostics for NiceGUI client and storage state."""
    storage_path = Path(getattr(storage, 'path', '.nicegui'))
    user_storages = dict(getattr(storage, '_users', {}) or {})
    tab_storages = dict(getattr(storage, '_tabs', {}) or {})

    active_client_count = 0
    active_session_ids: set[str] = set()
    for client in clients:
        active_client_count += 1
        try:
            session_id = client.request.session.get('id')
        except Exception:
            session_id = None
        if session_id:
            active_session_ids.add(session_id)

    top_user_sessions: list[dict[str, Any]] = []
    user_payload_total_bytes = 0
    for session_id, payload in user_storages.items():
        payload_size = _safe_json_size(payload)
        if payload_size is not None:
            user_payload_total_bytes += payload_size
        top_user_sessions.append({
            'session_id': session_id,
            'bytes_estimate': payload_size,
            'key_count': len(payload) if hasattr(payload, '__len__') else None,
            'top_keys': _top_payload_keys(payload),
            'last_modified_utc': _format_timestamp(getattr(payload, 'last_modified', 0.0))
            if getattr(payload, 'last_modified', None) else None,
            'has_active_client': session_id in active_session_ids,
        })
    top_user_sessions.sort(
        key=lambda item: item['bytes_estimate'] if item['bytes_estimate'] is not None else -1,
        reverse=True,
    )

    top_storage_files: list[dict[str, Any]] = []
    total_storage_bytes = 0
    storage_file_count = 0
    user_storage_file_count = 0
    if storage_path.exists():
        for filepath in storage_path.glob('storage-*.json'):
            try:
                stat = filepath.stat()
            except OSError:
                continue
            storage_file_count += 1
            if filepath.name.startswith('storage-user-'):
                user_storage_file_count += 1
            total_storage_bytes += stat.st_size
            top_storage_files.append({
                'name': filepath.name,
                'bytes': stat.st_size,
                'last_modified_utc': _format_timestamp(stat.st_mtime),
            })
    top_storage_files.sort(key=lambda item: item['bytes'], reverse=True)

    retained_user_storage_count = sum(
        1 for session_id in user_storages
        if session_id not in active_session_ids
    )

    return {
        'nicegui_client_count': active_client_count,
        'nicegui_user_storage_count': len(user_storages),
        'nicegui_retained_user_storage_count': retained_user_storage_count,
        'nicegui_tab_storage_count': len(tab_storages),
        'nicegui_storage_path': str(storage_path),
        'nicegui_storage_file_count': storage_file_count,
        'nicegui_user_storage_file_count': user_storage_file_count,
        'nicegui_storage_total_bytes': total_storage_bytes,
        'nicegui_user_storage_payload_total_bytes_estimate': user_payload_total_bytes,
        'nicegui_top_user_sessions': top_user_sessions[:5],
        'nicegui_top_storage_files': top_storage_files[:5],
    }
