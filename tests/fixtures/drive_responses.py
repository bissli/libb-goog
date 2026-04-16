"""Factory functions for realistic Google Drive API response dicts.

Shapes are based on real API responses captured from the Drive v3 API.
Sanitized JSON fixtures in data/ were captured from a live environment.
"""
import json
from pathlib import Path
from typing import Any

FIXTURES_DIR = Path(__file__).parent / 'data'


def load_fixture(name: str) -> dict:
    """Load a sanitized JSON fixture by name (without .json extension).
    """
    return json.loads((FIXTURES_DIR / f'{name}.json').read_text())


def file_entry(name: str, file_id: str | None = None,
               mime_type: str = 'application/pdf', **extra: Any) -> dict[str, Any]:
    """Build a single file metadata dict.
    """
    entry = {
        'id': file_id or f'id_{name}',
        'name': name,
        'mimeType': mime_type,
    }
    entry.update(extra)
    return entry


def folder_entry(name: str, folder_id: str | None = None,
                 **extra: Any) -> dict[str, Any]:
    """Build a folder metadata dict.
    """
    return file_entry(
        name, file_id=folder_id,
        mime_type='application/vnd.google-apps.folder', **extra)


def files_list_response(files: list[dict], next_page_token: str | None = None) -> dict:
    """Build a files().list() response.
    """
    resp = {'files': files}
    if next_page_token:
        resp['nextPageToken'] = next_page_token
    return resp


def files_get_response(file_id: str, name: str,
                       mime_type: str = 'text/plain',
                       **extra: Any) -> dict[str, Any]:
    """Build a files().get() response.
    """
    resp = {
        'id': file_id,
        'name': name,
        'mimeType': mime_type,
        'trashed': False,
        'parents': extra.pop('parents', ['parent_folder_id']),
        'createdTime': '2026-01-01T00:00:00.000Z',
        'modifiedTime': '2026-01-01T00:00:00.000Z',
    }
    resp.update(extra)
    return resp
