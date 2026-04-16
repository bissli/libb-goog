"""Shared fixtures, markers, and env-var gating for tests.
"""
import os
from unittest.mock import MagicMock, patch

import cachu
import pytest
from goog.base import Context
from goog.calendar import Calendar
from goog.drive import Drive
from goog.gmail import Gmail


def pytest_configure(config):
    config.addinivalue_line('markers', 'live: requires real Google API credentials')


def pytest_collection_modifyitems(config, items):
    """Skip live tests unless GOOG_TEST_KEY env var is set.
    """
    if not os.environ.get('GOOG_TEST_KEY'):
        skip = pytest.mark.skip(reason='GOOG_TEST_KEY not set')
        for item in items:
            if 'live' in item.keywords:
                item.add_marker(skip)


@pytest.fixture
def mock_cx():
    """Mock Discovery API service object supporting method chaining.
    """
    return MagicMock()


@pytest.fixture
def clean_settings():
    """Save and restore goog.base._settings.
    """
    import goog.base
    original = goog.base._settings.copy()
    goog.base._settings.clear()
    yield
    goog.base._settings.clear()
    goog.base._settings.update(original)


@pytest.fixture
def mock_drive(mock_cx):
    """Drive instance with patched auth, using mock service object.
    """
    with patch.object(Context, '__init__', lambda self, **kw: None):
        d = Drive.__new__(Drive)
        d.cx = mock_cx
        d.app = 'drive'
        d.account = 'test@example.com'
        d._rootid = {'TestDrive': 'root123', 'Other': 'root456'}
        d._tmpdir = '/tmp/test'
        cachu.configure(backend_default='memory')
        d.clear_cache()
        yield d
        d.clear_cache()


@pytest.fixture
def gmail(mock_cx):
    """Gmail instance with patched auth.
    """
    with patch.object(Context, '__init__', lambda self, **kw: None):
        g = Gmail.__new__(Gmail)
        g.cx = mock_cx
        g.app = 'gmail'
        g.account = 'test@example.com'
        return g


@pytest.fixture
def calendar(mock_cx):
    """Calendar instance with patched auth.
    """
    with patch.object(Context, '__init__', lambda self, **kw: None):
        c = Calendar.__new__(Calendar)
        c.cx = mock_cx
        c.app = 'calendar'
        c.account = 'test@example.com'
        return c


@pytest.fixture
def mock_sheets(clean_settings):
    """Sheets instance with patched auth and mock gspread/drive.
    """
    from goog.sheets import Sheets
    with patch('goog.sheets.service_account.Credentials.from_service_account_file') as mock_creds, \
    patch('goog.sheets.gspread.utils.convert_credentials') as mock_convert, \
    patch('goog.sheets.gspread.Client') as mock_gx_cls, \
    patch('goog.sheets.Drive') as mock_drive_cls:
        import goog.base
        goog.base._settings.update({
            'account': 'test@example.com',
            'app_configs': {
                'sheets': {
                    'key': '/fake/key.json',
                    'scopes': ['https://spreadsheets.google.com/feeds'],
                },
            },
        })
        mock_creds.return_value = MagicMock()
        mock_convert.return_value = MagicMock()
        mock_gx = MagicMock()
        mock_gx_cls.return_value = mock_gx
        mock_dx = MagicMock()
        mock_drive_cls.return_value = mock_dx

        s = Sheets()

        yield s, mock_gx, mock_dx
