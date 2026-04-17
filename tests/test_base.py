"""Tests for Context auth construction and rate-limit utilities.
"""
from unittest.mock import MagicMock, patch

import goog.base
import pytest
from goog.base import Context, is_rate_limit
from tests.fixtures.drive_responses import http_error_from_fixture


@pytest.fixture(autouse=True)
def _auto_clean(clean_settings):
    """Auto-apply clean_settings to all tests in this module.
    """


def test_missing_app_raises():
    """Verify app parameter is required.
    """
    with pytest.raises(ValueError, match='app parameter is required'):
        Context(app=None)


def test_missing_account_no_config_raises():
    """Verify account is required when not configured.
    """
    with pytest.raises(ValueError, match='account required'):
        Context(app='drive')


def test_missing_account_uses_configured():
    """Verify configured account is used as fallback.
    """
    goog.base._settings['account'] = 'configured@example.com'
    goog.base._settings['app_configs'] = {
        'drive': {
            'key': '/fake/key.json',
            'scopes': ['https://www.googleapis.com/auth/drive'],
            'version': 'v3',
        },
    }
    with patch('goog.base.service_account.Credentials.from_service_account_file') as mock_creds, \
    patch('goog.base.discovery.build') as mock_build:
        mock_creds.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        ctx = Context(app='drive')
    assert ctx.account == 'configured@example.com'


def test_unknown_app_no_key_raises():
    """Verify unknown app without explicit key raises.
    """
    goog.base._settings['account'] = 'test@example.com'
    with pytest.raises(ValueError, match='Unknown app'):
        Context(app='nonexistent')


def test_explicit_credentials_bypass_config():
    """Verify explicit key/scopes/version bypass app_configs.
    """
    goog.base._settings['account'] = 'test@example.com'
    with patch('goog.base.service_account.Credentials.from_service_account_file') as mock_creds, \
    patch('goog.base.discovery.build') as mock_build:
        mock_creds.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        ctx = Context(app='drive', key='/my/key.json',
                      scopes=['https://scope'], version='v3')
    assert ctx.cx is not None
    mock_creds.assert_called_once_with('/my/key.json')


class TestIsRateLimit:
    """Tests for is_rate_limit() classifier.
    """

    def test_429_is_rate_limit(self):
        """Verify 429 Too Many Requests is classified as rate limit.
        """
        resp = MagicMock()
        resp.status = 429
        exc = MagicMock()
        exc.resp = resp
        assert is_rate_limit(exc) is True

    def test_403_user_rate_limit_exceeded(self):
        """Verify 403 with userRateLimitExceeded reason is classified.
        """
        exc = http_error_from_fixture('files_update_rate_limit_exceeded', 403)
        assert is_rate_limit(exc) is True

    def test_403_rate_limit_exceeded(self):
        """Verify 403 with rateLimitExceeded reason is classified.
        """
        resp = MagicMock()
        resp.status = 403
        content = b'{"error": {"code": 403, "message": "Rate Limit Exceeded", "errors": [{"reason": "rateLimitExceeded", "domain": "usageLimits"}]}}'
        from googleapiclient.errors import HttpError
        exc = HttpError(resp, content)
        assert is_rate_limit(exc) is True

    def test_403_other_reason_not_rate_limit(self):
        """Verify 403 with non-rate-limit reason is not classified.
        """
        exc = http_error_from_fixture('files_update_move_folder_blocked', 403)
        assert is_rate_limit(exc) is False

    def test_403_string_error_details_not_rate_limit(self):
        """Verify 403 with string error_details does not crash.
        """
        resp = MagicMock()
        resp.status = 403
        content = b'{"error": {"code": 403, "message": "Forbidden"}}'
        from googleapiclient.errors import HttpError
        exc = HttpError(resp, content)
        assert is_rate_limit(exc) is False

    def test_404_not_rate_limit(self):
        """Verify 404 is not classified as rate limit.
        """
        resp = MagicMock()
        resp.status = 404
        exc = MagicMock()
        exc.resp = resp
        assert is_rate_limit(exc) is False

    def test_500_not_rate_limit(self):
        """Verify 500 server error is not classified as rate limit.
        """
        resp = MagicMock()
        resp.status = 500
        exc = MagicMock()
        exc.resp = resp
        assert is_rate_limit(exc) is False
