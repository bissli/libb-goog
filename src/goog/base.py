"""Base authentication context for Google APIs.
"""
import logging
from typing import Any

from apiclient import discovery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

_settings = {}


def configure(
    account: str | None = None,
    tmpdir: str | None = None,
    rootid: dict[str, str] | None = None,
    mail_from: str | None = None,
    app_configs: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Configure module defaults.

    This should be called once at application startup to set default values
    that will be used across all Google API clients.

    Example:
        >>> import goog
        >>> goog.configure(
        ...     account='service@example.com',
        ...     tmpdir='/tmp/goog',
        ...     rootid={'SharedDrive': 'abc123def456'},
        ...     mail_from='noreply@example.com',
        ...     app_configs={
        ...         'gmail': {
        ...             'key': '/path/to/gmail-key.json',
        ...             'scopes': ['https://www.googleapis.com/auth/gmail.send'],
        ...             'version': 'v1'
        ...         },
        ...         'drive': {
        ...             'key': '/path/to/drive-key.json',
        ...             'scopes': ['https://www.googleapis.com/auth/drive'],
        ...             'version': 'v3'
        ...         }
        ...     }
        ... )
    """
    if account is not None:
        _settings['account'] = account
    if tmpdir is not None:
        _settings['tmpdir'] = tmpdir
    if rootid is not None:
        _settings['rootid'] = rootid
    if mail_from is not None:
        _settings['mail_from'] = mail_from
    if app_configs is not None:
        _settings['app_configs'] = app_configs


def get_settings() -> dict[str, Any]:
    """Get current module settings.
    """
    return _settings


def clean_filename(fname: str) -> str:
    """Clean filename by escaping single quotes.
    """
    return fname.replace("'", "\\'")


class Context:
    """Base class that provides backend authentication for Google APIs.
    """

    def __init__(self, app: str | None = None, account: str | None = None,
                 key: str | None = None, scopes: list[str] | None = None,
                 version: str | None = None) -> None:
        if app is None:
            raise ValueError('app parameter is required')
        if account is None:
            account = _settings.get('account')
            if account is None:
                raise ValueError('account required when not configured')

        self.account = account
        self.app = app
        self.cx = self._build_service(key, scopes, version)

    def _build_service(self, key: str | None, scopes: list[str] | None,
                       version: str | None) -> Any:
        """Authenticate and build Google API service.
        """
        app_configs = _settings.get('app_configs', {})

        if key is None:
            if self.app not in app_configs:
                raise ValueError(f'Unknown app: {self.app}')
            app_config = app_configs[self.app]
            key = app_config['key']
            scopes = app_config['scopes']
            version = app_config.get('version')
        elif version is None and self.app in app_configs:
            version = app_configs[self.app].get('version')

        creds = service_account.Credentials.from_service_account_file(key)
        creds = creds.with_scopes(scopes)
        creds = creds.with_subject(self.account)
        api = discovery.build(self.app, version, credentials=creds)
        logger.info(f'Built Google OAuth API service for {self.app} {version} {self.account}')
        return api
