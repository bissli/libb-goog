"""Google API utilities for Calendar, Gmail, Drive, and Sheets.
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
        self.cx = self.__auth__(key, scopes, version)

    def __auth__(self, key: str | None, scopes: list[str] | None,
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


from goog.calendar import Calendar
from goog.drive import Drive
from goog.gmail import Gmail
from goog.sheets import Sheets

__all__ = [
    'configure',
    'get_settings',
    'clean_filename',
    'Context',
    'Calendar',
    'Gmail',
    'Drive',
    'Sheets',
    ]
