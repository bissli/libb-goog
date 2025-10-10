"""Google Calendar API client.
"""
import logging
from typing import Any

from goog.base import Context

logger = logging.getLogger(__name__)


class Calendar(Context):
    """Simple calendar context for Google Calendar API operations.
    """

    def __init__(self, account: str | None = None, key: str | None = None,
                 scopes: list[str] | None = None, version: str | None = None) -> None:
        super().__init__(app='calendar', account=account, key=key,
                         scopes=scopes, version=version)

    def list_calendar(self, **kw: Any) -> dict[str, Any]:
        """List calendars.
        """
        return self.cx.calendarList().list().execute()

    def list_events(self, **kw: Any) -> dict[str, Any]:
        """List calendar events.
        """
        return self.cx.events().list(**kw).execute()

    def get_events(self, **kw: Any) -> dict[str, Any]:
        """Get calendar events.
        """
        return self.cx.events().get(**kw).execute()

    def delete_events(self, **kw: Any) -> dict[str, Any]:
        """Delete calendar events.
        """
        logger.info('Deleting calendar events')
        return self.cx.events().delete(**kw).execute()

    def insert_events(self, **kw: Any) -> dict[str, Any]:
        """Insert calendar events.
        """
        logger.info('Creating calendar events')
        return self.cx.events().insert(**kw).execute()
