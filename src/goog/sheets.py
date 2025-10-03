"""Google Sheets API client.
"""
import logging
import os
import re
from pathlib import Path
from typing import Any

import gspread
from goog import get_settings
from goog.drive import Drive
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

from libb import copydoc, rand_retry

logger = logging.getLogger(__name__)


def _fmt(x: Any) -> int | float | str | None:
    """Format values from Google Sheets (handles commas, parentheses, percentages).
    """

    def strip_comma(x):
        if x is None:
            return
        y = re.match(r'^([0-9\.\-\,]+)$', x)
        if y:
            return x.replace(',', '')
        return x

    def strip_paren(x):
        if x is None:
            return
        y = re.match(r'^(?:\()([0-9\.\,]+)(?:\))$', x)
        if y:
            return '-' + (y.group(1))
        return x

    def strip_empty(x):
        if x is None or x in {'', '-'}:
            return
        return x

    def strip_pct(x):
        if x is None:
            return
        y = re.match(r'^([0-9\.-]+)(?:%)$', x)
        if y:
            return y.group(1)
        return x

    def guess_type(s):
        if re.match(r'\A[-0-9]+\.[0-9]+\Z', s):
            return float
        if re.match(r'\A[-0-9]+\Z', s):
            return int
        return str

    def pre(x):
        if x is None:
            return None
        return str(x).strip()

    def chain(x):
        return strip_comma(strip_paren(strip_pct(strip_empty(x))))

    def post(x):
        if x is None:
            return None
        try:
            typ = guess_type(x)
            if typ == float:
                return float(x)
            if typ == int:
                return int(x)
            return x
        except ValueError:
            return x

    return post(chain(pre(x)))


class Sheets:
    """Facade around gspread Client class.
    """

    def __init__(self, account: str | None = None, key: str | None = None,
                 scopes: list[str] | None = None, root: str | None = None) -> None:
        if root is not None:
            logger.warning('root parameter is deprecated and ignored')
        settings = get_settings()
        if account is None:
            account = settings.get('account')
        if key is None or scopes is None:
            app_configs = settings.get('app_configs', {})
            if 'sheets' not in app_configs:
                raise ValueError('sheets app config required when not provided')
            sheets_config = app_configs['sheets']
            key = key or sheets_config['key']
            scopes = scopes or sheets_config['scopes']
        if account is None or key is None or scopes is None:
            raise ValueError('account, key, and scopes required when not configured')

        creds = service_account.Credentials.from_service_account_file(key)
        creds = creds.with_scopes(scopes)
        creds = creds.with_subject(account)
        auth = gspread.utils.convert_credentials(creds)
        self._gx = gspread.Client(auth)
        self._dx = Drive(account=account, key=key, scopes=scopes)

    def _id(self, filepath: str) -> str:
        """Get file ID from filepath.
        """
        return self._dx.id(filepath)

    def _exists(self, filepath: str) -> bool:
        """Check if file exists.
        """
        return self._dx.exists(filepath)

    id = _id

    def _get_permission_id(self, filepath: str, user_email: str) -> str | None:
        """Get permission ID for a user email.
        """
        for permission in self.list_permissions(filepath):
            if permission['emailAddress'] == user_email:
                return permission['id']

    @copydoc(gspread.Client.open_by_key)
    def open_by_key(self, filepath: str) -> gspread.Spreadsheet:
        fileid = self._id(filepath)
        return self._gx.open_by_key(fileid)

    get_sheet = open_by_key

    @copydoc(gspread.Client.create)
    def create(self, filepath: str, overwrite: bool = False) -> None:
        filename = Path(filepath).name
        folderpath = filepath.split(filename, maxsplit=1)[0]
        if self._exists(filepath):
            if not overwrite:
                raise ValueError(f'{filename} exists in folder {folderpath} and overwrite set to False')
            self.del_spreadsheet(filepath)
        folder_id = self._dx._resolve_folderid(folderpath) if folderpath else None
        self._gx.create(filename, folder_id=folder_id)

    @rand_retry(x_times=3, exception=HttpError)
    @copydoc(gspread.Client.copy)
    def copy(self, filepath: str, title: str, copy_permissions: bool = True,
             folderpath: str | None = None, overwrite: bool = False) -> None:
        folder_id = self._dx._resolve_folderid(folderpath) if folderpath else None
        newfolder = folderpath or os.path.split(filepath)[0]
        newpath = os.path.join(newfolder, title)
        if self._exists(newpath):
            if not overwrite:
                raise ValueError(f'{title} exists in folder {newfolder} and overwrite set to False')
            self.del_spreadsheet(newpath)
        self._gx.copy(self._id(filepath), title=title, copy_permissions=copy_permissions, folder_id=folder_id)

    @rand_retry(x_times=3, exception=HttpError)
    @copydoc(gspread.Client.insert_permission)
    def insert_permission(self, filepath: str, value: str, perm_type: str, role: str,
                          notify: bool = False, email_message: bool = False,
                          with_link: bool = False) -> None:
        fileid = self._id(filepath)
        self._gx.insert_permission(fileid, value, perm_type, role, notify, email_message, with_link)

    @copydoc(gspread.Client.del_spreadsheet)
    def del_spreadsheet(self, filepath: str) -> None:
        fileid = self._id(filepath)
        self._gx.del_spreadsheet(fileid)

    @copydoc(gspread.Client.remove_permission)
    def remove_permission(self, filepath: str, email_address: str) -> Any:
        permission_id = self._get_permission_id(filepath, email_address)
        return self._gx.remove_permission(self.id(filepath), permission_id)

    @copydoc(gspread.Client.list_permissions)
    def list_permissions(self, filepath: str) -> list[dict[str, Any]]:
        fileid = self._id(filepath)
        return self._gx.list_permissions(fileid)

    def get_iterdict(self, filepath: str, header: int = 1, skip: int | None = None,
                     sheetname: str | None = None) -> list[dict[str, Any]]:
        """Get data from Google Sheet as list of dictionaries.
        """
        assert header >= 1, 'Must include header row'
        skip = skip or header + 1
        worksheet = self.get_sheet(filepath)
        title = worksheet.title
        sheets = worksheet.worksheets()
        if sheetname:
            sheets = [s for s in sheets if s.title == sheetname]
            if not sheets:
                logger.error(f'Unable to open {title}:{sheetname}')
                return []
        sheet = sheets[0]
        clean = lambda x: [_fmt(_) for _ in x]
        strip = lambda x: [_.strip() for _ in x]
        cols = strip(sheet.get(f'A{header}:ZZ{header}')[0])
        data = sheet.get(f'A{skip}:ZZ{sheet.row_count}')
        idict = [dict(list(zip(cols, clean(d)))) for d in data]
        idict = [{k: v for k, v in d.items() if k} for d in idict]
        logger.info(f'Built iterdict from {title}:{sheet.title}')
        return idict
