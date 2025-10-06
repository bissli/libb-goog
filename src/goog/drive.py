"""Google Drive API client for file operations.
"""
import io
import logging
import mimetypes
import os
import posixpath
from pathlib import Path

import filetype
from goog import Context, clean_filename, get_settings
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload
from tqdm import tqdm

logger = logging.getLogger(__name__)

SHARED_DRIVE_EXTRA = {'includeItemsFromAllDrives': True, 'supportsAllDrives': True}


class Drive(Context):
    """Google Drive API client for file operations.
    """

    def __init__(self, account: str | None = None, key: str | None = None, scopes:
                 list[str] | None = None, version: str | None = None) -> None:
        super().__init__(app='drive', account=account, key=key,
                         scopes=scopes, version=version)

    def delete(self, filepath: str) -> None:
        """Permanently delete a file from Google Drive (files only, not folders).
        """
        fileid = self.id(filepath)
        assert fileid, 'Can only delete a file, not a folder'
        self.cx.files().delete(fileId=fileid, supportsAllDrives=True).execute()
        logger.info(f'Deleted {filepath} from drive')

    def download(self, filepath: str, directory: str | None = None) -> str | None:
        """Downloads a file from drive location to local directory.
        """
        if directory is None:
            settings = get_settings()
            directory = settings.get('tmpdir')
            if directory is None:
                raise ValueError('directory required when not configured')
        fileid = self.id(filepath)
        assert fileid, 'Can only download a file, not a folder'
        fname = Path(filepath).name
        topath = posixpath.join(Path(directory).resolve(), fname)
        with Path(topath).open('wb') as f:
            request = self.cx.files().get_media(fileId=fileid)
            media = MediaIoBaseDownload(f, request)
            while True:
                try:
                    status, done = media.next_chunk()
                except Exception as err:
                    logger.exception('An error occured during download: %s', err)
                    return
                if status:
                    logger.info(f'Download Progress: {int(status.progress() * 100)}%')
                if done:
                    break
            logger.info(f'Downloaded file {fname}')
            return topath

    def read(self, filepath: str, **kw) -> io.BytesIO:
        """Opens file from drive location as a buffered i/o stream.
        """
        fileid = self.id(filepath)
        assert fileid, 'Can only read a file, not a folder'
        s = io.BytesIO()
        request = self.cx.files().get_media(fileId=fileid)
        media = MediaIoBaseDownload(s, request)
        while True:
            status, done = media.next_chunk()
            if status:
                logger.info(f'Download {int(status.progress() * 100)}%.')
            if done:
                break
        logger.info(f'Downloaded file {Path(filepath).name}')
        return s

    def walk(self, folder: str = '/', recursive: bool = False, **kw):
        """List files in Drive folder by path, optionally recursive.
        """
        links = kw.get('links')
        ctime = kw.get('ctime')
        mtime = kw.get('mtime')
        since = kw.get('since')
        exclude_trashed = kw.get('exclude_trashed', True)
        _fields = ['id', 'name', 'mimeType']
        if links:
            _fields.append('webContentLink')
        if ctime:
            _fields.append('createdTime')
        if mtime:
            _fields.append('modifiedTime')
        fields = f"nextPageToken, files({', '.join(_fields)})"
        folderid = self.id(folder)
        q = f"'{folderid}' in parents"
        if since:
            q = f'{q} and modifiedTime>={since}'
        if exclude_trashed:
            q = f'{q} and trashed=false'
        tok = None
        while True:
            param = dict(q=q, fields=fields, pageToken=tok, **SHARED_DRIVE_EXTRA)
            resp = self.cx.files().list(**param).execute()
            files = resp.get('files') or []
            logger.info(f'Returned {len(files)} items from {folder}')
            for f in files:
                filepath = posixpath.join(folder, f['name'])
                is_folder = f['mimeType'] == 'application/vnd.google-apps.folder'
                if is_folder and recursive:
                    yield from self.walk(filepath, recursive=True, **kw)
                elif not is_folder:
                    yield filepath
            tok = resp.get('nextPageToken')
            if tok is None:
                logger.info('No more items, exiting')
                break
            logger.debug('Next page token, continuing')

    def move(self, filepath: str, to_folder: str) -> None:
        """Move file from google filepath to new parent folder.
        """
        folder, fname = os.path.split(filepath)
        assert fname, 'Only suitable for moving files, not folders'
        folderid = self.id(folder)
        fileid = self.id(filepath)
        to_folderid = self.id(to_folder)
        param = {'fileId': fileid, 'fields': 'parents', 'supportsAllDrives': True}
        oldfile = self.cx.files().get(**param).execute()
        previous_folders = ','.join(oldfile.get('parents'))
        param = {
            'fileId': fileid,
            'addParents': to_folderid,
            'removeParents': previous_folders,
            'fields': 'id, parents',
            'supportsAllDrives': True,
        }
        self.cx.files().update(**param).execute()
        self.id(filepath)
        logger.info(f'Moved {fname} to Drive folder {to_folder}')

    def write(self, filepath_or_data: str | bytes | io.IOBase, fname: str, folder: str,
              mimetype: str | None = None, overwrite: bool = True) -> None:
        """Write file to Google Drive.
        """
        self._validate_folder(folder)
        filepath, data = None, None
        if isinstance(filepath_or_data, str):
            if Path(filepath_or_data).is_dir():
                raise AttributeError(f'{filepath_or_data} is a diretory, not a file')
            if not Path(filepath_or_data).is_file():
                raise AttributeError(f'Cannot verify `isfile` against {filepath_or_data}')
            filepath = filepath_or_data
        else:
            data = filepath_or_data
        if not mimetype:
            mimetype, _ = mimetypes.guess_type(fname)
        if not mimetype:
            logger.warning(f'Unable to guess mimetype of file name {fname}, trying again')
            mimetype = filetype.guess_mime(filepath)
        if not mimetype:
            raise AttributeError(f'Cannot resolve mimetype from {fname} and {filepath}')
        to_filepath = posixpath.join(folder, fname)
        self._protect(to_filepath, overwrite)
        folderid = self.id(folder)
        if data:
            if isinstance(data, io.IOBase):
                s = data
            else:
                s = io.BytesIO()
                s.write(data)
            media = MediaIoBaseUpload(s, mimetype=mimetype, resumable=True)
        else:
            media = MediaFileUpload(filepath, mimetype=mimetype, resumable=True)
        meta = {'name': fname, 'parents': [folderid]}
        request = self.cx.files().create(media_body=media, body=meta, supportsAllDrives=True)
        response = None
        with tqdm(total=100, unit='%', desc=f'Uploading {fname}') as pbar:
            while response is None:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    pbar.update(progress - pbar.n)
        done = response
        logger.info(f"Wrote file: {done['name']} id: {done['id']} to Drive {folder}")

    def _validate_folder(self, folder: str) -> None:
        """Validate that the topmost folder in the path is a known shared drive.
        """
        folder = (folder or '').replace(os.sep, '/')
        base, *_ = filter(len, posixpath.normpath(folder).split('/'))
        settings = get_settings()
        rootid = settings.get('rootid', {})
        if base not in rootid:
            raise LookupError(f'Unknown Shared Drive {base}')

    def _protect(self, filepath: str, overwrite: bool = False) -> None:
        """Prevent overwrite of existing file unless explicitly allowed.
        """
        try:
            self.id(filepath)
        except LookupError:
            return
        if overwrite:
            logger.info(f'Overwriting existing {filepath}')
            self.delete(filepath)
            return
        raise Exception(f'{filepath} already exists')

    def _resolve_fileid(self, filepath: str) -> str | None:
        """Resolve file ID from filepath.
        """
        folder, fname = os.path.split(filepath)
        folder = posixpath.join(folder, '')
        folderid = self._resolve_folderid(folder)
        fname = clean_filename(fname)
        query = f"name='{fname}' and '{folderid}' in parents"
        page_token = None
        while True:
            param = dict(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token,
                **SHARED_DRIVE_EXTRA,
            )
            response = self.cx.files().list(**param).execute()
            for f in response.get('files', []):
                logger.debug(f"Found file: {f.get('name')}")
                return f.get('id')
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    def _resolve_folderid(self, folderpath: str) -> str | None:
        """Resolve folder ID by walking from root down to target folder.
        """
        folder, _ = os.path.split(folderpath)
        folder = posixpath.join(folder, '')
        self._validate_folder(folder)
        settings = get_settings()
        rootid_map = settings.get('rootid', {})
        if not folder or folder == '/' or folder.replace('/', '') in rootid_map:
            logger.debug('Searching root path...')
            if folder:
                folder = folder.replace('/', '')
            return rootid_map.get(folder)
        folders = list(filter(len, posixpath.normpath(folder).split('/')))
        root, *folders = folders
        rootid = rootid_map.get(root)
        folderid = rootid

        def _walk(folder, folderid):
            mimetype = 'application/vnd.google-apps.folder'
            q = f"name='{folder}' and mimeType='{mimetype}' and '{folderid}' in parents"
            page_token = None
            while True:
                param = dict(
                    q=q,
                    spaces='drive',
                    fields='nextPageToken, files(id, name)',
                    pageToken=page_token,
                    **SHARED_DRIVE_EXTRA,
                )
                response = self.cx.files().list(**param).execute()
                for f in response.get('files', []):
                    logger.info(f"Found folder: {f.get('name')}")
                    return f.get('id')
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break
            logger.debug(f'No more children, returning folder {folder}')
            return folderid

        for _folder in folders:
            _folderid = _walk(_folder, folderid)
            if folderid == _folderid:
                logger.debug(f'Could not locate folder {folder}')
                return
            folderid = _folderid

        logger.debug(f'Found folder {folder} with folderid {folderid}')
        return folderid

    def id(self, filepath: str) -> str:
        """Get drive file id for a given full file path.
        """
        filepath = filepath.replace(os.sep, '/')
        folder, fname = os.path.split(filepath)
        if fname:
            fileid = self._resolve_fileid(filepath)
            if not fileid:
                raise LookupError(f'No such file {fname} in folder {folder}')
            return fileid
        folder = posixpath.join(folder, '')
        folderid = self._resolve_folderid(folder)
        if not folderid:
            raise LookupError(f'No such folder {folder}')
        return folderid

    def exists(self, filepath: str) -> bool:
        """Check if file or folder exists in Google Drive.
        """
        try:
            self.id(filepath)
        except LookupError:
            return False
        return True
