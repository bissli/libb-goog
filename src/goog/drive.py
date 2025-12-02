"""Google Drive API client for file operations.
"""
import io
import logging
import mimetypes
import os
import posixpath
import warnings
from pathlib import Path
from typing import Any, overload

import filetype
from goog.base import Context, clean_filename, get_settings
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
        settings = get_settings()
        self._rootid = settings.get('rootid', {})
        self._tmpdir = settings.get('tmpdir')

    def _normalize_path(self, path: str, trailing_slash: bool = False) -> str:
        """Normalize path to use forward slashes.
        """
        normalized = path.replace(os.sep, '/')
        if trailing_slash:
            normalized = posixpath.join(normalized, '')
        return normalized

    def _split_path(self, path: str) -> list[str]:
        """Split path into component folders.
        """
        normalized = self._normalize_path(path)
        return list(filter(len, posixpath.normpath(normalized).split('/')))

    def _check_filepath_usage(self, method_name: str, filepath: str | None,
                              folder: str | None, filename: str | None) -> None:
        """Check and warn about deprecated filepath parameter usage.
        """
        if filepath is not None:
            if folder is not None or filename is not None:
                raise TypeError('Cannot specify both filepath and folder/filename')
            warnings.warn(
                f"{method_name}(filepath) is deprecated for filenames containing '/'. "
                f"Use {method_name}(folder=..., filename=...) instead.",
                DeprecationWarning,
                stacklevel=3
            )

    def _get_file_id(self, folder: str, filename: str) -> str | None:
        """Get file ID by folder and filename (handles filenames with /).
        """
        try:
            folderid = self.id(folder)
        except LookupError:
            logger.debug(f'Folder {folder} not found')
            return None

        clean_name = clean_filename(filename)
        query = f"name='{clean_name}' and '{folderid}' in parents"
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
            for f in response['files']:
                logger.debug(f"Found file: {f['name']}")
                return f['id']

            page_token = response.get('nextPageToken')
            if page_token is None:
                break

        return None

    @overload
    def delete(self, filepath: str) -> None: ...

    @overload
    def delete(self, *, folder: str, filename: str) -> None: ...

    def delete(self, filepath: str | None = None, *,
               folder: str | None = None, filename: str | None = None) -> None:
        """Permanently delete a file from Google Drive (files only, not folders).
        """
        self._check_filepath_usage('delete', filepath, folder, filename)

        if filepath is not None:
            fileid = self.id(filepath)
            display_name = filepath
        else:
            if folder is None or filename is None:
                raise TypeError('Must provide either filepath or both folder and filename')
            fileid = self._get_file_id(folder, filename)
            if not fileid:
                raise ValueError('Can only delete a file, not a folder')
            display_name = filename

        self.cx.files().delete(fileId=fileid, supportsAllDrives=True).execute()
        logger.info(f'Deleted {display_name} from drive')

    @overload
    def download(self, filepath: str, directory: str | None = None) -> str | None: ...

    @overload
    def download(self, *, folder: str, filename: str,
                 directory: str | None = None) -> str | None: ...

    def download(self, filepath: str | None = None, directory: str | None = None, *,
                 folder: str | None = None, filename: str | None = None) -> str | None:
        """Downloads a file from drive location to local directory.
        """
        self._check_filepath_usage('download', filepath, folder, filename)

        if filepath is not None:
            fileid = self.id(filepath)
            fname = Path(filepath).name
        else:
            if folder is None or filename is None:
                raise TypeError('Must provide either filepath or both folder and filename')
            fileid = self._get_file_id(folder, filename)
            if not fileid:
                raise ValueError('Can only download a file, not a folder')
            fname = filename

        if directory is None:
            directory = self._tmpdir
            if directory is None:
                raise ValueError('directory required when not configured')

        topath = posixpath.join(Path(directory).resolve(), fname)
        with Path(topath).open('wb') as f:
            request = self.cx.files().get_media(fileId=fileid)
            media = MediaIoBaseDownload(f, request)
            while True:
                try:
                    status, done = media.next_chunk()
                except Exception:
                    logger.exception(f'Download failed for {fname}')
                    raise
                if status:
                    logger.info(f'Download Progress: {int(status.progress() * 100)}%')
                if done:
                    break
            logger.info(f'Downloaded file {fname}')
            return topath

    @overload
    def read(self, filepath: str, **kw) -> io.BytesIO: ...

    @overload
    def read(self, *, folder: str, filename: str, **kw) -> io.BytesIO: ...

    def read(self, filepath: str | None = None, *,
             folder: str | None = None, filename: str | None = None, **kw) -> io.BytesIO:
        """Opens file from drive location as a buffered i/o stream.
        """
        self._check_filepath_usage('read', filepath, folder, filename)

        if filepath is not None:
            fileid = self.id(filepath)
            fname = Path(filepath).name
        else:
            if folder is None or filename is None:
                raise TypeError('Must provide either filepath or both folder and filename')
            fileid = self._get_file_id(folder, filename)
            if not fileid:
                raise ValueError('Can only read a file, not a folder')
            fname = filename

        s = io.BytesIO()
        request = self.cx.files().get_media(fileId=fileid)
        media = MediaIoBaseDownload(s, request)
        while True:
            status, done = media.next_chunk()
            if status:
                logger.info(f'Download {int(status.progress() * 100)}%.')
            if done:
                break
        logger.info(f'Downloaded file {fname}')
        return s

    def walk(self, folder: str = '/', recursive: bool = False,
             links: bool = False, ctime: bool = False, mtime: bool = False,
             since: str | None = None, exclude_trashed: bool = True) -> Any:
        """List files in Drive folder by path, optionally recursive.
        """
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
            files = resp['files']
            logger.info(f'Returned {len(files)} items from {folder}')
            for f in files:
                filepath = posixpath.join(folder, f['name'])
                is_folder = f['mimeType'] == 'application/vnd.google-apps.folder'
                if is_folder and recursive:
                    yield from self.walk(filepath, recursive=True, links=links,
                                         ctime=ctime, mtime=mtime, since=since,
                                         exclude_trashed=exclude_trashed)
                elif not is_folder:
                    yield filepath
            tok = resp.get('nextPageToken')
            if tok is None:
                logger.info('No more items, exiting')
                break
            logger.debug('Next page token, continuing')

    @overload
    def move(self, filepath: str, to_folder: str) -> None: ...

    @overload
    def move(self, *, folder: str, filename: str, to_folder: str) -> None: ...

    def move(self, filepath: str | None = None, to_folder: str | None = None, *,
             folder: str | None = None, filename: str | None = None) -> None:
        """Move file from google filepath to new parent folder.
        """
        self._check_filepath_usage('move', filepath, folder, filename)

        if filepath is not None:
            if to_folder is None:
                raise TypeError('to_folder is required')
            source_folder, fname = os.path.split(filepath)
            if not fname:
                raise ValueError('Only suitable for moving files, not folders')
            fileid = self.id(filepath)
        else:
            if folder is None or filename is None or to_folder is None:
                raise TypeError('Must provide folder, filename, and to_folder')
            source_folder = folder
            fname = filename
            fileid = self._get_file_id(folder, filename)
            if not fileid:
                raise ValueError('Only suitable for moving files, not folders')

        folderid = self.id(source_folder)
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
        logger.info(f'Moved {fname} to Drive folder {to_folder}')

    def write(self, filepath_or_data: str | bytes | io.IOBase, fname: str, folder: str,
              mimetype: str | None = None, overwrite: bool = True,
              mkdir_p: bool = False) -> None:
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
            raise ValueError(f'Cannot resolve mimetype from {fname} and {filepath}')
        to_filepath = posixpath.join(folder, fname)
        self._protect(to_filepath, overwrite)
        if mkdir_p and not self.exists(folder):
            folderid = self._mkdir_p(folder)
        else:
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
            pbar.update(100 - pbar.n)
        done = response
        logger.info(f"Wrote file: {done['name']} id: {done['id']} to Drive {folder}")

    def _mkdir_p(self, folder: str) -> str:
        """Create folder path recursively (like mkdir -p).
        """
        self._validate_folder(folder)
        folders = self._split_path(folder)
        root, *subfolders = folders
        rootid = self._rootid.get(root)
        if not rootid:
            raise LookupError(f'Unknown Shared Drive {root}')
        parent_id = rootid
        current_path = f'/{root}'
        for subfolder in subfolders:
            current_path = posixpath.join(current_path, subfolder)
            if self.exists(current_path):
                parent_id = self.id(current_path)
                logger.debug(f'Folder {current_path} already exists')
                continue
            meta = {
                'name': subfolder,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            created = self.cx.files().create(body=meta,
                                             supportsAllDrives=True,
                                             fields='id').execute()
            parent_id = created['id']
            logger.info(f'Created folder {current_path} with id {parent_id}')
        return parent_id

    def _validate_folder(self, folder: str) -> None:
        """Validate that the topmost folder in the path is a known shared drive.
        """
        folders = self._split_path(folder or '')
        if not folders:
            return
        base = folders[0]
        if base not in self._rootid:
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
        raise FileExistsError(f'{filepath} already exists')

    def _resolve_fileid(self, filepath: str) -> str | None:
        """Resolve file ID from filepath.
        """
        folder, fname = os.path.split(filepath)
        folder = self._normalize_path(folder, trailing_slash=True)
        folderid = self._resolve_folderid(folder)
        if folderid is None:
            return None
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
            for f in response['files']:
                logger.debug(f"Found file: {f['name']}")
                return f['id']
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    def _resolve_folderid(self, folderpath: str) -> str | None:
        """Resolve folder ID by walking from root down to target folder.
        """
        folder, _ = os.path.split(folderpath)
        folder = self._normalize_path(folder, trailing_slash=True)
        self._validate_folder(folder)

        if not folder or folder == '/' or folder.replace('/', '') in self._rootid:
            logger.debug('Searching root path...')
            if folder:
                folder = folder.replace('/', '')
            return self._rootid.get(folder)

        folders = self._split_path(folder)
        root, *folders = folders
        rootid = self._rootid.get(root)
        folderid = rootid

        for _folder in folders:
            mimetype = 'application/vnd.google-apps.folder'
            q = f"name='{_folder}' and mimeType='{mimetype}' and '{folderid}' in parents"
            tok = None
            found_id = None

            while True:
                param = dict(
                    q=q,
                    spaces='drive',
                    fields='nextPageToken, files(id, name)',
                    pageToken=tok,
                    **SHARED_DRIVE_EXTRA,
                )
                resp = self.cx.files().list(**param).execute()
                for f in resp['files']:
                    logger.info(f"Found folder: {f['name']}")
                    found_id = f['id']
                    break

                if found_id or resp.get('nextPageToken') is None:
                    break
                tok = resp['nextPageToken']

            if not found_id:
                logger.debug(f'Could not locate folder {_folder}')
                return None

            folderid = found_id

        logger.debug(f'Found folder {folder} with folderid {folderid}')
        return folderid

    def id(self, filepath: str) -> str:
        """Get drive file id for a given full file path.
        """
        filepath = self._normalize_path(filepath)
        folder, fname = os.path.split(filepath)

        if fname:
            fileid = self._resolve_fileid(filepath)
            if not fileid:
                raise LookupError(f'No such file {fname} in folder {folder}')
            return fileid

        folder = self._normalize_path(folder, trailing_slash=True)
        folderid = self._resolve_folderid(folder)
        if not folderid:
            raise LookupError(f'No such folder {folder}')
        return folderid

    @overload
    def exists(self, filepath: str) -> bool: ...

    @overload
    def exists(self, *, folder: str, filename: str) -> bool: ...

    def exists(self, filepath: str | None = None, *, folder: str | None = None,
               filename: str | None = None) -> bool:
        """Check if file or folder exists in Google Drive.
        """
        self._check_filepath_usage('exists', filepath, folder, filename)

        if filepath is not None:
            try:
                self.id(filepath)
            except LookupError:
                return False
            return True

        if folder is None or filename is None:
            raise TypeError('Must provide either filepath or both folder and filename')

        fileid = self._get_file_id(folder, filename)
        if fileid:
            logger.debug(f'Found {filename} in {folder}')
            return True

        logger.debug(f'File {filename} not found in {folder}')
        return False
