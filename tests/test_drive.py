"""Mock integration tests for Drive module.

Tests path normalization, splitting, path resolution, walk, exists, write,
delete, move, copy, search, export, and cache behavior by mocking self.cx
(Discovery API service).
"""
import os
from unittest.mock import MagicMock, patch

import pytest
from goog.base import RateLimitError, clean_filename
from goog.drive import CHANGES_FIELDS, FOLDER_MIME
from googleapiclient.errors import HttpError
from tests.fixtures.drive_responses import change_entry, changes_list_response
from tests.fixtures.drive_responses import file_entry, files_get_response
from tests.fixtures.drive_responses import files_list_response, folder_entry
from tests.fixtures.drive_responses import http_error_from_fixture
from tests.fixtures.drive_responses import load_fixture


def _setup_folder_resolution(mock_cx, segments: list[tuple[str, str]]) -> None:
    """Configure files().list().execute() to resolve folder segments.

    Each call returns the next segment's folder entry.
    After all segments are exhausted, returns empty files list.
    """
    files = mock_cx.files.return_value
    responses = []
    for name, fid in segments:
        responses.append(files_list_response([folder_entry(name, fid)]))
    responses.append(files_list_response([]))
    files.list.return_value.execute.side_effect = responses


class TestNormalizePath:
    """Tests for Drive._normalize_path().
    """

    def test_forward_slashes_unchanged(self, mock_drive):
        """Verify forward slashes pass through.
        """
        assert mock_drive._normalize_path('TestDrive/sub/file.txt') == 'TestDrive/sub/file.txt'

    def test_backslash_normalization_on_windows(self, mock_drive):
        """Verify backslashes are normalized when os.sep is backslash.
        """
        with patch.object(os, 'sep', '\\'):
            result = mock_drive._normalize_path('TestDrive\\sub\\file.txt')
        assert result == 'TestDrive/sub/file.txt'

    def test_trailing_slash(self, mock_drive):
        """Verify trailing_slash appends slash.
        """
        assert mock_drive._normalize_path('TestDrive/sub', trailing_slash=True) == 'TestDrive/sub/'

    def test_trailing_slash_already_present(self, mock_drive):
        """Verify no double trailing slash.
        """
        result = mock_drive._normalize_path('TestDrive/sub/', trailing_slash=True)
        assert result == 'TestDrive/sub/'

    def test_no_trailing_slash_default(self, mock_drive):
        """Verify no trailing slash by default.
        """
        assert mock_drive._normalize_path('TestDrive/sub') == 'TestDrive/sub'


class TestSplitPath:
    """Tests for Drive._split_path().
    """

    @pytest.mark.parametrize(('path', 'expected'), [
        ('TestDrive/sub/file.txt', ['TestDrive', 'sub', 'file.txt']),
        ('/TestDrive/sub', ['TestDrive', 'sub']),
        ('/TestDrive/sub/', ['TestDrive', 'sub']),
        ('TestDrive', ['TestDrive']),
        ('', ['.']),
        ('/A/B/C/D/E', ['A', 'B', 'C', 'D', 'E']),
    ])
    def test_split_path(self, mock_drive, path, expected):
        """Verify path splitting for various inputs.
        """
        assert mock_drive._split_path(path) == expected


class TestCleanFilename:
    """Tests for clean_filename().
    """

    @pytest.mark.parametrize(('input_name', 'expected'), [
        ("it's a file", "it\\'s a file"),
        ('normal_file.txt', 'normal_file.txt'),
        ("a'b'c", "a\\'b\\'c"),
    ])
    def test_clean_filename(self, input_name, expected):
        """Verify quote escaping in filenames.
        """
        assert clean_filename(input_name) == expected


class TestPathResolution:
    """Tests for id() and the folder/file resolution chain.
    """

    def test_root_path_returns_rootid(self, mock_drive):
        """Verify root-level path returns configured rootid directly.
        """
        result = mock_drive.id('/TestDrive/')
        assert result == 'root123'

    def test_root_path_no_trailing_slash(self, mock_drive, mock_cx):
        """Verify root name without trailing slash resolves via folder fallback.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty
        result = mock_drive.id('TestDrive')
        assert result == 'root123'

    def test_file_in_root(self, mock_drive, mock_cx):
        """Verify file directly under root resolves via files.list.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_abc')])
        files.list.return_value.execute.return_value = file_resp
        result = mock_drive.id('/TestDrive/report.pdf')
        assert result == 'file_abc'

    def test_file_in_subfolder(self, mock_drive, mock_cx):
        """Verify file in nested folder resolves via segment-by-segment walk.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('sub', 'folder_sub')])
        file_resp = files_list_response([file_entry('data.csv', 'file_data')])
        files.list.return_value.execute.side_effect = [folder_resp, file_resp]
        result = mock_drive.id('/TestDrive/sub/data.csv')
        assert result == 'file_data'

    def test_missing_file_raises(self, mock_drive, mock_cx):
        """Verify missing file raises LookupError.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty
        with pytest.raises(LookupError, match='No such file or folder'):
            mock_drive.id('/TestDrive/missing.txt')

    def test_missing_intermediate_folder_raises(self, mock_drive, mock_cx):
        """Verify missing folder in path chain raises LookupError.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty
        with pytest.raises(LookupError):
            mock_drive.id('/TestDrive/no_such_dir/file.txt')

    def test_folder_path_resolves(self, mock_drive, mock_cx):
        """Verify folder path (trailing slash) resolves to folder id.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('reports', 'folder_reports')])
        files.list.return_value.execute.return_value = folder_resp
        result = mock_drive.id('/TestDrive/reports/')
        assert result == 'folder_reports'

    def test_unknown_root_in_id_raises(self, mock_drive):
        """Verify unknown root drive raises LookupError.
        """
        with pytest.raises(LookupError, match='Unknown Shared Drive'):
            mock_drive.id('/BadDrive/file.txt')


class TestExists:
    """Tests for exists() method.
    """

    def test_exists_true(self, mock_drive, mock_cx):
        """Verify exists returns True when file is found.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_abc')])
        files.list.return_value.execute.return_value = file_resp
        assert mock_drive.exists('/TestDrive/report.pdf') is True

    def test_exists_false(self, mock_drive, mock_cx):
        """Verify exists returns False when file is not found.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty
        assert mock_drive.exists('/TestDrive/missing.txt') is False

    def test_exists_folder_filename_form(self, mock_drive, mock_cx):
        """Verify exists with folder+filename keyword form.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('sub', 'folder_sub')])
        file_resp = files_list_response([file_entry('report.pdf', 'file_abc')])
        files.list.return_value.execute.side_effect = [folder_resp, file_resp]
        assert mock_drive.exists(folder='TestDrive/sub', filename='report.pdf') is True


class TestWalk:
    """Tests for walk() generator.
    """

    def test_walk_yields_files_only(self, mock_drive, mock_cx):
        """Verify walk yields file paths, skips folders.
        """
        files = mock_cx.files.return_value
        resp = files_list_response([
            file_entry('a.txt', 'id_a', 'text/plain'),
            folder_entry('subdir', 'id_sub'),
            file_entry('b.pdf', 'id_b', 'application/pdf'),
        ])
        folder_resolve = files_list_response([folder_entry('docs', 'folder_docs')])
        files.list.return_value.execute.side_effect = [folder_resolve, resp]
        results = list(mock_drive.walk('/TestDrive/docs'))
        assert results == ['/TestDrive/docs/a.txt', '/TestDrive/docs/b.pdf']

    def test_walk_detail_yields_dicts(self, mock_drive, mock_cx):
        """Verify walk with detail=True yields metadata dicts.
        """
        files = mock_cx.files.return_value
        resp = files_list_response([
            file_entry('a.txt', 'id_a', 'text/plain'),
        ])
        folder_resolve = files_list_response([folder_entry('docs', 'folder_docs')])
        files.list.return_value.execute.side_effect = [folder_resolve, resp]
        results = list(mock_drive.walk('/TestDrive/docs', detail=True))
        assert len(results) == 1
        assert results[0]['path'] == '/TestDrive/docs/a.txt'
        assert results[0]['id'] == 'id_a'
        assert results[0]['name'] == 'a.txt'
        assert results[0]['mimeType'] == 'text/plain'

    def test_walk_recursive(self, mock_drive, mock_cx):
        """Verify recursive walk descends into subfolders.

        Call sequence for recursive walk into /TestDrive/docs/sub:
        1. id('/TestDrive/docs') -> _resolve_fileid finds 'docs' from root
        2. walk lists folder_docs contents (top_resp)
        3. Hits subfolder 'sub', calls id('/TestDrive/docs/sub')
        4. _resolve_fileid -> _resolve_folderid('/TestDrive/docs/') ->
           _resolve_segment('root123','docs')
        5. Looks for file 'sub' in folder_docs -> empty
        6. Falls back _resolve_folderid('/TestDrive/docs/sub/') ->
           docs segment cached, _resolve_segment(folder_docs,'sub')
        7. walk lists sub contents
        """
        files = mock_cx.files.return_value
        top_resp = files_list_response([
            file_entry('a.txt', 'id_a', 'text/plain'),
            folder_entry('sub', 'id_sub'),
        ])
        sub_resp = files_list_response([
            file_entry('b.txt', 'id_b', 'text/plain'),
        ])
        docs_segment = files_list_response([folder_entry('docs', 'folder_docs')])
        sub_segment = files_list_response([folder_entry('sub', 'id_sub')])
        empty = files_list_response([])

        folder_resolve = files_list_response([folder_entry('docs', 'folder_docs')])
        files.list.return_value.execute.side_effect = [
            folder_resolve, top_resp,
            docs_segment, empty, sub_segment, sub_resp,
        ]
        results = list(mock_drive.walk('/TestDrive/docs', recursive=True))
        assert '/TestDrive/docs/a.txt' in results
        assert '/TestDrive/docs/sub/b.txt' in results

    def test_walk_pagination(self, mock_drive, mock_cx):
        """Verify walk handles multi-page responses.
        """
        files = mock_cx.files.return_value
        page1 = files_list_response(
            [file_entry('a.txt', 'id_a', 'text/plain')],
            next_page_token='tok2')
        page2 = files_list_response(
            [file_entry('b.txt', 'id_b', 'text/plain')])
        folder_resolve = files_list_response([folder_entry('docs', 'folder_docs')])
        files.list.return_value.execute.side_effect = [folder_resolve, page1, page2]
        results = list(mock_drive.walk('/TestDrive/docs'))
        assert results == ['/TestDrive/docs/a.txt', '/TestDrive/docs/b.txt']

    def test_walk_root(self, mock_drive, mock_cx):
        """Verify walk at root level.
        """
        files = mock_cx.files.return_value
        resp = files_list_response([
            file_entry('readme.md', 'id_readme', 'text/plain'),
            folder_entry('folder1', 'id_f1'),
        ])
        files.list.return_value.execute.return_value = resp
        results = list(mock_drive.walk('/TestDrive'))
        assert results == ['/TestDrive/readme.md']


class TestWalkFlat:
    """Tests for walk(flat=True) drive-wide scan mode.
    """

    def _setup_flat_responses(self, mock_cx, pages):
        """Configure files().list() for flat walk pages.

        Each page is a list of dicts with 'files' key and optional
        'nextPageToken'.
        """
        files = mock_cx.files.return_value
        files.list.return_value.execute.side_effect = pages

    def test_walk_flat_yields_files_under_prefix(
        self, mock_drive, mock_cx,
    ):
        """Verify flat walk yields files under requested folder.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response([
                {**folder_entry('SEC', 'folder_sec'),
                 'parents': ['root123']},
                {**file_entry('a.htm', 'id_a', 'text/html'),
                 'parents': ['folder_sec']},
                {**file_entry('b.htm', 'id_b', 'text/html'),
                 'parents': ['folder_sec']},
                {**file_entry('other.txt', 'id_oth', 'text/plain'),
                 'parents': ['root456']},
            ]),
        ])
        results = list(mock_drive.walk(
            'TestDrive/SEC', recursive=True, flat=True))
        assert results == [
            'TestDrive/SEC/a.htm',
            'TestDrive/SEC/b.htm',
        ]

    def test_walk_flat_detail_yields_dicts(
        self, mock_drive, mock_cx,
    ):
        """Verify flat walk with detail=True yields metadata dicts.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response([
                {**folder_entry('SEC', 'folder_sec'),
                 'parents': ['root123']},
                {**file_entry('a.htm', 'id_a', 'text/html'),
                 'parents': ['folder_sec']},
            ]),
        ])
        results = list(mock_drive.walk(
            'TestDrive/SEC', recursive=True,
            flat=True, detail=True))
        assert len(results) == 1
        assert results[0]['path'] == 'TestDrive/SEC/a.htm'
        assert results[0]['id'] == 'id_a'

    def test_walk_flat_nested_folders(
        self, mock_drive, mock_cx,
    ):
        """Verify flat walk reconstructs deep paths.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response([
                {**folder_entry('SEC', 'folder_sec'),
                 'parents': ['root123']},
                {**folder_entry('AAPL', 'folder_aapl'),
                 'parents': ['folder_sec']},
                {**file_entry('10-K.htm', 'id_10k', 'text/html'),
                 'parents': ['folder_aapl']},
            ]),
        ])
        results = list(mock_drive.walk(
            'TestDrive/SEC', recursive=True, flat=True))
        assert results == ['TestDrive/SEC/AAPL/10-K.htm']

    def test_walk_flat_pagination(self, mock_drive, mock_cx):
        """Verify flat walk handles multi-page responses.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response(
                [{**folder_entry('SEC', 'folder_sec'),
                  'parents': ['root123']},
                 {**file_entry('a.htm', 'id_a', 'text/html'),
                  'parents': ['folder_sec']}],
                next_page_token='tok2'),
            files_list_response(
                [{**file_entry('b.htm', 'id_b', 'text/html'),
                  'parents': ['folder_sec']}]),
        ])
        results = list(mock_drive.walk(
            'TestDrive/SEC', recursive=True, flat=True))
        assert len(results) == 2
        assert 'TestDrive/SEC/a.htm' in results
        assert 'TestDrive/SEC/b.htm' in results

    def test_walk_flat_uses_corpora_drive(
        self, mock_drive, mock_cx,
    ):
        """Verify flat walk passes corpora=drive and driveId.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response([]),
        ])
        list(mock_drive.walk(
            'TestDrive', recursive=True, flat=True))
        call_kwargs = (mock_cx.files.return_value
                       .list.call_args[1])
        assert call_kwargs['corpora'] == 'drive'
        assert call_kwargs['driveId'] == 'root123'
        assert call_kwargs['pageSize'] == 1000

    def test_walk_flat_skips_orphaned_files(
        self, mock_drive, mock_cx,
    ):
        """Verify files with unresolvable parents are skipped.
        """
        self._setup_flat_responses(mock_cx, [
            files_list_response([
                {**file_entry('orphan.htm', 'id_orph', 'text/html'),
                 'parents': ['unknown_parent']},
            ]),
        ])
        results = list(mock_drive.walk(
            'TestDrive', recursive=True, flat=True))
        assert results == []


class TestDelete:
    """Tests for delete() method.
    """

    def test_delete_by_filepath(self, mock_drive, mock_cx):
        """Verify delete resolves path and calls files().delete().
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('old.txt', 'file_old')])
        files.list.return_value.execute.return_value = file_resp
        files.delete.return_value.execute.return_value = None
        mock_drive.delete('/TestDrive/old.txt')
        files.delete.assert_called_once()
        call_kwargs = files.delete.call_args[1]
        assert call_kwargs['fileId'] == 'file_old'

    def test_delete_by_folder_filename(self, mock_drive, mock_cx):
        """Verify delete with folder+filename form.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('docs', 'folder_docs')])
        file_resp = files_list_response([file_entry('old.txt', 'file_old')])
        files.list.return_value.execute.side_effect = [folder_resp, file_resp]
        files.delete.return_value.execute.return_value = None
        mock_drive.delete(folder='TestDrive/docs', filename='old.txt')
        files.delete.assert_called_once()
        call_kwargs = files.delete.call_args[1]
        assert call_kwargs['fileId'] == 'file_old'


class TestProtect:
    """Tests for _protect() overwrite guard.
    """

    def test_protect_no_existing_file(self, mock_drive, mock_cx):
        """Verify _protect does nothing when file doesn't exist.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty
        mock_drive._protect('/TestDrive/new.txt', overwrite=False)

    def test_protect_existing_no_overwrite_raises(self, mock_drive, mock_cx):
        """Verify _protect raises FileExistsError when overwrite=False.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('existing.txt', 'file_exists')])
        files.list.return_value.execute.return_value = file_resp
        with pytest.raises(FileExistsError, match='already exists'):
            mock_drive._protect('/TestDrive/existing.txt', overwrite=False)

    def test_protect_existing_overwrite_deletes(self, mock_drive, mock_cx):
        """Verify _protect deletes existing file when overwrite=True.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('existing.txt', 'file_exists')])
        files.list.return_value.execute.return_value = file_resp
        files.delete.return_value.execute.return_value = None
        mock_drive._protect('/TestDrive/existing.txt', overwrite=True)
        files.delete.assert_called_once()
        call_kwargs = files.delete.call_args[1]
        assert call_kwargs['fileId'] == 'file_exists'


class TestValidateFolder:
    """Tests for _validate_folder().
    """

    def test_known_root_passes(self, mock_drive):
        """Verify known root does not raise.
        """
        mock_drive._validate_folder('TestDrive/sub')

    def test_unknown_root_raises(self, mock_drive):
        """Verify unknown root raises LookupError.
        """
        with pytest.raises(LookupError, match='Unknown Shared Drive'):
            mock_drive._validate_folder('BadDrive/sub')

    def test_empty_folder_passes(self, mock_drive):
        """Verify empty folder string is allowed (early return).
        """
        mock_drive._validate_folder('')


class TestSearch:
    """Tests for search() method.
    """

    def test_search_returns_results(self, mock_drive, mock_cx):
        """Verify search returns file list from API.
        """
        files = mock_cx.files.return_value
        resp = files_list_response([
            file_entry('match.txt', 'id_match', 'text/plain'),
        ])
        files.list.return_value.execute.return_value = resp
        results = mock_drive.search(query="name contains 'match'")
        assert len(results) == 1
        assert results[0]['name'] == 'match.txt'

    def test_search_respects_limit(self, mock_drive, mock_cx):
        """Verify search stops at limit.
        """
        files = mock_cx.files.return_value
        many_files = [file_entry(f'f{i}.txt', f'id_{i}') for i in range(10)]
        resp = files_list_response(many_files)
        files.list.return_value.execute.return_value = resp
        results = mock_drive.search(query='name contains "f"', limit=3)
        assert len(results) == 3

    def test_search_with_folder(self, mock_drive, mock_cx):
        """Verify search scoped to folder resolves folder first.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('docs', 'folder_docs')])
        search_resp = files_list_response([file_entry('a.txt', 'id_a')])
        files.list.return_value.execute.side_effect = [folder_resp, search_resp]
        results = mock_drive.search(folder='/TestDrive/docs')
        assert len(results) == 1


class TestChangesToken:
    """Tests for changes_token() method.
    """

    def test_returns_token_string(self, mock_drive, mock_cx):
        """Verify changes_token returns the startPageToken string.
        """
        changes = mock_cx.changes.return_value
        changes.getStartPageToken.return_value.execute.return_value = {
            'startPageToken': '12345',
        }
        result = mock_drive.changes_token()
        assert result == '12345'

    def test_always_passes_supports_all_drives(self, mock_drive, mock_cx):
        """Verify supportsAllDrives is always passed, even without drive_id.
        """
        changes = mock_cx.changes.return_value
        changes.getStartPageToken.return_value.execute.return_value = {
            'startPageToken': '12345',
        }
        mock_drive.changes_token()
        call_kwargs = changes.getStartPageToken.call_args[1]
        assert call_kwargs['supportsAllDrives'] is True
        assert 'driveId' not in call_kwargs

    def test_passes_drive_id(self, mock_drive, mock_cx):
        """Verify driveId is passed when drive_id is provided.
        """
        changes = mock_cx.changes.return_value
        changes.getStartPageToken.return_value.execute.return_value = {
            'startPageToken': 'drive_tok',
        }
        result = mock_drive.changes_token(drive_id='drive_abc')
        call_kwargs = changes.getStartPageToken.call_args[1]
        assert call_kwargs['driveId'] == 'drive_abc'
        assert call_kwargs['supportsAllDrives'] is True
        assert result == 'drive_tok'


class TestChanges:
    """Tests for changes() method.
    """

    def test_returns_changes_and_token(self, mock_drive, mock_cx):
        """Verify changes returns tuple of (changes_list, new_token).
        """
        changes_resource = mock_cx.changes.return_value
        entry = change_entry(
            'file_1', file=file_entry('doc.txt', 'file_1'))
        resp = changes_list_response(
            [entry], new_start_page_token='new_tok')
        changes_resource.list.return_value.execute.return_value = resp
        result_changes, result_token = mock_drive.changes('start_tok')
        assert len(result_changes) == 1
        assert result_changes[0]['fileId'] == 'file_1'
        assert result_token == 'new_tok'

    def test_handles_pagination(self, mock_drive, mock_cx):
        """Verify changes combines results across multiple pages.
        """
        changes_resource = mock_cx.changes.return_value
        entry1 = change_entry(
            'file_1', file=file_entry('a.txt', 'file_1'))
        entry2 = change_entry(
            'file_2', file=file_entry('b.txt', 'file_2'))
        page1 = changes_list_response(
            [entry1], next_page_token='page2_tok')
        page2 = changes_list_response(
            [entry2], new_start_page_token='final_tok')
        changes_resource.list.return_value.execute.side_effect = [
            page1, page2]
        result_changes, result_token = mock_drive.changes(
            'start_tok', limit=2000)
        assert len(result_changes) == 2
        assert result_changes[0]['fileId'] == 'file_1'
        assert result_changes[1]['fileId'] == 'file_2'
        assert result_token == 'final_tok'

    def test_respects_limit(self, mock_drive, mock_cx):
        """Verify changes stops at limit and returns resume token.
        """
        changes_resource = mock_cx.changes.return_value
        entries = [
            change_entry(f'file_{i}', file=file_entry(f'f{i}.txt', f'file_{i}'))
            for i in range(5)]
        resp = changes_list_response(
            entries, next_page_token='resume_tok')
        changes_resource.list.return_value.execute.return_value = resp
        result_changes, result_token = mock_drive.changes(
            'start_tok', limit=3)
        assert len(result_changes) == 3
        assert result_token == 'resume_tok'

    def test_empty_changes(self, mock_drive, mock_cx):
        """Verify empty changes returns empty list with valid token.
        """
        changes_resource = mock_cx.changes.return_value
        resp = changes_list_response([], new_start_page_token='new_tok')
        changes_resource.list.return_value.execute.return_value = resp
        result_changes, result_token = mock_drive.changes('start_tok')
        assert result_changes == []
        assert result_token == 'new_tok'

    def test_passes_drive_id(self, mock_drive, mock_cx):
        """Verify driveId is passed to changes.list when provided.
        """
        changes_resource = mock_cx.changes.return_value
        resp = changes_list_response([], new_start_page_token='tok')
        changes_resource.list.return_value.execute.return_value = resp
        mock_drive.changes('start_tok', drive_id='drive_xyz')
        call_kwargs = changes_resource.list.call_args[1]
        assert call_kwargs['driveId'] == 'drive_xyz'

    def test_always_includes_shared_drive_params(self, mock_drive, mock_cx):
        """Verify SHARED_DRIVE_EXTRA params are always passed.
        """
        changes_resource = mock_cx.changes.return_value
        resp = changes_list_response([], new_start_page_token='tok')
        changes_resource.list.return_value.execute.return_value = resp
        mock_drive.changes('start_tok')
        call_kwargs = changes_resource.list.call_args[1]
        assert call_kwargs['includeItemsFromAllDrives'] is True
        assert call_kwargs['supportsAllDrives'] is True

    def test_passes_changes_fields(self, mock_drive, mock_cx):
        """Verify CHANGES_FIELDS is passed as fields parameter.
        """
        changes_resource = mock_cx.changes.return_value
        resp = changes_list_response([], new_start_page_token='tok')
        changes_resource.list.return_value.execute.return_value = resp
        mock_drive.changes('start_tok')
        call_kwargs = changes_resource.list.call_args[1]
        assert call_kwargs['fields'] == CHANGES_FIELDS

    def test_removed_file_has_no_file_metadata(self, mock_drive, mock_cx):
        """Verify removed entries without file metadata are returned as-is.
        """
        changes_resource = mock_cx.changes.return_value
        entry = change_entry('file_gone', removed=True)
        resp = changes_list_response(
            [entry], new_start_page_token='new_tok')
        changes_resource.list.return_value.execute.return_value = resp
        result_changes, _ = mock_drive.changes('start_tok')
        assert len(result_changes) == 1
        assert result_changes[0]['removed'] is True
        assert 'file' not in result_changes[0]


class TestResolveParent:
    """Tests for _resolve_parent() cached helper.
    """

    def test_returns_name_and_parent_id(self, mock_drive, mock_cx):
        """Verify _resolve_parent returns (name, parent_id) tuple.
        """
        files = mock_cx.files.return_value
        files.get.return_value.execute.return_value = {
            'name': 'SEC',
            'parents': ['root123'],
        }
        result = mock_drive._resolve_parent('folder_sec')
        assert result == ('SEC', 'root123')
        files.get.assert_called_once()
        call_kwargs = files.get.call_args[1]
        assert call_kwargs['fileId'] == 'folder_sec'
        assert 'parents' in call_kwargs['fields']

    def test_returns_none_on_404(self, mock_drive, mock_cx):
        """Verify _resolve_parent returns None for missing folders.
        """
        files = mock_cx.files.return_value
        resp = MagicMock()
        resp.status = 404
        resp.reason = 'Not Found'
        files.get.return_value.execute.side_effect = HttpError(
            resp, b'not found')
        result = mock_drive._resolve_parent('gone_folder')
        assert result is None

    def test_no_parents_returns_none_parent(self, mock_drive, mock_cx):
        """Verify folder with no parents returns (name, None).
        """
        files = mock_cx.files.return_value
        files.get.return_value.execute.return_value = {
            'name': 'orphan',
        }
        result = mock_drive._resolve_parent('orphan_id')
        assert result == ('orphan', None)


class TestPathFromId:
    """Tests for path_from_id() reverse resolution.
    """

    def _mock_files_get(self, mock_cx, responses):
        """Set up files().get() to return different responses by fileId.
        """
        files = mock_cx.files.return_value

        def get_side_effect(**kwargs):
            fid = kwargs['fileId']
            if fid in responses:
                mock_exec = MagicMock()
                mock_exec.execute.return_value = responses[fid]
                return mock_exec
            mock_exec = MagicMock()
            resp = MagicMock()
            resp.status = 404
            resp.reason = 'Not Found'
            mock_exec.execute.side_effect = HttpError(
                resp, b'not found')
            return mock_exec

        files.get.side_effect = get_side_effect

    def test_simple_two_level_path(self, mock_drive, mock_cx):
        """Verify path reconstruction: root/SEC/doc.txt.
        """
        self._mock_files_get(mock_cx, {
            'file_1': {
                'name': 'doc.txt',
                'parents': ['folder_sec'],
            },
            'folder_sec': {
                'name': 'SEC',
                'parents': ['root123'],
            },
        })
        result = mock_drive.path_from_id('file_1')
        assert result == 'TestDrive/SEC/doc.txt'

    def test_file_directly_under_root(self, mock_drive, mock_cx):
        """Verify path for file directly under a root folder.
        """
        self._mock_files_get(mock_cx, {
            'file_1': {
                'name': 'readme.txt',
                'parents': ['root123'],
            },
        })
        result = mock_drive.path_from_id('file_1')
        assert result == 'TestDrive/readme.txt'

    def test_deep_path(self, mock_drive, mock_cx):
        """Verify path with 3 intermediate folders.
        """
        self._mock_files_get(mock_cx, {
            'file_1': {
                'name': 'report.pdf',
                'parents': ['folder_aapl'],
            },
            'folder_aapl': {
                'name': 'AAPL',
                'parents': ['folder_sec'],
            },
            'folder_sec': {
                'name': 'SEC',
                'parents': ['root123'],
            },
        })
        result = mock_drive.path_from_id('file_1')
        assert result == 'TestDrive/SEC/AAPL/report.pdf'

    def test_outside_known_roots(self, mock_drive, mock_cx):
        """Verify None for files not under any known root.
        """
        self._mock_files_get(mock_cx, {
            'file_1': {
                'name': 'doc.txt',
                'parents': ['unknown_folder'],
            },
            'unknown_folder': {
                'name': 'SomeFolder',
                'parents': ['other_unknown'],
            },
            'other_unknown': {
                'name': 'Top',
            },
        })
        result = mock_drive.path_from_id('file_1')
        assert result is None

    def test_file_not_found(self, mock_drive, mock_cx):
        """Verify None when file ID does not exist.
        """
        self._mock_files_get(mock_cx, {})
        result = mock_drive.path_from_id('nonexistent')
        assert result is None

    def test_depth_limit(self, mock_drive, mock_cx):
        """Verify None when parent chain exceeds depth limit.
        """
        responses = {}
        responses['file_1'] = {
            'name': 'doc.txt',
            'parents': ['folder_0'],
        }
        for i in range(60):
            responses[f'folder_{i}'] = {
                'name': f'level_{i}',
                'parents': [f'folder_{i + 1}'],
            }
        self._mock_files_get(mock_cx, responses)
        result = mock_drive.path_from_id('file_1')
        assert result is None

    def test_uses_other_root(self, mock_drive, mock_cx):
        """Verify resolution works with multiple roots.
        """
        self._mock_files_get(mock_cx, {
            'file_1': {
                'name': 'data.csv',
                'parents': ['root456'],
            },
        })
        result = mock_drive.path_from_id('file_1')
        assert result == 'Other/data.csv'

    def test_sibling_files_use_cache(self, mock_drive, mock_cx):
        """Verify second sibling reuses cached parent resolution.
        """
        self._mock_files_get(mock_cx, {
            'file_a': {
                'name': 'a.txt',
                'parents': ['folder_sec'],
            },
            'file_b': {
                'name': 'b.txt',
                'parents': ['folder_sec'],
            },
            'folder_sec': {
                'name': 'SEC',
                'parents': ['root123'],
            },
        })
        result_a = mock_drive.path_from_id('file_a')
        assert result_a == 'TestDrive/SEC/a.txt'

        call_count = mock_cx.files.return_value.get.call_count
        result_b = mock_drive.path_from_id('file_b')
        assert result_b == 'TestDrive/SEC/b.txt'
        assert mock_cx.files.return_value.get.call_count == call_count + 1


class TestCopy:
    """Tests for copy() method.
    """

    def test_copy_to_new_name(self, mock_drive, mock_cx):
        """Verify copy with new name in same folder.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('orig.txt', 'file_orig')])
        copy_resp = {'id': 'copy_id', 'name': 'renamed.txt'}
        files.list.return_value.execute.return_value = file_resp
        files.copy.return_value.execute.return_value = copy_resp
        result = mock_drive.copy('/TestDrive/orig.txt', to_name='renamed.txt')
        assert result == 'copy_id'
        files.copy.assert_called_once()
        call_kwargs = files.copy.call_args[1]
        assert call_kwargs['fileId'] == 'file_orig'
        assert call_kwargs['body']['name'] == 'renamed.txt'

    def test_copy_to_different_folder(self, mock_drive, mock_cx):
        """Verify copy to a different folder.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('orig.txt', 'file_orig')])
        dest_folder = files_list_response([folder_entry('dest', 'folder_dest')])
        copy_resp = {'id': 'copy_id', 'name': 'orig.txt'}
        files.list.return_value.execute.side_effect = [file_resp, dest_folder]
        files.copy.return_value.execute.return_value = copy_resp
        result = mock_drive.copy('/TestDrive/orig.txt', to_folder='/TestDrive/dest')
        assert result == 'copy_id'
        call_kwargs = files.copy.call_args[1]
        assert call_kwargs['body']['parents'] == ['folder_dest']


class TestMove:
    """Tests for move() method.
    """

    def test_move_file(self, mock_drive, mock_cx):
        """Verify move resolves source and dest, calls update with correct parents.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('doc.txt', 'file_doc')])
        dest_folder = files_list_response([folder_entry('archive', 'folder_archive')])
        parents_resp = {'parents': ['root123']}
        update_resp = {'id': 'file_doc', 'parents': ['folder_archive']}
        files.list.return_value.execute.side_effect = [file_resp, dest_folder]
        files.get.return_value.execute.return_value = parents_resp
        files.update.return_value.execute.return_value = update_resp
        mock_drive.move('/TestDrive/doc.txt', '/TestDrive/archive')
        files.update.assert_called_once()
        call_kwargs = files.update.call_args[1]
        assert call_kwargs['addParents'] == 'folder_archive'
        assert 'root123' in call_kwargs['removeParents']

    def test_move_folder_trailing_slash(self, mock_drive, mock_cx):
        """Verify folder move with trailing-slash path resolves and updates parents.
        """
        files = mock_cx.files.return_value
        src_resolve = files_list_response(
            [folder_entry('mydir', 'folder_mydir')])
        dest_resolve = files_list_response(
            [folder_entry('archive', 'folder_archive')])
        parents_resp = {'parents': ['root123']}
        update_resp = {'id': 'folder_mydir', 'parents': ['folder_archive']}
        files.list.return_value.execute.side_effect = [
            src_resolve, dest_resolve]
        files.get.return_value.execute.return_value = parents_resp
        files.update.return_value.execute.return_value = update_resp
        mock_drive.move('/TestDrive/mydir/', '/TestDrive/archive/')
        files.update.assert_called_once()
        call_kwargs = files.update.call_args[1]
        assert call_kwargs['addParents'] == 'folder_archive'
        assert 'root123' in call_kwargs['removeParents']

    def test_move_folder_no_trailing_slash(self, mock_drive, mock_cx):
        """Verify folder move without trailing slash uses id() fallback to folder.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        src_resolve = files_list_response(
            [folder_entry('mydir', 'folder_mydir')])
        dest_resolve = files_list_response(
            [folder_entry('archive', 'folder_archive')])
        parents_resp = {'parents': ['root123']}
        update_resp = {'id': 'folder_mydir', 'parents': ['folder_archive']}
        files.list.return_value.execute.side_effect = [
            empty, src_resolve, dest_resolve]
        files.get.return_value.execute.return_value = parents_resp
        files.update.return_value.execute.return_value = update_resp
        mock_drive.move('/TestDrive/mydir', '/TestDrive/archive/')
        files.update.assert_called_once()
        call_kwargs = files.update.call_args[1]
        assert call_kwargs['addParents'] == 'folder_archive'

    def test_move_not_found_raises(self, mock_drive, mock_cx):
        """Verify move raises LookupError when item not found via folder/filename.
        """
        files = mock_cx.files.return_value
        files.list.return_value.execute.return_value = files_list_response([])
        with pytest.raises(LookupError):
            mock_drive.move(folder='/TestDrive/', filename='ghost.txt',
                            to_folder='/TestDrive/archive/')


class TestInfo:
    """Tests for info() method.
    """

    def test_info_returns_metadata(self, mock_drive, mock_cx):
        """Verify info returns file metadata dict.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_report')])
        meta_resp = files_get_response('file_report', 'report.pdf', 'application/pdf')
        files.list.return_value.execute.return_value = file_resp
        files.get.return_value.execute.return_value = meta_resp
        result = mock_drive.info('/TestDrive/report.pdf')
        assert result['id'] == 'file_report'
        assert result['name'] == 'report.pdf'

    def test_info_with_realistic_fixture(self, mock_drive, mock_cx):
        """Verify info works with real API response shapes.
        """
        files = mock_cx.files.return_value
        lookup = load_fixture('files_list_file_lookup')
        metadata = load_fixture('files_get_metadata')
        files.list.return_value.execute.return_value = lookup
        files.get.return_value.execute.return_value = metadata
        result = mock_drive.info('/TestDrive/test_file.txt')
        assert result['id'] == metadata['id']
        assert result['name'] == 'test_file.txt'
        assert 'size' in result
        assert 'webViewLink' in result


class TestRead:
    """Tests for read() method.
    """

    def test_read_returns_bytesio(self, mock_drive, mock_cx):
        """Verify read returns BytesIO at position 0.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('data.bin', 'file_data')])
        files.list.return_value.execute.return_value = file_resp

        content = b'hello world'

        def fake_download_init(fh, request):
            fh.write(content)
            return MagicMock(next_chunk=MagicMock(return_value=(None, True)))

        with patch('goog.drive.MediaIoBaseDownload') as mock_dl:
            mock_instance = MagicMock()
            mock_instance.next_chunk.return_value = (None, True)

            def init_side_effect(fh, request):
                fh.write(content)
                return mock_instance

            mock_dl.side_effect = init_side_effect
            result = mock_drive.read('/TestDrive/data.bin')

        assert result.read() == content
        assert result.tell() == len(content)


class TestDownload:
    """Tests for download() method.
    """

    def test_download_to_directory(self, mock_drive, mock_cx, tmp_path):
        """Verify download writes file to specified directory.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_report')])
        files.list.return_value.execute.return_value = file_resp

        with patch('goog.drive.MediaIoBaseDownload') as mock_dl:
            mock_instance = MagicMock()
            mock_instance.next_chunk.return_value = (None, True)
            mock_dl.return_value = mock_instance
            result = mock_drive.download('/TestDrive/report.pdf', directory=str(tmp_path))

        assert result is not None
        assert 'report.pdf' in result

    def test_download_missing_directory_raises(self, mock_drive, mock_cx):
        """Verify download raises when no directory configured.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_report')])
        files.list.return_value.execute.return_value = file_resp
        mock_drive._tmpdir = None
        with pytest.raises(ValueError, match='directory required'):
            mock_drive.download('/TestDrive/report.pdf')


class TestExport:
    """Tests for export() method.
    """

    def test_export_with_explicit_mime(self, mock_drive, mock_cx):
        """Verify export with explicit mime_type skips auto-detection.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('doc', 'file_doc')])
        files.list.return_value.execute.return_value = file_resp

        content = b'exported text'

        with patch('goog.drive.MediaIoBaseDownload') as mock_dl:
            mock_instance = MagicMock()
            mock_instance.next_chunk.return_value = (None, True)

            def init_side_effect(fh, request):
                fh.write(content)
                return mock_instance

            mock_dl.side_effect = init_side_effect
            result = mock_drive.export('/TestDrive/doc', mime_type='text/plain')

        assert result.read() == content
        files.get.assert_not_called()

    def test_export_auto_detects_mime(self, mock_drive, mock_cx):
        """Verify export auto-detects mime type from GOOGLE_EXPORT_DEFAULTS.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('sheet', 'file_sheet')])
        files.list.return_value.execute.return_value = file_resp
        files.get.return_value.execute.return_value = {
            'mimeType': 'application/vnd.google-apps.spreadsheet',
        }

        with patch('goog.drive.MediaIoBaseDownload') as mock_dl:
            mock_instance = MagicMock()
            mock_instance.next_chunk.return_value = (None, True)
            mock_dl.return_value = mock_instance
            mock_drive.export('/TestDrive/sheet')

        files.export_media.assert_called_once()
        call_kwargs = files.export_media.call_args[1]
        assert call_kwargs['mimeType'] == 'text/csv'

    def test_export_unknown_type_raises(self, mock_drive, mock_cx):
        """Verify export raises for unsupported source type without explicit mime.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('weird', 'file_weird')])
        files.list.return_value.execute.return_value = file_resp
        files.get.return_value.execute.return_value = {
            'mimeType': 'application/octet-stream',
        }
        with pytest.raises(ValueError, match='No default export type'):
            mock_drive.export('/TestDrive/weird')


class TestWrite:
    """Tests for write() method.
    """

    def test_write_from_bytes(self, mock_drive, mock_cx):
        """Verify write from bytes data calls files().create().
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty

        request_mock = MagicMock()
        request_mock.next_chunk.return_value = (None, {'name': 'test.txt', 'id': 'new_id'})
        files.create.return_value = request_mock

        with patch('goog.drive.MediaIoBaseUpload'):
            mock_drive.write(b'hello', 'test.txt', '/TestDrive',
                             mimetype='text/plain')

        files.create.assert_called_once()
        call_kwargs = files.create.call_args[1]
        assert call_kwargs['body']['name'] == 'test.txt'
        assert call_kwargs['body']['parents'] == ['root123']

    def test_write_from_filepath(self, mock_drive, mock_cx, tmp_path):
        """Verify write from local file path.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        files.list.return_value.execute.return_value = empty

        local_file = tmp_path / 'upload.txt'
        local_file.write_text('content')

        request_mock = MagicMock()
        request_mock.next_chunk.return_value = (None, {'name': 'upload.txt', 'id': 'new_id'})
        files.create.return_value = request_mock

        with patch('goog.drive.MediaFileUpload'):
            mock_drive.write(str(local_file), 'upload.txt', '/TestDrive',
                             mimetype='text/plain')

        files.create.assert_called_once()


class TestCacheBehavior:
    """Tests for folder resolution caching.
    """

    def test_resolve_segment_is_cached(self, mock_drive, mock_cx):
        """Verify repeated calls use cache, not API.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('sub', 'folder_sub')])
        mock_drive.clear_cache()
        files.list.return_value.execute.return_value = folder_resp
        r1 = mock_drive._resolve_segment('root123', 'sub')
        r2 = mock_drive._resolve_segment('root123', 'sub')
        assert r1 == r2 == 'folder_sub'
        assert files.list.return_value.execute.call_count == 1

    def test_clear_cache_invalidates(self, mock_drive, mock_cx):
        """Verify clear_cache forces fresh API call.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response([folder_entry('sub', 'folder_sub')])
        mock_drive.clear_cache()
        files.list.return_value.execute.return_value = folder_resp
        mock_drive._resolve_segment('root123', 'sub')
        mock_drive.clear_cache()
        mock_drive._resolve_segment('root123', 'sub')
        assert files.list.return_value.execute.call_count == 2


class TestTrashedFilter:
    """Tests for trashed=false in name-based resolution queries.
    """

    def test_get_file_id_excludes_trashed(self, mock_drive, mock_cx):
        """Verify _get_file_id query includes trashed=false.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response([file_entry('report.pdf', 'file_abc')])
        files.list.return_value.execute.return_value = file_resp
        mock_drive._get_file_id('/TestDrive/', 'report.pdf')
        query = files.list.call_args[1]['q']
        assert 'trashed=false' in query

    def test_resolve_fileid_excludes_trashed(self, mock_drive, mock_cx):
        """Verify _resolve_fileid query includes trashed=false.
        """
        files = mock_cx.files.return_value
        file_resp = files_list_response(
            [file_entry('data.csv', 'file_data')])
        files.list.return_value.execute.return_value = file_resp
        mock_drive._resolve_fileid('/TestDrive/data.csv')
        query = files.list.call_args[1]['q']
        assert 'trashed=false' in query

    def test_resolve_segment_excludes_trashed(self, mock_drive, mock_cx):
        """Verify _resolve_segment query includes trashed=false.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response(
            [folder_entry('sub', 'folder_sub')])
        mock_drive.clear_cache()
        files.list.return_value.execute.return_value = folder_resp
        mock_drive._resolve_segment('root123', 'sub')
        query = files.list.call_args[1]['q']
        assert 'trashed=false' in query


class TestMakedirs:
    """Tests for makedirs() recursive folder creation.
    """

    def test_makedirs_creates_nested_folders(self, mock_drive, mock_cx):
        """Verify makedirs creates missing segments and returns final ID.
        """
        files = mock_cx.files.return_value
        create_resp = {'id': 'new_deep_id'}
        files.create.return_value.execute.return_value = create_resp
        segment_map = {
            ('root123', 'sub'): 'folder_sub',
            ('folder_sub', 'deep'): None,
            }
        with patch.object(mock_drive, '_resolve_segment',
                          side_effect=lambda pid, seg: segment_map[(pid, seg)]):
            result = mock_drive.makedirs('/TestDrive/sub/deep')
        assert result == 'new_deep_id'
        files.create.assert_called_once()
        body = files.create.call_args[1]['body']
        assert body['name'] == 'deep'
        assert body['mimeType'] == FOLDER_MIME
        assert body['parents'] == ['folder_sub']

    def test_makedirs_all_exist(self, mock_drive, mock_cx):
        """Verify makedirs returns existing folder ID when path exists.
        """
        files = mock_cx.files.return_value
        segment_map = {
            ('root123', 'sub'): 'folder_sub',
            ('folder_sub', 'deep'): 'folder_deep',
            }
        with patch.object(mock_drive, '_resolve_segment',
                          side_effect=lambda pid, seg: segment_map[(pid, seg)]):
            result = mock_drive.makedirs('/TestDrive/sub/deep')
        assert result == 'folder_deep'
        files.create.assert_not_called()

    def test_makedirs_unknown_root_raises(self, mock_drive):
        """Verify makedirs raises LookupError for unknown root.
        """
        with pytest.raises(LookupError, match='Unknown Shared Drive'):
            mock_drive.makedirs('/BadDrive/sub')

    def test_makedirs_clears_cache(self, mock_drive, mock_cx):
        """Verify makedirs clears folder cache after creation.
        """
        files = mock_cx.files.return_value
        sub_resp = files_list_response([folder_entry('sub', 'folder_sub')])
        files.list.return_value.execute.return_value = sub_resp
        mock_drive.clear_cache()
        mock_drive.makedirs('/TestDrive/sub')
        mock_drive._resolve_segment('root123', 'sub')
        assert files.list.return_value.execute.call_count >= 2


class TestListChildren:
    """Tests for _list_children() helper.
    """

    def test_list_children_returns_files_and_folders(self, mock_drive, mock_cx):
        """Verify _list_children returns both files and folders.
        """
        files = mock_cx.files.return_value
        children = [
            file_entry('doc.txt', 'file_doc', 'text/plain'),
            folder_entry('subdir', 'folder_sub'),
            ]
        files.list.return_value.execute.return_value = files_list_response(children)
        result = mock_drive._list_children('parent_id')
        assert len(result) == 2
        assert result[0]['name'] == 'doc.txt'
        assert result[1]['name'] == 'subdir'

    def test_list_children_paginates(self, mock_drive, mock_cx):
        """Verify _list_children handles pagination.
        """
        files = mock_cx.files.return_value
        page1 = files_list_response(
            [file_entry('a.txt', 'id_a', 'text/plain')],
            next_page_token='tok2')
        page2 = files_list_response(
            [file_entry('b.txt', 'id_b', 'text/plain')])
        files.list.return_value.execute.side_effect = [page1, page2]
        result = mock_drive._list_children('parent_id')
        assert len(result) == 2
        assert result[0]['name'] == 'a.txt'
        assert result[1]['name'] == 'b.txt'


class TestDeleteExtended:
    """Extended tests for delete() with folders and error handling.
    """

    def test_delete_folder_by_filepath(self, mock_drive, mock_cx):
        """Verify delete works for folders via filepath form.
        """
        files = mock_cx.files.return_value
        empty = files_list_response([])
        folder_resp = files_list_response(
            [folder_entry('old_dir', 'folder_old')])
        files.list.return_value.execute.side_effect = [empty, folder_resp]
        files.delete.return_value.execute.return_value = None
        mock_drive.delete('/TestDrive/old_dir')
        files.delete.assert_called_once()
        assert files.delete.call_args[1]['fileId'] == 'folder_old'

    def test_delete_not_found_raises_lookup_error(self, mock_drive, mock_cx):
        """Verify delete raises LookupError (not ValueError) when not found.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response(
            [folder_entry('docs', 'folder_docs')])
        empty = files_list_response([])
        files.list.return_value.execute.side_effect = [folder_resp, empty]
        with pytest.raises(LookupError, match='not found'):
            mock_drive.delete(folder='TestDrive/docs', filename='ghost.txt')


class TestDownloadExtended:
    """Extended tests for download() error handling.
    """

    def test_download_not_found_raises_lookup_error(self, mock_drive, mock_cx):
        """Verify download raises LookupError when file not found.
        """
        files = mock_cx.files.return_value
        folder_resp = files_list_response(
            [folder_entry('docs', 'folder_docs')])
        empty = files_list_response([])
        files.list.return_value.execute.side_effect = [folder_resp, empty]
        with pytest.raises(LookupError, match='not found'):
            mock_drive.download(folder='TestDrive/docs', filename='ghost.txt')


class TestMoveTree:
    """Tests for move_tree() method.
    """

    def test_move_tree_creates_dest_and_moves_children(self, mock_drive, mock_cx):
        """Verify move_tree creates destination folder and moves files.
        """
        files = mock_cx.files.return_value
        children = [
            file_entry('a.txt', 'file_a', 'text/plain'),
            file_entry('b.txt', 'file_b', 'text/plain'),
            ]
        with patch.object(mock_drive, 'makedirs', return_value='new_dest_id') as mk, \
        patch.object(mock_drive, '_list_children',
                     side_effect=[children, []]) as lc:
            files.update.return_value.execute.return_value = {'id': 'x', 'parents': ['new_dest_id']}
            files.delete.return_value.execute.return_value = None
            src_resolve = files_list_response(
                [folder_entry('ticker', 'folder_ticker')])
            files.list.return_value.execute.return_value = src_resolve
            mock_drive.move_tree('/TestDrive/ticker', '/TestDrive/dest')
        mk.assert_called_once_with('/TestDrive/dest/ticker')
        assert lc.call_count == 2
        assert files.update.return_value.execute.call_count == 2
        files.delete.assert_called_once()

    def test_move_tree_recursive_subfolders(self, mock_drive, mock_cx):
        """Verify move_tree recurses into child subfolders.
        """
        files = mock_cx.files.return_value
        children_with_subfolder = [
            file_entry('a.txt', 'file_a', 'text/plain'),
            folder_entry('nested', 'folder_nested'),
            ]
        nested_children = [
            file_entry('deep.txt', 'file_deep', 'text/plain'),
            ]
        makedirs_calls = []

        def track_makedirs(path):
            makedirs_calls.append(path)
            return f'dest_{path}'

        id_map = {'/TestDrive/parent': 'folder_parent',
                  '/TestDrive/parent/nested': 'folder_nested'}

        list_children_responses = [
            children_with_subfolder,
            nested_children,
            [],
            [],
            ]

        with patch.object(mock_drive, 'makedirs', side_effect=track_makedirs), \
        patch.object(mock_drive, '_list_children',
                     side_effect=list_children_responses), \
        patch.object(mock_drive, 'id', side_effect=lambda p: id_map[p]):
            files.update.return_value.execute.return_value = {'id': 'x', 'parents': ['y']}
            files.delete.return_value.execute.return_value = None
            mock_drive.move_tree('/TestDrive/parent', '/TestDrive/dest')
        assert '/TestDrive/dest/parent' in makedirs_calls
        assert '/TestDrive/dest/parent/nested' in makedirs_calls
        assert files.update.return_value.execute.call_count == 2

    def test_move_tree_deletes_empty_source(self, mock_drive, mock_cx):
        """Verify move_tree deletes source folder when empty after move.
        """
        files = mock_cx.files.return_value
        children = [file_entry('a.txt', 'file_a', 'text/plain')]
        with patch.object(mock_drive, 'makedirs', return_value='new_dest_id'), \
        patch.object(mock_drive, '_list_children',
                     side_effect=[children, []]):
            files.update.return_value.execute.return_value = {'id': 'x', 'parents': ['y']}
            files.delete.return_value.execute.return_value = None
            folder_resp = files_list_response(
                [folder_entry('ticker', 'folder_ticker')])
            files.list.return_value.execute.return_value = folder_resp
            mock_drive.move_tree('/TestDrive/ticker', '/TestDrive/dest')
        files.delete.assert_called_once()
        assert files.delete.call_args[1]['fileId'] == 'folder_ticker'

    def test_move_tree_preserves_source_on_remaining(self, mock_drive, mock_cx):
        """Verify move_tree does not delete source if children remain.
        """
        files = mock_cx.files.return_value
        children = [file_entry('a.txt', 'file_a', 'text/plain')]
        leftover = [file_entry('orphan.txt', 'file_orphan', 'text/plain')]
        with patch.object(mock_drive, 'makedirs', return_value='new_dest_id'), \
        patch.object(mock_drive, '_list_children',
                     side_effect=[children, leftover]):
            files.update.return_value.execute.return_value = {'id': 'x', 'parents': ['y']}
            folder_resp = files_list_response(
                [folder_entry('ticker', 'folder_ticker')])
            files.list.return_value.execute.return_value = folder_resp
            mock_drive.move_tree('/TestDrive/ticker', '/TestDrive/dest')
        files.delete.assert_not_called()

    def test_move_tree_raises_rate_limit_error(self, mock_drive, mock_cx):
        """Verify move_tree raises RateLimitError on sustained rate limit.
        """
        files = mock_cx.files.return_value
        children = [file_entry('a.txt', 'file_a', 'text/plain')]
        rate_limit_exc = http_error_from_fixture(
            'files_update_rate_limit_exceeded', 403)
        with patch.object(mock_drive, 'makedirs', return_value='new_dest_id'), \
        patch.object(mock_drive, '_list_children', return_value=children):
            files.update.return_value.execute.side_effect = rate_limit_exc
            folder_resp = files_list_response(
                [folder_entry('ticker', 'folder_ticker')])
            files.list.return_value.execute.return_value = folder_resp
            with pytest.raises(RateLimitError, match='Rate limit persisted'):
                mock_drive.move_tree('/TestDrive/ticker', '/TestDrive/dest')
