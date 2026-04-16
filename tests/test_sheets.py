"""Mock integration tests for Sheets module.
"""
from unittest.mock import MagicMock

import pytest


class TestGetIterdict:
    """Tests for get_iterdict() data extraction.
    """

    def test_basic_extraction(self, mock_sheets):
        """Verify get_iterdict returns formatted dicts from sheet data.
        """
        sheets, mock_gx, mock_dx = mock_sheets

        mock_worksheet = MagicMock()
        mock_worksheet.title = 'Sheet1'
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.title = 'TestBook'
        mock_spreadsheet.worksheets.return_value = [mock_worksheet]
        mock_gx.open_by_key.return_value = mock_spreadsheet
        mock_dx.id.return_value = 'sheet_id_1'

        mock_worksheet.get.side_effect = [
            [['Name', 'Value', 'Pct']],
            [['Alice', '1,234', '45.6%'], ['Bob', '(500)', '10%']],
        ]
        mock_worksheet.row_count = 10

        result = sheets.get_iterdict('/TestDrive/TestBook')
        assert len(result) == 2
        assert result[0] == {'Name': 'Alice', 'Value': 1234, 'Pct': 45.6}
        assert result[1] == {'Name': 'Bob', 'Value': -500, 'Pct': 10}

    def test_empty_sheet(self, mock_sheets):
        """Verify get_iterdict handles empty data.
        """
        sheets, mock_gx, mock_dx = mock_sheets

        mock_worksheet = MagicMock()
        mock_worksheet.title = 'Sheet1'
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.title = 'TestBook'
        mock_spreadsheet.worksheets.return_value = [mock_worksheet]
        mock_gx.open_by_key.return_value = mock_spreadsheet
        mock_dx.id.return_value = 'sheet_id_1'

        mock_worksheet.get.side_effect = [
            [['Name', 'Value']],
            [],
        ]
        mock_worksheet.row_count = 10

        result = sheets.get_iterdict('/TestDrive/TestBook')
        assert result == []

    def test_sheetname_filter(self, mock_sheets):
        """Verify get_iterdict filters to named sheet.
        """
        sheets, mock_gx, mock_dx = mock_sheets

        sheet1 = MagicMock()
        sheet1.title = 'Summary'
        sheet2 = MagicMock()
        sheet2.title = 'Detail'
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.title = 'TestBook'
        mock_spreadsheet.worksheets.return_value = [sheet1, sheet2]
        mock_gx.open_by_key.return_value = mock_spreadsheet
        mock_dx.id.return_value = 'sheet_id_1'

        sheet2.get.side_effect = [
            [['Col1']],
            [['data']],
        ]
        sheet2.row_count = 10

        result = sheets.get_iterdict('/TestDrive/TestBook', sheetname='Detail')
        assert len(result) == 1
        assert result[0] == {'Col1': 'data'}

    def test_missing_sheetname_returns_empty(self, mock_sheets):
        """Verify get_iterdict returns empty list for missing sheet name.
        """
        sheets, mock_gx, mock_dx = mock_sheets

        sheet1 = MagicMock()
        sheet1.title = 'Summary'
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.title = 'TestBook'
        mock_spreadsheet.worksheets.return_value = [sheet1]
        mock_gx.open_by_key.return_value = mock_spreadsheet
        mock_dx.id.return_value = 'sheet_id_1'

        result = sheets.get_iterdict('/TestDrive/TestBook', sheetname='NoSuchSheet')
        assert result == []


class TestIdCaching:
    """Tests for Sheets.id() caching.
    """

    def test_id_cached(self, mock_sheets):
        """Verify id() caches results from Drive.id().
        """
        sheets, mock_gx, mock_dx = mock_sheets
        mock_dx.id.return_value = 'resolved_id'
        r1 = sheets.id('/TestDrive/Sheet1')
        r2 = sheets.id('/TestDrive/Sheet1')
        assert r1 == r2 == 'resolved_id'
        assert mock_dx.id.call_count == 1


class TestCreate:
    """Tests for create() method.
    """

    def test_create_new(self, mock_sheets):
        """Verify create calls gspread.create with folder_id.
        """
        sheets, mock_gx, mock_dx = mock_sheets
        mock_dx.exists.return_value = False
        mock_dx._resolve_folderid.return_value = 'folder_abc'
        sheets.create('/TestDrive/NewSheet')
        mock_gx.create.assert_called_once_with('NewSheet', folder_id='folder_abc')

    def test_create_overwrite(self, mock_sheets):
        """Verify create with overwrite deletes then creates.
        """
        sheets, mock_gx, mock_dx = mock_sheets
        mock_dx.exists.return_value = True
        mock_dx.id.return_value = 'existing_id'
        mock_dx._resolve_folderid.return_value = 'folder_abc'
        sheets.create('/TestDrive/Existing', overwrite=True)
        mock_gx.del_spreadsheet.assert_called_once()
        mock_gx.create.assert_called_once()

    def test_create_exists_no_overwrite_raises(self, mock_sheets):
        """Verify create without overwrite raises when file exists.
        """
        sheets, mock_gx, mock_dx = mock_sheets
        mock_dx.exists.return_value = True
        with pytest.raises(ValueError, match='exists'):
            sheets.create('/TestDrive/Existing', overwrite=False)
