# goog

Google API utilities for Calendar, Gmail, Drive, and Sheets.

## Overview

This package provides simplified interfaces for working with Google APIs:
- **Calendar**: Manage calendar events
- **Gmail**: Search, read, and send emails
- **Drive**: Upload, download, and manage files
- **Sheets**: Read and write spreadsheet data

## Installation

```bash
poetry install
```

## Configuration

Configure the module with your Google account and credentials:

```python
from goog import configure

configure(
    account='your-email@domain.com',
    tmpdir='/path/to/temp',
    rootid={'SharedDrive': 'drive-id'},
    mail_from='sender@domain.com',
    app_configs={
        'calendar': {
            'key': '/path/to/credentials.json',
            'scopes': ['https://www.googleapis.com/auth/calendar'],
            'version': 'v3'
        },
        'gmail': {
            'key': '/path/to/credentials.json',
            'scopes': ['https://www.googleapis.com/auth/gmail.modify'],
            'version': 'v1'
        },
        'drive': {
            'key': '/path/to/credentials.json',
            'scopes': ['https://www.googleapis.com/auth/drive'],
            'version': 'v3'
        },
        'sheets': {
            'key': '/path/to/credentials.json',
            'scopes': ['https://www.googleapis.com/auth/spreadsheets']
        }
    }
)
```

## Usage Examples

### Gmail

```python
from goog import Gmail

gmail = Gmail()
for msg in gmail.get_emails(subject='Meeting'):
    print(msg['Subject'])

gmail.send_mail('recipient@example.com', 'Subject', 'Body text')
```

### Drive

```python
from goog import Drive

drive = Drive()
drive.write('local_file.txt', 'remote_file.txt', 'SharedDrive/folder')
drive.download('SharedDrive/folder/file.txt', '/local/directory')
```

### Sheets

```python
from goog import Sheets

sheets = Sheets()
data = sheets.get_iterdict('SharedDrive/folder/spreadsheet.xlsx')
```

## Dependencies

- google-api-python-client
- google-auth
- gspread
- filetype
- tqdm
- mail (custom package)
- libb (custom package)
