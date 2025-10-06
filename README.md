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

The Drive API uses **path-based file access** where you specify files using Unix-style paths starting from a configured shared drive.

#### Key Concepts

**Shared Drive Configuration**

Files are accessed via paths like `'SharedDrive/folder/file.txt'`. The topmost folder name must match a key in your `rootid` configuration:

```python
configure(
    rootid={
        'SharedDrive': '0ABcDeFgHiJkLmNoPqRsTuVwXyZ',
        'Marketing': '1XyZaBcDeFgHiJkLmNoPqRsTuVw',
        'Engineering': '2PqRsTuVwXyZaBcDeFgHiJkLmNo'
    }
)
```

The `rootid` is a dictionary mapping friendly names to Google Drive IDs (long alphanumeric strings).

**How to Find a Drive ID:**
1. **From URL**: Open the shared drive in Google Drive. The URL will be `https://drive.google.com/drive/folders/0ABcDeFgHiJkLmNoPqRsTuVwXyZ` - the ID is after `folders/`
2. **From API**: Use `drive.cx.drives().list()` to list all shared drives and their IDs

**Path Resolution**

The system resolves paths hierarchically by querying each level by name:
- `'SharedDrive'` → looks up root ID from `rootid` config
- `'SharedDrive/folder'` → queries for folder named "folder" in the shared drive
- `'SharedDrive/folder/file.txt'` → queries for file named "file.txt" in that folder

#### Common Operations

```python
from goog import Drive

drive = Drive()

# Download a file
local_path = drive.download('SharedDrive/Reports/report.xlsx', '/tmp')

# Upload a local file
drive.write('local_file.pdf', 'document.pdf', 'SharedDrive/Documents')

# Upload from bytes/stream
drive.write(file_bytes, 'output.csv', 'SharedDrive/Data')

# Check if file exists
if drive.exists('SharedDrive/Archive/old_file.txt'):
    print("File exists")

# List files (non-recursive)
for filepath in drive.walk('SharedDrive/Projects'):
    print(filepath)

# List files recursively
for filepath in drive.walk('SharedDrive/Projects', recursive=True):
    print(filepath)

# Move a file
drive.move('SharedDrive/Inbox/file.pdf', 'SharedDrive/Archive')

# Delete a file
drive.delete('SharedDrive/Temp/old_file.txt')
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
