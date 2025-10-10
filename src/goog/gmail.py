"""Gmail API client for email operations.
"""
import base64
import email
import logging
from collections.abc import Generator
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import filetype
from apiclient import errors
from goog import Context, get_settings

import mail

logger = logging.getLogger(__name__)


class Gmail(Context, mail.MailClient):
    """Gmail API client for email operations.
    """

    def __init__(self, account: str | None = None, key: str | None = None, scopes:
                 list[str] | None = None, version: str | None = None) -> None:
        super().__init__(app='gmail', account=account, key=key,
                         scopes=scopes, version=version)

    def _build_kw(self, **kw) -> dict[str, Any]:
        """Build Gmail search query from keyword arguments.
        """
        q = kw.get('q') or kw.get('query')
        if not q:
            kw = {'q': ' '.join([f'{k}:{v}' for k, v in list(kw.items())])}
        return kw

    def get_profile(self) -> dict[str, Any]:
        """Get Gmail profile information.
        """
        return self.cx.users().getProfile(userId=self.account).execute()

    def list_emails(self, **kw) -> dict[str, Any]:
        """Get email search result container, but don't pull actual emails.
        """
        token = kw.pop('token', None) or kw.pop('pageToken', None)
        kw = self._build_kw(**kw)
        if token:
            kw['pageToken'] = token
        logger.info(f"Searching Gmail API for {self.account} {kw['q']}")
        res = self.cx.users().messages().list(userId=self.account, **kw).execute()
        logger.info(f"Total matched emails estimate: {res['resultSizeEstimate']}")
        return res

    def get_emails(self, **kw) -> Generator[email.message.Message, None, None]:
        """Generate emails matching search criteria.
        """
        kw = self._build_kw(**kw)
        res = self.list_emails(**kw)
        while messages := res.get('messages'):
            for row in messages:
                try:
                    param = {'userId': self.account, 'id': row['id'], 'format': 'raw'}
                    data = self.cx.users().messages().get(**param).execute()
                except errors.HttpError as exc:
                    logger.error(f"API error fetching message {row['id']}: {exc}")
                    continue
                logger.info(data['snippet'].encode('ascii', errors='ignore').decode(errors='ignore'))
                raw = base64.urlsafe_b64decode(data['raw'].encode('ascii'))
                message = email.message_from_bytes(raw)
                logger.info(f"Returning email [{message['From']}]: {message['Subject']}")
                yield message
            token = res.get('nextPageToken')
            if not token:
                break
            kw.update({'token': token})
            res = self.list_emails(**kw)
        logger.info('No more emails - exiting')

    def mark_as(self, label: str, add: bool = False, **kw) -> None:
        """Mark emails matching the search criteria with a Gmail label.
        """
        kw = self._build_kw(**kw)
        res = self.list_emails(**kw)

        label_key = 'addLabelIds' if add else 'removeLabelIds'
        action = 'added' if add else 'removed'

        while messages := res.get('messages'):
            for row in messages:
                try:
                    param = {'userId': self.account, 'id': row['id'], 'body': {label_key: [label]}}
                    self.cx.users().messages().modify(**param).execute()
                    logger.info(f"{action.capitalize()} {label} label for message {row['id']}")
                except errors.HttpError as exc:
                    logger.error(f"Failed to modify {label} label for message {row['id']}: {exc}")
                    continue
            token = res.get('nextPageToken')
            if not token:
                break
            kw.update({'token': token})
            res = self.list_emails(**kw)
        logger.info(f'Finished marking emails: {action} {label} label')

    def send_mail(self, recipients: str | list[str], subject: str, body: str,
                  sender: str | None = None, attachments: list[str] | None = None) -> dict[str, Any]:
        """Send email via Gmail API (blocking, not asynchronous).
        """
        if sender is None:
            settings = get_settings()
            sender = settings.get('mail_from')
            if sender is None:
                raise ValueError('sender required when not configured')

        if not isinstance(recipients, tuple | list):
            recipients = [recipients]
        recipients = self._resolve_recipients(recipients)
        to = ','.join(recipients)

        if attachments is None:
            attachments = []

        if not attachments:
            msg = MIMEText(body)
        else:
            msg = MIMEMultipart()
            msg.attach(MIMEText(body))
            for file in attachments:
                kind = filetype.guess(file)
                content_type = kind.mime or 'application/octet-stream'
                main_type, sub_type = content_type.split('/', 1)
                with Path(file).open('rb') as fp:
                    if main_type == 'text':
                        part = MIMEText(fp.read(), _subtype=sub_type)
                    elif main_type == 'image':
                        part = MIMEImage(fp.read(), _subtype=sub_type)
                    elif main_type == 'audio':
                        part = MIMEAudio(fp.read(), _subtype=sub_type)
                    else:
                        part = MIMEBase(main_type, sub_type)
                        part.set_payload(fp.read())
                filename = Path(file).name
                part.add_header('Content-Disposition', 'attachment', filename=filename)
                msg.attach(part)

        msg['to'] = to
        msg['from'] = sender
        msg['subject'] = subject

        try:
            body_dict = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode()}
            res = self.cx.users().messages().send(userId=self.account, body=body_dict).execute()
            logger.info(f"Message Id: {res['id']}")
            return res
        except errors.HttpError as err:
            logger.error(f'Failed to send email: {err}')
            raise
