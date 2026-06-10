"""Mock integration tests for Gmail module.
"""
import base64
from unittest.mock import MagicMock

import pytest
from goog.gmail import PRECOND_RETRIES


def _precondition_error():
    """Build a 400 failedPrecondition HttpError like Gmail returns for format=raw.
    """
    from googleapiclient.errors import HttpError
    resp = MagicMock()
    resp.status = 400
    content = b'{"error": {"code": 400, "message": "Precondition check failed.", "errors": [{"reason": "failedPrecondition"}]}}'
    return HttpError(resp, content)


class TestBuildKw:
    """Tests for _build_kw() query construction.
    """

    @pytest.mark.parametrize(('kwargs', 'expected_q'), [
        ({'subject': 'test', 'to': 'x@y.com'}, 'subject:test to:x@y.com'),
        ({'q': 'from:boss label:inbox'}, 'from:boss label:inbox'),
        ({'query': 'is:unread'}, 'is:unread'),
        ({'subject': 'report'}, 'subject:report'),
    ])
    def test_build_kw(self, gmail, kwargs, expected_q):
        """Verify query construction from various input forms.
        """
        result = gmail._build_kw(**kwargs)
        assert result == {'q': expected_q}


class TestGetEmails:
    """Tests for get_emails() generator.
    """

    def test_yields_messages(self, gmail, mock_cx):
        """Verify get_emails yields parsed email.message.Message objects.
        """
        messages = mock_cx.users.return_value.messages.return_value
        raw_msg = b'From: a@b.com\r\nTo: c@d.com\r\nSubject: Test\r\n\r\nBody'
        encoded = base64.urlsafe_b64encode(raw_msg).decode('ascii')
        list_resp = {
            'resultSizeEstimate': 1,
            'messages': [{'id': 'msg_1'}],
        }
        get_resp = {'raw': encoded, 'snippet': 'Body'}
        messages.list.return_value.execute.return_value = list_resp
        messages.get.return_value.execute.return_value = get_resp
        results = list(gmail.get_emails(q='subject:Test'))
        assert len(results) == 1
        assert results[0]['Subject'] == 'Test'

    def test_pagination(self, gmail, mock_cx):
        """Verify get_emails follows nextPageToken.
        """
        messages = mock_cx.users.return_value.messages.return_value
        raw_msg = b'From: a@b.com\r\nSubject: X\r\n\r\n'
        encoded = base64.urlsafe_b64encode(raw_msg).decode('ascii')
        page1 = {
            'resultSizeEstimate': 2,
            'messages': [{'id': 'msg_1'}],
            'nextPageToken': 'tok2',
        }
        page2 = {
            'resultSizeEstimate': 2,
            'messages': [{'id': 'msg_2'}],
        }
        messages.list.return_value.execute.side_effect = [page1, page2]
        messages.get.return_value.execute.return_value = {
            'raw': encoded, 'snippet': '',
        }
        results = list(gmail.get_emails(q='subject:X'))
        assert len(results) == 2

    def test_retries_failed_precondition_then_succeeds(self, gmail, mock_cx, monkeypatch):
        """Verify a transient failedPrecondition is retried until the fetch succeeds.
        """
        monkeypatch.setattr('time.sleep', lambda *a: None)
        messages = mock_cx.users.return_value.messages.return_value
        raw_msg = b'From: a@b.com\r\nSubject: Late\r\n\r\nBody'
        encoded = base64.urlsafe_b64encode(raw_msg).decode('ascii')
        messages.list.return_value.execute.return_value = {
            'resultSizeEstimate': 1,
            'messages': [{'id': 'msg_1'}],
        }
        messages.get.return_value.execute.side_effect = [
            _precondition_error(),
            {'raw': encoded, 'snippet': 'Body'},
            ]
        results = list(gmail.get_emails(q='subject:Late'))
        assert len(results) == 1
        assert results[0]['Subject'] == 'Late'
        assert messages.get.return_value.execute.call_count == 2

    def test_persistent_failed_precondition_drops_only_that_message(self, gmail, mock_cx, monkeypatch):
        """Verify a message failing every retry is skipped without blocking the rest of the batch.
        """
        monkeypatch.setattr('time.sleep', lambda *a: None)
        messages = mock_cx.users.return_value.messages.return_value
        raw_msg = b'From: a@b.com\r\nSubject: Good\r\n\r\nBody'
        encoded = base64.urlsafe_b64encode(raw_msg).decode('ascii')
        messages.list.return_value.execute.return_value = {
            'resultSizeEstimate': 2,
            'messages': [{'id': 'bad'}, {'id': 'good'}],
        }
        messages.get.return_value.execute.side_effect = (
            [_precondition_error()] * (PRECOND_RETRIES + 1)
            + [{'raw': encoded, 'snippet': 'Body'}])
        results = list(gmail.get_emails(q='subject:Good'))
        assert len(results) == 1
        assert results[0]['Subject'] == 'Good'

    def test_non_precondition_error_not_retried(self, gmail, mock_cx, monkeypatch):
        """Verify a non-precondition HttpError is dropped without retrying.
        """
        from googleapiclient.errors import HttpError
        monkeypatch.setattr('time.sleep', lambda *a: None)
        messages = mock_cx.users.return_value.messages.return_value
        messages.list.return_value.execute.return_value = {
            'resultSizeEstimate': 1,
            'messages': [{'id': 'msg_1'}],
        }
        resp = MagicMock()
        resp.status = 404
        messages.get.return_value.execute.side_effect = HttpError(
            resp, b'{"error": {"code": 404, "message": "Not Found"}}')
        results = list(gmail.get_emails(q='subject:x'))
        assert results == []
        assert messages.get.return_value.execute.call_count == 1


class TestMarkAs:
    """Tests for mark_as() label modification.
    """

    @pytest.mark.parametrize(('add', 'expected_key'), [
        (True, 'addLabelIds'),
        (False, 'removeLabelIds'),
    ])
    def test_mark_as_label_direction(self, gmail, mock_cx, add, expected_key):
        """Verify mark_as uses correct label key based on add flag.
        """
        messages = mock_cx.users.return_value.messages.return_value
        list_resp = {
            'resultSizeEstimate': 1,
            'messages': [{'id': 'msg_1'}],
        }
        messages.list.return_value.execute.return_value = list_resp
        messages.modify.return_value.execute.return_value = {}
        gmail.mark_as('UNREAD', add=add, q='subject:test')
        messages.modify.assert_called_once()
        call_kwargs = messages.modify.call_args[1]
        assert expected_key in call_kwargs['body']
        assert call_kwargs['body'][expected_key] == ['UNREAD']


class TestSendMail:
    """Tests for send_mail() MIME construction.
    """

    def test_simple_send(self, gmail, mock_cx):
        """Verify send_mail builds correct MIME and calls API.
        """
        messages = mock_cx.users.return_value.messages.return_value
        send_resp = {'id': 'sent_1'}
        messages.send.return_value.execute.return_value = send_resp
        result = gmail.send_mail(
            recipients='to@example.com',
            subject='Hello',
            body='World',
            sender='from@example.com')
        assert result['id'] == 'sent_1'
        messages.send.assert_called_once()
        call_kwargs = messages.send.call_args[1]
        assert call_kwargs['userId'] == 'test@example.com'
        assert 'raw' in call_kwargs['body']

    def test_missing_sender_no_config_raises(self, gmail):
        """Verify missing sender without config raises ValueError.
        """
        import goog.base
        original = goog.base._settings.copy()
        goog.base._settings.clear()
        try:
            with pytest.raises(ValueError, match='sender required'):
                gmail.send_mail('to@example.com', 'Subject', 'Body')
        finally:
            goog.base._settings.update(original)

    def test_multiple_recipients(self, gmail, mock_cx):
        """Verify multiple recipients are joined.
        """
        messages = mock_cx.users.return_value.messages.return_value
        send_resp = {'id': 'sent_2'}
        messages.send.return_value.execute.return_value = send_resp
        gmail.send_mail(
            recipients=['a@b.com', 'c@d.com'],
            subject='Multi',
            body='Test',
            sender='from@example.com')
        messages.send.assert_called_once()
