"""Mock integration tests for Calendar module.
"""


class TestListCalendar:
    """Tests for list_calendar() method.
    """

    def test_returns_calendar_list(self, calendar, mock_cx):
        """Verify list_calendar calls calendarList.list and returns result.
        """
        cal_api = mock_cx.calendarList.return_value
        expected = {'kind': 'calendar#calendarList', 'items': [{'id': 'cal1'}]}
        cal_api.list.return_value.execute.return_value = expected
        result = calendar.list_calendar()
        assert result == expected
        cal_api.list.assert_called_once()


class TestListEvents:
    """Tests for list_events() method.
    """

    def test_passes_kwargs(self, calendar, mock_cx):
        """Verify list_events forwards kwargs to events.list.
        """
        events_api = mock_cx.events.return_value
        expected = {'kind': 'calendar#events', 'items': []}
        events_api.list.return_value.execute.return_value = expected
        result = calendar.list_events(calendarId='primary', maxResults=10)
        assert result == expected
        events_api.list.assert_called_once_with(calendarId='primary', maxResults=10)


class TestGetEvents:
    """Tests for get_events() method.
    """

    def test_returns_event(self, calendar, mock_cx):
        """Verify get_events calls events.get and returns result.
        """
        events_api = mock_cx.events.return_value
        expected = {'kind': 'calendar#event', 'id': 'evt1'}
        events_api.get.return_value.execute.return_value = expected
        result = calendar.get_events(calendarId='primary', eventId='evt1')
        assert result == expected
        events_api.get.assert_called_once_with(calendarId='primary', eventId='evt1')


class TestDeleteEvents:
    """Tests for delete_events() method.
    """

    def test_deletes_event(self, calendar, mock_cx):
        """Verify delete_events calls events.delete.
        """
        events_api = mock_cx.events.return_value
        events_api.delete.return_value.execute.return_value = ''
        result = calendar.delete_events(calendarId='primary', eventId='evt1')
        assert result == ''
        events_api.delete.assert_called_once_with(
            calendarId='primary', eventId='evt1')


class TestInsertEvents:
    """Tests for insert_events() method.
    """

    def test_inserts_event(self, calendar, mock_cx):
        """Verify insert_events calls events.insert and returns result.
        """
        events_api = mock_cx.events.return_value
        body = {'summary': 'Meeting', 'start': {}, 'end': {}}
        expected = {'kind': 'calendar#event', 'id': 'new_evt'}
        events_api.insert.return_value.execute.return_value = expected
        result = calendar.insert_events(calendarId='primary', body=body)
        assert result == expected
        events_api.insert.assert_called_once_with(calendarId='primary', body=body)
