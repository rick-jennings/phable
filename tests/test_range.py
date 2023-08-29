from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from phable.parser.range import (
    HisReadStartEndTypeError,
    to_haystack_date,
    to_haystack_datetime,
    to_haystack_range,
)


def test_to_haystack_range_validation():
    start1 = date.today() - timedelta(days=3)
    stop1 = datetime.now()
    with pytest.raises(HisReadStartEndTypeError):
        to_haystack_range(start1, stop1)


def test_to_haystack_range_with_date():
    haystack_range = to_haystack_range(date.today())
    assert haystack_range == date.today().isoformat()


def test_to_haystack_range_with_datetime():
    dt = datetime(2023, 8, 12, 10, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    haystack_range = to_haystack_range(dt)
    assert haystack_range == dt.isoformat() + " New_York"


def test_to_haystack_range_with_date_slice():
    start = date.today() - timedelta(days=3)
    end = date.today()

    haystack_range = to_haystack_range(start, end)
    assert haystack_range == start.isoformat() + "," + end.isoformat()


def test_to_haystack_range_with_datetime_slice():
    start = datetime(
        2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")
    )
    end = datetime(
        2023, 4, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")
    )

    haystack_range = to_haystack_range(start, end)
    assert haystack_range == (
        "2023-03-12T12:12:34-04:00 New_York,"
        "2023-04-12T12:12:34-04:00 New_York"
    )


def test_to_haystack_with_datetime():
    # America/New_York
    dt1 = datetime(
        2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")
    )
    haystack_dt = to_haystack_datetime(dt1)
    assert haystack_dt == "2023-03-12T12:12:34-04:00 New_York"

    # Asia/Bangkok
    dt2 = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("Asia/Bangkok"))
    haystack_dt = to_haystack_datetime(dt2)
    assert haystack_dt == "2023-03-12T12:12:34+07:00 Bangkok"

    # UTC
    dt2 = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("UTC"))
    haystack_dt = to_haystack_datetime(dt2)
    assert haystack_dt == "2023-03-12T12:12:34+00:00 UTC"


def test_to_haystack_with_date():
    haystack_dt = to_haystack_date(date.today())
    assert haystack_dt == date.today().isoformat()
