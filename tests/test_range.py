from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from phable.parser.range import (
    HisReadRangeSliceError,
    to_haystack_date,
    to_haystack_datetime,
    to_haystack_range,
)


def test_to_haystack_range_validation():
    start1 = date.today() - timedelta(days=3)
    stop1 = datetime.now()
    with pytest.raises(HisReadRangeSliceError):
        to_haystack_range(slice(start1, stop1))

    start2 = date.today() - timedelta(days=3)
    stop2 = date.today()
    step2 = timedelta(days=1)
    with pytest.raises(HisReadRangeSliceError):
        to_haystack_range(slice(start2, stop2, step2))


def test_to_haystack_range_with_date():
    haystack_range = to_haystack_range(date.today())
    assert haystack_range == date.today().isoformat()


def test_to_haystack_range_with_datetime():
    dt = datetime(2023, 8, 12, 10, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    haystack_range = to_haystack_range(dt)
    assert haystack_range == dt.isoformat()


def test_to_haystack_range_with_date_slice():
    start = date.today() - timedelta(days=3)
    stop = date.today()

    haystack_range = to_haystack_range(slice(start, stop))
    assert haystack_range == start.isoformat() + "," + stop.isoformat()


def test_to_haystack_range_with_datetime_slice():
    start = datetime(
        2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")
    )
    stop = datetime(
        2023, 4, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")
    )

    haystack_range = to_haystack_range(slice(start, stop))
    assert haystack_range == (
        "2023-03-12T12:12:34-04:00 New_York,"
        "2023-04-12T12:12:34-04:00 New_York"
    )


def test_to_haystack_with_datetime():
    dt = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))
    haystack_dt = to_haystack_datetime(dt)
    assert haystack_dt == "2023-03-12T12:12:34-04:00 New_York"


def test_to_haystack_with_date():
    haystack_dt = to_haystack_date(date.today())
    assert haystack_dt == date.today().isoformat()
