from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from phable import (
    NA,
    Coord,
    DateRange,
    DateTimeRange,
    Grid,
    GridCol,
    Marker,
    Number,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
)

# -----------------------------------------------------------------------------
# Haystack kind tests
# -----------------------------------------------------------------------------

TS_NOW = datetime.now()


def test_grid():
    grid = Grid(meta={}, cols=[], rows=[])

    # test display
    assert str(grid) == "Haystack Grid"


def test_to_grid_without_meta():
    # test #1
    rows = [
        {"ts": TS_NOW - timedelta(minutes=5), "v0": "50kW"},
        {"ts": TS_NOW, "v0": "45kW", "v1": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [GridCol("ts"), GridCol("v0"), GridCol("v1")]
    assert grid.meta == {
        "ver": "3.0",
        "hisStart": TS_NOW - timedelta(minutes=5),
        "hisEnd": TS_NOW + timedelta(minutes=1),
    }

    # test #2
    rows = [
        {"ts": TS_NOW - timedelta(minutes=5), "v0": "45kW", "v1": "50kW"},
        {"ts": TS_NOW, "v0": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [GridCol("ts"), GridCol("v0"), GridCol("v1")]
    assert grid.meta == {
        "ver": "3.0",
        "hisStart": TS_NOW - timedelta(minutes=5),
        "hisEnd": TS_NOW + timedelta(minutes=1),
    }

    # test #3
    rows = [
        {"ts": TS_NOW - timedelta(minutes=5), "v0": "45kW"},
        {"ts": TS_NOW, "v0": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [GridCol("ts"), GridCol("v0")]
    assert grid.meta == {
        "ver": "3.0",
        "hisStart": TS_NOW - timedelta(minutes=5),
        "hisEnd": TS_NOW + timedelta(minutes=1),
    }


def test_to_grid_with_meta():
    # test #1
    meta = {"test_meta": "Hi!"}
    rows = [
        {"ts": TS_NOW - timedelta(minutes=5), "v0": "50kW"},
        {"ts": TS_NOW, "v0": "45kW", "v1": "50kW"},
    ]
    grid = Grid.to_grid(rows, meta)
    assert grid.cols == [GridCol("ts"), GridCol("v0"), GridCol("v1")]
    assert grid.meta == {
        "ver": "3.0",
        "test_meta": "Hi!",
        "hisStart": TS_NOW - timedelta(minutes=5),
        "hisEnd": TS_NOW + timedelta(minutes=1),
    }


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (Number(10, "kW"), "10kW"),
        (Number(-10), "-10"),
        (Number(-10.2, "kW"), "-10.2kW"),
        (Number(10.2), "10.2"),
    ],
)
def test_number(test_input: Number, expected: str) -> None:
    assert str(test_input) == expected


def test_marker() -> None:
    assert str(Marker()) == "\u2713"


def test_remove() -> None:
    assert str(Remove()) == "remove"


def test_na() -> None:
    assert str(NA()) == "NA"


@pytest.mark.parametrize(
    "test_input,expected", [(Ref("foo"), "@foo"), (Ref("foo", "bar"), "bar")]
)
def test_ref(test_input: Ref, expected: str) -> None:
    assert str(test_input) == expected


def test_uri() -> None:
    # valid case
    assert Uri("basic_test")


@pytest.mark.parametrize(
    "test_lat,test_lng,expected",
    [
        (Decimal("37.548266"), Decimal("-77.4491888"), "C(37.548266, -77.4491888)"),
        (
            Decimal("98.230003023231"),
            Decimal("-21.450001023312"),
            "C(98.230003023231, -21.450001023312)",
        ),
    ],
)
def test_coord(test_lat: Decimal, test_lng: Decimal, expected: str) -> None:
    assert str(Coord(test_lat, test_lng)) == expected


def test_xstr() -> None:
    # valid case
    assert str(XStr("a", "b")) == "(a, b)"


def test_symbol() -> None:
    # valid case
    assert str(Symbol("a")) == "^a"


def test_date_range() -> None:
    start = date.today() - timedelta(days=3)
    end = date.today()
    date_range = DateRange(start, end)

    assert str(date_range) == start.isoformat() + "," + end.isoformat()


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York")),
            "2023-03-12T12:12:34-04:00 New_York",
        ),
        (
            datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("Asia/Bangkok")),
            "2023-03-12T12:12:34+07:00 Bangkok",
        ),
        (
            datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("UTC")),
            "2023-03-12T12:12:34+00:00 UTC",
        ),
    ],
)
def test_datetime_range_no_end(test_input: datetime, expected: str) -> None:
    assert str(DateTimeRange(test_input)) == expected


def test_datetime_range() -> None:
    start = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))
    end = datetime(2023, 4, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))

    datetime_range = DateTimeRange(start, end)
    assert str(datetime_range) == (
        "2023-03-12T12:12:34-04:00 New_York,2023-04-12T12:12:34-04:00 New_York"
    )


def test_datetime_range_raises_error() -> None:
    tzinfo = ZoneInfo("America/New_York")

    start_with_tz = datetime(2024, 11, 22, 8, 19, 0, tzinfo=tzinfo)
    end_with_tz = datetime(2024, 11, 22, 9, 19, 0, tzinfo=tzinfo)

    start = datetime(2024, 11, 22, 8, 19, 0)
    end = datetime(2024, 11, 22, 9, 19, 0)

    with pytest.raises(ValueError):
        DateTimeRange(start, end)

    with pytest.raises(ValueError):
        DateTimeRange(start, end_with_tz)

    with pytest.raises(ValueError):
        DateTimeRange(start_with_tz, end)

    with pytest.raises(ValueError):
        DateTimeRange(start, None)
