from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from phable import (
    NA,
    Coord,
    DateRange,
    DateTimeRange,
    Grid,
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
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}, {"name": "v1"}]
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
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}, {"name": "v1"}]
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
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}]
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
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}, {"name": "v1"}]
    assert grid.meta == {
        "ver": "3.0",
        "test_meta": "Hi!",
        "hisStart": TS_NOW - timedelta(minutes=5),
        "hisEnd": TS_NOW + timedelta(minutes=1),
    }


def test_number() -> None:
    # valid cases
    assert str(Number(10, "kW")) == "10kW"
    assert str(Number(-10)) == "-10"
    assert str(Number(-10.2, "kW")) == "-10.2kW"
    assert str(Number(10.2)) == "10.2"


def test_marker() -> None:
    assert str(Marker()) == "\u2713"


def test_remove() -> None:
    assert str(Remove()) == "remove"


def test_na() -> None:
    assert str(NA()) == "NA"


def test_ref() -> None:
    # valid cases
    assert str(Ref("foo")) == "@foo"
    assert str(Ref("foo", "bar")) == "bar"


def test_uri() -> None:
    # valid case
    assert Uri("basic_test")


def test_coord() -> None:
    # valid case
    lat = Decimal("37.548266")
    lng = Decimal("-77.4491888")
    assert str(Coord(lat, lng)) == f"C({lat}, {lng})"  # type: ignore

    lat = Decimal("98.230003023231")
    lng = Decimal("-21.450001023312")
    assert str(Coord(lat, lng)) == f"C({lat}, {lng})"  # type: ignore


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


def test_datetime_range_no_end() -> None:
    dt = datetime(2023, 8, 12, 10, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    datetime_range = DateTimeRange(dt)
    assert str(datetime_range) == dt.isoformat() + " New_York"

    # America/New_York
    dt1 = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))
    datetime_range = str(DateTimeRange(dt1))
    assert datetime_range == "2023-03-12T12:12:34-04:00 New_York"

    # Asia/Bangkok
    dt2 = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("Asia/Bangkok"))
    datetime_range = str(DateTimeRange(dt2))
    assert datetime_range == "2023-03-12T12:12:34+07:00 Bangkok"

    # UTC
    dt2 = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("UTC"))
    datetime_range = str(DateTimeRange(dt2))
    assert datetime_range == "2023-03-12T12:12:34+00:00 UTC"


def test_datetime_range() -> None:
    start = datetime(2023, 3, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))
    end = datetime(2023, 4, 12, 12, 12, 34, tzinfo=ZoneInfo("America/New_York"))

    datetime_range = DateTimeRange(start, end)
    assert str(datetime_range) == (
        "2023-03-12T12:12:34-04:00 New_York," "2023-04-12T12:12:34-04:00 New_York"
    )
