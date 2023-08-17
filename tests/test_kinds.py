from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from phable.kinds import (
    NA,
    Coordinate,
    Date,
    DateTime,
    DateSpan,
    DateTimeSpan,
    SpanTzMismatchError,
    Grid,
    Marker,
    Number,
    Ref,
    Remove,
    Symbol,
    Time,
    Uri,
    XStr,
)


# -----------------------------------------------------------------------------
# Haystack kind tests
# -----------------------------------------------------------------------------

def test_grid():
    # test #1
    rows = [
        {"ts": "some_time", "v0": "50kW"},
        {"ts": "some_time", "v0": "45kW", "v1": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}, {"name": "v1"}]

    # test #2
    rows = [
        {"ts": "some_time", "v0": "45kW", "v1": "50kW"},
        {"ts": "some_time", "v0": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}, {"name": "v1"}]

    # test #3
    rows = [
        {"ts": "some_time", "v0": "45kW"},
        {"ts": "some_time", "v0": "50kW"},
    ]
    grid = Grid.to_grid(rows)
    assert grid.cols == [{"name": "ts"}, {"name": "v0"}]

    # test display
    assert str(grid) == "Haystack Grid"


def test_number() -> None:
    # valid cases
    assert str(Number(10, "kW")) == "10kW"
    assert str(Number(-10)) == "-10"
    assert str(Number(-10.2, "kW")) == "-10.2kW"
    assert str(Number(10.2)) == "10.2"

    # invalid cases
    with pytest.raises(TypeError):
        Number("abc")  # type: ignore
    with pytest.raises(TypeError):
        Number(10, 10)  # type: ignore


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

    # invalid cases
    with pytest.raises(TypeError):
        Ref(1)  # type: ignore
    with pytest.raises(TypeError):
        Ref("foo", 1)  # type: ignore


def test_date() -> None:
    # valid case
    assert str(Date(date(2023, 1, 22))) == "2023-01-22"

    # invalid case
    with pytest.raises(TypeError):
        Date(1)  # type: ignore


def test_time() -> None:
    # valid cases
    assert str(Time(time(2, 0, 0))) == "02:00:00"
    assert str(Time(time(2, 0, 0, 12))) == "02:00:00.000012"
    assert str(Time(time(2, 0, 0, 12))) == "02:00:00.000012"

    # invalid case
    with pytest.raises(TypeError):
        Time(1)  # type: ignore


def test_datetime() -> None:
    # valid case
    assert (
        str(DateTime(datetime(2023, 1, 12, 1, 1, 36))) == "2023-01-12T01:01:36"
    )

    assert str(
        DateTime(datetime(2023, 1, 12, 1, 1, 36), "New_York")
        == "2023-01-12T01:01:36 New_York"
    )

    assert (
        str(
            DateTime(
                datetime(2023, 3, 3, 9, 40, 12, 121230).replace(
                    tzinfo=timezone.utc
                )
            )
        )
        == "2023-03-03T09:40:12.121+00:00"
    )

    assert (
        str(
            DateTime(
                datetime(2023, 3, 3, 9, 40, 12, 121230).replace(
                    tzinfo=timezone.utc
                ),
                "New_York",
            )
        )
        == "2023-03-03T09:40:12.121+00:00 New_York"
    )

    assert (
        str(DateTime(datetime(2023, 3, 3, 9, 40, 12, 121230), "New_York"))
        == "2023-03-03T09:40:12.121 New_York"
    )

    assert (
        str(
            DateTime(
                datetime(2023, 3, 3, 9, 40, 12, 121000).replace(
                    tzinfo=timezone.utc
                ),
                "New_York",
            )
        )
        == "2023-03-03T09:40:12.121+00:00 New_York"
    )

    # invalid case
    with pytest.raises(TypeError):
        DateTime(23)  # type: ignore


def test_uri() -> None:
    # valid case
    assert Uri("basic_test")

    # Note:  probably want to consider adding better validation support to Uri

    # invalid case
    with pytest.raises(TypeError):
        Uri(1)  # type: ignore


def test_coord() -> None:
    # valid case
    lat = Decimal("37.548266")
    lng = Decimal("-77.4491888")
    assert str(Coordinate(lat, lng)) == f"C({lat}, {lng})"  # type: ignore

    lat = Decimal("98.230003023231")
    lng = Decimal("-21.450001023312")
    assert str(Coordinate(lat, lng)) == f"C({lat}, {lng})"  # type: ignore


def test_xstr() -> None:
    # valid case
    assert str(XStr("a", "b")) == "(a, b)"

    # invalid case
    with pytest.raises(TypeError):
        XStr(1, "b")  # type: ignore

    with pytest.raises(TypeError):
        XStr("a", 2)  # type: ignore


def test_symbol() -> None:
    # valid case
    assert str(Symbol("a")) == "^a"

    # invalid case
    with pytest.raises(TypeError):
        Symbol(1)  # type: ignore


# -----------------------------------------------------------------------------
# tests for additional kinds not supported by Haystack
# -----------------------------------------------------------------------------

def test_date_span() -> None:
    # valid case
    x = Date(date(2023, 1, 22))
    y = Date(date(2023, 1, 25))

    assert str(DateSpan(x, y)) == "2023-01-22,2023-01-25"

    # invalid case #1
    with pytest.raises(TypeError):
        DateSpan(1, y)  # type: ignore

    # invalid case #2
    with pytest.raises(TypeError):
        DateSpan(x, 1)  # type: ignore


def test_datetime_span() -> None:
    # valid case
    x = DateTime(
                datetime(2023, 3, 3, 9, 40, 12, 121230).replace(
                    tzinfo=timezone.utc
                ),
                "New_York"
            )
    y = DateTime(
                datetime(2023, 3, 4, 9, 40, 12, 121230).replace(
                    tzinfo=timezone.utc
                ),
                "New_York"
            )

    assert (str(DateTimeSpan(x, y)) ==
            "2023-03-03T09:40:12.121+00:00 New_York,"
            "2023-03-04T09:40:12.121+00:00 New_York")

    # invalid case #1
    with pytest.raises(TypeError):
        DateTimeSpan(1, y)  # type: ignore

    # invalid case #2
    with pytest.raises(TypeError):
        DateTimeSpan(x, 1)  # type: ignore

    z = DateTime(
                datetime(2023, 3, 4, 9, 40, 12, 121230).replace(
                    tzinfo=timezone.utc
                ),
                "Los_Angeles"
            )

    # invalid case #3
    with pytest.raises(SpanTzMismatchError):
        DateTimeSpan(x, z)  # type: ignore