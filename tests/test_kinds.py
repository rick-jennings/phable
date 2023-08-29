from decimal import Decimal

from phable.kinds import (
    NA,
    Coord,
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
