import json
import logging
from datetime import date, datetime, time, timezone

import pandas as pd

from phable.kinds import (NA, Coordinate, Date, DateTime, Grid, Marker, Number,
                          Ref, Remove, Symbol, Time, Uri, XStr)
from phable.parser.json import _parse_kinds

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Test Haystack Grid using data received from a JSON file that describes history
# of multiple points at once.
#
# Note:  This is not formally supported by Project Haystack.  Project Haystack
# formally supports hisRead of a single point.  (See example in Test #1)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


def test_kinds_human_display():
    assert str(Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[])) == "Grid"
    assert str(Number(90.5, "kW")) == "90.5kW"
    assert str(Marker()) == "\u2713"
    assert str(Remove()) == "remove"
    assert str(NA()) == "NA"
    assert str(Ref("@foo", "Carytown")) == "Carytown"
    assert str(Date(date(2021, 1, 4))) == "2021-01-04"
    assert str(Time(time(hour=14, minute=59, second=0))) == "14:59:00"

    test_dt = datetime(2022, 6, 4, 0, 15, 20, tzinfo=timezone.utc)
    assert str(DateTime(test_dt)) == "2022-06-04T00:15:20+00:00"

    assert str(Uri("http://www.localhost:8080")) == "http://www.localhost:8080"

    assert str(Coordinate(39.154824, -77.209002)) == "C(39.154824, -77.209002)"

    assert str(XStr("Color", "red")) == "(Color, red)"
    assert str(Symbol("elec-meter")) == "^elec-meter"


def test_misc():
    with open("tests/json_test_data1.json") as f:
        ts_test = json.load(f)

    ts_test = _parse_kinds(ts_test)

    meta = ts_test["meta"]
    cols = ts_test["cols"]
    rows = ts_test["rows"]

    hg = Grid(meta, cols, rows)

    df = pd.DataFrame(data=hg.rows).rename(columns=hg.col_rename_map)

    # test first timestamp pandas df
    assert df[df.columns[0]].iloc[0].val == datetime.fromisoformat(
        "2023-03-13T23:45:00-04:00"
    )
    assert df["p:demo:r:2bae2387-d7707510"].iloc[0].val == 94.0
    assert df["p:demo:r:2bae2387-974f9223"].iloc[0].val == 206.0
    assert "Timestamp" in df.columns

    # test first timestamp pandas df
    assert df["Timestamp"].iloc[0].val == datetime.fromisoformat(
        "2023-03-13T23:45:00-04:00"
    )
    assert df["p:demo:r:2bae2387-d7707510"].iloc[0].val == 94.0
    assert df["p:demo:r:2bae2387-974f9223"].iloc[0].val == 206.0


# def test_ts_x():
#     with open("test_data/test1.json") as f:
#         ts_x = json.load(f)

#     ts_x = parse_kinds(ts_x)

#     meta = ts_x["meta"]
#     cols_by_name: dict[str, GridCol] = {}
#     for i, col in enumerate(ts_x["cols"]):
#         cols_by_name[str(col["name"])] = GridCol(i, col["name"], col["meta"])

#     rows: list[GridRow] = []
#     for row in ts_x["rows"]:
#         cells = [row[col_name] for col_name in cols_by_name.keys()]
#         rows.append(GridRow(cells))

#     hg = Grid(meta, cols_by_name, rows)

#     # let's run some tests now!

#     # --------------------------------------------------------------------------
#     # test vals @ first ts using col indices
#     # --------------------------------------------------------------------------

#     assert hg.rows[0].val(hg.cols_by_name["ts"]) == DateTime(
#         datetime.fromisoformat("2023-03-13T23:45:00-04:00"), "New_York"
#     )

#     assert hg.rows[0].val(hg.cols_by_name["v0"]) == Number(94, "kW")
#     assert hg.rows[0].val(hg.cols_by_name["v1"]) == Number(206, "kW")

#     # verify extra cols were not added
#     assert len(hg.col_names) == len(ts_x["cols"])

#     # --------------------------------------------------------------------------
#     # test vals @ last ts using col names
#     # --------------------------------------------------------------------------

#     assert hg.rows[-1].val(hg.cols_by_name["ts"]) == DateTime(
#         datetime.fromisoformat("2023-03-14T01:15:00-04:00"), "New_York"
#     )

#     assert hg.rows[-1].val(hg.cols_by_name["v0"]) == Number(87, "kW")

#     assert hg.rows[-1].val(hg.cols_by_name["v1"]) == Number(192, "kW")

#     # verify extra rows were not added
#     assert len(hg.rows) == len(ts_x["rows"])

#     # --------------------------------------------------------------------------
#     # misc Grid meta tests here
#     # --------------------------------------------------------------------------
#     assert hg.meta["ver"] == "3.0"

#     # --------------------------------------------------------------------------
#     # misc GridCol meta tests here
#     # --------------------------------------------------------------------------
#     assert hg.cols_by_name["v0"].meta["ac"] == Marker()  # {"_kind": "marker"}
