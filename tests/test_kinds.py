import json
import logging
from datetime import datetime

from phable.kinds import Grid
from phable.parser.json import grid_to_pandas, parse_kinds

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


def test_misc():
    with open("tests/json_test_data1.json") as f:
        ts_test = json.load(f)

    ts_test = parse_kinds(ts_test)

    meta = ts_test["meta"]
    cols = ts_test["cols"]
    rows = ts_test["rows"]

    hg = Grid(meta, cols, rows)

    df = grid_to_pandas(hg)

    # test first timestamp pandas df
    assert df[df.columns[0]].iloc[0] == datetime.fromisoformat(
        "2023-03-13T23:45:00-04:00"
    )
    # assert df[df.columns[1]].iloc[0] == 94.0
    assert df["Jacksonville ElecMeter-Main kW"].iloc[0] == 94.0
    assert df["Cleveland ElecMeter-Main kW"].iloc[0] == 206.0
    assert "Timestamp" in df.columns
    assert "Jacksonville ElecMeter-Main kW" in df.columns
    assert "Cleveland ElecMeter-Main kW" in df.columns

    # test first timestamp pandas df
    assert df["Timestamp"].iloc[0] == datetime.fromisoformat(
        "2023-03-13T23:45:00-04:00"
    )
    assert df["Jacksonville ElecMeter-Main kW"].iloc[0] == 94.0
    assert df["Cleveland ElecMeter-Main kW"].iloc[0] == 206.0


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
