from datetime import date, datetime, time

from phable.grid_builder import GridBuilder
from phable.kinds import Grid


def test_gb_his_items_zero_val_cols():
    gb = GridBuilder()
    gb.add_col("ts")
    gb.add_row({})

    assert gb.to_grid() == Grid({"ver": "3.0"}, [{"name": "ts"}], [{}])


def test_gb_his_items_one_val_cols():
    gb = GridBuilder()
    gb.add_col("ts").add_col("v0")
    gb.add_row({"ts": datetime.combine(date(2016, 7, 11), time(0, 5)), "v0": 5})
    gb.add_row({"ts": datetime.combine(date(2016, 7, 11), time(0, 10)), "v0": 10})
    gb.add_row({"ts": datetime.combine(date(2016, 7, 11), time(0, 15)), "v0": 15})
    gb.add_row({"ts": datetime.combine(date(2016, 7, 11), time(0, 20)), "v0": 20})

    assert gb.to_grid() == Grid(
        {"ver": "3.0"},
        [{"name": "ts"}, {"name": "v0"}],
        [
            {"ts": datetime.combine(date(2016, 7, 11), time(0, 5)), "v0": 5},
            {"ts": datetime.combine(date(2016, 7, 11), time(0, 10)), "v0": 10},
            {"ts": datetime.combine(date(2016, 7, 11), time(0, 15)), "v0": 15},
            {"ts": datetime.combine(date(2016, 7, 11), time(0, 20)), "v0": 20},
        ],
    )
