from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from phable.kinds import Grid, Number, Ref

foo1 = Ref("1234", "foo1 kW")
foo2 = Ref("2345", "foo2 kW")

ts_now = datetime.now(ZoneInfo("America/New_York"))
meta = {"ver": "3.0"}
cols = [
    {"name": "ts"},
    {"name": "v0", "meta": {"id": foo1}},
    {"name": "v1", "meta": {"id": foo2}},
]
rows = [
    {"ts": ts_now - timedelta(seconds=30), "v0": 72.2},
    {"ts": ts_now, "v1": 123.221},
    {
        "ts": ts_now - timedelta(seconds=30),
        "v0": 213.213,
        "v1": Number(122),
    },
    {},
    {},
]

his_df = Grid(meta, cols, rows).to_pandas()

print(his_df)
