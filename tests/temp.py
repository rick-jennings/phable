from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from phable.client import Client
from phable.kinds import Grid, Number, Ref

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


# define the data we want to write to the server
ts_now = datetime.now(ZoneInfo("America/New_York"))
meta = {"ver": "3.0", "id": Ref("2d6a2714-0d0a79fb")}
cols = [{"name": "ts"}, {"name": "val"}]
rows = [
    {
        "ts": ts_now - timedelta(seconds=30),
        "val": Number(1_000.0, "kW"),
    },
    {
        "ts": ts_now,
        "val": Number(2_000.0, "kW"),
    },
]

write_data = Grid(meta, cols, rows)


with Client(uri, username, password) as ph:
    ph.his_write(write_data)
