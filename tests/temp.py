from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from phable.client import Client
from phable.kinds import Number, Ref

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


ts_now = datetime.now(ZoneInfo("America/New_York"))
data_rows = [
    {
        "ts": ts_now - timedelta(seconds=30),
        "v0": Number(3_000.0, "kW"),
    },
    {
        "ts": ts_now,
        "v1": Number(4_000.0, "kW"),
    },
]

# Ref at index 0 of ids corresponds to data values in col v0
# Ref at index 1 of ids corresponds to data values in col v1
ids = [Ref("2d6a2714-0d0a79fb"), Ref("2d6a2714-503317c5")]

with Client(uri, username, password) as ph:
    ph.his_write_by_ids(ids, data_rows)
