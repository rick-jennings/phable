About
-----
Phable is a simple, modern Python toolkit for basic client connectivity to a [Project Haystack](https://project-haystack.org/) server.

We aimed to make code within Phable's client.py and kinds.py modules self-documenting by using type hints and docstrings and carefully selecting what is publicly exposed.  In the future we plan to improve docs and release a stable version 1.0.0 for Phable.

Installation
------------
Phable requires Python version 3.11 or higher and generally has no required software dependencies.  Download Phable from PyPI using:

```console
$ pip install phable
```

Phable uses the `zoneinfo` module for IANA time zone support, which by default uses the system's time zone data if available.  If no system time zone data is available, then Phable requires the `tzdata` package available on PyPI to be installed.

Phable has an optional `pandas` package dependency that is required for `phable.kinds.Grid.to_pandas()`.  Download Phable with `pandas` from PyPI using:

```console
$ pip install "phable[pandas]"
```

Note:  Most of the below examples require the optional `pandas` package dependency.

Example: A Custom SSL Context & Haystack's About op
---------------------------------------------------
```python
from datetime import date

from phable.client import Client
import ssl

# define these settings specific to your use case
uri = "https://host"
username = "<username>"
password = "<password>"
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

with Client(uri, username, password, ssl_context) as ph:
    about = ph.about()

print("Here is information about the Haystack server:\n")
print(about)
```

Example: Haystack's Read & HisRead ops
--------------------------------------
```python
from datetime import date

from phable.client import Client

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "<username>"
password = "<password>"

with Client(uri, username, password) as ph:
    # read history data on main electric meter power points
    pts = ph.read("point and power and equipRef->siteMeter")
    his_df = ph.his_read(pts, date.today()).to_pandas()

print("Here is the Pandas DataFrame showing point history data:\n")
print(his_df)
print()
print(f"Here are the DataFrame's attributes:\n{his_df.attrs}")

# Notes:
# 1. These attributes contain info from the point grid
# 2. Later we may add some funcs to help parse the DataFrame attributes
# 3. History Grids converted to Pandas DataFrames show point Ref display names
#    for their column names
```

Example: Haystack's Read & HisRead ops using IDs and DateRange
--------------------------------------------------------------
```python
from datetime import date, timedelta

from phable.client import Client
from phable.kinds import DateRange, Ref

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "<username>"
password = "<password>"

# update the ids for your server
id1 = Ref("p:demo:r:2caffc8e-aa932383")
id2 = Ref("p:demo:r:2caffc8e-1768df4f")

end = date.today()
start = end - timedelta(days=2)
range = DateRange(start, end)

with Client(uri, username, password) as ph:
    pts_df = ph.read_by_ids([id1, id2]).to_pandas()
    his_df = ph.his_read_by_ids([id1, id2], range).to_pandas()

print("Here is the Pandas DataFrame showing point data:\n")
print(pts_df)
print()
print("Here is the Pandas DataFrame showing point history data:\n")
print(his_df)
print()
print(f"Here are attributes on the History DataFrame:\n{his_df.attrs}")

# Notes:
# 1. There are fewer attributes preserved when using Client.his_read_by_ids()
#    compared to Client.read_by_ids()
# 2. History Grids converted to Pandas DataFrames show point Ref display names
#    for their column names
```

Example: Async Usage without Context Manager
--------------------------------------------
```python
import asyncio

from phable.client import Client


async def main() -> None:
    # define these settings specific to your use case
    uri = "http://localhost:8080/api/demo"
    username = "<username>"
    password = "<password>"

    ph = Client(uri, username, password)

    ph.open()
    power_pt_read = asyncio.to_thread(ph.read, "power and point")
    energy_pt_read = asyncio.to_thread(ph.read, "energy and point")
    power_pt_read, energy_pt_read = await asyncio.gather(power_pt_read, energy_pt_read)
    ph.close()

    # convert Grids to Pandas DataFrames and print the results
    power_pt_read = power_pt_read.to_pandas()
    energy_pt_read = energy_pt_read.to_pandas()

    print(power_pt_read)
    print(energy_pt_read)


if __name__ == "__main__":
    asyncio.run(main())
```

Example: History Write to a Single Point
----------------------------------------
```python
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
        "val": Number(1_000.0, "kW"),
    },
    {
        "ts": ts_now,
        "val": Number(2_000.0, "kW"),
    },
]

with Client(uri, username, password) as ph:
    ph.his_write_by_ids(Ref("2d6a2714-0d0a79fb"), data_rows)
```

Example: History Write to Multiple Points
-----------------------------------------
```python
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
```

Example: SkySpark's Eval Op
---------------------------
```python
from phable.client import Client

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


with Client(uri, username, password) as ph:
    his_df = ph.eval(
        """read(power and point and equipRef->siteMeter)
                        .hisRead(lastMonth)"""
    ).to_pandas()

print("Here is the Pandas DataFrame showing point history data:\n")
print(his_df)
print()
print(f"Here are the DataFrame's attributes:\n{his_df.attrs}")
```


Example: SkySpark's Commit Op
-----------------------------
```python
from phable.client import Client, CommitFlag
from phable.kinds import Grid, Marker

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"

# create the client object and open the connection
ph = Client(uri, username, password)
ph.open()

# create a new rec on SkySpark
rec = [{"dis": "TestRec", "testing": Marker(), "pytest": Marker()}]
response1: Grid = ph.commit(rec, CommitFlag.ADD)

# update the newly created rec
# Note:  the id column and current mod timestamp must be included
rec = [
    {"id": response1.rows[0]["id"], "mod": response1.rows[0]["mod"], "foo": "new tag"}
]
response2: Grid = ph.commit(rec, CommitFlag.UPDATE)

# delete the newly created and updated rec
# Note:  the rec should have only an id and mod column
rec = [{"id": response2.rows[0]["id"], "mod": response2.rows[0]["mod"]}]
response: Grid = ph.commit(rec, CommitFlag.REMOVE)

# close the session with the Haystack server (SkySpark)
ph.close()
```


Breaking Changes
----------------
The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  We plan to release a stable version 1.0.0 of Phable sometime in 2024 (TBD).
