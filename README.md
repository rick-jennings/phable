About
-----
Phable is a simple, modern Python toolkit for basic client connectivity to a [Project Haystack](https://project-haystack.org/) server.

We aimed to make code within Phable's client.py and kinds.py modules self-documenting by using type hints and docstrings and carefully selecting what is publicly exposed.  In the future we plan to improve docs and release a stable version 1.0.0 for Phable.

Installation
------------
Phable requires Python version 3.11 or higher and has no required software dependencies.  Download Phable without its optional dependencies from PyPI using:

```console
$ pip install phable
```

Phable has an optional Pandas dependency that is required for phable.kinds.Grid.to_pandas().  Download Phable with its optional Pandas dependency from PyPI using:

```console
$ pip install "phable[pandas]"
```

Note:  Most of the below examples require the optional Pandas dependency.

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
```


Breaking Changes
----------------
The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  We plan to release a stable version 1.0.0 of Phable sometime in 2024 (TBD).
