About
-----
Phable is a simple, modern Python toolkit for basic client connectivity to a [Project Haystack](https://project-haystack.org/) server.

Installation
------------
Phable requires Python 3.11 or higher and has no other software dependencies.  Phable is in early development and it is recommended to stay on the latest available version.

Download Phable from Pypi:

```console
$ pip install phable
```

Basic Usage Examples
--------------------
```python
from datetime import date
from typing import Any

import pandas as pd

from phable.client import Client
from phable.kinds import Grid

# define these settings specific to your use case
# Reminder:  Properly secure your login credentials!!!
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


# define convenience func for converting Haystack Grid to Pandas DataFrame
def to_pandas(grid: Grid) -> pd.DataFrame:
    return pd.DataFrame(data=grid.rows).rename(columns=grid.col_rename_map)


# open a session, execute queries, and simply close
# session with the context manager
with Client(uri, username, password) as ph:
    # fetch info about the Haystack server
    about_example: dict[str, Any] = ph.about()

    # fetch a rec
    read_example: Grid = ph.read("point and power and equipRef->siteMeter")

    # find the ids for two recs in read_example
    rec_id1 = read_example.rows[0]["id"]
    rec_id2 = read_example.rows[1]["id"]

    # fetch recs using Ref IDs
    read_by_id_example: dict[str, Any] = ph.read_by_id(rec_id1)
    read_by_ids_example: Grid = ph.read_by_ids([rec_id1, rec_id2])

    # single hisRead
    single_his_read_example: Grid = ph.his_read(
        rec_id1, date.today().isoformat()
    )

    # batch hisRead
    batch_his_read_example: Grid = ph.his_read(
        [rec_id1, rec_id2], date.today().isoformat()
    )


"""
Now print the results!

Note:
-----
Converting Haystack Grids to Pandas DataFrames for better print display and to
show how the conversion to Pandas works.  Keep in mind that the Haystack kinds
are preserved within the Pandas DataFrame.  You may want to convert Haystack
kind objects, especially Haystack Numbers, to other more optimal data types
within Pandas.
"""

print(f"Results from about example:\n{about_example}\n")
print(f"Results from read example:\n{to_pandas(read_example)}\n")
print(f"Results from read by id example:\n{read_by_id_example}\n")
print(
    "Results from single hisRead example:"
    f"\n{to_pandas(single_his_read_example)}\n"
)
print(
    "Results from batch hisRead example:"
    f"\n{to_pandas(batch_his_read_example)}\n"
)
```

Review the test code for more examples.  Here is a link to an example on how to perform a hisWrite op on a single data point:
https://github.com/rick-jennings/phable/blob/main/tests/test_client.py#L184

Async Usage Example
-------------------
```python
import asyncio
from phable.client import Client


# define these settings specific to your use case
# Reminder:  Properly secure your login credentials!!!
async def main() -> None:
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"
    ph = Client(uri, username, password)

    ph.open()
    read1 = asyncio.to_thread(ph.read, "power and point")
    read2 = asyncio.to_thread(ph.read, "energy and point")
    read1, read2 = await asyncio.gather(read1, read2)
    ph.close()

    print(read1.rows)
    print(read2.rows)

if __name__ == "__main__":
    asyncio.run(main())
```

Breaking Changes
----------------
Phable is a new open-source project.  The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  However, in the future we do plan to have more stable releases and updates of Phable.

TODO
----
- Reevaluate wherever # type: ignore is applied
- Add better validation to Uri kind
- Reconsider the use of singleton for Marker(), Remove(), and NA()
- Consider using Pydantic v2 for Haystack Kind objects
- Add date range support
- Add static method to DateTime to create DateTime objects
- Consider if there is a more clear type for data parameter to his_write()
- Consider decoupling the scram code from client