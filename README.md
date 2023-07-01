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
from phable.client import Client
from phable.kinds import Grid, Ref
from typing import Any
import pandas as pd

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
    about_example: dict[str, Any] = ph.about()
    read_example: Grid = ph.read("site")

    # Note: make sure to adapt the Ref IDs to ones that are valid
    # on your server!
    read_by_id_example: dict[str, Any] = ph.read_by_id(
        Ref("p:demo:r:2bae2387-576dd9b9")
    )
    read_by_ids_example: Grid = ph.read_by_ids(
        [Ref("p:demo:r:2bae2387-576dd9b9"), Ref("p:demo:r:2bae2387-cd79dce9")]
    )

    # single hisRead
    single_his_read_example: Grid = ph.his_read(
        Ref("p:demo:r:2bae2387-d7707510"), "2023-05-02"
    )

    # batch hisRead
    batch_his_read_example: Grid = ph.his_read(
        [Ref("p:demo:r:2bae2387-d7707510"), Ref("p:demo:r:2bae2387-ed0ce5b7")],
        "2023-05-02",
    )

# Now print the results!
# Note: Converting Haystack Grids to Pandas DataFrames for better print display and to
#       show how the conversion to Pandas works.  Keep in mind that the Haystack kinds
#       are preserved within the Pandas DataFrame.  You may want to convert Haystack
#       kind objects, especially Haystack Numbers, to other more optimal data types
#       within Pandas.
print(f"Results from about example:\n{about_example}\n")
print(f"Results from read example:\n{to_pandas(read_example)}\n")
print(f"Results from read by id example:\n{read_by_id_example}\n")
print(f"Results from single hisRead example:\n{to_pandas(single_his_read_example)}\n")
print(f"Results from batch hisRead example:\n{to_pandas(batch_his_read_example)}\n")
```

Breaking Changes
----------------
Phable is a new open-source project.  The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  However, in the future we do plan to have more stable releases and updates of Phable.