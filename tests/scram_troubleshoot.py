from datetime import date
from typing import Any

import pandas as pd

from phable.client import Client
from phable.kinds import Grid
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)


# define these settings specific to your use case
# Reminder:  Properly secure your login credentials!!!
uri = "http://localhost:8080/api/demo1"
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

print(f"Results from about example:\n{about_example}\n")
