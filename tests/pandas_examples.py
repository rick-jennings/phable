# TODO:  Maybe print out the types to terminal to show what's going on

# Let's fetch some data from a Haystack Server!

# Import dependencies
from phable.client import Client
from phable.kinds import Grid
from phable.parser.json import grid_to_pandas

# define these settings specific for your use case
# Reminder:  Probably secure your login credentials!!!
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"

# --------------------------------------------------------------------------------------
# Example #1: Fetch timeseries data using the eval op (specific to SkySpark)
# --------------------------------------------------------------------------------------

# define the Axon expression
axon_expr = """
            readAll(power and point and equipRef->siteMeter)
                                .hisRead(lastWeek)
            """

# open a session, execute queries, and simply close
# session with the context manager
with Client(uri, username, password) as hc:
    request_dict = {"expr": axon_expr}
    request_grid = Grid.to_grid(request_dict)
    response_grid = hc.eval(request_grid)

# convert response grid to pandas df and convert to csv
df = grid_to_pandas(response_grid)
df.to_csv("example1.csv", index=False)

# --------------------------------------------------------------------------------------
# Example #2: Read all site recs using the Haystack read op
# --------------------------------------------------------------------------------------

with Client(uri, username, password) as hc:
    grid = hc.read("site")

df = grid_to_pandas(grid)
df.to_csv("example2.csv", index=False)

# --------------------------------------------------------------------------------------
# Example #3: Read all equip recs using the Haystack read op
# --------------------------------------------------------------------------------------

with Client(uri, username, password) as hc:
    grid = hc.read("equip")

df = grid_to_pandas(grid)
df.to_csv("example3.csv", index=False)

# --------------------------------------------------------------------------------------
# Example #4: Read all point recs using the Haystack read op
# --------------------------------------------------------------------------------------

with Client(uri, username, password) as hc:
    grid = hc.read("point")

# print(f"Here is the Example #4 grid!\n{grid}")
df = grid_to_pandas(grid)
df.to_csv("example4.csv", index=False)
