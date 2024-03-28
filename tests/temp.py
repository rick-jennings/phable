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
