from datetime import date

from phable.client import Client

uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


with Client(uri, username, password) as ph:
    pt_grid = ph.read("power and point and equipRef->siteMeter")
    his_grid = ph.his_read(pt_grid.get_ids(), date.today())

his_df = his_grid.to_pandas()

print(his_df)
print(his_df.attrs)
