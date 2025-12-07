# Async Usage without Context Manager

```python
import asyncio

from phable import HaystackClient


async def main() -> None:
    # define these settings specific to your use case
    uri = "http://localhost:8080/api/demo"
    username = "<username>"
    password = "<password>"

    client = HaystackClient.open(uri=uri, username=username, password=password)

    power_pt_grid = asyncio.to_thread(client.read, "power and point")
    energy_pt_grid = asyncio.to_thread(client.read, "energy and point")
    power_pt_grid, energy_pt_grid = await asyncio.gather(power_pt_grid, energy_pt_grid)

    # remember to close the session with the server
    client.close()

    power_pt_df_meta, power_pt_df = power_pt_grid.to_polars_all()
    energy_pt_df_meta, energy_pt_df = energy_pt_grid.to_polars_all()


if __name__ == "__main__":
    asyncio.run(main())
```
