About
-----
Phable is a simple, modern Python toolkit for basic client connectivity to a [Project Haystack](https://project-haystack.org/) server.

Installation
------------
Phable requires Python 3.11 or higher and has no other software dependencies, except for the Pandas library.  Phable is in early development and it is recommended to stay on the latest available version.

Download Phable from Pypi:

```console
$ pip install phable
```

Basic Usage Examples
--------------------
```python
from datetime import date

from phable.client import Client

# define these settings specific to your use case
# Reminder:  Properly secure your login credentials!!!
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


with Client(uri, username, password) as ph:
    # get information about the Haystack server
    about = ph.about()

    # read history data on main electric meter power points
    pts = ph.read("point and power and equipRef->siteMeter")
    his_df = ph.his_read(pts, date.today()).to_pandas()

print("Here is information about the Haystack server:\n")
print(about)
print()
print("Here is the Pandas DataFrame showing point history data:\n")
print(his_df)
print()
print(f"Here are the DataFrame's attributes:\n{his_df.attrs}")


# Notes:
# 1. these attributes contain important info, such as a column's unit
# 2. later we will add some funcs to help parse the DataFrame attributes
# 3. see the other public available methods on the Client class in client.py
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

    print(read1.to_pandas())
    print(read2.to_pandas())


if __name__ == "__main__":
    asyncio.run(main())
```

Breaking Changes
----------------
Phable is a new open-source project.  The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  However, in the future we do plan to have more stable releases and updates of Phable.