About
-----
Phable provides a simple way to retrieve and process JSON formatted [Project Haystack](https://project-haystack.org/) data from a flat file or Haystack server using modern Python.

The initial focus of this project is to make it easy to work with a Jupyter Notebook connected to a Haystack server.

Installation
------------
Phable requires Python 3.11 or higher.  During the early stages of this project we will likely have several Python package dependencies, such as Pandas.  In the future we hope to remove these dependencies from Phable.

Download Phable from Pypi:

```console
$ pip install phable
```

Quick start
-----------
The below example shows how to obtain an auth token from a Haystack server.

```python
from phable.phable import Phable

# define these settings specific for your use case
# Reminder:  Probably secure your login credentials!!!
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"

# open a session, execute a query, and simply close
# session with the context manager
with Phable(uri, username, password) as ph:
    about_grid = ph.about()

print(f"Here are details about the Haystack Server:\n{about_grid}")
```

**More examples coming soon!**

TODO:
-----------
- Decide whether or not to add Haystack Bool, Str, List, Dict kinds
    - Note:  For now we assume the Python equivalent types map 1-to-1
- Review how to better support conversion of Python Grid to Pandas
- Authenticate the server by checking the validity of the server final
  message
- Refactor tests to not be restricted to a specific server
- Verify that the Haystack Kind objects print correctly