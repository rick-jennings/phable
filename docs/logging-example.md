# Saving Logs to a File

Running the below Python code and observing the generated log file may help with troubleshooting HTTP related issues. Also, the log configuration shown can be modified to address other use cases.

**Note:** In this example the logs are written to a file called `app.log` in the same directory as the executed Python script.

```python
import logging

from phable import open_haystack_client

logging.basicConfig(
    filename="app.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {name} - {levelname} - {message}",
    level=logging.DEBUG,
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "<username>"
password = "<password>"

with open_haystack_client(uri, username, password) as client:
    client.about()
```
