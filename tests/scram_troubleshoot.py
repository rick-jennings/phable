from typing import Any

from phable.client import Client
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)


# define these settings specific to your use case
# Reminder:  Properly secure your login credentials!!!
uri = "http://localhost:8080/api/demo1"
username = "su"
password = "su"


# open a session, execute queries, and simply close
# session with the context manager
with Client(uri, username, password) as ph:
    # fetch info about the Haystack server
    about_example: dict[str, Any] = ph.about()

print(f"Results from about example:\n{about_example}\n")
