# Changelog

## 0.1.16 (2024-10-10)

**Note:** This version has considerable breaking changes compared to the last version 0.1.15.  It is recommended to use the new Phable website for documentation to help upgrade code from earlier versions.  Fortunately, we are getting closer to a more stable 1.0.0 release.

#### New Features

* add Grid to Polars support
* add new context managers for HaystackClient and HaxallClient
* expose call() on HaystackClient
* intro his_write_by_id() to HaystackClient
* intro his_read_by_id() to HaystackClient
* intro read_all() and rework read() on HaystackClient
* add read_by_id method to HaystackClient
* introduce HaxallClient
* import data types using top level package
#### Fixes

* make Number vals type float only
* change point_write param from "dur" to "duration"
#### Refactorings

* rename Client to HaystackClient
* change HaystackClient's his_write_by_ids() input parameters
* change HaystackClient's his_read_by_ids() input parameters
* improve conversion methods to pandas
* remove HaystackClient's his_read()
* rework how HaystackClient is initialized
* rework errors raised by client classes
* return empty Grid for HaystackClient.close()
#### Docs

* intro [website](https://phable.dev)