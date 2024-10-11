# Welcome

## About

Phable is an open source, simple, modern Python toolkit for basic client connectivity to [Project Haystack](https://project-haystack.org/) and [Haxall](https://haxall.io/) defined server applications.

Phable is free to use and permissively licensed under the MIT license.

Project Haystack features include:

 - Reading `site`, `equip`, and `point` entities
 - Reading current values of real-time data points
 - Supervisory control of real-time data points
 - Reading history data for data points
 - Writing history data to already established data points

Haxall features include:

 - All Project Haystack features listed above
 - Add, update, and remove commit operations
 - Evaluation of an Axon string expression

Learn more about `Phable` by exploring the documentation!

## Installation

Phable requires Python version 3.11 or higher and generally has no required software dependencies.  Download Phable from PyPI using:

```console
$ pip install phable
```

Phable uses the `zoneinfo` module for IANA time zone support, which by default uses the system's time zone data if available.  If no system time zone data is available, then Phable requires the `tzdata` package available on PyPI to be installed.

Phable has optional `pandas` and `pyarrow` package dependencies that are required for `phable.Grid.to_pandas()` and `phable.Grid.to_pandas_all()`.  Download Phable with `pandas` and `pyarrow` from PyPI using:

```console
$ pip install "phable[pandas,pyarrow]"
```

Similarly, Phable has an optional `polars` package dependency that is required for `phable.Grid.to_polars()` and `phable.Grid.to_polars_all()`.  Download Phable with `polars` from PyPI using:

```console
$ pip install "phable[polars]"
```