About
-----
Phable is a simple, modern Python toolkit for basic client connectivity to a [Project Haystack](https://project-haystack.org/) server.

Installation
------------
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

Breaking Changes
----------------
The early focus of this project is to find the best practices for using modern Python with a Haystack server.  This may lead to breaking changes in newer Phable versions.  After there has been sufficient experience with Phable, we plan to release a stable version 1.0.0.

Learn More
----------
Phable's official website located [here](https://phable.dev/) has additional documentation and examples for Phable's API.