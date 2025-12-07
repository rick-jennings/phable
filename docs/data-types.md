## Introduction

Project Haystack defines a fixed set of data types called
[*kinds*](https://project-haystack.org/doc/docHaystack/Kinds), which are mapped to Python objects in Phable.

**Map for singleton data types**

| Project Haystack | Phable              |
| -----------------| -------------------
| `Marker`         | `phable.Marker`     |
| `NA`             | `phable.NA`         |
| `Remove`         | `phable.Remove`     |

**Map for scalar atomic data types**

| Project Haystack | Phable              |
| -----------------| -------------------
| `Bool`           | `bool`              |
| `Number`         | `phable.Number`     |
| `Str`            | `str`               |
| `Uri`            | `phable.Uri`        |
| `Ref`            | `phable.Ref`        |
| `Symbol`         | `phable.Symbol`     |
| `Date`           | `datetime.date`     |
| `Time`           | `datetime.time`     |
| `DateTime`       | `datetime.datetime` |
| `Coord`          | `phable.Coord`      |
| `XStr`           | `phable.XStr`       |

**Note:** Phable's `datetime.datetime` must be timezone aware to represent Project
Haystack's `DateTime`.

**Map for collection data types**

| Project Haystack | Phable              |
| -----------------| -------------------
| `List`           | `list`              |
| `Dict`           | `dict`              |
| `Grid`           | `phable.Grid`       |

**Data Types in Phable Only**

As a convenience, Phable defines these data types, which are not defined in Project
Haystack:

* `phable.DateRange`
* `phable.DateTimeRange`

::: phable.kinds.Marker

::: phable.kinds.NA

::: phable.kinds.Remove

::: phable.kinds.Number

::: phable.kinds.Uri

::: phable.kinds.Ref

::: phable.kinds.Symbol

::: phable.kinds.Coord

::: phable.kinds.XStr

::: phable.kinds.Grid

::: phable.kinds.DateRange

::: phable.kinds.DateTimeRange