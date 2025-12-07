# Welcome

## About

Phable is a modern Python toolkit for working with [Project Haystack](https://project-haystack.org/) tagged data, [Xeto](https://github.com/Project-Haystack/xeto?tab=readme-ov-file#overview) specs, and [Haxall](https://haxall.io/). It provides a simple, Pythonic interface for building automation systems, charging station management systems, and other energy management applications that use Project Haystack semantic data models.

**Open source and free** - Phable is permissively licensed under the MIT license.

### Core Capabilities

**Data Types & Serialization**

- Full support for Project Haystack [kinds](https://project-haystack.org/doc/docHaystack/Kinds) (data types)
- Serialize between Python objects and [Zinc](https://project-haystack.org/doc/docHaystack/Zinc) or [JSON](https://project-haystack.org/doc/docHaystack/Json) formats
- Native Python representations for working with semantic data

**Project Haystack Client**

- Read `site`, `equip`, and `point` entities
- Read current values of real-time data points
- Supervisory control of real-time data points
- Read and write history data for data points

**Haxall Client**

- All Project Haystack client features
- Add, update, and remove operations (commit API)
- Evaluate Axon expressions directly from Python

**Xeto**

- Type checking and validation of Haystack records against Xeto specifications via Haxall's CLI

Learn more about phable by exploring the documentation!

## Installation

Phable requires Python 3.11 or higher. Install from PyPI:

```console
$ pip install phable
```

**Time zone support:** Phable uses the `zoneinfo` module for IANA time zone support. On systems without time zone data, you'll need to install `tzdata`:

```console
$ pip install tzdata
```

**Optional dependencies:** For DataFrame support, install with your preferred library:

```console
$ pip install "phable[pandas,pyarrow]"  # For pandas support
$ pip install "phable[polars]"          # For polars support
```