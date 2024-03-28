# Changelog

## 0.1.14 (2024-03-28)

#### New Features

* add Grid.to_grid() for convenience
* enhance HisWrite op with simpler parameters
    Note:  This is a breaking change.  See examples in the README.
* add PointWrite op support
* add SkySpark commit op to Client
#### Refactorings

* make data input to Client._call() type Grid
* improve Haystack Grid to JSON parsing
#### Docs

* clarify Haystack server support for Close, HisRead and HisWrite ops
* add example for SkySpark's commit op
* add example for SkySpark's eval op
* add HisWrite op examples
* clarify possible tzdata package dependency