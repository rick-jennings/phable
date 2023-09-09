# Changelog

## 0.1.10 (2023-09-09)

#### New Features

* add Grid to Pandas DataFrame conversion support
* add DateRange and DateTimeRange support for HisRead
* add batch HisRead support
* add single and batch HisWrite support
#### Fixes

* map objects of type int or float to Number in json parser
* make scram properties protected
* correct scram parsing to allow for nHaystack compatibility
* change name of Coordinate kind to Coord
* make DateTime tz attribute support type str only
* make client tests use rel dates & w/o unique Refs
* allow Number kind to accept int or float
#### Refactorings

* change Client to return Pandas objects
* improve scram exceptions
* rename 'parser' package as 'parsers'
* remove poetry.lock file from source control
* remove Date and DateTime kinds
* add types and improve error msg in json.py
* remove redundant test files
* remove the exception module
#### Docs

* modify examples in README to reflect changes
* add async usage example in README
#### Others

* remove .vscode directory from source control
#### Notes

* Most Phable users require data in Pandas format.  Our intention was to avoid
adding Pandas as a dependency and to support various DataFrame formats with
Haystack Grid.  On the other hand we wanted to make it easier to return data
fetched from Client() methods into ready to use Pandas dataframes.
* In the Phable v0.1.10 release we decided to add Pandas as a dependency.
However, in the v0.1.11 release we plan to remove Pandas as a dependency.  We
plan to explore leveraging the Python DataFrame Interchange Protocol to avoid
the need to have DataFrame library dependencies to Phable.
* There have been a considerable amount of changes since Phable v0.1.9.  We
recommend that you work through the README examples and review the Client()
class in the client module.