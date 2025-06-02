# Changelog

## 0.1.18 (2025-06-02)

#### New Features

* add http debug logging
* add haxall file ops (GET, POST, PUT)
#### Fixes

* make http req mime type just 'application/json'
* allow trailing slash in uri
#### Refactorings

* improve underlying http request code
* apply uv & ruff tooling
* parametrize some test funcs
#### Docs

* describe new file methods
* add http debug logging example
* route links using package level imports
* add doc_venv/ to .gitignore
#### Others

* update package versions for docs
* specify python-version 3.11 for github action
* remove unused files from .gitignore
* refactor for skyspark-3.1.11 recent demo proj
