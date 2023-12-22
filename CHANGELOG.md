# Changelog

## 0.1.11 (2023-12-22)

#### New Features

* add optional ssl context to Client init
* allow Client.read_by_ids() accept a Ref
* add attributes to Pandas DataFrames
#### Refactorings

* make pandas an optional dependency
* remove Grid.get_ids()
* remove Grid.to_grid()
* introduce improved meta support for Client.his_read()
* introduce Client.his_read_by_ids()
* remove Client.read_by_id()
* remove DataFrameColumnDisplayHasInvalidUnitError
#### Docs

* update README with new examples
#### Notes

* Pandas is now an optional dependency which is required for phable.kinds.Grid.to_pandas()
* Pandas DataFrames created by Phable include former Grid metadata in their attributes and use Ref display names for column names.  This requires that no two Refs have the same display name.
* Grids created by phable.client.Client.his_read() will generally contain more metadata than Grids created by phable.client.Client.his_read_by_ids()
* We are beginning to prepare for a future stable release version 1.0.0 in 2024 (TBD)