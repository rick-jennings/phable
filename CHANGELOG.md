# Changelog

## 0.1.27 (2026-04-24)

### ⚠️ BREAKING CHANGES

* **Grid.to_pandas()** / **Grid.to_polars()**: Column `na` renamed to `val_na`
* **Grid.to_pandas()** / **Grid.to_polars()**: Raises `ValueError` when two or more columns share the same `id` value (and `col_names_as_ids=False`)

### Features

* **pandas_utils.his_long_to_wide()**: New utility to convert a Phable long-format Pandas DataFrame to wide format (experimental)
* **polar_utils.his_long_to_wide()**: New utility to convert a Phable long-format Polars DataFrame to wide format (experimental)
* **Grid.to_pandas()** / **Grid.to_polars()**: Added `col_names_as_ids` keyword argument — when `True`, uses grid column names as DataFrame `id` values instead of requiring `Ref` metadata

### Improvements

* Column `val_na` is now nullable (`None` when value is not NA, `True` when it is), consistent with `val_bool`, `val_str`, and `val_num`

---

## 0.1.26 (2026-02-07)

### ⚠️ BREAKING CHANGES

* **Grid.cols**: Now uses `list[GridCol]` dataclass instead of `list[dict[str, dict]]`
* **Grid.to_pandas()** / **Grid.to_polars()**: Only convert history grids to long format (raises `ValueError` for non-history grids)
* **Grid**: Removed methods `get_df_meta()`, `to_pandas_all()`, and `to_polars_all()`
* **Ref.__str__()**: Returns only the ID value (not display name). Access `ref.dis` directly for display name.
* **Type mappings**: Haystack Lists now map to `Sequence`, Dicts map to `Mapping` (instead of `list`/`dict`)

### Features

* **GridBuilder**: Added API for constructing grids

### Improvements

* Improved type hints using `Mapping` and `Sequence` for immutable collections
* fix: HaystackClient.about uses GET and handles dict response for nhaystack support
* test: replace skyspark dependency with haxall
