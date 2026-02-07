# Changelog

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

---

## Design Rationale

#### 1. Grid.cols Type Change

**What Changed:**
- Introduced `GridCol` dataclass for structured column metadata
- Grid columns are now represented as `list[GridCol]` instead of `list[dict[str, dict]]`

**Why This Change:**

The original list-of-dictionaries approach had several issues:

- **Weak typing**: `list[dict[str, dict]]` provides no type safety for column metadata structure
- **Unclear schema**: Dictionary structure doesn't communicate expected keys or values
- **Misalignment with best practices**: Modern Python favors dataclasses over raw dictionaries for structured data

**Benefits:**

```python
# Before (v0.1.25)
for col in grid.cols:
    print(col["name"], col["meta"])

# After (v0.1.26)
for col in grid.cols:
    print(col.name, col.meta)
```

This change:
- Provides stronger typing and better IDE support
- Enables improved validation and clearer error messages
- Makes the expected structure explicit and self-documenting

#### 2. Grid to DataFrame Conversion

**What Changed:**

- Non-history grids are no longer supported by these methods and raise `ValueError`
- `to_pandas()` and `to_polars()` convert history grids to long format
- `get_df_meta()`, `to_pandas_all()`, `to_polars_all()` have been removed

**Why These Changes:**

Phable represents Project Haystack kinds (e.g., Marker, Ref, NA, Number) as Python classes. In prior Phable versions, these types mapped to DataFrame object columns, which negated the performance benefits of using DataFrames. Future versions may provide optimized DataFrame representations for all Haystack types, but for now, DataFrame conversion supports only history grids.

Earlier Phable versions converted Number values to DataFrame native numeric types, but required separate dictionaries for essential metadata (point IDs, display names, units) and lost data fidelity when NA values were present.

Long format preserves Project Haystack conventions while using optimal DataFrame types:

- **Easy Access to Point ID**: Point identifiers from column metadata are transmitted on each row in the `id` column of the DataFrame.
- **Not Available (NA)**: Maintains the semantic difference between null values (missing data at unaligned timestamps) and NA (sensor error or unavailable reading). This distinction helps identify time periods where basic interpolation can be confidently used to fill missing data (null) versus periods where data quality is questionable (NA).

**Wide Format (Before):**

Example history data for three temperature sensors:

| ts                  | v0      | v1      | v2      |
|---------------------|---------|---------|---------|
| 2024-01-01T00:00:00 | 72.5°F  | 68.2°F  | 70.1°F  |
| 2024-01-01T01:00:00 | 73.1°F  | 68.5°F  | 70.3°F  |

Each row is a timestamp, each column (`v0`, `v1`, `v2`) is a different point's value. In this format, column metadata (point IDs, display names, units) must be stored separately in auxiliary data structures like dictionaries, requiring extra lookups to understand what each column represents. For example:

```python
column_metadata = {
    "v0": {"id": "point1", "dis": "Zone 1 Temp", "unit": "°F"},
    "v1": {"id": "point2", "dis": "Zone 2 Temp", "unit": "°F"},
    "v2": {"id": "point3", "dis": "Zone 3 Temp", "unit": "°F"}
}
```

**Long Format (After):**

The same three sensors, now in long format:

| id     | ts                  | val_bool | val_str | val_num | na    |
|--------|---------------------|----------|---------|---------|-------|
| point1 | 2024-01-01T00:00:00 | null     | null    | 72.5    | False |
| point1 | 2024-01-01T01:00:00 | null     | null    | 73.1    | False |
| point2 | 2024-01-01T00:00:00 | null     | null    | 68.2    | False |
| point2 | 2024-01-01T01:00:00 | null     | null    | 68.5    | False |
| point3 | 2024-01-01T00:00:00 | null     | null    | 70.1    | False |
| point3 | 2024-01-01T01:00:00 | null     | null    | 70.3    | False |

This format combines both history data and point IDs in a single, self-contained DataFrame.

#### 3. Ref.__str__() Returns ID Only

**What Changed:**
- `Ref.__str__()` now always returns `ref.val`
- Previously, it returned `ref.dis` if present, falling back to `ref.val` with an `@` prefix

**Why This Change:**

The previous behavior was inconsistent and could cause subtle bugs.

**Problem Example:**
```python
# Before (v0.1.25)
ref1 = Ref(val="abc-123", dis="Room 101")
ref2 = Ref(val="abc-123")

str(ref1)  # "Room 101"
str(ref2)  # "@abc-123"

# Same entity, different string representations!
```

**After (v0.1.26):**
```python
ref1 = Ref(val="abc-123", dis="Room 101")
ref2 = Ref(val="abc-123")

str(ref1)  # "abc-123"
str(ref2)  # "abc-123"
ref1.dis   # "Room 101" (explicit access when needed)
```

#### 4. Improved Type Mappings for Collections

**What Changed:**
- Project Haystack Lists now map to Python `Sequence` types instead of `list`
- Project Haystack Dicts now map to Python `Mapping` types instead of `dict`

**Why This Change:**

Project Haystack data types are immutable, but Python's lists and dicts are mutable. Type checkers can use `typing.Sequence` and `typing.Mapping` to detect mutations while giving programmers flexibility to use either mutable (list/dict) or immutable (tuple/frozendict) types at runtime. Native frozendict support may be added in Python 3.15.

#### 5. GridBuilder Addition

**Why This Change:**

Grid construction was previously verbose and error-prone:

```python
# Before
grid = Grid(
    meta={"dis": "Temperature Data"},
    cols=[
        GridCol(name="ts", meta={}),
        GridCol(name="val", meta={"unit": "°F"}),
    ],
    rows=[
        {"ts": datetime(...), "val": Number(72.5, "°F")},
        {"ts": datetime(...), "val": Number(73.1, "°F")},
    ]
)
```

**GridBuilder** provides a fluent, step-by-step interface:

```python
# After
grid = (
    GridBuilder()
    .set_meta({"dis": "Temperature Data"})
    .add_col("ts")
    .add_col("val", meta={"unit": "°F"})
    .add_row({"ts": datetime(...), "val": Number(72.5, "°F")})
    .add_row({"ts": datetime(...), "val": Number(73.1, "°F")})
    .build()
)
```

**Benefits:**

- **Progressive disclosure**: Build grids step by step
- **Better error messages**: Can validate at each step
- **Method chaining**: Natural, readable construction flow
- **Reduces boilerplate**: Don't have to construct intermediate lists and dicts
