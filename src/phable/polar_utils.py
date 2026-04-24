from __future__ import annotations

from collections.abc import Mapping

import polars as pl


def _validate_long(df: pl.DataFrame) -> None:
    missing = {"id", "ts", "val_bool", "val_str", "val_num", "val_na"} - set(df.columns)
    if missing:
        raise ValueError(
            f"DataFrame is missing required columns: {sorted(missing)}. "
            "Pass a DataFrame returned by Grid.to_polars()."
        )

    if not isinstance(df["id"].dtype, pl.Categorical):
        raise ValueError(
            "Column 'id' must be Categorical. "
            "Pass a DataFrame returned by Grid.to_polars()."
        )

    ts_dtype = df["ts"].dtype
    if not isinstance(ts_dtype, pl.Datetime) or ts_dtype.time_zone is None:
        raise ValueError(
            "Column 'ts' must be Datetime with timezone. "
            "Pass a DataFrame returned by Grid.to_polars()."
        )

    expected = {
        "val_bool": pl.Boolean,
        "val_str": pl.String,
        "val_num": pl.Float64,
        "val_na": pl.Boolean,
    }
    for col, dtype in expected.items():
        if df[col].dtype != dtype:
            raise ValueError(
                f"Column '{col}' must be {dtype}. "
                "Pass a DataFrame returned by Grid.to_polars()."
            )


def his_long_to_wide(
    df: pl.DataFrame,
    columns: Mapping[str, str] | None = None,
) -> pl.DataFrame:
    """Converts a Phable long-format Polars DataFrame to wide format.

    **Note:** This function is experimental and subject to change.

    Automatically detects whether to pivot on `val_bool`, `val_str`, or `val_num` based
    on which column has non-null data. Points whose rows are all Haystack `NA` are
    omitted from the wide-format result. Users are encouraged to handle `NA` values in
    the long-format DataFrame before pivoting, since pivoting loses `NA` semantics which
    define where interpolation should not occur.

    Parameters:
        df:
            Phable long-format Polars DataFrame.
        columns:
            Optional mapping of `id` value to wide-format column name. If omitted,
            `id` values are used as column names.

    Returns:
        Wide-format DataFrame with `ts` as the first column and one column per point,
        sorted by `ts`. Column order beyond `ts` is not guaranteed; it depends on the
        order points are encountered across the three value-type pivots
        (`val_bool`, `val_str`, `val_num`).

    **Example:**

    ```python
    df_long = his_grid.to_polars()

    # if applicable, interpolate using val_na before pivoting (pivoting loses NA semantics)

    # use id values as column names
    df_wide = his_long_to_wide(df_long)

    # or use Grid column names
    columns = {col.meta["id"].val: col.name for col in his_grid.cols if col.name != "ts"}
    df_wide = his_long_to_wide(df_long, columns)
    ```
    """
    _validate_long(df)

    df = df.filter(pl.col("val_na").is_null())

    frames: list[pl.DataFrame] = []
    for val_col in ("val_bool", "val_str", "val_num"):
        pivot = df.pivot(values=val_col, index="ts", on="id")

        non_null_cols = [
            c for c in pivot.columns if c == "ts" or pivot[c].is_not_null().any()
        ]
        pivot = pivot.select(non_null_cols)

        if len(pivot.columns) <= 1:
            continue

        frames.append(pivot)

    if not frames:
        raise ValueError("No data to pivot.")

    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, on="ts", how="full", coalesce=True)

    result = result.sort("ts")

    if columns:
        result = result.rename(columns)

    return result
