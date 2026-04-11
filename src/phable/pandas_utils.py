from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
import pyarrow as pa


def _validate_long(df: pd.DataFrame) -> None:
    missing = {"id", "ts", "val_bool", "val_str", "val_num", "val_na"} - set(df.columns)
    if missing:
        raise ValueError(
            f"DataFrame is missing required columns: {sorted(missing)}. "
            "Pass a DataFrame returned by Grid.to_pandas()."
        )

    if not isinstance(df["id"].dtype, pd.CategoricalDtype):
        raise ValueError(
            "Column 'id' must be CategoricalDtype. "
            "Pass a DataFrame returned by Grid.to_pandas()."
        )

    ts_dtype = df["ts"].dtype
    if (
        not isinstance(ts_dtype, pd.ArrowDtype)
        or not pa.types.is_timestamp(ts_dtype.pyarrow_dtype)
        or ts_dtype.pyarrow_dtype.tz is None
    ):
        raise ValueError(
            "Column 'ts' must be ArrowDtype timestamp with timezone. "
            "Pass a DataFrame returned by Grid.to_pandas()."
        )

    expected = {
        "val_bool": pd.ArrowDtype(pa.bool_()),
        "val_str": pd.ArrowDtype(pa.string()),
        "val_num": pd.ArrowDtype(pa.float64()),
        "val_na": pd.ArrowDtype(pa.bool_()),
    }
    for col, dtype in expected.items():
        if df[col].dtype != dtype:
            raise ValueError(
                f"Column '{col}' must be {dtype}. "
                "Pass a DataFrame returned by Grid.to_pandas()."
            )


def his_long_to_wide(
    df: pd.DataFrame,
    columns: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Converts a Phable long-format Pandas DataFrame to wide format.

    **Note:** This function is experimental and subject to change.

    Automatically detects whether to pivot on `val_bool`, `val_str`, or `val_num` based
    on which column has non-null data. Points whose rows are all Haystack `NA` are
    omitted from the wide-format result. Users are encouraged to handle `NA` values in
    the long-format DataFrame before pivoting, since pivoting loses `NA` semantics which
    define where interpolation should not occur.

    If `(ts, id)` pairs are not unique in `df`, `pivot_table` will raise a `ValueError`.

    Parameters:
        df:
            Phable long-format Pandas DataFrame.
        columns:
            Optional mapping of `id` value to wide-format column name. If omitted,
            `id` values are used as column names.

    Returns:
        Wide-format DataFrame indexed by `ts` with one column per point. Column order
        is not guaranteed; it depends on the order points are encountered across the
        three value-type pivots (`val_bool`, `val_str`, `val_num`).

    **Example:**

    ```python
    df_long = his_grid.to_pandas()

    # optionally process NA values here before pivoting

    # use id values as column names
    df_wide = his_long_to_wide(df_long)

    # or use Grid column names
    columns = {col.meta["id"].val: col.name for col in his_grid.cols if col.name != "ts"}
    df_wide = his_long_to_wide(df_long, columns)
    ```
    """
    _validate_long(df)

    frames = []
    for val_col in ("val_bool", "val_str", "val_num"):
        pivot = df.pivot_table(
            index="ts", columns="id", values=val_col, aggfunc="first", observed=True
        )
        pivot = pivot.dropna(axis=1, how="all")
        if not pivot.empty:
            frames.append(pivot)

    df_wide = pd.concat(frames, axis=1)

    if columns:
        df_wide = df_wide.rename(columns=columns)

    df_wide.columns = pd.Index(df_wide.columns.tolist())

    return df_wide
