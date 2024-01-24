from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from phable.kinds import NA, Number

if TYPE_CHECKING:
    from phable.kinds import Grid, Ref


# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------
class UnitMismatchError(Exception):
    help_msg: str


class NotFoundError(Exception):
    help_msg: str


class DuplicateColNameError(Exception):
    help_msg: str


# -----------------------------------------------------------------------------
# API exposed funcs
# -----------------------------------------------------------------------------


def grid_to_pandas(grid: Grid) -> pd.DataFrame:
    df = pd.DataFrame(data=grid.rows)

    df.attrs["meta"] = grid.meta.copy()
    df.attrs["cols"] = grid.cols.copy()

    # handle Grid without history data
    if not grid.cols[0]["name"] == "ts":
        return df

    # create a map of HisGrid col names to their point display names
    his_col_name_map = _his_col_name_map(grid)

    # verify there are no duplicate column display names
    _verify_no_duplicate_his_col_names(his_col_name_map)

    # some more validation and parse types
    df = _parse_types(df, grid.meta, grid.cols)

    # configure df
    df = df.rename(columns=his_col_name_map)
    df = df.set_index("Timestamp")

    # _find_his_col_meta(pt_data, his_col_name_map)

    return df


def get_col_meta(df_attrs: dict[str, Any], id: str | Ref) -> dict[str, Any]:
    """Finds a column's metadata using a Ref or a Ref's str representation."""

    cols_meta = df_attrs["cols"]

    for col_meta in cols_meta:
        if str(col_meta["meta"]["id"]) == id or col_meta["meta"]["id"] == id:
            return col_meta
    raise NotFoundError(f"Unable to find meta for the Column titled '{id}'")


# -----------------------------------------------------------------------------
# Module internal util funcs
# -----------------------------------------------------------------------------


def _his_col_name_map(his_grid: Grid) -> dict[str, str]:
    # create a map of the grid col name to col dis
    col_name_map = {}
    for col in his_grid.cols:
        if col["name"] == "ts":
            col_name_map["ts"] = "Timestamp"
        else:
            # case when there is only one non-ts col
            if col["name"] == "val":
                id = his_grid.meta["id"]
            # case when there is more than one non-ts col
            elif "meta" in col.keys():
                id = col["meta"]["id"]

            # map the Haystack col name to the __str__ of the Ref it represents
            col_name_map[col["name"]] = str(id)

    return col_name_map


def _verify_no_duplicate_his_col_names(col_name_map: dict[str, str]) -> None:
    # verify there are no duplicate column display names
    col_dis_names = [col_name_map[i] for i in col_name_map]
    for col_dis_name in col_dis_names:
        count = col_dis_names.count(col_dis_name)
        if count != 1:
            raise DuplicateColNameError(
                f"Column display name is used {count} times when it may only "
                "be used once."
            )


def _parse_types(
    his_df: pd.DataFrame,
    grid_meta: dict[str, Any],
    grid_cols: list[dict[str, Any]],
) -> pd.DataFrame:
    """Replaces Haystack Numbers in his_df with native Pandas types.  Uses the
    DataFrame's attributes to determine which columns to apply the
    transformation to."""

    cols_meta = []
    for col_name in his_df.columns.to_list():
        if col_name == "ts":
            cols_meta.append({"name": "ts"})
            continue

        # case with Haystack single HisRead op
        if "id" in grid_meta.keys():
            if grid_cols[1].get("meta") is None:
                grid_col_meta = {}
            else:
                grid_col_meta = grid_cols[1]["meta"]
            ref_id = grid_meta["id"]
        # case with Haystack batch HisRead op
        else:
            grid_col_meta = _get_col_meta_by_name(grid_cols, col_name)
            ref_id = grid_col_meta["id"]

        # establish what kind each of the column values should be
        col_first_valid_index = his_df[col_name].first_valid_index()
        col_first_val = his_df[col_name].iloc[col_first_valid_index]

        col_meta = {"name": col_name, "meta": {"id": ref_id}}

        if isinstance(col_first_val, Number):
            his_df[col_name] = his_df[col_name].map(
                lambda x: _parse_col_data(x, col_first_val.unit)
            )
            col_meta["meta"]["kind"] = "Number"
            col_meta["meta"]["unit"] = col_first_val.unit

        col_meta["meta"] = col_meta["meta"] | grid_col_meta
        cols_meta.append(col_meta)

    his_df.attrs["cols"] = cols_meta

    return his_df


def _parse_col_data(x: Number | NA, expected_unit: str | None):
    if isinstance(x, Number):
        if not x.unit == expected_unit:
            raise UnitMismatchError(
                "One of the DataFrame columns has a Haystack Number with a"
                " unit that is different than what is defined in the column's"
                " metadata."
            )

    else:
        return pd.NA

    # Note:  we might want to be more explicit on the else condition (TBD)

    # if isinstance(x, NA):
    #     return pd.NA

    # elif math.isnan(x):
    #     return pd.NA

    return x.val


def _get_col_meta_by_name(cols: list[dict], col_name: str) -> dict[str, Any]:
    for col in cols:
        if col["name"] == col_name:
            return col["meta"]

    raise NotFoundError(
        f"Unable to find meta for the Column titled '{col_name}'"
    )
