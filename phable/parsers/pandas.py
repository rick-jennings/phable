from typing import Any

import pandas as pd

from phable.kinds import Grid, Number, Ref


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

    return df


def his_grid_to_pandas(his_grid: Grid, pt_data: pd.DataFrame) -> pd.DataFrame:
    # create a map of HisGrid col names to their point display names
    his_col_name_map = _his_col_name_map(his_grid)

    # verify there are no duplicate column display names
    _verify_no_duplicate_his_col_names(his_col_name_map)

    # create Pandas DataFrame
    df = pd.DataFrame(his_grid.rows)
    df = df.rename(columns=his_col_name_map)
    df = df.set_index("Timestamp")

    # add meta to DataFrame
    df.attrs["meta"] = his_grid.meta
    df.attrs["cols"] = _find_his_col_meta(pt_data, his_col_name_map)

    # validate and parse types
    df = _parse_types(df)

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


def _find_his_col_meta(
    pt_data: pd.DataFrame, his_col_name_map: dict[str, str]
) -> list[dict[str, Any]]:
    """Returns a dictionary that has Ref value keys"""

    # Note: his_col_name_map has unique dict values

    cols_meta = []

    # at minimum capture col id and kind
    # if applicable capture unit
    for row in pt_data.iterrows():
        row_data = row[1].to_dict()

        pt_id: Ref = row_data["id"]

        for key, val in his_col_name_map.items():
            if val == str(pt_id):
                grid_col_name = key

        pt_meta = {"name": grid_col_name, "meta": {}}

        pt_meta["meta"]["id"] = pt_id
        kind = row_data["kind"]
        pt_meta["meta"]["kind"] = kind

        if kind == "Number":
            if "unit" in row_data.keys():
                pt_meta["meta"]["unit"] = row_data["unit"]

        cols_meta.append(pt_meta)

    return cols_meta


def _parse_types(his_df: pd.DataFrame) -> pd.DataFrame:
    """Replaces Haystack Numbers in his_df with native Pandas types.  Uses the
    DataFrame's attributes to determine which columns to apply the
    transformation to."""

    df_attrs = his_df.attrs

    for col_name in his_df.columns.to_list():
        col_meta = get_col_meta(df_attrs, col_name)
        col_kind = col_meta["meta"]["kind"]

        if col_kind == "Number":
            col_unit = col_meta["meta"].get(
                "unit"
            )  # a Number's unit can be None
            his_df[col_name] = his_df[col_name].map(
                lambda x: _parse_number(x, col_unit)
            )

    return his_df


def _parse_number(x, expected_unit: str | None):
    # the DataFrame may be sparse involving non Number types
    if isinstance(x, Number):
        if not x.unit == expected_unit:
            raise UnitMismatchError(
                "One of the DataFrame columns has a Haystack Number with a"
                " unit that is different than what is defined in the column's"
                " metadata."
            )
        return x.val
    return x
