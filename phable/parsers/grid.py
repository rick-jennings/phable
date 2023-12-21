"""This module is just a placeholder for now.  Probably we will relocate this
code in the future.
"""


from dataclasses import dataclass
from typing import Any

from phable.kinds import Grid, Ref


@dataclass
class ColIdMismatchError(Exception):
    help_msg: str


@dataclass
class ColNumMismatchError(Exception):
    help_msg: str


def merge_pt_data_to_his_grid_cols(
    his_grid: Grid, pt_data: Grid
) -> list[dict]:
    """Merges point ID data into a history grid's column metadata.  If the
    history grid's column metadata has the same key as the point ID's data,
    then the point ID's key value takes precedence."""

    _validate_num_of_cols(his_grid.cols, pt_data.rows)

    new_his_grid_cols = [{"name": "ts"}]

    for his_col in his_grid.cols[1:]:
        his_col_id = _find_his_col_id(his_col, his_grid.meta)
        pt_row = _find_pt_row_by_id(pt_data, his_col_id)

        new_his_grid_cols.append(
            {
                "name": his_col["name"],
                "meta": _find_new_col_meta(his_col, pt_row),
            }
        )

    return new_his_grid_cols


def _validate_num_of_cols(
    his_grid_cols: list[dict], pt_data_rows: list[dict]
) -> None:
    num_his_cols = len(his_grid_cols) - 1
    num_pt_rows = len(pt_data_rows)
    if num_his_cols != num_pt_rows:
        raise ColNumMismatchError(
            f"Expected {num_pt_rows} row(s) in the Point Data Grid "
            f"and {num_his_cols} non-ts col(s) in the His Grid"
        )


def _find_his_col_id(his_col: dict, his_meta: dict) -> Ref:
    # handle case there is only one non ts col
    if his_col.get("meta") is None or his_col["meta"].get("id") is None:
        his_col_id = his_meta["id"]
    else:
        his_col_id = his_col["meta"]["id"]
    return his_col_id


def _find_new_col_meta(his_col: dict, pt_row: dict) -> dict:
    if his_col.get("meta") is not None:
        new_col_meta = his_col["meta"] | pt_row
    elif his_col.get("meta") is None:
        new_col_meta = pt_row
    return new_col_meta


def _find_pt_row_by_id(pt_data: Grid, row_id: Ref) -> dict[Any]:
    for row in pt_data.rows:
        if row["id"] == row_id:
            return row
    raise ColIdMismatchError(
        f"Unable to locate {row_id} in the Point Data Grid"
    )
