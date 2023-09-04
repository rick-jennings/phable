import pandas as pd

from phable.kinds import Grid, Number

# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------


class HaystackHisGridUnitMismatchError(Exception):
    help_msg: str


class DataFrameColumnDisplayHasInvalidUnitError(Exception):
    help_msg: str


# -----------------------------------------------------------------------------
# Primary func
# -----------------------------------------------------------------------------


def to_pandas(grid: Grid) -> pd.DataFrame:
    if _is_his_grid(grid):
        df = _his_grid_to_pandas(grid)
    else:
        df = pd.DataFrame(data=grid.rows)

    df.attrs["meta"] = grid.meta.copy()
    df.attrs["cols"] = grid.cols.copy()

    return df


# -----------------------------------------------------------------------------
# Util funcs
# -----------------------------------------------------------------------------


def _is_his_grid(grid: Grid) -> bool:
    """Returns True if grid is a history grid and False if it is not."""

    is_his_grid = False
    col_names = [col["name"] for col in grid.cols]
    if "ts" in col_names:
        if "val" in col_names or "v0" in col_names:
            is_his_grid = True

    return is_his_grid


def _his_grid_to_pandas(his_grid: Grid) -> pd.DataFrame:
    # map the grid col name to col dis
    col_name_map = {}
    for col in his_grid.cols:
        if col["name"] == "ts":
            col_name_map["ts"] = "Timestamp"
        else:
            if col["name"] == "val":
                ref = his_grid.meta["id"]
            elif "meta" in col.keys():
                ref = col["meta"]["id"]
            col_name_map[col["name"]] = ref.dis

    # in each of the data rows replace Number with just its val
    # TODO: figure out why if we hiswrite a Number without units the his_read
    # returns a non-Number data type
    col_unit_map = {}
    new_grid_rows = []
    for row in his_grid.rows:
        new_row = row.copy()
        for col_name in row.keys():
            if col_name == "ts":
                new_row[col_name] = row[col_name]
            else:
                if isinstance(row[col_name], Number):
                    new_row[col_name] = row[col_name].val
                    # new_row[col_name] = row[col_name]
                    if col_name not in col_unit_map.keys():
                        col_unit_map[col_name] = row[col_name].unit
                    else:
                        if row[col_name].unit != col_unit_map[col_name]:
                            raise HaystackHisGridUnitMismatchError(
                                f"The column {col_name} does not have the same"
                                " unit in each of its his_grid rows.  This is "
                                "required to convert a his_grid to a Pandas "
                                "DataFrame."
                            )
                else:
                    new_row[col_name] = row[col_name]
        new_grid_rows.append(new_row)

    # verify that the col dis names have the expected units
    for col_name in col_unit_map.keys():
        if col_unit_map[col_name] is None:
            continue
        elif col_unit_map[col_name] != col_name_map[col_name].split(" ")[-1]:
            raise DataFrameColumnDisplayHasInvalidUnitError(
                "The column display name must specify the unit for the data"
                " values in the rows that it represents, unless the values "
                "have no units.  This must be such that splitting the column"
                " display name by ' ' and fetching the value at the last index"
                " of the returned list results in the expected unit."
            )

    df = pd.DataFrame(data=new_grid_rows)
    df = df.rename(columns=col_name_map)
    df = df.set_index("Timestamp")

    return df
