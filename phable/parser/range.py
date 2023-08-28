from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class HisReadRangeSliceError(Exception):
    help_msg: str


def to_haystack_range(range: str | date | datetime | slice) -> str:
    """Convert range to a Python type str required for the Haystack HisRead op.

    Note: We need to check for type datetime before type date since a datetime
    object is also a date object.
    """
    if isinstance(range, str):
        return range
    elif isinstance(range, datetime):
        return to_haystack_datetime(range)
    elif isinstance(range, date):
        return to_haystack_date(range)
    elif isinstance(range, slice):
        _validate_slice(range)

        if isinstance(range.start, datetime):
            return (
                to_haystack_datetime(range.start)
                + ","
                + to_haystack_datetime(range.stop)
            )
        elif isinstance(range.start, date):
            return (
                to_haystack_date(range.start)
                + ","
                + to_haystack_date(range.stop)
            )


def _validate_slice(range: slice) -> None:
    if not isinstance(range.start, date | datetime) or not isinstance(
        range.start, type(range.stop)
    ):
        raise HisReadRangeSliceError(
            "slice[start] and slice[stop] must both either be of type"
            " date or datetime"
        )
    if range.step is not None:
        raise HisReadRangeSliceError(
            "Range cannot be a slice object with a step"
        )


def to_haystack_date(x: date) -> str:
    return x.isoformat()


def to_haystack_datetime(x: datetime) -> str:
    iana_tz = str(x.tzinfo)
    if "/" in iana_tz:
        haystack_tz = iana_tz.split("/")[-1]
    else:
        haystack_tz = iana_tz

    if x.microsecond == 0:
        dt = x.isoformat(timespec="seconds")
    else:
        dt = x.isoformat(timespec="milliseconds")

    return f"{dt} {haystack_tz}"
