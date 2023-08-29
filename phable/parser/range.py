from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class HisReadStartEndTypeError(Exception):
    help_msg: str


def to_haystack_range(
    start: date | datetime, end: date | datetime | None = None
) -> str:
    """Convert defined start and end to a range required for the Haystack
    HisRead op.

    Note: We need to check for type datetime before type date since a datetime
    object is also a date object.
    """
    if end is None:
        if isinstance(start, datetime):
            return to_haystack_datetime(start)
        elif isinstance(start, date):
            return to_haystack_date(start)
    else:
        _validate_start_and_end(start, end)

        if isinstance(start, datetime):
            return (
                to_haystack_datetime(start) + "," + to_haystack_datetime(end)
            )
        elif isinstance(start, date):
            return to_haystack_date(start) + "," + to_haystack_date(end)


def _validate_start_and_end(start, end) -> None:
    if not isinstance(start, date | datetime) or not isinstance(
        start, type(end)
    ):
        raise HisReadStartEndTypeError(
            "Range start and end must both either be of type"
            " date or datetime"
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
