from __future__ import annotations

from functools import lru_cache
from zoneinfo import ZoneInfo, available_timezones


def _tz_iana_to_haystack(iana_tz: str) -> str:
    if iana_tz == "UTC":
        return "UTC"
    return iana_tz.split("/")[-1]


@lru_cache(maxsize=16)
def _haystack_to_iana_tz(haystack_tz: str) -> ZoneInfo:
    if haystack_tz == "UTC":
        return ZoneInfo("UTC")
    for iana_tz in available_timezones():
        if haystack_tz == _tz_iana_to_haystack(iana_tz):
            return ZoneInfo(iana_tz)

    raise ValueError(
        f"Unable to locate the city {haystack_tz} in the IANA database.  "
        + "Consider installing the tzdata package."
    )
