"""Project Haystack does not strictly follow timezone conventions per the IANA
database.  The purpose of this module is to allow simple and robust conversion
between IANA and Project Haystack defined timezones.

Reference:
https://project-haystack.org/doc/docHaystack/TimeZones#zoneinfo
"""

from importlib.resources import as_file, files


# Note: In the future we might want to consider how to minimize the number of
# times haystack_iana_tz_map() gets executed.
def haystack_iana_tz_map() -> list[tuple[str, str]]:
    """Create a map between Project Haystack and IANA timezones.

    Each element of the returned list is a single map where the first element
    is the Project Haystack timezone and the second element is the IANA
    timezone.
    """
    source = files("phable.parser").joinpath("tz.txt")

    tz_map = []
    with as_file(source) as file_path:
        with open(file_path, "r") as file:
            for line in file:
                line_split = line.replace("\n", "").split(",")
                tz_map.append((line_split[0], line_split[1]))

    return tz_map


def find_iana_tz(haystack_tz: str) -> str:
    """Find the IANA timezone given a Project Haystack timezone"""
    for pair in haystack_iana_tz_map():
        if pair[0] == haystack_tz:
            return pair[1]


def find_haystack_tz(iana_tz: str) -> str:
    """Find the Project Haystack timezone given an IANA timezone"""
    for pair in haystack_iana_tz_map():
        if pair[1] == iana_tz:
            return pair[0]
