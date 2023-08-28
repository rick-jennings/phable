from phable.parser.tz import find_haystack_tz, find_iana_tz


def test_find_iana_tz() -> None:
    assert find_iana_tz("New_York") == "America/New_York"


def test_find_haystack_tz() -> None:
    assert find_haystack_tz("America/New_York") == "New_York"
