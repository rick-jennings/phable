# flake8: noqa

from datetime import datetime
from typing import Any, Callable, Generator

import pytest

from phable import (
    CallError,
    Grid,
    HaxallClient,
    Marker,
    Number,
    Ref,
    UnknownRecError,
    open_haxall_client,
)
from phable.http import IncorrectHttpResponseStatus

from .test_haystack_client import client, create_kw_pt_rec_fn


@pytest.fixture
def sample_recs() -> list[dict[str, Any]]:
    data = [
        {"dis": "Rec1...", "testing": Marker()},
        {"dis": "Rec2...", "testing": Marker()},
    ]
    return data


@pytest.fixture(scope="module")
def create_pt_that_is_not_removed_fn(
    client: HaxallClient,
) -> Generator[Callable[[], dict[str, Any]], None, None]:
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number"}, {add}).commit"""
    )

    def _create_pt():
        response = client.eval(axon_expr)
        writable_kw_pt_rec = response.rows[0]
        return writable_kw_pt_rec

    yield _create_pt


def test_open_hx_client():
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    with open_haxall_client(uri, username, password) as hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "web-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

        auth_token = hc._auth_token

        pt_grid = hc.eval("""read(point)""")

        assert len(pt_grid.rows) == 1

    with pytest.raises(IncorrectHttpResponseStatus) as incorrectHttpResponseStatus:
        HaxallClient._create(uri, auth_token).about()

    assert incorrectHttpResponseStatus.value.actual_status == 403


def test_commit_add_one_rec(client: HaxallClient, sample_recs: list[dict, Any]):
    data = sample_recs[0].copy()
    response = client.commit_add(data)

    actual_keys = list(response.rows[0].keys())
    actual_keys.sort()
    expected_keys = list(data.keys()) + ["id", "mod"]
    expected_keys.sort()

    assert actual_keys == expected_keys
    assert response.rows[0]["dis"] == data["dis"]
    assert response.rows[0]["testing"] == data["testing"]
    assert isinstance(response.rows[0]["id"], Ref)

    client.commit_remove(Grid.to_grid(response.rows))


def test_commit_add_multiple_recs(client: HaxallClient, sample_recs: list[dict, Any]):
    data = sample_recs.copy()
    response = client.commit_add(data)

    for row in response.rows:
        actual_keys = list(row.keys())
        actual_keys.sort()
        expected_keys = ["dis", "testing", "id", "mod"]
        expected_keys.sort()

        assert actual_keys == expected_keys
        assert row["testing"] == Marker()

    assert response.rows[0]["dis"] == sample_recs[0]["dis"]
    assert response.rows[1]["dis"] == sample_recs[1]["dis"]
    assert isinstance(response.rows[0]["id"], Ref)
    assert isinstance(response.rows[1]["id"], Ref)

    client.commit_remove(Grid.to_grid(response.rows))


def test_commit_add_multiple_recs_as_grid(
    client: HaxallClient, sample_recs: list[dict, Any]
):
    data = sample_recs.copy()
    response = client.commit_add(Grid.to_grid(data))

    for row in response.rows:
        actual_keys = list(row.keys())
        actual_keys.sort()
        expected_keys = ["dis", "testing", "id", "mod"]
        expected_keys.sort()

        assert actual_keys == expected_keys
        assert row["testing"] == Marker()

    assert response.rows[0]["dis"] == sample_recs[0]["dis"]
    assert response.rows[1]["dis"] == sample_recs[1]["dis"]
    assert isinstance(response.rows[0]["id"], Ref)
    assert isinstance(response.rows[1]["id"], Ref)

    client.commit_remove(Grid.to_grid(response.rows))


def test_commit_add_with_new_id_does_not_raise_error(
    client: HaxallClient, sample_recs: list[dict, Any]
):
    data = sample_recs[0].copy()
    data["id"] = Ref("2e006480-5896960d")

    response = client.commit_add(data)
    assert response.rows[0]["dis"] == sample_recs[0]["dis"]
    assert isinstance(response.rows[0]["id"], Ref)

    client.commit_remove(response)


def test_commit_add_with_existing_id_raises_error(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]],
    client: HaxallClient,
):
    pt_rec = create_kw_pt_rec_fn()
    with pytest.raises(CallError):
        client.commit_add(pt_rec)


def test_commit_update_with_one_rec(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
):
    pt_rec = create_kw_pt_rec_fn()
    rec_sent = pt_rec.copy()
    rec_sent["newTag"] = Marker()

    recs_recv = client.commit_update(rec_sent).rows

    assert_commit_update_recs_sent_and_recv_match([rec_sent], recs_recv)


def test_commit_update_with_multiple_recs(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
):
    pt_rec1 = create_kw_pt_rec_fn()
    pt_rec2 = create_kw_pt_rec_fn()

    rec_sent1 = pt_rec1.copy()
    rec_sent1["newTag"] = Marker()

    rec_sent2 = pt_rec2.copy()
    rec_sent2["newTag"] = Marker()

    recs_sent = [rec_sent1, rec_sent2]
    response = client.commit_update(recs_sent)

    assert_commit_update_recs_sent_and_recv_match(recs_sent, response.rows)


def test_commit_update_with_multiple_recs_as_grid(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
):
    pt_rec1 = create_kw_pt_rec_fn()
    pt_rec2 = create_kw_pt_rec_fn()

    rec_sent1 = pt_rec1.copy()
    rec_sent1["newTag"] = Marker()

    rec_sent2 = pt_rec2.copy()
    rec_sent2["newTag"] = Marker()

    recs_sent = Grid.to_grid([rec_sent1, rec_sent2])
    response = client.commit_update(recs_sent)

    assert_commit_update_recs_sent_and_recv_match(recs_sent.rows, response.rows)


def assert_commit_update_recs_sent_and_recv_match(
    recs_sent: list[dict[str, Any]], recs_recv: list[dict[str, Any]]
) -> None:
    for count, rec_sent in enumerate(recs_sent):
        rec_recv = recs_recv[count]
        for key in rec_recv.keys():
            if key == "mod":
                assert rec_recv[key] > rec_sent[key]
            elif key == "writeLevel":
                assert rec_recv[key] == Number(17)
            else:
                assert rec_sent[key] == rec_recv[key]


def test_commit_update_recs_with_only_id_and_mod_tags_sent(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
) -> None:
    pt_rec1 = create_kw_pt_rec_fn()
    new_pt_rec1 = pt_rec1.copy()

    pt_rec2 = create_kw_pt_rec_fn()
    new_pt_rec2 = pt_rec2.copy()

    rec_sent1 = {"id": pt_rec1["id"], "mod": pt_rec1["mod"]}
    rec_sent2 = {"id": pt_rec2["id"], "mod": pt_rec2["mod"]}

    recs_sent = [rec_sent1, rec_sent2]
    response = client.commit_update(recs_sent)

    assert_commit_update_recs_sent_and_recv_match(
        [new_pt_rec1, new_pt_rec2], response.rows
    )


def test_commit_update_recs_with_only_id_mod_and_new_tag_sent(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
) -> None:
    pt_rec1 = create_kw_pt_rec_fn()
    new_pt_rec1 = pt_rec1.copy()
    new_pt_rec1["newTag"] = Marker()

    pt_rec2 = create_kw_pt_rec_fn()
    new_pt_rec2 = pt_rec2.copy()
    new_pt_rec2["newTag"] = Marker()

    rec_sent1 = {"id": pt_rec1["id"], "mod": pt_rec1["mod"], "newTag": Marker()}
    rec_sent2 = {"id": pt_rec2["id"], "mod": pt_rec2["mod"], "newTag": Marker()}

    recs_sent = [rec_sent1, rec_sent2]
    response = client.commit_update(recs_sent)

    assert_commit_update_recs_sent_and_recv_match(
        [new_pt_rec1, new_pt_rec2], response.rows
    )


def test_commit_remove_with_only_id_rec_tags(
    create_kw_pt_rec_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec1 = create_kw_pt_rec_fn()
    pt_rec2 = create_kw_pt_rec_fn()

    with pytest.raises(CallError):
        response = client.commit_remove(
            Grid.to_grid(
                [
                    {"id": pt_rec1["id"]},
                    {"id": pt_rec2["id"]},
                ]
            )
        )


def test_commit_remove_with_id_and_mod_rec_tags(
    create_pt_that_is_not_removed_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec1 = create_pt_that_is_not_removed_fn()
    pt_rec2 = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(
        [
            {"id": pt_rec1["id"], "mod": pt_rec1["mod"]},
            {"id": pt_rec2["id"], "mod": pt_rec2["mod"]},
        ]
    )

    # verify it returns an empty Grid
    assert response.rows == []
    assert response.cols == [{"name": "empty"}]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_with_id_and_mod_rec_tags_as_grid(
    create_pt_that_is_not_removed_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec1 = create_pt_that_is_not_removed_fn()
    pt_rec2 = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(
        Grid.to_grid(
            [
                {"id": pt_rec1["id"], "mod": pt_rec1["mod"]},
                {"id": pt_rec2["id"], "mod": pt_rec2["mod"]},
            ]
        )
    )

    # verify it returns an empty Grid
    assert response.rows == []
    assert response.cols == [{"name": "empty"}]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_one_rec(
    create_pt_that_is_not_removed_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(pt_rec)

    assert response.rows == []
    assert response.cols == [{"name": "empty"}]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec["id"])


def test_commit_remove_with_all_rec_tags(
    create_pt_that_is_not_removed_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec1 = create_pt_that_is_not_removed_fn()
    pt_rec2 = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(Grid.to_grid([pt_rec1, pt_rec2]))

    # verify it returns an empty Grid
    assert response.rows == []
    assert response.cols == [{"name": "empty"}]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_with_non_existing_rec(
    create_kw_pt_rec_fn: Callable[[], Ref], client: HaxallClient
):
    pt_rec_mod1 = create_kw_pt_rec_fn()
    pt_rec_mod2 = create_kw_pt_rec_fn()["mod"]
    sent_recs = [
        {"id": pt_rec_mod1["id"], "mod": pt_rec_mod1["mod"]},
        {"id": Ref("dog"), "mod": pt_rec_mod2},
    ]

    with pytest.raises(CallError):
        client.commit_remove(sent_recs)


def test_eval(client: HaxallClient):
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number"}, {add}).commit"""
    )

    response = client.eval(axon_expr)
    assert "id" in response.rows[0].keys()
    assert "mod" in response.rows[0].keys()
