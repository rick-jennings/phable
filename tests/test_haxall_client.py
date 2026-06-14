# flake8: noqa

from datetime import datetime
from typing import Any, Callable, Generator, Sequence, Mapping
from urllib.error import HTTPError
import pytest

from phable import (
    CallError,
    Grid,
    GridCol,
    HaxallClient,
    Marker,
    Number,
    Ref,
    UnknownRecError,
    open_haxall_client,
)

@pytest.mark.order(0)
def test_configure_proj(client: HaxallClient):
    data = [
        {
            "id": Ref("ph-001"),
            "site": Marker(),
            "pytest": Marker(),
            "dis": "Carytown",
            "geoState": "VA",
        },
        {
            "id": Ref("ph-002"),
            "siteRef": Ref("ph-001"),
            "equip": Marker(),
            "pytest": Marker(),
            "siteMeter": Marker(),
            "dis": "Elec-Meter-01",
            "elec": Marker(),
            "meter": Marker(),
        },
        {
            "id": Ref("ph-003"),
            "siteRef": Ref("ph-001"),
            "equipRef": Ref("ph-002"),
            "point": Marker(),
            "pytest": Marker(),
            "his": Marker(),
            "demand": Marker(),
            "navName": "kW",
            "kind": "Number",
            "unit": "kW",
            "tz": "New_York",
        },
        {
            "id": Ref("ph-004"),
            "siteRef": Ref("ph-001"),
            "equipRef": Ref("ph-002"),
            "point": Marker(),
            "pytest": Marker(),
            "his": Marker(),
            "demand": Marker(),
            "navName": "kW",
            "kind": "Number",
            "unit": "kW",
            "tz": "New_York",
        },
    ]

    try:
        client.commit_add(data)
    except CallError as e:
        if "Rec already exists" in e.help_msg.meta["errTrace"]:
            print("Previous test records still in database, clearing then re-adding")
            clear_test_data(client)
            client.commit_add(data)
        else:
            raise e

    try:
        client.eval('libAdd("hx.point")')
    except CallError as e:
        if "Lib already enabled: hx.point" not in e.help_msg.meta["errTrace"]:
            raise e

    point_count = client.eval("readCount(point and pytest)").rows[0]["val"].val
    equip_count = client.eval("readCount(equip and pytest)").rows[0]["val"].val
    site_count = client.eval("readCount(site and pytest)").rows[0]["val"].val

    if point_count != 2:
        raise ValueError(
            f"Unexpected number of pytest points in database. {point_count} != 2"
        )
    if equip_count != 1:
        raise ValueError(
            f"Unexpected number of pytest equips in database. {equip_count} != 1"
        )
    if site_count != 1:
        raise ValueError(
            f"Unexpected number of pytest sites in database. {site_count} != 1"
        )


@pytest.mark.order(-1)
def test_teardown_proj(client: HaxallClient):
    clear_test_data(client)

    point_count = client.eval("readCount(point and pytest)").rows[0]["val"].val
    equip_count = client.eval("readCount(equip and pytest)").rows[0]["val"].val
    site_count = client.eval("readCount(site and pytest)").rows[0]["val"].val

    if point_count != 0:
        raise ValueError(
            f"Unexpected number of pytest points in database. {point_count} != 0"
        )
    if equip_count != 0:
        raise ValueError(
            f"Unexpected number of pytest equips in database. {equip_count} != 0"
        )
    if site_count != 0:
        raise ValueError(
            f"Unexpected number of pytest sites in database. {site_count} != 0"
        )


def clear_test_data(client: HaxallClient):
    client.eval(
        "readAll((site or point or equip) and pytest).toRecList.map(r=> diff(r, null, {remove})).commit()"
    )


def test_about_op_with_trailing_uri_slash(URI: str, USERNAME: str, PASSWORD: str):
    client = HaxallClient.open(URI + "/", USERNAME, PASSWORD)
    assert client.about()["vendorName"] == "SkyFoundry"
    client.close()


def test_about_op_with_trailing_uri_slash_using_context(
    URI: str, USERNAME: str, PASSWORD: str
):
    with open_haxall_client(URI + "/", USERNAME, PASSWORD) as client:
        assert client.about()["vendorName"] == "SkyFoundry"


def test_open_hx_client(URI: str, USERNAME: str, PASSWORD: str):
    with open_haxall_client(URI, USERNAME, PASSWORD) as hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "s-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

        auth_token = hc._auth_token

        pt_grid = hc.eval("""read(point)""")

        assert len(pt_grid.rows) == 1

    with pytest.raises(HTTPError) as e:
        HaxallClient(URI, auth_token).about()

    assert e.value.status == 403


def test_commit_add_one_rec(client: HaxallClient, sample_recs: list[dict[str, Any]]):
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


def test_commit_add_multiple_recs(
    client: HaxallClient, sample_recs: list[dict[str, Any]]
):
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
    client: HaxallClient, sample_recs: list[dict[str, Any]]
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
    client: HaxallClient, sample_recs: list[dict[str, Any]]
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
    recs_sent: Sequence[Mapping[str, Any]], recs_recv: Sequence[Mapping[str, Any]]
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

    rec_sent1 = {
        "id": pt_rec1["id"],
        "mod": pt_rec1["mod"],
        "newTag": Marker(),
    }
    rec_sent2 = {
        "id": pt_rec2["id"],
        "mod": pt_rec2["mod"],
        "newTag": Marker(),
    }

    recs_sent = [rec_sent1, rec_sent2]
    response = client.commit_update(recs_sent)

    assert_commit_update_recs_sent_and_recv_match(
        [new_pt_rec1, new_pt_rec2], response.rows
    )


def test_commit_remove_with_only_id_rec_tags(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
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
    create_pt_that_is_not_removed_fn: Callable[[], dict[str, Any]], client: HaxallClient
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
    assert response.cols == [GridCol("empty")]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_with_id_and_mod_rec_tags_as_grid(
    create_pt_that_is_not_removed_fn: Callable[[], dict[str, Any]], client: HaxallClient
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
    assert response.cols == [GridCol("empty")]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_one_rec(
    create_pt_that_is_not_removed_fn: Callable[[], dict[str, Any]], client: HaxallClient
):
    pt_rec = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(pt_rec)

    assert response.rows == []
    assert response.cols == [GridCol("empty")]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec["id"])


def test_commit_remove_with_all_rec_tags(
    create_pt_that_is_not_removed_fn: Callable[[], dict[str, Any]], client: HaxallClient
):
    pt_rec1 = create_pt_that_is_not_removed_fn()
    pt_rec2 = create_pt_that_is_not_removed_fn()

    response = client.commit_remove(Grid.to_grid([pt_rec1, pt_rec2]))

    # verify it returns an empty Grid
    assert response.rows == []
    assert response.cols == [GridCol("empty")]
    assert response.meta == {"ver": "3.0"}

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec1["id"])

    with pytest.raises(UnknownRecError):
        client.read_by_id(pt_rec2["id"])


def test_commit_remove_with_non_existing_rec(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaxallClient
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


# -----------------------------------------------------------------------------
# file op tests -- currently ignored until added to Haxall
# -----------------------------------------------------------------------------

# def test_file_post_using_txt_file(client: HaxallClient):
#     remote_file_uri = "/proj/demo/io/phable-file-test.txt"
#     with open("tests/phable-file-test.txt", "rb") as file:
#         res_data = client.file_post(file, remote_file_uri)

#     assert ".txt" in str(res_data["uri"])
#     assert remote_file_uri.replace(".txt", "") in str(res_data["uri"])


# def test_file_put_using_txt_file(client: HaxallClient):
#     remote_file_uri = "/proj/demo/io/phable-file-test.txt"

#     with open("tests/phable-file-test.txt", "rb") as file:
#         res_data = client.file_put(file, remote_file_uri)

#     assert remote_file_uri == str(res_data["uri"])


# def test_file_get_using_txt_file(client: HaxallClient):
#     stream = client.file_get("/proj/demo/io/phable-file-test.txt")
#     data = stream.read()
#     stream.close()

#     assert data == b"Hello World!\n"
