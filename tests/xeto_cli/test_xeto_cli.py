from typing import Any, Generator

import pytest

from phable import Grid, GridCol, Marker, Ref, XetoCLI


@pytest.fixture(params=["json", "zinc"], scope="module")
def io_format(request) -> Generator[str, None, None]:
    yield request.param


@pytest.fixture(
    params=[
        XetoCLI(
            io_format="json",
        ),
        XetoCLI(
            io_format="zinc",
        ),
        XetoCLI(
            docker_cli=True,
            io_format="json",
        ),
        XetoCLI(
            docker_cli=True,
            io_format="zinc",
        ),
    ],
    scope="module",
)
def xeto_cli(request) -> Generator[XetoCLI, None, None]:
    yield request.param


SITE = {"id": Ref("site"), "site": Marker(), "spec": Ref("ph::Site")}
SITEMETER = {
    "id": Ref("site-meter"),
    "elec": Marker(),
    "meter": Marker(),
    "siteMeter": Marker(),
    "equip": Marker(),
    "spec": Ref("phable.test::ElecSiteMeter"),
    "siteRef": Ref("site"),
}

SITEMETER_PT = {
    "id": Ref("site-meter-point"),
    "point": Marker(),
    "spec": Ref("ph.points::ElecAcTotalImportActiveDemandSensor"),
    "kind": "Number",
    "unit": "kW",
    "elec": Marker(),
    "ac": Marker(),
    "total": Marker(),
    "import": Marker(),
    "active": Marker(),
    "demand": Marker(),
    "sensor": Marker(),
    "equipRef": Ref("site-meter"),
}

SUBMETER1 = {
    "id": Ref("submeter1"),
    "elec": Marker(),
    "meter": Marker(),
    "subMeter": Marker(),
    "subMeterOf": Ref("site-meter"),
    "equip": Marker(),
    "spec": Ref("phable.test::ElecSubMeter"),
    "siteRef": Ref("site"),
}
SUBMETER1_PT = {
    "id": Ref("submeter1-point"),
    "point": Marker(),
    "spec": Ref("ph.points::ElecAcTotalImportActiveDemandSensor"),
    "kind": "Number",
    "unit": "kW",
    "elec": Marker(),
    "ac": Marker(),
    "total": Marker(),
    "import": Marker(),
    "active": Marker(),
    "demand": Marker(),
    "sensor": Marker(),
    "equipRef": Ref("submeter1"),
}

SUBMETER2 = {
    "id": Ref("submeter2"),
    "elec": Marker(),
    "meter": Marker(),
    "subMeter": Marker(),
    "subMeterOf": Ref("site-meter"),
    "equip": Marker(),
    "spec": Ref("phable.test::ElecSubMeter"),
    "siteRef": Ref("site"),
}

SUBMETER2_PT = {
    "id": Ref("submeter2-point"),
    "point": Marker(),
    "spec": Ref("ph.points::ElecAcTotalImportActiveDemandSensor"),
    "kind": "Number",
    "unit": "kW",
    "elec": Marker(),
    "ac": Marker(),
    "total": Marker(),
    "import": Marker(),
    "active": Marker(),
    "demand": Marker(),
    "sensor": Marker(),
    "equipRef": Ref("submeter2"),
}


@pytest.mark.parametrize(
    "recs,expected",
    [
        (
            [
                SITE,
                SITEMETER,
                SITEMETER_PT,
                SUBMETER1,
                SUBMETER1_PT,
                SUBMETER2,
                SUBMETER2_PT,
            ],
            Grid(
                {"ver": "3.0"},
                [GridCol("id"), GridCol("msg")],
                [],
            ),
        ),
        (
            [
                SITE,
                SITEMETER,
                # SITEMETER_PT,
                SUBMETER1,
                SUBMETER1_PT,
                SUBMETER2,
                SUBMETER2_PT,
            ],
            Grid(
                {"ver": "3.0"},
                [GridCol("id"), GridCol("msg")],
                [
                    {
                        "id": Ref("site-meter"),
                        "msg": "Slot 'points': Missing required Point: ph.points::ElecAcTotalImportActiveDemandSensor",
                    }
                ],
            ),
        ),
        (
            [
                SITE,
                SITEMETER,
                {k: v for k, v in SITEMETER_PT.items() if k != "demand"},
                SUBMETER1,
                SUBMETER1_PT,
                SUBMETER2,
                SUBMETER2_PT,
            ],
            Grid(
                {"ver": "3.0"},
                [GridCol("id"), GridCol("msg")],
                [
                    {
                        "id": Ref("site-meter"),
                        "msg": "Slot 'points': Missing required Point: ph.points::ElecAcTotalImportActiveDemandSensor",
                    },
                    {
                        "id": Ref("site-meter-point"),
                        "msg": "Slot 'demand': Missing required marker",
                    },
                ],
            ),
        ),
    ],
)
def test_fits_explain(
    recs: list[dict[str, Any]], expected: Grid, xeto_cli: XetoCLI
) -> None:
    assert xeto_cli.fits_explain(recs) == expected
