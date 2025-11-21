from types import MappingProxyType
from typing import Any, Generator

import pytest

from phable import Grid, Marker, Ref

from .fits_explain import fits_explain


@pytest.fixture(params=["json", "zinc"], scope="module")
def io_format(request) -> Generator[str, None, None]:
    yield request.param


SITE = MappingProxyType({"id": Ref("site"), "site": Marker(), "spec": Ref("ph::Site")})
SITEMETER = MappingProxyType(
    {
        "id": Ref("site-meter"),
        "elec": Marker(),
        "meter": Marker(),
        "siteMeter": Marker(),
        "equip": Marker(),
        "spec": Ref("phable::ElecSiteMeter"),
        "siteRef": Ref("site"),
    }
)
SITEMETER_PT = MappingProxyType(
    {
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
)
SUBMETER1 = MappingProxyType(
    {
        "id": Ref("submeter1"),
        "elec": Marker(),
        "meter": Marker(),
        "subMeter": Marker(),
        "subMeterOf": Ref("site-meter"),
        "equip": Marker(),
        "spec": Ref("phable::ElecSubMeter"),
        "siteRef": Ref("site"),
    }
)
SUBMETER1_PT = MappingProxyType(
    {
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
)
SUBMETER2 = MappingProxyType(
    {
        "id": Ref("submeter2"),
        "elec": Marker(),
        "meter": Marker(),
        "subMeter": Marker(),
        "subMeterOf": Ref("site-meter"),
        "equip": Marker(),
        "spec": Ref("phable::ElecSubMeter"),
        "siteRef": Ref("site"),
    }
)
SUBMETER2_PT = MappingProxyType(
    {
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
)


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
                [{"name": "id"}, {"name": "msg"}],
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
                [{"name": "id"}, {"name": "msg"}],
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
                MappingProxyType(
                    {k: v for k, v in SITEMETER_PT.items() if k != "demand"}
                ),
                SUBMETER1,
                SUBMETER1_PT,
                SUBMETER2,
                SUBMETER2_PT,
            ],
            Grid(
                {"ver": "3.0"},
                [{"name": "id"}, {"name": "msg"}],
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
def test_fits_explain(recs: dict[str, Any], expected: Grid, io_format: str) -> None:
    recs = [dict(x) for x in recs]
    x = fits_explain(recs, io_format=io_format)

    assert x == expected
