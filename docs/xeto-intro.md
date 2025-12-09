# Xeto Introduction

This tutorial demonstrates how to create a Xeto library and use it with Python to validate Haystack instance data. You'll learn to define custom Xeto specs for electric meters and verify data conformance using the phable package.

## Prerequisites

- Install the latest [haxall build](https://github.com/Project-Haystack/xeto/blob/master/src/xeto/doc.xeto.tools/Setup.md) for Java.
- Ensure the Haxall `bin` directory is in the `PATH` environment variable.
- Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

## Project Setup

In the terminal, navigate to the directory where the new webinar project will be created.

### Create Python Project

Create a Python project called `webinar` using uv, add the `phable` package dependency and sync dependencies:

```shell
uv init webinar && cd webinar
uv add phable
uv sync
```

### Create Xeto Library

Create a Xeto library called `webinar`:

```shell
mkdir src && cd src
mkdir xeto && cd xeto
xeto init -dir . -noconfirm webinar
cd ../../
```

### Open in VS Code

Open VS Code Editor:

```shell
code .
```

If applicable, install and enable the [Xeto IDE Extension](https://marketplace.visualstudio.com/items?itemName=xeto.xeto-vscode-extension) for VS Code Editor by XetoBase.

## Define Xeto Specs

In this example, we'll model a common electrical metering scenario: a site with one main electric meter and two submeters. The site meter measures total electrical demand for the facility, while the submeters track demand for specific areas or systems within the site. By establishing the relationships between these meters and their points, applications can perform useful calculations—such as computing total site demand from submeter readings or identifying discrepancies between site and submeter totals.

### Create Electric Meter Specs

Replace the contents of `specs.xeto` with the below text to define Xeto specs for an electric site meter and submeters:

```xeto
ElecSiteMeter : ElecMeter {
  siteMeter
  subMeters: Query<of:ElecSubMeter, inverse:"phable::ElecSubMeter.mySiteMeter">
  points: {
    ElecAcTotalImportActiveDemandSensor
  }
}

ElecSubMeter : ElecMeter {
  subMeter
  subMeterOf: Ref
  mySiteMeter: Query<of:ElecSiteMeter, via:"subMeterOf+">
  points: {
    ElecAcTotalImportActiveDemandSensor
  }
}
```

### Configure Library Metadata

Create the Xeto library configuration by replacing the contents of `lib.xeto` with the below text. This defines the library metadata (name, description, version) and declares dependencies on other Xeto libraries that our specs will reference:

```xeto
pragma: Lib <
  doc: "Xeto Example for Haystack 5 and Xeto for Python Developers Webinar"
  version: "0.0.1"
  depends: {
    { lib: "sys" }
    { lib: "ph" }
    { lib: "ph.points" }
    { lib: "ph.equips" }
  }
  org: {
    dis: "Project Haystack"
    uri: "https://project-haystack.org/"
  }
>
```

### Build the Xeto Library

Verify `webinar` successfully compiles as a xetolib in the `lib` directory:

```shell
xeto build -allIn .
```

> **Note:** This command should be run every time changes are made to xeto files.

## Create Python Validation Script

Replace the contents of `hello.py` with the below Python code. This script creates Haystack instance data representing a site, one site meter with its demand point, and two submeters (each with its own demand point). The `subMeterOf` references establish the meter hierarchy, enabling applications to navigate these relationships. For example, applications can sum submeter demands or compare them against the site meter reading. The script validates this data structure against the Xeto specs we defined:

```python
from phable import Grid, Marker, Ref, XetoCLI


def main():
    recs = [
        {"id": Ref("site"), "site": Marker(), "spec": Ref("ph::Site")},
        {
            "id": Ref("site-meter"),
            "elec": Marker(),
            "meter": Marker(),
            "siteMeter": Marker(),
            "equip": Marker(),
            "spec": Ref("webinar::ElecSiteMeter"),
            "siteRef": Ref("site"),
        },
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
        },
        {
            "id": Ref("submeter1"),
            "elec": Marker(),
            "meter": Marker(),
            "subMeter": Marker(),
            "subMeterOf": Ref("site-meter"),
            "equip": Marker(),
            "spec": Ref("webinar::ElecSubMeter"),
            "siteRef": Ref("site"),
        },
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
        },
        {
            "id": Ref("submeter2"),
            "elec": Marker(),
            "meter": Marker(),
            "subMeter": Marker(),
            "subMeterOf": Ref("site-meter"),
            "equip": Marker(),
            "spec": Ref("webinar::ElecSubMeter"),
            "siteRef": Ref("site"),
        },
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
        },
    ]

    xeto_cli = XetoCLI()
    recs_fit = xeto_cli.fits_explain(recs)

    try:
        assert recs_fit == Grid(
            {"ver": "3.0"},
            [{"name": "id"}, {"name": "msg"}],
            [],
        )
        print("Validation passed!!!")
    except Exception:
        print(f"Validation failed for the following reasons:\n{recs_fit.rows}")


if __name__ == "__main__":
    main()
```

## Test and Verify

### Run the Validation Test

Run the Python script and verify "Validation passed!!!" is shown in the terminal:

```shell
uv run hello.py
```

### Test Failure Cases

To verify the validation is working correctly, modify the `recs` to fail validation, run the script again, and verify "Validation failed" is shown in the terminal.
