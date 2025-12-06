# Xeto Introduction

This tutorial demonstrates how to create a Xeto library and use it with Python to validate Haystack instance data. You'll learn to define custom Xeto specs for electric meters and verify data conformance using the phable package.

## Prerequisites

- Install the latest [haxall build](https://github.com/Project-Haystack/xeto/blob/master/src/xeto/doc.xeto.tools/Setup.md) for Java.
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

### Link to Haxall Runtime

Link the project to the Haxall runtime (adjust the path to match your Haxall installation):

```shell
~/haxall-4.0.4/bin/xeto env
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

### Create Electric Meter Specs

Replace the contents of `specs.xeto` with the below text to define Xeto specs for an electric sitemeter and submeter:

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
}
```

### Configure Library Dependencies

Specify the Xeto library dependencies by replacing the contents of `lib.xeto` with the below text:

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

Replace the contents of `hello.py` with the below Python code that creates Haystack instance data and validates it against the Xeto specs just created:

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

    try:
        # validate recs adhere to their Xeto specs
        # Note: An empty grid means no errors!
        assert xeto_cli.fits_explain(recs) == Grid(
            {"ver": "3.0"},
            [{"name": "id"}, {"name": "msg"}],
            [],
        )
        print("Test passed!!!")
    except Exception:
        print("Test failed!!!")


if __name__ == "__main__":
    main()
```

## Test and Verify

### Run the Validation Test

Run the Python script and verify "Test passed!!!" is shown in the terminal:

```shell
uv run hello.py
```

### Test Failure Cases

To verify the validation is working correctly, modify the `recs` to fail validation, run the script again, and verify "Test failed!!!" is shown in the terminal.
