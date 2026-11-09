"""ASHRAE Guideline 36-2021, 4.8: single-zone VAV air handling unit.

The Python twin of ``4.8-sz-vav-ahu.shm``, written with
``shifty.shapeshift``. Run it to print the ``.shm`` it generates, or to
write it and the modules it imports into a directory:

    uv run --project python python examples/shapeshift/g36/sz_vav_ahu.py [out-dir]

``python/tests/test_shapeshift.py`` checks that the output matches the
hand-written files exactly.
"""

from __future__ import annotations

import sys

from brick import BRICK, brick, parts, points
from components import components
from vav_cooling_only import vav as vav41

from shifty import shapeshift as ss
from shifty.shapeshift import Shape

ahu = ss.Module(
    "4.8-sz-vav-ahu",
    base="urn:ashrae/g36/4.8/sz-vav-ahu/",
    comment="""
        ASHRAE Guideline 36-2021, 4.8: single-zone VAV air handling unit.
        Translated from BuildingMOTIF's libraries/ashrae/guideline36/4.8-sz-vav-ahu.ttl.
    """,
)
ahu.uses(brick)
ahu.uses(components, alias="components")
ahu.uses(vav41, alias="vav41")


# --- the AHU -------------------------------------------------------------------

ahu.sz_vav_ahu = Shape(
    BRICK.AHU,
    parts.has(
        sa_fan=ahu.sa_fan,
        oa_damper=BRICK.Outside_Damper
        & points.has(dmppos=BRICK.Damper_Position_Command),
    ),
    points.has(
        sa_temp=BRICK.Supply_Air_Temperature_Sensor,
        oa_temp=BRICK.Outside_Air_Temperature_Sensor,
        ma_temp=BRICK.Mixed_Air_Temperature_Sensor,
        ra_temp=BRICK.Return_Air_Temperature_Sensor,
        filter_pd=BRICK.Filter_Differential_Pressure_Sensor,
    ),
    components.clg_signal,
    components.htg_signal,
    vav41.zone_temperature,
    vav41.occupancy_sensor,
    vav41.zone_co2_level,
    doc="4.8 Single-zone VAV air handling unit",
)

ahu.sa_fan = components.vfd_fan(BRICK.Supply_Fan)


# --- points that state their unit ------------------------------------------------

with_unit = components.with_unit

ahu.sa_temp = Shape(
    with_unit(BRICK.Supply_Air_Temperature_Sensor),
    comment="Points that must state their unit.",
)
ahu.oa_temp = with_unit(BRICK.Outside_Air_Temperature_Sensor)
ahu.ma_temp = with_unit(BRICK.Mixed_Air_Temperature_Sensor)
ahu.ra_temp = with_unit(BRICK.Return_Air_Temperature_Sensor)
ahu.filter_pd = with_unit(BRICK.Filter_Differential_Pressure_Sensor)
ahu.clg_signal = with_unit(BRICK.Cooling_Command)
ahu.htg_signal = with_unit(BRICK.Heating_Command)

ahu.zone_temperature = Shape(
    with_unit(BRICK.Zone_Temperature_Sensor),
    comment="""
        As in the original. Brick has no `Zone_Temperature_Sensor` (it has
        `Zone_Air_Temperature_Sensor`), so `shifty shape check` with Brick
        imported flags this line.
    """,
)
ahu.occupancy_sensor = with_unit(BRICK.Occupancy_Sensor)
ahu.zone_co2_level = with_unit(BRICK.CO2_Level_Sensor)


# --- dampers ---------------------------------------------------------------------

ahu.oa_ra_damper = BRICK.Damper & points.required(dmppos=BRICK.Damper_Position_Command)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for written in ss.write(sys.argv[1], ahu):
            print(f"wrote {written}")
    else:
        print(ahu, end="")
