"""Guideline 36 sections 4.6 and 4.8: air handling units."""

from __future__ import annotations

from brick import BRICK, brick, parts, points
from components import components
from terminal_units import vav

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Shape

# --- 4.6 -----------------------------------------------------------------------

mz_ahu = ss.Module(
    "4.6-multiple-zone-vav-AHU",
    base="urn:ashrae/g36/4.6/mz-vav-ahu/",
    comment="""
        ASHRAE Guideline 36-2021, 4.6: multiple-zone VAV air handling unit.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.6-multiple-zone-vav-AHU.ttl.
    """,
)
mz_ahu.uses(brick)
mz_ahu.uses(components, alias="components")

mz_ahu.AHU = Shape(
    BRICK.AHU,
    parts.has(
        sa_fan=components.vfd_fan(BRICK.Supply_Fan),
        ra_damper=components.positioned_damper(BRICK.Return_Damper),
        htg_coil=components.valved_coil(BRICK.Heating_Coil),
        clg_coil=components.valved_coil(BRICK.Cooling_Coil),
    ),
    points.has(
        sa_temp=components.with_unit(BRICK.Supply_Air_Temperature_Sensor),
        oa_temp=components.with_unit(BRICK.Outside_Air_Temperature_Sensor),
        ma_temp=components.with_unit(BRICK.Mixed_Air_Temperature_Sensor),
        ra_temp=components.with_unit(BRICK.Return_Air_Temperature_Sensor),
        filter_pd=components.with_unit(BRICK.Filter_Differential_Pressure_Sensor),
    ).comment("4.6 states the unit here too; brick.shm asks it of every point"),
    doc="4.6 Multiple-zone VAV air handling unit",
)

# --- 4.8 -----------------------------------------------------------------------

sz_ahu = ss.Module(
    "4.8-sz-vav-ahu",
    base="urn:ashrae/g36/4.8/sz-vav-ahu/",
    comment="""
        ASHRAE Guideline 36-2021, 4.8: single-zone VAV air handling unit.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.8-sz-vav-ahu.ttl.
    """,
)
sz_ahu.uses(brick)
sz_ahu.uses(components, alias="components")
sz_ahu.uses(vav, alias="vav41")

sz_ahu.sz_vav_ahu = Shape(
    BRICK.AHU,
    parts.has(
        sa_fan=components.vfd_fan(BRICK.Supply_Fan),
        oa_damper=components.positioned_damper(BRICK.Outside_Damper),
    ),
    points.has(
        sa_temp=BRICK.Supply_Air_Temperature_Sensor,
        oa_temp=BRICK.Outside_Air_Temperature_Sensor,
        ma_temp=BRICK.Mixed_Air_Temperature_Sensor,
        ra_temp=BRICK.Return_Air_Temperature_Sensor,
        filter_pd=BRICK.Filter_Differential_Pressure_Sensor,
        clg=BRICK.Cooling_Command,
        htg=BRICK.Heating_Command,
    ),
    vav.zone_temperature,
    vav.occupancy_sensor,
    vav.zone_co2_level,
    doc="4.8 Single-zone VAV air handling unit",
)
