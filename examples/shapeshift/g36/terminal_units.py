"""Guideline 36 sections 4.1 to 4.5: terminal units.

Each module is one section. 4.2 to 4.5 reuse the zone sensors of 4.1.
"""

from __future__ import annotations

from brick import BRICK, brick, parts, points
from components import components

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Class, P, Shape, any_of

# --- 4.1 -----------------------------------------------------------------------

vav = ss.Module(
    "4.1-vav-cooling-only",
    base="urn:ashrae/g36/4.1/vav-cooling-only/",
    comment="""
        ASHRAE Guideline 36-2021, 4.1: VAV terminal unit, cooling only.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.1-vav-cooling-only.ttl.
    """,
)
vav.uses(brick)

vav.vav_cooling_only = Shape(
    BRICK.VAV,
    (
        points.required(dmppos=BRICK.Damper_Position_Command)
        | parts.required(dmp=BRICK.Damper & points.each(BRICK.Damper_Position_Command))
    ).comment("a damper position command, on the box or on its damper"),
    points.required(flow=BRICK.Discharge_Air_Flow_Sensor),
    vav.zone_temperature,
    vav.occupancy_sensor,
    vav.zone_co2_level,
    BRICK.feeds.plus.at_least(
        0,
        BRICK.HVAC_Zone
        & parts.required(room=BRICK.Room & points.required(BRICK.Open_Close_Status)),
    ).comment("optional: a window switch in a room of a zone the box feeds"),
    doc="4.1 VAV terminal unit, cooling only",
)


@vav.template(
    comment="""
        At least `n` points of class `cls` on the box, or in a zone it feeds (or a
        `room` of that zone, with points of class `room_cls`).
    """
)
def on_box_or_downstream(
    cls: Class, room: Class = BRICK.Room, room_cls: Class = P.cls, n: int = 1
):
    """Points of class `cls` on the box, or in a zone or room it feeds."""
    return any_of(
        points.at_least(n, on_box=cls),
        in_zone=(BRICK.feeds.plus / BRICK.hasPart.maybe).at_least(
            n,
            (BRICK.HVAC_Zone & points.required(cls))
            | (room & points.required(room_cls)),
        ),
    )


vav.zone_temperature = Shape(
    ztemp=on_box_or_downstream(
        BRICK.Zone_Air_Temperature_Sensor, room_cls=BRICK.Temperature_Sensor
    ),
    doc="A zone temperature sensor, on the box or in a zone or room it feeds.",
)

vav.occupancy_sensor = Shape(
    occ=on_box_or_downstream(BRICK.Occupancy_Sensor, n=0),
    doc="Optional: an occupancy sensor, on the box or in a zone or room it feeds.",
    comment="""
        G36 lists occupancy, CO2, and the window switch as optional. The library
        states that as `at least 0`, which every node satisfies: these lines
        document the points and never fail.
    """,
)

vav.zone_co2_level = Shape(
    co2=on_box_or_downstream(BRICK.CO2_Level_Sensor, room=BRICK.Space, n=0),
    doc="Optional: a CO2 sensor, on the box or in a zone or space it feeds.",
)

# --- 4.2 -----------------------------------------------------------------------

reheat = ss.Module(
    "4.2-vav-with-reheat",
    base="urn:ashrae/g36/4.2/vav-with-reheat/",
    comment="""
        ASHRAE Guideline 36-2021, 4.2: VAV terminal unit with reheat.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.2-vav-with-reheat.ttl.
    """,
)
reheat.uses(brick)
reheat.uses(vav, alias="vav41")

reheat.vav_with_reheat = Shape(
    BRICK.RVAV,
    vav.vav_cooling_only,
    reheat.heating_signal,
    doc="4.2 VAV terminal unit with reheat",
)

reheat.heating_signal = Shape(
    points.required(htg=BRICK.Heating_Command)
    | parts.required(
        rhc=BRICK.Heating_Coil & points.required(cmd=BRICK.Position_Command)
    ),
    doc="A heating command on the box, or a position command on its heating coil.",
)

# --- 4.3 -----------------------------------------------------------------------

fan_powered = ss.Module(
    "4.3-fan-powered",
    base="urn:ashrae/g36/4.3/fan-powered/",
    comment="""
        ASHRAE Guideline 36-2021, 4.3: fan-powered terminal unit.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.3-fan-powered.ttl.
    """,
)
fan_powered.uses(brick)
fan_powered.uses(components, alias="components")
fan_powered.uses(vav, alias="vav41")
fan_powered.uses(reheat, alias="vav42")

fan_powered.terminal_unit = Shape(
    BRICK.Terminal_Unit,
    parts.has(
        fan=components.vfd_fan(BRICK.Fan),
        damper=components.positioned_damper(BRICK.Damper),
    ),
    points.has(
        sa_flow=BRICK.Supply_Air_Flow_Sensor,
        sa_temp=BRICK.Supply_Air_Temperature_Sensor,
    ),
    vav.zone_temperature,
    reheat.heating_signal,
    doc="4.3 Fan-powered terminal unit",
)

# --- 4.4 -----------------------------------------------------------------------

inlet = ss.Module(
    "4.4-dual-duct-terminal-unit-with-inlet",
    base="urn:ashrae/g36/4.4/dual-duct-terminal-unit-with-inlet-sensors/",
    comment="""
        ASHRAE Guideline 36-2021, 4.4: dual-duct terminal unit with inlet sensors.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.4-dual-duct-terminal-unit-with-inlet.ttl.
    """,
)
inlet.uses(brick)
inlet.uses(components, alias="components")
inlet.uses(vav, alias="vav41")

# Each deck measures its own airflow, at the inlet.
inlet.terminal_unit = Shape(
    BRICK.Terminal_Unit,
    parts.has(
        hot_deck=Shape(
            BRICK.Hot_Deck,
            components.positioned_damper(BRICK.Damper),
            points.has(sa_flow=BRICK.Supply_Air_Flow_Sensor),
        ),
        cold_deck=Shape(
            BRICK.Cold_Deck,
            components.positioned_damper(BRICK.Damper),
            points.has(sa_flow=BRICK.Supply_Air_Flow_Sensor),
        ),
    ),
    points.optional(
        override=BRICK.Override_Command,
        temp_adjust=BRICK.Temperature_Adjust_Sensor,
    ),
    vav.zone_temperature,
    vav.zone_co2_level,
    vav.occupancy_sensor,
    doc="4.4 Dual-duct terminal unit with inlet sensors",
)

# --- 4.5 -----------------------------------------------------------------------

discharge = ss.Module(
    "4.5-dual-duct-terminal-unit-with-discharge",
    base="urn:ashrae/g36/4.5/dual-duct-terminal-unit-with-discharge/",
    comment="""
        ASHRAE Guideline 36-2021, 4.5: dual-duct terminal unit with a discharge sensor.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/4.5-dual-duct-terminal-unit-with-discharge.ttl.
    """,
)
discharge.uses(brick)
discharge.uses(components, alias="components")
discharge.uses(vav, alias="vav41")

# As 4.4, but the box measures its airflow once, at the discharge.
discharge.terminal_unit = Shape(
    BRICK.Terminal_Unit,
    parts.has(
        hot_deck=BRICK.Hot_Deck & components.positioned_damper(BRICK.Damper),
        cold_deck=BRICK.Cold_Deck & components.positioned_damper(BRICK.Damper),
    ),
    points.has(sa_flow=BRICK.Supply_Air_Flow_Sensor),
    points.optional(
        override=BRICK.Override_Command,
        temp_adjust=BRICK.Temperature_Adjust_Sensor,
    ),
    vav.zone_temperature,
    vav.zone_co2_level,
    vav.occupancy_sensor,
    doc="4.5 Dual-duct terminal unit with a discharge airflow sensor",
)
