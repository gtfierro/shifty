"""ASHRAE Guideline 36-2021, 4.1: VAV terminal unit, cooling only.

The Python twin of ``4.1-vav-cooling-only.shm`` (and of the ``brick.shm``
it imports), written with ``shifty.shapeshift``. Run it to print the
``.shm`` it generates, or to write both files into a directory:

    uv run --project python python examples/shapeshift/g36/4.1-vav-cooling-only.py [out-dir]

``python/tests/test_shapeshift.py`` checks that the output matches the
hand-written files exactly.
"""

from __future__ import annotations

import sys

from shifty import shapeshift as ss
from shifty.shapeshift import Class, P, Path, Shape, any_of

BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")

# --- brick.shm: names for the point and part lists ----------------------------

brick = ss.Module(
    "brick",
    base="urn:ashrae/g36/brick-lists",
    comment="""
        Names for the Brick properties that point and part lists follow.

        Import this file without an alias and write, in any shape,

          points {
            speed:  brick:Frequency_Command
            status: brick:Fan_Status optional
          }

        `points` is `brick:hasPoint`, so this is the same as writing
        `brick:hasPoint { ... }`. Each row counts the values along the path that
        are instances of the row's class: exactly one, unless the row (or the
        group, as in `points required { ... }`) says otherwise. `required` means
        at least one; `optional` means at most one.
    """,
)
brick.points = Path(
    BRICK.hasPoint, doc="Points the equipment reports (brick:hasPoint)."
)
brick.parts = Path(
    BRICK.hasPart, doc="Equipment the equipment is made of (brick:hasPart)."
)
points, parts = brick.points, brick.parts

# --- 4.1-vav-cooling-only.shm ---------------------------------------------------

vav = ss.Module(
    "4.1-vav-cooling-only",
    base="urn:ashrae/g36/4.1/vav-cooling-only/",
    comment="""
        ASHRAE Guideline 36-2021, 4.1: VAV terminal unit, cooling only.
        Translated from BuildingMOTIF's libraries/ashrae/guideline36/4.1-vav-cooling-only.ttl.
    """,
)

vav.downstream = Path(
    BRICK.feeds.plus / BRICK.hasPart.maybe,
    comment="Zone sensors may sit on the box, or in a zone or room downstream of it.",
)


# --- the box -------------------------------------------------------------------

vav.vav_cooling_only = Shape(
    BRICK.VAV,
    vav.box_damper_position,
    vav.zone_temperature,
    vav.occupancy_sensor,
    vav.zone_co2_level,
    vav.window_switch,
    vav.discharge_airflow,
    doc="4.1 VAV terminal unit, cooling only",
)

vav.box_damper_position = Shape(
    points.required(dmppos=BRICK.Damper_Position_Command)
    | parts.required(dmp=vav.damper_with_position),
    doc="A damper position command, on the box or on its damper.",
)

vav.damper_with_position = BRICK.Damper & BRICK.hasPoint.each(
    BRICK.Damper_Position_Command
)

vav.discharge_airflow = points.required(flow=BRICK.Discharge_Air_Flow_Sensor)


# --- zone sensors ----------------------------------------------------------------


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
    zone = BRICK.HVAC_Zone & points.required(cls)
    in_room = room & points.required(room_cls)
    return any_of(
        points.at_least(n, on_box=cls),
        in_zone=vav.downstream.at_least(n, zone | in_room),
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
        states that as `at least 0`, which every node satisfies: these shapes
        document the points and never fail.
    """,
)

vav.zone_co2_level = Shape(
    co2=on_box_or_downstream(BRICK.CO2_Level_Sensor, room=BRICK.Space, n=0),
    doc="Optional: a CO2 sensor, on the box or in a zone or space it feeds.",
)

vav.window_switch = Shape(
    BRICK.feeds.plus.at_least(0, vav.zone_with_room),
    doc="Optional: a window switch in a room of a zone the box feeds.",
)

vav.zone_with_room = BRICK.HVAC_Zone & parts.required(
    room=BRICK.Room & points.required(BRICK.Open_Close_Status)
)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for written in ss.write(sys.argv[1], vav):
            print(f"wrote {written}")
    else:
        print(vav, end="")
