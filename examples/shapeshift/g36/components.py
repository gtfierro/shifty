"""``components.shm``: components the G36 system specifications share.

uv run --project python python examples/shapeshift/g36/components.py
"""

from __future__ import annotations

from brick import BRICK, points

from shifty import shapeshift as ss
from shifty.shapeshift import Class, Shape

components = ss.Module(
    "components",
    base="urn:ashrae/g36/components/",
    comment="""
        ASHRAE Guideline 36-2021: components shared by the system specifications.
        Translated from BuildingMOTIF's libraries/ashrae/guideline36/components.ttl;
        the templates at the end factor out what 4.3, 4.6, and 4.8 each repeat.
    """,
)

# BuildingMOTIF spells these two with `_`, so they are named as written.
components["cooling_damper"] = Shape(
    components.positioned_damper(BRICK.Damper), doc="Cooling Damper"
)
components["heating_damper"] = Shape(
    components.positioned_damper(BRICK.Damper), doc="Heating Damper"
)

components.clg_signal = Shape(
    points.has(clg=BRICK.Cooling_Command), doc="Cooling Signal"
)
components.htg_signal = Shape(
    points.has(htg=BRICK.Heating_Command), doc="Heating Signal"
)

components.variable_speed_fan_vfd = Shape(BRICK.Fan, doc="Variable Speed Fan VFD")


@components.template(
    comment="Templates ------------------------------------------------------------------"
)
def vfd_fan(cls: Class):
    """A fan of class `cls` on a VFD: speed command, start/stop, and status."""
    return cls & points.has(
        speed=BRICK.Frequency_Command,
        start_stop=BRICK.Start_Stop_Command,
        status=BRICK.Fan_Status,
    )


@components.template
def positioned_damper(cls: Class):
    """A damper of class `cls` with a position command."""
    return cls & points.has(dmppos=BRICK.Damper_Position_Command)


@components.template
def valved_coil(cls: Class):
    """A coil of class `cls` with a valve position command."""
    return cls & points.has(cmd=BRICK.Valve_Position_Command)


@components.template
def with_unit(cls: Class):
    """A point of class `cls` that states its unit."""
    return cls & BRICK.hasUnit.exactly(1)


if __name__ == "__main__":
    print(components, end="")
