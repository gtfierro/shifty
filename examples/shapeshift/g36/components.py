"""``components.shm``: the equipment the G36 system specifications share."""

from __future__ import annotations

from brick import BRICK, points

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Class

components = ss.Module(
    "components",
    base="urn:ashrae/g36/components/",
    comment="""
        ASHRAE Guideline 36-2021: the equipment the system specifications share,
        from BuildingMOTIF's libraries/ashrae/guideline36/components.ttl, as
        templates over the equipment's class.
    """,
)


@components.template
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
