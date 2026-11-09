"""ASHRAE Guideline 36-2021, 4.3: fan-powered terminal unit.

The Python twin of ``4.3-fan-powered.shm``, written with
``shifty.shapeshift``. Run it to print the ``.shm`` it generates, or to
write it and the modules it imports into a directory:

    uv run --project python python examples/shapeshift/g36/fan_powered.py [out-dir]

``python/tests/test_shapeshift.py`` checks that the output matches the
hand-written files exactly.
"""

from __future__ import annotations

import sys

from brick import BRICK, brick, parts, points
from components import components
from vav_cooling_only import vav as vav41
from vav_with_reheat import reheat as vav42

from shifty import shapeshift as ss
from shifty.shapeshift import Shape

fan_powered = ss.Module(
    "4.3-fan-powered",
    base="urn:ashrae/g36/4.3/fan-powered/",
    comment="""
        ASHRAE Guideline 36-2021, 4.3: fan-powered terminal unit.
        Translated from BuildingMOTIF's libraries/ashrae/guideline36/4.3-fan-powered.ttl.
    """,
)
fan_powered.uses(brick)
fan_powered.uses(components, alias="components")
fan_powered.uses(vav41, alias="vav41")
fan_powered.uses(vav42, alias="vav42")

# The zone sensor of a 4.1 box and the heating signal of a 4.2 box, with a
# fan and a damper of its own.
fan_powered.terminal_unit = Shape(
    BRICK.Terminal_Unit,
    vav41.zone_temperature,
    vav42.heating_signal,
    parts.has(fan=fan_powered.fan, damper=fan_powered.damper),
    points.has(
        sa_flow=BRICK.Supply_Air_Flow_Sensor,
        sa_temp=BRICK.Supply_Air_Temperature_Sensor,
    ),
    doc="4.3 Fan-powered terminal unit",
)

fan_powered.fan = components.vfd_fan(BRICK.Fan)
fan_powered.damper = components.positioned_damper(BRICK.Damper)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for written in ss.write(sys.argv[1], fan_powered):
            print(f"wrote {written}")
    else:
        print(fan_powered, end="")
