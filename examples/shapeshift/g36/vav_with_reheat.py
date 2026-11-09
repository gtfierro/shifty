"""ASHRAE Guideline 36-2021, 4.2: VAV terminal unit with reheat.

The Python twin of ``4.2-vav-with-reheat.shm``, written with
``shifty.shapeshift``. Run it to print the ``.shm`` it generates, or to
write it and the modules it imports into a directory:

    uv run --project python python examples/shapeshift/g36/vav_with_reheat.py [out-dir]

``python/tests/test_shapeshift.py`` checks that the output matches the
hand-written files exactly.
"""

from __future__ import annotations

import sys

from brick import BRICK, brick, parts, points
from vav_cooling_only import vav as vav41

from shifty import shapeshift as ss
from shifty.shapeshift import Shape

reheat = ss.Module(
    "4.2-vav-with-reheat",
    base="urn:ashrae/g36/4.2/vav-with-reheat/",
    comment="""
        ASHRAE Guideline 36-2021, 4.2: VAV terminal unit with reheat.
        Translated from BuildingMOTIF's libraries/ashrae/guideline36/4.2-vav-with-reheat.ttl.
    """,
)
reheat.uses(brick)
reheat.uses(vav41, alias="vav41")

# A reheat box is a 4.1 box with a heating signal.
reheat.vav_with_reheat = Shape(
    BRICK.RVAV,
    vav41.vav_cooling_only,
    reheat.heating_signal,
    doc="4.2 VAV terminal unit with reheat",
)

reheat.heating_signal = Shape(
    reheat.heating_signal1 | reheat.heating_signal2,
    doc="A heating command on the box, or a position command on its heating coil.",
)

reheat.heating_signal1 = points.required(htg=BRICK.Heating_Command)

reheat.heating_signal2 = parts.required(
    rhc=BRICK.Heating_Coil & points.required(cmd=BRICK.Position_Command)
)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for written in ss.write(sys.argv[1], reheat):
            print(f"wrote {written}")
    else:
        print(reheat, end="")
