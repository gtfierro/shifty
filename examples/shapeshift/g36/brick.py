"""``brick.shm``: names for the Brick point and part lists every G36 module uses.

uv run --project python python examples/shapeshift/g36/brick.py
"""

from __future__ import annotations

from shifty import shapeshift as ss
from shifty.shapeshift import Path

BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")

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

if __name__ == "__main__":
    print(brick, end="")
