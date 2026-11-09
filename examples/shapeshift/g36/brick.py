"""``brick.shm``: the Brick point and part lists, and what every point states.

Every G36 module imports it.
"""

from __future__ import annotations

from bacnet import bacnet

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Path, Shape, objects_of

BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")

brick = ss.Module(
    "brick",
    base="urn:ashrae/g36/brick/",
    comment="""
        Names for the Brick properties that point and part lists follow, and the
        rule every point meets.

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

brick.point = Shape(
    BRICK.hasUnit.exactly(1),
    bacnet.bacnet_point,
    targets=[objects_of(BRICK.hasPoint)],
    doc="Every point states its unit and the BACnet object that holds its value.",
)

points, parts = brick.points, brick.parts
