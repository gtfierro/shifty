from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Shape

BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")

m = ss.Module(
    "vav",
    base="urn:example/tutorial/",
    comment="Shapes for the VAV boxes of a building.",
)

m.vav = Shape(
    fed=BRICK.isFedBy.exactly(1).each(BRICK.AHU),
    flow=BRICK.hasPoint.at_least(1, BRICK.Air_Flow_Sensor),
    damper=BRICK.hasPart.at_least(1, m.damper),
    for_every=BRICK.VAV,
    doc="A VAV box: fed by one AHU, with an airflow sensor and a damper.",
)

m.damper = Shape(
    BRICK.Damper,
    BRICK.hasPoint.at_least(1, BRICK.Damper_Position_Command),
    doc="A damper that can be commanded to a position.",
)

print(m, end="")
