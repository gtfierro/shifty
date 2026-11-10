from datetime import date

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Class, Path, Shape

BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")
EX = ss.Namespace("ex", "https://example.org/building#")

m = ss.Module("vav", base="urn:example/vav/")
m.points = Path(BRICK.hasPoint)


@m.template
def with_point(cls: Class, n: int = 1):
    """At least `n` points of class `cls`."""
    return m.points.at_least(n, cls)


m.vav = Shape(
    BRICK.VAV,
    m.points.required(flow=BRICK.Air_Flow_Sensor),
    m.damper | m.damper_command,
    temp=with_point(BRICK.Zone_Air_Temperature_Sensor),
    fed=BRICK.isFedBy.exactly(1).each(BRICK.AHU | BRICK.RTU),
    commissioned=EX.commissioned.optional().each(
        ss.datatype("date"), ss.value_range(date(2000, 1, 1))
    ),
    for_every=BRICK.VAV,
    doc="A VAV box.",
)
m.damper = BRICK.hasPart.at_least(1, BRICK.Damper)
m.damper_command = m.points.required(BRICK.Damper_Position_Command)

print(m)
