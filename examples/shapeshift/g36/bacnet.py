"""``bacnet.shm``: the BACnet object that holds each point's value.

Brick links a point to the BACnet object behind it with
``ref:hasExternalReference``
(https://docs.brickschema.org/metadata/external-representations.html). The
shapes follow the ref schema Brick ships (``imports/ref.ttl``), which names
the device with ``bacnet:device-identifier`` and the URI with
``ref:BACnetURI``. The documentation's ``bacnet:objectOf`` link to a
``bacnet:BACnetDevice`` is accepted too.
"""

from __future__ import annotations

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Shape, datatype, pattern, value_range

REF = ss.Namespace("ref", "https://brickschema.org/schema/Brick/ref#")
BACNET = ss.Namespace("bacnet", "http://data.ashrae.org/bacnet/")

bacnet = ss.Module(
    "bacnet",
    base="urn:ashrae/g36/bacnet/",
    comment="""
        The BACnet object that holds a point's value, as Brick's ref schema links it:

          ex:sat ref:hasExternalReference [
              bacnet:object-identifier "analog-input,3" ;
              bacnet:device-identifier "device,123" ] .

        or, in one string, `ref:BACnetURI "bacnet://123/analog-input,3/present-value"`.
    """,
)

bacnet.bacnet_point = Shape(
    REF.hasExternalReference.at_least(1, bacnet.bacnet_object),
    doc="A point whose external references include the BACnet object holding its value.",
)

# Object types that hold a point's present value; instances are 22-bit.
bacnet.bacnet_object = Shape(
    Shape(
        BACNET["object-identifier"]
        .exactly(1)
        .each(pattern("^(analog|binary|multi-state)-(input|output|value),[0-9]{1,7}$")),
        bacnet.on_device,
    )
    | REF.BACnetURI.exactly(1).each(
        pattern(
            "^bacnet://[0-9]{1,7}/(analog|binary|multi-state)-(input|output|value),[0-9]{1,7}"
            "(/[a-z0-9-]+(/[0-9]+)?)?$"
        )
    ),
    doc=(
        "A BACnet object: its identifier (`analog-input,3`) on a device, or a BACnet URI "
        "(`bacnet://<device>/<object>[/<property>[/<index>]]`, ASHRAE 135 Clause Q.8)."
    ),
)

# Device instance 4194303 is reserved: it means "not yet configured".
bacnet.on_device = Shape(
    BACNET["device-identifier"].exactly(1).each(pattern("^device,[0-9]{1,7}$"))
    | BACNET.objectOf.exactly(
        1,
        BACNET.BACnetDevice
        & BACNET["device-instance"]
        .exactly(1)
        .each(datatype("integer"), value_range(0, 4194302)),
    ),
    doc="The device hosting the object: a device identifier, or a link to the device.",
)
