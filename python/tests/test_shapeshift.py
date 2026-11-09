"""The Python API for writing Shapeshift modules."""

from __future__ import annotations

import datetime
import importlib
from pathlib import Path

import pytest
import rdflib

import shifty
from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Class, Shape, any_of

ROOT = Path(__file__).resolve().parents[2]
G36 = ROOT / "examples" / "shapeshift" / "g36"
EX = ss.Namespace("ex", "http://example.org/")


def body(m: ss.Module) -> str:
    """The module's text after its header (its base, imports, and prefixes)."""
    lines = m.source().splitlines(keepends=True)
    while lines and lines[0].startswith(("base ", "import ", "prefix ", "\n")):
        lines.pop(0)
    return "".join(lines)


def one(*lines, **named) -> str:
    """The text of a lone shape `S` made of these lines."""
    m = ss.Module("t")
    m.S = Shape(*lines, **named)
    return body(m)


@pytest.fixture
def g36(monkeypatch):
    """The Python modules in `examples/shapeshift/g36`, imported."""
    monkeypatch.syspath_prepend(str(G36))
    names = (
        "bacnet",
        "brick",
        "components",
        "terminal_units",
        "air_handlers",
        "afdd",
        "generate",
    )
    return {name: importlib.import_module(name) for name in names}


def test_the_g36_files_are_generated(g36, tmp_path):
    written = ss.write(tmp_path, *g36["generate"].MODULES)
    # every file in the folder is generated, and none is out of date
    assert sorted(p.name for p in written) == sorted(p.name for p in G36.glob("*.shm"))
    stale = [p.name for p in written if p.read_text() != (G36 / p.name).read_text()]
    assert not stale, f"run examples/shapeshift/g36/generate.py to update {stale}"


def test_g36_modules_write_check_and_export(g36, tmp_path):
    ahu = g36["air_handlers"].sz_ahu
    written = ss.write(tmp_path, ahu)
    # the module first, then what it imports
    assert written[0].name == "4.8-sz-vav-ahu.shm"
    assert {p.name for p in written[1:]} == {
        "brick.shm",
        "bacnet.shm",
        "components.shm",
        "4.1-vav-cooling-only.shm",
    }
    assert ahu.check() == []
    g = rdflib.Graph().parse(data=ahu.export(), format="turtle")
    assert (rdflib.URIRef("urn:ashrae/g36/4.8/sz-vav-ahu/sz-vav-ahu"), None, None) in g


BRICK_NS = rdflib.Namespace("https://brickschema.org/schema/Brick#")
REF_NS = rdflib.Namespace("https://brickschema.org/schema/Brick/ref#")
BACNET_NS = rdflib.Namespace("http://data.ashrae.org/bacnet/")
UNIT_NS = rdflib.Namespace("http://qudt.org/vocab/unit/")


def described(model: str) -> rdflib.Graph:
    """`model`, with a unit and a BACnet object for each of its points."""
    g = rdflib.Graph().parse(data=model, format="turtle")
    for i, point in enumerate(sorted(set(g.objects(None, BRICK_NS.hasPoint)))):
        ref = rdflib.BNode()
        g.add((point, BRICK_NS.hasUnit, UNIT_NS.UNITLESS))
        g.add((point, REF_NS.hasExternalReference, ref))
        g.add(
            (ref, BACNET_NS["object-identifier"], rdflib.Literal(f"analog-value,{i}"))
        )
        g.add((ref, BACNET_NS["device-identifier"], rdflib.Literal("device,1")))
    return g


POINT_MODEL = """
@prefix bacnet: <http://data.ashrae.org/bacnet/> .
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ex: <urn:ex/> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix unit: <http://qudt.org/vocab/unit/> .

ex:ahu brick:hasPoint ex:sat .
ex:sat a brick:Supply_Air_Temperature_Sensor ;
    brick:hasUnit unit:DEG_C ;
    ref:hasExternalReference [ REFERENCE ] .
ex:device a bacnet:BACnetDevice ; bacnet:device-instance 123 .
"""


@pytest.mark.parametrize(
    "reference, conforms",
    [
        (
            'bacnet:object-identifier "analog-input,3" ; bacnet:device-identifier "device,123"',
            True,
        ),
        (
            'bacnet:object-identifier "multi-state-value,7" ; bacnet:objectOf ex:device',
            True,
        ),
        ('ref:BACnetURI "bacnet://123/analog-input,3/present-value"', True),
        ('ref:BACnetURI "bacnet://123/binary-output,1"', True),
        # the object, but not the device that hosts it
        ('bacnet:object-identifier "analog-input,3"', False),
        # a device is not an object that holds a point's value
        (
            'bacnet:object-identifier "device,3" ; bacnet:device-identifier "device,123"',
            False,
        ),
        ('ref:BACnetURI "http://123/analog-input,3"', False),
        # an external reference, but not to BACnet
        ('ref:hasTimeseriesId "a5e3" ; ref:storedAt "postgres://db"', False),
    ],
)
def test_every_point_names_its_bacnet_object(g36, reference, conforms):
    data = rdflib.Graph().parse(
        data=POINT_MODEL.replace("REFERENCE", reference), format="turtle"
    )
    ok, _report, text = g36["brick"].brick.validate(data)
    assert ok == conforms, text


def test_every_point_states_one_unit(g36):
    brick = g36["brick"].brick
    model = POINT_MODEL.replace(
        "REFERENCE", 'ref:BACnetURI "bacnet://1/analog-input,1"'
    )

    def conforms(text: str) -> bool:
        return brick.validate(rdflib.Graph().parse(data=text, format="turtle"))[0]

    assert conforms(model)
    assert not conforms(model.replace("brick:hasUnit unit:DEG_C ;", ""))
    assert not conforms(model.replace("unit:DEG_C", "unit:DEG_C, unit:DEG_F"))


VAV_MODEL = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ex: <urn:ex/> .

ex:vav a brick:VAV ;
    brick:hasPoint ex:dmp, ex:flow ;
    brick:feeds ex:zone .
ex:dmp a brick:Damper_Position_Command .
ex:flow a brick:Discharge_Air_Flow_Sensor .
ex:zone a brick:HVAC_Zone ;
    brick:hasPoint ex:temp .
ex:temp a brick:Zone_Air_Temperature_Sensor .
"""


def test_exported_shapes_validate(g36):
    vav, BRICK = g36["terminal_units"].vav, g36["brick"].BRICK
    good = described(VAV_MODEL)
    # the G36 shapes have no targets: say which nodes to check
    targets = {vav.vav_cooling_only: BRICK.VAV}
    assert vav.validate(good, targets=targets)[0]
    # without the damper position command, the box fails
    bad = described(
        VAV_MODEL.replace("brick:hasPoint ex:dmp, ex:flow", "brick:hasPoint ex:flow")
    )
    conforms, _report, text = vav.validate(bad, targets=targets)
    assert not conforms and "urn:ex/vav" in text
    # the same, with a target written into a module and shifty.validate
    check = ss.Module("check", base="urn:example/check/")
    check.every_vav = Shape(vav.vav_cooling_only, for_every=BRICK.VAV)
    shapes = rdflib.Graph().parse(data=check.export(), format="turtle")
    assert shifty.validate(good, shapes)[0] and not shifty.validate(bad, shapes)[0]
    assert check.validate(good)[0]


def test_reheat_boxes_need_a_heating_signal(g36):
    reheat, BRICK = g36["terminal_units"].reheat, g36["brick"].BRICK
    targets = {reheat.vav_with_reheat: BRICK.RVAV}
    # without the Brick ontology, say outright that the box is also a VAV
    box = VAV_MODEL.replace("ex:vav a brick:VAV", "ex:vav a brick:RVAV, brick:VAV")
    # 4.1 alone is not enough: the box needs a heating signal
    assert not reheat.validate(described(box), targets=targets)[0]
    # a heating command on the box is one
    command = box + "ex:vav brick:hasPoint ex:htg .\nex:htg a brick:Heating_Command .\n"
    assert reheat.validate(described(command), targets=targets)[0]
    # a position command on its heating coil is the other
    coil = box + (
        "ex:vav brick:hasPart ex:coil .\n"
        "ex:coil a brick:Heating_Coil ; brick:hasPoint ex:pos .\n"
        "ex:pos a brick:Position_Command .\n"
    )
    assert reheat.validate(described(coil), targets=targets)[0]


FAN_POWERED_MODEL = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ex: <urn:ex/> .

ex:fpu a brick:Terminal_Unit ;
    brick:hasPart ex:fan, ex:dmp ;
    brick:hasPoint ex:flow, ex:sat, ex:htg ;
    brick:feeds ex:zone .
ex:fan a brick:Fan ; brick:hasPoint ex:speed, ex:start, ex:status .
ex:speed a brick:Frequency_Command .
ex:start a brick:Start_Stop_Command .
ex:status a brick:Fan_Status .
ex:dmp a brick:Damper ; brick:hasPoint ex:pos .
ex:pos a brick:Damper_Position_Command .
ex:flow a brick:Supply_Air_Flow_Sensor .
ex:sat a brick:Supply_Air_Temperature_Sensor .
ex:htg a brick:Heating_Command .
ex:zone a brick:HVAC_Zone ; brick:hasPoint ex:temp .
ex:temp a brick:Zone_Air_Temperature_Sensor .
"""


def test_fan_powered_units_need_their_fan_and_damper(g36):
    unit, BRICK = g36["terminal_units"].fan_powered, g36["brick"].BRICK
    targets = {unit.terminal_unit: BRICK.Terminal_Unit}
    conforms, _report, text = unit.validate(
        described(FAN_POWERED_MODEL), targets=targets
    )
    assert conforms, text
    # the fan must report its status
    bad = described(FAN_POWERED_MODEL.replace(", ex:status .", " ."))
    conforms, _report, text = unit.validate(bad, targets=targets)
    assert not conforms and "urn:ex/fpu" in text
    # and each point must state its unit and its BACnet object
    bare = rdflib.Graph().parse(data=FAN_POWERED_MODEL, format="turtle")
    conforms, _report, text = unit.validate(bare, targets=targets)
    assert not conforms and "urn:ex/speed" in text


def test_validating_without_targets_is_an_error():
    m = ss.Module("t", base="urn:t/")
    m.S = Shape(EX.p.required())
    data = rdflib.Graph()
    with pytest.raises(ValueError, match="would check nothing"):
        m.validate(data)
    # targets may be classes, other targets, or lists of either
    assert m.validate(data, targets={m.S: [EX.C, ss.subjects_of(EX.q)]})[0]
    with pytest.raises(TypeError, match="not a target"):
        m.validate(data, targets={m.S: "ex:C"})


def test_write_shacl(tmp_path):
    m = ss.Module("t", base="urn:t/")
    m.S = Shape(EX.p.required(), for_every=EX.C)
    # into a directory: <name>.ttl
    written = m.write_shacl(tmp_path)
    assert written == tmp_path / "t.ttl"
    assert written.read_text() == m.export()
    # to a file, creating its directory
    nested = m.write_shacl(tmp_path / "out" / "shapes.ttl", core=True)
    assert nested == tmp_path / "out" / "shapes.ttl"
    g = rdflib.Graph().parse(nested, format="turtle")
    SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
    assert (rdflib.URIRef("urn:t/S"), SH.targetClass, rdflib.URIRef(EX.C.iri)) in g
    # a trailing slash names a directory, even one that does not exist yet
    assert m.write_shacl(f"{tmp_path}/new/") == tmp_path / "new" / "t.ttl"


def test_assignment_defines_names():
    m = ss.Module("t")
    m.points = ss.Path(EX.hasPoint)
    m.zone_temperature = m.points.required(EX.Temp)  # a line is a one-line shape
    m["with_underscore"] = EX.C & m.points.required()
    m.fed = EX.feeds.plus  # a path expression is a path
    assert m.source() == (
        "prefix ex: <http://example.org/>\n\n"
        "path points = ex:hasPoint\n"
        "path fed = ex:feeds+\n\n"
        "shape zone-temperature { points required { ex:Temp } }\n\n"
        "shape with_underscore {\n  is a ex:C\n  points required\n}\n"
    )
    assert "zone-temperature" in m and m.zone_temperature is m["zone-temperature"]
    assert "zone_temperature" in dir(m)
    assert [d.name for d in m] == [
        "points",
        "zone-temperature",
        "with_underscore",
        "fed",
    ]


def test_keywords_can_be_labels_but_not_names():
    m = ss.Module("t", base="urn:t/")
    m.S = Shape(
        EX.p.has(length=EX.Length, node=EX.Node),
        any_of(value=EX.v.required(), path=EX.w.required()),
        language=EX.q.required(),
    )
    assert body(m) == (
        "shape S {\n"
        "  ex:p {\n"
        "    length: ex:Length\n"
        "    node:   ex:Node\n"
        "  }\n"
        "  any of { value: ex:v required } { path: ex:w required }\n"
        "  language: ex:q required\n"
        "}\n"
    )
    assert m.check() == []
    ttl = m.export()
    assert ":S.length\n" in ttl and ":S.value\n" in ttl
    # a name or parameter is written bare, where a keyword reads as the keyword
    with pytest.raises(ValueError, match="`value` reads as the keyword"):
        m.value = Shape()
    with pytest.raises(ValueError, match="parameter 'length'"):

        @m.template
        def t(length):
            return EX.p.exactly(length)


def test_definitions_are_checked():
    m = ss.Module("t")
    with pytest.raises(ValueError, match="keyword"):
        m.a = Shape()
    with pytest.raises(ValueError, match="not a valid name"):
        m["has space"] = Shape()
    with pytest.raises(AttributeError, match=r"m\['source'\]"):
        m.source = Shape()
    with pytest.raises(TypeError, match=r"Path\(ex:p\)"):
        m.p = EX.p
    m.S = Shape()
    with pytest.raises(ValueError, match="already defines"):
        m.S = Shape()
    with pytest.raises(TypeError, match="not a line"):
        Shape(42)
    with pytest.raises(TypeError, match="is a path, not a line"):
        Shape(EX.p / EX.q)


def test_forward_references_resolve_when_written():
    m = ss.Module("t")
    m.S = Shape(m.later, EX.p.each(m.later))
    m.later = EX.p.required()
    assert body(m).startswith("shape S {\n  later\n  ex:p each matches later\n}")
    bad = ss.Module("u")
    bad.S = Shape(bad.zone_temprature)
    bad.zone_temperature = Shape()
    with pytest.raises(ValueError, match="did you mean 'zone-temperature'"):
        bad.source()


def test_templates_can_be_called_before_they_are_defined():
    def define_vfd(m):
        @m.template
        def vfd(cls: Class, n: int = 1):
            return EX.hasPoint.at_least(n, cls)

    m = ss.Module("v")
    m.S = Shape(m.vfd(EX.Fan), m.vfd(EX.Pump, n=2))
    define_vfd(m)
    assert "shape S {\n  vfd(ex:Fan)\n  vfd(ex:Pump, n = 2)\n}" in m.source()
    # the arguments are checked when the module is written
    bad = ss.Module("v")
    bad.S = Shape(bad.vfd(EX.Fan, bogus=1))
    define_vfd(bad)
    with pytest.raises(TypeError, match="vfd: got an unexpected keyword argument"):
        bad.source()
    # and the name must turn out to be a template
    w = ss.Module("w")
    w.S = Shape(w.T(EX.C))
    w.T = Shape()
    with pytest.raises(TypeError, match="is not a template"):
        w.source()


def test_a_named_shape_is_reused_by_name_elsewhere():
    m = ss.Module("t")
    m.S = Shape(EX.C)
    m.T = m.S  # already named: T composes S
    assert body(m) == "shape S { is a ex:C }\nshape T { S }\n"


def test_operators_combine_shapes():
    m = ss.Module("t")
    m.A = EX.a.required()
    m.B = EX.b.required()
    m.S = Shape(m.A | m.B | EX.c.required(), ~m.A, EX.C & m.B)
    m.U = (m.A & m.B) | (EX.C & EX.D)
    assert body(m).split("\n\n", 1)[1] == (
        "shape S {\n"
        "  any of { A } { B } { ex:c required }\n"
        "  none of { A }\n"
        "  is a ex:C\n"
        "  B\n"
        "}\n\n"
        "shape U {\n"
        "  any of { A; B } { is a ex:C; is a ex:D }\n"
        "}\n"
    )


def test_keyword_arguments_are_labels():
    m = ss.Module("t")

    @m.template
    def vfd(cls: Class):
        return Shape(cls, speed=EX.hasPoint.required())

    m.S = Shape(
        fan=vfd(EX.Fan),
        flow=EX.flow.required(),
    )
    m.T = any_of(EX.a.required(), b=EX.b.required(), c=m.S)
    assert body(m).split("\n\n", 1)[1] == (
        "shape S {\n"
        "  fan: vfd(ex:Fan)\n"
        "  flow: ex:flow required\n"
        "}\n\n"
        "shape T {\n"
        "  any of { ex:a required } { b: ex:b required } { c: S }\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="names a field or a composed shape"):
        Shape(c=EX.C)


def test_unnamed_shapes_are_written_in_place():
    inner = EX.Sensor & EX.unit.required()
    assert one(EX.p.each(inner), EX.q.at_least(2, inner), inner) == (
        "shape S {\n"
        "  ex:p each { is a ex:Sensor; ex:unit required }\n"
        "  ex:q at least 2 is { is a ex:Sensor; ex:unit required }\n"
        "  is a ex:Sensor\n"
        "  ex:unit required\n"
        "}\n"
    )
    with pytest.raises(ValueError, match="only a named shape"):
        one(EX.p.each(Shape(EX.C, doc="hmm")))


def test_or_and_not_of_classes_depend_on_where_they_go():
    m = ss.Module("t")
    m.fed = ss.Path(EX.feeds | EX.serves)  # where a path goes: `or`
    m.S = Shape(
        EX.A | EX.B,  # where a line goes: one of the classes
        ~EX.C,  # none of
        ~(EX.D | EX.E),
        EX.p.each(EX.A | EX.B),  # where a test goes, too
        EX.s.at_least(2, EX.A | EX.B | EX.C),
        EX.q.at_least(1, ~EX.C),
        (EX.A | EX.B) | EX.r.required(),  # one frame, not two
        m.fed.required(),
    )
    assert body(m) == (
        "path fed = ex:feeds or ex:serves\n\n"
        "shape S {\n"
        "  is a ex:A or ex:B\n"
        "  none of { is a ex:C }\n"
        "  none of { is a ex:D or ex:E }\n"
        "  ex:p each a ex:A or ex:B\n"
        "  ex:s at least 2 is a ex:A or ex:B or ex:C\n"
        "  ex:q at least 1 is {\n"
        "    none of { is a ex:C }\n"
        "  }\n"
        "  any of { is a ex:A or ex:B } { ex:r required }\n"
        "  fed required\n"
        "}\n"
    )
    assert m.check() == []
    # in a template, class parameters combine the same way
    t = ss.Module("u")

    @t.template
    def either(x: Class, y: Class):
        return EX.p.each(x | y)

    assert "shape either(x, y) { ex:p each a x or y }" in t.source()
    # a parameter that is not a class is not part of a union of classes
    u = ss.Module("v")

    @u.template
    def mixed(x, y: Class):
        return EX.p.each(x | y)

    assert "any of" in u.source()
    # assigned, it could be either; a path expression with steps is a path
    with pytest.raises(TypeError, match="could be a path or a shape"):
        m.x = EX.A | EX.B
    with pytest.raises(TypeError, match="is a path, not a line"):
        Shape(EX.p / EX.q | EX.r)


def test_paths_parenthesize_by_precedence():
    m = ss.Module("t")
    m.w = EX.p / EX.q | ~EX.r
    m.x = (EX.p | EX.q) / EX.r.star
    m.y = (EX.p / EX.q).plus
    m.z = ~(EX.p | EX.q).maybe
    assert m.source() == (
        "prefix ex: <http://example.org/>\n\n"
        "path w = ex:p/ex:q or ^ex:r\n"
        "path x = (ex:p or ex:q)/ex:r*\n"
        "path y = (ex:p/ex:q)+\n"
        "path z = ^(ex:p or ex:q)?\n"
    )


def test_fields_and_counts():
    assert one(
        EX.a.required(),
        EX.b.optional(),
        EX.c.exactly(2),
        EX.d.between(1, 3),
        EX.e.at_least(1, EX.C),
        EX.f.each(EX.C, ss.datatype("string")),
        EX.g.required().each(ss.one_of(EX.x, "y", 3)),
        EX.h.same_as(EX.k).unique_language(),
        EX.i.at_most(2, "on"),
    ) == (
        "shape S {\n"
        "  ex:a required\n"
        "  ex:b optional\n"
        "  ex:c exactly 2\n"
        "  ex:d between 1 and 3\n"
        "  ex:e at least 1 is a ex:C\n"
        "  ex:f each a ex:C, each string\n"
        '  ex:g required, each one of ex:x "y" 3\n'
        "  ex:h same as ex:k, unique language\n"
        '  ex:i at most 2 is one of "on"\n'
        "}\n"
    )


def test_tests():
    assert one(
        EX.a.each(ss.length(3), ss.length(min=1), ss.length(max=8)),
        EX.b.each(ss.value_range(0, 100), ss.value_range(hi=1.5)),
        EX.c.each(ss.pattern("^[A-Z]", flags="i"), ss.language("en", "de")),
        EX.d.each(ss.node_kind("iri"), ss.datatype(EX.T)),
        ss.is_(ss.one_of(EX.x)),
    ) == (
        "shape S {\n"
        "  ex:a each length 3, each length from 1, each length up to 8\n"
        "  ex:b each from 0 to 100, each up to 1.5\n"
        '  ex:c each pattern "^[A-Z]" flags "i", each language "en" "de"\n'
        "  ex:d each iri, each datatype ex:T\n"
        "  is one of ex:x\n"
        "}\n"
    )


def test_one_spelling_for_each_construct():
    # counts are written the way the language writes them
    assert one(
        EX.a.at_least(1),
        EX.b.at_most(1),
        EX.c.between(0, 1),
        EX.d.between(2, 2),
        EX.e.between(0, 3),
    ) == (
        "shape S {\n"
        "  ex:a required\n"
        "  ex:b optional\n"
        "  ex:c optional\n"
        "  ex:d exactly 2\n"
        "  ex:e at most 3\n"
        "}\n"
    )
    # ranges, comparisons, and a builtin datatype by its word
    assert one(
        EX.a.each(ss.value_range(above=0), ss.value_range(0, below=1)),
        EX.b.each(ss.value_range(above=0, hi=1), ss.value_range(below=1)),
        EX.c.below(EX.d),
        EX.e.up_to(EX.f),
        EX.g.each(ss.datatype(ss.XSD.string)),
    ) == (
        "shape S {\n"
        "  ex:a each above 0, each from 0 below 1\n"
        "  ex:b each above 0 to 1, each below 1\n"
        "  ex:c below ex:d\n"
        "  ex:e up to ex:f\n"
        "  ex:g each string\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="not both"):
        ss.value_range(0, above=1)


def test_counted_frames():
    a, b, c = EX.a.required(), EX.b.required(), EX.c.required()
    assert one(ss.at_most_of(1, a, b, c), ss.at_least_of(2, a, b, c)) == (
        "shape S {\n"
        "  at most 1 of { ex:a required } { ex:b required } { ex:c required }\n"
        "  at least 2 of { ex:a required } { ex:b required } { ex:c required }\n"
        "}\n"
    )
    with pytest.raises(ValueError, match="any_of"):
        ss.at_least_of(1, a, b)
    with pytest.raises(ValueError, match="none_of"):
        ss.at_most_of(0, a, b)


def test_dates_and_times():
    utc = datetime.timezone.utc
    assert one(
        EX.a.each(
            ss.datatype("date"),
            ss.value_range(datetime.date(2020, 1, 1), below=datetime.date(2025, 1, 1)),
        ),
        EX.b.each(ss.value_range(above=datetime.datetime(2024, 1, 31, 8, tzinfo=utc))),
        EX.c.each(ss.value_range(datetime.time(8), datetime.time(18, 30))),
        EX.d.each(ss.one_of(datetime.date(2024, 12, 25))),
    ) == (
        "shape S {\n"
        "  ex:a each date, each from 2020-01-01 below 2025-01-01\n"
        "  ex:b each above 2024-01-31T08:00:00Z\n"
        "  ex:c each from 08:00:00 to 18:30:00\n"
        "  ex:d each one of 2024-12-25\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="cannot end a range"):
        ss.value_range("2024-01-01")

    m = ss.Module("t", base="urn:t/")
    m.S = Shape(
        # the datatype and a range of its values hold together
        EX.made.each(ss.datatype("date"), ss.value_range(datetime.date(2020, 1, 1))),
        for_every=EX.Thing,
    )
    assert m.check() == []

    def conforms(made: str) -> bool:
        data = rdflib.Graph().parse(
            data=f"""
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:x a ex:Thing ; ex:made {made} .
            """,
            format="turtle",
        )
        return m.validate(data)[0]

    assert conforms('"2022-06-01"^^xsd:date')
    assert not conforms('"2019-12-31"^^xsd:date')
    # a dateTime is not ordered with a date
    assert not conforms('"2022-06-01T00:00:00"^^xsd:dateTime')


def test_groups_and_rows():
    assert one(
        EX.p.has(EX.A, b=EX.B, c=ss.row(EX.C, "optional", doc="if any")),
        EX.p.required(d=EX.D & EX.q.required(), e=ss.row(EX.E, (2, None))),
        EX.p.at_least(1, f=EX.F),
    ) == (
        "shape S {\n"
        "  ex:p {\n"
        "    ex:A\n"
        "    b: ex:B\n"
        '    doc "if any"\n'
        "    c: ex:C optional\n"
        "  }\n"
        "  ex:p required {\n"
        "    d: ex:D { ex:q required }\n"
        "    e: ex:E at least 2\n"
        "  }\n"
        "  ex:p required { f: ex:F }\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="cannot be a row"):
        one(EX.p.has(x=EX.q.required()))


def test_rows_can_carry_a_trailing_comment():
    assert one(
        EX.p.required(
            EX.A,
            ss.row(EX.B, doc="the b", comment="NOTE unclear"),
            c=ss.row(ss.row(EX.C, "optional"), comment="kept when wrapped"),
        )
    ) == (
        "shape S {\n"
        "  ex:p required {\n"
        "    ex:A\n"
        '    doc "the b"\n'
        "    ex:B  -- NOTE unclear\n"
        "    c: ex:C optional  -- kept when wrapped\n"
        "  }\n"
        "}\n"
    )
    with pytest.raises(ValueError, match="one line"):
        ss.row(EX.A, comment="two\nlines")


def test_attribute_reads_find_names_as_written():
    points = ss.Module("points", base="urn:points/")
    points["SAT_AVG"] = Shape(EX.A)
    points.mixed_air = Shape(EX.B)
    m = ss.Module("t", base="urn:t/")
    # an exact name first, then its kebab form; a later one either way
    m.S = Shape(
        EX.p.required(points.SAT_AVG, points.mixed_air, m.later_one, m.Later_Two)
    )
    m.later_one = Shape(EX.C)
    m["Later_Two"] = Shape(EX.D)
    assert "ex:p required { SAT_AVG; mixed-air; later-one; Later_Two }" in str(m)


def test_annotations_targets_and_closed_shapes():
    m = ss.Module("t")
    m.S = Shape(
        EX.a.required() | [EX.b.required(), EX.c.required()],
        ss.when(EX.Pump).then(EX.flow.required()),
        EX.name.required().doc("a name").severity("warning").message("needs a name"),
        for_every=EX.Equipment,
        targets=[ss.subjects_of(EX.hasPoint)],
        severity="violation",
        message="bad",
        comment="Every piece of equipment.",
        closed=True,
    )
    assert body(m) == (
        "-- Every piece of equipment.\n"
        "severity violation\n"
        'message "bad"\n'
        "shape S\n"
        "  for every ex:Equipment\n"
        "  for every node with ex:hasPoint\n"
        "{\n"
        "  any of { ex:a required } { ex:b required; ex:c required }\n"
        "  when { is a ex:Pump }\n"
        "  then { ex:flow required }\n"
        '  doc "a name"\n'
        "  severity warning\n"
        '  message "needs a name"\n'
        "  ex:name required\n"
        "  only these properties\n"
        "}\n"
    )


def test_templates():
    m = ss.Module("t")

    @m.template
    def with_point(cls: Class, prop=EX.hasPoint, part=EX.Default, n: int = 1):
        """Points of class `cls`."""
        return [cls, part, prop.at_least(n, cls), EX.q.each(part)]

    @m.template(name="Fallback")
    def fallback(cls: Class, other: Class = ss.P.cls):
        return other

    m.S = Shape(with_point(EX.T), fan=with_point(EX.F, n=2), fb=fallback(EX.X))
    assert body(m) == (
        'doc "Points of class `cls`."\n'
        "shape with-point(cls, prop = ex:hasPoint, part = ex:Default, n = 1) {\n"
        "  is a cls\n"  # annotated `Class`: a class
        "  part\n"  # not annotated: a shape, composed
        "  prop at least n is a cls\n"
        "  ex:q each matches part\n"
        "}\n\n"
        "shape Fallback(cls, other = cls) { is a other }\n\n"
        "shape S {\n"
        "  with-point(ex:T)\n"
        "  fan: with-point(ex:F, n = 2)\n"
        "  fb: Fallback(ex:X)\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="with-point"):
        with_point(EX.T, bogus=1)


def test_path_templates():
    m = ss.Module("t")

    @m.path_template
    def fed(p=EX.hasPoint):
        return EX.feeds.plus / p

    m.S = Shape(fed(EX.hasPart).required(), fed().has(t=EX.T))
    assert m.source() == (
        "prefix ex: <http://example.org/>\n\n"
        "path fed(p = ex:hasPoint) = ex:feeds+/p\n\n"
        "shape S {\n"
        "  fed(ex:hasPart) required\n"
        "  fed { t: ex:T }\n"
        "}\n"
    )


def test_shapes_from_data():
    m = ss.Module("t")

    @m.template
    def with_unit(cls: Class):
        return cls & EX.unit.required()

    units = {"sa-temp": EX.Supply_Air_Temp, "oa-temp": EX.Outside_Air_Temp}
    for name, cls in units.items():
        m[name] = with_unit(cls)
    assert body(m).split("\n\n", 1)[1] == (
        "shape sa-temp { with-unit(ex:Supply_Air_Temp) }\n"
        "shape oa-temp { with-unit(ex:Outside_Air_Temp) }\n"
    )


def test_cross_module_imports_and_aliases(tmp_path):
    lib = ss.Module("lib", base="urn:lib/")
    lib.pts = ss.Path(EX.hasPoint)
    lib.sensor = Shape(EX.Sensor)
    other = ss.Module("other", base="urn:other/")
    other.temp = lib.pts.required(lib.sensor)

    m = ss.Module("main", base="urn:main/").uses(other, alias="o")
    m.S = Shape(other.temp, lib.pts.each(lib.sensor))
    assert m.source() == (
        "base <urn:main/>\n"
        'import "./other.shm" as o\n'
        'import "./lib.shm"\n\n'
        "shape S {\n"
        "  o.temp\n"
        "  pts each matches sensor\n"
        "}\n"
    )
    assert [p.name for p in m.closure()] == ["main", "other", "lib"]
    assert [p.name for p in ss.write(tmp_path, m)] == [
        "main.shm",
        "other.shm",
        "lib.shm",
    ]
    assert m.check() == []
    ttl = m.export()
    assert ":S\n    a sh:NodeShape" in ttl and "sh:node <urn:other/temp>" in ttl


def test_prefix_conflicts_are_errors():
    other = ss.Namespace("ex", "http://other.example/")
    with pytest.raises(ValueError, match="stands for both"):
        one(EX.p.required(), other.p.required())


def test_terms_outside_a_prefix_are_iris():
    assert one(ss.iri("http://x.org/p").required(), EX["has.dot."].required()) == (
        "shape S {\n  <http://x.org/p> required\n  <http://example.org/has.dot.> required\n}\n"
    )


def test_format_and_errors():
    assert ss.format("shape S{ex:p required}\nprefix ex: <http://e/>") == (
        "prefix ex: <http://e/>\n\nshape S { ex:p required }\n"
    )
    with pytest.raises(ss.ShapeshiftError, match="error"):
        ss.format("shape S {")
