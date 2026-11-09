"""The Python API for writing Shapeshift modules."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import rdflib

import shifty
from shifty import shapeshift as ss
from shifty.shapeshift import Class, Shape, any_of

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


def g36_example():
    """`examples/shapeshift/g36/4.1-vav-cooling-only.py`, run as a module."""
    spec = importlib.util.spec_from_file_location(
        "vav_cooling_only", G36 / "4.1-vav-cooling-only.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_regenerates_the_g36_files_exactly(tmp_path):
    ex = g36_example()
    assert ex.brick.source() == (G36 / "brick.shm").read_text()
    assert ex.vav.source() == (G36 / "4.1-vav-cooling-only.shm").read_text()
    written = ss.write(tmp_path, ex.vav)
    assert [p.name for p in written] == ["4.1-vav-cooling-only.shm", "brick.shm"]


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


def test_exported_shapes_validate():
    ex = g36_example()
    # The G36 shapes have no targets; hold every VAV to the 4.1 shape.
    check = ss.Module("check", base="urn:example/check/")
    check.every_vav = Shape(ex.vav.vav_cooling_only, for_every=ex.BRICK.VAV)
    shapes = rdflib.Graph().parse(data=check.export(), format="turtle")
    good = rdflib.Graph().parse(data=VAV_MODEL, format="turtle")
    assert shifty.validate(good, shapes)[0]
    # without the damper position command, the box fails
    bad = rdflib.Graph().parse(
        data=VAV_MODEL.replace(
            "brick:hasPoint ex:dmp, ex:flow", "brick:hasPoint ex:flow"
        ),
        format="turtle",
    )
    assert not shifty.validate(bad, shapes)[0]


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
        "shape with_underscore {\n  is ex:C\n  points required\n}\n"
    )
    assert "zone-temperature" in m and m.zone_temperature is m["zone-temperature"]
    assert "zone_temperature" in dir(m)
    assert [d.name for d in m] == [
        "points",
        "zone-temperature",
        "with_underscore",
        "fed",
    ]


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
    assert body(m).startswith("shape S {\n  later\n  ex:p each later\n}")
    bad = ss.Module("u")
    bad.S = Shape(bad.zone_temprature)
    bad.zone_temperature = Shape()
    with pytest.raises(ValueError, match="did you mean 'zone-temperature'"):
        bad.source()
    with pytest.raises(TypeError, match="define a template before calling it"):
        bad.not_yet(EX.C)


def test_a_named_shape_is_reused_by_name_elsewhere():
    m = ss.Module("t")
    m.S = Shape(EX.C)
    m.T = m.S  # already named: T composes S
    assert body(m) == "shape S { is ex:C }\nshape T { S }\n"


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
        "  is ex:C\n"
        "  B\n"
        "}\n\n"
        "shape U {\n"
        "  any of { A; B } { is ex:C; is ex:D }\n"
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
        "  ex:p each { is ex:Sensor; ex:unit required }\n"
        "  ex:q at least 2 is { is ex:Sensor; ex:unit required }\n"
        "  is ex:Sensor\n"
        "  ex:unit required\n"
        "}\n"
    )
    with pytest.raises(ValueError, match="only a named shape"):
        one(EX.p.each(Shape(EX.C, doc="hmm")))


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
        "  ex:e at least 1 is ex:C\n"
        "  ex:f each ex:C, each string\n"
        '  ex:g required, each one of ex:x "y" 3\n'
        "  ex:h same as ex:k, unique language\n"
        '  ex:i at most 2 is "on"\n'
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
        "  ex:a each length 3, each length at least 1, each length at most 8\n"
        "  ex:b each from 0 to 100, each up to 1.5\n"
        '  ex:c each pattern "^[A-Z]" flags "i", each language "en" "de"\n'
        "  ex:d each iri, each datatype ex:T\n"
        "  is one of ex:x\n"
        "}\n"
    )


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
        '    c: ex:C optional doc "if any"\n'
        "  }\n"
        "  ex:p required {\n"
        "    d: ex:D { ex:q required }\n"
        "    e: ex:E at least 2\n"
        "  }\n"
        "  ex:p at least 1 { f: ex:F }\n"
        "}\n"
    )
    with pytest.raises(TypeError, match="cannot be a row"):
        one(EX.p.has(x=EX.q.required()))


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
        "  when { is ex:Pump }\n"
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
        "  is cls\n"  # annotated `Class`: a class
        "  part\n"  # not annotated: a shape, composed
        "  prop at least n is cls\n"
        "  ex:q each part\n"
        "}\n\n"
        "shape Fallback(cls, other = cls) { is other }\n\n"
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
        "  pts each sensor\n"
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
