//! Path aliases and field groups: parsing, desugaring to named fields, and
//! their errors.

use shapeshift_macro::build::{Program, build};
use shapeshift_macro::diagnostic::render_all;
use shapeshift_macro::display::format_module;
use shapeshift_macro::groups::desugar;
use shapeshift_parse::parse;

fn desugared(src: &str) -> String {
    let out = parse(src, "t.shm");
    assert!(
        out.diagnostics.is_empty(),
        "{}",
        render_all(&out.diagnostics)
    );
    let (p, diags) = desugar(&Program::single("t.shm", out.module));
    assert!(diags.is_empty(), "{}", render_all(&diags));
    format_module(&p.modules[0].module)
}

fn errors(src: &str) -> String {
    let out = parse(src, "t.shm");
    assert!(
        out.diagnostics.is_empty(),
        "{}",
        render_all(&out.diagnostics)
    );
    render_all(&build(&Program::single("t.shm", out.module)).diagnostics)
}

fn parse_errors(src: &str) -> String {
    render_all(&parse(src, "t.shm").diagnostics)
}

fn expect(got: &str, want: &str) {
    assert_eq!(got, want, "\n--- got ---\n{got}");
}

#[test]
fn rows_become_named_qualified_fields() {
    let src = r#"prefix brick: <https://brickschema.org/schema/Brick#>
path points = brick:hasPoint

shape sensor { is a brick:Temperature_Sensor }

shape vav {
  is a brick:VAV
  points {
    sat: brick:Supply_Air_Temperature_Sensor
    doc "if metered"
    flow: brick:Air_Flow_Sensor optional
    sensor
  }
  points between 1 and 3 { dmp: brick:Damper_Position_Command }
  brick:feeds+/brick:hasPart? required { zone: brick:HVAC_Zone }
}
"#;
    expect(
        &desugared(src),
        r#"prefix brick: <https://brickschema.org/schema/Brick#>

path points = brick:hasPoint

shape sensor { is a brick:Temperature_Sensor }

shape vav {
  is a brick:VAV
  sat: brick:hasPoint exactly 1 is a brick:Supply_Air_Temperature_Sensor
  doc "if metered"
  flow: brick:hasPoint at most 1 is a brick:Air_Flow_Sensor
  sensor: brick:hasPoint exactly 1 matches sensor
  dmp: brick:hasPoint at least 1 is a brick:Damper_Position_Command, at most 3 is a brick:Damper_Position_Command
  zone: brick:feeds+/brick:hasPart? at least 1 is a brick:HVAC_Zone
}
"#,
    );
}

#[test]
fn aliases_stand_for_their_paths_everywhere() {
    let src = r#"prefix ex: <http://example.org/>
path points = ex:hasPoint
path downstream = ex:feeds+/points
path label = ex:label

shape T(p) { p required }

shape S
  for every node with points
{
  points each a ex:Point
  ^downstream optional
  T(downstream)
  ex:name same as label
  ex:part each matches T(points)
}
"#;
    expect(
        &desugared(src),
        r#"prefix ex: <http://example.org/>

path points = ex:hasPoint
path downstream = ex:feeds+/ex:hasPoint
path label = ex:label

shape T(p) { p required }

shape S for every node with ex:hasPoint {
  ex:hasPoint each a ex:Point
  ^(ex:feeds+/ex:hasPoint) optional
  T(ex:feeds+/ex:hasPoint)
  ex:name same as ex:label
  ex:part each matches T(ex:hasPoint)
}
"#,
    );
}

#[test]
fn a_parameter_shadows_an_alias() {
    let src = "prefix ex: <http://example.org/>\npath p = ex:q\nshape T(p) { p required }\n";
    assert!(desugared(src).contains("shape T(p) { p required }"));
}

#[test]
fn path_and_group_errors_explain_themselves() {
    let pre = "prefix ex: <http://example.org/>\npath points = ex:hasPoint\n";
    let cases = [
        (
            "shape ahu { pionts { x: ex:X } }",
            "did you mean the path `points`?",
        ),
        (
            "shape ahu { parts { x: ex:X } }",
            "name a path: `path parts = brick:hasPoint`",
        ),
        ("path points = ex:p", "path `points` is declared twice"),
        (
            "shape points { is a ex:P }",
            "`points` names both a path and a shape",
        ),
        (
            "path fed = ex:x/feeding\npath feeding = fed\nshape s { fed required }",
            "is defined in terms of itself",
        ),
        (
            "path chain = ex:a/ex:b\nshape s { ex:c same as chain }",
            "`same as` needs a single property, but `chain` is the path `ex:a/ex:b`",
        ),
        (
            "shape ahu { points { x: ex:X; x: ex:Y } }",
            "field `x` is defined twice in shape `ahu`",
        ),
    ];
    for (body, want) in cases {
        let got = errors(&format!("{pre}{body}\n"));
        assert!(got.contains(want), "{body}: expected {want:?} in\n{got}");
    }
}

#[test]
fn group_syntax_errors_explain_themselves() {
    let cases = [
        (
            "path points brick:hasPoint",
            "expected `=` after `path points`",
        ),
        (
            "shape s { ex:p | ex:q required }",
            "alternative paths are written `p or q`",
        ),
        (
            "shape s { sat: brick:hasPoint { x: brick:X } }",
            "a group has no name of its own",
        ),
        (
            "shape s { brick:hasPoint { x: brick:X brick:Y } }",
            "in a group row",
        ),
        ("shape s { brick:hasPoint { x: brick:X }", "never closed"),
    ];
    for (src, want) in cases {
        let got = parse_errors(src);
        assert!(got.contains(want), "{src}: expected {want:?} in\n{got}");
    }
}

#[test]
fn an_empty_program_builds() {
    let out = build(&Program::default());
    assert!(!out.has_errors(), "{}", render_all(&out.diagnostics));
}
