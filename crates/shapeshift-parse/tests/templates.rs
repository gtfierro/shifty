//! Bare terms, composition, and template arguments: named arguments,
//! defaults, blocks, and path templates.

use shapeshift_macro::build::{Program, build};
use shapeshift_macro::diagnostic::render_all;
use shapeshift_parse::{format, parse};
use shifty_algebra::render::describe_shape;

const PRE: &str = "prefix ex: <http://example.org/>\n";

/// The algebra of shape `name` in `src`, as text.
fn algebra(src: &str, name: &str) -> String {
    let src = format!("{PRE}{src}");
    let out = parse(&src, "t.shm");
    assert!(
        out.diagnostics.is_empty(),
        "{}",
        render_all(&out.diagnostics)
    );
    let built = build(&Program::single("t.shm", out.module));
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    describe_shape(&built.schema.arena, built.shapes[name])
}

/// `a` and `b` define shape `S` to mean the same thing.
fn same(a: &str, b: &str) {
    assert_eq!(algebra(a, "S"), algebra(b, "S"), "\n{a}\n  vs\n{b}");
}

fn errors(src: &str) -> String {
    let src = format!("{PRE}{src}");
    let out = parse(&src, "t.shm");
    let mut d = out.diagnostics.clone();
    if d.is_empty() {
        d = build(&Program::single("t.shm", out.module)).diagnostics;
    }
    render_all(&d)
}

fn expect_error(src: &str, want: &str) {
    let got = errors(src);
    assert!(got.contains(want), "{src}\nexpected {want:?} in\n{got}");
}

#[test]
fn a_bare_class_or_shape_is_a_test() {
    same("shape S { ex:p each ex:C }", "shape S { ex:p each a ex:C }");
    same(
        "shape S { ex:p at least 1 is ex:C }",
        "shape S { ex:p at least 1 is a ex:C }",
    );
    same("shape S { is ex:C }", "shape S { is a ex:C }");
    same(
        "shape T { ex:q required }\nshape S { ex:p each T }",
        "shape T { ex:q required }\nshape S { ex:p each matches T }",
    );
    // node equality is `one of`
    same(
        "shape S { ex:p at least 1 is one of ex:n }",
        "shape S { ex:p at least 1 is one of ex:n }",
    );
    // literals stay values
    assert!(algebra("shape S { ex:p each is \"x\" }", "S").contains("\"x\""));
}

#[test]
fn a_shape_alone_on_a_line_is_composed() {
    same(
        "shape T { ex:q required }\nshape S { T }",
        "shape S { ex:q required }",
    );
    same(
        "shape T(c) { ex:q each c }\nshape S { T(ex:C) }",
        "shape S { ex:q each ex:C }",
    );
    // `include` still means the same, with a warning
    let src = format!("{PRE}shape T {{ ex:q required }}\nshape S {{ include T }}");
    let out = parse(&src, "t.shm");
    let msg = render_all(&out.diagnostics);
    assert!(
        msg.contains("`include` is deprecated: write `T` alone on the line"),
        "{msg}"
    );
    assert_eq!(
        format(&src, "t.shm").unwrap(),
        format!("{PRE}\nshape T {{ ex:q required }}\nshape S {{ T }}\n")
    );
}

#[test]
fn a_parameter_used_as_a_test_takes_a_class_or_a_shape() {
    let t = "shape T(x) { ex:p each x }\nshape U { ex:q required }\n";
    same(
        &format!("{t}shape S {{ T(ex:C) }}"),
        "shape S { ex:p each a ex:C }",
    );
    same(
        &format!("{t}shape S {{ T(U) }}"),
        "shape U { ex:q required }\nshape S { ex:p each matches U }",
    );
    // and alone on a line
    same(
        "shape W(x) { x }\nshape S { W(ex:C) }",
        "shape S { is a ex:C }",
    );
}

#[test]
fn named_arguments_and_defaults() {
    let t = "shape T(cls, room = ex:Room, n = 1) { ex:p at least n is cls\n ex:in each room }\n";
    same(
        &format!("{t}shape S {{ T(ex:C) }}"),
        "shape S { ex:p at least 1 is a ex:C\n ex:in each a ex:Room }",
    );
    same(
        &format!("{t}shape S {{ T(ex:C, n = 2) }}"),
        "shape S { ex:p at least 2 is a ex:C\n ex:in each a ex:Room }",
    );
    same(
        &format!("{t}shape S {{ T(cls = ex:C, room = ex:Space) }}"),
        "shape S { ex:p at least 1 is a ex:C\n ex:in each a ex:Space }",
    );
    // a default may use the parameters before it
    same(
        "shape T(cls, other = cls) { ex:p each cls\n ex:q each other }\nshape S { T(ex:C) }",
        "shape S { ex:p each a ex:C\n ex:q each a ex:C }",
    );
}

#[test]
fn an_inline_shape_is_an_argument_like_any_other() {
    same(
        "shape W(c, body) { is c\n body }\nshape S { W(ex:C, { ex:p required }) }",
        "shape S { is a ex:C\n ex:p required }",
    );
    same(
        "shape W(c, body) { is c\n body }\nshape S { W(ex:C, body = { ex:p required }) }",
        "shape S { is a ex:C\n ex:p required }",
    );
    // a `{ ... }` after a test is another test of each value
    same(
        "shape U { is ex:C }\nshape S { ex:q each U { ex:p required } }",
        "shape U { is ex:C }\nshape S { ex:q each { is a ex:C; ex:p required } }",
    );
    same(
        "shape S { ex:q each ex:C { ex:p required } }",
        "shape S { ex:q each { is a ex:C; ex:p required } }",
    );
}

#[test]
fn path_templates() {
    let p = "path downstream(p = ex:hasPart?) = ex:feeds+/p\n";
    same(
        &format!("{p}shape S {{ downstream(ex:hasPoint) required }}"),
        "shape S { ex:feeds+/ex:hasPoint required }",
    );
    same(
        &format!("{p}shape S {{ downstream required }}"),
        "shape S { ex:feeds+/ex:hasPart? required }",
    );
    same(
        &format!("{p}shape S {{ downstream(p = ex:x) each ex:C }}"),
        "shape S { ex:feeds+/ex:x each a ex:C }",
    );
    // in a group head
    same(
        &format!("{p}shape S {{ downstream(ex:hasPoint) {{ t: ex:T }} }}"),
        "shape S { t: ex:feeds+/ex:hasPoint exactly 1 is a ex:T }",
    );
    same(
        &format!("{p}shape S {{ (downstream(ex:hasPoint)) {{ t: ex:T }} }}"),
        "shape S { t: ex:feeds+/ex:hasPoint exactly 1 is a ex:T }",
    );
    // as a template argument
    same(
        &format!("{p}shape T(q) {{ q required }}\nshape S {{ T(downstream(ex:y)) }}"),
        "shape S { ex:feeds+/ex:y required }",
    );
}

#[test]
fn a_row_body_adds_lines_to_its_item() {
    same(
        "path points = ex:hasPoint\nshape W(c) { is c }\nshape S { points { x: W(ex:X) { ex:unit required } } }",
        "shape W(c) { is c }\nshape S { x: ex:hasPoint exactly 1 is { is W(ex:X); ex:unit required } }",
    );
}

/// The local names (after `#`) the build gives slots, sorted.
fn names(src: &str) -> Vec<String> {
    let src = format!("{PRE}{src}");
    let out = parse(&src, "t.shm");
    assert!(
        out.diagnostics.is_empty(),
        "{}",
        render_all(&out.diagnostics)
    );
    let built = build(&Program::single("t.shm", out.module));
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    let mut v: Vec<String> = built
        .schema
        .names
        .values()
        .flatten()
        .filter_map(|n| n.rsplit_once('#').map(|(_, l)| l.to_string()))
        .collect();
    v.sort();
    v
}

fn has(got: &[String], want: &[&str]) {
    for w in want {
        assert!(got.iter().any(|g| g == w), "no {w} in {got:?}");
    }
}

#[test]
fn labels_name_what_their_lines_make() {
    let t = "path points = ex:hasPoint\n\
             shape vfd(cls) { is cls\n points { speed: ex:Speed; status: ex:Status } }\n";
    // through a label, a template's names nest under it; without one, they
    // belong to the shared expansion
    let got = names(&format!(
        "{t}shape ahu {{\n ex:hasPart {{ sa: vfd(ex:SF) }}\n fan: vfd(ex:F)\n vfd(ex:EF) }}"
    ));
    has(
        &got,
        &[
            "ahu",
            "ahu.fan",
            "ahu.fan.speed",
            "ahu.fan.status",
            "ahu.sa",
            "ahu.sa.speed",
            "ahu.sa.status",
        ],
    );
    assert!(
        got.iter()
            .any(|g| g.starts_with("vfd-") && g.ends_with(".speed")),
        "{got:?}"
    );
    // names written on a labelled line nest too: in inline shapes and in
    // blocks passed to calls
    let got = names(
        "shape W(body) { body }\n\
         shape S {\n sat: ex:p each { unit: ex:u required }\n w: W({ x: ex:q required }) }\n\
         shape R { S.sat.unit }",
    );
    has(&got, &["S.sat", "S.sat.unit", "S.w", "S.w.x"]);
    // a template that calls itself under a label shares the inner expansion
    names("shape tree(c) { is c\n kid: ex:child each tree(c) }\nshape S { t: tree(ex:C) }");
}

#[test]
fn keywords_can_be_labels() {
    // the colon sets a label apart, so `value:` is not the keyword `value`
    let got = names(
        "shape S {\n value: ex:p required\n length: ex:q each string\n \
         ex:hasPoint { is: ex:A; of: ex:B }\n}\nshape R { S.value }",
    );
    has(&got, &["S.value", "S.length", "S.is", "S.of"]);
    formats_as(
        "shape S {\n  value: ex:p required\n  ex:q { node: ex:N }\n}\n",
        "shape S {\n  value: ex:p required\n  ex:q { node: ex:N }\n}\n",
    );
}

#[test]
fn argument_errors_explain_themselves() {
    let t = "shape T(cls, room = ex:Room) { ex:p each cls }\n";
    let cases = [
        ("shape S { T() }", "`T` needs an argument for `cls`"),
        ("shape S { T(ex:C, rooms = ex:R) }", "did you mean `room`?"),
        (
            "shape S { T(ex:C, room = ex:R) { ex:q required } }",
            "to pass the lines to `T`, write `T(ex:C, room = ex:R, { ... })`",
        ),
        (
            "shape U { ex:q required }\nshape S { U { ex:p required } }",
            "`U` is a shape, so `U { ... }` is not a group",
        ),
        (
            "shape S { ex:q at least 1 is ex:C { ex:p required } }",
            "write `is { is ex:C; ... }`",
        ),
        (
            "shape S { T(ex:C, ex:R, ex:Q) }",
            "`T` takes 2 arguments, but was given 3",
        ),
        (
            "shape S { T(cls = ex:C, ex:R) }",
            "a positional argument after the named argument `cls`",
        ),
        ("shape S { T(ex:C, cls = ex:D) }", "`cls` is given twice"),
        (
            "shape U(x = ex:A, y) { ex:p each x }",
            "parameter `y` needs a default",
        ),
        ("shape S { ex:p ex:C }", "write `each ex:C`"),
        ("shape S { nope(ex:x) required }", "no path named `nope`"),
        (
            "shape V(c) { x: ex:p each c }\nshape S { k: ex:q each V(ex:C) at least 1 is V(ex:D) }",
            "`S.k.x` names two lines",
        ),
    ];
    for (body, want) in cases {
        expect_error(&format!("{t}{body}"), want);
    }
}

/// `src`, run through the formatter, is `want`; and `want` is a fixed point.
fn formats_as(src: &str, want: &str) {
    let got = format(src, "t.shm").unwrap();
    assert_eq!(got, want, "\n--- formatted ---\n{got}");
    assert_eq!(format(want, "t.shm").unwrap(), want, "not a fixed point");
}

#[test]
fn long_lines_wrap() {
    let pre = "prefix ex: <http://example.org/>\n\n\
               shape T(cls, room = ex:Room) { ex:p each cls }\n\n";
    // a call that does not fit takes one argument per line
    formats_as(
        &format!(
            "{pre}shape S {{\n  sensor: T(ex:A_Rather_Long_Sensor_Class_Name, room = ex:A_Rather_Long_Room_Class_Name_Indeed)\n}}\n"
        ),
        &format!(
            "{pre}shape S {{\n  sensor: T(\n    ex:A_Rather_Long_Sensor_Class_Name,\n    room = ex:A_Rather_Long_Room_Class_Name_Indeed,\n  )\n}}\n"
        ),
    );
    // a field that does not fit opens the inline shape that ends it
    formats_as(
        &format!(
            "{pre}shape S {{\n  fed: ex:feeds at least 1 is {{ is ex:HVAC_Zone; ex:hasPoint required; ex:hasPart required }}\n}}\n"
        ),
        &format!(
            "{pre}shape S {{\n  fed: ex:feeds at least 1 is {{\n    is ex:HVAC_Zone\n    ex:hasPoint required\n    ex:hasPart required\n  }}\n}}\n"
        ),
    );
    // and a comment inside an inline shape keeps it open
    formats_as(
        &format!("{pre}shape S {{\n  ex:p each {{\n    -- why\n    ex:q required\n  }}\n}}\n"),
        &format!("{pre}shape S {{\n  ex:p each {{\n    -- why\n    ex:q required\n  }}\n}}\n"),
    );
    // when one branch needs lines of its own, every branch gets them
    formats_as(
        &format!(
            "{pre}shape S {{\n  any of {{ ex:a required }} {{ ex:b required\n -- note\n ex:c required }}\n}}\n"
        ),
        &format!(
            "{pre}shape S {{\n  any of {{\n    ex:a required\n  }} {{\n    ex:b required\n    -- note\n    ex:c required\n  }}\n}}\n"
        ),
    );
}

#[test]
fn new_forms_survive_formatting() {
    let src = r#"prefix ex: <http://example.org/>

path downstream(p = ex:hasPart?) = ex:feeds+/p

shape T(cls, room = ex:Room, n = 1) { ex:p at least n is cls }
shape W(body) { body }

shape S {
  T(ex:C, n = 2)
  W({ ex:p required; ex:q each ex:C })
  W({
    -- a comment keeps the block open
    ex:p required
  })
  downstream(ex:x) { first: ex:A }
  ex:s each T(ex:D), each { ex:t required }
  fan: W(body = { speed: ex:p required })
  pump: T(ex:P)
  is one of ex:n
}
"#;
    let once = format(src, "t.shm").unwrap();
    assert_eq!(once, src, "\n--- formatted ---\n{once}");
}
