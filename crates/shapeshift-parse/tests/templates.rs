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
        "shape T { ex:q required }\nshape S { include T }",
    );
    same(
        "shape T(c) { ex:q each c }\nshape S { T(ex:C) }",
        "shape T(c) { ex:q each c }\nshape S { include T(ex:C) }",
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
fn a_block_after_a_call_is_its_last_argument() {
    same(
        "shape W(c, body) { is c\n body }\nshape S { W(ex:C) { ex:p required } }",
        "shape S { is a ex:C\n ex:p required }",
    );
    // `include` and tests take blocks too
    same(
        "shape W(body) { ex:q each body }\nshape S { include W() { ex:p required } }",
        "shape S { ex:q each { ex:p required } }",
    );
    // an inline shape is an argument like any other
    same(
        "shape W(c, body) { is c\n body }\nshape S { W(ex:C, { ex:p required }) }",
        "shape S { is a ex:C\n ex:p required }",
    );
    // after a shape that is not a template, a block adds lines
    same(
        "shape U { is ex:C }\nshape S { ex:q each U { ex:p required } }",
        "shape U { is ex:C }\nshape S { ex:q each { is a ex:C; ex:p required } }",
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
    // in a group head, in parentheses
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
fn rows_hand_their_block_to_a_template() {
    same(
        "path points = ex:hasPoint\nshape W(c, body) { is c\n body }\nshape S { points { x: W(ex:X) { ex:unit required } } }",
        "shape S { x: ex:hasPoint exactly 1 is { is a ex:X; ex:unit required } }",
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
            "the block after `T(...)` is its last argument, `room`, which is already given",
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
        (
            "shape S { ex:p each ex:C { ex:q required } }",
            "a class takes no block",
        ),
        ("shape S { nope(ex:x) required }", "no path named `nope`"),
    ];
    for (body, want) in cases {
        expect_error(&format!("{t}{body}"), want);
    }
}

#[test]
fn new_forms_survive_formatting() {
    let src = r#"prefix ex: <http://example.org/>

path downstream(p = ex:hasPart?) = ex:feeds+/p

shape T(cls, room = ex:Room, n = 1) { ex:p at least n is cls }
shape W(body) { body }

shape S {
  T(ex:C, n = 2)
  W() { ex:p required; ex:q each ex:C }
  W() {
    -- a comment keeps the block open
    ex:p required
  }
  include W() { ex:r required }
  (downstream(ex:x)) { first: ex:A }
  ex:s each T(ex:D) { ex:t required }
  is one of ex:n
}
"#;
    let once = format(src, "t.shm").unwrap();
    assert_eq!(once, src, "\n--- formatted ---\n{once}");
}
