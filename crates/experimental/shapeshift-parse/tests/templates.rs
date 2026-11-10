//! Tests, composition, and template arguments: named arguments, defaults,
//! blocks, and path templates. And the one way of writing each of them.

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
fn every_test_names_its_kind() {
    let cases = [
        (
            "shape S { ex:p each ex:C }",
            "write `a ex:C` for instances of the class, or `one of ex:C` for the node itself",
        ),
        ("shape S { ex:p at least 1 is ex:C }", "write `a ex:C`"),
        ("shape S { is ex:C }", "write `a ex:C`"),
        (
            "shape T { ex:q required }\nshape S { ex:p each T }",
            "write `matches T` for a shape",
        ),
        ("shape S { is \"x\" }", "write `one of \"x\"`"),
        (
            "shape S { ex:p at least 1 is \"x\" }",
            "write `one of \"x\"`",
        ),
        (
            "shape S { ex:p each is a ex:C }",
            "`is` here is not part of the language",
        ),
        (
            "shape T { ex:q required }\nshape S { ex:p at least 1 is matches T }",
            "write `at least 1 matches T`",
        ),
        (
            "shape T { ex:q required }\nshape S { is matches T }",
            "a shape alone on a line composes it: write `T`",
        ),
        ("shape S { ex:p string }", "write `each string ...`"),
    ];
    for (src, want) in cases {
        expect_error(src, want);
    }
    // a qualified count of a shape says `matches`
    same(
        "shape T { ex:q required }\nshape S { ex:p at least 1 matches T }",
        "shape T { ex:q required }\nshape S { ex:p at least 1 is { T } }",
    );
}

#[test]
fn removed_spellings_name_their_replacement() {
    let cases = [
        (
            "shape S { ex:p at least 1 }",
            "`at least 1` is written `required`",
        ),
        (
            "shape S { ex:p at most 1 }",
            "`at most 1` is written `optional`",
        ),
        ("shape S { ex:p between 0 and 1 }", "write `optional`"),
        ("shape S { ex:p between 2 and 2 }", "write `exactly 2`"),
        ("shape S { ex:p between 0 and 3 }", "write `at most 3`"),
        ("shape S { ex:p { ex:C at least 1 } }", "write `required`"),
        ("shape S { ex:a less than ex:b }", "write `below ex:b`"),
        ("shape S { ex:a at most ex:b }", "write `up to ex:b`"),
        (
            "shape S { ex:a above ex:b }",
            "SHACL compares the other way round",
        ),
        (
            "shape S { ex:a each length 1 to 8 }",
            "write `length from 1 to 8`",
        ),
        (
            "shape S { ex:a each length at least 2 }",
            "write `length from 2`",
        ),
        (
            "shape S { ex:a each length at most 8 }",
            "write `length up to 8`",
        ),
        (
            "shape S { ex:a each from 0 up to 1 }",
            "write `from 0 to ...`",
        ),
        (
            "shape S { ex:a each datatype xsd:string }",
            "write `string`",
        ),
        ("shape S { ex:a is \"x\" }", "write `each one of \"x\"`"),
        (
            "shape S { ex:a is a ex:C }",
            "a property's values are tested with `each`",
        ),
        (
            "shape S { all of { ex:a required } { ex:b required } }",
            "`all of` is not part of the language",
        ),
        (
            "shape S { at least 1 of { ex:a required } }",
            "write `any of`",
        ),
        (
            "shape S { at most 0 of { ex:a required } }",
            "write `none of`",
        ),
        (
            "shape T { ex:q required }\nshape S { include T }",
            "a shape alone on a line composes it: write `T`",
        ),
        (
            "shape S { f: {\n ex:p\n required\n doc \"d\"\n } }",
            "write `f: ex:p required`, with its `doc`",
        ),
        (
            "shape S { ex:p { ex:A doc \"d\" } }",
            "`doc` goes on the line before the row it describes",
        ),
        (
            "shape S { ex:a each at least 5 }",
            "a count goes right after the property",
        ),
        (
            "shape S { ex:a each between 1 and 5 }",
            "for a range of values, write `from 5`",
        ),
    ];
    for (src, want) in cases {
        expect_error(src, want);
    }
}

#[test]
fn counts_mean_the_same_on_fields_and_rows() {
    // `optional` is at most one, on a field as in a row
    let p = "<http://example.org/p>";
    assert_eq!(
        algebra("shape S { ex:p optional }", "S"),
        format!("∃[..1] {p}")
    );
    assert!(algebra("shape S { ex:p { ex:C optional } }", "S").starts_with(&format!("∃[..1] {p}")));
    assert_eq!(
        algebra("shape S { ex:p required }", "S"),
        format!("∃[1..] {p}")
    );
    assert!(algebra("shape S { ex:p { ex:C required } }", "S").starts_with(&format!("∃[1..] {p}")));
}

#[test]
fn ranges_and_comparisons() {
    let range = |test: &str| algebra(&format!("shape S {{ ex:p each {test} }}"), "S");
    assert!(
        range("above 0 below 100").contains(r#"range(>"0"^^xsd:integer, <"100"^^xsd:integer)"#)
    );
    assert!(
        range("from 0 to 100").contains(r#"range(≥"0"^^xsd:integer, ≤"100"^^xsd:integer)"#),
        "{}",
        range("from 0 to 100")
    );
    assert!(range("up to 1.5").contains(r#"range(≤"1.5"^^xsd:decimal)"#));
    assert!(range("above 0").contains(r#"range(>"0"^^xsd:integer)"#));
    assert!(algebra("shape S { ex:start below ex:end }", "S").contains("example.org/end"));
    assert!(algebra("shape S { ex:low up to ex:high }", "S").contains("example.org/high"));
    expect_error("shape S { ex:p each above 5 below 5 }", "can never hold");
    expect_error("shape S { ex:p below 5 }", "write `each below ...`");
}

#[test]
fn class_unions() {
    same(
        "shape S { ex:p each a ex:A or ex:B }",
        "shape S { ex:p each { any of { is a ex:A } { is a ex:B } } }",
    );
    same(
        "shape S { is a ex:A or ex:B or ex:C }",
        "shape S { any of { is a ex:A } { is a ex:B } { is a ex:C } }",
    );
    same(
        "shape S { ex:p at least 2 is a ex:A or ex:B }",
        "shape S { ex:p at least 2 is { any of { is a ex:A } { is a ex:B } } }",
    );
    // a class parameter can be one of them
    same(
        "shape T(cls) { ex:p each a cls or ex:B }\nshape S { T(ex:A) }",
        "shape S { ex:p each a ex:A or ex:B }",
    );
}

#[test]
fn ranges_of_dates_and_times() {
    let range = |test: &str| algebra(&format!("shape S {{ ex:p each {test} }}"), "S");
    let got = range("from 2024-01-01 below 2025-01-01");
    assert!(
        got.contains(r#"range(≥"2024-01-01"^^xsd:date, <"2025-01-01"^^xsd:date)"#),
        "{got}"
    );
    let got = range("above 2024-01-31T08:00:00Z");
    assert!(
        got.contains(r#"range(>"2024-01-31T08:00:00Z"^^xsd:dateTime)"#),
        "{got}"
    );
    let got = range("from 08:00:00 to 18:30:00.5");
    assert!(
        got.contains(r#"range(≥"08:00:00"^^xsd:time, ≤"18:30:00.5"^^xsd:time)"#),
        "{got}"
    );
    // a date is a value, so `one of` takes dates too
    let got = range("one of 2024-12-25 2024-12-26");
    assert!(got.contains(r#""2024-12-25"^^xsd:date"#), "{got}");
    // the datatype and a range of its values are satisfiable together
    let got = range("date, each from 2024-01-01");
    assert!(got.contains("range("), "{got}");
    expect_error(
        "shape S { ex:p each from 2024-13-01 }",
        "`2024-13-01` is not a valid xsd:date",
    );
    expect_error(
        "shape S { ex:p each from 25:00:00 }",
        "`25:00:00` is not a valid xsd:time",
    );
    expect_error(
        "shape S { ex:p each from 0 to 2024-01-01 }",
        "compares a number with a date",
    );
    expect_error(
        "shape S { ex:p each from 2024-01-01 to 2024-01-01T00:00:00 }",
        "compares a date with a dateTime",
    );
    expect_error(
        "shape S { ex:p each from 2025-01-01 to 2024-01-01 }",
        "can never hold: no date is in it",
    );
    expect_error(
        "shape S { ex:p each from \"2024\" }",
        "expected a number, a date, or a time",
    );
}

#[test]
fn at_most_n_of() {
    same(
        "shape S { at most 1 of { ex:a required } { ex:b required } }",
        "shape S { none of { ex:a required; ex:b required } }",
    );
}

#[test]
fn only_these_properties_allows_rdf_type() {
    assert!(
        algebra("shape S { ex:a required\n only these properties }", "S").contains("rdf:type"),
        "{}",
        algebra("shape S { ex:a required\n only these properties }", "S")
    );
}

#[test]
fn warnings_for_lines_that_mislead() {
    expect_error(
        "shape S { rdfs:label each string, each language \"en\" }",
        "no value can be both `string` and `language ...`",
    );
    expect_error(
        "shape S { ex:p { ex:C; ex:C } }",
        "for 2 of them, write one row: `ex:C exactly 2`",
    );
}

#[test]
fn a_shape_alone_on_a_line_is_composed() {
    same(
        "shape T { ex:q required }\nshape S { T }",
        "shape S { ex:q required }",
    );
    same(
        "shape T(c) { ex:q each a c }\nshape S { T(ex:C) }",
        "shape S { ex:q each a ex:C }",
    );
}

#[test]
fn a_parameter_used_as_a_test_says_what_it_stands_for() {
    same(
        "shape T(x) { ex:p each a x }\nshape S { T(ex:C) }",
        "shape S { ex:p each a ex:C }",
    );
    same(
        "shape T(x) { ex:p each matches x }\nshape U { ex:q required }\nshape S { T(U) }",
        "shape U { ex:q required }\nshape S { ex:p each matches U }",
    );
    // alone on a line, a parameter is the class or shape it is given
    same(
        "shape W(x) { x }\nshape S { W(ex:C) }",
        "shape S { is a ex:C }",
    );
}

#[test]
fn named_arguments_and_defaults() {
    let t =
        "shape T(cls, room = ex:Room, n = 1) { ex:p at least n is a cls\n ex:in each a room }\n";
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
        "shape T(cls, other = cls) { ex:p each a cls\n ex:q each a other }\nshape S { T(ex:C) }",
        "shape S { ex:p each a ex:C\n ex:q each a ex:C }",
    );
}

#[test]
fn an_inline_shape_is_an_argument_like_any_other() {
    same(
        "shape W(c, body) { is a c\n body }\nshape S { W(ex:C, { ex:p required }) }",
        "shape S { is a ex:C\n ex:p required }",
    );
    same(
        "shape W(c, body) { is a c\n body }\nshape S { W(ex:C, body = { ex:p required }) }",
        "shape S { is a ex:C\n ex:p required }",
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
        &format!("{p}shape S {{ downstream(p = ex:x) each a ex:C }}"),
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
        "path points = ex:hasPoint\nshape W(c) { is a c }\nshape S { points { x: W(ex:X) { ex:unit required } } }",
        "shape W(c) { is a c }\nshape S { x: ex:hasPoint exactly 1 is { W(ex:X); ex:unit required } }",
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
             shape vfd(cls) { is a cls\n points { speed: ex:Speed; status: ex:Status } }\n";
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
    names(
        "shape tree(c) { is a c\n kid: ex:child each matches tree(c) }\nshape S { t: tree(ex:C) }",
    );
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
    let t = "shape T(cls, room = ex:Room) { ex:p each a cls }\n";
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
            "shape S { ex:q at least 1 is a ex:C { ex:p required } }",
            "write `is { is a ex:C; ... }`",
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
            "shape U(x = ex:A, y) { ex:p each a x }",
            "parameter `y` needs a default",
        ),
        ("shape S { ex:p ex:C }", "write `each a ex:C`"),
        ("shape S { nope(ex:x) required }", "no path named `nope`"),
        (
            "shape V(c) { x: ex:p each a c }\nshape S { k: ex:q each matches V(ex:C), at least 1 matches V(ex:D) }",
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
               shape T(cls, room = ex:Room) { ex:p each a cls }\n\n";
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
            "{pre}shape S {{\n  fed: ex:feeds at least 1 is {{ is a ex:HVAC_Zone; ex:hasPoint required; ex:hasPart required }}\n}}\n"
        ),
        &format!(
            "{pre}shape S {{\n  fed: ex:feeds at least 1 is {{\n    is a ex:HVAC_Zone\n    ex:hasPoint required\n    ex:hasPart required\n  }}\n}}\n"
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

shape T(cls, room = ex:Room, n = 1) { ex:p at least n is a cls }
shape W(body) { body }

shape S {
  T(ex:C, n = 2)
  W({ ex:p required; ex:q each a ex:C })
  W({
    -- a comment keeps the block open
    ex:p required
  })
  downstream(ex:x) { first: ex:A }
  ex:s each matches T(ex:D), each { ex:t required }
  ex:m at least 2 matches W({ ex:z required })
  fan: W(body = { speed: ex:p required })
  pump: T(ex:P)
  is one of ex:n
  ex:v each above 0 below 100, each length from 1 to 8
  ex:w each from 0 to 1.5, each up to 2
  ex:start below ex:end
  ex:low up to ex:high
  at most 1 of { ex:a required } { ex:b required }
  ex:kind each a ex:A or ex:B, each one of 2024-12-25 08:00:00
  ex:made each from 2020-01-01 below 2025-01-01T00:00:00Z
  is a ex:Thing or ex:Other
  ex:hasPoint {
    doc "outdoor air"
    oat: ex:OAT optional
    rat: ex:RAT
  }
}
"#;
    let once = format(src, "t.shm").unwrap();
    assert_eq!(once, src, "\n--- formatted ---\n{once}");
}
