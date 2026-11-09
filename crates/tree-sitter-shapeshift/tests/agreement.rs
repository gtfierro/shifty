//! The tree-sitter grammar and the compiler's parser must agree on what is
//! syntactically valid, or the REPL and editors would flag correct code (or
//! miss broken code).

use std::path::PathBuf;

fn parser() -> tree_sitter::Parser {
    let mut p = tree_sitter::Parser::new();
    p.set_language(&tree_sitter_shapeshift::LANGUAGE.into())
        .unwrap();
    p
}

fn ts_ok(src: &str) -> bool {
    let tree = parser().parse(src, None).unwrap();
    !tree.root_node().has_error()
}

fn compiler_ok(src: &str) -> bool {
    !shapeshift_parse::parse(src, "t.shm").has_errors()
}

const VALID: &[&str] = &[
    "shape S { }",
    "shape S { ex:p required }",
    "shape S for every ex:C { ex:p required; ex:q optional }",
    "shape S\n  for every ex:C\n  for node ex:n\n{\n  ex:p at least 1 is a ex:D\n}",
    "shape T(x, y) { x each matches y }",
    "shape S { name: ex:p exactly 1, each a ex:D }",
    "shape S { name: {\n  ex:p\n  exactly 1\n  doc \"d\"\n} }",
    "shape S { any of { ex:a required } { ex:b required } }",
    "shape S { when { ex:a required }\n  then { ex:b required } }",
    "shape S { at least 2 of { ex:a required } { ex:b required } { ex:c required } }",
    "shape S { ex:p/ex:q or ^ex:r each iri }",
    "shape S { ex:p each length 1 to 5, each pattern \"^a\" flags \"i\" }",
    "shape S { ex:p each from 0 to 1.5 }",
    "shape S { ex:p each up to 10 }",
    "shape S { ex:p each one of ex:a ex:b \"c\"@en 3 true }",
    "shape S { ex:start less than ex:end }",
    "shape S { ex:a at most ex:b }",
    "shape S { only these properties }",
    "shape S for sparql \"SELECT ?this WHERE { ?this a ?c }\" { ex:p required }",
    "doc \"x\"\nseverity warning\nshape S { message \"m\"\n ex:p required }",
    "prefix ex: <http://ex/>\nbase <http://ex/b>\nimport \"./x.shm\" as m\n",
    "shape S { include m.Sensor\n include T(ex:a, ^ex:b) }",
    "shape S { ex:p required -- trailing comment\n}",
    "path points = brick:hasPoint\npath downstream = brick:feeds+/brick:hasPart?\n",
    "shape S { is a ex:C\n is ex:n }",
    "shape S { ex:p each { ex:q required } }",
    "shape S { ex:p exactly 1 is { is a ex:C } }",
    "shape T(x) { x required }",
    "shape S { points { sat: ex:SAT\n ex:RAT optional doc \"if any\"\n Fan } }",
    "shape S { points between 1 and 3 { dmp: ex:Dmp } }",
    "shape S { brick:hasPoint { sat: ex:SAT } }",
    "shape S { ^brick:feeds/brick:hasPart? required { ahu: ex:AHU } }",
    "shape S { <http://ex/p> { ex:A } }",
    "shape S { parts required {\n  doc \"coil\"\n  hw: ex:Coil { points { HC } }\n} }",
    "shape S { parts { fan: vfd(ex:Fan) at least 2 } }",
    "shape S {\n  any of { ex:a required }\n         { ex:b required }\n}",
    // bare terms, composition, and template arguments
    "shape S { ex:p each ex:C }",
    "shape S { ex:p at least 1 is ex:C }",
    "shape S { ex:p each Sensor }",
    "shape S { ex:p each T(ex:C) { ex:q required } }",
    "shape S { ex:p each matches T() { ex:q required } }",
    "shape S { is ex:C }",
    "shape S { is \"x\" }",
    "shape S { is one of ex:n }",
    "shape S { Sensor }",
    "shape S { T(ex:C) }",
    "shape S { T(ex:C, room = ex:R, n = 2) }",
    "shape S { W() { ex:p required } }",
    "shape S { W(ex:C) {\n  ex:p required\n} }",
    "shape S { include W() { ex:p required } }",
    "shape S { T(ex:C, { ex:p required }) }",
    "shape S { T(U(ex:C) { ex:p required }) }",
    "shape T(cls, room = ex:Room, n = 1) { ex:p at least n is cls }",
    "path d(p = ex:x?) = ex:f+/p",
    "path d(p) = ex:f+/p\nshape S { d(ex:y) required }",
    "shape S { d(p = ex:y)/ex:z each ex:C }",
    "shape S { (d(ex:y)) { t: ex:T } }",
    "shape S { points { x: W(ex:X) { ex:unit required } } }",
];

const INVALID: &[&str] = &[
    "shape { }",
    "shape S { ex:p required",
    "shape S ex:p required }",
    "shape S { when { ex:a required } }",
    "shape S { ex:p at least }",
    "shape S { ex:p ex:q }",
    "shape S for { }",
    "prefix ex <http://ex/>",
    "path points brick:hasPoint",
    "path = brick:hasPoint",
    "shape S { sat: brick:hasPoint { ex:A } }",
    "shape S { points { sat: } }",
    "shape S { points { sat: ex:A ex:B } }",
    "shape S { ex:p ex:C }",
    // (`ex:p each ex:C { ... }` is left out: tree-sitter reads two qualifiers,
    // `each ex:C` and an inline shape, where the compiler stops to ask for
    // `each { is ex:C; ... }`, the likelier meaning.)
    "shape S { T(x = ) }",
    "shape S { T(x = ex:a, ex:b }",
    "path d( = ex:f",
    "shape S { is }",
];

#[test]
fn grammars_agree_on_valid_inputs() {
    for src in VALID {
        assert!(compiler_ok(src), "compiler rejects: {src}");
        assert!(
            ts_ok(src),
            "tree-sitter rejects: {src}\n{}",
            parser().parse(src, None).unwrap().root_node().to_sexp()
        );
    }
}

#[test]
fn grammars_agree_on_invalid_inputs() {
    for src in INVALID {
        assert!(!compiler_ok(src), "compiler accepts: {src}");
        assert!(!ts_ok(src), "tree-sitter accepts: {src}");
    }
}

#[test]
fn grammars_agree_on_examples() {
    fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "shm") {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/shapeshift"),
        &mut files,
    );
    assert!(files.iter().any(|p| p.ends_with("g36/brick.shm")));
    for path in files {
        let src = std::fs::read_to_string(&path).unwrap();
        assert!(compiler_ok(&src), "compiler rejects {}", path.display());
        assert!(
            ts_ok(&src),
            "tree-sitter rejects {}:\n{}",
            path.display(),
            parser().parse(&src, None).unwrap().root_node().to_sexp()
        );
        // and the formatter's output too
        let formatted = shapeshift_parse::format(&src, "x").unwrap();
        assert!(
            ts_ok(&formatted),
            "tree-sitter rejects formatted {}:\n{formatted}",
            path.display()
        );
    }
}

#[test]
fn highlight_query_compiles() {
    tree_sitter::Query::new(
        &tree_sitter_shapeshift::LANGUAGE.into(),
        tree_sitter_shapeshift::HIGHLIGHTS_QUERY,
    )
    .expect("highlights.scm is valid for this grammar");
}

/// `PATH COUNT? {` also reads as a field whose qualifier is an inline shape;
/// like the compiler, the editor must see a group.
#[test]
fn group_wins_over_field_with_inline_shape() {
    for src in [
        "shape S { points { sat: ex:SAT } }",
        "shape S { ex:hasPoint required { ex:SAT } }",
        "shape S { ex:a/ex:b { x: ex:X } }",
        "shape S { parts { c: ex:Coil { points { HC } } } }",
    ] {
        let tree = parser().parse(src, None).unwrap();
        let sexp = tree.root_node().to_sexp();
        assert!(sexp.contains("(group"), "{src}\n{sexp}");
        assert!(!sexp.contains("(field"), "{src}\n{sexp}");
    }
    // `T(args) { ... }` alone on a line composes `T`
    let src = "shape S { W(ex:C) { ex:q required } }";
    let sexp = parser().parse(src, None).unwrap().root_node().to_sexp();
    assert!(
        sexp.contains("(composition") && !sexp.contains("(group"),
        "{sexp}"
    );
    // `each { ... }` is the inline shape
    let src = "shape S { ex:p each { ex:q required } }";
    let sexp = parser().parse(src, None).unwrap().root_node().to_sexp();
    assert!(
        sexp.contains("(field") && !sexp.contains("(group"),
        "{sexp}"
    );
}
