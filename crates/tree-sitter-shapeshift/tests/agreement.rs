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
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/shapeshift");
    for e in std::fs::read_dir(dir).unwrap() {
        let path = e.unwrap().path();
        if path.extension().is_none_or(|x| x != "shm") {
            continue;
        }
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
