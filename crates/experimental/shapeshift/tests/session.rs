//! The REPL's session layer: transactional definitions, data, and images.

use shapeshift::Session;
use std::path::PathBuf;

const PREFIXES: &str = "prefix ex: <https://example.org/>\n";

const DATA: &str = r#"
@prefix ex: <https://example.org/> .
ex:a a ex:Thing ; ex:name "a" .
ex:b a ex:Thing .
"#;

fn examples() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../examples/shapeshift")
}

#[test]
fn a_definition_that_does_not_build_leaves_the_session_unchanged() {
    let mut s = Session::new("t");
    assert!(s.apply(PREFIXES).accepted);
    let ok = s.apply("shape Named for every ex:Thing { ex:name required }");
    assert!(ok.accepted, "{:?}", ok.diagnostics);
    assert_eq!(ok.defined, ["Named"]);

    // syntax error
    let bad = s.apply("shape Broken { ex:name at least }");
    assert!(!bad.accepted);
    // builds syntactically but references nothing
    let bad = s.apply("shape Broken { Missing }");
    assert!(!bad.accepted);
    assert!(
        bad.diagnostics
            .iter()
            .any(|d| d.message.contains("Missing"))
    );
    assert!(s.shape("Broken").is_none());
    assert_eq!(s.history.len(), 2, "rejected input is not recorded");

    // redefinition replaces in place
    let re = s.apply("shape Named for every ex:Thing { ex:name optional }");
    assert!(re.accepted);
    assert_eq!(re.redefined, ["Named"]);
    assert_eq!(s.module.shapes.len(), 1);
}

#[test]
fn path_aliases_persist_across_inputs() {
    let mut s = Session::new("t");
    s.apply(PREFIXES);
    assert!(s.apply("path names = ex:name").accepted);
    let r = s.apply("shape Named { names { first: ex:Name } }");
    assert!(r.accepted, "{:?}", r.diagnostics);
    // redefining the alias replaces it
    assert!(s.apply("path names = ex:label").accepted);
    assert_eq!(s.module.paths.len(), 1);
    assert!(s.source().contains("path names = ex:label"));
}

#[test]
fn removing_a_shape_that_others_use_is_refused() {
    let mut s = Session::new("t");
    s.apply(PREFIXES);
    assert!(s.apply("shape Base { ex:name required }").accepted);
    assert!(s.apply("shape User { Base }").accepted);
    assert!(!s.remove("Base").accepted);
    assert!(s.remove("User").accepted);
    assert!(s.remove("Base").accepted);
    assert!(s.module.shapes.is_empty());
}

#[test]
fn an_image_restores_shapes_data_settings_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data.ttl");
    std::fs::write(&data, DATA).unwrap();

    let mut s = Session::new("t");
    s.apply(PREFIXES);
    s.apply("shape Named for every ex:Thing { ex:name required }");
    s.add_data(&data).unwrap();
    s.imports
        .catalog
        .insert("https://example.org/onto".into(), data.clone());
    let before = s.validate().unwrap();
    assert!(!before.conforms, "ex:b has no name");

    let image = dir.path().join("session.json");
    s.save_image(&image).unwrap();
    std::fs::remove_file(&data).unwrap(); // the image carries the data itself

    let r = Session::load_image(&image).unwrap();
    assert_eq!(r.source(), s.source());
    assert_eq!(r.history, s.history);
    assert_eq!(r.imports.catalog, s.imports.catalog);
    let after = r.validate().unwrap();
    assert_eq!(after.conforms, before.conforms);
    assert_eq!(after.results.len(), before.results.len());
    assert_eq!(after.results[0].focus, before.results[0].focus);
}

#[test]
fn saved_source_reloads_to_the_same_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::new("t");
    let loaded = s.load_shm(&examples().join("hvac.shm")).unwrap();
    assert!(loaded.accepted, "{:?}", loaded.diagnostics);
    s.apply("shape Extra for every brick:Point { brick:hasUnit optional }");

    let file = dir.path().join("saved.shm");
    s.save_source(&file).unwrap();
    // relative imports were written against the original directory
    std::fs::copy(
        examples().join("sensors.shm"),
        dir.path().join("sensors.shm"),
    )
    .unwrap();

    let mut r = Session::new("r");
    let reloaded = r.load_shm(&file).unwrap();
    assert!(reloaded.accepted, "{:?}", reloaded.diagnostics);
    assert_eq!(r.source(), s.source());
    assert_eq!(
        r.compile().built.shapes.keys().collect::<Vec<_>>(),
        s.compile().built.shapes.keys().collect::<Vec<_>>()
    );
}

#[test]
fn images_from_elsewhere_are_rejected_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("x.json");
    std::fs::write(&f, r#"{"format":"something-else","version":1,"label":"x","module":{},"data":[],"imports":{},"history":[]}"#).unwrap();
    let e = Session::load_image(&f).err().expect("rejected").to_string();
    assert!(e.contains("not a Shapeshift image"), "{e}");
}
