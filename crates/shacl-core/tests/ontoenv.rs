use shifty_shacl_core::{
    load_and_parse_with_ontoenv, lower_to_program,
    source::{RefreshMode, ShapeSource, SourceLoadOptions},
};
use std::fs;
use tempfile::tempdir;

#[test]
fn ontoenv_loads_local_import_closure() {
    let dir = tempdir().expect("tempdir should exist");
    let imported = dir.path().join("imported.ttl");
    let root = dir.path().join("root.ttl");

    fs::write(
        &imported,
        r#"
@prefix ex: <http://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

ex:ImportedShape
    a sh:NodeShape ;
    sh:targetClass ex:Thing .
"#,
    )
    .expect("should write imported fixture");

    fs::write(
        &root,
        format!(
            r#"
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

<file://{root_path}>
    a owl:Ontology ;
    owl:imports <file://{imported_path}> .

ex:RootShape
    a sh:NodeShape ;
    sh:node ex:ImportedShape .
"#,
            root_path = root.display(),
            imported_path = imported.display()
        ),
    )
    .expect("should write root fixture");

    let syntax = load_and_parse_with_ontoenv(
        &[ShapeSource::File(root)],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::Force,
        },
    )
    .expect("ontoenv load should succeed");

    let program = lower_to_program(&syntax);
    assert!(
        program
            .shapes
            .iter()
            .any(|shape| shape.source.to_string().contains("ImportedShape"))
    );
    assert!(
        program
            .source_inventory
            .iter()
            .any(|source| !source.is_root)
    );
}
