use shifty_engine::{
    validate_graphs, validate_graphs_with_mode, validate_plan_graphs, validate_report,
    ValidationGraphMode,
};

#[test]
fn validation_graph_modes_have_distinct_scope() {
    let shapes_ttl = br#"
        @prefix ex: <http://ex/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .

        ex:Parent a rdfs:Class, sh:NodeShape ;
            sh:property [ sh:path ex:forbidden ; sh:maxCount 0 ] .
        ex:Child rdfs:subClassOf ex:Parent .
        ex:shapeItem a ex:Child ; ex:forbidden ex:value .
    "#;
    let data_ttl = br#"
        @prefix ex: <http://ex/> .
        ex:dataItem a ex:Child ; ex:forbidden ex:value .
    "#;
    let parsed = shifty_parse::parse_turtle(shapes_ttl, None).unwrap();
    let shapes = shifty_parse::load_turtle(shapes_ttl, None).unwrap();
    let data = shifty_parse::load_turtle(data_ttl, None).unwrap();

    let data_only = validate_graphs_with_mode(
        &data.graph,
        &shapes.graph,
        &parsed.schema,
        ValidationGraphMode::Data,
    )
    .unwrap();
    assert!(data_only.conforms);

    let union = validate_graphs(
        &data.graph,
        &shapes.graph,
        &parsed.schema,
    )
    .unwrap();
    assert!(!union.conforms);
    assert_eq!(union.violations.len(), 1);
    assert_eq!(union.violations[0].focus.to_string(), "<http://ex/dataItem>");

    let plan = shifty_opt::plan(&parsed.schema);
    let planned_union = validate_plan_graphs(&data.graph, &shapes.graph, &plan).unwrap();
    assert_eq!(planned_union, union);

    let union_all = validate_graphs_with_mode(
        &data.graph,
        &shapes.graph,
        &parsed.schema,
        ValidationGraphMode::UnionAll,
    )
    .unwrap();
    assert!(!union_all.conforms);
    assert_eq!(union_all.violations.len(), 2);
}

#[test]
fn implicit_class_targets_follow_the_shapes_class_hierarchy() {
    let ttl = br#"
        @prefix ex: <http://ex/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .

        ex:Class rdfs:subClassOf rdfs:Class .
        ex:AbstractClass rdfs:subClassOf ex:Class .

        ex:ConnectionPoint a ex:AbstractClass, sh:NodeShape ;
            sh:property [
                sh:path ex:hasMedium ;
                sh:minCount 1
            ] .

        ex:item a ex:ConnectionPoint .
    "#;
    let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
    let parsed = shifty_parse::parse_loaded(&loaded);

    let outcome = shifty_engine::validate(&loaded.graph, &parsed.schema).unwrap();
    assert!(!outcome.conforms);
    assert_eq!(outcome.violations.len(), 1);
    assert_eq!(outcome.violations[0].focus.to_string(), "<http://ex/item>");

    let report = validate_report(&loaded, &loaded.graph);
    assert!(!report.conforms);
    assert_eq!(report.results.len(), 1);
}
