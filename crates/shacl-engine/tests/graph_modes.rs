use shacl_engine::{
    validate_graphs, validate_graphs_with_mode, validate_plan_graphs, ValidationGraphMode,
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
    let parsed = shacl_parse::parse_turtle(shapes_ttl, None).unwrap();
    let shapes = shacl_parse::load_turtle(shapes_ttl, None).unwrap();
    let data = shacl_parse::load_turtle(data_ttl, None).unwrap();

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

    let plan = shacl_opt::plan(&parsed.schema);
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
