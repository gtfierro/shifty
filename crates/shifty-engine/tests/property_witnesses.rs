//! [`shifty_engine::property_witnesses_graphs_with_mode`] — the observed
//! `sh:property` bindings for *conforming* focus nodes, the inverse of a
//! violation report.

use oxrdf::Term;
use shifty_algebra::Path;
use shifty_engine::{ValidationGraphMode, property_witnesses_graphs_with_mode};
use shifty_parse::parse_property_path;

fn key_str(key: &Term) -> &str {
    match key {
        Term::Literal(lit) => lit.value(),
        _ => panic!("expected a literal key, got {key:?}"),
    }
}

fn value_strings(values: &[Term]) -> Vec<String> {
    let mut out: Vec<String> = values.iter().map(ToString::to_string).collect();
    out.sort();
    out
}

fn shapes_and_data(ttl: &[u8]) -> (shifty_parse::Loaded, oxrdf::Graph) {
    let shapes = shifty_parse::load_turtle(ttl, None).expect("valid shapes");
    let data = shifty_parse::load_turtle(ttl, None)
        .expect("valid data")
        .graph;
    (shapes, data)
}

#[test]
fn qualified_value_shape_disambiguates_same_typed_siblings() {
    // Four temperature-quantity-kind sensors on one focus node; a key is
    // disambiguated per-property via a qualifying tag, mirroring the "four
    // same-quantity-kind temperature sensors" scenario.
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property [
                zea:roleName "outsideAirTemp" ;
                sh:path ex:hasPoint ;
                sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
                sh:qualifiedMinCount 1 ;
                sh:qualifiedMaxCount 1 ;
            ] ;
            sh:property [
                zea:roleName "returnAirTemp" ;
                sh:path ex:hasPoint ;
                sh:qualifiedValueShape [ sh:hasValue ex:rat ] ;
                sh:qualifiedMinCount 1 ;
                sh:qualifiedMaxCount 1 ;
            ] .

        ex:vav1 ex:hasPoint ex:oat, ex:rat, ex:sat, ex:mat .
        ex:oat a ex:TemperatureSensor .
        ex:rat a ex:TemperatureSensor .
        ex:sat a ex:TemperatureSensor .
        ex:mat a ex:TemperatureSensor .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = parse_property_path("zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(
        witnesses.len(),
        2,
        "one witness per sh:property: {witnesses:?}"
    );
    for w in &witnesses {
        assert_eq!(w.focus.to_string(), "<http://ex/vav1>");
        assert_eq!(w.shape.to_string(), "<http://ex/Profile>");
    }

    let outside = witnesses
        .iter()
        .find(|w| key_str(&w.key) == "outsideAirTemp")
        .expect("outsideAirTemp witness present");
    assert_eq!(value_strings(&outside.values), vec!["<http://ex/oat>"]);

    let return_air = witnesses
        .iter()
        .find(|w| key_str(&w.key) == "returnAirTemp")
        .expect("returnAirTemp witness present");
    assert_eq!(value_strings(&return_air.values), vec!["<http://ex/rat>"]);
}

#[test]
fn multi_hop_key_path_resolves_through_an_intermediate_node() {
    // The stable key isn't a direct annotation on the property shape — it's
    // one hop further, through an intermediate "role descriptor" node. This
    // is the case a bare single-predicate lookup can't reach.
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property [
                zea:role ex:OutsideAirTempRole ;
                sh:path ex:hasPoint ;
                sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
                sh:qualifiedMinCount 1 ;
                sh:qualifiedMaxCount 1 ;
            ] .
        ex:OutsideAirTempRole zea:roleName "outsideAirTemp" .

        ex:vav1 ex:hasPoint ex:oat, ex:sat .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = parse_property_path("zea:role/zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(witnesses.len(), 1);
    assert_eq!(key_str(&witnesses[0].key), "outsideAirTemp");
    assert_eq!(value_strings(&witnesses[0].values), vec!["<http://ex/oat>"]);
}

#[test]
fn inverse_key_path_resolves_through_a_reverse_hop() {
    // The role descriptor points *at* the property shape rather than being
    // pointed to by it, so the key path needs an inverse hop.
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property ex:OatProp .
        ex:OatProp a sh:PropertyShape ;
            sh:path ex:hasPoint ;
            sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
            sh:qualifiedMinCount 1 ;
            sh:qualifiedMaxCount 1 .
        ex:RoleDescriptor zea:describes ex:OatProp ; zea:roleName "outsideAirTemp" .

        ex:vav1 ex:hasPoint ex:oat, ex:sat .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = parse_property_path("^zea:describes/zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(witnesses.len(), 1);
    assert_eq!(key_str(&witnesses[0].key), "outsideAirTemp");
}

#[test]
fn violating_focus_yields_no_witnesses() {
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:good, ex:bad ;
            sh:property [
                zea:roleName "outsideAirTemp" ;
                sh:path ex:hasPoint ;
                sh:minCount 1 ;
            ] .

        ex:good ex:hasPoint ex:oat .
        ex:bad a ex:Thing .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = parse_property_path("zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(
        witnesses.len(),
        1,
        "only the conforming focus witnesses: {witnesses:?}"
    );
    assert_eq!(witnesses[0].focus.to_string(), "<http://ex/good>");
    assert_eq!(value_strings(&witnesses[0].values), vec!["<http://ex/oat>"]);
}

#[test]
fn plain_cardinality_property_yields_raw_path_values() {
    // No sh:qualifiedValueShape: the witness is every deduped sh:path value.
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property [
                zea:roleName "points" ;
                sh:path ex:hasPoint ;
                sh:minCount 1 ;
            ] .

        ex:vav1 ex:hasPoint ex:a, ex:b .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = parse_property_path("zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(witnesses.len(), 1);
    assert_eq!(
        value_strings(&witnesses[0].values),
        vec!["<http://ex/a>", "<http://ex/b>"]
    );
}

#[test]
fn missing_key_path_falls_back_to_property_shape_node() {
    let ttl = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property ex:PointRole .
        ex:PointRole a sh:PropertyShape ;
            sh:path ex:hasPoint ;
            sh:minCount 1 .

        ex:vav1 ex:hasPoint ex:a .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let witnesses =
        property_witnesses_graphs_with_mode(&shapes, &data, ValidationGraphMode::Union, None);

    assert_eq!(witnesses.len(), 1);
    assert_eq!(witnesses[0].key.to_string(), "<http://ex/PointRole>");
}

#[test]
fn key_path_resolving_to_nothing_falls_back_to_property_shape_node() {
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property ex:PointRole .
        ex:PointRole a sh:PropertyShape ;
            sh:path ex:hasPoint ;
            sh:minCount 1 .

        ex:vav1 ex:hasPoint ex:a .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    // ex:PointRole has no zea:roleName, so the key path resolves to nothing.
    let key_path = parse_property_path("zea:roleName", &shapes).unwrap();
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(witnesses.len(), 1);
    assert_eq!(witnesses[0].key.to_string(), "<http://ex/PointRole>");
}

#[test]
fn key_path_is_rust_algebra_not_just_strings() {
    // Rust callers can build an arbitrary Path directly, without going
    // through the SPARQL-path-string parser at all.
    let ttl = br#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://ex/> .

        ex:Profile a sh:NodeShape ;
            sh:targetNode ex:vav1 ;
            sh:property [
                zea:roleName "outsideAirTemp" ;
                sh:path ex:hasPoint ;
                sh:minCount 1 ;
            ] .

        ex:vav1 ex:hasPoint ex:oat .
    "#;
    let (shapes, data) = shapes_and_data(ttl);

    let key_path = Path::Pred(oxrdf::NamedNode::new("http://zea.example/ns#roleName").unwrap());
    let witnesses = property_witnesses_graphs_with_mode(
        &shapes,
        &data,
        ValidationGraphMode::Union,
        Some(&key_path),
    );

    assert_eq!(witnesses.len(), 1);
    assert_eq!(key_str(&witnesses[0].key), "outsideAirTemp");
}
