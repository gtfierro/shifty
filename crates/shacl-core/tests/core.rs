use oxrdf::{Literal, NamedNode, Quad, Term};
use shifty_shacl_core::{
    BackendBucket, BackendClosureMode, BackendViewOptions, ContextFootprint, DependencyClass,
    InMemoryValidationBackend, NormalizeOptions, RewriteOptions, SharedWorkUnitKind, SliceReason,
    SliceRoots, StaticCostHint, ValidationBackend, analyze_program, analyze_static,
    analyze_static_with_roots, build_validation_report, canonicalize_program, context_requirements,
    derive_backend_logical_plans, derive_backend_views, derive_inference_logical_plan,
    derive_inference_view, derive_validation_logical_plan, derive_validation_view,
    fingerprint_program, lower_to_program, normalize_program, parse_quads,
    prune_deactivated_program, render_shape_program_dot, rewrite_program, shared_work_candidates,
    slice_program,
    source::{RefreshMode, ShapeSource, SourceLoadOptions, load_with_ontoenv},
    static_cost_hints,
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/fixtures")
        .join(name)
}

fn suite_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/test-suite")
        .join(name)
}

#[test]
fn lowers_af_targets_into_advanced_target_algebra() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_target_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);

    assert!(!program.shapes.is_empty());
    assert!(program.targets.iter().any(|target| {
        matches!(
            target.expr,
            shifty_shacl_core::algebra::TargetExpr::Advanced(_)
        )
    }));
    assert!(program.features.iter().any(|feature| matches!(
        feature,
        shifty_shacl_core::algebra::FeatureUse::AdvancedTargets
    )));

    let parsed_target = syntax
        .shapes
        .iter()
        .find(|shape| shape.subject.to_string() == "<http://example.org/FilteredShape>")
        .and_then(|shape| shape.targets.first())
        .expect("filtered shape should have an advanced target");
    let shifty_shacl_core::syntax::TargetSyntax::Advanced(parsed_target) = parsed_target else {
        panic!("expected advanced target syntax");
    };
    assert_eq!(parsed_target.declarations.len(), 1);
    assert_eq!(parsed_target.declarations[0].prefix.as_deref(), Some("ex"));

    let lowered_target = program
        .targets
        .iter()
        .find(|target| target.owner == program.shape_index["<http://example.org/FilteredShape>"])
        .expect("filtered shape should lower a target");
    let shifty_shacl_core::algebra::TargetExpr::Advanced(lowered_target) = &lowered_target.expr
    else {
        panic!("expected advanced target algebra");
    };
    assert_eq!(lowered_target.declarations.len(), 1);
    assert_eq!(lowered_target.declarations[0].prefix.as_deref(), Some("ex"));
    assert!(lowered_target.target_shape_id.is_some());
    assert!(lowered_target.filter_shape_id.is_some());
}

#[test]
fn direct_quad_parsing_preserves_custom_component_constraints() {
    let shape = NamedNode::new("urn:shape").unwrap();
    let property = NamedNode::new("urn:property").unwrap();
    let path = NamedNode::new("urn:path").unwrap();
    let activate = NamedNode::new("http://example.org/activate").unwrap();
    let component = NamedNode::new("urn:activate-component").unwrap();
    let parameter = NamedNode::new("urn:activate-parameter").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_constraint_component =
        NamedNode::new("http://www.w3.org/ns/shacl#ConstraintComponent").unwrap();
    let sh_parameter = NamedNode::new("http://www.w3.org/ns/shacl#parameter").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_property,
            Term::NamedNode(property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            sh_path.clone(),
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property,
            activate.clone(),
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            component.clone(),
            rdf_type,
            Term::NamedNode(sh_constraint_component),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            component,
            sh_parameter,
            Term::NamedNode(parameter.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            parameter,
            sh_path,
            Term::NamedNode(activate),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    assert_eq!(program.shapes.len(), 2);
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent { predicate, .. }
            if predicate.as_str() == "http://example.org/activate"
        )
    }));
}

#[test]
fn non_sh_predicates_without_component_definitions_lower_as_generic_predicates() {
    let shape = NamedNode::new("urn:shape").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let rdfs_subclass_of =
        NamedNode::new("http://www.w3.org/2000/01/rdf-schema#subClassOf").unwrap();
    let parent = NamedNode::new("urn:parent").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            rdfs_subclass_of,
            Term::NamedNode(parent),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::GenericPredicate { predicate, .. }
            if predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        )
    }));
    assert!(!program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent { component, predicate, .. }
            if component.is_none() && predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        )
    }));
}

#[test]
fn analysis_reports_recursive_components() {
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let a = NamedNode::new("urn:a").unwrap();
    let b = NamedNode::new("urn:b").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            a.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a,
            sh_node.clone(),
            Term::NamedNode(b.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b,
            sh_node,
            Term::NamedNode(NamedNode::new("urn:a").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let analysis = analyze_program(&program);

    assert!(
        analysis
            .dependency_components
            .iter()
            .any(|component| component.recursive)
    );
    assert_eq!(analysis.root_shapes.len(), 0);
}

#[test]
fn analysis_reports_reachability_and_inventories() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let child = NamedNode::new("urn:child").unwrap();
    let orphan = NamedNode::new("urn:orphan").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            child.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            orphan.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root,
            sh_node,
            Term::NamedNode(child.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let analysis = analyze_program(&program);
    assert_eq!(analysis.root_shapes.len(), 1);
    assert_eq!(analysis.reachable_shapes.len(), 2);
    assert_eq!(analysis.unreachable_shapes.len(), 1);
    assert_eq!(analysis.target_kind_counts["node"], 1);
    assert_eq!(analysis.dependency_kind_counts["node"], 1);
    let orphan_id = program.shape_index[&Term::NamedNode(orphan).to_string()];
    assert!(analysis.unreachable_shapes.contains(&orphan_id));
}

#[test]
fn static_slice_keeps_only_target_reachable_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let orphan = NamedNode::new("urn:orphan").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            orphan.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root,
            sh_node,
            Term::NamedNode(helper.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let slice = slice_program(&program, SliceRoots::TargetShapes);
    assert_eq!(slice.roots.len(), 1);
    assert_eq!(slice.retained_shape_ids.len(), 2);
    let orphan_id = program.shape_index[&Term::NamedNode(orphan).to_string()];
    assert!(slice.dropped_shape_ids.contains(&orphan_id));
    assert_eq!(slice.reduced_program.shapes.len(), 2);
}

#[test]
fn static_slice_supports_explicit_root_selectors_and_reasons() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let property_shape = NamedNode::new("urn:property-shape").unwrap();
    let path = NamedNode::new("urn:p").unwrap();
    let orphan = NamedNode::new("urn:orphan").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            orphan.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let slice = slice_program(
        &program,
        SliceRoots::ExplicitSelectors(vec!["<urn:root>".to_string()]),
    );

    assert_eq!(slice.roots.len(), 1);
    assert!(slice.unresolved_root_selectors.is_empty());
    assert_eq!(slice.retained_shape_ids.len(), 2);
    let orphan_id = program.shape_index[&Term::NamedNode(orphan).to_string()];
    assert!(matches!(
        slice.retained_shape_reasons[&slice.roots[0]],
        SliceReason::Root { .. }
    ));
    assert!(matches!(
        slice.dropped_shape_reasons[&orphan_id],
        SliceReason::UnreachableFromRoots
    ));
    let property_shape_id = program.shape_index
        [&Term::NamedNode(NamedNode::new("urn:property-shape").unwrap()).to_string()];
    let property_shape_key = program
        .shapes
        .iter()
        .find(|shape| shape.id == property_shape_id)
        .map(|shape| shape.normalized_key.clone())
        .expect("property shape should have a normalized key");
    assert!(matches!(
        slice.retained_shape_reasons[&property_shape_id],
        SliceReason::ReachableFromRoots { .. }
    ));

    let normalized_slice = slice_program(
        &program,
        SliceRoots::ExplicitSelectors(vec![property_shape_key]),
    );
    assert_eq!(normalized_slice.roots, vec![property_shape_id]);
    assert!(normalized_slice.unresolved_root_selectors.is_empty());
}

#[test]
fn static_context_classifies_node_path_and_global_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let sh_sparql = NamedNode::new("http://www.w3.org/ns/shacl#sparql").unwrap();
    let sh_select = NamedNode::new("http://www.w3.org/ns/shacl#select").unwrap();
    let local = NamedNode::new("urn:local").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let path_shape = NamedNode::new("urn:path-shape").unwrap();
    let sparql_shape = NamedNode::new("urn:sparql-shape").unwrap();
    let property = NamedNode::new("urn:property").unwrap();
    let sparql_node = NamedNode::new("urn:sparql-node").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            local.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            local.clone(),
            sh_path.clone(),
            Term::NamedNode(property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            local.clone(),
            sh_has_value,
            Term::Literal(Literal::from("ready")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            path_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            path_shape.clone(),
            sh_node,
            Term::NamedNode(helper),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            sparql_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            sparql_shape,
            sh_sparql,
            Term::NamedNode(sparql_node.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            sparql_node,
            sh_select,
            Term::Literal(Literal::from("SELECT $this WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let report = context_requirements(&program);
    assert_eq!(
        report.shape_footprints[&program.shape_index[&Term::NamedNode(path_shape).to_string()]],
        ContextFootprint::ShapeReferenceTraversal
    );
    assert_eq!(
        report.shape_footprints[&program.shape_index[&Term::NamedNode(local).to_string()]],
        ContextFootprint::SingleHopPath
    );
    assert_eq!(
        report.shape_footprints[&program.shape_index
            [&Term::NamedNode(NamedNode::new("urn:sparql-shape").unwrap()).to_string()]],
        ContextFootprint::GlobalSparql
    );
}

#[test]
fn static_context_distinguishes_single_hop_bounded_and_shape_reference() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let sh_zero_or_more_path = NamedNode::new("http://www.w3.org/ns/shacl#zeroOrMorePath").unwrap();
    let single = NamedNode::new("urn:single").unwrap();
    let bounded = NamedNode::new("urn:bounded").unwrap();
    let referenced = NamedNode::new("urn:referenced").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let p = NamedNode::new("urn:p").unwrap();
    let path_node = oxrdf::BlankNode::new("path-node").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            single.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            single.clone(),
            sh_path.clone(),
            Term::NamedNode(p.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            bounded.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            bounded.clone(),
            sh_path.clone(),
            Term::BlankNode(path_node.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            path_node,
            sh_zero_or_more_path,
            Term::NamedNode(p),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            referenced.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            referenced.clone(),
            sh_node,
            Term::NamedNode(helper),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let report = context_requirements(&program);

    assert_eq!(
        report.shape_footprints[&program.shape_index[&Term::NamedNode(single).to_string()]],
        ContextFootprint::SingleHopPath
    );
    assert_eq!(
        report.shape_footprints[&program.shape_index[&Term::NamedNode(bounded).to_string()]],
        ContextFootprint::BoundedTraversal
    );
    assert_eq!(
        report.shape_footprints[&program.shape_index[&Term::NamedNode(referenced).to_string()]],
        ContextFootprint::ShapeReferenceTraversal
    );
}

#[test]
fn static_fingerprints_group_identical_component_constraints() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let mut syntax = shifty_shacl_core::parse_resolved(&resolved);
    let second_property = NamedNode::new("urn:duplicate-property").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let activate = NamedNode::new("http://example.org/activate").unwrap();
    let name = NamedNode::new("http://example.org/name").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();

    syntax.quads.push(Quad::new(
        NamedNode::new("http://example.org/NameShape").unwrap(),
        sh_property,
        Term::NamedNode(second_property.clone()),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property.clone(),
        rdf_type,
        Term::NamedNode(sh_property_shape),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property.clone(),
        sh_path,
        Term::NamedNode(name),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property,
        activate,
        Term::Literal(Literal::from(true)),
        oxrdf::GraphName::DefaultGraph,
    ));

    let syntax = shifty_shacl_core::parse_quads(syntax.quads);
    let program = lower_to_program(&syntax);
    let fingerprints = fingerprint_program(&program);
    assert!(
        fingerprints
            .duplicate_constraints
            .iter()
            .any(|group| group.ids.len() >= 2)
    );
    let static_summary = analyze_static(&program);
    assert!(!static_summary.fingerprints.duplicate_constraints.is_empty());
}

#[test]
fn static_shared_work_and_cost_hints_report_duplicates() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_constraint_component =
        NamedNode::new("http://www.w3.org/ns/shacl#ConstraintComponent").unwrap();
    let sh_parameter = NamedNode::new("http://www.w3.org/ns/shacl#parameter").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let ex_activate = NamedNode::new("http://example.org/activate").unwrap();
    let component = NamedNode::new("urn:activate-component").unwrap();
    let parameter = NamedNode::new("urn:activate-parameter").unwrap();
    let a = NamedNode::new("urn:a").unwrap();
    let b = NamedNode::new("urn:b").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            a.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a.clone(),
            ex_activate.clone(),
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            ex_activate,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            component.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_constraint_component),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            component,
            sh_parameter,
            Term::NamedNode(parameter.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            parameter,
            sh_path,
            Term::NamedNode(NamedNode::new("http://example.org/activate").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let baseline = analyze_program(&program);
    let context = context_requirements(&program);
    let fingerprints = fingerprint_program(&program);
    let shared_work = shared_work_candidates(&program, &fingerprints);
    let cost_hints = static_cost_hints(&program, &baseline, &context, &shared_work);

    assert_eq!(shared_work.duplicate_constraints.len(), 1);
    assert_eq!(shared_work.duplicate_custom_component_constraints.len(), 1);
    assert!(
        cost_hints.shape_hints[&program.shape_index[&Term::NamedNode(a).to_string()]]
            .contains(&StaticCostHint::DuplicateConstraintWork)
    );
    assert!(
        cost_hints.shape_hints[&program.shape_index[&Term::NamedNode(b).to_string()]]
            .contains(&StaticCostHint::DuplicateConstraintWork)
    );
}

#[test]
fn static_analyze_uses_explicit_root_set() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let ignored = NamedNode::new("urn:ignored").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            ignored.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            ignored,
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root,
            sh_node,
            Term::NamedNode(helper),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let summary = analyze_static_with_roots(
        &program,
        SliceRoots::ExplicitSelectors(vec!["<urn:root>".to_string()]),
    );
    assert_eq!(summary.slice.roots.len(), 1);
    assert_eq!(summary.slice.retained_shape_ids.len(), 2);
}

#[test]
fn rewrite_program_slices_and_prunes_dead_structure() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let orphan = NamedNode::new("urn:orphan").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            orphan.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root,
            sh_node,
            Term::NamedNode(helper),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let rewritten = rewrite_program(
        &program,
        RewriteOptions {
            roots: Some(SliceRoots::ExplicitSelectors(vec![
                "<urn:root>".to_string(),
            ])),
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
    );
    assert_eq!(rewritten.program.shapes.len(), 2);
    assert!(
        rewritten
            .summary
            .passes
            .iter()
            .any(|pass| pass.pass == "slice_to_roots")
    );
    assert_eq!(rewritten.summary.shapes_removed, 1);
}

#[test]
fn rewrite_program_reorders_constraints_by_static_cost() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_sparql = NamedNode::new("http://www.w3.org/ns/shacl#sparql").unwrap();
    let sh_select = NamedNode::new("http://www.w3.org/ns/shacl#select").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();
    let property = NamedNode::new("urn:p").unwrap();
    let sparql = NamedNode::new("urn:sparql").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_path,
            Term::NamedNode(property),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_sparql,
            Term::NamedNode(sparql.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            sparql,
            sh_select,
            Term::Literal(Literal::from("SELECT $this WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let rewritten = rewrite_program(&program, RewriteOptions::default());
    let owner = rewritten.program.shapes[0].id;
    let ordered = rewritten
        .program
        .constraints
        .iter()
        .filter(|constraint| constraint.owner == owner)
        .map(|constraint| &constraint.expr)
        .collect::<Vec<_>>();
    assert!(matches!(
        ordered[0],
        shifty_shacl_core::algebra::ConstraintExpr::Cardinality { .. }
    ));
    assert!(matches!(
        ordered[1],
        shifty_shacl_core::algebra::ConstraintExpr::Sparql(_)
    ));
}

#[test]
fn rewrite_program_reports_recursive_regions() {
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let a = NamedNode::new("urn:a").unwrap();
    let b = NamedNode::new("urn:b").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            a.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a,
            sh_node.clone(),
            Term::NamedNode(b.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b,
            sh_node,
            Term::NamedNode(NamedNode::new("urn:a").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let rewritten = rewrite_program(&program, RewriteOptions::default());
    assert_eq!(rewritten.summary.recursive_regions.len(), 1);
    assert_eq!(rewritten.summary.recursive_regions[0].shapes.len(), 2);
}

#[test]
fn validation_view_drops_rules_but_keeps_shape_partitions() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_target_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let view = derive_validation_view(&program, BackendViewOptions::default());
    assert_eq!(view.program.rules.len(), 0);
    assert!(!view.entry_shapes.is_empty());
    assert!(!view.partition.histogram.is_empty());
}

#[test]
fn inference_view_keeps_rule_owners_and_condition_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let condition = NamedNode::new("urn:condition").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            condition.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(condition.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from(
                "CONSTRUCT { $this <urn:p> <urn:o> } WHERE { }",
            )),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let view = derive_inference_view(
        &program,
        BackendViewOptions {
            rewrite: RewriteOptions {
                roots: Some(SliceRoots::ExplicitSelectors(vec![
                    "<urn:owner>".to_string(),
                ])),
                prune_unreachable: true,
                ..RewriteOptions::default()
            },
            closure_mode: BackendClosureMode::InferenceClosure,
        },
    );
    assert_eq!(view.program.rules.len(), 1);
    assert_eq!(view.rule_owner_shapes.len(), 1);
    assert_eq!(view.condition_shapes.len(), 1);
    assert!(matches!(
        view.rule_buckets.values().next(),
        Some(BackendBucket::ShapeReference)
    ));
}

#[test]
fn backend_views_can_be_derived_together() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let views = derive_backend_views(&program, BackendViewOptions::default());
    assert!(!views.validation.program.shapes.is_empty());
    assert!(!views.inference.program.shapes.is_empty());
}

#[test]
fn backend_views_materialize_shared_work_units() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let mut syntax = shifty_shacl_core::parse_resolved(&resolved);
    let second_property = NamedNode::new("urn:duplicate-property").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let activate = NamedNode::new("http://example.org/activate").unwrap();
    let name = NamedNode::new("http://example.org/name").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();

    syntax.quads.push(Quad::new(
        NamedNode::new("http://example.org/NameShape").unwrap(),
        sh_property,
        Term::NamedNode(second_property.clone()),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property.clone(),
        rdf_type,
        Term::NamedNode(sh_property_shape),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property.clone(),
        sh_path,
        Term::NamedNode(name),
        oxrdf::GraphName::DefaultGraph,
    ));
    syntax.quads.push(Quad::new(
        second_property,
        activate,
        Term::Literal(Literal::from(true)),
        oxrdf::GraphName::DefaultGraph,
    ));

    let program = lower_to_program(&shifty_shacl_core::parse_quads(syntax.quads));
    let views = derive_backend_views(&program, BackendViewOptions::default());
    assert!(
        views
            .validation
            .shared_work_units
            .iter()
            .any(|unit| { matches!(unit.kind, SharedWorkUnitKind::CustomComponentConstraint) })
    );
}

#[test]
fn backend_views_distinguish_validation_and_inference_closures() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let condition = NamedNode::new("urn:condition").unwrap();
    let unrelated = NamedNode::new("urn:unrelated").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            condition.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            unrelated.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(condition.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from(
                "CONSTRUCT { $this <urn:p> <urn:o> } WHERE { }",
            )),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let validation = derive_validation_view(
        &program,
        BackendViewOptions {
            rewrite: RewriteOptions::default(),
            closure_mode: BackendClosureMode::ValidationClosure,
        },
    );
    let inference = derive_inference_view(
        &program,
        BackendViewOptions {
            rewrite: RewriteOptions::default(),
            closure_mode: BackendClosureMode::InferenceClosure,
        },
    );
    let condition_id = program.shape_index[&Term::NamedNode(condition).to_string()];
    assert!(
        !validation
            .program
            .shapes
            .iter()
            .any(|shape| shape.id == condition_id)
    );
    assert!(
        inference
            .program
            .shapes
            .iter()
            .any(|shape| shape.id == condition_id)
    );
}

#[test]
fn backend_views_classify_dependencies() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let helper = NamedNode::new("urn:helper").unwrap();
    let condition = NamedNode::new("urn:condition").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            helper.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            condition.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_node,
            Term::NamedNode(helper),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(condition),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from(
                "CONSTRUCT { $this <urn:p> <urn:o> } WHERE { }",
            )),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let inference = derive_inference_view(
        &program,
        BackendViewOptions {
            rewrite: RewriteOptions::default(),
            closure_mode: BackendClosureMode::MixedClosure,
        },
    );
    assert!(
        inference
            .dependencies
            .iter()
            .any(|edge| edge.kind == "node" && edge.class == DependencyClass::ValidationOnly)
    );
    assert!(inference.dependencies.iter().any(|edge| {
        edge.kind == "rule_condition" && edge.class == DependencyClass::InferenceOnly
    }));
}

#[test]
fn validation_logical_plan_contains_target_scans_and_constraint_batches() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    assert!(plan.summary.node_counts.contains_key("target_scan"));
    assert!(plan.summary.node_counts.contains_key("constraint_batch"));
}

#[test]
fn inference_logical_plan_contains_rule_batches() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let condition = NamedNode::new("urn:condition").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            condition.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(condition),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from(
                "CONSTRUCT { $this <urn:p> <urn:o> } WHERE { }",
            )),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let plan = derive_inference_logical_plan(
        &program,
        BackendViewOptions {
            rewrite: RewriteOptions::default(),
            closure_mode: BackendClosureMode::InferenceClosure,
        },
    );
    assert!(plan.summary.node_counts.contains_key("seed_scan"));
    assert!(plan.summary.node_counts.contains_key("rule_batch"));
}

#[test]
fn backend_logical_plans_can_be_derived_together() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plans = derive_backend_logical_plans(&program, BackendViewOptions::default());
    assert!(!plans.validation.nodes.is_empty());
    assert!(!plans.inference.nodes.is_empty());
}

#[test]
fn validation_backend_resolves_targets_and_local_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let sh_datatype = NamedNode::new("http://www.w3.org/ns/shacl#datatype").unwrap();
    let xsd_string = NamedNode::new("http://www.w3.org/2001/XMLSchema#string").unwrap();
    let shape = NamedNode::new("urn:person-shape").unwrap();
    let property_shape = NamedNode::new("urn:name-shape").unwrap();
    let alice = NamedNode::new("urn:alice").unwrap();
    let name = NamedNode::new("urn:name").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(alice.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(name.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_datatype,
            Term::NamedNode(xsd_string),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data").unwrap(),
            quads: vec![Quad::new(
                alice,
                name,
                Term::Literal(Literal::new_simple_literal("Alice")),
                oxrdf::GraphName::DefaultGraph,
            )],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(result.conforms);
    assert_eq!(result.focus_nodes_evaluated, 1);
    assert!(!result.trace.is_empty());
}

#[test]
fn validation_backend_reports_property_path_violations() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let shape = NamedNode::new("urn:task-shape").unwrap();
    let property_shape = NamedNode::new("urn:status-shape").unwrap();
    let task = NamedNode::new("urn:task").unwrap();
    let status = NamedNode::new("urn:status").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(task.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(status.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_has_value,
            Term::Literal(Literal::new_simple_literal("complete")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data2").unwrap(),
            quads: vec![Quad::new(
                task,
                status,
                Term::Literal(Literal::new_simple_literal("pending")),
                oxrdf::GraphName::DefaultGraph,
            )],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(!result.violations.is_empty());
}

#[test]
fn validation_backend_executes_numeric_and_property_comparison_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_inclusive = NamedNode::new("http://www.w3.org/ns/shacl#minInclusive").unwrap();
    let sh_less_than = NamedNode::new("http://www.w3.org/ns/shacl#lessThan").unwrap();
    let shape = NamedNode::new("urn:score-shape").unwrap();
    let property_shape = NamedNode::new("urn:score-property").unwrap();
    let focus = NamedNode::new("urn:item").unwrap();
    let score = NamedNode::new("urn:score").unwrap();
    let limit = NamedNode::new("urn:limit").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(score.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_min_inclusive,
            Term::Literal(Literal::from(10)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_less_than,
            Term::NamedNode(limit.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-numeric").unwrap(),
            quads: vec![
                Quad::new(
                    focus.clone(),
                    score,
                    Term::Literal(Literal::from(8)),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus,
                    limit,
                    Term::Literal(Literal::from(7)),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("minimum"))
    );
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("does not satisfy comparison"))
    );
}

#[test]
fn validation_backend_executes_logical_not_and_qualified_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_class = NamedNode::new("http://www.w3.org/ns/shacl#class").unwrap();
    let sh_not = NamedNode::new("http://www.w3.org/ns/shacl#not").unwrap();
    let sh_or = NamedNode::new("http://www.w3.org/ns/shacl#or").unwrap();
    let sh_qvs = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap();
    let sh_qmin = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedMinCount").unwrap();
    let ex_allowed = NamedNode::new("urn:Allowed").unwrap();
    let ex_friend = NamedNode::new("urn:Friend").unwrap();
    let target_shape = NamedNode::new("urn:target-shape").unwrap();
    let allowed_shape = NamedNode::new("urn:allowed-shape").unwrap();
    let alternate_shape = NamedNode::new("urn:alternate-shape").unwrap();
    let forbidden_shape = NamedNode::new("urn:forbidden-shape").unwrap();
    let friend_shape = NamedNode::new("urn:friend-shape").unwrap();
    let property_shape = NamedNode::new("urn:friend-property").unwrap();
    let focus = NamedNode::new("urn:entity").unwrap();
    let friend = NamedNode::new("urn:friend").unwrap();
    let member = NamedNode::new("urn:member").unwrap();
    let list = oxrdf::BlankNode::new("list-1").unwrap();
    let tail = oxrdf::BlankNode::new("list-2").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            target_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            allowed_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            forbidden_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            alternate_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            friend_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            target_shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            allowed_shape.clone(),
            sh_class.clone(),
            Term::NamedNode(ex_allowed.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            forbidden_shape.clone(),
            sh_class.clone(),
            Term::NamedNode(ex_friend.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            friend_shape.clone(),
            sh_class.clone(),
            Term::NamedNode(ex_friend.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            alternate_shape.clone(),
            sh_class.clone(),
            Term::NamedNode(NamedNode::new("urn:Missing").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            target_shape.clone(),
            sh_not,
            Term::NamedNode(forbidden_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            target_shape.clone(),
            sh_or,
            Term::BlankNode(list.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list.clone(),
            rdf_first.clone(),
            Term::NamedNode(allowed_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list,
            rdf_rest.clone(),
            Term::BlankNode(tail.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            tail.clone(),
            rdf_first,
            Term::NamedNode(alternate_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            tail,
            rdf_rest,
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            target_shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(member.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_qvs,
            Term::NamedNode(friend_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_qmin,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-logical").unwrap(),
            quads: vec![
                Quad::new(
                    focus.clone(),
                    rdf_type.clone(),
                    Term::NamedNode(ex_friend),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus.clone(),
                    member,
                    Term::NamedNode(friend.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    friend,
                    rdf_type,
                    Term::NamedNode(ex_allowed),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("forbidden shape"))
    );
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("logical constraint"))
    );
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("qualified value shape"))
    );
}

#[test]
fn validation_backend_reports_closed_shapes_and_unsupported_constraints_with_metadata() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_closed = NamedNode::new("http://www.w3.org/ns/shacl#closed").unwrap();
    let sh_sparql = NamedNode::new("http://www.w3.org/ns/shacl#sparql").unwrap();
    let sh_select = NamedNode::new("http://www.w3.org/ns/shacl#select").unwrap();
    let sh_severity = NamedNode::new("http://www.w3.org/ns/shacl#severity").unwrap();
    let sh_warning = NamedNode::new("http://www.w3.org/ns/shacl#Warning").unwrap();
    let shape = NamedNode::new("urn:closed-shape").unwrap();
    let property_shape = NamedNode::new("urn:allowed-property").unwrap();
    let sparql = NamedNode::new("urn:unsupported-sparql").unwrap();
    let focus = NamedNode::new("urn:focus-closed").unwrap();
    let allowed = NamedNode::new("urn:allowed").unwrap();
    let extra = NamedNode::new("urn:extra").unwrap();

    let resolved = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:shape-source").unwrap(),
            quads: vec![
                Quad::new(
                    shape.clone(),
                    rdf_type.clone(),
                    Term::NamedNode(sh_node_shape),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    shape.clone(),
                    sh_target_node,
                    Term::NamedNode(focus.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    shape.clone(),
                    sh_property,
                    Term::NamedNode(property_shape.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    shape.clone(),
                    sh_closed,
                    Term::Literal(Literal::from(true)),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    shape.clone(),
                    sh_sparql,
                    Term::NamedNode(sparql.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    shape.clone(),
                    sh_severity,
                    Term::NamedNode(sh_warning),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    property_shape.clone(),
                    rdf_type,
                    Term::NamedNode(sh_property_shape),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    property_shape,
                    sh_path,
                    Term::NamedNode(allowed.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    sparql,
                    sh_select,
                    Term::Literal(Literal::new_simple_literal("SELECT $this WHERE { }")),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("shapes load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-closed").unwrap(),
            quads: vec![
                Quad::new(
                    focus.clone(),
                    allowed,
                    Term::Literal(Literal::from(1)),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus,
                    extra,
                    Term::Literal(Literal::from(2)),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("not allowed by closed shape"))
    );
    assert!(result.unsupported.is_empty());
    assert_eq!(result.coverage.unsupported_constraints, 0);
}

#[test]
fn validation_backend_executes_language_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_language_in = NamedNode::new("http://www.w3.org/ns/shacl#languageIn").unwrap();
    let sh_unique_lang = NamedNode::new("http://www.w3.org/ns/shacl#uniqueLang").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let shape = NamedNode::new("urn:lang-shape").unwrap();
    let property_shape = NamedNode::new("urn:label-property").unwrap();
    let focus = NamedNode::new("urn:labelled").unwrap();
    let label = NamedNode::new("urn:label").unwrap();
    let list = oxrdf::BlankNode::new("lang-list-1").unwrap();
    let tail = oxrdf::BlankNode::new("lang-list-2").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(label.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_language_in,
            Term::BlankNode(list.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list.clone(),
            rdf_first,
            Term::Literal(Literal::new_simple_literal("en")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list,
            rdf_rest,
            Term::BlankNode(tail.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            tail.clone(),
            NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap(),
            Term::Literal(Literal::new_simple_literal("fr")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            tail,
            NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap(),
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_unique_lang,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-lang").unwrap(),
            quads: vec![
                Quad::new(
                    focus.clone(),
                    label.clone(),
                    Term::Literal(Literal::new_language_tagged_literal("Hello", "en").unwrap()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus.clone(),
                    label.clone(),
                    Term::Literal(Literal::new_language_tagged_literal("Bonjour", "fr").unwrap()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus.clone(),
                    label.clone(),
                    Term::Literal(Literal::new_language_tagged_literal("Salut", "fr").unwrap()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    focus,
                    label,
                    Term::Literal(Literal::new_language_tagged_literal("Hallo", "de").unwrap()),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("language tag de"))
    );
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("duplicate language tag fr"))
    );
}

#[test]
fn validation_backend_executes_transitive_property_paths() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_zero_or_more = NamedNode::new("http://www.w3.org/ns/shacl#zeroOrMorePath").unwrap();
    let sh_one_or_more = NamedNode::new("http://www.w3.org/ns/shacl#oneOrMorePath").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let shape = NamedNode::new("urn:path-shape").unwrap();
    let zero_shape = NamedNode::new("urn:zero-shape").unwrap();
    let one_shape = NamedNode::new("urn:one-shape").unwrap();
    let focus = NamedNode::new("urn:start").unwrap();
    let link = NamedNode::new("urn:link").unwrap();
    let node2 = NamedNode::new("urn:node2").unwrap();
    let node3 = NamedNode::new("urn:node3").unwrap();
    let zero_path = oxrdf::BlankNode::new("zero-path").unwrap();
    let one_path = oxrdf::BlankNode::new("one-path").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property.clone(),
            Term::NamedNode(zero_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(one_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            zero_shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            zero_shape.clone(),
            sh_path.clone(),
            Term::BlankNode(zero_path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            zero_path,
            sh_zero_or_more,
            Term::NamedNode(link.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            zero_shape,
            sh_has_value.clone(),
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_shape.clone(),
            sh_path,
            Term::BlankNode(one_path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_path,
            sh_one_or_more,
            Term::NamedNode(link.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_shape.clone(),
            sh_min_count,
            Term::Literal(Literal::from(2)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_shape,
            sh_has_value,
            Term::NamedNode(node3.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-path").unwrap(),
            quads: vec![
                Quad::new(
                    focus.clone(),
                    link.clone(),
                    Term::NamedNode(node2.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    node2,
                    link,
                    Term::NamedNode(node3),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(result.conforms);
    assert!(result.unsupported.is_empty());
}

#[test]
fn validation_backend_reports_unsupported_path_forms_explicitly() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let unsupported_predicate = NamedNode::new("urn:unsupportedPathKind").unwrap();
    let shape = NamedNode::new("urn:unsupported-path-shape").unwrap();
    let property_shape = NamedNode::new("urn:unsupported-property-shape").unwrap();
    let focus = NamedNode::new("urn:unsupported-focus").unwrap();
    let unsupported_path = oxrdf::BlankNode::new("unsupported-path").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::BlankNode(unsupported_path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            unsupported_path,
            unsupported_predicate,
            Term::NamedNode(NamedNode::new("urn:any").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-unsupported-path").unwrap(),
            quads: vec![],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert_eq!(result.unsupported.len(), 1);
    assert_eq!(result.unsupported[0].kind, "property_path");
    assert!(
        result.unsupported[0]
            .reason
            .contains("not executable in the in-memory backend")
    );
}

#[test]
fn validation_backend_executes_regex_pattern_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_pattern = NamedNode::new("http://www.w3.org/ns/shacl#pattern").unwrap();
    let shape = NamedNode::new("urn:regex-shape").unwrap();
    let property_shape = NamedNode::new("urn:code-property").unwrap();
    let focus = NamedNode::new("urn:regex-focus").unwrap();
    let code = NamedNode::new("urn:code").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::NamedNode(code.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_pattern,
            Term::Literal(Literal::new_simple_literal("^[A-Z]{3}-\\d{2}$")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-regex").unwrap(),
            quads: vec![Quad::new(
                focus,
                code,
                Term::Literal(Literal::new_simple_literal("abc-12")),
                oxrdf::GraphName::DefaultGraph,
            )],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.message.contains("does not match pattern"))
    );
}

#[test]
fn validation_backend_executes_nested_inverse_transitive_paths() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_inverse_path = NamedNode::new("http://www.w3.org/ns/shacl#inversePath").unwrap();
    let sh_one_or_more = NamedNode::new("http://www.w3.org/ns/shacl#oneOrMorePath").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let shape = NamedNode::new("urn:inverse-shape").unwrap();
    let property_shape = NamedNode::new("urn:inverse-property").unwrap();
    let focus = NamedNode::new("urn:leaf").unwrap();
    let link = NamedNode::new("urn:link").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let middle = NamedNode::new("urn:middle").unwrap();
    let inverse_path = oxrdf::BlankNode::new("inv-path").unwrap();
    let one_or_more = oxrdf::BlankNode::new("oom-path").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape.clone(),
            sh_path,
            Term::BlankNode(inverse_path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inverse_path,
            sh_inverse_path,
            Term::BlankNode(one_or_more.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            one_or_more,
            sh_one_or_more,
            Term::NamedNode(link.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property_shape,
            sh_has_value,
            Term::NamedNode(root.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-inverse").unwrap(),
            quads: vec![
                Quad::new(
                    root,
                    link.clone(),
                    Term::NamedNode(middle.clone()),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    middle,
                    link,
                    Term::NamedNode(focus),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(result.conforms);
    assert!(result.unsupported.is_empty());
}

#[test]
fn validation_backend_executes_advanced_targets() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_target_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());

    let ex_targeted = NamedNode::new("http://example.org/Targeted").unwrap();
    let ex_thing = NamedNode::new("http://example.org/Thing").unwrap();
    let ex_flag = NamedNode::new("http://example.org/flag").unwrap();
    let ex_status = NamedNode::new("http://example.org/status").unwrap();
    let a = NamedNode::new("http://example.org/a").unwrap();
    let b = NamedNode::new("http://example.org/b").unwrap();
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-advanced-targets").unwrap(),
            quads: vec![
                Quad::new(
                    a.clone(),
                    NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
                    Term::NamedNode(ex_targeted),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    a,
                    ex_flag.clone(),
                    Term::Literal(Literal::new_simple_literal("yes")),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    b.clone(),
                    NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
                    Term::NamedNode(ex_thing),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    b.clone(),
                    ex_flag,
                    Term::Literal(Literal::new_simple_literal("keep")),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    b,
                    ex_status,
                    Term::Literal(Literal::new_simple_literal("review")),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(result.focus_nodes_evaluated >= 2);
    assert!(
        result
            .violations
            .iter()
            .any(|violation| violation.focus_node.contains("http://example.org/b"))
    );
}

#[test]
fn validation_backend_executes_sparql_constraints() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path(
            "../test-suite/sparql/node/sparql-001.ttl",
        ))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());

    let backend = InMemoryValidationBackend;
    let result = backend
        .execute(&plan, &resolved)
        .expect("validation executes");
    assert!(!result.conforms);
    assert_eq!(result.violations.len(), 3);
    assert!(result.unsupported.is_empty());
}

#[test]
fn validation_report_captures_core_constraint_component_metadata() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(suite_path("core/node/hasValue-001.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());

    let backend = InMemoryValidationBackend;
    let result = backend
        .execute(&plan, &resolved)
        .expect("validation executes");
    let report = build_validation_report(&result, &plan.view.program);

    assert!(!result.conforms);
    assert_eq!(result.violations.len(), 1);
    assert_eq!(
        report
            .quads
            .iter()
            .filter(|quad| quad.predicate.as_str() == "http://www.w3.org/ns/shacl#result")
            .count(),
        1
    );
    assert!(report.quads.iter().any(|quad| {
        quad.predicate.as_str() == "http://www.w3.org/ns/shacl#sourceConstraintComponent"
            && quad.object.to_string() == "<http://www.w3.org/ns/shacl#HasValueConstraintComponent>"
    }));
}

#[test]
fn validation_backend_executes_custom_component_sparql_validators() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());

    let ex_item = NamedNode::new("http://example.org/Item").unwrap();
    let ex_name = NamedNode::new("http://example.org/name").unwrap();
    let item = NamedNode::new("http://example.org/item1").unwrap();
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-component").unwrap(),
            quads: vec![
                Quad::new(
                    item.clone(),
                    NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
                    Term::NamedNode(ex_item),
                    oxrdf::GraphName::DefaultGraph,
                ),
                Quad::new(
                    item,
                    ex_name,
                    Term::Literal(Literal::new_simple_literal("abc")),
                    oxrdf::GraphName::DefaultGraph,
                ),
            ],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(!result.conforms);
    assert!(result.violations.iter().any(|violation| {
        violation
            .message
            .contains("Value shorter than 5 characters.")
    }));
}

#[test]
fn validation_report_captures_sparql_source_constraint_and_path() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(suite_path("sparql/node/sparql-001.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());

    let backend = InMemoryValidationBackend;
    let result = backend
        .execute(&plan, &resolved)
        .expect("validation executes");
    let report = build_validation_report(&result, &plan.view.program);

    assert_eq!(result.violations.len(), 3);
    assert!(report.quads.iter().any(|quad| {
        quad.predicate.as_str() == "http://www.w3.org/ns/shacl#sourceConstraint"
            && quad.object.to_string()
                == "<http://datashapes.org/sh/tests/sparql/node/sparql-001.test#TestShape-sparql>"
    }));
    assert!(report.quads.iter().any(|quad| {
        quad.predicate.as_str() == "http://www.w3.org/ns/shacl#resultPath"
            && quad.object.to_string() == "<http://www.w3.org/2000/01/rdf-schema#label>"
    }));
    let turtle = report
        .serialize(oxigraph::io::RdfFormat::Turtle)
        .expect("report should serialize");
    assert!(turtle.contains("sh:ValidationReport"));
}

#[test]
fn validation_backend_executes_triple_rules_before_validation() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_triple_rule = NamedNode::new("http://www.w3.org/ns/shacl#TripleRule").unwrap();
    let sh_subject = NamedNode::new("http://www.w3.org/ns/shacl#subject").unwrap();
    let sh_predicate = NamedNode::new("http://www.w3.org/ns/shacl#predicate").unwrap();
    let sh_object = NamedNode::new("http://www.w3.org/ns/shacl#object").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let sh_this = NamedNode::new("http://www.w3.org/ns/shacl#this").unwrap();
    let shape = NamedNode::new("urn:rule-shape").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();
    let property = NamedNode::new("urn:property").unwrap();
    let focus = NamedNode::new("urn:focus-rule").unwrap();
    let inferred_predicate = NamedNode::new("urn:p").unwrap();
    let inferred_object = NamedNode::new("urn:o").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            sh_path,
            Term::NamedNode(inferred_predicate.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property,
            sh_has_value,
            Term::NamedNode(inferred_object.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_triple_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_subject,
            Term::NamedNode(sh_this),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_predicate,
            Term::NamedNode(inferred_predicate),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_object,
            Term::NamedNode(inferred_object),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-rule").unwrap(),
            quads: vec![],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(result.conforms);
    assert!(result.unsupported.is_empty());
    assert_eq!(result.coverage.executed_rules, 2);
    assert_eq!(result.coverage.inferred_triples, 1);
    assert_eq!(result.coverage.inference_iterations, 2);
}

#[test]
fn validation_backend_executes_sparql_rules_before_validation() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let sh_prefixes = NamedNode::new("http://www.w3.org/ns/shacl#prefixes").unwrap();
    let sh_declare = NamedNode::new("http://www.w3.org/ns/shacl#declare").unwrap();
    let sh_prefix = NamedNode::new("http://www.w3.org/ns/shacl#prefix").unwrap();
    let sh_namespace = NamedNode::new("http://www.w3.org/ns/shacl#namespace").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let shape = NamedNode::new("urn:rule-shape-sparql").unwrap();
    let rule = NamedNode::new("urn:rule-sparql").unwrap();
    let property = NamedNode::new("urn:property-sparql").unwrap();
    let focus = NamedNode::new("urn:focus-rule-sparql").unwrap();
    let prefix_bundle = NamedNode::new("urn:prefixes").unwrap();
    let declaration = NamedNode::new("urn:declare").unwrap();
    let inferred_predicate = NamedNode::new("urn:sparql-p").unwrap();
    let inferred_object = NamedNode::new("urn:sparql-o").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_property,
            Term::NamedNode(property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            sh_path,
            Term::NamedNode(inferred_predicate.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property,
            sh_has_value,
            Term::NamedNode(inferred_object.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_prefixes,
            Term::NamedNode(prefix_bundle.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::new_simple_literal(
                "CONSTRUCT { $this ex:sparql-p ex:sparql-o } WHERE { }",
            )),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prefix_bundle,
            sh_declare,
            Term::NamedNode(declaration.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            declaration.clone(),
            sh_prefix,
            Term::Literal(Literal::new_simple_literal("ex")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            declaration,
            sh_namespace,
            Term::NamedNode(NamedNode::new("urn:").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-rule-sparql").unwrap(),
            quads: vec![],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(result.conforms);
    assert!(result.unsupported.is_empty());
    assert_eq!(result.coverage.executed_rules, 2);
    assert_eq!(result.coverage.inferred_triples, 1);
    assert_eq!(result.coverage.inference_iterations, 2);
}

#[test]
fn validation_backend_reports_generic_rules_as_unsupported() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let shape = NamedNode::new("urn:generic-rule-shape").unwrap();
    let rule = NamedNode::new("urn:generic-rule").unwrap();
    let focus = NamedNode::new("urn:focus-generic-rule").unwrap();

    let shapes = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_node,
            Term::NamedNode(focus.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_rule,
            Term::NamedNode(rule),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&shapes);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let data = load_with_ontoenv(
        &[ShapeSource::Quads {
            graph_iri: NamedNode::new("urn:data-generic-rule").unwrap(),
            quads: vec![],
        }],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: 0,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("data graph loads");

    let backend = InMemoryValidationBackend;
    let result = backend.execute(&plan, &data).expect("validation executes");
    assert!(
        result
            .unsupported
            .iter()
            .any(|unsupported| unsupported.kind == "rule")
    );
}

#[test]
fn lowers_property_paths_and_qualified_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_alt = NamedNode::new("http://www.w3.org/ns/shacl#alternativePath").unwrap();
    let sh_qvs = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap();
    let sh_qmin = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedMinCount").unwrap();
    let sh_qdisjoint =
        NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint").unwrap();
    let prop = NamedNode::new("urn:prop").unwrap();
    let thumb = NamedNode::new("urn:thumb").unwrap();
    let p1 = NamedNode::new("urn:p1").unwrap();
    let p2 = NamedNode::new("urn:p2").unwrap();
    let alt = oxrdf::BlankNode::new("a1").unwrap();
    let list1 = oxrdf::BlankNode::new("l1").unwrap();
    let list2 = oxrdf::BlankNode::new("l2").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            prop.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            thumb.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_path,
            Term::BlankNode(alt.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            alt.clone(),
            sh_alt,
            Term::BlankNode(list1.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1.clone(),
            rdf_first.clone(),
            Term::NamedNode(p1),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1,
            rdf_rest.clone(),
            Term::BlankNode(list2.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2.clone(),
            rdf_first,
            Term::NamedNode(p2),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2,
            rdf_rest,
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qvs,
            Term::NamedNode(thumb),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qmin,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qdisjoint,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let prop_shape = program
        .shapes
        .iter()
        .find(|shape| shape.source == Term::NamedNode(prop.clone()))
        .unwrap();

    assert!(matches!(
        prop_shape.path,
        Some(shifty_shacl_core::algebra::PropertyPath::Alternative(ref inner)) if inner.len() == 2
    ));
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::QualifiedValueShape {
                shape: Some(_),
                min_count: Some(1),
                disjoint: Some(true),
                ..
            }
        )
    }));
}

#[test]
fn rule_conditions_create_owner_dependencies() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let cond = NamedNode::new("urn:cond").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            cond.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type,
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(cond.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from("CONSTRUCT { } WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let owner_id = program.shape_index[&Term::NamedNode(owner).to_string()];
    let cond_id = program.shape_index[&Term::NamedNode(cond).to_string()];
    assert!(
        program.dependencies.iter().any(|edge| edge.from == owner_id
            && edge.to == cond_id
            && edge.kind == "rule_condition")
    );
}

#[test]
fn parse_separates_metadata_from_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdfs_label = NamedNode::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_class = NamedNode::new("http://www.w3.org/ns/shacl#targetClass").unwrap();
    let ex_activate = NamedNode::new("http://example.org/activate").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();
    let target_class = NamedNode::new("urn:Target").unwrap();

    let syntax = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            rdfs_label.clone(),
            Term::Literal(Literal::from("A shape label")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_class,
            Term::NamedNode(target_class),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            ex_activate.clone(),
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let shape = syntax
        .shapes
        .iter()
        .find(|shape| !shape.targets.is_empty())
        .unwrap();
    assert!(
        shape
            .extras
            .iter()
            .any(|extra| extra.predicate == rdfs_label)
    );
    assert!(
        shape
            .constraints
            .iter()
            .any(|constraint| constraint.predicate == ex_activate)
    );
}

#[test]
fn core_feature_is_always_present() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();

    let doc = parse_quads(vec![Quad::new(
        shape,
        rdf_type,
        Term::NamedNode(sh_node_shape),
        oxrdf::GraphName::DefaultGraph,
    )]);
    let program = lower_to_program(&doc);

    assert!(
        program
            .features
            .iter()
            .any(|feature| matches!(feature, shifty_shacl_core::algebra::FeatureUse::Core))
    );
}

#[test]
fn discovers_inline_shapes_from_logical_lists_and_rule_conditions() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_or = NamedNode::new("http://www.w3.org/ns/shacl#or").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let path = NamedNode::new("urn:path").unwrap();
    let list1 = oxrdf::BlankNode::new("ll1").unwrap();
    let list2 = oxrdf::BlankNode::new("ll2").unwrap();
    let inline_node = oxrdf::BlankNode::new("inline-node").unwrap();
    let inline_prop = oxrdf::BlankNode::new("inline-prop").unwrap();
    let inline_cond = oxrdf::BlankNode::new("inline-cond").unwrap();
    let inline_cond_prop = oxrdf::BlankNode::new("inline-cond-prop").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let syntax = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_or,
            Term::BlankNode(list1.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1.clone(),
            rdf_first.clone(),
            Term::BlankNode(inline_node.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1,
            rdf_rest.clone(),
            Term::BlankNode(list2.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2.clone(),
            rdf_first,
            Term::NamedNode(owner.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2,
            rdf_rest,
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_node.clone(),
            sh_property.clone(),
            Term::BlankNode(inline_prop.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_prop.clone(),
            sh_path.clone(),
            Term::NamedNode(path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_prop.clone(),
            sh_min_count.clone(),
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type,
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::BlankNode(inline_cond.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from("CONSTRUCT { } WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond.clone(),
            sh_property,
            Term::BlankNode(inline_cond_prop.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond_prop.clone(),
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond_prop,
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_node.clone()))
    );
    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_prop.clone()))
    );
    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_cond.clone()))
    );
}

#[test]
fn discovers_and_normalizes_inline_target_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target = NamedNode::new("http://www.w3.org/ns/shacl#target").unwrap();
    let sh_target_shape = NamedNode::new("http://www.w3.org/ns/shacl#targetShape").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_has_value = NamedNode::new("http://www.w3.org/ns/shacl#hasValue").unwrap();
    let root = NamedNode::new("urn:root").unwrap();
    let target = oxrdf::BlankNode::new("target").unwrap();
    let inline_shape = oxrdf::BlankNode::new("inline-target-shape").unwrap();
    let inline_property = oxrdf::BlankNode::new("inline-target-property").unwrap();
    let path = NamedNode::new("urn:status").unwrap();

    let syntax = parse_quads(vec![
        Quad::new(
            root.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            root,
            sh_target,
            Term::BlankNode(target.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            target.clone(),
            sh_target_shape,
            Term::BlankNode(inline_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_shape.clone(),
            sh_property,
            Term::BlankNode(inline_property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_property.clone(),
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_property,
            sh_has_value,
            Term::Literal(Literal::from("ready")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&syntax);

    assert!(
        program
            .shapes
            .iter()
            .any(|shape| shape.source == Term::BlankNode(inline_shape.clone()))
    );
    assert_eq!(
        program.shape_index[&Term::BlankNode(inline_shape.clone()).to_string()],
        program.normalized_shape_index["<urn:root>/target[0]/targetShape"]
    );
    assert!(
        program
            .normalized_shape_index
            .contains_key("<urn:root>/target[0]/targetShape/property[0]")
    );
}

#[test]
fn renders_graphviz_for_shape_program() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let dot = render_shape_program_dot(&program);

    assert!(dot.contains("digraph shacl_core"));
    assert!(dot.contains("shape_1"));
    assert!(dot.contains("target_1"));
}

#[test]
fn parses_constraint_component_definitions() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    assert_eq!(syntax.shapes.len(), 2);
    assert_eq!(syntax.constraint_components.len(), 1);
    let component = &syntax.constraint_components[0];
    assert!(component.subject.to_string().contains("LengthComponent"));
    assert_eq!(component.parameters.len(), 2);
    assert_eq!(
        component.parameters[1].var_name.as_deref(),
        Some("minLength")
    );
    assert_eq!(component.validators.len(), 1);
    assert!(component.validators[0].select.is_some());
    assert_eq!(component.declarations.len(), 0);
    assert_eq!(component.label_template, None);
}

#[test]
fn lowers_custom_component_attachments() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);

    assert_eq!(program.constraint_components.len(), 1);
    let component = &program.constraint_components[0];
    assert_eq!(
        component.parameters[1].var_name.as_deref(),
        Some("minLength")
    );
    assert_eq!(component.message_templates.len(), 1);
    assert!(
        component.message_templates[0]
            .parts
            .iter()
            .any(|part| matches!(
                part,
                shifty_shacl_core::algebra::TemplatePart::Slot {
                    kind: shifty_shacl_core::algebra::TemplateSlotKind::Variable,
                    name,
                } if name == "minLength"
            ))
    );
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent {
                predicate,
                component: Some(_),
                bindings,
                message_templates,
                ..
            } if predicate.as_str() == "http://example.org/activate"
                && bindings.iter().any(|binding| {
                    binding.name == "minLength"
                        && binding.from_default
                        && binding.values.iter().any(|value| value.to_string().contains('5'))
                })
                && message_templates.iter().any(|template| template.raw.contains("minLength"))
        )
    }));
}

#[test]
fn derives_custom_component_binding_names_from_parameter_paths() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(suite_path(
            "sparql/component/validator-001.ttl",
        ))],
        &SourceLoadOptions {
            include_imports: false,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load without imports");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);

    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent {
                predicate,
                bindings,
                ..
            } if predicate.as_str()
                == "http://datashapes.org/sh/tests/sparql/component/validator-001.test#test1"
                && bindings.iter().any(|binding| binding.name == "test1")
                && bindings.iter().any(|binding| binding.name == "test2")
        )
    }));
}

#[test]
fn parses_first_class_sparql_constraints() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path(
            "../test-suite/sparql/node/sparql-001.ttl",
        ))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let shape = syntax
        .shapes
        .iter()
        .find(|shape| shape.subject.to_string().contains("TestShape"))
        .expect("test fixture should include the shape");
    assert_eq!(shape.sparql_constraints.len(), 1);
    let sparql = &shape.sparql_constraints[0];
    assert!(sparql.select.is_some());
    assert_eq!(sparql.messages.len(), 1);
    assert_eq!(sparql.prefixes.len(), 1);

    let program = lower_to_program(&syntax);
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::Sparql(sparql)
            if sparql.select.is_some() && sparql.messages.len() == 1 && sparql.prefixes.len() == 1
        )
    }));
}

#[test]
fn lowers_label_templates_on_constraint_components() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path(
            "../test-suite/sparql/component/propertyValidator-select-001.ttl",
        ))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let component = syntax
        .constraint_components
        .iter()
        .find(|component| {
            component
                .subject
                .to_string()
                .contains("LanguageConstraintComponentUsingSELECT")
        })
        .expect("fixture should include the language component");
    assert!(component.label_template.is_some());
    assert_eq!(component.validators.len(), 1);
    assert_eq!(component.validators[0].prefixes.len(), 1);

    let program = lower_to_program(&syntax);
    let component = program
        .constraint_components
        .iter()
        .find(|component| {
            component
                .subject
                .to_string()
                .contains("LanguageConstraintComponentUsingSELECT")
        })
        .expect("component should lower");
    assert!(component.label_template.is_some());
    assert!(component.label_template_expr.is_some());
    assert!(
        component
            .label_template_expr
            .as_ref()
            .unwrap()
            .parts
            .iter()
            .any(|part| matches!(
                part,
                shifty_shacl_core::algebra::TemplatePart::Slot {
                    kind: shifty_shacl_core::algebra::TemplateSlotKind::Parameter,
                    name,
                } if name == "lang"
            ))
    );
    assert_eq!(component.validators[0].prefixes.len(), 1);
    assert!(
        program
            .features
            .iter()
            .any(|feature| matches!(feature, shifty_shacl_core::algebra::FeatureUse::Templates))
    );
    assert!(
        program
            .features
            .iter()
            .any(|feature| matches!(feature, shifty_shacl_core::algebra::FeatureUse::Sparql))
    );
}

#[test]
fn canonicalize_program_deduplicates_and_rebuilds_indexes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let a = NamedNode::new("urn:a").unwrap();
    let b = NamedNode::new("urn:b").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            a.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a.clone(),
            sh_node.clone(),
            Term::NamedNode(b.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a,
            sh_node,
            Term::NamedNode(b),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    assert_eq!(program.dependencies.len(), 1);

    let mut duplicated = program.clone();
    duplicated
        .dependencies
        .push(duplicated.dependencies[0].clone());
    duplicated
        .features
        .push(shifty_shacl_core::algebra::FeatureUse::Core);

    let canonical = canonicalize_program(&duplicated);
    assert_eq!(canonical.dependencies.len(), 1);
    assert_eq!(
        canonical
            .features
            .iter()
            .filter(|feature| matches!(feature, shifty_shacl_core::algebra::FeatureUse::Core))
            .count(),
        1
    );
    assert_eq!(canonical.shape_index.len(), canonical.shapes.len());
    assert_eq!(
        canonical.normalized_shape_index.len(),
        canonical.shapes.len()
    );
}

#[test]
fn prunes_deactivated_shapes_and_rules() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let sh_deactivated = NamedNode::new("http://www.w3.org/ns/shacl#deactivated").unwrap();
    let active = NamedNode::new("urn:active").unwrap();
    let inactive = NamedNode::new("urn:inactive").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            active.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            active.clone(),
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            active.clone(),
            sh_node,
            Term::NamedNode(inactive.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            active.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inactive.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inactive,
            sh_deactivated.clone(),
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type,
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_construct,
            Term::Literal(Literal::from("CONSTRUCT { } WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_deactivated,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    assert_eq!(program.shapes.len(), 2);
    assert_eq!(program.rules.len(), 1);

    let pruned = prune_deactivated_program(&program);
    assert_eq!(pruned.shapes.len(), 1);
    assert_eq!(pruned.rules.len(), 0);
    assert!(
        pruned
            .shape_index
            .contains_key(&Term::NamedNode(active.clone()).to_string())
    );
    assert!(
        !pruned
            .shape_index
            .contains_key(&Term::NamedNode(NamedNode::new("urn:inactive").unwrap()).to_string())
    );

    let normalized = normalize_program(
        &program,
        NormalizeOptions {
            prune_deactivated: true,
        },
    );
    assert_eq!(normalized.shapes.len(), 1);
    assert_eq!(normalized.rules.len(), 0);
}
