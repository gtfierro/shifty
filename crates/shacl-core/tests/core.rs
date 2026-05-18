use oxrdf::{Literal, NamedNode, Quad, Term};
use shifty_shacl_core::{
    BackendBucket, BackendViewOptions, ContextFootprint, NormalizeOptions, RewriteOptions,
    SliceReason, SliceRoots, StaticCostHint, analyze_program, analyze_static,
    analyze_static_with_roots, canonicalize_program, context_requirements, derive_backend_views,
    derive_inference_view, derive_validation_view, fingerprint_program, lower_to_program,
    normalize_program, parse_quads, prune_deactivated_program, render_shape_program_dot,
    rewrite_program, shared_work_candidates, slice_program,
    source::{RefreshMode, ShapeSource, SourceLoadOptions, load_with_ontoenv},
    static_cost_hints,
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/fixtures")
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
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
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
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property,
            activate,
            Term::Literal(Literal::from(true)),
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
    let ex_activate = NamedNode::new("http://example.org/activate").unwrap();
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
