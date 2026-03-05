use crate::codegen::render_tokens_as_module;
use crate::ir::{SrcGenIR, SrcGenPath};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

fn report_path_spec_expr(path: &SrcGenPath) -> TokenStream {
    match path {
        SrcGenPath::SimplePredicate { predicate_iri } => {
            let predicate_lit = LitStr::new(predicate_iri, Span::call_site());
            quote! { ReportPathSpec::SimplePredicate(#predicate_lit) }
        }
        SrcGenPath::Inverse { inner } => {
            let inner_expr = report_path_spec_expr(inner);
            quote! { ReportPathSpec::Inverse(Box::new(#inner_expr)) }
        }
        SrcGenPath::Sequence { items } => {
            let item_exprs: Vec<TokenStream> = items.iter().map(report_path_spec_expr).collect();
            quote! { ReportPathSpec::Sequence(vec![#(#item_exprs),*]) }
        }
        SrcGenPath::Alternative { items } => {
            let item_exprs: Vec<TokenStream> = items.iter().map(report_path_spec_expr).collect();
            quote! { ReportPathSpec::Alternative(vec![#(#item_exprs),*]) }
        }
        SrcGenPath::ZeroOrMore { inner } => {
            let inner_expr = report_path_spec_expr(inner);
            quote! { ReportPathSpec::ZeroOrMore(Box::new(#inner_expr)) }
        }
        SrcGenPath::OneOrMore { inner } => {
            let inner_expr = report_path_spec_expr(inner);
            quote! { ReportPathSpec::OneOrMore(Box::new(#inner_expr)) }
        }
        SrcGenPath::ZeroOrOne { inner } => {
            let inner_expr = report_path_spec_expr(inner);
            quote! { ReportPathSpec::ZeroOrOne(Box::new(#inner_expr)) }
        }
    }
}

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let path_spec_arms: Vec<TokenStream> = ir
        .property_shapes
        .iter()
        .map(|shape| {
            let shape_id = shape.id;
            let path_expr = report_path_spec_expr(&shape.path);
            quote! { #shape_id => Some(#path_expr), }
        })
        .collect();

    let tokens = quote! {
        const SH_RESULT: &str = "http://www.w3.org/ns/shacl#result";
        const SH_FOCUS_NODE: &str = "http://www.w3.org/ns/shacl#focusNode";
        const SH_SOURCE_SHAPE: &str = "http://www.w3.org/ns/shacl#sourceShape";
        const SH_SOURCE_COMPONENT: &str = "http://www.w3.org/ns/shacl#sourceConstraintComponent";
        const SH_SOURCE_CONSTRAINT: &str = "http://www.w3.org/ns/shacl#sourceConstraint";
        const SH_VALUE: &str = "http://www.w3.org/ns/shacl#value";
        const SH_RESULT_PATH: &str = "http://www.w3.org/ns/shacl#resultPath";
        const SH_MESSAGE: &str = "http://www.w3.org/ns/shacl#message";
        const SH_SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";

        #[derive(Clone)]
        enum ReportPathSpec {
            SimplePredicate(&'static str),
            Inverse(Box<ReportPathSpec>),
            Sequence(Vec<ReportPathSpec>),
            Alternative(Vec<ReportPathSpec>),
            ZeroOrMore(Box<ReportPathSpec>),
            OneOrMore(Box<ReportPathSpec>),
            ZeroOrOne(Box<ReportPathSpec>),
        }

        fn path_spec_for_shape(shape_id: u64) -> Option<ReportPathSpec> {
            match shape_id {
                #(#path_spec_arms)*
                _ => None,
            }
        }

        fn path_spec_requires_materialization(path_spec: &ReportPathSpec) -> bool {
            !matches!(path_spec, ReportPathSpec::SimplePredicate(_))
        }

        fn is_skolemized_path_term(term: &oxigraph::model::Term) -> bool {
            match term {
                oxigraph::model::Term::NamedNode(node) => node.as_str().contains("/.sk/"),
                _ => false,
            }
        }

        fn build_path_list_from_specs(
            items: &[ReportPathSpec],
            graph: &mut oxigraph::model::Graph,
        ) -> oxigraph::model::Term {
            let rdf_first =
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#first");
            let rdf_rest =
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest");
            let rdf_nil = oxigraph::model::Term::NamedNode(
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"),
            );

            if items.is_empty() {
                return rdf_nil;
            }

            let mut head: Option<oxigraph::model::BlankNode> = None;
            let mut prev: Option<oxigraph::model::BlankNode> = None;
            for item in items {
                let list_node = oxigraph::model::BlankNode::default();
                if let Some(prev_node) = prev.as_ref() {
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(prev_node.clone()),
                        rdf_rest.clone(),
                        oxigraph::model::Term::BlankNode(list_node.clone()),
                    ));
                } else {
                    head = Some(list_node.clone());
                }

                let value_term = build_path_term_from_spec(item, graph);
                graph.insert(&oxigraph::model::Triple::new(
                    oxigraph::model::NamedOrBlankNode::BlankNode(list_node.clone()),
                    rdf_first.clone(),
                    value_term,
                ));
                prev = Some(list_node);
            }

            if let Some(prev_node) = prev {
                graph.insert(&oxigraph::model::Triple::new(
                    oxigraph::model::NamedOrBlankNode::BlankNode(prev_node),
                    rdf_rest,
                    rdf_nil,
                ));
            }

            oxigraph::model::Term::BlankNode(
                head.expect("path list should have at least one blank node"),
            )
        }

        fn build_path_term_from_spec(
            path_spec: &ReportPathSpec,
            graph: &mut oxigraph::model::Graph,
        ) -> oxigraph::model::Term {
            match path_spec {
                ReportPathSpec::SimplePredicate(predicate_iri) => {
                    oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                        *predicate_iri,
                    ))
                }
                ReportPathSpec::Inverse(inner) => {
                    let path_node = oxigraph::model::BlankNode::default();
                    let inner_term = build_path_term_from_spec(inner, graph);
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(path_node.clone()),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#inversePath"),
                        inner_term,
                    ));
                    oxigraph::model::Term::BlankNode(path_node)
                }
                ReportPathSpec::Sequence(items) => build_path_list_from_specs(items, graph),
                ReportPathSpec::Alternative(items) => {
                    let path_node = oxigraph::model::BlankNode::default();
                    let list_term = build_path_list_from_specs(items, graph);
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(path_node.clone()),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#alternativePath"),
                        list_term,
                    ));
                    oxigraph::model::Term::BlankNode(path_node)
                }
                ReportPathSpec::ZeroOrMore(inner) => {
                    let path_node = oxigraph::model::BlankNode::default();
                    let inner_term = build_path_term_from_spec(inner, graph);
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(path_node.clone()),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#zeroOrMorePath"),
                        inner_term,
                    ));
                    oxigraph::model::Term::BlankNode(path_node)
                }
                ReportPathSpec::OneOrMore(inner) => {
                    let path_node = oxigraph::model::BlankNode::default();
                    let inner_term = build_path_term_from_spec(inner, graph);
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(path_node.clone()),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#oneOrMorePath"),
                        inner_term,
                    ));
                    oxigraph::model::Term::BlankNode(path_node)
                }
                ReportPathSpec::ZeroOrOne(inner) => {
                    let path_node = oxigraph::model::BlankNode::default();
                    let inner_term = build_path_term_from_spec(inner, graph);
                    graph.insert(&oxigraph::model::Triple::new(
                        oxigraph::model::NamedOrBlankNode::BlankNode(path_node.clone()),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#zeroOrOnePath"),
                        inner_term,
                    ));
                    oxigraph::model::Term::BlankNode(path_node)
                }
            }
        }

        pub fn build_violations(report_graph: &oxigraph::model::Graph) -> Vec<Violation> {
            let mut result_nodes: Vec<oxigraph::model::Term> = Vec::new();
            for triple in report_graph.iter() {
                if triple.predicate.as_str() == SH_RESULT {
                    result_nodes.push(triple.object.into());
                }
            }

            let mut violations = Vec::with_capacity(result_nodes.len());
            for result_node in result_nodes {
                let mut focus: Option<oxigraph::model::Term> = None;
                let mut value: Option<oxigraph::model::Term> = None;
                let mut path: Option<ResultPath> = None;
                let mut shape_id = 0_u64;
                let mut component_id = 0_u64;
                let mut source_component_iri: Option<String> = None;
                let mut source_constraint_term: Option<oxigraph::model::Term> = None;

                for triple in report_graph.iter() {
                    let subject_term = oxigraph::model::Term::from(triple.subject.clone());
                    if subject_term != result_node {
                        continue;
                    }

                    match triple.predicate.as_str() {
                        SH_FOCUS_NODE => {
                            focus = Some(triple.object.into());
                        }
                        SH_VALUE => {
                            value = Some(triple.object.into());
                        }
                        SH_RESULT_PATH => {
                            path = Some(ResultPath::Term(triple.object.into()));
                        }
                        SH_SOURCE_SHAPE => {
                            if let oxigraph::model::TermRef::NamedNode(node) = triple.object {
                                if let Some(id) = shape_id_for_iri(node.as_str()) {
                                    shape_id = id;
                                }
                            }
                        }
                        SH_SOURCE_COMPONENT => {
                            if let oxigraph::model::TermRef::NamedNode(node) = triple.object {
                                source_component_iri = Some(node.as_str().to_string());
                                if let Some(id) = component_id_for_iri(node.as_str()) {
                                    component_id = id;
                                }
                            }
                        }
                        SH_SOURCE_CONSTRAINT => {
                            source_constraint_term = Some(triple.object.into());
                        }
                        _ => {}
                    }
                }

                if let Some(source_constraint) = source_constraint_term.as_ref() {
                    if let Some(id) = component_id_for_source_constraint(source_constraint) {
                        component_id = id;
                    }
                }
                if let Some(source_component_iri) = source_component_iri.as_ref() {
                    if let Some(id) = component_id_for_shape_and_iri(shape_id, source_component_iri) {
                        component_id = id;
                    }
                }

                if let Some(focus) = focus {
                    violations.push(Violation {
                        shape_id,
                        component_id,
                        focus,
                        value,
                        path,
                    });
                }
            }

            violations
        }

        fn term_stable_string(term: &oxigraph::model::Term) -> String {
            match term {
                oxigraph::model::Term::NamedNode(node) => node.as_str().to_string(),
                _ => term.to_string(),
            }
        }

        fn shape_messages(shape_id: u64) -> Vec<oxigraph::model::Term> {
            let Ok(shape_ir) = embedded_shape_ir() else {
                return Vec::new();
            };
            let shape = shape_iri(shape_id);
            if shape.is_empty() {
                return Vec::new();
            }
            let Ok(message_predicate) = oxigraph::model::NamedNode::new(SH_MESSAGE) else {
                return Vec::new();
            };

            let mut messages = Vec::new();
            for quad in &shape_ir.shape_quads {
                if term_stable_string(&oxigraph::model::Term::from(quad.subject.clone())) == shape
                    && quad.predicate == message_predicate
                {
                    messages.push(quad.object.clone());
                }
            }
            messages
        }

        fn severity_term_from_shape(shape_id: u64) -> Option<oxigraph::model::Term> {
            let Ok(shape_ir) = embedded_shape_ir() else {
                return None;
            };

            let severity = shape_ir
                .node_shapes
                .iter()
                .find(|shape| shape.id.0 == shape_id)
                .map(|shape| shape.severity.clone())
                .or_else(|| {
                    shape_ir
                        .property_shapes
                        .iter()
                        .find(|shape| shape.id.0 == shape_id)
                        .map(|shape| shape.severity.clone())
                })?;

            match severity {
                shifty::shacl_ir::Severity::Violation => {
                    oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#Violation")
                        .ok()
                        .map(oxigraph::model::Term::NamedNode)
                }
                shifty::shacl_ir::Severity::Warning => {
                    oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#Warning")
                        .ok()
                        .map(oxigraph::model::Term::NamedNode)
                }
                shifty::shacl_ir::Severity::Info => {
                    oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#Info")
                        .ok()
                        .map(oxigraph::model::Term::NamedNode)
                }
                shifty::shacl_ir::Severity::Custom(iri) => {
                    Some(oxigraph::model::Term::NamedNode(iri))
                }
            }
        }

        fn shape_messages_from_store(
            store: &oxigraph::store::Store,
            shape_iri: &str,
        ) -> Vec<oxigraph::model::Term> {
            let Ok(shape_node) = oxigraph::model::NamedNode::new(shape_iri) else {
                return Vec::new();
            };
            let Ok(message_predicate) = oxigraph::model::NamedNode::new(SH_MESSAGE) else {
                return Vec::new();
            };
            let Ok(shape_graph) = oxigraph::model::NamedNode::new(SHAPE_GRAPH) else {
                return Vec::new();
            };

            let mut out = Vec::new();
            for quad in store.quads_for_pattern(
                None,
                Some(message_predicate.as_ref()),
                None,
                Some(oxigraph::model::GraphNameRef::NamedNode(shape_graph.as_ref())),
            ) {
                let Ok(quad) = quad else {
                    continue;
                };
                if quad.subject == oxigraph::model::NamedOrBlankNode::NamedNode(shape_node.clone()) {
                    out.push(quad.object);
                }
            }
            out
        }

        fn constraint_messages_from_store(
            store: &oxigraph::store::Store,
            source_constraint: &oxigraph::model::Term,
        ) -> Vec<oxigraph::model::Term> {
            let shape_subject = match source_constraint {
                oxigraph::model::Term::NamedNode(node) => {
                    oxigraph::model::NamedOrBlankNodeRef::NamedNode(node.as_ref())
                }
                oxigraph::model::Term::BlankNode(node) => {
                    oxigraph::model::NamedOrBlankNodeRef::BlankNode(node.as_ref())
                }
                _ => return Vec::new(),
            };

            let Ok(message_predicate) = oxigraph::model::NamedNode::new(SH_MESSAGE) else {
                return Vec::new();
            };
            let Ok(shape_graph) = oxigraph::model::NamedNode::new(SHAPE_GRAPH) else {
                return Vec::new();
            };
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(shape_graph.as_ref());

            let mut out = Vec::new();
            for quad in store.quads_for_pattern(
                Some(shape_subject),
                Some(message_predicate.as_ref()),
                None,
                Some(graph_ref),
            ) {
                let Ok(quad) = quad else {
                    continue;
                };
                out.push(quad.object);
            }
            out
        }

        fn shape_severity_from_store(
            store: &oxigraph::store::Store,
            shape_iri: &str,
        ) -> Option<oxigraph::model::Term> {
            let Ok(shape_node) = oxigraph::model::NamedNode::new(shape_iri) else {
                return None;
            };
            let Ok(severity_predicate) = oxigraph::model::NamedNode::new(SH_SEVERITY) else {
                return None;
            };
            let Ok(shape_graph) = oxigraph::model::NamedNode::new(SHAPE_GRAPH) else {
                return None;
            };

            for quad in store.quads_for_pattern(
                None,
                Some(severity_predicate.as_ref()),
                None,
                Some(oxigraph::model::GraphNameRef::NamedNode(shape_graph.as_ref())),
            ) {
                let Ok(quad) = quad else {
                    continue;
                };
                if quad.subject == oxigraph::model::NamedOrBlankNode::NamedNode(shape_node.clone()) {
                    return Some(quad.object);
                }
            }
            None
        }

        pub fn render_violations_to_turtle(
            violations: &[Violation],
            store: &oxigraph::store::Store,
        ) -> Result<String, String> {
            let mut graph = oxigraph::model::Graph::new();
            let report_node: oxigraph::model::NamedOrBlankNode =
                oxigraph::model::BlankNode::default().into();

            graph.insert(&oxigraph::model::Triple::new(
                report_node.clone(),
                oxigraph::model::vocab::rdf::TYPE,
                oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/ns/shacl#ValidationReport",
                )),
            ));

            graph.insert(&oxigraph::model::Triple::new(
                report_node.clone(),
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#conforms"),
                oxigraph::model::Term::Literal(oxigraph::model::Literal::from(violations.is_empty())),
            ));

            for violation in violations {
                let source_shape = shape_iri(violation.shape_id);
                let result_node: oxigraph::model::NamedOrBlankNode =
                    oxigraph::model::BlankNode::default().into();
                graph.insert(&oxigraph::model::Triple::new(
                    report_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#result"),
                    oxigraph::model::Term::from(result_node.clone()),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::vocab::rdf::TYPE,
                    oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                        "http://www.w3.org/ns/shacl#ValidationResult",
                    )),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked(
                        "http://www.w3.org/ns/shacl#resultSeverity",
                    ),
                    shape_severity_from_store(store, source_shape)
                        .or_else(|| severity_term_from_shape(violation.shape_id))
                        .unwrap_or_else(|| {
                            oxigraph::model::Term::NamedNode(
                                oxigraph::model::NamedNode::new_unchecked(
                                    "http://www.w3.org/ns/shacl#Violation",
                                ),
                            )
                        }),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#focusNode"),
                    violation.focus.clone(),
                ));
                let source_constraint_term =
                    source_constraint_term_for_component(violation.component_id);

                let allow_focus_value_fallback = component_iri(violation.component_id)
                    != "http://www.w3.org/ns/shacl#HasValueConstraintComponent";
                if let Some(value_term) = violation
                    .value
                    .clone()
                    .or_else(|| {
                        if violation.path.is_none() && allow_focus_value_fallback {
                            Some(violation.focus.clone())
                        } else {
                            None
                        }
                    })
                {
                    let value_term = match (&violation.path, &value_term) {
                        (
                            Some(ResultPath::Term(oxigraph::model::Term::NamedNode(predicate))),
                            oxigraph::model::Term::Literal(literal),
                        ) => resolve_original_literal_term(
                            &violation.focus,
                            predicate,
                            literal,
                        )
                        .unwrap_or_else(|| value_term.clone()),
                        _ => value_term,
                    };
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node.clone(),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#value"),
                        value_term,
                    ));
                }

                if let Some(path) = &violation.path {
                    let mut resolved_path_term = match path {
                        ResultPath::Term(path_term) => Some(path_term.clone()),
                        ResultPath::PathId(_) => None,
                    };
                    if let Some(path_spec) = path_spec_for_shape(violation.shape_id) {
                        let should_materialize = path_spec_requires_materialization(&path_spec)
                            || matches!(
                                path,
                                ResultPath::Term(path_term) if is_skolemized_path_term(path_term)
                            )
                            || matches!(path, ResultPath::PathId(_));
                        if should_materialize {
                            resolved_path_term =
                                Some(build_path_term_from_spec(&path_spec, &mut graph));
                        }
                    }
                    if let Some(path_term) = resolved_path_term {
                        graph.insert(&oxigraph::model::Triple::new(
                            result_node.clone(),
                            oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#resultPath"),
                            path_term,
                        ));
                    }
                }

                let mut message_terms = shape_messages(violation.shape_id);
                for message_term in shape_messages_from_store(store, source_shape) {
                    if !message_terms.contains(&message_term) {
                        message_terms.push(message_term);
                    }
                }
                if let Some(source_constraint) = source_constraint_term.as_ref() {
                    for message_term in constraint_messages_from_store(store, source_constraint) {
                        if !message_terms.contains(&message_term) {
                            message_terms.push(message_term);
                        }
                    }
                }
                for message_term in message_terms {
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node.clone(),
                        oxigraph::model::NamedNode::new_unchecked(
                            "http://www.w3.org/ns/shacl#resultMessage",
                        ),
                        message_term,
                    ));
                }

                if !source_shape.is_empty() {
                    if let Ok(shape_node) = oxigraph::model::NamedNode::new(source_shape) {
                        graph.insert(&oxigraph::model::Triple::new(
                            result_node.clone(),
                            oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#sourceShape"),
                            oxigraph::model::Term::NamedNode(shape_node),
                        ));
                    }
                }

                if let Some(source_constraint) = source_constraint_term {
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node.clone(),
                        oxigraph::model::NamedNode::new_unchecked(
                            "http://www.w3.org/ns/shacl#sourceConstraint",
                        ),
                        source_constraint,
                    ));
                }

                let source_component = component_iri(violation.component_id);
                if let Ok(component_node) = oxigraph::model::NamedNode::new(source_component) {
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node.clone(),
                        oxigraph::model::NamedNode::new_unchecked(
                            "http://www.w3.org/ns/shacl#sourceConstraintComponent",
                        ),
                        oxigraph::model::Term::NamedNode(component_node),
                    ));
                }
            }

            let mut writer = Vec::new();
            let mut serializer = oxigraph::io::RdfSerializer::from_format(oxigraph::io::RdfFormat::Turtle)
                .with_prefix("sh", "http://www.w3.org/ns/shacl#")
                .map_err(|err| format!("failed to set sh prefix: {err}"))?
                .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                .map_err(|err| format!("failed to set rdf prefix: {err}"))?
                .for_writer(&mut writer);
            for triple in graph.iter() {
                serializer
                    .serialize_triple(triple)
                    .map_err(|err| format!("failed to serialize report triple: {err}"))?;
            }
            serializer
                .finish()
                .map_err(|err| format!("failed to finish report serialization: {err}"))?;
            String::from_utf8(writer).map_err(|err| format!("report is not valid utf8: {err}"))
        }

        pub fn build_report_from_specialized_violations(
            violations: Vec<Violation>,
            store: &oxigraph::store::Store,
        ) -> Result<Report, String> {
            record_violation_metrics(&violations);
            let report_turtle = render_violations_to_turtle(&violations, store)?;
            Ok(Report {
                violations,
                report_turtle: report_turtle.clone(),
                report_turtle_follow_bnodes: report_turtle,
            })
        }

        pub fn build_report_from_runtime_validation_report(
            report: &shifty::ValidationReport,
        ) -> Report {
            let report_graph = report.to_graph_with_options(shifty::ValidationReportOptions {
                follow_bnodes: false,
            });
            let violations = build_violations(&report_graph);
            record_violation_metrics(&violations);

            let report_turtle = report
                .to_turtle_with_options(shifty::ValidationReportOptions {
                    follow_bnodes: false,
                })
                .unwrap_or_else(|err| {
                    eprintln!("srcgen runtime error: failed to serialize report: {err}");
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n".to_string()
                });
            let report_turtle_follow_bnodes = report
                .to_turtle_with_options(shifty::ValidationReportOptions {
                    follow_bnodes: true,
                })
                .unwrap_or_else(|_| report_turtle.clone());

            Report {
                violations,
                report_turtle,
                report_turtle_follow_bnodes,
            }
        }

        fn record_violation_metrics(violations: &[Violation]) {
            for violation in violations {
                record_component_violation(violation.component_id);
            }
        }

        pub fn empty_conforming_report() -> Report {
            Report {
                violations: Vec::new(),
                report_turtle: String::from(
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n",
                ),
                report_turtle_follow_bnodes: String::from(
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n",
                ),
            }
        }

        pub fn strict_incomplete_report(reason: &str) -> Report {
            let report = format!(
                "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms false ; sh:result [ a sh:ValidationResult ; sh:resultSeverity sh:Violation ; sh:resultMessage {:?} ] .\n",
                reason
            );
            Report {
                violations: vec![Violation {
                    shape_id: 0,
                    component_id: 0,
                    focus: oxigraph::model::Term::BlankNode(oxigraph::model::BlankNode::default()),
                    value: None,
                    path: None,
                }],
                report_turtle: report.clone(),
                report_turtle_follow_bnodes: report,
            }
        }

        pub fn render_report(
            report: &Report,
            _store: &oxigraph::store::Store,
            follow_bnodes: bool,
        ) -> String {
            if follow_bnodes {
                return report.report_turtle_follow_bnodes.clone();
            }
            report.report_turtle.clone()
        }
    };
    render_tokens_as_module(tokens)
}
