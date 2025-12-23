use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use oxigraph::model::{
    BlankNode, GraphName, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
    NamedOrBlankNodeRef as SubjectRef, Quad, Term, TermRef,
};

use super::{
    component_registry::COMPONENT_REGISTRY, parse_node_shape, ParsingContext, ToSubjectRef,
};
use crate::context::model::CustomComponentCache;
use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::model::components::ComponentDescriptor;
use crate::model::templates::{
    ComponentTemplateDefinition, PrefixDeclaration, ShapeTemplateDefinition, TemplateParameter,
    TemplateValidators,
};
use crate::named_nodes::{RDF, RDFS, SHACL};
use crate::types::{ComponentID, ID};

type CustomComponentMaps = (
    HashMap<NamedNode, CustomConstraintComponentDefinition>,
    HashMap<NamedNode, Vec<NamedNode>>,
);

/// Parses all constraint components attached to a given shape subject (`start`) from the shapes graph.
///
/// Returns data-only `ComponentDescriptor`s keyed by `ComponentID` for later runtime instantiation.
pub(crate) fn parse_components(
    shape_term: &Term,
    context: &mut ParsingContext,
    unique_lang_lexicals: &HashMap<Term, String>,
    is_property_shape: bool,
) -> Result<HashMap<ComponentID, ComponentDescriptor>, String> {
    let mut descriptors = HashMap::new();
    let shacl = SHACL::new();
    let shape_ref = shape_term.as_ref();

    let pred_obj_pairs: HashMap<NamedNode, Vec<Term>> = context
        .store
        .quads_for_pattern(
            Some(shape_ref.to_subject_ref()),
            None,
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .fold(HashMap::new(), |mut acc, quad| {
            acc.entry(quad.predicate).or_default().push(quad.object);
            acc
        });

    let mut processed_predicates = HashSet::new();

    for entry in COMPONENT_REGISTRY {
        (entry.apply)(
            &shacl,
            shape_term,
            context,
            unique_lang_lexicals,
            &pred_obj_pairs,
            &mut processed_predicates,
            &mut descriptors,
            is_property_shape,
        )?;
    }

    if let Some(sparql_terms) = pred_obj_pairs.get(&shacl.sparql.into_owned()) {
        processed_predicates.insert(shacl.sparql.into_owned());
        for sparql_term in sparql_terms {
            validate_sparql_constraint_node(context, sparql_term, is_property_shape)?;
            let component_id = context.get_or_create_component_id(sparql_term.clone());
            descriptors.insert(
                component_id,
                ComponentDescriptor::Sparql {
                    constraint_node: sparql_term.clone(),
                },
            );
        }
    }

    if context.features.enable_af {
        let cache = get_or_init_custom_component_cache(context)?;
        let param_to_component = &cache.param_to_component;

        let mut shape_predicates: HashSet<NamedNode> = pred_obj_pairs.keys().cloned().collect();
        for p in processed_predicates {
            shape_predicates.remove(&p);
        }

        let mut component_candidates: HashSet<NamedNode> = HashSet::new();
        for p in &shape_predicates {
            if let Some(ccs) = param_to_component.get(p) {
                component_candidates.extend(ccs.iter().cloned());
            }
        }

        for cc_iri in component_candidates {
            if let Some(cc_def) = cache.definitions.get(&cc_iri) {
                let mut has_all_mandatory = true;
                let mut parameter_values = HashMap::new();

                for param in &cc_def.parameters {
                    if let Some(values) = pred_obj_pairs.get(&param.path) {
                        parameter_values.insert(param.path.clone(), values.clone());
                    } else if !param.default_values.is_empty() {
                        parameter_values.insert(param.path.clone(), param.default_values.clone());
                    } else if !param.optional {
                        has_all_mandatory = false;
                        break;
                    }
                }

                if has_all_mandatory {
                    let param_entries: Vec<String> = cc_def
                        .parameters
                        .iter()
                        .map(|param| {
                            let mut values: Vec<String> = parameter_values
                                .get(&param.path)
                                .map(|vals| vals.iter().map(|v| v.to_string()).collect())
                                .unwrap_or_else(|| vec!["<none>".to_string()]);
                            values.sort();
                            format!("{}={}", param.path.as_str(), values.join(","))
                        })
                        .collect();
                    let component_key = format!(
                        "CustomConstraint:{}|{}",
                        cc_iri.as_str(),
                        param_entries.join("|")
                    );
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(component_key),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::Custom {
                            definition: Box::new(cc_def.clone()),
                            parameter_values,
                        },
                    );
                }
            }
        }
    }

    Ok(descriptors)
}

fn get_or_init_custom_component_cache(
    context: &mut ParsingContext,
) -> Result<Arc<CustomComponentCache>, String> {
    if context.custom_component_cache.is_none() {
        register_shape_templates(context)?;
        let (mut custom_component_defs, param_to_component) =
            parse_custom_constraint_components(context)?;
        register_component_templates(context, &custom_component_defs)?;
        for definition in custom_component_defs.values_mut() {
            if definition.template.is_none() {
                if let Some(template) = context.component_templates.get(&definition.iri).cloned() {
                    definition.template = Some(template);
                }
            }
        }
        context.custom_component_cache = Some(Arc::new(CustomComponentCache {
            definitions: custom_component_defs,
            param_to_component,
        }));
    }
    Ok(context
        .custom_component_cache
        .as_ref()
        .expect("custom component cache must be initialized")
        .clone())
}

fn validate_sparql_constraint_node(
    context: &ParsingContext,
    constraint_term: &Term,
    is_property_shape: bool,
) -> Result<(), String> {
    let shacl = SHACL::new();
    let subject_node = constraint_term
        .as_ref()
        .try_to_subject_ref()
        .map_err(|e| {
            format!(
                "Invalid sh:sparql constraint node {:?}: {}",
                constraint_term, e
            )
        })?
        .into_owned();

    let shape_graph = context.shape_graph_iri_ref();
    let mut found_query = false;

    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_node.as_ref()),
            Some(shacl.select),
            None,
            Some(shape_graph),
        )
        .filter_map(Result::ok)
    {
        let query_term = quad.object;
        let query_str = match &query_term {
            Term::Literal(lit) => lit.value().to_string(),
            _ => {
                return Err(format!(
                    "SPARQL constraint {} must provide its sh:select query as a literal.",
                    constraint_term
                ))
            }
        };
        crate::sparql::validate_prebound_variable_usage(
            &query_str,
            &format!("SPARQL constraint {}", constraint_term),
            true,
            is_property_shape,
        )?;
        found_query = true;
    }

    let ask_pred = NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#ask");
    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_node.as_ref()),
            Some(ask_pred),
            None,
            Some(shape_graph),
        )
        .filter_map(Result::ok)
    {
        let query_term = quad.object;
        let query_str = match &query_term {
            Term::Literal(lit) => lit.value().to_string(),
            _ => {
                return Err(format!(
                    "SPARQL constraint {} must provide its sh:ask query as a literal.",
                    constraint_term
                ))
            }
        };
        crate::sparql::validate_prebound_variable_usage(
            &query_str,
            &format!("SPARQL constraint {}", constraint_term),
            true,
            is_property_shape,
        )?;
        found_query = true;
    }

    if !found_query {
        return Err(format!(
            "SPARQL constraint {} must declare sh:select or sh:ask.",
            constraint_term
        ));
    }

    Ok(())
}

fn to_subject_ref(term: TermRef<'_>) -> Result<SubjectRef<'_>, String> {
    match term {
        TermRef::NamedNode(n) => Ok(n.into()),
        TermRef::BlankNode(b) => Ok(b.into()),
        _ => Err(format!("Invalid subject term {:?}", term)),
    }
}

fn parse_custom_constraint_components(
    context: &ParsingContext,
) -> Result<CustomComponentMaps, String> {
    crate::sparql::parse_custom_constraint_components(context, context.sparql.as_ref())
        .map_err(|e| format!("Error parsing custom constraint components: {}", e))
}

fn register_component_templates(
    context: &mut ParsingContext,
    definitions: &HashMap<NamedNode, CustomConstraintComponentDefinition>,
) -> Result<(), String> {
    let shacl = SHACL::new();
    let rdfs = RDFS::new();

    for (template_iri, definition) in definitions {
        if context.component_templates.contains_key(template_iri) {
            continue;
        }

        let template_term = Term::NamedNode(template_iri.clone());
        let label = literal_for_predicate(context, &template_term, rdfs.label);
        let comment = literal_for_predicate(context, &template_term, rdfs.comment);
        let prefix_declarations = collect_prefix_declarations(context, &template_term, &shacl);

        let mut ignored_predicates = vec![
            shacl.parameter.into_owned(),
            shacl.message.into_owned(),
            shacl.severity.into_owned(),
            shacl.declare.into_owned(),
            shacl.prefixes.into_owned(),
            rdfs.label.into_owned(),
            rdfs.comment.into_owned(),
        ];
        ignored_predicates.push(shacl.validator.into_owned());
        ignored_predicates.push(shacl.node_validator.into_owned());
        ignored_predicates.push(shacl.property_validator.into_owned());

        let extra = collect_component_extras(context, &template_term, &ignored_predicates);

        let template_parameters: Vec<TemplateParameter> = definition
            .parameters
            .iter()
            .map(|param| TemplateParameter {
                subject: param.subject.clone(),
                path: param.path.clone(),
                name: param.name.clone(),
                description: param.description.clone(),
                optional: param.optional,
                default_values: param.default_values.clone(),
                var_name: param.var_name.clone(),
                extra: param.extra.clone(),
            })
            .collect();

        let validators = TemplateValidators {
            validator: definition.validator.clone(),
            node_validator: definition.node_validator.clone(),
            property_validator: definition.property_validator.clone(),
        };

        let template_definition = ComponentTemplateDefinition {
            iri: template_iri.clone(),
            label,
            comment,
            parameters: template_parameters,
            validators,
            messages: definition.messages.clone(),
            severity: definition.severity.clone(),
            prefix_declarations,
            extra,
        };

        context
            .component_templates
            .insert(template_iri.clone(), template_definition);
    }

    Ok(())
}

fn register_shape_templates(context: &mut ParsingContext) -> Result<(), String> {
    let shacl = SHACL::new();
    let rdfs = RDFS::new();
    let rdf = RDF::new();

    let mut seen = HashSet::new();

    for quad in context
        .store
        .quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(shacl.shape_class.into()),
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        let template_term = match quad.subject {
            NamedOrBlankNode::NamedNode(nn) => Term::NamedNode(nn),
            NamedOrBlankNode::BlankNode(bn) => Term::BlankNode(bn),
        };

        let template_iri = match &template_term {
            Term::NamedNode(nn) => nn.clone(),
            _ => continue,
        };

        if !seen.insert(template_iri.clone()) || context.shape_templates.contains_key(&template_iri)
        {
            continue;
        }

        let label = literal_for_predicate(context, &template_term, rdfs.label);
        let comment = literal_for_predicate(context, &template_term, rdfs.comment);
        let prefix_declarations = collect_prefix_declarations(context, &template_term, &shacl);
        let parameters = collect_template_parameters(context, &template_term, &shacl)?;
        let body = literal_or_term(context, &template_term, shacl.shape_prop)
            .unwrap_or_else(|| template_term.clone());
        let mut ignored = vec![
            shacl.parameter.into_owned(),
            shacl.declare.into_owned(),
            shacl.prefixes.into_owned(),
            shacl.shape_prop.into_owned(),
            rdfs.label.into_owned(),
            rdfs.comment.into_owned(),
        ];
        ignored.push(shacl.message.into_owned());
        ignored.push(shacl.severity.into_owned());
        let extra = collect_component_extras(context, &template_term, &ignored);

        let template_definition = ShapeTemplateDefinition {
            iri: template_iri.clone(),
            label,
            comment,
            parameters,
            body,
            prefix_declarations,
            extra,
        };

        context
            .shape_templates
            .insert(template_iri, template_definition);
    }

    Ok(())
}

fn literal_or_term(
    context: &ParsingContext,
    subject: &Term,
    predicate: NamedNodeRef<'_>,
) -> Option<Term> {
    let subject_ref = to_subject_ref(subject.as_ref()).ok()?;
    context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(predicate),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .map(|quad| quad.object)
        .next()
}

fn collect_template_parameters(
    context: &ParsingContext,
    template_term: &Term,
    shacl: &SHACL,
) -> Result<Vec<TemplateParameter>, String> {
    let mut parameters = Vec::new();
    let template_subject = to_subject_ref(template_term.as_ref())?;

    for quad in context
        .store
        .quads_for_pattern(
            Some(template_subject),
            Some(shacl.parameter),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        let param_term = quad.object;
        let param_subject_ref = to_subject_ref(param_term.as_ref())?;

        let path = context
            .store
            .quads_for_pattern(
                Some(param_subject_ref),
                Some(shacl.path),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .find_map(|q| match q.object {
                Term::NamedNode(nn) => Some(nn.to_owned()),
                _ => None,
            })
            .ok_or_else(|| "Template parameter missing sh:path".to_string())?;

        let optional = context
            .store
            .quads_for_pattern(
                Some(param_subject_ref),
                Some(shacl.optional),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .any(|q| matches!(q.object, Term::Literal(ref lit) if lit.value().eq_ignore_ascii_case("true") || lit.value() == "1"));

        let var_name = literal_for_predicate(context, &param_term, shacl.var_name);
        let name = literal_for_predicate(context, &param_term, shacl.name);
        let description = literal_for_predicate(context, &param_term, shacl.description);
        let default_values: Vec<Term> = context
            .store
            .quads_for_pattern(
                Some(param_subject_ref),
                Some(shacl.default_value),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();
        let extras = collect_component_extras(
            context,
            &param_term,
            &[
                shacl.path.into_owned(),
                shacl.optional.into_owned(),
                shacl.var_name.into_owned(),
                shacl.default_value.into_owned(),
                shacl.name.into_owned(),
                shacl.description.into_owned(),
            ],
        );

        parameters.push(TemplateParameter {
            subject: param_term,
            path,
            name,
            description,
            optional,
            default_values,
            var_name,
            extra: extras,
        });
    }

    Ok(parameters)
}

pub(crate) fn ensure_node_shape(
    context: &mut ParsingContext,
    shape_term: Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<ID, String> {
    let id = context.get_or_create_node_id(shape_term.clone());
    if !context.node_shapes.contains_key(&id) {
        parse_node_shape(context, shape_term, unique_lang)?;
    }
    Ok(id)
}

pub(crate) fn resolve_shape_reference(
    context: &mut ParsingContext,
    reference: &Term,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<ID, String> {
    match reference {
        Term::NamedNode(iri) => {
            if let Some(template) = context.shape_templates.get(iri).cloned() {
                instantiate_shape_template(context, &template, owner_shape, unique_lang)
            } else {
                ensure_node_shape(context, reference.clone(), unique_lang)
            }
        }
        Term::BlankNode(_) => ensure_node_shape(context, reference.clone(), unique_lang),
        _ => Err(format!("Unsupported sh:shape reference {:?}", reference)),
    }
}

fn instantiate_shape_template(
    context: &mut ParsingContext,
    template: &ShapeTemplateDefinition,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<ID, String> {
    let bindings = collect_template_parameter_bindings(context, owner_shape, template)?;

    let mut cache_entries: Vec<String> = template
        .parameters
        .iter()
        .map(|param| {
            let mut values: Vec<String> = bindings
                .get(&param.path)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|term| term.to_string())
                .collect();
            values.sort();
            format!("{}={}", param.path.as_str(), values.join(","))
        })
        .collect();
    cache_entries.sort();
    let cache_key = format!("{}|{}", template.iri.as_str(), cache_entries.join("|"));
    if let Some(existing) = context.shape_template_cache.get(&cache_key) {
        return Ok(*existing);
    }

    let new_root = clone_template_body(context, &template.body, &bindings)?;
    let new_id = parse_node_shape(context, new_root, unique_lang)?;
    context.shape_template_cache.insert(cache_key, new_id);
    Ok(new_id)
}

fn collect_template_parameter_bindings(
    context: &ParsingContext,
    owner_shape: &Term,
    template: &ShapeTemplateDefinition,
) -> Result<HashMap<NamedNode, Vec<Term>>, String> {
    let mut bindings = HashMap::new();
    let owner_subject = to_subject_ref(owner_shape.as_ref())?;
    for param in &template.parameters {
        let values: Vec<Term> = context
            .store
            .quads_for_pattern(
                Some(owner_subject),
                Some(param.path.as_ref()),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();

        let resolved = if !values.is_empty() {
            values
        } else if !param.default_values.is_empty() {
            param.default_values.clone()
        } else {
            return Err(format!(
                "Shape template {} requires a value for parameter {}",
                template.iri, param.path
            ));
        };
        bindings.insert(param.path.clone(), resolved);
    }
    Ok(bindings)
}

fn clone_template_body(
    context: &mut ParsingContext,
    root: &Term,
    bindings: &HashMap<NamedNode, Vec<Term>>,
) -> Result<Term, String> {
    let mut map: HashMap<Term, Term> = HashMap::new();
    let mut queue: VecDeque<Term> = VecDeque::new();

    let initial = match root {
        Term::NamedNode(_) | Term::BlankNode(_) => Term::BlankNode(BlankNode::default()),
        _ => {
            return Err(format!(
                "Shape template body {:?} must be a named or blank node",
                root
            ))
        }
    };
    map.insert(root.clone(), initial.clone());
    queue.push_back(root.clone());

    let graph_name = GraphName::NamedNode(context.shape_graph_iri.clone());
    let shacl = SHACL::new();
    let rdf = RDF::new();

    while let Some(current) = queue.pop_front() {
        let subject_ref = to_subject_ref(current.as_ref())?;
        let new_subject_term = map
            .get(&current)
            .cloned()
            .ok_or_else(|| format!("Internal error instantiating template for {:?}", current))?;
        let new_subject = term_to_subject(&new_subject_term)?;

        for quad in context
            .store
            .quads_for_pattern(
                Some(subject_ref),
                None,
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
        {
            let predicate = quad.predicate;
            let mut object = quad.object;

            if let Term::NamedNode(nn) = &object {
                if let Some(values) = bindings.get(nn) {
                    if predicate == shacl.path && !values.is_empty() {
                        object = values[0].clone();
                    }
                }
            }

            let new_object = match object.clone() {
                Term::BlankNode(bn) => {
                    let original = Term::BlankNode(bn);
                    let is_new = !map.contains_key(&original);
                    let entry = map
                        .entry(original.clone())
                        .or_insert_with(|| Term::BlankNode(BlankNode::default()))
                        .clone();
                    if is_new {
                        queue.push_back(original);
                    }
                    entry
                }
                Term::NamedNode(nn) => {
                    if let Some(values) = bindings.get(&nn) {
                        if predicate == shacl.path && !values.is_empty() {
                            values[0].clone()
                        } else {
                            Term::NamedNode(nn)
                        }
                    } else {
                        Term::NamedNode(nn)
                    }
                }
                other => other,
            };

            if predicate.as_str() == rdf.type_.as_str()
                && matches!(new_object, Term::NamedNode(ref nn)
                    if nn.as_str() == shacl.shape_class.as_str())
            {
                continue;
            }

            context
                .store
                .insert(&Quad::new(
                    new_subject.clone(),
                    predicate.clone(),
                    new_object.clone(),
                    graph_name.clone(),
                ))
                .map_err(|e| e.to_string())?;
        }
    }

    Ok(initial)
}

fn term_to_subject(term: &Term) -> Result<NamedOrBlankNode, String> {
    match term {
        Term::NamedNode(nn) => Ok(NamedOrBlankNode::from(nn.clone())),
        Term::BlankNode(bn) => Ok(NamedOrBlankNode::from(bn.clone())),
        _ => Err(format!("Term {:?} cannot act as a subject", term)),
    }
}

fn literal_for_predicate(
    context: &ParsingContext,
    subject: &Term,
    predicate: NamedNodeRef<'_>,
) -> Option<String> {
    let subject_ref = to_subject_ref(subject.as_ref()).ok()?;
    context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(predicate),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .find_map(|quad| match quad.object {
            Term::Literal(lit) => Some(lit.value().to_string()),
            _ => None,
        })
}

fn collect_component_extras(
    context: &ParsingContext,
    subject: &Term,
    ignored_predicates: &[NamedNode],
) -> BTreeMap<NamedNode, Vec<Term>> {
    let mut extras = BTreeMap::new();
    let subject_ref = match to_subject_ref(subject.as_ref()) {
        Ok(subject_ref) => subject_ref,
        Err(_) => return extras,
    };

    let ignored: HashSet<String> = ignored_predicates
        .iter()
        .map(|pred| pred.as_str().to_string())
        .collect();

    let rdf = RDF::new();

    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            None,
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        let predicate_owned = quad.predicate.clone();
        if ignored.contains(predicate_owned.as_str())
            || predicate_owned.as_str() == rdf.type_.as_str()
        {
            continue;
        }
        extras
            .entry(predicate_owned)
            .or_insert_with(Vec::new)
            .push(quad.object);
    }

    extras
}

fn collect_prefix_declarations(
    context: &ParsingContext,
    subject: &Term,
    shacl: &SHACL,
) -> Vec<PrefixDeclaration> {
    let mut declarations = Vec::new();
    let subject_ref = match to_subject_ref(subject.as_ref()) {
        Ok(subject_ref) => subject_ref,
        Err(_) => return declarations,
    };

    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(shacl.declare),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        let decl_subject_term = match quad.object {
            Term::NamedNode(_) | Term::BlankNode(_) => quad.object,
            _ => continue,
        };

        let prefix = literal_for_predicate(context, &decl_subject_term, shacl.prefix);
        let namespace = literal_for_predicate(context, &decl_subject_term, shacl.namespace);

        if let (Some(prefix), Some(namespace)) = (prefix, namespace) {
            declarations.push(PrefixDeclaration { prefix, namespace });
        }
    }

    declarations
}
