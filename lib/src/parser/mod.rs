mod component_registry;
mod components;
mod rules;

use crate::context::ParsingContext;
use crate::named_nodes::{OWL, RDF, RDFS, SHACL};
use crate::shape::{NodeShape, PropertyShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, Severity, SeverityExt, ID};
use components::parse_components;
use log::{debug, info, warn};
use ontoenv::ontology::OntologyLocation;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{
    vocab::xsd, GraphName, GraphNameRef, NamedOrBlankNodeRef as SubjectRef, QuadRef, Term, TermRef,
};
use rules::parse_rules_for_shape;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use url::Url;

trait ToSubjectRef {
    fn to_subject_ref(&self) -> SubjectRef<'_>;
    #[allow(dead_code)]
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String>;
}

impl ToSubjectRef for TermRef<'_> {
    fn to_subject_ref(&self) -> SubjectRef<'_> {
        match self {
            TermRef::NamedNode(n) => (*n).into(),
            TermRef::BlankNode(b) => (*b).into(),
            _ => panic!("Invalid subject term {:?}", self),
        }
    }

    #[allow(dead_code)]
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String> {
        match self {
            TermRef::NamedNode(n) => Ok((*n).into()),
            TermRef::BlankNode(b) => Ok((*b).into()),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

fn load_unique_lang_lexicals(context: &ParsingContext) -> HashMap<Term, String> {
    let mut map = HashMap::new();
    let shacl = SHACL::new();

    let mut candidate_paths: Vec<std::path::PathBuf> = Vec::new();

    if let Ok(url) = Url::parse(context.shape_graph_iri.as_str()) {
        if url.scheme() == "file" {
            if let Ok(path) = url.to_file_path() {
                candidate_paths.push(path);
            }
        }
    }

    if let Some(ontology) = context
        .env
        .ontologies()
        .values()
        .find(|ontology| ontology.name() == context.shape_graph_iri)
    {
        if let Some(OntologyLocation::File(path)) = ontology.location() {
            let mut candidate = path.clone();
            if !candidate.is_absolute() {
                if let Ok(cwd) = std::env::current_dir() {
                    candidate = cwd.join(candidate);
                }
            }
            candidate_paths.push(candidate);
        }
    }

    let mut seen = HashSet::new();
    for path in candidate_paths {
        let canonical_path = std::fs::canonicalize(&path).unwrap_or(path.clone());
        if !seen.insert(canonical_path.clone()) {
            continue;
        }
        if let Ok(file) = File::open(&canonical_path) {
            let reader = BufReader::new(file);
            let parser = RdfParser::from_format(RdfFormat::Turtle).without_named_graphs();
            for quad in parser.for_reader(reader).flatten() {
                if quad.predicate == shacl.unique_lang {
                    if let Term::Literal(lit) = quad.object.clone() {
                        map.insert(quad.subject.clone().into(), lit.value().to_string());
                    }
                }
            }
        }
    }

    map
}

/// Runs the parser to discover all shapes and components from the shapes graph
/// and populates them into the `ParsingContext`.
pub(crate) fn run_parser(context: &mut ParsingContext) -> Result<(), String> {
    // parses the shape graph to get all of the shapes and components defined within
    let unique_lang_lexicals = load_unique_lang_lexicals(context);
    let rdf = RDF::new();
    let sh = SHACL::new();
    let shape_graph_name_ref = context.shape_graph_iri_ref();
    let property_shape_count = context
        .store
        .quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(sh.property_shape.into()),
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .count();
    let property_shape_total = context
        .store
        .quads_for_pattern(None, Some(rdf.type_), Some(sh.property_shape.into()), None)
        .filter_map(Result::ok)
        .count();
    info!(
        "run_parser shape graph {} has {} property shape type triples (total across graphs {})",
        context.shape_graph_iri, property_shape_count, property_shape_total
    );
    let shapes = get_node_shapes(context);
    let skip_invalid = context.features.skip_invalid_rules;

    for shape in shapes {
        let shape_term = shape.clone();
        if let Err(err) = parse_node_shape(context, shape_term.clone(), &unique_lang_lexicals) {
            if skip_invalid {
                warn!(
                    "Skipping node shape {} due to parse error: {}",
                    shape_term, err
                );
                continue;
            } else {
                return Err(err);
            }
        }
    }

    let pshapes = get_property_shapes(context);
    for pshape in pshapes {
        let pshape_term = pshape.clone();
        if let Err(err) = parse_property_shape(context, pshape_term.clone(), &unique_lang_lexicals)
        {
            if skip_invalid {
                warn!(
                    "Skipping property shape {} due to parse error: {}",
                    pshape_term, err
                );
                continue;
            } else {
                return Err(err);
            }
        }
    }
    info!(
        "run_parser parsed node_shapes={} prop_shapes={}",
        context.node_shapes.len(),
        context.prop_shapes.len()
    );
    Ok(())
}

fn get_property_shapes(context: &ParsingContext) -> Vec<Term> {
    let rdf = RDF::new();
    let sh = SHACL::new();
    let mut prop_shapes = HashSet::new();
    let shape_graph_name_ref = GraphNameRef::NamedNode(context.shape_graph_iri.as_ref());

    // - <pshape> a sh:PropertyShape
    for quad in context
        .store
        .quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(sh.property_shape.into()),
            Some(shape_graph_name_ref),
        )
        .flatten()
    {
        prop_shapes.insert(quad.subject.into()); // quad.subject is Subject, .into() converts to Term
    }

    // - ? sh:property <pshape>
    for quad in context
        .store
        .quads_for_pattern(None, Some(sh.property), None, Some(shape_graph_name_ref))
        .flatten()
    {
        prop_shapes.insert(quad.object); // quad.object is Term
    }

    prop_shapes.into_iter().collect()
}

fn get_node_shapes(context: &ParsingContext) -> Vec<Term> {
    // here are all the ways to get a node shape:
    // - <shape> rdf:type sh:NodeShape
    // - ? sh:node <shape>
    // - ? sh:qualifiedValueShape <shape>
    // - ? sh:not <shape>
    // - ? sh:or (list of <shape>)
    // - ? sh:and (list of <shape>)
    // - ? sh:xone (list of <shape>)
    let rdf = RDF::new();
    let shacl = SHACL::new();
    let shape_graph_name_ref = GraphNameRef::NamedNode(context.shape_graph_iri.as_ref());

    // parse these out of the shape graph and return a vector of IDs
    let mut node_shapes = HashSet::new();

    // Track property shapes so we don't misclassify them as node shapes when they
    // declare explicit targets. Property shapes may be typed via rdf:type or
    // discovered as objects of sh:property.
    let property_shape_terms: HashSet<Term> = get_property_shapes(context).into_iter().collect();

    // sh:path is only valid on property shapes; cache subjects using it so we can
    // skip them when walking explicit target declarations below.
    let shapes_with_path: HashSet<Term> = context
        .store
        .quads_for_pattern(None, Some(shacl.path), None, Some(shape_graph_name_ref))
        .filter_map(Result::ok)
        .map(|quad| quad.subject.into())
        .collect();

    // <shape> rdf:type sh:NodeShape
    for quad in context
        .store
        .quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(shacl.node_shape.into()),
            Some(shape_graph_name_ref),
        )
        .flatten()
    {
        node_shapes.insert(quad.subject.into());
    }

    // ? sh:node <shape>
    for quad in context
        .store
        .quads_for_pattern(None, Some(shacl.node), None, Some(shape_graph_name_ref))
        .flatten()
    {
        node_shapes.insert(quad.object);
    }

    // ? sh:qualifiedValueShape <shape>
    for quad in context
        .store
        .quads_for_pattern(
            None,
            Some(shacl.qualified_value_shape),
            None,
            Some(shape_graph_name_ref),
        )
        .flatten()
    {
        node_shapes.insert(quad.object);
    }

    // ? sh:not <shape>
    for quad in context
        .store
        .quads_for_pattern(None, Some(shacl.not), None, Some(shape_graph_name_ref))
        .flatten()
    {
        node_shapes.insert(quad.object);
    }

    // Shapes with explicit targets (sh:targetNode, sh:targetClass, sh:targetSubjectsOf, sh:targetObjectsOf)
    for target_predicate in [
        shacl.target,
        shacl.target_node,
        shacl.target_class,
        shacl.target_subjects_of,
        shacl.target_objects_of,
    ] {
        for quad in context
            .store
            .quads_for_pattern(
                None,
                Some(target_predicate),
                None,
                Some(shape_graph_name_ref),
            )
            .flatten()
        {
            let subject_term: Term = quad.subject.into();
            if property_shape_terms.contains(&subject_term)
                || shapes_with_path.contains(&subject_term)
            {
                continue;
            }
            node_shapes.insert(subject_term);
        }
    }

    // Helper to process lists for logical constraints
    let mut process_list_constraint = |predicate_ref| {
        for quad in context
            .store
            .quads_for_pattern(None, Some(predicate_ref), None, Some(shape_graph_name_ref))
            .flatten()
        {
            let list_head_term = quad.object; // This is Term
                                              // parse_rdf_list will also use shape_graph_name_ref internally
            for item_term in parse_rdf_list(context, list_head_term) {
                node_shapes.insert(item_term);
            }
        }
    };

    // ? sh:or (list of <shape>)
    process_list_constraint(shacl.or_);

    // ? sh:and (list of <shape>)
    process_list_constraint(shacl.and_);

    // ? sh:xone (list of <shape>)
    process_list_constraint(shacl.xone);

    // Shapes that declare sh:rule but are otherwise implicit.
    for quad in context
        .store
        .quads_for_pattern(None, Some(shacl.rule), None, Some(shape_graph_name_ref))
        .flatten()
    {
        node_shapes.insert(quad.subject.into());
    }

    node_shapes.into_iter().collect()
}

pub(crate) fn parse_node_shape(
    context: &mut ParsingContext,
    shape_term: Term,
    unique_lang_lexicals: &HashMap<Term, String>,
) -> Result<ID, String> {
    // Parses a shape from the shape graph and returns its ID.
    // Adds the shape to the node_shapes map.
    let id = context.get_or_create_node_id(shape_term.clone());
    let sh = SHACL::new();
    let shape_ref = shape_term.as_ref();

    let subject: SubjectRef = shape_ref.to_subject_ref();
    let shape_graph_name = GraphName::NamedNode(context.shape_graph_iri.clone());

    // get the targets
    let mut targets: Vec<crate::types::Target> = context
        .store
        .quads_for_pattern(Some(subject), None, None, Some(shape_graph_name.as_ref()))
        .filter_map(Result::ok)
        .filter_map(|quad| {
            crate::types::target_from_predicate_object(
                quad.predicate.as_ref(),
                quad.object.as_ref(),
            )
        })
        .collect();

    // check for implicit classes. If 'shape' is also a class (rdfs:Class or owl:Class)
    // then add a Target::Class for it.
    // use store.contains(quad) to check
    let rdf = RDF::new();
    let rdfs = RDFS::new();
    let owl = OWL::new();
    let is_rdfs_class = context
        .store
        .contains(QuadRef::new(
            subject,
            rdf.type_,
            rdfs.class,
            shape_graph_name.as_ref(),
        ))
        .map_err(|e| e.to_string())?;
    let is_owl_class = context
        .store
        .contains(QuadRef::new(
            subject,
            rdf.type_,
            owl.class,
            shape_graph_name.as_ref(),
        ))
        .map_err(|e| e.to_string())?;
    if is_rdfs_class || is_owl_class {
        targets.push(crate::types::Target::Class(subject.into()));
    }

    // get constraint components
    // parse_components will internally use context.store() and context.shape_graph_iri_ref()
    let constraints = parse_components(&shape_term, context, unique_lang_lexicals, false)?;
    let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
    for (component_id, descriptor) in constraints {
        context
            .component_descriptors
            .insert(component_id, descriptor);
    }

    let _property_shapes: Vec<PropShapeID> = context // This seems to be about sh:property linking to PropertyShapes.
        .store // It was collected but not used in NodeShape::new.
        .quads_for_pattern(
            Some(subject),
            Some(sh.property),
            None,
            Some(shape_graph_name.as_ref()),
        )
        .filter_map(Result::ok)
        .filter_map(|quad| {
            context
                .propshape_id_lookup
                .read()
                .unwrap()
                .get(&quad.object)
        })
        .collect();
    // TODO: property_shapes are collected but not used in NodeShape::new. This might be an existing oversight or for future use.

    let severity_term_opt = context
        .store
        .quads_for_pattern(
            Some(subject),
            Some(sh.severity),
            None,
            Some(shape_graph_name.as_ref()),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next();

    let severity = severity_term_opt.as_ref().and_then(Severity::from_term);

    let deactivated = shape_is_deactivated(context, subject, shape_graph_name.as_ref());

    let node_shape = NodeShape::new(id, targets, component_ids, severity, deactivated);
    let rule_ids = parse_rules_for_shape(context, &shape_term, unique_lang_lexicals)?;
    if !rule_ids.is_empty() {
        context.node_shape_rules.insert(id, rule_ids);
    }
    context.node_shapes.insert(id, node_shape);
    Ok(id)
}

fn parse_property_shape(
    context: &mut ParsingContext,
    shape_term: Term,
    unique_lang_lexicals: &HashMap<Term, String>,
) -> Result<PropShapeID, String> {
    debug!("parse_property_shape: {}", shape_term);
    let id = context.get_or_create_prop_id(shape_term.clone());
    let shacl = SHACL::new();
    let shape_ref = shape_term.as_ref();
    let subject: SubjectRef = shape_ref.to_subject_ref();
    let ps_shape_graph_name = GraphName::NamedNode(context.shape_graph_iri.clone());

    let path_object_term: Term = context
        .store
        .quads_for_pattern(
            Some(subject),
            Some(shacl.path),
            None,
            Some(ps_shape_graph_name.as_ref()),
        )
        .filter_map(Result::ok)
        .map(|quad| quad.object)
        .next()
        .ok_or_else(|| format!("Property shape {:?} must have a sh:path", shape_term))?;

    let path = parse_shacl_path_recursive(context, path_object_term.as_ref())?;

    // get the targets
    let targets: Vec<crate::types::Target> = context
        .store
        .quads_for_pattern(
            Some(subject),
            None,
            None,
            Some(ps_shape_graph_name.as_ref()),
        )
        .filter_map(Result::ok)
        .filter_map(|quad| {
            crate::types::target_from_predicate_object(
                quad.predicate.as_ref(),
                quad.object.as_ref(),
            )
        })
        .collect();

    // get constraint components
    // parse_components will internally use context.store() and context.shape_graph_iri_ref()
    let constraints = parse_components(&shape_term, context, unique_lang_lexicals, true)?;
    let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
    for (component_id, descriptor) in constraints {
        context
            .component_descriptors
            .insert(component_id, descriptor);
    }

    let severity_term_opt = context
        .store
        .quads_for_pattern(
            Some(subject),
            Some(shacl.severity),
            None,
            Some(ps_shape_graph_name.as_ref()),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next();

    let severity = severity_term_opt.as_ref().and_then(Severity::from_term);

    let deactivated = shape_is_deactivated(context, subject, ps_shape_graph_name.as_ref());

    let prop_shape = PropertyShape::new(
        id,
        targets,
        path,
        path_object_term.clone(),
        component_ids,
        severity,
        deactivated,
    );
    let rule_ids = parse_rules_for_shape(context, &shape_term, unique_lang_lexicals)?;
    if !rule_ids.is_empty() {
        context.prop_shape_rules.insert(id, rule_ids);
    }
    context.prop_shapes.insert(id, prop_shape);
    Ok(id)
}

fn shape_is_deactivated(
    context: &ParsingContext,
    subject: SubjectRef,
    graph_name: GraphNameRef<'_>,
) -> bool {
    let sh = SHACL::new();
    context
        .store
        .quads_for_pattern(Some(subject), Some(sh.deactivated), None, Some(graph_name))
        .filter_map(Result::ok)
        .any(|quad| term_is_true(&quad.object))
}

pub(super) fn term_is_true(term: &Term) -> bool {
    match term {
        Term::Literal(lit) => {
            let value = lit.value();
            if lit.datatype() == xsd::BOOLEAN {
                value.eq_ignore_ascii_case("true") || value == "1"
            } else if lit.datatype() == xsd::STRING && lit.language().is_none() {
                value.eq_ignore_ascii_case("true")
            } else {
                false
            }
        }
        _ => false,
    }
}

// Helper function to recursively parse SHACL paths
pub(super) fn parse_shacl_path_recursive(
    context: &ParsingContext,
    path_term_ref: TermRef,
) -> Result<PShapePath, String> {
    let shacl = SHACL::new();
    let _rdf = RDF::new();
    let shape_graph_name_ref = context.shape_graph_iri_ref();

    // Check if this term directly encodes an RDF list for a sequence path.
    let seq_paths_terms = parse_rdf_list(context, path_term_ref.into_owned());
    if !seq_paths_terms.is_empty() {
        let seq_paths: Result<Vec<PShapePath>, String> = seq_paths_terms
            .iter()
            .map(|term| parse_shacl_path_recursive(context, term.as_ref()))
            .collect();
        return Ok(PShapePath::Sequence(seq_paths?));
    }

    // Check for sh:inversePath
    if let Some(inverse_path_obj) = context
        .store
        .quads_for_pattern(
            Some(path_term_ref.to_subject_ref()),
            Some(shacl.inverse_path),
            None,
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next()
    {
        let inner_path = parse_shacl_path_recursive(context, inverse_path_obj.as_ref())?;
        return Ok(PShapePath::Inverse(Box::new(inner_path)));
    }

    // Check for sh:alternativePath (RDF list)
    if let Some(alt_list_head) = context
        .store
        .quads_for_pattern(
            Some(path_term_ref.to_subject_ref()),
            Some(shacl.alternative_path),
            None,
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next()
    {
        let alt_paths_terms = parse_rdf_list(context, alt_list_head);
        let alt_paths: Result<Vec<PShapePath>, String> = alt_paths_terms
            .iter()
            .map(|term| parse_shacl_path_recursive(context, term.as_ref()))
            .collect();
        return Ok(PShapePath::Alternative(alt_paths?));
    }

    // Check for sh:zeroOrMorePath
    if let Some(zom_path_obj) = context
        .store
        .quads_for_pattern(
            Some(path_term_ref.to_subject_ref()),
            Some(shacl.zero_or_more_path),
            None,
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next()
    {
        let inner_path = parse_shacl_path_recursive(context, zom_path_obj.as_ref())?;
        return Ok(PShapePath::ZeroOrMore(Box::new(inner_path)));
    }

    // Check for sh:oneOrMorePath
    if let Some(oom_path_obj) = context
        .store
        .quads_for_pattern(
            Some(path_term_ref.to_subject_ref()),
            Some(shacl.one_or_more_path),
            None,
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next()
    {
        let inner_path = parse_shacl_path_recursive(context, oom_path_obj.as_ref())?;
        return Ok(PShapePath::OneOrMore(Box::new(inner_path)));
    }

    // Check for sh:zeroOrOnePath
    if let Some(zoo_path_obj) = context
        .store
        .quads_for_pattern(
            Some(path_term_ref.to_subject_ref()),
            Some(shacl.zero_or_one_path),
            None,
            Some(shape_graph_name_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next()
    {
        let inner_path = parse_shacl_path_recursive(context, zoo_path_obj.as_ref())?;
        return Ok(PShapePath::ZeroOrOne(Box::new(inner_path)));
    }

    // If it's not a complex path node, it must be a simple path (an IRI)
    match path_term_ref {
        TermRef::NamedNode(_) => Ok(PShapePath::Simple(path_term_ref.into_owned())),
        _ => Err(format!(
            "Expected an IRI for a simple path or a blank node for a complex path, found: {:?}",
            path_term_ref
        )),
    }
}

/// Parses an RDF list starting from list_head_term (owned Term) and returns a Vec of owned Terms.
pub(crate) fn parse_rdf_list(context: &ParsingContext, list_head_term: Term) -> Vec<Term> {
    let mut items: Vec<Term> = Vec::new();
    let rdf = RDF::new();
    let mut current_term = list_head_term;
    let nil_term: Term = rdf.nil.into_owned().into(); // Convert NamedNodeRef to Term
    let shape_graph_name_ref = GraphNameRef::NamedNode(context.shape_graph_iri.as_ref());

    while current_term != nil_term {
        let subject_ref = match current_term.as_ref() {
            TermRef::NamedNode(nn) => SubjectRef::NamedNode(nn),
            TermRef::BlankNode(bn) => SubjectRef::BlankNode(bn),
            _ => return items, // Or handle error: list node not an IRI/BlankNode
        };

        let first_val_opt: Option<Term> = context
            .store
            .quads_for_pattern(
                Some(subject_ref),
                Some(rdf.first),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next();

        if let Some(val) = first_val_opt {
            items.push(val);
        } else {
            break; // Malformed list (no rdf:first)
        }

        let rest_node_opt: Option<Term> = context
            .store
            .quads_for_pattern(
                Some(subject_ref),
                Some(rdf.rest),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next();

        if let Some(rest_term) = rest_node_opt {
            current_term = rest_term;
        } else {
            break; // Malformed list (no rdf:rest)
        }
    }
    items
}
