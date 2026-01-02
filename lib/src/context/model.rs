#![allow(deprecated)]
use super::ids::IDLookupTable;
use crate::model::{
    components::sparql::CustomConstraintComponentDefinition, components::ComponentDescriptor,
    ComponentTemplateDefinition, Rule, ShapeTemplateDefinition,
};
use crate::optimize::Optimizer;
use crate::parser;
use crate::shape::{NodeShape, PropertyShape};
use crate::skolem::skolem_base;
use crate::sparql::SparqlServices;
use crate::types::{ComponentID, PropShapeID, RuleID, ID};
use log::info;
use ontoenv::api::OntoEnv;
use ontoenv::ontology::OntologyLocation;
use ontoenv::options::{Overwrite, RefreshStrategy};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphNameRef, NamedNode, Term};
use oxigraph::model::{Literal, Subject};
use oxigraph::store::Store;
use shacl_ir::{FeatureToggles, ShapeIR};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Hash, Eq, PartialEq, Clone)]
struct LiteralKey {
    lexical: String,
    language: Option<String>,
}

impl LiteralKey {
    fn from_literal(lit: &Literal) -> Self {
        let lexical = lit.value().to_string();
        let language = lit.language().map(|l| l.to_ascii_lowercase());
        LiteralKey { lexical, language }
    }
}

#[derive(Default, Clone)]
pub struct OriginalValueIndex {
    literals: HashMap<Term, HashMap<NamedNode, HashMap<LiteralKey, VecDeque<Term>>>>,
}

impl OriginalValueIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_path(path: &Path, skolem_base: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let mut index = Self::new();
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase());

        let format = match extension.as_deref() {
            Some("ttl") | Some("turtle") => Some(RdfFormat::Turtle),
            Some("nt") => Some(RdfFormat::NTriples),
            _ => None,
        };

        let format = match format {
            Some(f) => f,
            None => return Ok(index),
        };

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let parser = RdfParser::from_format(format).without_named_graphs();
        for quad in parser.for_reader(reader) {
            let triple = quad?;
            index.record_triple(
                triple.subject,
                triple.predicate.clone(),
                triple.object,
                skolem_base,
            );
        }
        Ok(index)
    }

    fn record_triple(
        &mut self,
        subject: Subject,
        predicate: NamedNode,
        object: Term,
        skolem_base: Option<&str>,
    ) {
        if let Term::Literal(lit) = object {
            let subject_term = Self::canonicalize_subject(subject, skolem_base);
            let object_term = Self::canonicalize_object(Term::Literal(lit.clone()), skolem_base);
            let entry = self
                .literals
                .entry(subject_term)
                .or_default()
                .entry(predicate)
                .or_default()
                .entry(LiteralKey::from_literal(&lit))
                .or_default();
            entry.push_back(object_term);
        }
    }

    fn canonicalize_subject(subject: Subject, skolem_base: Option<&str>) -> Term {
        match subject {
            Subject::NamedNode(nn) => Term::NamedNode(nn),
            Subject::BlankNode(bn) => {
                if let Some(base) = skolem_base {
                    Term::NamedNode(NamedNode::new_unchecked(format!("{}{}", base, bn.as_str())))
                } else {
                    Term::BlankNode(bn)
                }
            }
        }
    }

    fn canonicalize_object(object: Term, skolem_base: Option<&str>) -> Term {
        if let Term::BlankNode(bn) = object {
            if let Some(base) = skolem_base {
                Term::NamedNode(NamedNode::new_unchecked(format!("{}{}", base, bn.as_str())))
            } else {
                Term::BlankNode(bn)
            }
        } else {
            object
        }
    }

    pub fn resolve_literal(
        &self,
        subject: &Term,
        predicate: &NamedNode,
        candidate: &Literal,
    ) -> Option<Term> {
        let key = LiteralKey::from_literal(candidate);
        let candidates = self.literals.get(subject)?.get(predicate)?.get(&key)?;

        if candidates.is_empty() {
            return None;
        }

        let candidate_term = Term::Literal(candidate.clone());
        if candidates.iter().any(|term| term == &candidate_term) {
            Some(candidate_term)
        } else {
            candidates.front().cloned()
        }
    }
}

pub struct ShapesModel {
    pub(crate) nodeshape_id_lookup: RwLock<IDLookupTable<ID>>,
    pub(crate) propshape_id_lookup: RwLock<IDLookupTable<PropShapeID>>,
    #[allow(dead_code)]
    pub(crate) component_id_lookup: RwLock<IDLookupTable<ComponentID>>,
    #[allow(dead_code)]
    pub(crate) rule_id_lookup: RwLock<IDLookupTable<RuleID>>,
    pub(crate) store: Store,
    pub(crate) shape_graph_iri: NamedNode,
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    pub(crate) component_descriptors: HashMap<ComponentID, ComponentDescriptor>,
    #[allow(dead_code)]
    pub(crate) component_templates: HashMap<NamedNode, ComponentTemplateDefinition>,
    #[allow(dead_code)]
    pub(crate) shape_templates: HashMap<NamedNode, ShapeTemplateDefinition>,
    #[allow(dead_code)]
    pub(crate) shape_template_cache: HashMap<String, ID>,
    pub(crate) rules: HashMap<RuleID, Rule>,
    pub(crate) node_shape_rules: HashMap<ID, Vec<RuleID>>,
    pub(crate) prop_shape_rules: HashMap<PropShapeID, Vec<RuleID>>,
    pub(crate) env: OntoEnv,
    pub(crate) sparql: Arc<SparqlServices>,
    #[allow(dead_code)]
    pub(crate) features: FeatureToggles,
    pub(crate) original_values: Option<OriginalValueIndex>,
}

pub(crate) struct CustomComponentCache {
    pub(crate) definitions: HashMap<NamedNode, CustomConstraintComponentDefinition>,
    pub(crate) param_to_component: HashMap<NamedNode, Vec<NamedNode>>,
}

impl ShapesModel {
    #[allow(dead_code)]
    pub fn from_file(shape_graph_path: &str) -> Result<Self, Box<dyn Error>> {
        let mut env = OntoEnv::new_in_memory_online_with_search()?;

        let shape_graph_location = OntologyLocation::from_str(shape_graph_path)?;
        info!("Added shape graph: {}", shape_graph_location);
        let shape_id = env.add(
            shape_graph_location,
            Overwrite::Preserve,
            RefreshStrategy::Force,
        )?;
        let shape_graph_iri = env.get_ontology(&shape_id).unwrap().name().clone();

        let dummy_data_graph_iri = NamedNode::new("urn:dummy:data_graph")?;
        let store = env.io().store().clone();

        let shape_graph_base_iri = skolem_base(&shape_graph_iri);
        info!(
            "Skolemizing shape graph <{}> with base IRI <{}>",
            shape_graph_iri, shape_graph_base_iri
        );

        info!("Optimizing store with shape graph <{}>", shape_graph_iri);
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Error optimizing store: {}",
                e
            )))
        })?;

        let mut ctx = ParsingContext::new(
            store,
            env,
            shape_graph_iri.clone(),
            dummy_data_graph_iri,
            FeatureToggles::default(),
            false,
            None,
        );
        info!(
            "Parsing shapes from graph <{}> into context",
            ctx.shape_graph_iri_ref()
        );
        parser::run_parser(&mut ctx).map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Error parsing shapes: {}",
                e
            )))
        })?;
        info!("Optimizing shape graph");
        let mut optimizer = Optimizer::new(ctx);
        optimizer.optimize()?;
        info!("Finished parsing shapes and optimizing context");
        let final_ctx = optimizer.finish();

        Ok(ShapesModel {
            nodeshape_id_lookup: final_ctx.nodeshape_id_lookup,
            propshape_id_lookup: final_ctx.propshape_id_lookup,
            component_id_lookup: final_ctx.component_id_lookup,
            rule_id_lookup: final_ctx.rule_id_lookup,
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            node_shapes: final_ctx.node_shapes,
            prop_shapes: final_ctx.prop_shapes,
            component_descriptors: final_ctx.component_descriptors,
            component_templates: final_ctx.component_templates,
            shape_templates: final_ctx.shape_templates,
            shape_template_cache: final_ctx.shape_template_cache,
            rules: final_ctx.rules,
            node_shape_rules: final_ctx.node_shape_rules,
            prop_shape_rules: final_ctx.prop_shape_rules,
            env: final_ctx.env,
            sparql: final_ctx.sparql.clone(),
            features: final_ctx.features.clone(),
            original_values: final_ctx.original_values,
        })
    }

    pub(crate) fn from_shape_ir(
        shape_ir: ShapeIR,
        store: Store,
        env: OntoEnv,
        original_values: Option<OriginalValueIndex>,
    ) -> Result<Self, Box<dyn Error>> {
        let ShapeIR {
            shape_graph,
            node_shapes: node_shape_irs,
            property_shapes: prop_shape_irs,
            components,
            component_templates,
            shape_templates,
            shape_template_cache,
            node_shape_terms,
            property_shape_terms,
            shape_quads: _,
            rules,
            node_shape_rules,
            prop_shape_rules,
            features,
            ..
        } = shape_ir;

        let mut nodeshape_lookup = IDLookupTable::<ID>::new();
        for (id, term) in node_shape_terms {
            nodeshape_lookup.insert(term, id);
        }

        let mut propshape_lookup = IDLookupTable::<PropShapeID>::new();
        for (id, term) in property_shape_terms {
            propshape_lookup.insert(term, id);
        }

        let node_shapes = node_shape_irs
            .into_iter()
            .map(|shape| {
                (
                    shape.id,
                    NodeShape::new(
                        shape.id,
                        shape.targets,
                        shape.constraints,
                        Some(shape.severity),
                        shape.deactivated,
                    ),
                )
            })
            .collect();

        let prop_shapes = prop_shape_irs
            .into_iter()
            .map(|shape| {
                (
                    shape.id,
                    PropertyShape::new(
                        shape.id,
                        shape.targets,
                        shape.path,
                        shape.path_term,
                        shape.constraints,
                        Some(shape.severity),
                        shape.deactivated,
                    ),
                )
            })
            .collect();

        Ok(ShapesModel {
            nodeshape_id_lookup: RwLock::new(nodeshape_lookup),
            propshape_id_lookup: RwLock::new(propshape_lookup),
            component_id_lookup: RwLock::new(IDLookupTable::<ComponentID>::new()),
            rule_id_lookup: RwLock::new(IDLookupTable::<RuleID>::new()),
            store,
            shape_graph_iri: shape_graph,
            node_shapes,
            prop_shapes,
            component_descriptors: components,
            component_templates,
            shape_templates,
            shape_template_cache,
            rules,
            node_shape_rules,
            prop_shape_rules,
            env,
            sparql: Arc::new(SparqlServices::new()),
            features,
            original_values,
        })
    }

    pub(crate) fn nodeshape_id_lookup(&self) -> &RwLock<IDLookupTable<ID>> {
        &self.nodeshape_id_lookup
    }

    pub(crate) fn propshape_id_lookup(&self) -> &RwLock<IDLookupTable<PropShapeID>> {
        &self.propshape_id_lookup
    }

    pub(crate) fn get_component_descriptor(
        &self,
        id: &ComponentID,
    ) -> Option<&ComponentDescriptor> {
        self.component_descriptors.get(id)
    }

    pub(crate) fn get_prop_shape_by_id(&self, id: &PropShapeID) -> Option<&PropertyShape> {
        self.prop_shapes.get(id)
    }

    pub(crate) fn get_node_shape_by_id(&self, id: &ID) -> Option<&NodeShape> {
        self.node_shapes.get(id)
    }
}

pub(crate) struct ParsingContext {
    pub(crate) nodeshape_id_lookup: RwLock<IDLookupTable<ID>>,
    pub(crate) propshape_id_lookup: RwLock<IDLookupTable<PropShapeID>>,
    pub(crate) component_id_lookup: RwLock<IDLookupTable<ComponentID>>,
    pub(crate) rule_id_lookup: RwLock<IDLookupTable<RuleID>>,
    pub(crate) store: Store,
    pub(crate) shape_graph_iri: NamedNode,
    pub(crate) data_graph_iri: NamedNode,
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    pub(crate) component_descriptors: HashMap<ComponentID, ComponentDescriptor>,
    pub(crate) component_templates: HashMap<NamedNode, ComponentTemplateDefinition>,
    pub(crate) shape_templates: HashMap<NamedNode, ShapeTemplateDefinition>,
    pub(crate) shape_template_cache: HashMap<String, ID>,
    pub(crate) rules: HashMap<RuleID, Rule>,
    pub(crate) node_shape_rules: HashMap<ID, Vec<RuleID>>,
    pub(crate) prop_shape_rules: HashMap<PropShapeID, Vec<RuleID>>,
    pub(crate) env: OntoEnv,
    pub(crate) sparql: Arc<SparqlServices>,
    #[allow(dead_code)]
    pub(crate) features: FeatureToggles,
    pub(crate) strict_custom_constraints: bool,
    pub(crate) original_values: Option<OriginalValueIndex>,
    pub(crate) custom_component_cache: Option<Arc<CustomComponentCache>>,
}

impl ParsingContext {
    pub(crate) fn shape_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.shape_graph_iri.as_ref())
    }

    pub(crate) fn new(
        store: Store,
        env: OntoEnv,
        shape_graph_iri: NamedNode,
        data_graph_iri: NamedNode,
        features: FeatureToggles,
        strict_custom_constraints: bool,
        original_values: Option<OriginalValueIndex>,
    ) -> Self {
        Self {
            nodeshape_id_lookup: RwLock::new(IDLookupTable::<ID>::new()),
            propshape_id_lookup: RwLock::new(IDLookupTable::<PropShapeID>::new()),
            component_id_lookup: RwLock::new(IDLookupTable::<ComponentID>::new()),
            rule_id_lookup: RwLock::new(IDLookupTable::<RuleID>::new()),
            store,
            shape_graph_iri,
            data_graph_iri,
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            component_descriptors: HashMap::new(),
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            env,
            sparql: Arc::new(SparqlServices::new()),
            features,
            strict_custom_constraints,
            original_values,
            custom_component_cache: None,
        }
    }

    pub(crate) fn get_or_create_node_id(&self, term: Term) -> ID {
        self.nodeshape_id_lookup
            .write()
            .unwrap()
            .get_or_create_id(term)
    }

    pub(crate) fn get_or_create_prop_id(&self, term: Term) -> PropShapeID {
        self.propshape_id_lookup
            .write()
            .unwrap()
            .get_or_create_id(term)
    }

    pub(crate) fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup
            .write()
            .unwrap()
            .get_or_create_id(term)
    }

    pub(crate) fn get_or_create_rule_id(&self, term: Term) -> RuleID {
        self.rule_id_lookup.write().unwrap().get_or_create_id(term)
    }
}
