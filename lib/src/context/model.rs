use super::ids::IDLookupTable;
use crate::model::components::ComponentDescriptor;
use crate::optimize::Optimizer;
use crate::parser;
use crate::shape::{NodeShape, PropertyShape};
use crate::types::{ComponentID, PropShapeID, ID};
use log::info;
use ontoenv::api::OntoEnv;
use ontoenv::ontology::OntologyLocation;
use ontoenv::options::{Overwrite, RefreshStrategy};
use oxigraph::model::{GraphNameRef, NamedNode, Term};
use oxigraph::store::Store;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;

pub struct ShapesModel {
    pub(crate) nodeshape_id_lookup: RefCell<IDLookupTable<ID>>,
    pub(crate) propshape_id_lookup: RefCell<IDLookupTable<PropShapeID>>,
    #[allow(dead_code)]
    pub(crate) component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    pub(crate) store: Store,
    pub(crate) shape_graph_iri: NamedNode,
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    pub(crate) component_descriptors: HashMap<ComponentID, ComponentDescriptor>,
    pub(crate) env: OntoEnv,
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

        let shape_graph_base_iri = format!(
            "{}/.well-known/skolem/",
            shape_graph_iri.as_str().trim_end_matches('/')
        );
        info!(
            "Skolemizing shape graph <{}> with base IRI <{}>",
            shape_graph_iri, shape_graph_base_iri
        );

        info!("Optimizing store with shape graph <{}>", shape_graph_iri);
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error optimizing store: {}", e),
            ))
        })?;

        let mut ctx =
            ParsingContext::new(store, env, shape_graph_iri.clone(), dummy_data_graph_iri);
        info!(
            "Parsing shapes from graph <{}> into context",
            ctx.shape_graph_iri_ref()
        );
        parser::run_parser(&mut ctx).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error parsing shapes: {}", e),
            ))
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
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            node_shapes: final_ctx.node_shapes,
            prop_shapes: final_ctx.prop_shapes,
            component_descriptors: final_ctx.component_descriptors,
            env: final_ctx.env,
        })
    }

    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    #[allow(dead_code)]
    pub(crate) fn env(&self) -> &OntoEnv {
        &self.env
    }

    pub(crate) fn shape_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.shape_graph_iri.as_ref())
    }

    pub(crate) fn nodeshape_id_lookup(&self) -> &RefCell<IDLookupTable<ID>> {
        &self.nodeshape_id_lookup
    }

    pub(crate) fn propshape_id_lookup(&self) -> &RefCell<IDLookupTable<PropShapeID>> {
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
    pub(crate) nodeshape_id_lookup: RefCell<IDLookupTable<ID>>,
    pub(crate) propshape_id_lookup: RefCell<IDLookupTable<PropShapeID>>,
    pub(crate) component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    pub(crate) store: Store,
    pub(crate) shape_graph_iri: NamedNode,
    pub(crate) data_graph_iri: NamedNode,
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    pub(crate) component_descriptors: HashMap<ComponentID, ComponentDescriptor>,
    pub(crate) env: OntoEnv,
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
    ) -> Self {
        Self {
            nodeshape_id_lookup: RefCell::new(IDLookupTable::<ID>::new()),
            propshape_id_lookup: RefCell::new(IDLookupTable::<PropShapeID>::new()),
            component_id_lookup: RefCell::new(IDLookupTable::<ComponentID>::new()),
            store,
            shape_graph_iri,
            data_graph_iri,
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            component_descriptors: HashMap::new(),
            env,
        }
    }

    pub(crate) fn get_or_create_node_id(&self, term: Term) -> ID {
        self.nodeshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    pub(crate) fn get_or_create_prop_id(&self, term: Term) -> PropShapeID {
        self.propshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    pub(crate) fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup.borrow_mut().get_or_create_id(term)
    }
}
