use super::graphviz::format_term_for_label;
use super::model::ShapesModel;
use crate::backend::{Binding, GraphBackend, OxigraphBackend};
use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::model::components::ComponentDescriptor;
use crate::runtime::engine::build_custom_constraint_component;
use crate::runtime::{build_component_from_descriptor, Component, CustomConstraintComponent};
use crate::shacl_ir::ShapeIR;
use crate::skolem::skolem_base;
use crate::sparql::{SparqlExecutor, SparqlServices};
use crate::trace::{MemoryTraceSink, TraceEvent, TraceSink};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, TraceItem, ID};
use fixedbitset::FixedBitSet;
use oxigraph::model::{
    vocab::rdf, GraphNameRef, NamedNode, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad,
    Term, TermRef,
};
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ComponentGraphCallKey {
    component_id: ComponentID,
    source_shape: SourceShape,
}

#[derive(Debug, Clone, Copy, Default)]
struct GraphCallStats {
    quads_for_pattern_calls: u64,
    execute_prepared_calls: u64,
    component_invocations: u64,
    runtime_nanos_total: u128,
    runtime_nanos_sum_squares: f64,
    runtime_nanos_min: u64,
    runtime_nanos_max: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct TimingStats {
    invocations: u64,
    runtime_nanos_total: u128,
    runtime_nanos_sum_squares: f64,
    runtime_nanos_min: u64,
    runtime_nanos_max: u64,
}

pub(crate) struct ActiveComponentScope;

#[derive(Debug, Clone)]
pub(crate) struct ComponentGraphCallStatRecord {
    pub(crate) component_id: ComponentID,
    pub(crate) source_shape: SourceShape,
    pub(crate) quads_for_pattern_calls: u64,
    pub(crate) execute_prepared_calls: u64,
    pub(crate) component_invocations: u64,
    pub(crate) runtime_nanos_total: u128,
    pub(crate) runtime_nanos_sum_squares: f64,
    pub(crate) runtime_nanos_min: u64,
    pub(crate) runtime_nanos_max: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ShapeTimingPhase {
    NodeTargetSelection,
    NodeValueSelection,
    PropertyTargetSelection,
    PropertyValueSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShapeTimingKey {
    source_shape: SourceShape,
    phase: ShapeTimingPhase,
}

#[derive(Debug, Clone)]
pub(crate) struct ShapeTimingStatRecord {
    pub(crate) source_shape: SourceShape,
    pub(crate) phase: ShapeTimingPhase,
    pub(crate) invocations: u64,
    pub(crate) runtime_nanos_total: u128,
    pub(crate) runtime_nanos_sum_squares: f64,
    pub(crate) runtime_nanos_min: u64,
    pub(crate) runtime_nanos_max: u64,
}

#[derive(Debug, Clone, Copy)]
enum GraphCallKind {
    QuadsForPattern,
    ExecutePrepared,
}

thread_local! {
    static ACTIVE_COMPONENT_STACK: RefCell<Vec<ComponentGraphCallKey>> = const { RefCell::new(Vec::new()) };
}

#[derive(Debug)]
struct ClassConstraintIndex {
    term_to_id: HashMap<Term, usize>,
    descendants_by_super: Vec<FixedBitSet>,
    subject_type_bits: HashMap<Term, FixedBitSet>,
}

impl ClassConstraintIndex {
    fn class_bitset(&self, class: &Term) -> Option<&FixedBitSet> {
        let class_id = self.term_to_id.get(class).copied()?;
        self.descendants_by_super.get(class_id)
    }

    /// Return all subjects that are instances of `class` (transitively via rdfs:subClassOf).
    fn instances_of_class(&self, class: &Term) -> Vec<Term> {
        let Some(descendants) = self.class_bitset(class) else {
            return Vec::new();
        };
        self.subject_type_bits
            .iter()
            .filter(|(_, type_bits)| type_bits.intersection(descendants).next().is_some())
            .map(|(subject, _)| subject.clone())
            .collect()
    }
}

/// Runtime context shared across validators during a validation run.
///
/// Owns caches (targets, advanced targets), shape/model data, and shared services
/// (SPARQL executor, trace sinks) to avoid recomputation and to centralize state.
pub struct ValidationContext {
    pub(crate) model: Arc<ShapesModel>,
    pub(crate) data_graph_iri: NamedNode,
    data_graph_skolem_base: String,
    shape_graph_skolem_base: String,
    pub(crate) warnings_are_errors: bool,
    pub(crate) skip_sparql_blank_targets: bool,
    pub(crate) execution_traces: Mutex<Vec<Vec<TraceItem>>>,
    pub(crate) components: Arc<HashMap<ComponentID, Component>>,
    pub(crate) advanced_target_cache: RwLock<HashMap<Term, Vec<Term>>>,
    pub(crate) node_target_cache: RwLock<HashMap<ID, Vec<Term>>>,
    pub(crate) prop_target_cache: RwLock<HashMap<PropShapeID, Vec<Term>>>,
    pub(crate) backend: Arc<OxigraphBackend>,
    pub(crate) trace_sink: Arc<dyn TraceSink>,
    pub(crate) trace_events: Arc<Mutex<Vec<TraceEvent>>>,
    pub(crate) shape_ir: Arc<ShapeIR>,
    graph_call_stats: Mutex<HashMap<ComponentGraphCallKey, GraphCallStats>>,
    shape_timing_stats: Mutex<HashMap<ShapeTimingKey, TimingStats>>,
    class_constraint_index: RwLock<Option<Arc<ClassConstraintIndex>>>,
    class_constraint_memo: RwLock<HashMap<(Term, Term), bool>>,
}

impl ValidationContext {
    pub(crate) fn new(
        model: Arc<ShapesModel>,
        data_graph_iri: NamedNode,
        warnings_are_errors: bool,
        skip_sparql_blank_targets: bool,
        shape_ir: Arc<ShapeIR>,
    ) -> Self {
        let data_graph_skolem_base = skolem_base(&data_graph_iri);
        let shape_graph_skolem_base = skolem_base(&model.shape_graph_iri);
        let backend = Arc::new(OxigraphBackend::new(
            model.store.clone(),
            data_graph_iri.clone(),
            model.shape_graph_iri.clone(),
            Arc::clone(&model.sparql),
        ));
        let trace_events = Arc::new(Mutex::new(Vec::new()));
        let memory_sink = MemoryTraceSink::new(Arc::clone(&trace_events));
        let mut custom_cache: HashMap<String, CustomConstraintComponent> = HashMap::new();
        let components: HashMap<ComponentID, Component> = model
            .component_descriptors
            .iter()
            .map(|(id, descriptor)| {
                let component = match descriptor {
                    ComponentDescriptor::Custom {
                        definition,
                        parameter_values,
                    } => {
                        let cache_key = custom_component_cache_key(definition, parameter_values);
                        let cached = custom_cache.entry(cache_key).or_insert_with(|| {
                            build_custom_constraint_component(definition, parameter_values)
                        });
                        Component::CustomConstraint(cached.clone())
                    }
                    _ => build_component_from_descriptor(descriptor),
                };
                (*id, component)
            })
            .collect();

        Self {
            model,
            data_graph_iri,
            data_graph_skolem_base,
            shape_graph_skolem_base,
            warnings_are_errors,
            skip_sparql_blank_targets,
            execution_traces: Mutex::new(Vec::new()),
            components: Arc::new(components),
            advanced_target_cache: RwLock::new(HashMap::new()),
            node_target_cache: RwLock::new(HashMap::new()),
            prop_target_cache: RwLock::new(HashMap::new()),
            backend,
            trace_sink: Arc::new(memory_sink),
            trace_events,
            shape_ir,
            graph_call_stats: Mutex::new(HashMap::new()),
            shape_timing_stats: Mutex::new(HashMap::new()),
            class_constraint_index: RwLock::new(None),
            class_constraint_memo: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn data_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.data_graph_iri.as_ref())
    }

    pub(crate) fn shape_graph_iri_ref(&self) -> GraphNameRef<'_> {
        self.backend.shapes_graph()
    }

    pub(crate) fn sparql_services(&self) -> &SparqlServices {
        &self.model.sparql
    }

    pub(crate) fn warnings_are_errors(&self) -> bool {
        self.warnings_are_errors
    }

    pub(crate) fn skip_sparql_blank_targets(&self) -> bool {
        self.skip_sparql_blank_targets
    }

    pub(crate) fn new_trace(&self) -> usize {
        let mut traces = self.execution_traces.lock().unwrap();
        traces.push(Vec::new());
        traces.len() - 1
    }

    pub(crate) fn get_component(&self, id: &ComponentID) -> Option<&Component> {
        self.components.get(id)
    }

    pub(crate) fn order_constraints(&self, ids: &[ComponentID]) -> Vec<ComponentID> {
        fn cost_of(descriptor: Option<&ComponentDescriptor>) -> u8 {
            match descriptor {
                Some(ComponentDescriptor::Sparql { .. }) => 10,
                Some(ComponentDescriptor::Custom { .. }) => 9,
                Some(ComponentDescriptor::Or { .. })
                | Some(ComponentDescriptor::Xone { .. })
                | Some(ComponentDescriptor::Not { .. })
                | Some(ComponentDescriptor::And { .. }) => 8,
                _ => 1,
            }
        }

        let mut ordered: Vec<ComponentID> = ids.to_vec();
        let ir_components = &self.shape_ir.components;
        let model = &self.model;
        ordered.sort_by_key(|id| {
            let descriptor = ir_components
                .get(id)
                .or_else(|| model.get_component_descriptor(id));
            cost_of(descriptor)
        });
        ordered
    }

    #[allow(dead_code)]
    pub(crate) fn backend(&self) -> &impl GraphBackend<Error = String> {
        &*self.backend
    }

    #[allow(dead_code)]
    pub(crate) fn trace_sink(&self) -> &Arc<dyn TraceSink> {
        &self.trace_sink
    }

    pub(crate) fn prepare_query(&self, query: &str) -> Result<PreparedSparqlQuery, String> {
        self.backend.prepare_query(query)
    }

    pub(crate) fn objects_for_predicate(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        predicate: NamedNodeRef<'_>,
        graph: GraphNameRef<'_>,
    ) -> Result<Vec<Term>, String> {
        self.backend
            .objects_for_predicate(subject, predicate, graph)
    }

    pub(crate) fn execute_prepared<'a>(
        &'a self,
        query_str: &str,
        prepared: &'a PreparedSparqlQuery,
        substitutions: &[Binding],
        enforce_values_clause: bool,
    ) -> Result<QueryResults<'a>, String> {
        self.record_graph_call(GraphCallKind::ExecutePrepared);
        self.backend
            .execute_prepared(query_str, prepared, substitutions, enforce_values_clause)
    }

    pub(crate) fn contains_quad(&self, quad: &Quad) -> Result<bool, String> {
        self.backend.contains(quad)
    }

    pub(crate) fn quads_for_pattern(
        &self,
        subject: Option<NamedOrBlankNodeRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<&Term>,
        graph: Option<GraphNameRef<'_>>,
    ) -> Result<Vec<Quad>, String> {
        self.record_graph_call(GraphCallKind::QuadsForPattern);
        self.backend
            .quads_for_pattern(subject, predicate, object, graph)
    }

    pub(crate) fn insert_quads(&self, quads: &[Quad]) -> Result<(), String> {
        self.backend.insert_quads(quads)
    }

    pub(crate) fn prefixes_for_node(&self, node: &Term) -> Result<String, String> {
        self.model.sparql.prefixes_for_node(
            node,
            self.backend.store(),
            &self.model.env,
            self.shape_graph_iri_ref(),
        )
    }

    pub fn trace_events(&self) -> Arc<Mutex<Vec<TraceEvent>>> {
        Arc::clone(&self.trace_events)
    }

    pub fn shape_ir(&self) -> &ShapeIR {
        &self.shape_ir
    }

    pub(crate) fn cached_advanced_target(&self, selector: &Term) -> Option<Vec<Term>> {
        self.advanced_target_cache
            .read()
            .unwrap()
            .get(selector)
            .cloned()
    }

    pub(crate) fn store_advanced_target(&self, selector: &Term, nodes: &[Term]) {
        self.advanced_target_cache
            .write()
            .unwrap()
            .insert(selector.clone(), nodes.to_vec());
    }

    pub(crate) fn cached_node_targets(&self, id: &ID) -> Option<Vec<Term>> {
        self.node_target_cache.read().unwrap().get(id).cloned()
    }

    pub(crate) fn store_node_targets(&self, id: ID, nodes: Vec<Term>) {
        self.node_target_cache.write().unwrap().insert(id, nodes);
    }

    pub(crate) fn cached_prop_targets(&self, id: &PropShapeID) -> Option<Vec<Term>> {
        self.prop_target_cache.read().unwrap().get(id).cloned()
    }

    pub(crate) fn store_prop_targets(&self, id: PropShapeID, nodes: Vec<Term>) {
        self.prop_target_cache.write().unwrap().insert(id, nodes);
    }

    pub(crate) fn enter_component_scope(
        &self,
        component_id: ComponentID,
        source_shape: SourceShape,
    ) -> ActiveComponentScope {
        ACTIVE_COMPONENT_STACK.with(|stack| {
            stack.borrow_mut().push(ComponentGraphCallKey {
                component_id,
                source_shape,
            });
        });
        ActiveComponentScope
    }

    pub(crate) fn reset_component_graph_call_stats(&self) {
        self.graph_call_stats.lock().unwrap().clear();
    }

    pub(crate) fn reset_validation_run_state(&self) {
        self.reset_component_graph_call_stats();
        self.shape_timing_stats.lock().unwrap().clear();
        self.class_constraint_memo.write().unwrap().clear();
        *self.class_constraint_index.write().unwrap() = None;
    }

    pub(crate) fn component_graph_call_stats(&self) -> Vec<ComponentGraphCallStatRecord> {
        let mut rows: Vec<ComponentGraphCallStatRecord> = self
            .graph_call_stats
            .lock()
            .unwrap()
            .iter()
            .map(|(key, stats)| ComponentGraphCallStatRecord {
                component_id: key.component_id,
                source_shape: key.source_shape.clone(),
                quads_for_pattern_calls: stats.quads_for_pattern_calls,
                execute_prepared_calls: stats.execute_prepared_calls,
                component_invocations: stats.component_invocations,
                runtime_nanos_total: stats.runtime_nanos_total,
                runtime_nanos_sum_squares: stats.runtime_nanos_sum_squares,
                runtime_nanos_min: stats.runtime_nanos_min,
                runtime_nanos_max: stats.runtime_nanos_max,
            })
            .collect();
        rows.sort_by(|a, b| {
            b.runtime_nanos_total
                .cmp(&a.runtime_nanos_total)
                .then_with(|| b.component_invocations.cmp(&a.component_invocations))
                .then_with(|| {
                    let a_total = a.quads_for_pattern_calls + a.execute_prepared_calls;
                    let b_total = b.quads_for_pattern_calls + b.execute_prepared_calls;
                    b_total.cmp(&a_total)
                })
                .then_with(|| b.quads_for_pattern_calls.cmp(&a.quads_for_pattern_calls))
                .then_with(|| b.execute_prepared_calls.cmp(&a.execute_prepared_calls))
        });
        rows
    }

    pub(crate) fn record_component_duration(
        &self,
        component_id: ComponentID,
        source_shape: SourceShape,
        duration: Duration,
    ) {
        let nanos_u128 = duration.as_nanos();
        let nanos_u64 = if nanos_u128 > u64::MAX as u128 {
            u64::MAX
        } else {
            nanos_u128 as u64
        };
        let nanos_f64 = nanos_u64 as f64;
        let mut stats = self.graph_call_stats.lock().unwrap();
        let counters = stats
            .entry(ComponentGraphCallKey {
                component_id,
                source_shape,
            })
            .or_default();
        counters.component_invocations += 1;
        counters.runtime_nanos_total = counters.runtime_nanos_total.saturating_add(nanos_u128);
        counters.runtime_nanos_sum_squares += nanos_f64 * nanos_f64;
        if counters.runtime_nanos_min == 0 || nanos_u64 < counters.runtime_nanos_min {
            counters.runtime_nanos_min = nanos_u64;
        }
        if nanos_u64 > counters.runtime_nanos_max {
            counters.runtime_nanos_max = nanos_u64;
        }
    }

    pub(crate) fn record_shape_phase_duration(
        &self,
        source_shape: SourceShape,
        phase: ShapeTimingPhase,
        duration: Duration,
    ) {
        let nanos_u128 = duration.as_nanos();
        let nanos_u64 = if nanos_u128 > u64::MAX as u128 {
            u64::MAX
        } else {
            nanos_u128 as u64
        };
        let nanos_f64 = nanos_u64 as f64;
        let mut stats = self.shape_timing_stats.lock().unwrap();
        let counters = stats
            .entry(ShapeTimingKey {
                source_shape,
                phase,
            })
            .or_default();
        counters.invocations += 1;
        counters.runtime_nanos_total = counters.runtime_nanos_total.saturating_add(nanos_u128);
        counters.runtime_nanos_sum_squares += nanos_f64 * nanos_f64;
        if counters.runtime_nanos_min == 0 || nanos_u64 < counters.runtime_nanos_min {
            counters.runtime_nanos_min = nanos_u64;
        }
        if nanos_u64 > counters.runtime_nanos_max {
            counters.runtime_nanos_max = nanos_u64;
        }
    }

    pub(crate) fn shape_timing_stats(&self) -> Vec<ShapeTimingStatRecord> {
        let mut rows: Vec<ShapeTimingStatRecord> = self
            .shape_timing_stats
            .lock()
            .unwrap()
            .iter()
            .map(|(key, stats)| ShapeTimingStatRecord {
                source_shape: key.source_shape.clone(),
                phase: key.phase,
                invocations: stats.invocations,
                runtime_nanos_total: stats.runtime_nanos_total,
                runtime_nanos_sum_squares: stats.runtime_nanos_sum_squares,
                runtime_nanos_min: stats.runtime_nanos_min,
                runtime_nanos_max: stats.runtime_nanos_max,
            })
            .collect();
        rows.sort_by(|a, b| {
            b.runtime_nanos_total
                .cmp(&a.runtime_nanos_total)
                .then_with(|| b.invocations.cmp(&a.invocations))
        });
        rows
    }

    fn current_component_key(&self) -> Option<ComponentGraphCallKey> {
        ACTIVE_COMPONENT_STACK.with(|stack| stack.borrow().last().cloned())
    }

    fn record_graph_call(&self, call_kind: GraphCallKind) {
        let Some(key) = self.current_component_key() else {
            return;
        };
        let mut stats = self.graph_call_stats.lock().unwrap();
        let counters = stats.entry(key).or_default();
        match call_kind {
            GraphCallKind::QuadsForPattern => {
                counters.quads_for_pattern_calls += 1;
            }
            GraphCallKind::ExecutePrepared => {
                counters.execute_prepared_calls += 1;
            }
        }
    }

    pub(crate) fn class_constraint_matches_fast(
        &self,
        value_node: &Term,
        class_term: &Term,
    ) -> Result<Option<bool>, String> {
        if !matches!(class_term, Term::NamedNode(_) | Term::BlankNode(_)) {
            return Ok(None);
        }
        if !matches!(value_node, Term::NamedNode(_) | Term::BlankNode(_)) {
            return Ok(Some(false));
        }

        let memo_key = (value_node.clone(), class_term.clone());
        if let Some(result) = self.class_constraint_memo.read().unwrap().get(&memo_key) {
            return Ok(Some(*result));
        }

        let index = self.ensure_class_constraint_index()?;
        let Some(descendants) = index.class_bitset(class_term) else {
            self.class_constraint_memo
                .write()
                .unwrap()
                .insert(memo_key, false);
            return Ok(Some(false));
        };
        let result = index
            .subject_type_bits
            .get(value_node)
            .map(|type_bits| type_bits.intersection(descendants).next().is_some())
            .unwrap_or(false);

        self.class_constraint_memo
            .write()
            .unwrap()
            .insert(memo_key, result);
        Ok(Some(result))
    }

    /// Return all subjects that are `rdf:type` instances of `class` (or any
    /// subclass of it), using the precomputed bitmap index.
    pub(crate) fn instances_of_class(&self, class: &Term) -> Result<Vec<Term>, String> {
        let index = self.ensure_class_constraint_index()?;
        Ok(index.instances_of_class(class))
    }

    fn ensure_class_constraint_index(&self) -> Result<Arc<ClassConstraintIndex>, String> {
        if let Some(index) = self.class_constraint_index.read().unwrap().as_ref() {
            return Ok(Arc::clone(index));
        }
        let index = Arc::new(self.build_class_constraint_index()?);
        let mut cache = self.class_constraint_index.write().unwrap();
        if let Some(existing) = cache.as_ref() {
            return Ok(Arc::clone(existing));
        }
        *cache = Some(Arc::clone(&index));
        Ok(index)
    }

    fn build_class_constraint_index(&self) -> Result<ClassConstraintIndex, String> {
        let sub_class_of =
            NamedNodeRef::new_unchecked("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        let subclass_quads = self.quads_for_pattern(None, Some(sub_class_of), None, None)?;

        let mut term_to_id: HashMap<Term, usize> = HashMap::new();
        let mut next_id = 0usize;
        let mut parent_edges: Vec<(usize, usize)> = Vec::new();

        let mut intern = |term: Term| {
            if let Some(id) = term_to_id.get(&term).copied() {
                id
            } else {
                let id = next_id;
                next_id += 1;
                term_to_id.insert(term, id);
                id
            }
        };

        for quad in subclass_quads {
            let subclass = term_from_subject(&quad.subject);
            let superclass = term_from_term_ref(quad.object.as_ref());
            let (Some(subclass), Some(superclass)) = (subclass, superclass) else {
                continue;
            };
            let sub_id = intern(subclass);
            let super_id = intern(superclass);
            parent_edges.push((sub_id, super_id));
        }

        let type_quads =
            self.quads_for_pattern(None, Some(rdf::TYPE), None, Some(self.data_graph_iri_ref()))?;

        let mut subject_type_ids: HashMap<Term, Vec<usize>> = HashMap::new();
        for quad in type_quads {
            let subject = term_from_subject(&quad.subject);
            let class = term_from_term_ref(quad.object.as_ref());
            let (Some(subject), Some(class)) = (subject, class) else {
                continue;
            };
            let class_id = intern(class);
            subject_type_ids.entry(subject).or_default().push(class_id);
        }

        let class_count = next_id;
        let mut parents: Vec<Vec<usize>> = vec![Vec::new(); class_count];
        for (child, parent) in parent_edges {
            if !parents[child].contains(&parent) {
                parents[child].push(parent);
            }
        }

        let mut ancestors_by_subclass: Vec<FixedBitSet> = (0..class_count)
            .map(|_| FixedBitSet::with_capacity(class_count))
            .collect();
        for subclass in 0..class_count {
            let mut visited = HashSet::new();
            let mut stack = vec![subclass];
            while let Some(node) = stack.pop() {
                if !visited.insert(node) {
                    continue;
                }
                ancestors_by_subclass[subclass].insert(node);
                for parent in &parents[node] {
                    stack.push(*parent);
                }
            }
        }

        let mut descendants_by_super: Vec<FixedBitSet> = (0..class_count)
            .map(|_| FixedBitSet::with_capacity(class_count))
            .collect();
        for (subclass, ancestors) in ancestors_by_subclass.iter().enumerate() {
            for super_id in ancestors.ones() {
                descendants_by_super[super_id].insert(subclass);
            }
        }

        let mut subject_type_bits: HashMap<Term, FixedBitSet> = HashMap::new();
        for (subject, class_ids) in subject_type_ids {
            let mut bits = FixedBitSet::with_capacity(class_count);
            for class_id in class_ids {
                bits.insert(class_id);
            }
            subject_type_bits.insert(subject, bits);
        }

        Ok(ClassConstraintIndex {
            term_to_id,
            descendants_by_super,
            subject_type_bits,
        })
    }

    pub(crate) fn is_data_skolem_iri(&self, node: NamedNodeRef<'_>) -> bool {
        node.as_str().starts_with(&self.data_graph_skolem_base)
    }

    pub(crate) fn is_shape_skolem_iri(&self, node: NamedNodeRef<'_>) -> bool {
        node.as_str().starts_with(&self.shape_graph_skolem_base)
    }

    pub(crate) fn get_trace_item_label_and_type(&self, item: &TraceItem) -> (String, String) {
        match item {
            TraceItem::NodeShape(id) => {
                let label = self
                    .model
                    .nodeshape_id_lookup
                    .read()
                    .unwrap()
                    .get_term(*id)
                    .map_or_else(
                        || format!("Unknown NodeShape ID: {:?}", id),
                        format_term_for_label,
                    );
                (label, "NodeShape".to_string())
            }
            TraceItem::PropertyShape(id) => {
                let label = self.model.get_prop_shape_by_id(id).map_or_else(
                    || format!("Unknown PropertyShape ID: {:?}", id),
                    |ps| ps.sparql_path(),
                );
                (label, "PropertyShape".to_string())
            }
            TraceItem::Component(id) => {
                let label = self
                    .model
                    .get_component_descriptor(id)
                    .map(|descriptor| build_component_from_descriptor(descriptor).label())
                    .unwrap_or_else(|| format!("Unknown Component ID: {:?}", id));
                (label, "Component".to_string())
            }
        }
    }
}

fn custom_component_cache_key(
    definition: &CustomConstraintComponentDefinition,
    parameter_values: &HashMap<NamedNode, Vec<Term>>,
) -> String {
    let mut entries: Vec<String> = parameter_values
        .iter()
        .map(|(param, values)| {
            let mut value_strings: Vec<String> =
                values.iter().map(|term| term.to_string()).collect();
            value_strings.sort();
            format!("{}={}", param.as_str(), value_strings.join(","))
        })
        .collect();
    entries.sort();
    format!("{}|{}", definition.iri.as_str(), entries.join("|"))
}

fn term_from_subject(subject: &NamedOrBlankNode) -> Option<Term> {
    match subject {
        NamedOrBlankNode::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
        NamedOrBlankNode::BlankNode(bn) => Some(Term::BlankNode(bn.clone())),
    }
}

fn term_from_term_ref(term: TermRef<'_>) -> Option<Term> {
    match term {
        TermRef::NamedNode(nn) => Some(Term::NamedNode(nn.into_owned())),
        TermRef::BlankNode(bn) => Some(Term::BlankNode(bn.into_owned())),
        TermRef::Literal(_) => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SourceShape {
    NodeShape(ID),
    PropertyShape(PropShapeID),
}

impl fmt::Display for SourceShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceShape::NodeShape(id) => write!(f, "{}", id),
            SourceShape::PropertyShape(id) => write!(f, "{}", id),
        }
    }
}

impl SourceShape {
    pub(crate) fn as_prop_id(&self) -> Option<&PropShapeID> {
        match self {
            SourceShape::PropertyShape(id) => Some(id),
            _ => None,
        }
    }

    pub(crate) fn as_node_id(&self) -> Option<&ID> {
        match self {
            SourceShape::NodeShape(id) => Some(id),
            _ => None,
        }
    }

    pub(crate) fn get_term(&self, ctx: &ValidationContext) -> Option<Term> {
        match self {
            SourceShape::NodeShape(id) => ctx
                .model
                .nodeshape_id_lookup()
                .read()
                .unwrap()
                .get_term(*id)
                .cloned(),
            SourceShape::PropertyShape(id) => ctx
                .model
                .propshape_id_lookup()
                .read()
                .unwrap()
                .get_term(*id)
                .cloned(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Context {
    focus_node: Term,
    pub(crate) result_path: Option<PShapePath>,
    value_nodes: Option<Vec<Term>>,
    value: Option<Term>,
    source_shape: SourceShape,
    trace_index: usize,
    pub source_constraint: Option<Term>,
}

impl PartialEq for Context {
    fn eq(&self, other: &Self) -> bool {
        self.focus_node == other.focus_node
            && self.result_path == other.result_path
            && self.value_nodes == other.value_nodes
            && self.value == other.value
            && self.source_shape == other.source_shape
            && self.source_constraint == other.source_constraint
    }
}

impl Eq for Context {}

impl Hash for Context {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.focus_node.hash(state);
        self.result_path.hash(state);
        self.value_nodes.hash(state);
        self.value.hash(state);
        self.source_shape.hash(state);
        self.source_constraint.hash(state);
    }
}

impl Context {
    pub(crate) fn new(
        focus_node: Term,
        result_path: Option<PShapePath>,
        value_nodes: Option<Vec<Term>>,
        source_shape: SourceShape,
        trace_index: usize,
    ) -> Self {
        Self {
            focus_node,
            result_path,
            value_nodes,
            source_shape,
            value: None,
            trace_index,
            source_constraint: None,
        }
    }

    pub(crate) fn with_value(&mut self, value: Term) {
        self.value = Some(value);
    }

    pub(crate) fn with_result_path(&mut self, result_path: Term) {
        self.result_path = Some(crate::types::Path::Simple(result_path));
    }

    pub(crate) fn set_focus_node(&mut self, focus_node: Term) {
        self.focus_node = focus_node;
    }

    pub(crate) fn focus_node(&self) -> &Term {
        &self.focus_node
    }

    pub(crate) fn result_path(&self) -> Option<&PShapePath> {
        self.result_path.as_ref()
    }

    pub(crate) fn value_nodes(&self) -> Option<&Vec<Term>> {
        self.value_nodes.as_ref()
    }

    pub(crate) fn value_nodes_mut(&mut self) -> Option<&mut Vec<Term>> {
        self.value_nodes.as_mut()
    }

    pub(crate) fn value(&self) -> Option<&Term> {
        self.value.as_ref()
    }

    pub(crate) fn source_shape(&self) -> SourceShape {
        self.source_shape.clone()
    }

    pub(crate) fn trace_index(&self) -> usize {
        self.trace_index
    }
}

impl Drop for ActiveComponentScope {
    fn drop(&mut self) {
        ACTIVE_COMPONENT_STACK.with(|stack| {
            let _ = stack.borrow_mut().pop();
        });
    }
}
