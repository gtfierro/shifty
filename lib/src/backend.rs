//! Backend abstraction for graph access and SPARQL execution.
//!
//! The v1 engine talks to Oxigraph directly; v2 will route through this trait so
//! different executors (interpreted vs. compiled) can swap backends without
//! touching parser or planner code.

use crate::sparql::SparqlExecutor;
use crate::sparql::SparqlServices;
use oxigraph::model::{
    GraphNameRef, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Quad, Term, TermRef,
};
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, Variable};
use oxigraph::store::Store;
use std::sync::Arc;

/// Typed binding used when substituting variables into prepared SPARQL queries.
pub type Binding = (Variable, Term);

/// Minimal interface required by executors to read and update RDF data.
pub trait GraphBackend {
    type Error;

    /// Default data graph for validation.
    fn data_graph(&self) -> GraphNameRef<'_>;

    /// Shapes graph that produced the validation plan.
    fn shapes_graph(&self) -> GraphNameRef<'_>;

    /// Retrieve objects for a subject/predicate pair within a named graph.
    fn objects_for_predicate(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        predicate: NamedNodeRef<'_>,
        graph: GraphNameRef<'_>,
    ) -> Result<Vec<Term>, Self::Error>;

    /// Prepare (and optionally cache) a SPARQL query for reuse.
    fn prepare_query(&self, query: &str) -> Result<PreparedSparqlQuery, Self::Error>;

    /// Execute a prepared query with variable substitutions.
    fn execute_prepared<'a>(
        &'a self,
        query_str: &str,
        prepared: &'a PreparedSparqlQuery,
        substitutions: &[Binding],
        enforce_values_clause: bool,
    ) -> Result<QueryResults<'a>, Self::Error>;

    /// Check whether the store already contains a quad.
    fn contains(&self, quad: &Quad) -> Result<bool, Self::Error>;

    /// Retrieve quads matching a pattern.
    fn quads_for_pattern(
        &self,
        subject: Option<NamedOrBlankNodeRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<&Term>,
        graph: Option<GraphNameRef<'_>>,
    ) -> Result<Vec<Quad>, Self::Error>;

    /// Insert quads, typically used by rule engines.
    fn insert_quads(&self, quads: &[Quad]) -> Result<(), Self::Error>;

    /// Access to the underlying store for escape hatches while migrating.
    fn store(&self) -> &Store;
}

/// Oxigraph-backed implementation that mirrors the current engine behaviour.
#[derive(Clone)]
pub struct OxigraphBackend {
    store: Arc<Store>,
    data_graph: NamedNode,
    shapes_graph: NamedNode,
    sparql: Arc<SparqlServices>,
}

impl OxigraphBackend {
    pub fn new(
        store: Store,
        data_graph: NamedNode,
        shapes_graph: NamedNode,
        sparql: Arc<SparqlServices>,
    ) -> Self {
        Self {
            store: Arc::new(store),
            data_graph,
            shapes_graph,
            sparql,
        }
    }
}

impl GraphBackend for OxigraphBackend {
    type Error = String;

    fn data_graph(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.data_graph.as_ref())
    }

    fn shapes_graph(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.shapes_graph.as_ref())
    }

    fn objects_for_predicate(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        predicate: NamedNodeRef<'_>,
        graph: GraphNameRef<'_>,
    ) -> Result<Vec<Term>, Self::Error> {
        let mut results = Vec::new();
        for quad in
            self.store
                .as_ref()
                .quads_for_pattern(Some(subject), Some(predicate), None, Some(graph))
        {
            let q = quad.map_err(|e| e.to_string())?;
            results.push(q.object);
        }
        Ok(results)
    }

    fn prepare_query(&self, query: &str) -> Result<PreparedSparqlQuery, Self::Error> {
        self.sparql.prepared_query(query)
    }

    fn execute_prepared<'a>(
        &'a self,
        query_str: &str,
        prepared: &'a PreparedSparqlQuery,
        substitutions: &[Binding],
        enforce_values_clause: bool,
    ) -> Result<QueryResults<'a>, Self::Error> {
        self.sparql.execute_with_substitutions(
            query_str,
            prepared,
            self.store.as_ref(),
            substitutions,
            enforce_values_clause,
        )
    }

    fn contains(&self, quad: &Quad) -> Result<bool, Self::Error> {
        self.store
            .contains(quad.as_ref())
            .map_err(|e| e.to_string())
    }

    fn quads_for_pattern(
        &self,
        subject: Option<NamedOrBlankNodeRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<&Term>,
        graph: Option<GraphNameRef<'_>>,
    ) -> Result<Vec<Quad>, Self::Error> {
        let subject_ref = subject;
        let object_ref: Option<TermRef<'_>> = object.map(|t| match t {
            Term::NamedNode(nn) => nn.as_ref().into(),
            Term::BlankNode(bn) => bn.as_ref().into(),
            Term::Literal(l) => l.as_ref().into(),
            #[allow(unreachable_patterns)]
            _ => unreachable!("Unsupported term type in quads_for_pattern object"),
        });

        let mut out = Vec::new();
        for q in self
            .store
            .as_ref()
            .quads_for_pattern(subject_ref, predicate, object_ref, graph)
        {
            out.push(q.map_err(|e| e.to_string())?);
        }
        Ok(out)
    }

    fn insert_quads(&self, quads: &[Quad]) -> Result<(), Self::Error> {
        for quad in quads {
            self.store.insert(quad).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn store(&self) -> &Store {
        self.store.as_ref()
    }
}
