use crate::named_nodes::SHACL;
use crate::context::{ValidationContext, Context};
use oxigraph::model::{NamedNodeRef, Term, TermRef, Variable};
use oxigraph::sparql::{Query, QueryResults, QueryOptions}; // Added Query
use std::fmt; // Added for Display trait
use std::hash::Hash; // Added Hash for derived traits

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ID(pub u64);

impl From<u64> for ID {
    fn from(item: u64) -> Self {
        ID(item)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ID {
    pub fn to_graphviz_id(&self) -> String {
        format!("n{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ComponentID(pub u64);

impl From<u64> for ComponentID {
    fn from(item: u64) -> Self {
        ComponentID(item)
    }
}

impl fmt::Display for ComponentID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ComponentID {
    pub fn to_graphviz_id(&self) -> String {
        format!("c{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct PropShapeID(pub u64);

impl From<u64> for PropShapeID {
    fn from(item: u64) -> Self {
        PropShapeID(item)
    }
}

impl fmt::Display for PropShapeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PropShapeID {
    pub fn to_graphviz_id(&self) -> String {
        format!("p{}", self.0)
    }
}

#[derive(Debug)]
pub enum Path {
    Simple(Term),
}

#[derive(Debug)]
pub enum Target {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
}

impl Target {
    pub fn from_predicate_object(predicate: NamedNodeRef, object: TermRef) -> Option<Self> {
        let shacl = SHACL::new();
        if predicate == shacl.target_class {
            Some(Target::Class(object.into_owned()))
        } else if predicate == shacl.target_node {
            Some(Target::Node(object.into_owned()))
        } else if predicate == shacl.target_subjects_of {
            Some(Target::SubjectsOf(object.into_owned()))
        } else if predicate == shacl.target_objects_of {
            Some(Target::ObjectsOf(object.into_owned()))
        } else {
            None
        }
    }

    pub fn get_target_nodes(&self, context: &ValidationContext) -> Vec<Context> {
        match self {
            Target::Node(t) => {
                vec![Context::new(t.clone(), None, None)]
            }
            Target::Class(c) => {
                let query_str = "SELECT DISTINCT ?inst WHERE { ?inst rdf:type/rdfs:subClassOf* ?target_class . }";
                let target_class_var = Variable::new_unchecked("target_class");

                match Query::parse(query_str, None) {
                    Ok(mut parsed_query) => {
                        parsed_query.dataset_mut().set_default_graph_as_union();
                        // NOTE: The following method call 'execute_parsed_query_with_substitutions' is hypothetical.
                        // It assumes that 'context.store()' provides a way to execute a pre-parsed 'Query' object
                        // with variable substitutions, and that it respects the dataset configuration
                        // set on the 'parsed_query' object.
                        match context.store().execute_parsed_query_with_substitutions(
                            &parsed_query,
                            QueryOptions::default(), // Base options, assuming dataset config is from parsed_query
                            [(target_class_var, c.clone())],
                        ) {
                            Ok(QueryResults::Solutions(solutions)) => solutions
                                .filter_map(|solution_result| {
                                    solution_result.ok().and_then(|solution| {
                                        solution.get("inst").map(|term_ref| {
                                            Context::new(term_ref.to_owned(), None, None)
                                        })
                                    })
                                })
                                .collect(),
                            _ => vec![], // Handles query errors or unexpected result types
                        }
                    }
                    Err(_) => {
                        // eprintln!("SPARQL parse error for Target::Class: {}", query_str); // Optional: log error
                        vec![] // Handle SPARQL parse error
                    }
                }
            }
            Target::SubjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!("SELECT DISTINCT ?s WHERE {{ ?s <{}> ?any . }}", predicate_node.as_str());
                    match Query::parse(&query_str, None) {
                        Ok(mut parsed_query) => {
                            parsed_query.dataset_mut().set_default_graph_as_union();
                            // NOTE: The following method call 'execute_parsed_query' is hypothetical.
                            // It assumes that 'context.store()' provides a way to execute a pre-parsed 'Query' object
                            // and that it respects the dataset configuration set on the 'parsed_query' object.
                            match context.store().execute_parsed_query(&parsed_query, QueryOptions::default()) {
                                Ok(QueryResults::Solutions(solutions)) => solutions
                                    .filter_map(|solution_result| {
                                        solution_result.ok().and_then(|solution| {
                                            solution.get("s").map(|term_ref| {
                                                Context::new(term_ref.to_owned(), None, None)
                                            })
                                        })
                                    })
                                    .collect(),
                                _ => vec![], // Handles query errors or unexpected result types
                            }
                        }
                        Err(_) => {
                            // eprintln!("SPARQL parse error for Target::SubjectsOf: {}", query_str); // Optional: log error
                            vec![] // Handle SPARQL parse error
                        }
                    }
                } else {
                    vec![] // Predicate for SubjectsOf must be an IRI
                }
            }
            Target::ObjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!("SELECT DISTINCT ?o WHERE {{ ?any <{}> ?o . }}", predicate_node.as_str());
                    match Query::parse(&query_str, None) {
                        Ok(mut parsed_query) => {
                            parsed_query.dataset_mut().set_default_graph_as_union();
                            // NOTE: The following method call 'execute_parsed_query' is hypothetical.
                            // See note in Target::SubjectsOf.
                            match context.store().execute_parsed_query(&parsed_query, QueryOptions::default()) {
                                Ok(QueryResults::Solutions(solutions)) => solutions
                                    .filter_map(|solution_result| {
                                        solution_result.ok().and_then(|solution| {
                                            solution.get("o").map(|term_ref| {
                                                Context::new(term_ref.to_owned(), None, None)
                                            })
                                        })
                                    })
                                    .collect(),
                                _ => vec![], // Handles query errors or unexpected result types
                            }
                        }
                        Err(_) => {
                            // eprintln!("SPARQL parse error for Target::ObjectsOf: {}", query_str); // Optional: log error
                            vec![] // Handle SPARQL parse error
                        }
                    }
                } else {
                    vec![] // Predicate for ObjectsOf must be an IRI
                }
            }
        }
    }
}
