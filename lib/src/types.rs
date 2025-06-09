use crate::context::{Context, ValidationContext};
use crate::named_nodes::SHACL;
use oxigraph::model::{NamedNodeRef, Term, TermRef, Variable};
use oxigraph::sparql::{Query, QueryOptions, QueryResults}; // Added Query
use std::fmt; // Added for Display trait
use std::hash::Hash; // Added Hash for derived traits

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TermID(pub u64);

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Path {
    Simple(Term), // An IRI
    Inverse(Box<Path>),
    Sequence(Vec<Path>),
    Alternative(Vec<Path>),
    ZeroOrMore(Box<Path>),
    OneOrMore(Box<Path>),
    ZeroOrOne(Box<Path>),
}

impl Path {
    // Helper to format a NamedNode for SPARQL path
    fn format_named_node_for_sparql(nn: &oxigraph::model::NamedNode) -> String {
        format!("<{}>", nn.as_str())
    }

    pub fn to_sparql_path(&self) -> Result<String, String> {
        match self {
            Path::Simple(term) => match term {
                Term::NamedNode(nn) => Ok(Self::format_named_node_for_sparql(nn)),
                _ => Err(format!("Simple path must be an IRI {:?}", self).to_string()),
            },
            Path::Inverse(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                // Based on SPARQL grammar, if inner_sparql is already a (Path) or an IRIref,
                // it doesn't need more parentheses for ^.
                // Our Sequence and Alternative variants return parenthesized strings.
                // Simple returns an IRIref.
                Ok(format!("^{}", inner_sparql))
            }
            Path::Sequence(paths) => {
                if paths.len() < 2 {
                    // SHACL specification: sequence path is a list of at least two members.
                    // If parsing allowed a single path, it should be simplified upstream.
                    // For now, error if not adhering to the typical structure.
                    return Err(format!(
                        "Sequence path must have at least two elements, found {}",
                        paths.len()
                    ));
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                let result = sparql_paths.join(" / ");
                Ok(format!("({})", result)) // Always parenthesize sequence for clarity and safety.
            }
            Path::Alternative(paths) => {
                if paths.len() < 2 {
                    // SHACL specification: alternative path is a list of at least two members.
                    return Err(format!(
                        "Alternative path must have at least two elements, found {}",
                        paths.len()
                    ));
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                let result = sparql_paths.join(" | ");
                Ok(format!("({})", result)) // Always parenthesize alternative for clarity and safety.
            }
            Path::ZeroOrMore(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                Ok(format!("{}*", inner_sparql))
            }
            Path::OneOrMore(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                Ok(format!("{}+", inner_sparql))
            }
            Path::ZeroOrOne(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                Ok(format!("{}?", inner_sparql))
            }
        }
    }
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

    pub fn get_target_nodes(&self, context: &ValidationContext, source_shape_id: ID) -> Vec<Context> {
        match self {
            Target::Node(t) => vec![Context::new(t.clone(), None, Some(vec![t.clone()]), source_shape_id)],
            Target::Class(c) => {
                let query_str = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT DISTINCT ?inst ?target_class WHERE { ?inst rdf:type ?c . ?c rdfs:subClassOf* ?target_class }";
                let target_class_var = Variable::new_unchecked("target_class");

                match Query::parse(query_str, None) {
                    Ok(mut parsed_query) => {
                        parsed_query.dataset_mut().set_default_graph_as_union();
                        match context.store().query_opt_with_substituted_variables(
                            parsed_query,
                            QueryOptions::default(), // Base options, assuming dataset config is from parsed_query
                            [(target_class_var, c.clone())],
                        ) {
                            Ok(QueryResults::Solutions(solutions)) => solutions
                                .filter_map(|solution_result| {
                                    solution_result.ok().and_then(|solution| {
                                        solution.get("inst").map(|term_ref| {
                                            Context::new(term_ref.to_owned(), None, Some(vec![term_ref.clone()]), source_shape_id)
                                        })
                                    })
                                })
                                .collect(),
                            Err(e) => {
                                eprintln!(
                                    "SPARQL query error for Target::Class: {} {:?}",
                                    query_str, e
                                ); // Optional: log error
                                vec![] // Handle query error
                            }
                            _ => {
                                eprintln!(
                                    "Unexpected result type for Target::Class: {}",
                                    query_str
                                ); // Optional: log unexpected result type
                                vec![] // Handle unexpected result type
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!(
                            "SPARQL parse error for Target::Class: {} {:?}",
                            query_str, e
                        ); // Optional: log error
                        vec![] // Handle SPARQL parse error
                    }
                }
            }
            Target::SubjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?s WHERE {{ ?s <{}> ?any . }}",
                        predicate_node.as_str()
                    );
                    match Query::parse(&query_str, None) {
                        Ok(mut parsed_query) => {
                            parsed_query.dataset_mut().set_default_graph_as_union();
                            match context
                                .store()
                                .query_opt(parsed_query, QueryOptions::default())
                            {
                                Ok(QueryResults::Solutions(solutions)) => solutions
                                    .filter_map(|solution_result| {
                                        solution_result.ok().and_then(|solution| {
                                            solution.get("s").map(|term_ref| {
                                                Context::new(term_ref.to_owned(), None, Some(vec![term_ref.clone()]), source_shape_id)
                                            })
                                        })
                                    })
                                    .collect(),
                                _ => vec![], // Handles query errors or unexpected result types
                            }
                        }
                        Err(_) => {
                            eprintln!("SPARQL parse error for Target::SubjectsOf: {}", query_str); // Optional: log error
                            vec![] // Handle SPARQL parse error
                        }
                    }
                } else {
                    vec![] // Predicate for SubjectsOf must be an IRI
                }
            }
            Target::ObjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?o WHERE {{ ?any <{}> ?o . }}",
                        predicate_node.as_str()
                    );
                    match Query::parse(&query_str, None) {
                        Ok(mut parsed_query) => {
                            parsed_query.dataset_mut().set_default_graph_as_union();
                            match context
                                .store()
                                .query_opt(parsed_query, QueryOptions::default())
                            {
                                Ok(QueryResults::Solutions(solutions)) => solutions
                                    .filter_map(|solution_result| {
                                        solution_result.ok().and_then(|solution| {
                                            solution.get("o").map(|term_ref| {
                                                Context::new(term_ref.to_owned(), None, Some(vec![term_ref.clone()]), source_shape_id)
                                            })
                                        })
                                    })
                                    .collect(),
                                _ => vec![], // Handles query errors or unexpected result types
                            }
                        }
                        Err(_) => {
                            eprintln!("SPARQL parse error for Target::ObjectsOf: {}", query_str); // Optional: log error
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
