use crate::context::{Context, ValidationContext};
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, PropShapeID, ID};
use crate::types::{Path, Target};
// SHACL, Term, NamedNode, TermRef were unused

pub trait ValidateShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String>;
}

#[derive(Debug)]
pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape),
}

#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    // TODO severity
    // TODO message
}

impl NodeShape {
    pub fn new(identifier: ID, targets: Vec<Target>, constraints: Vec<ComponentID>) -> Self {
        NodeShape {
            identifier,
            targets,
            constraints,
        }
    }

    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }
}

impl ValidateShape for NodeShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        println!("Validating NodeShape with identifier: {}", self.identifier);
        // first gather all of the targets
        println!("targets: {:?}", self.targets);
        let target_contexts = self
            .targets
            .iter()
            .map(|t| t.get_target_nodes(context))
            .flatten();
        let target_contexts: Vec<_> = target_contexts.collect();

        if target_contexts.len() > 0 {
            println!("Targets: {:?}", target_contexts.len());
        }

        // for target in target_contexts {
        //     // for each target, validate the constraints
        //     for constraint in &self.constraints {
        //         // if let Err(e) = constraint.validate(&target, rb) {
        //         //     rb.add_error(&target, e);
        //         // }
        //     }
        // }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    path: Path,
    constraints: Vec<ComponentID>,
    // TODO severity
    // TODO message
}

impl PropertyShape {
    pub fn new(identifier: PropShapeID, path: Path, constraints: Vec<ComponentID>) -> Self {
        PropertyShape {
            identifier,
            path,
            constraints,
        }
    }
    pub fn identifier(&self) -> &PropShapeID {
        &self.identifier
    }
    pub fn path(&self) -> String {
        match &self.path {
            Path::Simple(t) => format!("{}", t),
        }
    }

    pub fn path_term(&self) -> &oxigraph::model::Term {
        match &self.path {
            Path::Simple(t) => t,
        }
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }
}
