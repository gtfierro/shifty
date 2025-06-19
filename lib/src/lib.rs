#![allow(dead_code, unused_variables)]

// Public modules defining the public API.
pub mod context;
pub mod report;
pub mod test_utils; // Often pub for integration tests

// Internal modules.
mod canonicalization;
mod components;
mod named_nodes;
mod optimize;
mod parser;
mod shape;
mod types;
mod validate;

use context::ValidationContext;
use report::ValidationReport;
use std::error::Error;

/// A simple facade for the SHACL validator.
///
/// This provides a straightforward interface for common validation tasks.
/// For more advanced control, use `ValidationContext` directly.
pub struct Validator {
    context: ValidationContext,
}

impl Validator {
    /// Creates a new Validator from shapes and data files.
    ///
    /// # Arguments
    ///
    /// * `shapes_file_path` - Path to the file containing the SHACL shapes.
    /// * `data_file_path` - Path to the file containing the data to be validated.
    pub fn from_files(
        shapes_file_path: &str,
        data_file_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let context = ValidationContext::from_files(shapes_file_path, data_file_path)?;
        Ok(Validator { context })
    }

    /// Validates the data graph against the shapes graph.
    /// The returned report is tied to the lifetime of the Validator.
    pub fn validate(&self) -> ValidationReport<'_> {
        let report_builder = self.context.validate();
        // The report needs the context to be able to serialize itself later.
        ValidationReport::new(report_builder, &self.context)
    }

    /// Generates a Graphviz DOT string representation of the shapes.
    pub fn to_graphviz(&self) -> Result<String, String> {
        self.context.graphviz()
    }
}
