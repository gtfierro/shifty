//! A SHACL validator library.
#![deny(clippy::all)]

// Publicly visible items
pub mod components;
pub mod shape;
pub mod types;

pub use report::ValidationReport;

// Internal modules.
pub mod canonicalization;
pub(crate) mod context;
pub(crate) mod named_nodes;
pub(crate) mod optimize;
pub(crate) mod parser;
pub(crate) mod report;
pub mod test_utils; // Often pub for integration tests
pub(crate) mod validate;

use crate::context::ValidationContext;
use std::error::Error;

/// A simple facade for the SHACL validator.
///
/// This provides a straightforward interface for common validation tasks.
/// It handles the creation of a `ValidationContext`, parsing of shapes and data,
/// running the validation, and generating reports.
///
/// For more advanced control, such as inspecting the parsed shapes or performing
/// optimizations, use `ValidationContext` directly.
pub struct Validator {
    context: ValidationContext,
}

impl Validator {
    /// Creates a new Validator from shapes and data files.
    ///
    /// This method initializes the underlying `ValidationContext`, loads the specified
    /// files into the store, and parses the SHACL shapes.
    ///
    /// # Arguments
    ///
    /// * `shapes_file_path` - Path to the file containing the SHACL shapes (e.g., in Turtle format).
    /// * `data_file_path` - Path to the file containing the data to be validated.
    pub fn from_files(
        shapes_file_path: &str,
        data_file_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let context = ValidationContext::from_files(shapes_file_path, data_file_path)?;
        Ok(Validator { context })
    }

    /// Validates the data graph against the shapes graph.
    ///
    /// This method executes the core validation logic and returns a `ValidationReport`.
    /// The report contains the outcome of the validation (conformity) and detailed
    /// results for any failures. The returned report is tied to the lifetime of the Validator.
    pub fn validate(&self) -> ValidationReport<'_> {
        let report_builder = self.context.validate();
        // The report needs the context to be able to serialize itself later.
        ValidationReport::new(report_builder, &self.context)
    }

    /// Generates a Graphviz DOT string representation of the shapes.
    ///
    /// This can be used to visualize the structure of the SHACL shapes, including
    /// their constraints and relationships.
    pub fn to_graphviz(&self) -> Result<String, String> {
        self.context.graphviz()
    }

    /// Generates a Graphviz DOT string representation of the shapes, with nodes colored by execution frequency.
    ///
    /// This can be used to visualize which parts of the shapes graph were most active during validation.
    /// Note: `validate()` must be called before this method to populate the execution traces.
    pub fn to_graphviz_heatmap(&self, include_all_nodes: bool) -> Result<String, String> {
        self.context.graphviz_heatmap(include_all_nodes)
    }
}
