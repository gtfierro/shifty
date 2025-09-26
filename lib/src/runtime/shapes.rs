//! Shape-level orchestration for runtime validation passes.

use crate::context::ValidationContext;
use crate::report::ValidationReportBuilder;

/// Coordinates validation of a shape using runtime evaluators.
pub(crate) trait ValidateShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String>;
}
