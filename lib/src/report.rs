use crate::context::Context;
use std::collections::HashMap;
use oxigraph::model::Term; // For using Term as a HashMap key

pub struct ValidationReportBuilder {
    pub(crate) results: Vec<(Context, String)>, // Made pub(crate)
}

impl ValidationReportBuilder {
    pub fn new() -> Self {
        ValidationReportBuilder {
            results: Vec::new(),
        }
    }

    pub fn add_error(&mut self, context: &Context, error: String) {
        // Store the context by cloning it, as the original context might have a shorter lifetime.
        // The error string is moved.
        self.results.push((context.clone(), error));
        // The println! macro is removed as per the request to track errors instead of printing.
    }

    pub fn results(&self) -> &[(Context, String)] {
        &self.results
    }

    pub fn dump(&self) {
        if self.results.is_empty() {
            println!("Validation report: No errors found.");
            return;
        }

        println!("Validation Report:");
        println!("------------------");

        let mut grouped_errors: HashMap<Term, Vec<(&Context, &String)>> = HashMap::new();

        for (context, error_message) in &self.results {
            grouped_errors
                .entry(context.focus_node().clone())
                .or_default()
                .push((context, error_message));
        }

        for (focus_node, context_error_pairs) in grouped_errors {
            println!("\nFocus Node: {}", focus_node);
            for (context, error) in context_error_pairs {
                println!("  - Error: {}", error);
                println!("    From shape: {}", context.source_shape());
                println!("    Trace: {:?}", context.execution_trace());
            }
        }
        println!("\n------------------");
    }
}
