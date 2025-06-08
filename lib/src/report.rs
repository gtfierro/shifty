use crate::context::Context;
use std::collections::HashMap;
use oxigraph::model::Term; // For using Term as a HashMap key

pub struct ValidationReportBuilder {
    results: Vec<(Context, String)>,
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

    pub fn dump(&self) {
        if self.results.is_empty() {
            println!("Validation report: No errors found.");
            return;
        }

        println!("Validation Report:");
        println!("------------------");

        let mut grouped_errors: HashMap<Term, Vec<String>> = HashMap::new();

        for (context, error_message) in &self.results {
            grouped_errors
                .entry(context.focus_node().clone())
                .or_default()
                .push(error_message.clone());
        }

        for (focus_node, errors) in grouped_errors {
            println!("\nFocus Node: {}", focus_node);
            for error in errors {
                println!("  - Error: {}", error);
                // Optionally, print more context details if needed
                // e.g., println!("    Path: {:?}", context.path());
                // e.g., println!("    Value Nodes: {:?}", context.value_nodes());
            }
        }
        println!("\n------------------");
    }
}
