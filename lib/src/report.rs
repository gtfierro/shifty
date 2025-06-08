use crate::context::Context;

pub struct ValidationReportBuilder {}

impl ValidationReportBuilder {
    pub fn new() -> Self {
        ValidationReportBuilder {}
    }

    pub fn add_error(&mut self, context: &Context, error: String) {
        println!("Error in context {:?}: {}", context, error);
    }
}
