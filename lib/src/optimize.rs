use crate::context::ValidationContext;

pub struct Optimizer {
    pub ctx: ValidationContext,
}

impl Optimizer {
    pub fn optimize(ctx: ValidationContext) -> Self {
        Optimizer { ctx }
    }

    pub fn validation_context(&self) -> &ValidationContext {
        &self.validation_context
    }

    // Add methods for optimization logic here
    fn remove_unreachable_targets(&self) -> Result<(), String> {
        // run a query on  ctx.data_graph to figure out what types of things there are:
        // SELECT DISTINCT ?type WHERE { ?thing rdf:type/rdfs:subClassOf* ?type . }
        // make a hashset of these types
        // Then remove all TargetClasses from nodeshapes where their class does not exist in this
        // hashset.
        let query = "SELECT DISTINCT ?type WHERE { ?thing rdf:type/rdfs:subClassOf* ?type . }";
        let mut types = std::collections::HashSet::new();

        Ok(())
    }
}
