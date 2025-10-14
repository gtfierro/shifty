use oxigraph::model::Term;
use std::collections::HashMap;
use std::hash::Hash;

/// Lookup table mapping RDF terms to compact numeric identifiers.
pub(crate) struct IDLookupTable<IdType: Copy + Eq + Hash> {
    id_map: HashMap<Term, IdType>,
    id_to_term: HashMap<IdType, Term>,
    next_id: u64,
}

impl<IdType: Copy + Eq + Hash + From<u64>> IDLookupTable<IdType> {
    pub(crate) fn new() -> Self {
        Self {
            id_map: HashMap::new(),
            id_to_term: HashMap::new(),
            next_id: 0,
        }
    }

    pub(crate) fn get_or_create_id(&mut self, term: Term) -> IdType {
        if let Some(&id) = self.id_map.get(&term) {
            id
        } else {
            let id_val = self.next_id;
            let id: IdType = id_val.into();
            self.id_map.insert(term.clone(), id);
            self.id_to_term.insert(id, term);
            self.next_id += 1;
            id
        }
    }

    pub(crate) fn get(&self, term: &Term) -> Option<IdType> {
        self.id_map.get(term).copied()
    }

    pub(crate) fn get_term(&self, id: IdType) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}
