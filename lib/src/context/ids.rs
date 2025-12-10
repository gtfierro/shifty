use oxigraph::model::Term;
use shacl_ir::{ComponentID, PropShapeID, RuleID, ID};
use std::collections::HashMap;
use std::hash::Hash;

/// Trait exposing the underlying numeric value for an identifier.
pub(crate) trait IdValue {
    fn value(&self) -> u64;
}

impl IdValue for ID {
    fn value(&self) -> u64 {
        self.0
    }
}

impl IdValue for PropShapeID {
    fn value(&self) -> u64 {
        self.0
    }
}

impl IdValue for ComponentID {
    fn value(&self) -> u64 {
        self.0
    }
}

impl IdValue for RuleID {
    fn value(&self) -> u64 {
        self.0
    }
}

/// Lookup table mapping RDF terms to compact numeric identifiers.
pub(crate) struct IDLookupTable<IdType: Copy + Eq + Hash + IdValue> {
    id_map: HashMap<Term, IdType>,
    id_to_term: HashMap<IdType, Term>,
    next_id: u64,
}

impl<IdType: Copy + Eq + Hash + From<u64> + IdValue> IDLookupTable<IdType> {
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

    pub(crate) fn insert(&mut self, term: Term, id: IdType) {
        self.id_map.insert(term.clone(), id);
        self.id_to_term.insert(id, term);
        let next_candidate = id.value().checked_add(1).unwrap_or(id.value());
        if self.next_id < next_candidate {
            self.next_id = next_candidate;
        }
    }
}

impl<IdType: Copy + Eq + Hash + IdValue> IDLookupTable<IdType> {
    pub(crate) fn get(&self, term: &Term) -> Option<IdType> {
        self.id_map.get(term).copied()
    }

    pub(crate) fn get_term(&self, id: IdType) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}
