#![allow(dead_code, unused_variables)]
mod components;
pub mod context;
mod named_nodes;
mod shape;
mod types;

use components::Component;
use shape::Shape;
use std::collections::HashMap;
use types::ID;

pub struct Store {
    shape_lookup: HashMap<ID, Shape>,
    component_lookup: HashMap<ID, Component>,
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
