use crate::types::{ComponentID, PropShapeID, ID};
use shacl_ir::{ComponentDescriptor, ShapeIR};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ShapeRef {
    Node(ID),
    Property(PropShapeID),
}

#[derive(Debug)]
pub(crate) struct ShapeTreePlan {
    pub(crate) shapes: Vec<ShapeRef>,
}

#[derive(Debug, Default)]
pub(crate) struct ValidationPlan {
    pub(crate) trees: Vec<ShapeTreePlan>,
}

pub(crate) fn build_validation_plan(shape_ir: &ShapeIR) -> ValidationPlan {
    let mut all_shapes: HashSet<ShapeRef> = HashSet::new();
    let mut adjacency: HashMap<ShapeRef, Vec<ShapeRef>> = HashMap::new();
    let mut undirected: HashMap<ShapeRef, HashSet<ShapeRef>> = HashMap::new();

    let mut constraint_owner: HashMap<ComponentID, ShapeRef> = HashMap::new();

    for node in &shape_ir.node_shapes {
        let owner = ShapeRef::Node(node.id);
        push_shape(&mut all_shapes, &mut adjacency, &mut undirected, owner);
        for cid in &node.constraints {
            constraint_owner.insert(*cid, owner);
        }
    }

    for prop in &shape_ir.property_shapes {
        let owner = ShapeRef::Property(prop.id);
        push_shape(&mut all_shapes, &mut adjacency, &mut undirected, owner);
        for cid in &prop.constraints {
            constraint_owner.insert(*cid, owner);
        }
    }

    for (cid, descriptor) in &shape_ir.components {
        if let Some(&owner) = constraint_owner.get(cid) {
            add_descriptor_dependencies(
                descriptor,
                owner,
                &mut all_shapes,
                &mut adjacency,
                &mut undirected,
            );
        }
    }

    let topo_order = topological_sort(&all_shapes, &adjacency);
    let tree_assignments = build_tree_assignments(&all_shapes, &undirected);
    let num_trees = tree_assignments
        .values()
        .copied()
        .max()
        .map(|max_id| max_id + 1)
        .unwrap_or(0);
    let mut trees: Vec<ShapeTreePlan> = (0..num_trees)
        .map(|_| ShapeTreePlan { shapes: Vec::new() })
        .collect();

    for shape in topo_order {
        if let Some(&tree_id) = tree_assignments.get(&shape) {
            if let Some(tree) = trees.get_mut(tree_id) {
                tree.shapes.push(shape);
            }
        }
    }

    ValidationPlan { trees }
}

fn push_shape(
    all_shapes: &mut HashSet<ShapeRef>,
    adjacency: &mut HashMap<ShapeRef, Vec<ShapeRef>>,
    undirected: &mut HashMap<ShapeRef, HashSet<ShapeRef>>,
    shape: ShapeRef,
) {
    all_shapes.insert(shape);
    adjacency.entry(shape).or_default();
    undirected.entry(shape).or_default();
}

fn add_descriptor_dependencies(
    descriptor: &ComponentDescriptor,
    owner: ShapeRef,
    all_shapes: &mut HashSet<ShapeRef>,
    adjacency: &mut HashMap<ShapeRef, Vec<ShapeRef>>,
    undirected: &mut HashMap<ShapeRef, HashSet<ShapeRef>>,
) {
    let mut add_dep = |dependency: ShapeRef| {
        push_shape(all_shapes, adjacency, undirected, dependency);
        push_shape(all_shapes, adjacency, undirected, owner);
        adjacency.entry(dependency).or_default().push(owner);
        undirected.entry(dependency).or_default().insert(owner);
        undirected.entry(owner).or_default().insert(dependency);
    };

    match descriptor {
        ComponentDescriptor::Node { shape } => {
            add_dep(ShapeRef::Node(*shape));
        }
        ComponentDescriptor::Property { shape } => {
            add_dep(ShapeRef::Property(*shape));
        }
        ComponentDescriptor::QualifiedValueShape { shape, .. } => {
            add_dep(ShapeRef::Node(*shape));
        }
        ComponentDescriptor::Not { shape } => {
            add_dep(ShapeRef::Node(*shape));
        }
        ComponentDescriptor::And { shapes }
        | ComponentDescriptor::Or { shapes }
        | ComponentDescriptor::Xone { shapes } => {
            for dep in shapes {
                add_dep(ShapeRef::Node(*dep));
            }
        }
        _ => {}
    }
}

fn topological_sort(
    all_shapes: &HashSet<ShapeRef>,
    adjacency: &HashMap<ShapeRef, Vec<ShapeRef>>,
) -> Vec<ShapeRef> {
    let mut indegree: HashMap<ShapeRef, usize> =
        all_shapes.iter().map(|shape| (*shape, 0)).collect();
    for neighbors in adjacency.values() {
        for neighbor in neighbors {
            if let Some(entry) = indegree.get_mut(neighbor) {
                *entry += 1;
            }
        }
    }

    let mut queue: VecDeque<ShapeRef> = indegree
        .iter()
        .filter(|(_, &count)| count == 0)
        .map(|(&shape, _)| shape)
        .collect();
    let mut order = Vec::new();

    while let Some(shape) = queue.pop_front() {
        order.push(shape);
        if let Some(neighbors) = adjacency.get(&shape) {
            for &neighbor in neighbors {
                if let Some(entry) = indegree.get_mut(&neighbor) {
                    *entry -= 1;
                    if *entry == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
    }

    if order.len() != all_shapes.len() {
        for &shape in all_shapes {
            if !order.contains(&shape) {
                order.push(shape);
            }
        }
    }

    order
}

fn build_tree_assignments(
    all_shapes: &HashSet<ShapeRef>,
    undirected: &HashMap<ShapeRef, HashSet<ShapeRef>>,
) -> HashMap<ShapeRef, usize> {
    let mut assignments: HashMap<ShapeRef, usize> = HashMap::new();
    let mut current_id = 0;

    for &shape in all_shapes {
        if assignments.contains_key(&shape) {
            continue;
        }
        let mut stack = vec![shape];
        while let Some(node) = stack.pop() {
            if assignments.contains_key(&node) {
                continue;
            }
            assignments.insert(node, current_id);
            if let Some(neighbors) = undirected.get(&node) {
                for &neighbor in neighbors {
                    if !assignments.contains_key(&neighbor) {
                        stack.push(neighbor);
                    }
                }
            }
        }
        current_id += 1;
    }

    assignments
}
