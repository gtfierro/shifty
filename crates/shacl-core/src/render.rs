use crate::algebra::{ConstraintExpr, PropertyPath, RuleExpr, ShapeKind, ShapeProgram, TargetExpr};

pub fn render_shape_program_dot(program: &ShapeProgram) -> String {
    let mut out = String::from("digraph shacl_core {\n");
    out.push_str("  rankdir=TB;\n");
    out.push_str("  node [shape=box, style=rounded];\n");

    for shape in &program.shapes {
        let kind = match shape.kind {
            ShapeKind::Node => "NodeShape",
            ShapeKind::Property => "PropertyShape",
        };
        let label = escape_label(&format!(
            "{kind}\n{}{}",
            shape.source,
            shape
                .path
                .as_ref()
                .map(|path| format!("\npath: {}", path_label(path)))
                .unwrap_or_default()
        ));
        out.push_str(&format!("  shape_{} [label=\"{}\"];\n", shape.id.0, label));
    }

    for constraint in &program.constraints {
        let label = escape_label(&constraint_label(&constraint.expr));
        out.push_str(&format!(
            "  constraint_{} [shape=ellipse, label=\"{}\"];\n",
            constraint.id.0, label
        ));
        out.push_str(&format!(
            "  shape_{} -> constraint_{} [label=\"constraint\"];\n",
            constraint.owner.0, constraint.id.0
        ));
    }

    for target in &program.targets {
        let label = escape_label(&target_label(&target.expr));
        out.push_str(&format!(
            "  target_{} [shape=diamond, label=\"{}\"];\n",
            target.id.0, label
        ));
        out.push_str(&format!(
            "  shape_{} -> target_{} [label=\"target\"];\n",
            target.owner.0, target.id.0
        ));
    }

    for rule in &program.rules {
        let label = escape_label(&rule_label(&rule.expr));
        out.push_str(&format!(
            "  rule_{} [shape=hexagon, label=\"{}\"];\n",
            rule.id.0, label
        ));
        out.push_str(&format!(
            "  shape_{} -> rule_{} [label=\"rule\"];\n",
            rule.owner.0, rule.id.0
        ));
    }

    for dep in &program.dependencies {
        out.push_str(&format!(
            "  shape_{} -> shape_{} [color=gray50, label=\"{}\"];\n",
            dep.from.0,
            dep.to.0,
            escape_label(&dep.kind)
        ));
    }

    out.push_str("}\n");
    out
}

fn constraint_label(expr: &ConstraintExpr) -> String {
    match expr {
        ConstraintExpr::NodeRef { source, .. } => format!("node {}", source),
        ConstraintExpr::PropertyRef { source, .. } => format!("property {}", source),
        ConstraintExpr::QualifiedValueShape {
            source,
            min_count,
            max_count,
            disjoint,
            ..
        } => format!(
            "qualified {} min={:?} max={:?} disjoint={:?}",
            source, min_count, max_count, disjoint
        ),
        ConstraintExpr::Logical { kind, shapes } => format!("{kind:?} {:?}", shapes),
        ConstraintExpr::Not { source, .. } => format!("not {}", source),
        ConstraintExpr::Class(term) => format!("class {}", term),
        ConstraintExpr::Datatype(term) => format!("datatype {}", term),
        ConstraintExpr::NodeKind(term) => format!("nodeKind {}", term),
        ConstraintExpr::Cardinality {
            predicate,
            min,
            max,
        } => {
            format!("{} min={:?} max={:?}", predicate, min, max)
        }
        ConstraintExpr::NumericRange { predicate, values } => {
            format!("{} {:?}", predicate, values)
        }
        ConstraintExpr::StringConstraint { predicate, values } => {
            format!("{} {:?}", predicate, values)
        }
        ConstraintExpr::PropertyComparison { predicate, values } => {
            format!("{} {:?}", predicate, values)
        }
        ConstraintExpr::Closed { ignored_properties } => {
            format!("closed {:?}", ignored_properties)
        }
        ConstraintExpr::HasValue(term) => format!("hasValue {}", term),
        ConstraintExpr::In(values) => format!("in {:?}", values),
        ConstraintExpr::Sparql { node } => format!("sparql {}", node),
        ConstraintExpr::CustomComponent {
            predicate,
            component,
            values,
        } => format!("custom {} component={:?} {:?}", predicate, component, values),
        ConstraintExpr::GenericPredicate { predicate, values } => {
            format!("{} {:?}", predicate, values)
        }
    }
}

fn target_label(expr: &TargetExpr) -> String {
    match expr {
        TargetExpr::Class(term) => format!("targetClass {}", term),
        TargetExpr::Node(term) => format!("targetNode {}", term),
        TargetExpr::SubjectsOf(term) => format!("targetSubjectsOf {}", term),
        TargetExpr::ObjectsOf(term) => format!("targetObjectsOf {}", term),
        TargetExpr::Advanced {
            node,
            select,
            ask,
            target_shape,
            filter_shape,
        } => format!(
            "advanced {}\nselect={:?}\nask={:?}\ntargetShape={:?}\nfilterShape={:?}",
            node, select, ask, target_shape, filter_shape
        ),
    }
}

fn rule_label(expr: &RuleExpr) -> String {
    match expr {
        RuleExpr::Triple {
            subject,
            predicate,
            object,
            conditions,
            ..
        } => format!(
            "triple\nsubject={:?}\npredicate={:?}\nobject={:?}\nconditions={:?}",
            subject, predicate, object, conditions
        ),
        RuleExpr::Sparql {
            query, conditions, ..
        } => format!("sparql\nquery={:?}\nconditions={:?}", query, conditions),
        RuleExpr::Generic {
            node, conditions, ..
        } => format!("generic {} {:?}", node, conditions),
    }
}

fn path_label(path: &PropertyPath) -> String {
    match path {
        PropertyPath::Predicate(node) => node.to_string(),
        PropertyPath::Inverse(inner) => format!("^({})", path_label(inner)),
        PropertyPath::Sequence(paths) => {
            paths.iter().map(path_label).collect::<Vec<_>>().join(" / ")
        }
        PropertyPath::Alternative(paths) => {
            paths.iter().map(path_label).collect::<Vec<_>>().join(" | ")
        }
        PropertyPath::ZeroOrMore(inner) => format!("({})*", path_label(inner)),
        PropertyPath::OneOrMore(inner) => format!("({})+", path_label(inner)),
        PropertyPath::ZeroOrOne(inner) => format!("({})?", path_label(inner)),
        PropertyPath::Unsupported(term) => format!("unsupported {}", term),
    }
}

fn escape_label(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
