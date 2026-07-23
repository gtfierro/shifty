//! Human-readable rendering of a [`NativeQueryPlan`] (doc §289: `inspect`
//! should expose the native physical plan). Used to explain *how a SPARQL
//! constraint was evaluated* when it lowered to the native subset, as opposed
//! to opaque Spareval fallback execution.
//!
//! The rendering is a flat, indented pipeline in execution order (leaf first),
//! matching the executor's actual left-deep evaluation order (doc §128) rather
//! than a top-down operator tree, since that is the more natural reading for
//! "what pattern did this query check."

use crate::sparql_native::plan::{
    ClosureKind, ExprPlan, GraphScan, NativeOp, NativeQueryPlan, OpId, PathScan, ScanTerm,
    TripleScan,
};
use shifty_algebra::render::path_to_string;

/// Render `plan` as an indented, human-readable pipeline of physical operators,
/// e.g.:
///
/// ```text
/// InputFocus(?this)
/// Scan ?this rdf:type ex:Sensor
/// PathScan ?this +(ex:hasPoint) ?point
/// Filter BOUND(?point)
/// Project [?point]
/// ```
pub fn render_native_plan(plan: &NativeQueryPlan) -> String {
    render_op(plan, plan.root, 0).join("\n")
}

fn pad(depth: usize) -> String {
    "  ".repeat(depth)
}

fn var_name(plan: &NativeQueryPlan, id: u32) -> &str {
    plan.var_names
        .get(id as usize)
        .map(String::as_str)
        .unwrap_or("?")
}

fn render_scan_term(plan: &NativeQueryPlan, t: &ScanTerm) -> String {
    match t {
        ScanTerm::Var(id) => format!("?{}", var_name(plan, *id)),
        ScanTerm::Const(term) => term.to_string(),
    }
}

fn render_graph(g: &GraphScan) -> String {
    match g {
        GraphScan::Default => String::new(),
        GraphScan::Named(n) => format!(" GRAPH {n}"),
    }
}

fn closure_sigil(k: ClosureKind) -> &'static str {
    match k {
        ClosureKind::Star => "*",
        ClosureKind::Plus => "+",
        ClosureKind::Opt => "?",
    }
}

fn render_triple_scan(plan: &NativeQueryPlan, t: &TripleScan) -> String {
    format!(
        "{} {} {}{}",
        render_scan_term(plan, &t.subject),
        render_scan_term(plan, &t.predicate),
        render_scan_term(plan, &t.object),
        render_graph(&t.graph),
    )
}

fn render_path_scan(plan: &NativeQueryPlan, s: &PathScan) -> String {
    format!(
        "{} {}({}) {}{}",
        render_scan_term(plan, &s.subject),
        closure_sigil(s.kind),
        path_to_string(&s.step),
        render_scan_term(plan, &s.object),
        render_graph(&s.graph),
    )
}

fn render_expr(plan: &NativeQueryPlan, e: &ExprPlan, depth: usize) -> String {
    match e {
        ExprPlan::Var(id) => format!("?{}", var_name(plan, *id)),
        ExprPlan::Const(t) => t.to_string(),
        ExprPlan::Bound(id) => format!("BOUND(?{})", var_name(plan, *id)),
        ExprPlan::Not(inner) => format!("!({})", render_expr(plan, inner, depth)),
        ExprPlan::And(l, r) => format!(
            "({} && {})",
            render_expr(plan, l, depth),
            render_expr(plan, r, depth)
        ),
        ExprPlan::Or(l, r) => format!(
            "({} || {})",
            render_expr(plan, l, depth),
            render_expr(plan, r, depth)
        ),
        ExprPlan::SameTerm(l, r) => format!(
            "sameTerm({}, {})",
            render_expr(plan, l, depth),
            render_expr(plan, r, depth)
        ),
        ExprPlan::Str(inner) => format!("STR({})", render_expr(plan, inner, depth)),
        ExprPlan::StrStarts(l, r) => format!(
            "STRSTARTS({}, {})",
            render_expr(plan, l, depth),
            render_expr(plan, r, depth)
        ),
        ExprPlan::Equal(l, r) => format!(
            "({} = {})",
            render_expr(plan, l, depth),
            render_expr(plan, r, depth)
        ),
        ExprPlan::Exists(op) => {
            let inner = render_op(plan, *op, depth + 1).join("\n");
            format!("EXISTS {{\n{inner}\n{}}}", pad(depth))
        }
    }
}

/// Render the pipeline rooted at `id`, in execution order (the operator's own
/// input is rendered first), one line per operator at `depth` indentation.
fn render_op(plan: &NativeQueryPlan, id: OpId, depth: usize) -> Vec<String> {
    let p = pad(depth);
    match &plan.nodes[id as usize] {
        NativeOp::InputFocus => vec![format!(
            "{p}InputFocus(?{})",
            var_name(plan, plan.focus_var)
        )],
        NativeOp::Scan { input, pattern } => {
            let mut lines = render_op(plan, *input, depth);
            lines.push(format!("{p}Scan {}", render_triple_scan(plan, pattern)));
            lines
        }
        NativeOp::PathScan { input, scan } => {
            let mut lines = render_op(plan, *input, depth);
            lines.push(format!("{p}PathScan {}", render_path_scan(plan, scan)));
            lines
        }
        NativeOp::Union { left, right } => {
            let mut lines = vec![format!("{p}Union")];
            lines.extend(render_op(plan, *left, depth + 1));
            lines.extend(render_op(plan, *right, depth + 1));
            lines
        }
        NativeOp::Filter { input, expr } => {
            let mut lines = render_op(plan, *input, depth);
            lines.push(format!("{p}Filter {}", render_expr(plan, expr, depth)));
            lines
        }
        NativeOp::Extend { input, var, expr } => {
            let mut lines = render_op(plan, *input, depth);
            lines.push(format!(
                "{p}Bind ?{} = {}",
                var_name(plan, *var),
                render_expr(plan, expr, depth)
            ));
            lines
        }
        NativeOp::Project { input, vars } => {
            let mut lines = render_op(plan, *input, depth);
            let names: Vec<String> = vars
                .iter()
                .map(|v| format!("?{}", var_name(plan, *v)))
                .collect();
            lines.push(format!("{p}Project [{}]", names.join(", ")));
            lines
        }
        NativeOp::Distinct { input } => {
            let mut lines = render_op(plan, *input, depth);
            lines.push(format!("{p}Distinct"));
            lines
        }
    }
}
