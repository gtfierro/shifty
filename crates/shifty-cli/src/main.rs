//! CLI for the formalism-first SHACL engine.
//!
//! `inspect` visualizes how a shapes graph is transformed through the layers,
//! one `--stage` at a time. As later layers land (normalized, planned, …), they
//! become additional stages here.

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::path::PathBuf;
use std::process::ExitCode;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(name = "shacl", about = "Formalism-first SHACL/SHACL-AF engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the shifty CLI version.
    Version,
    /// Show a layer's view of a shapes graph.
    Inspect(InspectArgs),
    /// Validate a data graph against a shapes graph (normalized planned evaluator).
    Validate(ValidateArgs),
    /// Run SHACL-AF rule inference (forward chaining to a fixpoint).
    Infer(InferArgs),
    /// Show symbolic-repair structures for a data graph's violations.
    Repair(RepairArgs),
}

#[derive(Args)]
struct RepairArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Which repair structure to print.
    #[arg(long, value_enum, default_value_t = RepairStage::Tree)]
    stage: RepairStage,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Skip SHACL-AF rule inference before witnessing.
    #[arg(long)]
    no_infer: bool,
    /// Run the fixpoint driver and emit the repaired data graph (N-Triples)
    /// instead of inspecting structures. Overrides `--stage`.
    #[arg(long)]
    apply: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum RepairStage {
    /// The witness tree per failing focus node (why each violates).
    Witness,
    /// The synthesized RepairTree per failing focus node (how to fix it).
    Tree,
    /// A concrete repair (ΔG) the enumeration driver finds for each focus.
    Solve,
}

#[derive(Args)]
struct InferArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Print shape, cache, and SPARQL execution telemetry after inference.
    #[arg(long)]
    profile: bool,
}

#[derive(Args)]
struct ValidateArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Emit a W3C `sh:ValidationReport` graph (N-Triples) instead of a summary.
    #[arg(long)]
    report: bool,
    /// Skip SHACL-AF rule inference before validation.
    #[arg(long)]
    no_infer: bool,
    /// RDF graph scope used during validation.
    #[arg(long, visible_alias = "graph-scope", value_enum, default_value_t = GraphMode::Union)]
    graph_mode: GraphMode,
    /// Named shape IRI to use as a validation entry point (repeatable). When
    /// omitted, every target-bearing shape is used.
    #[arg(long = "shape-name", visible_alias = "entry-shape", value_name = "IRI", action = clap::ArgAction::Append)]
    entry_shape_names: Vec<String>,
    /// Lowest result severity that makes validation non-conforming.
    #[arg(long, value_enum, default_value_t = SeverityLevel::Info)]
    minimum_severity: SeverityLevel,
    /// Print shape, cache, and SPARQL execution telemetry after validation.
    #[arg(long)]
    profile: bool,
}

#[derive(Args)]
struct InspectArgs {
    /// Turtle shapes file.
    file: PathBuf,
    /// Which layer's representation to print.
    #[arg(long, value_enum, default_value_t = Stage::Algebra)]
    stage: Stage,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Stage {
    /// The raw parsed RDF triples (input to lowering).
    Rdf,
    /// The lowered formalism IR (Layer 2 output).
    Algebra,
    /// The normalized IR (Layer 4: CSE + simplification).
    Normalized,
    /// The recursion/stratification analysis (Layer 4).
    Strata,
    /// The physical plan (Layer 5: focus sources + cost-ordered checks).
    Plan,
    /// SPARQL capability classification: which constraint queries lower to the
    /// native executor vs. fall back to Spareval.
    Capability,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
    /// Graphviz DOT (algebra-ast stage only).
    Dot,
}

#[derive(Clone, Copy, ValueEnum)]
enum GraphMode {
    /// Focus nodes and evaluation use only the data graph.
    Data,
    /// Focus nodes come from data; evaluation uses data + shapes.
    Union,
    /// Focus nodes and evaluation both use data + shapes.
    UnionAll,
}

#[derive(Clone, Copy, ValueEnum)]
enum SeverityLevel {
    Info,
    Warning,
    Violation,
}

impl From<SeverityLevel> for shifty_algebra::Severity {
    fn from(value: SeverityLevel) -> Self {
        match value {
            SeverityLevel::Info => Self::Info,
            SeverityLevel::Warning => Self::Warning,
            SeverityLevel::Violation => Self::Violation,
        }
    }
}

impl From<GraphMode> for shifty_engine::ValidationGraphMode {
    fn from(mode: GraphMode) -> Self {
        match mode {
            GraphMode::Data => Self::Data,
            GraphMode::Union => Self::Union,
            GraphMode::UnionAll => Self::UnionAll,
        }
    }
}

fn main() -> ExitCode {
    env_logger::init();
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Command::Version => {
            println!("{VERSION}");
            Ok(())
        }
        Command::Inspect(args) => inspect(args),
        Command::Validate(args) => validate(args),
        Command::Infer(args) => infer(args),
        Command::Repair(args) => repair(args),
    }
}

struct SourceBytes {
    bytes: Vec<u8>,
    content_type: Option<String>,
}

fn fetch_bytes(src: &str) -> Result<SourceBytes, Box<dyn Error>> {
    if src.starts_with("http://") || src.starts_with("https://") {
        let response = ureq::get(src).call()?;
        let content_type = response.header("content-type").map(ToOwned::to_owned);
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut response.into_reader(), &mut bytes)?;
        Ok(SourceBytes {
            bytes,
            content_type,
        })
    } else {
        Ok(SourceBytes {
            bytes: std::fs::read(src)?,
            content_type: None,
        })
    }
}

fn load_sources(
    sources: &[String],
    base: Option<&str>,
) -> Result<shifty_parse::Loaded, Box<dyn Error>> {
    let mut merged: Option<shifty_parse::Loaded> = None;
    for src in sources {
        let fetched = fetch_bytes(src)?;
        let parsed_base = base.or_else(|| {
            (src.starts_with("http://") || src.starts_with("https://")).then_some(src.as_str())
        });
        let loaded = shifty_parse::load_rdf_auto(
            &fetched.bytes,
            fetched.content_type.as_deref(),
            Some(src),
            parsed_base,
        )?;
        match merged.as_mut() {
            None => merged = Some(loaded),
            Some(m) => m.merge_from(&loaded),
        }
    }
    merged.ok_or_else(|| "no sources provided".into())
}

fn infer(args: InferArgs) -> Result<(), Box<dyn Error>> {
    if args.profile {
        shifty_engine::profile::enable();
    }
    let base = args.base.as_deref();
    let shapes = load_sources(&args.shapes, base)?;
    let parsed = shifty_parse::parse_loaded(&shapes);
    parsed.require_valid()?;
    let normalized = shifty_opt::normalize(&parsed.schema);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }

    let outcome = if args.data.is_empty() {
        shifty_engine::infer(&shapes.graph, &normalized)
    } else {
        let data = load_sources(&args.data, base)?;
        shifty_engine::infer_graphs(&data.graph, &shapes.graph, &normalized)
    };
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => return Err(format!("{e}; cannot infer (see `inspect --stage strata`)").into()),
    };
    for d in &outcome.diagnostics {
        eprintln!("warning: {d}");
    }

    match args.format {
        Format::Dot => return Err("--format dot is not supported for infer".into()),
        Format::Json => {
            let triples: Vec<_> = outcome
                .inferred
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "subject": t.subject.to_string(),
                        "predicate": t.predicate.to_string(),
                        "object": t.object.to_string(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&triples)?);
        }
        Format::Text => {
            println!("inferred {} triple(s):", outcome.inferred.len());
            let mut lines: Vec<String> = outcome.inferred.iter().map(|t| t.to_string()).collect();
            lines.sort();
            for line in lines {
                println!("  {line}");
            }
        }
    }
    if args.profile
        && let Some(col) = shifty_engine::profile::take()
    {
        col.print_summary();
    }
    Ok(())
}

/// The validation outcome as JSON, enriched so a consumer never has to hold the
/// schema to make sense of it.
///
/// The engine's own serialization is kept verbatim — new engine fields flow
/// through without touching this — and three things are layered on:
/// `definition`/`definition_pretty` per reason (the constraint in words, so a
/// reader needs no arena at all), `target`/`shape_name` per violation (which the
/// text report already showed), and a top-level `shapes` map.
///
/// `shapes` is the transitive closure of every reported constraint, keyed by the
/// same ids that `constraint_id` and the algebra's own `qualifier`/child fields
/// use, so those pointers resolve. It is deliberately *not* the whole arena: for
/// the s223 shapes that is 2412 slots against the 19 a report actually reaches,
/// and a reader who wants all of them has `inspect --stage plan --format json`.
fn json_report(
    outcome: &shifty_engine::ValidationOutcome,
    schema: &shifty_algebra::Schema,
    plan: &shifty_opt::PhysicalPlan,
    px: &shifty_algebra::Prefixes,
) -> Result<serde_json::Value, Box<dyn Error>> {
    use serde_json::{Map, Value, json};
    use shifty_algebra::render::{PRETTY_WIDTH, describe_shape_in, describe_shape_pretty};

    let mut doc = serde_json::to_value(outcome)?;
    let mut referenced: std::collections::BTreeSet<u32> = Default::default();

    if let Some(violations) = doc.get_mut("violations").and_then(Value::as_array_mut) {
        for (value, v) in violations.iter_mut().zip(&outcome.violations) {
            let Some(object) = value.as_object_mut() else {
                continue;
            };
            if let Some(statement) = schema.statements.get(v.statement) {
                object.insert(
                    "target".into(),
                    json!(shifty_algebra::render::selector_to_string_in_px(
                        &statement.selector,
                        &schema.arena,
                        &schema.prefixes
                    )),
                );
                if let Some(name) = schema.name_of(statement.shape) {
                    object.insert("shape_name".into(), json!(name));
                }
            }
            let reasons = object
                .get_mut("reasons")
                .and_then(Value::as_array_mut)
                .map(|r| r.iter_mut().zip(&v.reasons));
            for (value, r) in reasons.into_iter().flatten() {
                let Some(object) = value.as_object_mut() else {
                    continue;
                };
                object.insert(
                    "definition".into(),
                    json!(describe_shape_in(&plan.arena, r.constraint_id, px)),
                );
                object.insert(
                    "definition_pretty".into(),
                    json!(describe_shape_pretty(
                        &plan.arena,
                        r.constraint_id,
                        px,
                        PRETTY_WIDTH
                    )),
                );
                collect_shapes(&plan.arena, r.constraint_id, &mut referenced);
            }
        }
    }

    let shapes: Map<String, Value> = referenced
        .iter()
        .map(|id| {
            Ok((
                id.to_string(),
                serde_json::to_value(plan.arena.get(shifty_algebra::ShapeId(*id)))?,
            ))
        })
        .collect::<Result<_, serde_json::Error>>()?;
    if let Some(object) = doc.as_object_mut() {
        object.insert("shapes".into(), Value::Object(shapes));
    }
    Ok(doc)
}

/// Every arena slot reachable from `id`, itself included. Uses the arena's own
/// child links, so a shape referenced only through a `sh:filterShape` inside a
/// node expression is collected too.
fn collect_shapes(
    arena: &shifty_algebra::ShapeArena,
    id: shifty_algebra::ShapeId,
    out: &mut std::collections::BTreeSet<u32>,
) {
    if !out.insert(id.0) {
        return;
    }
    for child in arena.get(id).child_shapes() {
        collect_shapes(arena, child, out);
    }
}

/// The symbols a report can use, and what each means. Printed at the end of a
/// text report, but only for the ones that actually appear: a key for notation
/// the reader never met is noise, and the common report uses none of it.
const NOTATION: &[(&str, &str, &str)] = &[
    (
        "∀",
        "∀ p . X",
        "every value along p satisfies X (holds when there are none)",
    ),
    (
        "∃[",
        "∃[m..n] p . X",
        "between m and n values along p satisfy X",
    ),
    ("∄", "∄ p", "no values along p at all"),
    // `^^` is a typed literal's datatype separator, not an inverse path, and
    // matching it would gloss notation the report never used.
    ("^", "^p", "p followed backwards, from object to subject"),
    ("*", "p*", "p repeated zero or more times"),
];

fn notation_key(lines: &[String]) -> Vec<String> {
    let used: Vec<(&str, &str)> = NOTATION
        .iter()
        .filter(|(symbol, ..)| {
            lines
                .iter()
                .any(|line| line.replace("^^", "").contains(symbol))
        })
        .map(|(_, form, gloss)| (*form, *gloss))
        .collect();
    if used.is_empty() {
        return Vec::new();
    }
    let width = used
        .iter()
        .map(|(form, _)| form.chars().count())
        .max()
        .unwrap_or(0);
    let mut out = vec![String::new(), "notation".to_string()];
    out.extend(used.into_iter().map(|(form, gloss)| {
        let padding = " ".repeat(width - form.chars().count());
        format!("  {form}{padding}   {gloss}")
    }));
    out
}

/// `1 violation` / `2 violations`. A counted noun in a summary line is read, not
/// parsed, so `violation(s)` is a small tax on every reader.
fn plural(n: usize, word: &str) -> String {
    if n == 1 {
        format!("{n} {word}")
    } else {
        format!("{n} {word}s")
    }
}

/// One thing wrong with the graph, and every node it is wrong on.
///
/// Reasons that fail the same statement with the same rendered explanation are
/// the same finding: the constraint, the message and the requirement are
/// identical, and only the nodes differ.
struct Finding {
    /// The `(selector, shape)` statement this came from. Two findings sharing it
    /// are two parts of one authored shape.
    statement: usize,
    target: String,
    severity: String,
    shape: Option<String>,
    /// The shared explanation — the reason block, already rendered.
    body: Vec<String>,
    /// `(focus node, value node)`, one per grouped reason. The value is the node
    /// reached from the focus along the path, absent when the constraint failed
    /// on the focus node itself.
    members: Vec<(String, Option<String>)>,
}

/// The other findings from the same authored shape that these same nodes also
/// fail, as `Finding 5` / `Finding 5 (2 of 53 nodes)`.
///
/// Grouping by reason splits one shape's parts into separate findings, which is
/// right — they are separate problems — but leaves a reader with no sign that
/// two of them came from one `sh:and`. Only same-statement siblings are
/// reported: in a large graph nearly every pair of findings shares some node,
/// and saying so would be noise rather than a relationship.
fn related_findings(index: usize, findings: &[Finding]) -> String {
    let current = &findings[index];
    let mine: BTreeSet<&str> = current.members.iter().map(|(f, _)| f.as_str()).collect();
    let mut parts = Vec::new();
    for (other_index, other) in findings.iter().enumerate() {
        if other_index == index || other.statement != current.statement {
            continue;
        }
        let shared = other
            .members
            .iter()
            .filter(|(focus, _)| mine.contains(focus.as_str()))
            .count();
        if shared == 0 {
            continue;
        }
        let label = format!("Finding {}", other_index + 1);
        parts.push(if shared == mine.len() {
            label
        } else {
            format!("{label} ({shared} of {} nodes)", mine.len())
        });
    }
    parts.join(", ")
}

/// Who a finding is wrong on.
///
/// A single node reads inline as labelled fields. Several get a counted list;
/// when the constraint failed on a value reached from the node rather than on
/// the node itself, the heading says so and each line carries that value — a
/// bare parenthesised IRI leaves the reader guessing what it is.
fn render_affected(members: &[(String, Option<String>)]) -> Vec<String> {
    if let [(focus, value)] = members {
        let mut out = field(2, "affects", focus);
        // Said either way. A missing line would leave the reader to infer that
        // the constraint applied to the focus node itself.
        out.extend(field(
            2,
            "value node",
            value.as_deref().unwrap_or("(the focus node itself)"),
        ));
        return out;
    }
    let counted = plural(members.len(), "focus node");
    let any_values = members.iter().any(|(_, value)| value.is_some());
    let mut out = field(
        2,
        "affects",
        &if any_values {
            format!("{counted}, each with the value node that failed")
        } else {
            format!("{counted}; the constraint applies to each node itself")
        },
    );
    // Pad the focus column so the values line up, but never so far that one long
    // IRI pushes every value off the edge.
    let column = members
        .iter()
        .filter(|(_, value)| value.is_some())
        .map(|(focus, _)| focus.chars().count())
        .filter(|width| *width <= 56)
        .max()
        .unwrap_or(0);
    out.extend(members.iter().map(|(focus, value)| match value {
        Some(value) => {
            let padding = " ".repeat(column.saturating_sub(focus.chars().count()));
            format!("    {focus}{padding}   {value}")
        }
        None => format!("    {focus}"),
    }));
    out
}

/// Column the labelled values start at. Wide enough for the longest label, so
/// values line up into a column a reader can scan without reading the labels.
const LABEL_WIDTH: usize = 13;

/// One `label   value` line, or several when the value has to wrap. Wrapped
/// lines hang to the value column so the label column stays clean.
fn field(indent: usize, label: &str, value: &str) -> Vec<String> {
    let pad = " ".repeat(indent);
    let hang = " ".repeat(indent + LABEL_WIDTH);
    let width = shifty_algebra::render::PRETTY_WIDTH;
    let mut out = Vec::new();
    for (i, line) in wrap(value, width.saturating_sub(indent + LABEL_WIDTH))
        .into_iter()
        .enumerate()
    {
        if i == 0 {
            out.push(format!("{pad}{label:<LABEL_WIDTH$}{line}"));
        } else {
            out.push(format!("{hang}{line}"));
        }
    }
    if out.is_empty() {
        out.push(format!("{pad}{label}"));
    }
    out
}

/// Wrap on spaces. A token longer than `width` — an IRI, usually — is left
/// over-long rather than split: a broken IRI is not copy-pastable, which is most
/// of what a reader wants one for.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for word in text.split_whitespace() {
        match out.last_mut() {
            Some(line) if line.chars().count() + 1 + word.chars().count() <= width => {
                line.push(' ');
                line.push_str(word);
            }
            _ => out.push(word.to_string()),
        }
    }
    out
}

/// A reason as labelled fields.
///
/// The labels exist because the two nodes in a reason are easy to confuse: the
/// focus node is what was selected for checking, the value node is what was
/// reached from it along the path and actually failed. Unlabelled, a reader
/// meeting a report for the first time reads the value node as the subject.
fn render_reason(
    r: &shifty_engine::Reason,
    arena: &shifty_algebra::ShapeArena,
    px: &shifty_algebra::Prefixes,
    focus: &str,
    violation_severity: &str,
    indent: usize,
    // When set, the reason's own value node is captured here instead of printed:
    // the caller is grouping by explanation and lists the nodes together.
    // Sub-reasons always print theirs, since which value took which `sh:or`
    // branch is part of that branch's explanation.
    hoist_value: Option<&mut Option<String>>,
) -> Vec<String> {
    let requirement = describe_requirement(r, arena, px, indent + LABEL_WIDTH);
    // A cardinality reason's requirement already says everything its generated
    // message says — the bound is in the constraint, the count is on the `found`
    // line — so printing both restates the same sentence twice. Anything else
    // keeps it: the generated message often names specifics the constraint does
    // not, such as which predicates a `closed` shape did not expect.
    let mut lines = Vec::new();

    // Severity only when it differs from the violation's, which is the max of
    // its reasons; repeating the same word on every line is noise.
    if r.severity.to_string() != violation_severity {
        lines.extend(field(indent, "severity", &r.severity.to_string()));
    }
    if let Some(author) = &r.author_message {
        lines.extend(field(indent, "message", author));
    }
    // The generated message is a prose paraphrase of the labelled fields below.
    // Print it only when it still adds something: with no author message and no
    // restatement it is the only prose the reason has, but for a cardinality
    // failure `found` and `requirement` already say it — and say it better, since
    // the paraphrase inlines the whole description onto one line.
    // `message` is the shape author's sentence about intent; `failure` is what
    // went wrong here. Always both: a field that appears only when some rule
    // decides it is not redundant makes the reader work out why it is missing,
    // and an explanation that is complete every time is worth a repeated line.
    lines.extend(field(indent, "failure", &r.message));
    if let Some(path) = &r.path {
        lines.extend(field(indent, "path", path));
    }
    let value = shifty_algebra::render::term_to_string_in(&r.value, px);
    match hoist_value {
        Some(slot) if value != focus => *slot = Some(value),
        Some(_) => {}
        None if value != focus => lines.extend(field(indent, "value node", &value)),
        None => {}
    }
    if let Some(found) = found_line(r, arena) {
        lines.extend(field(indent, "found", &found));
    }
    if let Some(requirement) = requirement {
        lines.extend(labelled_block(indent, "requirement", &requirement));
    }
    if let Some(d) = &r.sparql_diagnostic {
        lines.extend(render_sparql_diagnostic(d, indent + 2));
    }
    for (i, sub) in r.sub_reasons.iter().enumerate() {
        lines.push(String::new());
        lines.push(format!(
            "{}or-branch {} of {} (satisfying any one of these fixes it)",
            " ".repeat(indent + 2),
            i + 1,
            r.sub_reasons.len()
        ));
        lines.extend(render_reason(
            sub,
            arena,
            px,
            focus,
            violation_severity,
            indent + 4,
            None,
        ));
    }
    lines
}

/// A value that may be several lines: the label leads the first, the rest are
/// indented under it.
fn labelled_block(indent: usize, label: &str, value: &str) -> Vec<String> {
    let mut lines = value.lines();
    let pad = " ".repeat(indent);
    let hang = " ".repeat(indent + LABEL_WIDTH);
    let mut out = match lines.next() {
        Some(first) => vec![format!("{pad}{label:<LABEL_WIDTH$}{first}")],
        None => return Vec::new(),
    };
    out.extend(lines.map(|line| format!("{hang}{line}")));
    out
}

/// What the constraint demands, laid out over several lines when it is nested.
fn describe_requirement(
    r: &shifty_engine::Reason,
    arena: &shifty_algebra::ShapeArena,
    px: &shifty_algebra::Prefixes,
    indent: usize,
) -> Option<String> {
    use shifty_algebra::render::{PRETTY_WIDTH, describe_shape_pretty};
    let width = PRETTY_WIDTH.saturating_sub(indent);
    let text = describe_shape_pretty(arena, r.constraint_id, px, width);
    (!text.is_empty()).then_some(text)
}

/// The count the algebra does not carry, paired with the bound it missed.
///
/// A plain `sh:minCount`/`sh:maxCount` counts everything along the path; a
/// qualified count only counts values satisfying the qualifier. Saying which is
/// the difference between "found 0" meaning the path was empty and it meaning
/// the path held values that did not match.
fn found_line(r: &shifty_engine::Reason, arena: &shifty_algebra::ShapeArena) -> Option<String> {
    let found = r.observed_count?;
    let shifty_algebra::Shape::Count {
        min,
        max,
        qualifier,
        ..
    } = &r.constraint
    else {
        return Some(format!("{found} value(s)"));
    };
    let counted = match arena.get(*qualifier) {
        shifty_algebra::Shape::Top | shifty_algebra::Shape::Pending => {
            format!("{found} value(s) along the path")
        }
        _ => format!("{found} value(s) matching the requirement"),
    };
    let bound = match (min, max) {
        (Some(m), _) if found < *m => format!("; at least {m} required"),
        (_, Some(x)) if found > *x => format!("; at most {x} allowed"),
        _ => String::new(),
    };
    Some(format!("{counted}{bound}"))
}

/// Render a [`shifty_engine::SparqlDiagnostic`]: the query that ran, what it
/// was bound to, and what rows it actually returned — so a SPARQL constraint
/// failure is never just "not satisfied."
fn render_sparql_diagnostic(d: &shifty_engine::SparqlDiagnostic, indent: usize) -> Vec<String> {
    let pad = " ".repeat(indent);
    let mut lines = vec![format!("{pad}SPARQL:")];
    lines.push(format!("{pad}  Query:"));
    for line in d.query.lines() {
        lines.push(format!("{pad}    {line}"));
    }
    if !d.bindings.is_empty() {
        lines.push(format!("{pad}  Bound:"));
        for (k, v) in &d.bindings {
            lines.push(format!("{pad}    ${k} = {v}"));
        }
    }
    if !d.results.is_empty() {
        lines.push(format!("{pad}  Results:"));
        for (i, row) in d.results.iter().enumerate() {
            if row.is_empty() {
                lines.push(format!("{pad}    [{}] (no projected variables)", i + 1));
                continue;
            }
            let cols = row
                .iter()
                .map(|(k, v)| format!("?{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{pad}    [{}] {cols}", i + 1));
        }
    }
    if let Some(reason) = &d.fallback_reason {
        lines.push(format!("{pad}  Did not use the native executor: {reason}"));
    }
    lines
}

fn validate(args: ValidateArgs) -> Result<(), Box<dyn Error>> {
    if args.profile {
        shifty_engine::profile::enable();
    }
    let base = args.base.as_deref();
    let shapes_loaded = load_sources(&args.shapes, base)?;
    if shapes_loaded.graph.is_empty() {
        return Err("explicit shapes graph is empty".into());
    }
    let parsed = shifty_parse::parse_loaded(&shapes_loaded);
    parsed.require_valid()?;
    let normalized = shifty_opt::normalize(&parsed.schema);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }
    let graph_mode = args.graph_mode.into();
    let threshold: shifty_algebra::Severity = args.minimum_severity.into();
    let validation_options = shifty_engine::ValidationOptions {
        minimum_severity: threshold.clone(),
        sort_results: true,
        entry_shape_names: args.entry_shape_names.clone(),
        ..Default::default()
    };

    let data_loaded = if args.data.is_empty() {
        None
    } else {
        Some(load_sources(&args.data, base)?)
    };
    // Report display draws on both documents: focus and value nodes are
    // data-graph terms, constraints are shapes-graph terms, and each reads best
    // spelled the way its own document spelled it.
    let display_prefixes = shifty_algebra::Prefixes::merged([
        data_loaded
            .as_ref()
            .map(|d| d.prefixes.clone())
            .unwrap_or_default(),
        shapes_loaded.prefixes.clone(),
    ]);
    let inference = if args.no_infer {
        None
    } else {
        let outcome = match data_loaded.as_ref() {
            Some(data) => {
                shifty_engine::infer_graphs(&data.graph, &shapes_loaded.graph, &normalized)
            }
            None => shifty_engine::infer(&shapes_loaded.graph, &normalized),
        };
        match outcome {
            Ok(outcome) => Some(outcome),
            Err(e) => {
                return Err(format!(
                    "{e}; cannot infer before validation (see `inspect --stage strata`)"
                )
                .into());
            }
        }
    };
    if let Some(inference) = &inference {
        for d in &inference.diagnostics {
            eprintln!("warning: {d}");
        }
    }
    let data_graph = inference.as_ref().map_or_else(
        || {
            data_loaded
                .as_ref()
                .map_or(&shapes_loaded.graph, |data| &data.graph)
        },
        |inference| &inference.graph,
    );

    // W3C report mode: component-granular validator + RDF report output.
    if args.report {
        let report = if data_loaded.is_some() {
            shifty_engine::validate_report_graphs_with_mode_and_options(
                &shapes_loaded,
                data_graph,
                graph_mode,
                &validation_options,
            )
        } else {
            shifty_engine::validate_report_with_options(
                &shapes_loaded,
                data_graph,
                &validation_options,
            )
        };
        let graph = shifty_engine::report_to_graph(&report);
        // Collect prefixes from shapes + data, deduplicating by name.
        // Fall back to standard entries for sh:/rdf:/xsd: if not declared.
        let mut prefixes: Vec<(&str, &str)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (name, iri) in shapes_loaded.prefixes.iter().chain(
            data_loaded
                .as_ref()
                .map(|d| d.prefixes.iter())
                .into_iter()
                .flatten(),
        ) {
            if seen.insert(name.as_str()) {
                prefixes.push((name.as_str(), iri.as_str()));
            }
        }
        for (name, iri) in [
            ("sh", "http://www.w3.org/ns/shacl#"),
            ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("xsd", "http://www.w3.org/2001/XMLSchema#"),
        ] {
            if seen.insert(name) {
                prefixes.push((name, iri));
            }
        }
        let mut ser = oxttl::TurtleSerializer::new();
        for (name, iri) in &prefixes {
            ser = ser.with_prefix(*name, *iri).unwrap();
        }
        let bytes = graph
            .iter()
            .try_fold(ser.for_writer(Vec::new()), |mut s, triple| {
                s.serialize_triple(triple).map(|()| s)
            })?
            .finish()?;
        print!("{}", String::from_utf8_lossy(&bytes));
        return Ok(());
    }

    let physical = shifty_opt::plan(&normalized);
    let mut outcome = match if data_loaded.is_some() {
        shifty_engine::validate_plan_graphs_with_mode_and_options(
            data_graph,
            &shapes_loaded.graph,
            &physical,
            graph_mode,
            &validation_options,
        )
    } else {
        shifty_engine::validate_plan_with_options(data_graph, &physical, &validation_options)
    } {
        Ok(o) => o,
        Err(e) => {
            return Err(format!("{e}; cannot validate (see `inspect --stage strata`)").into());
        }
    };

    // The engine retains every finding; `--minimum-severity` scopes both
    // `conforms` (already applied) and what we display/serialize here. Drop
    // findings below the threshold; a violation's own severity is the max of its
    // reasons, so any violation that survives keeps at least its top reason.
    outcome.violations.retain(|v| v.severity.meets(&threshold));
    for v in &mut outcome.violations {
        v.reasons.retain(|r| r.severity.meets(&threshold));
    }

    match args.format {
        Format::Dot => return Err("--format dot is not supported for validate".into()),
        Format::Json => {
            let doc = json_report(&outcome, &parsed.schema, &physical, &display_prefixes)?;
            println!("{}", serde_json::to_string_pretty(&doc)?);
        }
        Format::Text => {
            // Findings, not violations. The same constraint failing on 53 nodes
            // is one thing wrong with the graph, and printing its explanation 53
            // times buries the two other things that are also wrong.
            //
            // The unit is one *reason*, not one violation: a focus node that
            // fails two constraints has two things wrong with it, and grouping
            // them together would force a member to carry several unrelated
            // value nodes with nothing to say which belonged to which.
            let mut findings: Vec<Finding> = Vec::new();
            let mut index: HashMap<(usize, String, String, String), usize> = HashMap::new();
            for v in &outcome.violations {
                let st = &parsed.schema.statements[v.statement];
                let focus = shifty_algebra::render::term_to_string_in(&v.focus, &display_prefixes);
                let target = shifty_algebra::render::selector_to_string_in_px(
                    &st.selector,
                    &parsed.schema.arena,
                    &parsed.schema.prefixes,
                );
                // The source shape's IRI. Printed even when the target line
                // already names it — an implicit class target renders as
                // `class(<that same IRI>)` — because which shape a finding came
                // from is the first thing a reader goes to fix, and it should
                // not be conditional on how the target happened to render.
                let shape = parsed
                    .schema
                    .name_of(st.shape)
                    .map(|name| display_prefixes.compact(name));

                for r in &v.reasons {
                    let severity = r.severity.to_string();
                    let mut value = None;
                    let body = render_reason(
                        r,
                        &physical.arena,
                        &display_prefixes,
                        &focus,
                        &severity,
                        2,
                        Some(&mut value),
                    );
                    let key = (
                        v.statement,
                        severity.clone(),
                        target.clone(),
                        body.join("\n"),
                    );
                    match index.get(&key) {
                        Some(at) => findings[*at].members.push((focus.clone(), value)),
                        None => {
                            index.insert(key, findings.len());
                            findings.push(Finding {
                                statement: v.statement,
                                target: target.clone(),
                                severity,
                                shape: shape.clone(),
                                body,
                                members: vec![(focus.clone(), value)],
                            });
                        }
                    }
                }
            }

            let total = outcome.violations.len();
            if outcome.conforms {
                println!("conforms: true");
            } else if findings.len() == total {
                println!("conforms: false — {}", plural(total, "violation"));
            } else {
                println!(
                    "conforms: false — {} in {}",
                    plural(total, "violation"),
                    plural(findings.len(), "finding")
                );
            }

            let mut out: Vec<String> = Vec::new();
            for (i, finding) in findings.iter().enumerate() {
                out.push(String::new());
                out.push(format!("Finding {} of {}", i + 1, findings.len()));
                out.extend(field(2, "target", &finding.target));
                out.extend(field(2, "severity", &finding.severity));
                if let Some(shape) = &finding.shape {
                    out.extend(field(2, "shape", shape));
                }
                out.extend(finding.body.iter().cloned());
                out.push(String::new());
                out.extend(render_affected(&finding.members));
                let related = related_findings(i, &findings);
                if !related.is_empty() {
                    out.extend(field(2, "also fails", &related));
                }
            }
            for line in &out {
                println!("{line}");
            }
            for line in notation_key(&out) {
                println!("{line}");
            }
        }
    }

    Ok(())
}

fn repair(args: RepairArgs) -> Result<(), Box<dyn Error>> {
    let base = args.base.as_deref();
    let shapes_loaded = load_sources(&args.shapes, base)?;
    let parsed = shifty_parse::parse_loaded(&shapes_loaded);
    parsed.require_valid()?;
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }
    let schema = &parsed.schema;

    let data_loaded = if args.data.is_empty() {
        None
    } else {
        Some(load_sources(&args.data, base)?)
    };

    // Witness/synthesize against the (optionally inferred) data graph.
    let inference = if args.no_infer {
        None
    } else {
        let outcome = match data_loaded.as_ref() {
            Some(data) => shifty_engine::infer_graphs(&data.graph, &shapes_loaded.graph, schema),
            None => shifty_engine::infer(&shapes_loaded.graph, schema),
        };
        match outcome {
            Ok(o) => Some(o),
            Err(e) => {
                return Err(format!("{e}; cannot infer (see `inspect --stage strata`)").into());
            }
        }
    };
    let data_graph = match inference {
        Some(inf) => inf.graph,
        None => data_loaded
            .as_ref()
            .map_or_else(|| shapes_loaded.graph.clone(), |d| d.graph.clone()),
    };
    // Witness/gate against `data ∪ shapes` so paths and the class hierarchy
    // (e.g. `rdfs:subClassOf` for `sh:class`) resolve against the shapes/ontology
    // graph, while focus and the emitted repair stay the data graph. When the
    // shapes embed the data, `data_graph` already is the union.
    let context = if data_loaded.is_some() {
        shifty_engine::graph_union(&data_graph, &shapes_loaded.graph)
    } else {
        data_graph.clone()
    };

    // --apply: run the fixpoint driver and emit the repaired graph.
    if args.apply {
        let result = match shifty_engine::repair_to_fixpoint(
            &data_graph,
            &context,
            schema,
            shifty_engine::EnumOptions::default(),
        ) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("{e}; cannot repair (see `inspect --stage strata`)").into());
            }
        };
        let mut lines: Vec<String> = result.graph.iter().map(|t| t.to_string()).collect();
        lines.sort();
        for line in lines {
            println!("{line}");
        }
        eprintln!(
            "repaired: applied {} repair(s) over {} iteration(s); {} violation(s) remain",
            result.applied.len(),
            result.iterations,
            result.remaining,
        );
        return Ok(());
    }

    let witnesses = match shifty_engine::witness_violations(&data_graph, &context, schema) {
        Ok(ws) => ws,
        Err(e) => {
            return Err(format!("{e}; cannot witness (see `inspect --stage strata`)").into());
        }
    };

    if matches!(args.format, Format::Dot) {
        return Err("--format dot is not supported for repair".into());
    }

    let target = |statement: usize| {
        shifty_algebra::render::selector_to_string_in_px(
            &schema.statements[statement].selector,
            &schema.arena,
            &schema.prefixes,
        )
    };

    match args.stage {
        RepairStage::Witness => match args.format {
            Format::Json => println!("{}", serde_json::to_string_pretty(&witnesses)?),
            Format::Text => {
                if witnesses.is_empty() {
                    println!("conforms: no violations to witness");
                }
                for fw in &witnesses {
                    println!("{}  [target: {}]", fw.focus, target(fw.statement));
                    for line in render_witness(&fw.failure, &schema.prefixes, 2) {
                        println!("{line}");
                    }
                }
            }
            Format::Dot => unreachable!(),
        },
        RepairStage::Tree => {
            let trees: Vec<(&shifty_engine::FocusWitness, shifty_repair::RepairTree)> = witnesses
                .iter()
                .map(|fw| (fw, shifty_engine::synthesize(&schema.arena, fw)))
                .collect();
            match args.format {
                Format::Json => {
                    let arr: Vec<_> = trees
                        .iter()
                        .map(|(fw, t)| {
                            serde_json::json!({
                                "focus": fw.focus.to_string(),
                                "statement": fw.statement,
                                "tree": t,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&arr)?);
                }
                Format::Text => {
                    if trees.is_empty() {
                        println!("conforms: no violations to repair");
                    }
                    for (fw, t) in &trees {
                        println!("{}  [target: {}]", fw.focus, target(fw.statement));
                        for line in render_tree(t, &schema.arena, &schema.prefixes, 2) {
                            println!("{line}");
                        }
                    }
                }
                Format::Dot => unreachable!(),
            }
        }
        RepairStage::Solve => {
            let opts = shifty_engine::EnumOptions::default();
            let mut json_items = Vec::new();
            if witnesses.is_empty() {
                match args.format {
                    Format::Json => println!("[]"),
                    _ => println!("conforms: no violations to repair"),
                }
            }
            for fw in &witnesses {
                let tree = shifty_engine::synthesize(&schema.arena, fw);
                let sol = match shifty_engine::enumerate_repair(
                    &tree,
                    &data_graph,
                    &context,
                    schema,
                    opts,
                ) {
                    Ok(s) => s,
                    Err(e) => return Err(format!("{e}; cannot solve").into()),
                };
                match args.format {
                    Format::Text => {
                        println!("{}  [target: {}]", fw.focus, target(fw.statement));
                        match &sol {
                            Some(s) => {
                                println!(
                                    "  repair (fixes {}, introduces {}):",
                                    s.outcome.fixed.len(),
                                    s.outcome.introduced.len()
                                );
                                for t in &s.delta.delete {
                                    println!("    del  {t}");
                                }
                                for t in &s.delta.add {
                                    println!("    add  {t}");
                                }
                            }
                            None => println!("  no repair found within budget"),
                        }
                    }
                    Format::Json => json_items.push(serde_json::json!({
                        "focus": fw.focus.to_string(),
                        "statement": fw.statement,
                        "repair": sol.as_ref().map(|s| serde_json::json!({
                            "add": s.delta.add.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                            "delete": s.delta.delete.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                            "fixed": s.outcome.fixed.len(),
                            "introduced": s.outcome.introduced.len(),
                        })),
                    })),
                    Format::Dot => unreachable!(),
                }
            }
            if matches!(args.format, Format::Json) {
                println!("{}", serde_json::to_string_pretty(&json_items)?);
            }
        }
    }
    Ok(())
}

fn path_str(p: &shifty_algebra::Path, px: &shifty_algebra::Prefixes) -> String {
    shifty_algebra::render::path_to_string_in(p, px)
}

fn render_witness(
    w: &shifty_engine::Witness,
    px: &shifty_algebra::Prefixes,
    indent: usize,
) -> Vec<String> {
    use shifty_engine::Witness as W;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match w {
        W::Atom {
            node,
            reached_by,
            produced_by,
            ..
        } => out.push(format!(
            "{pad}Atom at {node} via {}{}",
            path_str(reached_by, px),
            if produced_by.is_some() {
                " [cuttable]"
            } else {
                ""
            }
        )),
        W::Relational {
            kind, offending, ..
        } => out.push(format!(
            "{pad}Relational {kind:?}: {} offending pair(s)",
            offending.len()
        )),
        W::Closed { offenders, .. } => {
            out.push(format!(
                "{pad}Closed: {} disallowed triple(s)",
                offenders.len()
            ));
            for (p, o) in offenders {
                out.push(format!("{pad}  - {p} {o}"));
            }
        }
        W::Not { inner, .. } => {
            out.push(format!("{pad}Not — falsify the inner shape:"));
            out.extend(render_sat(inner, px, indent + 2));
        }
        W::All { failed, .. } => {
            out.push(format!("{pad}All — fix every:"));
            for f in failed {
                out.extend(render_witness(f, px, indent + 2));
            }
        }
        W::Any { branches, .. } => {
            out.push(format!("{pad}Any — fix any one of:"));
            for b in branches {
                out.extend(render_witness(b, px, indent + 2));
            }
        }
        W::CountLow {
            path, have, min, ..
        } => out.push(format!(
            "{pad}CountLow along {}: have {have}, need {min}",
            path_str(path, px)
        )),
        W::CountHigh {
            path,
            matched,
            max,
            per_value,
            ..
        } => {
            out.push(format!(
                "{pad}CountHigh along {}: {} match(es), max {max}",
                path_str(path, px),
                matched.len()
            ));
            for (v, sub) in per_value {
                out.push(format!("{pad}  value {v}:"));
                out.extend(render_witness(sub, px, indent + 4));
            }
        }
        W::Opaque { .. } => out.push(format!("{pad}Opaque (SPARQL) — no algebraic witness")),
    }
    out
}

fn render_sat(
    s: &shifty_engine::SatTrace,
    px: &shifty_algebra::Prefixes,
    indent: usize,
) -> Vec<String> {
    use shifty_engine::SatTrace as S;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match s {
        S::Irrefutable { .. } => out.push(format!("{pad}Irrefutable (⊤)")),
        S::Atom { node, .. } => out.push(format!("{pad}Atom holds at {node} [cut to break]")),
        S::AllHeld { children, .. } => {
            out.push(format!("{pad}AllHeld — break any one:"));
            for c in children {
                out.extend(render_sat(c, px, indent + 2));
            }
        }
        S::AnyHeld { satisfied, .. } => {
            out.push(format!("{pad}AnyHeld — break every:"));
            for c in satisfied {
                out.extend(render_sat(c, px, indent + 2));
            }
        }
        S::CountHeld { matches, .. } => {
            out.push(format!("{pad}CountHeld: {} match(es)", matches.len()))
        }
        S::ForAllHeld { values, .. } => {
            out.push(format!(
                "{pad}ForAllHeld: {} checked value(s)",
                values.len()
            ));
            for (_, _, trace) in values {
                out.extend(render_sat(trace, px, indent + 2));
            }
        }
        S::NotHeld { inner_fails, .. } => {
            out.push(format!("{pad}NotHeld — make the inner shape hold:"));
            out.extend(render_witness(inner_fails, px, indent + 2));
        }
        S::Blocked { reason, .. } => out.push(format!("{pad}Blocked: {reason:?}")),
        S::Coinductive { .. } => out.push(format!("{pad}Coinductive (gfp back-edge)")),
    }
    out
}

fn render_tree(
    t: &shifty_repair::RepairTree,
    arena: &shifty_algebra::ShapeArena,
    px: &shifty_algebra::Prefixes,
    indent: usize,
) -> Vec<String> {
    use shifty_repair::RepairTree as T;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match t {
        T::Noop(_) => out.push(format!("{pad}Noop")),
        T::Blocked(_, r) => out.push(format!("{pad}Blocked: {r:?}")),
        T::Edits { edits, holes, .. } => {
            out.push(format!("{pad}Edits:"));
            for e in edits {
                out.push(format!("{pad}  {}", edit_str(e)));
            }
            for (h, c) in holes {
                out.push(format!(
                    "{pad}  ?{} : {}",
                    h.0,
                    constraint_str(c, arena, px)
                ));
            }
        }
        T::All { children, .. } => {
            out.push(format!("{pad}All — do all:"));
            for c in children {
                out.extend(render_tree(c, arena, px, indent + 2));
            }
        }
        T::Any { children, .. } => {
            out.push(format!("{pad}Any — choose one:"));
            for c in children {
                out.extend(render_tree(c, arena, px, indent + 2));
            }
        }
        T::Repeat { body, min, max, .. } => {
            let hi = max.map_or_else(|| "∞".to_string(), |m| m.to_string());
            out.push(format!("{pad}Repeat [{min}..{hi}]:"));
            out.extend(render_tree(body, arena, px, indent + 2));
        }
    }
    out
}

fn edit_str(e: &shifty_repair::Edit) -> String {
    use shifty_repair::EditOp;
    let (sign, p) = match &e.op {
        EditOp::Add(p) => ("add", p),
        EditOp::Delete(p) => ("del", p),
    };
    format!(
        "{sign} {} {} {}",
        slot_str(&p.s),
        slot_str(&p.p),
        slot_str(&p.o)
    )
}

fn slot_str(s: &shifty_repair::Slot) -> String {
    match s {
        shifty_repair::Slot::Bound(t) => t.to_string(),
        shifty_repair::Slot::Open(h) => format!("?{}", h.0),
    }
}

fn constraint_str(
    c: &shifty_repair::HoleConstraint,
    arena: &shifty_algebra::ShapeArena,
    px: &shifty_algebra::Prefixes,
) -> String {
    use shifty_repair::HoleConstraint as H;
    match c {
        H::AnyNode => "any node".to_string(),
        H::Fresh => "fresh node".to_string(),
        H::Const(t) => format!("= {t}"),
        H::Typed(_) => "typed value".to_string(),
        H::Kind(_) => "nodeKind".to_string(),
        H::OneOf(v) => format!("one of {} value(s)", v.len()),
        H::ConformsTo(s) => shifty_algebra::render::describe_shape_in(arena, *s, px),
        H::ConformsToAll(ss) => shifty_algebra::render::describe_shapes_in(arena, ss, px),
    }
}

fn inspect(args: InspectArgs) -> Result<(), Box<dyn Error>> {
    let bytes = std::fs::read(&args.file)?;
    let base = args.base.as_deref();
    let source = args.file.to_string_lossy();
    let loaded = shifty_parse::load_rdf_auto(&bytes, None, Some(source.as_ref()), base)?;
    let out = shifty_parse::parse_loaded(&loaded);

    match args.stage {
        Stage::Rdf => match args.format {
            Format::Text => {
                let mut lines: Vec<String> = loaded.graph.iter().map(|t| t.to_string()).collect();
                lines.sort();
                for line in lines {
                    println!("{line}");
                }
            }
            Format::Json => {
                let triples: Vec<_> = loaded
                    .graph
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "subject": t.subject.to_string(),
                            "predicate": t.predicate.to_string(),
                            "object": t.object.to_string(),
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&triples)?);
            }
            Format::Dot => {
                return Err(
                    "--format dot is only supported for --stage algebra or --stage normalized"
                        .into(),
                );
            }
        },
        Stage::Algebra => {
            match args.format {
                Format::Text => print!("{}", shifty_algebra::render::schema_to_text(&out.schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&out.schema)?),
                Format::Dot => print!("{}", shifty_algebra::render::schema_to_dot(&out.schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Normalized => {
            let schema = shifty_opt::normalize(&out.schema);
            match args.format {
                Format::Text => print!("{}", shifty_algebra::render::schema_to_text(&schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&schema)?),
                Format::Dot => print!("{}", shifty_algebra::render::schema_to_dot(&schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Strata => {
            let strat = shifty_opt::analyze(&out.schema.arena);
            match args.format {
                Format::Json => println!("{}", serde_json::to_string_pretty(&strat)?),
                Format::Text => print_strata(&strat),
                Format::Dot => {
                    return Err(
                        "--format dot is only supported for --stage algebra or --stage normalized"
                            .into(),
                    );
                }
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Plan => {
            let normalized = shifty_opt::normalize(&out.schema);
            let physical = shifty_opt::plan(&normalized);
            match args.format {
                Format::Text => print!("{}", shifty_opt::plan::plan_to_text(&physical)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&physical)?),
                Format::Dot => return Err("--format dot is not supported for --stage plan".into()),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Capability => {
            if !matches!(args.format, Format::Text) {
                return Err("--stage capability only supports --format text".into());
            }
            let normalized = shifty_opt::normalize(&out.schema);
            print_capability(&normalized);
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
    }
    Ok(())
}

fn print_strata(strat: &shifty_opt::Stratification) {
    let recursive = strat.recursive().count();
    println!(
        "strata: stratifiable = {}; {} shape(s) in {} stratum(strata); {} recursive component(s)",
        strat.stratifiable,
        strat.shape_count(),
        strat.strata.len(),
        recursive,
    );
    let fmt = |shapes: &[shifty_algebra::ShapeId]| {
        shapes
            .iter()
            .map(|s| format!("@{}", s.0))
            .collect::<Vec<_>>()
            .join(" ")
    };
    if recursive > 0 {
        println!("recursive components (in dependency order):");
        for (level, s) in strat.strata.iter().enumerate() {
            if !s.recursive {
                continue;
            }
            let tag = if s.stratifiable {
                "positive recursion, ok"
            } else {
                "NON-STRATIFIABLE: recursion through negation"
            };
            println!("  stratum {level}: {}  ({tag})", fmt(&s.shapes));
        }
    }
}

fn print_capability(schema: &shifty_algebra::Schema) {
    use shifty_algebra::Shape;
    use shifty_opt::lower_query;
    use spargebra::SparqlParser;

    let mut sparql_queries: Vec<String> = Vec::new();
    for i in 0..schema.arena.len() {
        let id = shifty_algebra::ShapeId(i as u32);
        if let Shape::Sparql(c) = schema.arena.get(id) {
            sparql_queries.push(c.query.clone());
        }
    }

    // `lower_query` is the routing gate: a query runs on the native executor iff
    // it lowers to a native plan, otherwise it falls back to Spareval. This
    // reports what actually happens, not the broader designed subset (which lives
    // in docs/05-sparql-execution.md §129-141).
    let lowered_count = sparql_queries
        .iter()
        .filter(|q| {
            SparqlParser::new()
                .parse_query(q)
                .map(|parsed| lower_query(&parsed).is_ok())
                .unwrap_or(false)
        })
        .count();

    println!(
        "capability: {} SPARQL constraint query/queries ({} native, {} fall back)",
        sparql_queries.len(),
        lowered_count,
        sparql_queries.len() - lowered_count,
    );

    for (i, q) in sparql_queries.iter().enumerate() {
        match SparqlParser::new().parse_query(q) {
            Ok(parsed) => {
                let tag = match lower_query(&parsed) {
                    Ok(_) => "NATIVE".to_string(),
                    Err(reason) => format!("FALLBACK ({reason})"),
                };
                println!("  [{i}] {tag}:\n{q}");
            }
            Err(e) => println!("  [{i}] PARSE ERROR: {e}"),
        }
    }
}
