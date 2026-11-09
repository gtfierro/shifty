//! `shifty shape`: the Shapeshift language from the command line.
//!
//! Every subcommand goes through [`shapeshift::Session`], the same layer the
//! REPL uses, so a file checked here behaves exactly like the same text typed
//! at the prompt.

mod repl;

use clap::{Args, Subcommand, ValueEnum};
use shapeshift::Session;
use shapeshift::macros::Diagnostic;
use shapeshift::macros::diagnostic::Level;
use std::error::Error;
use std::path::{Path, PathBuf};

#[derive(Subcommand)]
pub enum ShapeCommand {
    /// Parse, resolve imports, and build; report diagnostics (exit 1 on error).
    Check(FileArgs),
    /// Build and print a summary (or the shifty schema as JSON).
    Build(BuildArgs),
    /// Re-render a file canonically (comments and docstrings kept).
    Fmt(FmtArgs),
    /// Raise the definitions to SHACL Turtle.
    Export(ExportArgs),
    /// Validate data; report failures against the lines you wrote (exit 1 if it does not conform).
    Validate(ValidateArgs),
    /// List the import closure.
    Imports(ImportsArgs),
    /// Show one shape's source, algebra, and targets.
    Explain(ExplainArgs),
    /// Interactive session: define shapes, load data, validate, save.
    Repl(ReplArgs),
}

#[derive(Args, Clone, Default)]
pub struct ImportOpts {
    /// Map an import IRI to a local file: `--map IRI=PATH` (repeatable).
    #[arg(long = "map", value_name = "IRI=PATH", action = clap::ArgAction::Append)]
    pub maps: Vec<String>,
    /// Allow fetching http(s) imports that are not mapped.
    #[arg(long)]
    pub fetch: bool,
}

#[derive(Args)]
pub struct FileArgs {
    /// A `.shm` file.
    pub file: PathBuf,
    #[command(flatten)]
    pub imports: ImportOpts,
    /// Output format for diagnostics.
    #[arg(long, value_enum, default_value_t = OutFormat::Text)]
    pub format: OutFormat,
}

#[derive(Args)]
pub struct BuildArgs {
    pub file: PathBuf,
    #[command(flatten)]
    pub imports: ImportOpts,
    /// What to print.
    #[arg(long, value_enum, default_value_t = Emit::Summary)]
    pub emit: Emit,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Emit {
    /// Shapes, targets, and portability flags.
    Summary,
    /// The parsed AST as JSON (the interchange format).
    Ast,
    /// The built shifty schema as JSON.
    Schema,
    /// The algebra, one shape per line.
    Algebra,
}

#[derive(Args)]
pub struct FmtArgs {
    pub file: PathBuf,
    /// Rewrite the file in place.
    #[arg(long, short)]
    pub write: bool,
    /// Exit 1 if the file is not formatted (for CI).
    #[arg(long)]
    pub check: bool,
}

#[derive(Args)]
pub struct ExportArgs {
    pub file: PathBuf,
    #[command(flatten)]
    pub imports: ImportOpts,
    /// Pure SHACL Core: fail on shape-based and SPARQL targets.
    #[arg(long)]
    pub core: bool,
    /// Write to a file instead of stdout.
    #[arg(long, short)]
    pub output: Option<PathBuf>,
}

#[derive(Args)]
pub struct ValidateArgs {
    pub file: PathBuf,
    /// Data graph(s) to validate (repeatable).
    #[arg(long, short, required = true, action = clap::ArgAction::Append)]
    pub data: Vec<PathBuf>,
    #[command(flatten)]
    pub imports: ImportOpts,
    #[arg(long, value_enum, default_value_t = OutFormat::Text)]
    pub format: OutFormat,
}

#[derive(Args)]
pub struct ImportsArgs {
    pub file: PathBuf,
    #[command(flatten)]
    pub imports: ImportOpts,
    /// Fetch every import and print canonical identities and SHA-256 hashes.
    #[arg(long)]
    pub resolve: bool,
    #[arg(long, value_enum, default_value_t = OutFormat::Text)]
    pub format: OutFormat,
}

#[derive(Args)]
pub struct ExplainArgs {
    pub file: PathBuf,
    /// The shape to explain.
    pub shape: String,
    #[command(flatten)]
    pub imports: ImportOpts,
}

#[derive(Args)]
pub struct ReplArgs {
    /// A `.shm` file or a session image (`.json`) to start from.
    pub file: Option<PathBuf>,
    /// Data graph(s) to load (repeatable).
    #[arg(long, short, action = clap::ArgAction::Append)]
    pub data: Vec<PathBuf>,
    #[command(flatten)]
    pub imports: ImportOpts,
    /// Disable colors and as-you-type feedback.
    #[arg(long)]
    pub plain: bool,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum OutFormat {
    Text,
    Json,
}

fn apply_imports(session: &mut Session, opts: &ImportOpts) -> Result<(), Box<dyn Error>> {
    for m in &opts.maps {
        let (iri, path) = m
            .split_once('=')
            .ok_or_else(|| format!("--map expects IRI=PATH, got `{m}`"))?;
        let path = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
        session.imports.catalog.insert(iri.to_string(), path);
    }
    session.imports.allow_http = opts.fetch;
    Ok(())
}

/// Load a file into a fresh session, failing on syntax errors.
fn session_for(
    file: &Path,
    opts: &ImportOpts,
) -> Result<(Session, Vec<Diagnostic>), Box<dyn Error>> {
    let mut s = Session::new(file.display().to_string());
    apply_imports(&mut s, opts)?;
    let text = std::fs::read_to_string(file)
        .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    let parsed = shapeshift::parse::parse(&text, &file.display().to_string());
    if let Some(dir) = std::fs::canonicalize(file)
        .ok()
        .and_then(|p| p.parent().map(Path::to_path_buf))
    {
        s.dir = Some(dir);
    }
    s.module = parsed.module;
    Ok((s, parsed.diagnostics))
}

/// Render diagnostics with the offending source line and a caret.
pub fn render_diagnostics(diags: &[Diagnostic], color: bool) -> String {
    render_diagnostics_in(diags, color, None)
}

/// Like [`render_diagnostics`], but diagnostics labeled `input.0` take their
/// source line from the text `input.1` (REPL input) instead of the disk.
pub fn render_diagnostics_in(
    diags: &[Diagnostic],
    color: bool,
    input: Option<(&str, &str)>,
) -> String {
    let mut out = String::new();
    let paint = |code: &str, s: &str| {
        if color {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    };
    for d in diags {
        let (word, code) = match d.level {
            Level::Error => ("error", "1;31"),
            Level::Warning => ("warning", "1;33"),
        };
        out.push_str(&format!(
            "{}: {}\n",
            paint(code, word),
            paint("1", &d.message)
        ));
        if let Some(loc) = &d.location {
            out.push_str(&format!(
                "  {} {}:{}:{}\n",
                paint("34", "-->"),
                loc.file,
                loc.line,
                loc.column
            ));
            let src = match input {
                Some((label, text)) if label == loc.file => Some(text.to_string()),
                Some(_) | None => std::fs::read_to_string(&loc.file).ok(),
            };
            if let Some(line) = src
                .as_deref()
                .and_then(|s| s.lines().nth(loc.line.saturating_sub(1) as usize))
            {
                let n = loc.line.to_string();
                out.push_str(&format!(
                    "  {} {} {line}\n",
                    paint("34", &n),
                    paint("34", "|")
                ));
                let pad = " ".repeat(n.len());
                let caret = " ".repeat(loc.column.saturating_sub(1) as usize);
                out.push_str(&format!(
                    "  {pad} {} {caret}{}\n",
                    paint("34", "|"),
                    paint(code, "^")
                ));
            }
        }
        for note in &d.notes {
            out.push_str(&format!("  {note}\n"));
        }
        if let Some(h) = &d.hint {
            out.push_str(&format!("  {}: {h}\n", paint("36", "hint")));
        }
    }
    out
}

fn color_stderr() -> bool {
    std::io::IsTerminal::is_terminal(&std::io::stderr()) && std::env::var_os("NO_COLOR").is_none()
}

fn fail(msg: impl Into<String>) -> Box<dyn Error> {
    msg.into().into()
}

pub fn run(cmd: ShapeCommand) -> Result<(), Box<dyn Error>> {
    match cmd {
        ShapeCommand::Check(a) => check(a),
        ShapeCommand::Build(a) => build(a),
        ShapeCommand::Fmt(a) => fmt(a),
        ShapeCommand::Export(a) => export(a),
        ShapeCommand::Validate(a) => validate(a),
        ShapeCommand::Imports(a) => imports(a),
        ShapeCommand::Explain(a) => explain(a),
        ShapeCommand::Repl(a) => repl::run(a),
    }
}

fn compile_file(
    file: &Path,
    opts: &ImportOpts,
) -> Result<(Session, shapeshift::Compiled, Vec<Diagnostic>), Box<dyn Error>> {
    let (session, mut diags) = session_for(file, opts)?;
    if diags.iter().any(Diagnostic::is_error) {
        return Ok((session, empty_compiled(), diags));
    }
    let compiled = session.compile();
    diags.extend(compiled.diagnostics.iter().cloned());
    Ok((session, compiled, diags))
}

fn empty_compiled() -> shapeshift::Compiled {
    let program = shapeshift::macros::Program::default();
    shapeshift::Compiled {
        built: shapeshift::macros::build(&program),
        resolved: shapeshift::import::Resolved {
            program,
            graphs: Vec::new(),
            closure: Vec::new(),
            diagnostics: Vec::new(),
        },
        diagnostics: Vec::new(),
    }
}

fn check(a: FileArgs) -> Result<(), Box<dyn Error>> {
    let (_, _, diags) = compile_file(&a.file, &a.imports)?;
    let errors = diags.iter().filter(|d| d.is_error()).count();
    match a.format {
        OutFormat::Json => println!("{}", serde_json::to_string_pretty(&diags)?),
        OutFormat::Text => {
            eprint!("{}", render_diagnostics(&diags, color_stderr()));
            if errors == 0 {
                eprintln!("ok: {} builds", a.file.display());
            }
        }
    }
    if errors > 0 {
        return Err(fail(format!(
            "{errors} error{} in {}",
            if errors == 1 { "" } else { "s" },
            a.file.display()
        )));
    }
    Ok(())
}

fn require_clean(diags: &[Diagnostic]) -> Result<(), Box<dyn Error>> {
    let rendered = render_diagnostics(diags, color_stderr());
    eprint!("{rendered}");
    let errors = diags.iter().filter(|d| d.is_error()).count();
    if errors > 0 {
        return Err(fail(format!(
            "{errors} error{}; nothing was produced",
            if errors == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

fn build(a: BuildArgs) -> Result<(), Box<dyn Error>> {
    if matches!(a.emit, Emit::Ast) {
        let text = std::fs::read_to_string(&a.file)?;
        let parsed = shapeshift::parse::parse(&text, &a.file.display().to_string());
        require_clean(&parsed.diagnostics)?;
        println!("{}", serde_json::to_string_pretty(&parsed.module)?);
        return Ok(());
    }
    let (_, compiled, diags) = compile_file(&a.file, &a.imports)?;
    require_clean(&diags)?;
    let b = &compiled.built;
    match a.emit {
        Emit::Schema => println!("{}", serde_json::to_string_pretty(&b.schema)?),
        Emit::Algebra => print!("{}", shifty_algebra::render::schema_to_text(&b.schema)),
        Emit::Summary => {
            println!(
                "{} shape{}",
                b.shapes.len(),
                if b.shapes.len() == 1 { "" } else { "s" }
            );
            for (name, slot) in &b.shapes {
                let targets: Vec<String> = b
                    .schema
                    .statements
                    .iter()
                    .filter(|st| st.shape == *slot)
                    .map(|st| {
                        shifty_algebra::render::selector_to_string_in_px(
                            &st.selector,
                            &b.schema.arena,
                            &b.schema.prefixes,
                        )
                    })
                    .collect();
                if targets.is_empty() {
                    println!("  {name}");
                } else {
                    println!("  {name}  for {}", targets.join(", "));
                }
            }
            if !b.expansions.is_empty() {
                println!(
                    "{} template expansion{}",
                    b.expansions.len(),
                    if b.expansions.len() == 1 { "" } else { "s" }
                );
                for e in &b.expansions {
                    println!("  {}({})", e.template, e.args.join(", "));
                }
            }
            let p = &b.portability;
            let mut flags = Vec::new();
            if p.recursive {
                flags.push("recursive");
            }
            if p.shape_targets {
                flags.push("shape targets");
            }
            if p.sparql_targets {
                flags.push("SPARQL targets");
            }
            if !flags.is_empty() {
                println!("needs beyond SHACL Core: {}", flags.join(", "));
            }
        }
        Emit::Ast => unreachable!(),
    }
    Ok(())
}

fn fmt(a: FmtArgs) -> Result<(), Box<dyn Error>> {
    let text = std::fs::read_to_string(&a.file)?;
    let formatted = shapeshift::parse::format(&text, &a.file.display().to_string())
        .map_err(|d| fail(render_diagnostics(&d, color_stderr())))?;
    if a.check {
        if formatted != text {
            return Err(fail(format!(
                "{} is not formatted; run `shifty shape fmt --write`",
                a.file.display()
            )));
        }
        return Ok(());
    }
    if a.write {
        if formatted != text {
            std::fs::write(&a.file, formatted)?;
            eprintln!("formatted {}", a.file.display());
        }
    } else {
        print!("{formatted}");
    }
    Ok(())
}

fn export(a: ExportArgs) -> Result<(), Box<dyn Error>> {
    let (session, _, diags) = compile_file(&a.file, &a.imports)?;
    require_clean(&diags)?;
    let (ttl, warnings) = session.export(a.core).map_err(|e| fail(e.to_string()))?;
    for w in warnings {
        eprintln!("warning: {w}");
    }
    match a.output {
        Some(p) => std::fs::write(&p, ttl)?,
        None => print!("{ttl}"),
    }
    Ok(())
}

fn validate(a: ValidateArgs) -> Result<(), Box<dyn Error>> {
    let (mut session, _, diags) = compile_file(&a.file, &a.imports)?;
    require_clean(&diags)?;
    for d in &a.data {
        session.add_data(d).map_err(|e| fail(e.to_string()))?;
    }
    let report = session.validate().map_err(|e| fail(e.to_string()))?;
    match a.format {
        OutFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutFormat::Text => print!("{}", report.to_text()),
    }
    if !report.conforms {
        std::process::exit(1);
    }
    Ok(())
}

fn imports(a: ImportsArgs) -> Result<(), Box<dyn Error>> {
    let (session, diags) = session_for(&a.file, &a.imports)?;
    require_clean(&diags)?;
    let resolver = shapeshift::import::FsResolver {
        catalog: session.imports.catalog.clone().into_iter().collect(),
        allow_http: session.imports.allow_http,
    };
    let loc = std::fs::canonicalize(&a.file)
        .ok()
        .map(shapeshift::import::Location::File);
    let resolved = shapeshift::import::resolve(
        session.module.clone(),
        &a.file.display().to_string(),
        loc,
        &resolver,
        a.resolve,
    );
    match a.format {
        OutFormat::Json => println!("{}", serde_json::to_string_pretty(&resolved.closure)?),
        OutFormat::Text => {
            if resolved.closure.is_empty() {
                println!("no imports");
            }
            for i in &resolved.closure {
                let kind = match i.kind {
                    shifty_algebra::ImportKind::Shapeshift => "shapeshift",
                    shifty_algebra::ImportKind::Rdf => "rdf",
                };
                let alias = i
                    .alias
                    .as_ref()
                    .map(|a| format!(" as {a}"))
                    .unwrap_or_default();
                match &i.sha256 {
                    Some(h) => println!(
                        "{kind:<10} {}{alias}  {}  sha256:{h}",
                        i.location, i.canonical
                    ),
                    None => println!(
                        "{kind:<10} {}{alias}  (imported by {})",
                        i.location, i.imported_by
                    ),
                }
            }
        }
    }
    require_clean(&resolved.diagnostics)
}

fn explain(a: ExplainArgs) -> Result<(), Box<dyn Error>> {
    let (session, _, diags) = compile_file(&a.file, &a.imports)?;
    require_clean(&diags)?;
    print!(
        "{}",
        session.explain(&a.shape).map_err(|e| fail(e.to_string()))?
    );
    Ok(())
}
