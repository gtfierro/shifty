//! `shifty shape repl`: an interactive Shapeshift session.
//!
//! Plain input is Shapeshift text: a definition is accepted only if the whole
//! session still builds, so the session is never left broken. Lines starting
//! with `:` are commands. As you type, the tree-sitter grammar colors the
//! input, underlines syntax errors in red, shows the compiler's first error as
//! an inline hint, and keeps a definition open across lines until its braces
//! balance. A blank line submits whatever is pending.

use super::{ImportOpts, ReplArgs, apply_imports, render_diagnostics, render_diagnostics_in};
use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Editor, Helper};
use shapeshift::{Applied, Session};
use shifty_experimental::shapeshift;
use std::borrow::Cow;
use std::cell::RefCell;
use std::error::Error;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

const COMMANDS: &[(&str, &str)] = &[
    (":help", "show this help"),
    (":quit", "leave (also :q or Ctrl-D)"),
    (":load FILE", "load a .shm file into the session"),
    (":data FILE", "add a data graph (Turtle, N-Triples, …)"),
    (":clear-data", "drop all loaded data"),
    (
        ":validate",
        "validate the data against the session (also :v)",
    ),
    (":check", "rebuild and show warnings"),
    (":list", "list the shapes in the session"),
    (":show NAME", "print one shape's source"),
    (":explain NAME", "source, algebra, and targets of one shape"),
    (":rm NAME", "remove a shape"),
    (":source", "print the whole session as .shm"),
    (":save FILE", "write the session as a .shm file"),
    (
        ":export [--core] [FILE]",
        "raise the session to SHACL Turtle",
    ),
    (
        ":image save FILE",
        "save the whole session (shapes, data, settings, history)",
    ),
    (":image load FILE", "restore a saved session"),
    (":history", "show the accepted inputs, in order"),
    (":map IRI PATH", "resolve an import IRI from a local file"),
    (":fetch on|off", "allow fetching http(s) imports"),
    (
        ":auto on|off",
        "re-validate after every accepted definition",
    ),
];

/// The input-time feedback: tree-sitter for structure, the compiler parser
/// for wording.
struct ShapeHelper {
    parser: RefCell<tree_sitter::Parser>,
    color: bool,
    names: RefCell<Vec<String>>,
    prefixes: RefCell<Vec<String>>,
    files: FilenameCompleter,
}

impl ShapeHelper {
    fn new(color: bool) -> Self {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&shapeshift::grammar::LANGUAGE.into())
            .expect("the bundled grammar matches the tree-sitter runtime");
        ShapeHelper {
            parser: RefCell::new(parser),
            color,
            names: RefCell::new(Vec::new()),
            prefixes: RefCell::new(Vec::new()),
            files: FilenameCompleter::new(),
        }
    }

    fn refresh(&self, session: &Session) {
        *self.names.borrow_mut() = session
            .module
            .shapes
            .iter()
            .map(|s| s.name.clone())
            .collect();
        let mut px: Vec<String> = session
            .module
            .prefixes
            .iter()
            .map(|p| format!("{}:", p.name))
            .collect();
        for (p, _) in shapeshift::macros::build::DEFAULT_PREFIXES {
            px.push(format!("{p}:"));
        }
        *self.prefixes.borrow_mut() = px;
    }

    fn parse(&self, src: &str) -> Option<tree_sitter::Tree> {
        self.parser.borrow_mut().parse(src, None)
    }

    /// ANSI-color Shapeshift source using the tree-sitter parse.
    fn colorize(&self, src: &str) -> String {
        let Some(tree) = self.parse(src) else {
            return src.to_string();
        };
        let mut spans: Vec<(usize, usize, &'static str)> = Vec::new();
        collect_spans(tree.root_node(), &mut spans);
        spans.sort_by_key(|s| (s.0, std::cmp::Reverse(s.1)));
        let mut out = String::with_capacity(src.len() * 2);
        let mut at = 0;
        for (start, end, code) in spans {
            if start < at || end > src.len() || start >= end {
                continue;
            }
            out.push_str(&src[at..start]);
            out.push_str(&format!("\x1b[{code}m{}\x1b[0m", &src[start..end]));
            at = end;
        }
        out.push_str(&src[at..]);
        out
    }

    /// The first syntax problem in `src`, if the input is otherwise complete.
    fn problem(&self, src: &str) -> Option<String> {
        if src.trim().is_empty() || src.trim_start().starts_with(':') || unbalanced(src) > 0 {
            return None;
        }
        let tree = self.parse(src)?;
        if !tree.root_node().has_error() {
            return None;
        }
        // Tree-sitter says where; the compiler's parser says what, in the
        // language's own words.
        let parsed = shapeshift::parse::parse(src, "input");
        if let Some(d) = parsed.diagnostics.iter().find(|d| d.is_error()) {
            return Some(d.message.clone());
        }
        first_error(tree.root_node()).map(|n| {
            if n.is_missing() {
                format!("missing `{}`", n.kind())
            } else {
                let text = &src[n.start_byte()..n.end_byte().min(src.len())];
                format!("unexpected `{}`", text.trim())
            }
        })
    }
}

fn collect_spans(node: tree_sitter::Node, out: &mut Vec<(usize, usize, &'static str)>) {
    if node.is_error() {
        out.push((node.start_byte(), node.end_byte(), "4;31"));
        return;
    }
    let kind = node.kind();
    let leafish = matches!(
        kind,
        "string" | "iri" | "comment" | "label" | "prefixed_name"
    ) && node.is_named();
    if node.child_count() == 0 || leafish {
        if let Some(code) = color_for(node) {
            out.push((node.start_byte(), node.end_byte(), code));
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_spans(child, out);
    }
}

fn color_for(node: tree_sitter::Node) -> Option<&'static str> {
    let kind = node.kind();
    let parent = node.parent().map(|p| p.kind()).unwrap_or("");
    if !node.is_named() {
        if kind.chars().all(|c| c.is_ascii_alphabetic()) {
            return Some(match parent {
                "datatype" => "36",
                "shape_decl" | "base_decl" | "prefix_decl" | "import_decl" | "target"
                | "boolean" | "frame" | "closed" | "path_decl" | "is_line" => "1;35",
                "annotation" => "33",
                _ => "34",
            });
        }
        return match kind {
            "{" | "}" | "(" | ")" => Some("2"),
            "^" | "/" | "*" | "+" | "?" => Some("35"),
            _ => None,
        };
    }
    match kind {
        "comment" => Some("2;3"),
        "string" | "iri" => Some("32"),
        "integer" | "decimal" | "date" | "date_time" | "time" | "boolean_literal"
        | "severity_level" => Some("36"),
        "label" => Some("33"),
        "prefixed_name" => Some("36"),
        "self" => Some("35"),
        "identifier" => Some(match parent {
            "shape_decl" => "1;33",
            "path_decl" | "group" => "1;36",
            "call" => "1;34",
            "parameter" | "named_argument" => "3",
            _ => "0",
        }),
        _ => None,
    }
}

fn first_error(node: tree_sitter::Node) -> Option<tree_sitter::Node> {
    if node.is_error() || node.is_missing() {
        return Some(node);
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.has_error()
            && let Some(n) = first_error(child)
        {
            return Some(n);
        }
    }
    None
}

/// Open braces and parentheses not yet closed, ignoring strings, IRIs, and comments.
fn unbalanced(src: &str) -> i64 {
    let mut depth = 0i64;
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                while let Some(d) = chars.next() {
                    match d {
                        '\\' => {
                            chars.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            '<' => {
                // IRIs never contain spaces; `<` is not otherwise used.
                for d in chars.by_ref() {
                    if d == '>' || d.is_whitespace() {
                        break;
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                for d in chars.by_ref() {
                    if d == '\n' {
                        break;
                    }
                }
            }
            '{' | '(' => depth += 1,
            '}' | ')' => depth -= 1,
            _ => {}
        }
    }
    depth
}

/// Whether the buffer is ready to submit. A blank last line always submits.
fn complete(src: &str) -> bool {
    let t = src.trim_start();
    let blank_last_line = src
        .rsplit_once('\n')
        .is_some_and(|(_, last)| last.trim().is_empty());
    if t.is_empty() || t.starts_with(':') || blank_last_line {
        return true;
    }
    if unbalanced(src) > 0 {
        return false;
    }
    // A docstring or severity on its own line belongs to the next shape.
    let last = src.trim_end().lines().last().unwrap_or("").trim_start();
    let dangling = ["doc ", "severity ", "message "]
        .iter()
        .any(|k| last.starts_with(k))
        || last.ends_with(" then")
        || last == "then"
        || last.ends_with(',');
    !dangling
}

impl Highlighter for ShapeHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if !self.color {
            return Cow::Borrowed(line);
        }
        if let Some(rest) = line.strip_prefix(':') {
            let (cmd, args) = rest.split_once(' ').unwrap_or((rest, ""));
            let sep = if line.contains(' ') { " " } else { "" };
            return Cow::Owned(format!("\x1b[1;35m:{cmd}\x1b[0m{sep}{args}"));
        }
        Cow::Owned(self.colorize(line))
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if self.color {
            Cow::Owned(format!("\x1b[2;31m{hint}\x1b[0m"))
        } else {
            Cow::Borrowed(hint)
        }
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        self.color
    }
}

impl Hinter for ShapeHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }
        if let Some(rest) = line.strip_prefix(':')
            && !rest.contains(' ')
            && !rest.is_empty()
        {
            let hit = COMMANDS.iter().find(|(c, _)| c[1..].starts_with(rest))?;
            let word = hit.0.split(' ').next().unwrap_or(hit.0);
            return Some(format!("{}   {}", &word[1 + rest.len()..], hit.1));
        }
        self.problem(line).map(|p| format!("   ← {p}"))
    }
}

impl Validator for ShapeHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        Ok(if complete(ctx.input()) {
            ValidationResult::Valid(None)
        } else {
            ValidationResult::Incomplete
        })
    }
}

impl Completer for ShapeHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let before = &line[..pos];
        if let Some(rest) = before.strip_prefix(':') {
            if !rest.contains(' ') {
                let pairs = COMMANDS
                    .iter()
                    .map(|(c, _)| c.split(' ').next().unwrap_or(c))
                    .filter(|c| c[1..].starts_with(rest))
                    .map(|c| Pair {
                        display: c.to_string(),
                        replacement: format!("{c} "),
                    })
                    .collect();
                return Ok((0, pairs));
            }
            let cmd = rest.split(' ').next().unwrap_or("");
            if matches!(cmd, "load" | "data" | "save" | "image" | "export" | "map") {
                return self.files.complete(line, pos, ctx);
            }
            if matches!(cmd, "show" | "explain" | "rm") {
                let start = before.rfind(' ').map_or(0, |i| i + 1);
                let word = &before[start..];
                let pairs = self
                    .names
                    .borrow()
                    .iter()
                    .filter(|n| n.starts_with(word))
                    .map(|n| Pair {
                        display: n.clone(),
                        replacement: n.clone(),
                    })
                    .collect();
                return Ok((start, pairs));
            }
            return Ok((pos, Vec::new()));
        }
        let start = before
            .rfind(|c: char| c.is_whitespace() || "{}(),;^/".contains(c))
            .map_or(0, |i| i + c_len(before, i));
        let word = &before[start..];
        if word.is_empty() {
            return Ok((pos, Vec::new()));
        }
        let mut words: Vec<String> = shapeshift::parse::KEYWORDS
            .iter()
            .map(|k| k.to_string())
            .collect();
        words.extend(self.names.borrow().iter().cloned());
        words.extend(self.prefixes.borrow().iter().cloned());
        words.sort();
        words.dedup();
        let pairs = words
            .into_iter()
            .filter(|w| w.starts_with(word) && w != word)
            .map(|w| Pair {
                display: w.clone(),
                replacement: w,
            })
            .collect();
        Ok((start, pairs))
    }
}

fn c_len(s: &str, i: usize) -> usize {
    s[i..].chars().next().map_or(1, char::len_utf8)
}

impl Helper for ShapeHelper {}

struct Repl {
    session: Session,
    helper_color: bool,
    /// `None`: on whenever data is loaded.
    auto: Option<bool>,
    import_opts: ImportOpts,
}

fn green(color: bool, s: &str) -> String {
    if color {
        format!("\x1b[32m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

fn dim(color: bool, s: &str) -> String {
    if color {
        format!("\x1b[2m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

enum Flow {
    Continue,
    Quit,
}

impl Repl {
    fn say_applied(&self, applied: &Applied, input: Option<&str>) {
        let c = self.helper_color;
        let rendered = match input {
            Some(text) => {
                render_diagnostics_in(&applied.diagnostics, c, Some((&self.session.label, text)))
            }
            None => render_diagnostics(&applied.diagnostics, c),
        };
        print!("{rendered}");
        if !applied.accepted {
            println!("{}", dim(c, "(not applied; the session is unchanged)"));
            return;
        }
        for n in &applied.defined {
            println!("{} {n}", green(c, "defined"));
        }
        for n in &applied.redefined {
            println!("{} {n}", green(c, "redefined"));
        }
        if applied.defined.is_empty() && applied.redefined.is_empty() {
            println!("{}", green(c, "ok"));
        }
    }

    fn auto_validate(&self) {
        if self.auto.unwrap_or(true) && !self.session.data.is_empty() {
            self.validate();
        }
    }

    fn validate(&self) {
        if self.session.data.is_empty() {
            println!("no data loaded; use :data FILE");
            return;
        }
        match self.session.validate() {
            Ok(report) => {
                let text = report.to_text();
                if report.conforms {
                    println!("{}", green(self.helper_color, text.trim_end()));
                } else {
                    print!("{text}");
                }
            }
            Err(e) => println!("{e}"),
        }
    }

    fn input(&mut self, text: &str) {
        let applied = self.session.apply(text);
        self.say_applied(&applied, Some(text));
        if applied.accepted {
            self.auto_validate();
        }
    }

    fn command(&mut self, line: &str, helper: &ShapeHelper) -> Result<Flow, Box<dyn Error>> {
        let mut parts = line[1..].split_whitespace();
        let cmd = parts.next().unwrap_or("");
        let args: Vec<&str> = parts.collect();
        let arg = |i: usize, what: &str| -> Result<String, Box<dyn Error>> {
            args.get(i)
                .map(|s| s.to_string())
                .ok_or_else(|| format!(":{cmd} needs {what}").into())
        };
        let c = self.helper_color;
        match cmd {
            "q" | "quit" | "exit" => return Ok(Flow::Quit),
            "h" | "help" | "?" => {
                println!(
                    "Type Shapeshift definitions; they apply when the whole session still builds."
                );
                println!(
                    "A definition continues until its braces balance; a blank line submits.\n"
                );
                let w = COMMANDS.iter().map(|(c, _)| c.len()).max().unwrap_or(0);
                for (c, d) in COMMANDS {
                    println!("  {c:<w$}  {d}");
                }
            }
            "load" => {
                let applied = self.session.load_shm(Path::new(&arg(0, "a file")?))?;
                self.say_applied(&applied, None);
                if applied.accepted {
                    self.auto_validate();
                }
            }
            "data" => {
                for f in &args {
                    let n = self.session.add_data(Path::new(f))?;
                    println!("loaded {n} triples from {f}");
                }
                if args.is_empty() {
                    return Err(":data needs a file".into());
                }
                self.auto_validate();
            }
            "clear-data" => {
                self.session.clear_data();
                println!("data cleared");
            }
            "v" | "validate" => self.validate(),
            "check" => {
                let compiled = self.session.compile();
                print!("{}", render_diagnostics(&compiled.diagnostics, c));
                if !compiled.has_errors() {
                    println!(
                        "{} {} shape{}",
                        green(c, "ok:"),
                        compiled.built.shapes.len(),
                        if compiled.built.shapes.len() == 1 {
                            ""
                        } else {
                            "s"
                        }
                    );
                }
            }
            "list" | "ls" => {
                if self.session.module.shapes.is_empty() {
                    println!("no shapes yet");
                }
                for s in &self.session.module.shapes {
                    let params = if s.params.is_empty() {
                        String::new()
                    } else {
                        format!("({})", shapeshift::macros::display::params(&s.params))
                    };
                    let targets = s.targets.len();
                    let t = match targets {
                        0 => String::new(),
                        1 => "  (1 target)".to_string(),
                        n => format!("  ({n} targets)"),
                    };
                    let doc = s
                        .annotations
                        .doc
                        .as_ref()
                        .map(|d| format!("  -- {}", d.lines().next().unwrap_or("")))
                        .unwrap_or_default();
                    println!("  {}{params}{t}{}", s.name, dim(c, &doc));
                }
            }
            "show" => {
                let name = arg(0, "a shape name")?;
                let decl = self
                    .session
                    .shape(&name)
                    .ok_or_else(|| format!("no shape named `{name}`"))?;
                let text =
                    shapeshift::macros::display::format_module(&shapeshift::macros::ast::Module {
                        shapes: vec![decl.clone()],
                        ..Default::default()
                    });
                print!("{}", self.paint_source(&text, helper));
            }
            "explain" => print!("{}", self.session.explain(&arg(0, "a shape name")?)?),
            "rm" => {
                let name = arg(0, "a shape name")?;
                let applied = self.session.remove(&name);
                print!("{}", render_diagnostics(&applied.diagnostics, c));
                if applied.accepted {
                    println!("{} {name}", green(c, "removed"));
                } else {
                    println!("{}", dim(c, "(not removed: other shapes depend on it)"));
                }
            }
            "source" | "src" => print!("{}", self.paint_source(&self.session.source(), helper)),
            "save" => {
                let path = arg(0, "a file")?;
                self.session.save_source(Path::new(&path))?;
                println!("wrote {path}");
            }
            "export" => {
                let core = args.contains(&"--core");
                let file = args.iter().find(|a| !a.starts_with("--"));
                let (ttl, warnings) = self.session.export(core)?;
                for w in warnings {
                    println!("warning: {w}");
                }
                match file {
                    Some(f) => {
                        std::fs::write(f, ttl)?;
                        println!("wrote {f}");
                    }
                    None => print!("{ttl}"),
                }
            }
            "image" => {
                let sub = arg(0, "`save FILE` or `load FILE`")?;
                let path = PathBuf::from(arg(1, "a file")?);
                match sub.as_str() {
                    "save" => {
                        self.session.save_image(&path)?;
                        println!("saved session to {} ({})", path.display(), self.contents());
                    }
                    "load" => {
                        self.session = Session::load_image(&path)?;
                        println!("restored {} ({})", path.display(), self.contents());
                    }
                    other => {
                        return Err(format!(":image {other}: expected `save` or `load`").into());
                    }
                }
            }
            "history" => {
                for (i, h) in self.session.history.iter().enumerate() {
                    println!("{}", dim(c, &format!("-- [{}]", i + 1)));
                    print!("{}", self.paint_source(h.trim_end(), helper));
                }
            }
            "map" => {
                let iri = arg(0, "an IRI and a path")?;
                let path = arg(1, "a path")?;
                let path = std::fs::canonicalize(&path).unwrap_or_else(|_| PathBuf::from(path));
                self.session
                    .imports
                    .catalog
                    .insert(iri.clone(), path.clone());
                self.import_opts
                    .maps
                    .push(format!("{iri}={}", path.display()));
                println!("{iri} -> {}", path.display());
            }
            "fetch" => {
                self.session.imports.allow_http = on_off(&arg(0, "on or off")?)?;
                println!(
                    "fetching http(s) imports: {}",
                    if self.session.imports.allow_http {
                        "on"
                    } else {
                        "off"
                    }
                );
            }
            "auto" => {
                self.auto = Some(on_off(&arg(0, "on or off")?)?);
                println!(
                    "auto-validate: {}",
                    if self.auto.unwrap_or(true) {
                        "on"
                    } else {
                        "off"
                    }
                );
            }
            "" => {}
            other => {
                let names: Vec<&str> = COMMANDS
                    .iter()
                    .map(|(c, _)| c.split(' ').next().unwrap_or(c).trim_start_matches(':'))
                    .collect();
                let mut msg = format!("unknown command `:{other}`");
                if let Some(n) =
                    shapeshift::macros::diagnostic::nearest(other, names.iter().copied())
                {
                    msg.push_str(&format!("; did you mean `:{n}`?"));
                }
                return Err(format!("{msg} (:help lists commands)").into());
            }
        }
        helper.refresh(&self.session);
        Ok(Flow::Continue)
    }

    fn contents(&self) -> String {
        format!(
            "{}, {}",
            plural(self.session.module.shapes.len(), "shape"),
            plural(self.session.data.len(), "data graph")
        )
    }

    fn paint_source(&self, text: &str, helper: &ShapeHelper) -> String {
        let mut s = if self.helper_color {
            helper.colorize(text)
        } else {
            text.to_string()
        };
        if !s.ends_with('\n') {
            s.push('\n');
        }
        s
    }

    /// Handle one submitted buffer.
    fn submit(&mut self, buffer: &str, helper: &ShapeHelper) -> Flow {
        let text = buffer.trim();
        if text.is_empty() {
            return Flow::Continue;
        }
        if text.starts_with(':') {
            match self.command(text, helper) {
                Ok(flow) => return flow,
                Err(e) => {
                    let msg = e.to_string();
                    let msg = msg.trim_end();
                    if self.helper_color {
                        println!("\x1b[1;31merror:\x1b[0m {msg}");
                    } else {
                        println!("error: {msg}");
                    }
                }
            }
            return Flow::Continue;
        }
        self.input(buffer);
        helper.refresh(&self.session);
        Flow::Continue
    }
}

fn plural(n: usize, word: &str) -> String {
    format!("{n} {word}{}", if n == 1 { "" } else { "s" })
}

fn on_off(s: &str) -> Result<bool, Box<dyn Error>> {
    match s {
        "on" | "true" | "yes" => Ok(true),
        "off" | "false" | "no" => Ok(false),
        _ => Err(format!("expected on or off, got `{s}`").into()),
    }
}

fn history_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".shifty_shape_history"))
}

pub fn run(a: ReplArgs) -> Result<(), Box<dyn Error>> {
    let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    let color = interactive && !a.plain && std::env::var_os("NO_COLOR").is_none();
    let mut session = match &a.file {
        Some(f) if f.extension().is_some_and(|e| e == "json") => Session::load_image(f)?,
        _ => Session::new("repl"),
    };
    apply_imports(&mut session, &a.imports)?;
    let mut repl = Repl {
        session,
        helper_color: color,
        auto: None,
        import_opts: a.imports.clone(),
    };
    let helper = ShapeHelper::new(color);
    if let Some(f) = a
        .file
        .as_ref()
        .filter(|f| f.extension().is_none_or(|e| e != "json"))
    {
        let applied = repl.session.load_shm(f)?;
        repl.say_applied(&applied, None);
    }
    for d in &a.data {
        let n = repl.session.add_data(d)?;
        println!("loaded {n} triples from {}", d.display());
    }
    helper.refresh(&repl.session);

    if !interactive || a.plain {
        return run_plain(&mut repl, &helper);
    }

    println!(
        "shapeshift {} — {} shape{} loaded. :help for commands, Ctrl-D to quit.",
        env!("CARGO_PKG_VERSION"),
        repl.session.module.shapes.len(),
        if repl.session.module.shapes.len() == 1 {
            ""
        } else {
            "s"
        }
    );
    let config = rustyline::Config::builder()
        .auto_add_history(true)
        .completion_type(rustyline::CompletionType::List)
        .build();
    let mut editor: Editor<ShapeHelper, DefaultHistory> = Editor::with_config(config)?;
    editor.set_helper(Some(helper));
    let hist = history_path();
    if let Some(h) = &hist {
        let _ = editor.load_history(h);
    }
    loop {
        let prompt = if repl.helper_color {
            "\x1b[1;36mshape>\x1b[0m "
        } else {
            "shape> "
        };
        match editor.readline(prompt) {
            Ok(line) => {
                let helper = editor.helper().expect("helper installed");
                if let Flow::Quit = repl.submit(&line, helper) {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", dim(color, "(Ctrl-D or :quit to leave)"));
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(e.into()),
        }
    }
    if let Some(h) = &hist {
        let _ = editor.save_history(h);
    }
    Ok(())
}

/// Non-interactive mode: read stdin, grouping lines into definitions the same
/// way the interactive editor does. Useful for scripting and tests.
fn run_plain(repl: &mut Repl, helper: &ShapeHelper) -> Result<(), Box<dyn Error>> {
    let stdin = std::io::stdin();
    let mut buffer = String::new();
    for line in stdin.lock().lines() {
        let line = line?;
        if buffer.is_empty() && line.trim().is_empty() {
            continue;
        }
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);
        if complete(&buffer) || line.trim().is_empty() {
            let text = std::mem::take(&mut buffer);
            if let Flow::Quit = repl.submit(&text, helper) {
                return Ok(());
            }
            std::io::stdout().flush()?;
        }
    }
    if !buffer.trim().is_empty() {
        repl.submit(&buffer, helper);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip_ansi(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for d in chars.by_ref() {
                    if d == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn definitions_stay_open_until_braces_balance() {
        assert!(!complete("shape S {"));
        assert!(!complete("shape S {\n  ex:p required"));
        assert!(complete("shape S {\n  ex:p required\n}"));
        // braces inside strings, IRIs, and comments do not count
        assert!(complete("shape S { ex:p each pattern \"{\" }"));
        assert!(complete("shape S { ex:p required -- {\n}"));
        // annotations wait for their shape; `when … then` waits for its block
        assert!(!complete("doc \"a sensor\""));
        assert!(!complete("shape S { when { ex:a required } then"));
        // a blank line submits anyway, and commands are always one line
        assert!(complete("shape S {\n"));
        assert!(complete(":load x.shm"));
    }

    #[test]
    fn highlighting_keeps_text_and_marks_errors() {
        let h = ShapeHelper::new(true);
        let src = "shape S for every ex:C { name: ex:p at least 1 is a ex:D -- c\n}";
        let colored = h.colorize(src);
        assert_eq!(strip_ansi(&colored), src);
        assert!(colored.contains("\x1b[1;35mshape\x1b[0m"));
        assert!(colored.contains("\x1b[1;33mS\x1b[0m"));
        assert!(colored.contains("\x1b[2;3m-- c\x1b[0m"));
        let broken = h.colorize("shape S { ex:p ex:q }");
        assert!(
            broken.contains("\x1b[4;31m"),
            "syntax errors are underlined: {broken:?}"
        );
        assert_eq!(strip_ansi(&broken), "shape S { ex:p ex:q }");
    }

    #[test]
    fn hints_name_the_first_error_in_the_compilers_words() {
        let h = ShapeHelper::new(false);
        assert_eq!(h.problem("shape S { ex:p required }"), None);
        assert_eq!(
            h.problem("shape S { ex:p"),
            None,
            "incomplete input is not an error yet"
        );
        let p = h.problem("shape S { ex:p at least }").expect("an error");
        assert!(p.contains("whole number"), "{p}");
    }
}
