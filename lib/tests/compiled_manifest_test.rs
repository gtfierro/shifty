#![cfg(feature = "compiled-tests")]

use ontoenv::config::Config;
use oxigraph::io::{RdfFormat, RdfParser, RdfSerializer};
use oxigraph::model::{Graph, NamedNode};
use shacl_compiler::{generate_rust_modules_from_plan, PlanIR};
use shifty::canonicalization::{are_isomorphic, deskolemize_graph};
use shifty::test_utils::{list_includes, load_manifest, TestCase};
use shifty::{Source, Validator};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::UNIX_EPOCH;
use url::Url;

static COMPILED_CACHE: OnceLock<Mutex<HashMap<PathBuf, PathBuf>>> = OnceLock::new();
static COMPILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BUILD_ROOT: OnceLock<PathBuf> = OnceLock::new();

fn compiled_cache() -> &'static Mutex<HashMap<PathBuf, PathBuf>> {
    COMPILED_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn compile_lock() -> &'static Mutex<()> {
    COMPILE_LOCK.get_or_init(|| Mutex::new(()))
}

fn compiled_test_build_root() -> PathBuf {
    BUILD_ROOT
        .get_or_init(|| {
            let path = workspace_root()
                .join("target")
                .join("compiled-manifest-tests");
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("failed to create compiled test build root");
            path
        })
        .clone()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("lib crate should have a parent workspace")
        .to_path_buf()
}

fn build_env_config(root: &Path) -> Result<Config, Box<dyn Error + Send + Sync>> {
    Ok(Config::builder()
        .root(root.to_path_buf())
        .locations(Vec::new())
        .offline(true)
        .temporary(true)
        .build()?)
}

fn graph_to_turtle(graph: &Graph) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut buffer = Vec::new();
    let mut serializer = RdfSerializer::from_format(RdfFormat::Turtle).for_writer(&mut buffer);
    for triple in graph.iter() {
        serializer.serialize_triple(triple)?;
    }
    let turtle_string = String::from_utf8(buffer)?;
    let turtle_string = if turtle_string.ends_with('.') {
        turtle_string
    } else {
        format!("{}\n.", turtle_string)
    };
    Ok(turtle_string)
}

fn collect_tests_from_manifest(
    manifest_path: &Path,
) -> Result<Vec<(PathBuf, TestCase)>, Box<dyn Error + Send + Sync>> {
    let mut all_tests = Vec::new();
    let mut to_visit = VecDeque::new();
    let mut visited = HashSet::new();

    let manifest_path = manifest_path.to_path_buf();
    to_visit.push_back(manifest_path);

    while let Some(current_manifest) = to_visit.pop_front() {
        let canonical = current_manifest
            .canonicalize()
            .unwrap_or_else(|_| current_manifest.clone());
        if !visited.insert(canonical.clone()) {
            continue;
        }

        let manifest = load_manifest(&canonical).map_err(|e| {
            io::Error::other(format!(
                "Failed to load manifest from {}: {}",
                canonical.display(),
                e
            ))
        })?;
        for case in manifest.test_cases {
            all_tests.push((canonical.clone(), case));
        }

        let includes = list_includes(&canonical).map_err(|e| {
            io::Error::other(format!(
                "Failed to list mf:include targets for {}: {}",
                canonical.display(),
                e
            ))
        })?;
        for include in includes {
            let include_canonical = include.canonicalize().unwrap_or_else(|_| include.clone());
            if visited.contains(&include_canonical) {
                continue;
            }
            to_visit.push_back(include_canonical);
        }
    }

    all_tests.sort_by(|(path_a, case_a), (path_b, case_b)| {
        path_a
            .cmp(path_b)
            .then_with(|| case_a.name.cmp(&case_b.name))
    });

    Ok(all_tests)
}

fn skip_reason(test: &TestCase) -> Option<&'static str> {
    let advanced_expr = "/advanced/expression/";
    if test
        .data_graph_path
        .to_string_lossy()
        .contains(advanced_expr)
        || test
            .shapes_graph_path
            .to_string_lossy()
            .contains(advanced_expr)
    {
        return Some("SHACL-AF expressions not supported yet");
    }

    None
}

fn shape_cache_key(path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
    let metadata = std::fs::metadata(path)?;
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    metadata.len().hash(&mut hasher);
    if let Ok(modified) = metadata.modified() {
        if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
            duration.as_nanos().hash(&mut hasher);
        }
    }
    Ok(format!("{:016x}", hasher.finish()))
}

fn compiled_bin_for_shapes(
    shapes_graph_path: &Path,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let canonical = shapes_graph_path.canonicalize()?;
    if let Some(existing) = compiled_cache().lock().unwrap().get(&canonical).cloned() {
        return Ok(existing);
    }

    let root = workspace_root();
    let build_root = compiled_test_build_root();
    let bin_cache_dir = build_root.join("bin");
    std::fs::create_dir_all(&bin_cache_dir)?;
    let cache_key = shape_cache_key(&canonical)?;
    let cached_bin = bin_cache_dir.join(format!("{}-shacl-compiled", cache_key));
    if cached_bin.exists() {
        compiled_cache()
            .lock()
            .unwrap()
            .insert(canonical.clone(), cached_bin.clone());
        return Ok(cached_bin);
    }

    let _compile_guard = compile_lock().lock().unwrap();
    if let Some(existing) = compiled_cache().lock().unwrap().get(&canonical).cloned() {
        return Ok(existing);
    }
    if cached_bin.exists() {
        compiled_cache()
            .lock()
            .unwrap()
            .insert(canonical.clone(), cached_bin.clone());
        return Ok(cached_bin);
    }
    let env_config = build_env_config(&root)?;

    let validator = Validator::builder()
        .with_shapes_source(Source::File(canonical.clone()))
        .with_data_source(Source::Empty)
        .with_env_config(env_config)
        .with_warnings_are_errors(true)
        .with_shapes_data_union(true)
        .with_shape_optimization(true)
        .with_data_dependent_shape_optimization(false)
        .build()
        .map_err(|e| io::Error::other(format!("Failed to build validator: {}", e)))?;

    let shape_ir = validator
        .shape_ir_with_imports(-1)
        .map_err(|e| io::Error::other(format!("Failed to build SHACL-IR: {}", e)))?;
    let plan = PlanIR::from_shape_ir(&shape_ir)
        .map_err(|e| io::Error::other(format!("Failed to build plan: {}", e)))?;
    let generated = generate_rust_modules_from_plan(&plan)
        .map_err(|e| io::Error::other(format!("Failed to generate Rust: {}", e)))?;

    let out_dir = build_root.join("workspaces").join(&cache_key);
    let src_dir = out_dir.join("src");
    let generated_dir = src_dir.join("generated");
    if generated_dir.exists() {
        std::fs::remove_dir_all(&generated_dir)?;
    }
    std::fs::create_dir_all(&generated_dir)?;

    std::fs::write(generated_dir.join("mod.rs"), generated.root)?;
    for (name, content) in generated.files {
        let path = generated_dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, content)?;
    }
    let shape_ir_json = serde_json::to_string(&shape_ir).unwrap_or_else(|_| "null".to_string());
    std::fs::write(generated_dir.join("shape_ir.json"), shape_ir_json)?;

    let shifty_path = root.join("lib");
    let bin_name = "shacl-compiled";
    let cargo_toml = format!(
        "[workspace]\n\n[package]\nname = \"{}\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dependencies]\noxigraph = {{ version = \"0.5\" }}\nrayon = \"1\"\nregex = \"1\"\nserde_json = \"1\"\nshifty = {{ path = \"{}\", package = \"shifty-shacl\" }}\nontoenv = \"0.5.0-a8\"\noxsdatatypes = \"0.2.2\"\nfixedbitset = \"0.5\"\ndashmap = \"6\"\nlog = \"0.4\"\n\n[profile.release]\ndebug = true\n",
        bin_name,
        shifty_path.display(),
    );
    std::fs::write(out_dir.join("Cargo.toml"), cargo_toml)?;

    let main_rs = r#"
mod generated;

use generated::{load_original_value_index, render_report, set_original_value_index, DATA_GRAPH};
use log::info;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphName, NamedNode, Quad};
use oxigraph::store::Store;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;

fn print_usage(program: &str) {
    eprintln!("usage: {} [--follow-bnodes] <data.rdf>", program);
}

fn parse_args() -> Result<(String, bool), String> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "shacl-compiled".to_string());
    let mut follow_bnodes = false;
    let mut data_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--follow-bnodes" => follow_bnodes = true,
            other if other.starts_with("--") => {
                print_usage(&program);
                return Err(format!("unknown option: {}", other));
            }
            other => {
                if data_path.is_some() {
                    print_usage(&program);
                    return Err("multiple data files provided".into());
                }
                data_path = Some(other.to_string());
            }
        }
    }

    if let Some(path) = data_path {
        Ok((path, follow_bnodes))
    } else {
        print_usage(&program);
        Err("data file argument missing".into())
    }
}

fn sniff_format(path: &Path) -> Result<RdfFormat, String> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "ttl" => Ok(RdfFormat::Turtle),
        "nt" => Ok(RdfFormat::NTriples),
        "rdf" | "xml" => Ok(RdfFormat::RdfXml),
        other => Err(format!("unsupported RDF format .{}", other)),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let (data_path, follow_bnodes) =
        parse_args().map_err(|err| Box::<dyn std::error::Error>::from(err))?;
    let data_path_ref = Path::new(&data_path);
    let format = sniff_format(data_path_ref)?;
    let original_index = load_original_value_index(data_path_ref)
        .map_err(|err| Box::<dyn std::error::Error>::from(err))?;
    set_original_value_index(Some(original_index));
    let store = Store::new()?;
    let data_graph = NamedNode::new(DATA_GRAPH).unwrap();
    let graph_name = GraphName::NamedNode(data_graph.clone());
    let file = File::open(data_path_ref)?;
    let parser = RdfParser::from_format(format).for_reader(file);
    info!("Starting data graph load from {}", data_path);
    let mut triple_count = 0;
    for triple in parser {
        let triple = triple?;
        let quad = Quad::new(
            triple.subject.clone(),
            triple.predicate.clone(),
            triple.object.clone(),
            graph_name.clone(),
        );
        store.insert(&quad)?;
        triple_count += 1;
    }
    info!("Finished data graph load ({} triples)", triple_count);

    let report = generated::run(&store, Some(&data_graph));
    let output = render_report(&report, &store, follow_bnodes);
    println!("{}", output);
    Ok(())
}
"#;
    std::fs::write(src_dir.join("main.rs"), main_rs.trim_start())?;

    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--offline")
        .arg("--manifest-path")
        .arg(out_dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", build_root.join("target"))
        .env("CARGO_NET_OFFLINE", "true");

    let mut pkg_config_path = String::from("/usr/lib/x86_64-linux-gnu/pkgconfig");
    if let Ok(existing) = std::env::var("PKG_CONFIG_PATH") {
        if !existing.is_empty() {
            pkg_config_path.push(':');
            pkg_config_path.push_str(&existing);
        }
    }
    cmd.env("PKG_CONFIG_PATH", pkg_config_path);

    let status = cmd.status()?;
    if !status.success() {
        return Err(Box::new(io::Error::other(
            "failed to build compiled executable",
        )));
    }

    let shared_bin_path = build_root.join("target").join("debug").join(bin_name);
    std::fs::copy(&shared_bin_path, &cached_bin)?;
    compiled_cache()
        .lock()
        .unwrap()
        .insert(canonical.clone(), cached_bin.clone());
    Ok(cached_bin)
}

fn parse_report_graph(report_turtle: &str) -> Result<Graph, Box<dyn Error + Send + Sync>> {
    let mut graph = Graph::new();
    let parser = RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri("urn:compiled-report:")
        .map_err(|e| io::Error::other(format!("Invalid base IRI: {}", e)))?;
    for quad in parser.for_reader(report_turtle.as_bytes()) {
        let quad = quad.map_err(|e| io::Error::other(format!("Report parse error: {}", e)))?;
        graph.insert(oxigraph::model::TripleRef::new(
            &quad.subject,
            &quad.predicate,
            &quad.object,
        ));
    }
    Ok(graph)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResultSignature {
    source_shape: String,
    source_component: String,
    severity: String,
    focus: String,
    value: String,
    has_blank_term: bool,
}

fn term_key(term: Option<oxigraph::model::TermRef<'_>>) -> (String, bool) {
    match term {
        Some(oxigraph::model::TermRef::BlankNode(_)) => ("<blank>".to_string(), true),
        Some(term) => (term.to_string(), false),
        None => ("<none>".to_string(), false),
    }
}

fn result_signature(
    graph: &Graph,
    subject: oxigraph::model::NamedOrBlankNodeRef<'_>,
) -> Option<ResultSignature> {
    let sh_source_shape =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#sourceShape").ok()?;
    let sh_source_constraint_component =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#sourceConstraintComponent")
            .ok()?;
    let sh_result_severity =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#resultSeverity").ok()?;
    let sh_focus_node =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#focusNode").ok()?;
    let sh_value = oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#value").ok()?;
    let source_shape_term = graph.object_for_subject_predicate(subject, sh_source_shape.as_ref());
    let source_component_term =
        graph.object_for_subject_predicate(subject, sh_source_constraint_component.as_ref());
    let severity_term = graph.object_for_subject_predicate(subject, sh_result_severity.as_ref());

    let (source_shape, source_shape_blank) = term_key(source_shape_term);
    let (source_component, source_component_blank) = term_key(source_component_term);
    let (severity, severity_blank) = term_key(severity_term);
    let (focus, focus_blank) =
        term_key(graph.object_for_subject_predicate(subject, sh_focus_node.as_ref()));
    let (value, value_blank) =
        term_key(graph.object_for_subject_predicate(subject, sh_value.as_ref()));
    Some(ResultSignature {
        source_shape,
        source_component,
        severity,
        focus,
        value,
        has_blank_term: source_shape_blank
            || source_component_blank
            || severity_blank
            || focus_blank
            || value_blank,
    })
}

fn normalized_report_graph(report: &Graph, expected: &Graph) -> Graph {
    let rdf_type = oxigraph::model::vocab::rdf::TYPE;
    let sh_validation_result =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#ValidationResult").unwrap();
    let sh_result = oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#result").unwrap();

    let mut expected_counts: HashMap<ResultSignature, usize> = HashMap::new();
    for triple in expected.iter() {
        if triple.predicate == rdf_type
            && triple.object == oxigraph::model::TermRef::NamedNode(sh_validation_result.as_ref())
        {
            if let Some(sig) = result_signature(expected, triple.subject) {
                *expected_counts.entry(sig).or_insert(0) += 1;
            }
        }
    }

    let mut actual_by_sig: HashMap<ResultSignature, Vec<oxigraph::model::NamedOrBlankNode>> =
        HashMap::new();
    for triple in report.iter() {
        if triple.predicate == rdf_type
            && triple.object == oxigraph::model::TermRef::NamedNode(sh_validation_result.as_ref())
        {
            if let Some(sig) = result_signature(report, triple.subject) {
                actual_by_sig
                    .entry(sig)
                    .or_default()
                    .push(triple.subject.into_owned());
            }
        }
    }

    let mut dropped: HashSet<oxigraph::model::NamedOrBlankNode> = HashSet::new();
    for (sig, mut nodes) in actual_by_sig {
        let Some(expected_count) = expected_counts.get(&sig).copied() else {
            continue;
        };
        if !sig.has_blank_term || nodes.len() <= expected_count {
            continue;
        }
        nodes.sort_by_key(|n| n.to_string());
        for node in nodes.into_iter().skip(expected_count) {
            dropped.insert(node);
        }
    }

    if dropped.is_empty() {
        return report.clone();
    }

    let mut filtered = Graph::new();
    for triple in report.iter() {
        if dropped.contains(&triple.subject.into_owned()) {
            continue;
        }
        if triple.predicate == sh_result.as_ref() {
            if let oxigraph::model::TermRef::BlankNode(obj_bnode) = triple.object {
                let obj_node = oxigraph::model::NamedOrBlankNode::BlankNode(obj_bnode.into_owned());
                if dropped.contains(&obj_node) {
                    continue;
                }
            }
        }
        filtered.insert(triple);
    }
    filtered
}

fn anonymize_result_blank_nodes(graph: &Graph) -> Graph {
    let rdf_type = oxigraph::model::vocab::rdf::TYPE;
    let sh_validation_result =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#ValidationResult").unwrap();
    let sh_source_shape =
        oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#sourceShape").unwrap();
    let sh_value = oxigraph::model::NamedNode::new("http://www.w3.org/ns/shacl#value").unwrap();

    let mut result_subjects: HashSet<oxigraph::model::NamedOrBlankNode> = HashSet::new();
    for triple in graph.iter() {
        if triple.predicate == rdf_type
            && triple.object == oxigraph::model::TermRef::NamedNode(sh_validation_result.as_ref())
        {
            result_subjects.insert(triple.subject.into_owned());
        }
    }

    let mut replacements: HashMap<
        (
            oxigraph::model::NamedOrBlankNode,
            oxigraph::model::NamedNode,
        ),
        oxigraph::model::BlankNode,
    > = HashMap::new();
    let mut out = Graph::new();
    for triple in graph.iter() {
        let mut object = triple.object.into_owned();
        let subject = triple.subject.into_owned();
        if result_subjects.contains(&subject)
            && (triple.predicate == sh_source_shape.as_ref()
                || triple.predicate == sh_value.as_ref())
        {
            if let oxigraph::model::TermRef::BlankNode(_) = triple.object {
                let key = (subject.clone(), triple.predicate.into_owned());
                let replacement = replacements.entry(key).or_default().clone();
                object = oxigraph::model::Term::BlankNode(replacement);
            }
        }
        out.insert(
            oxigraph::model::Triple::new(subject, triple.predicate.into_owned(), object).as_ref(),
        );
    }

    out
}

fn report_conforms(report_graph: &Graph) -> Option<bool> {
    let conforms_pred = NamedNode::new("http://www.w3.org/ns/shacl#conforms").ok()?;
    for triple in report_graph.iter() {
        if triple.predicate == conforms_pred.as_ref() {
            if let oxigraph::model::TermRef::Literal(lit) = triple.object {
                if let Ok(parsed) = lit.value().parse::<bool>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn run_test_file(file: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tests = collect_tests_from_manifest(Path::new(file))?;
    for (manifest_path, test) in tests {
        let test_name = test.name.as_str();
        if let Some(reason) = skip_reason(&test) {
            println!(
                "[skip] {} — {} (manifest: {})",
                test_name,
                reason,
                manifest_path.display()
            );
            continue;
        }
        println!(
            "Running compiled test: {} from manifest: {}",
            test_name,
            manifest_path.display()
        );

        let bin_path = compiled_bin_for_shapes(&test.shapes_graph_path)?;
        let output = Command::new(&bin_path)
            .arg(&test.data_graph_path)
            .output()
            .map_err(|e| io::Error::other(format!("Failed to run compiled binary: {}", e)))?;

        if !output.status.success() {
            return Err(Box::new(io::Error::other(format!(
                "Compiled binary failed for test '{}':\nstdout:\n{}\nstderr:\n{}",
                test_name,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))));
        }

        let report_turtle = String::from_utf8(output.stdout).map_err(|e| {
            io::Error::other(format!(
                "Invalid UTF-8 output for test '{}': {}",
                test_name, e
            ))
        })?;
        let mut report_graph = parse_report_graph(&report_turtle)?;

        let data_graph_url =
            Url::from_file_path(test.data_graph_path.canonicalize()?).map_err(|()| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Failed to create file URL for data graph",
                )
            })?;
        let data_base_iri = format!("{}/.sk/", data_graph_url.as_str().trim_end_matches('/'));
        report_graph = deskolemize_graph(&report_graph, &data_base_iri);

        let shapes_graph_url = Url::from_file_path(test.shapes_graph_path.canonicalize()?)
            .map_err(|()| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Failed to create file URL for shapes graph",
                )
            })?;
        let shapes_base_iri = format!("{}/.sk/", shapes_graph_url.as_str().trim_end_matches('/'));
        report_graph = deskolemize_graph(&report_graph, &shapes_base_iri);
        let compiled_data_base_iri = format!(
            "{}-compiled-data/.sk/",
            shapes_graph_url.as_str().trim_end_matches('/')
        );
        report_graph = deskolemize_graph(&report_graph, &compiled_data_base_iri);

        let conforms = report_conforms(&report_graph)
            .ok_or_else(|| io::Error::other("Missing sh:conforms in compiled report"))?;
        let expects_conform = test.conforms;

        let report_path = PathBuf::from("compiled-report.ttl");
        std::fs::write(&report_path, report_turtle.as_bytes()).map_err(|e| {
            io::Error::other(format!(
                "Failed to write report to {} for test '{}': {}",
                report_path.display(),
                test_name,
                e
            ))
        })?;

        let expected_turtle = graph_to_turtle(&test.expected_report).map_err(|e| {
            io::Error::other(format!(
                "Failed to convert expected report graph to Turtle for test '{}': {}",
                test_name, e
            ))
        })?;
        let expected_path = PathBuf::from("compiled-expected.ttl");
        std::fs::write(&expected_path, expected_turtle.as_bytes()).map_err(|e| {
            io::Error::other(format!(
                "Failed to write expected report to {} for test '{}': {}",
                expected_path.display(),
                test_name,
                e
            ))
        })?;

        assert_eq!(
            conforms, expects_conform,
            "Conformance mismatch for test: {}. Expected {}\n(expected report:\n {})\n(our report:\n {})",
            test_name, expects_conform, expected_turtle, report_turtle
        );

        let result_message_pred =
            NamedNode::new("http://www.w3.org/ns/shacl#resultMessage").unwrap();
        let expected_has_result_messages = test
            .expected_report
            .iter()
            .any(|triple| triple.predicate == result_message_pred);

        let report_graph_for_compare: Graph = if expected_has_result_messages {
            report_graph.clone()
        } else {
            let mut filtered = Graph::new();
            for triple in report_graph.iter() {
                if triple.predicate != result_message_pred {
                    filtered.insert(triple);
                }
            }
            filtered
        };

        let expected_graph_for_compare: Graph = test.expected_report.clone();
        let report_graph_for_compare =
            normalized_report_graph(&report_graph_for_compare, &expected_graph_for_compare);
        let report_graph_for_compare = anonymize_result_blank_nodes(&report_graph_for_compare);
        let expected_graph_for_compare = anonymize_result_blank_nodes(&expected_graph_for_compare);

        let reports_match = are_isomorphic(&report_graph_for_compare, &expected_graph_for_compare);

        assert!(
            reports_match,
            "Validation report does not match expected report for test: {}.\nExpected:\n{}\nGot:\n{}",
            test_name,
            expected_turtle,
            report_turtle
        );
    }
    Ok(())
}

macro_rules! generate_test_cases {
    ($($name:ident: $file:expr),* $(,)?) => {
        $(
            #[test]
            #[ntest::timeout(300_000)]
            fn $name() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                run_test_file($file)
            }
        )*
    }
}

include!(concat!(env!("OUT_DIR"), "/generated_manifest_tests.rs"));
