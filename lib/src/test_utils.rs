use crate::named_nodes::{MF, RDF, RDFS, SHACL, SHT};
use crate::runtime::ToSubjectRef;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{Graph, NamedOrBlankNodeRef as SubjectRef, TermRef, TripleRef, vocab::xsd};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use url::Url;

/// Represents a single test case from a SHACL test suite manifest.
#[derive(Debug)]
pub struct TestCase {
    /// The name of the test case.
    pub name: String,
    /// Whether the data graph is expected to conform to the shapes graph.
    pub conforms: bool,
    /// The path to the data graph file.
    pub data_graph_path: PathBuf,
    /// The path to the shapes graph file.
    pub shapes_graph_path: PathBuf,
    /// The expected validation report graph.
    pub expected_report: Graph,
}

/// Represents a parsed SHACL test suite manifest file.
#[derive(Debug)]
pub struct Manifest {
    /// The path to the manifest file.
    pub path: PathBuf,
    /// A vector of `TestCase`s included in the manifest.
    pub test_cases: Vec<TestCase>,
}

fn resolve_path(base_path: &Path, relative_path: &str) -> PathBuf {
    if relative_path.is_empty() || relative_path == "<>" {
        return base_path.to_path_buf();
    }
    let base_dir = base_path
        .parent()
        .expect("Manifest path should have a parent directory");
    base_dir.join(relative_path)
}

fn read_manifest_content(path: &Path) -> Result<String, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read manifest file {}: {}", path.display(), e))?;
    let has_rdf_prefix = content.contains("@prefix rdf:")
        || content.contains("@PREFIX rdf:")
        || content.contains("PREFIX rdf:");
    if has_rdf_prefix {
        Ok(content)
    } else {
        Ok(format!(
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n{}",
            content
        ))
    }
}

fn extract_path_graph(manifest_graph: &Graph, path_node: SubjectRef, report_graph: &mut Graph) {
    let sh = SHACL::new();
    let rdf = RDF::new();
    for triple in manifest_graph.triples_for_subject(path_node) {
        report_graph.insert(triple);
        // Recurse for nested paths
        let predicate_ref = triple.predicate;
        if predicate_ref == sh.inverse_path
            || predicate_ref == sh.alternative_path
            || predicate_ref == sh.sequence_path
            || predicate_ref == sh.zero_or_more_path
            || predicate_ref == sh.one_or_more_path
            || predicate_ref == sh.zero_or_one_path
        {
            if let TermRef::NamedNode(_) | TermRef::BlankNode(_) = triple.object {
                extract_path_graph(manifest_graph, triple.object.to_subject_ref(), report_graph);
            }
        } else if (predicate_ref == rdf.rest || predicate_ref == rdf.first)
            && let TermRef::BlankNode(_) = triple.object
        {
            extract_path_graph(manifest_graph, triple.object.to_subject_ref(), report_graph);
        }
    }
}

fn extract_report_graph(manifest_graph: &Graph, result_node: SubjectRef) -> Graph {
    let mut report_graph = Graph::new();
    let sh = SHACL::new();

    // Add triples where result_node is the subject
    for triple in manifest_graph.triples_for_subject(result_node) {
        report_graph.insert(triple);
    }

    // Add triples for each sh:result
    let results = manifest_graph.objects_for_subject_predicate(result_node, sh.result);
    for result in results {
        if let TermRef::NamedNode(_) | TermRef::BlankNode(_) = result {
            let s = result.to_subject_ref();
            for triple in manifest_graph.triples_for_subject(s) {
                report_graph.insert(triple);

                // Recursively handle sh:resultPath if it's a blank node
                if triple.predicate == sh.result_path
                    && let TermRef::BlankNode(_) = triple.object
                {
                    let path_subject = triple.object.to_subject_ref();
                    extract_path_graph(manifest_graph, path_subject, &mut report_graph);
                }
            }
        }
    }
    report_graph
}

/// Loads and parses a SHACL test suite manifest file from the given path.
fn iri_to_file_path(iri: &str) -> Option<PathBuf> {
    if let Ok(url) = Url::parse(iri)
        && url.scheme() == "file"
    {
        return url.to_file_path().ok();
    }
    None
}

fn resolve_graph_path(
    manifest_path: &Path,
    manifest_url: &str,
    obj: TermRef,
) -> Result<PathBuf, String> {
    match obj {
        TermRef::NamedNode(nn) => {
            let iri = nn.as_str();
            if iri == manifest_url {
                return Ok(manifest_path.to_path_buf());
            }
            if let Some(path) = iri_to_file_path(iri) {
                Ok(path)
            } else if iri.starts_with("urn:") {
                Ok(manifest_path.to_path_buf())
            } else {
                Err(format!("Unsupported IRI for local file: {}", iri))
            }
        }
        TermRef::BlankNode(_) => Ok(manifest_path.to_path_buf()), // Fallback to same file
        TermRef::Literal(l) => Ok(resolve_path(manifest_path, l.value())),
    }
}

pub fn load_manifest(path: &Path) -> Result<Manifest, String> {
    let manifest_content = read_manifest_content(path)?;

    let manifest_url = Url::from_file_path(path.canonicalize().map_err(|e| e.to_string())?)
        .map_err(|_| "Invalid path".to_string())?
        .to_string();

    let mut manifest_graph = Graph::new();
    let parser = RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri(&manifest_url)
        .map_err(|e| e.to_string())?;
    for quad in parser.for_reader(Cursor::new(manifest_content)) {
        let quad = quad.map_err(|e| e.to_string())?;
        // Construct a TripleRef by ignoring the graph name:
        manifest_graph.insert(TripleRef::new(&quad.subject, &quad.predicate, &quad.object));
    }

    let mf = MF::new();
    let sht = SHT::new();
    let rdf = RDF::new();
    let rdfs = RDFS::new();
    let sh = SHACL::new();

    let manifest_node = manifest_graph
        .subjects_for_predicate_object(rdf.type_, mf.manifest)
        .next()
        .ok_or_else(|| format!("mf:Manifest not found in {}", path.display()))?;

    let mut test_cases = Vec::new();

    // Handle test entries
    if let Some(entries_list_head) =
        manifest_graph.object_for_subject_predicate(manifest_node, mf.entries)
        && let TermRef::NamedNode(_) | TermRef::BlankNode(_) = entries_list_head
    {
        let mut current_node = entries_list_head;
        let nil_ref: TermRef = rdf.nil.into();
        while current_node != nil_ref {
            let list_node = current_node.to_subject_ref();
            let obj = manifest_graph
                .object_for_subject_predicate(list_node, rdf.first)
                .ok_or_else(|| {
                    format!(
                        "Invalid RDF list for mf:entries: missing rdf:first at {}",
                        current_node
                    )
                })?;
            let entry = obj.to_subject_ref();

            let next_node = manifest_graph
                .object_for_subject_predicate(list_node, rdf.rest)
                .ok_or_else(|| "Invalid RDF list for mf:entries: missing rdf:rest".to_string())?;

            let is_validate_test =
                manifest_graph.contains(TripleRef::new(entry, rdf.type_, sht.validate));

            if is_validate_test {
                let name = manifest_graph
                    .object_for_subject_predicate(entry, rdfs.label)
                    .and_then(|t| match t {
                        TermRef::Literal(l) => Some(l.value().to_string()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "Unnamed test".to_string());

                let action_node = manifest_graph
                    .object_for_subject_predicate(entry, mf.action)
                    .ok_or_else(|| format!("Test '{}' has no mf:action", name))?;
                let action_s = action_node.to_subject_ref();

                // Defaults: many tests embed data+shapes in the same file
                let mut data_graph_path = path.to_path_buf();
                let mut shapes_graph_path = path.to_path_buf();

                // Optional explicit data/shapes graph paths
                if let Some(dg) =
                    manifest_graph.object_for_subject_predicate(action_s, SHT::new().data_graph)
                {
                    data_graph_path = resolve_graph_path(path, &manifest_url, dg)?;
                }
                if let Some(sg) =
                    manifest_graph.object_for_subject_predicate(action_s, SHT::new().shapes_graph)
                {
                    shapes_graph_path = resolve_graph_path(path, &manifest_url, sg)?;
                }

                let result_term = manifest_graph
                    .object_for_subject_predicate(entry, mf.result)
                    .ok_or_else(|| format!("Test '{}' has no mf:result", name))?;

                let skip_syntax_only = if let TermRef::NamedNode(nn) = result_term {
                    let iri = nn.as_str();
                    iri == "http://www.w3.org/ns/shacl-test#Failure"
                        || iri == "http://www.w3.org/ns/shacl-test#Success"
                } else {
                    false
                };

                if skip_syntax_only {
                    current_node = next_node;
                    continue;
                }

                let result_node =
                    if matches!(result_term, TermRef::NamedNode(_) | TermRef::BlankNode(_)) {
                        result_term.to_subject_ref()
                    } else {
                        current_node = next_node;
                        continue;
                    };

                let conforms = manifest_graph
                    .object_for_subject_predicate(result_node, sh.conforms)
                    .and_then(|t| {
                        if let TermRef::Literal(l) = t
                            && l.datatype() == xsd::BOOLEAN
                        {
                            return l.value().parse::<bool>().ok();
                        }
                        None
                    })
                    .ok_or_else(|| {
                        format!(
                            "Test '{}' has no valid sh:conforms boolean literal in its result",
                            name
                        )
                    })?;

                let expected_report = extract_report_graph(&manifest_graph, result_node);

                test_cases.push(TestCase {
                    name,
                    conforms,
                    data_graph_path,
                    shapes_graph_path,
                    expected_report,
                });
            }

            current_node = next_node;
        }
    }

    Ok(Manifest {
        path: path.to_path_buf(),
        test_cases,
    })
}

/// Lists mf:include targets from a manifest file (resolved to filesystem paths).
pub fn list_includes(path: &Path) -> Result<Vec<PathBuf>, String> {
    let manifest_content = read_manifest_content(path)?;

    let manifest_url = Url::from_file_path(path.canonicalize().map_err(|e| e.to_string())?)
        .map_err(|_| "Invalid path".to_string())?
        .to_string();

    let mut manifest_graph = Graph::new();
    let parser = RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri(&manifest_url)
        .map_err(|e| e.to_string())?;
    for quad in parser.for_reader(Cursor::new(manifest_content)) {
        let quad = quad.map_err(|e| e.to_string())?;
        manifest_graph.insert(TripleRef::new(&quad.subject, &quad.predicate, &quad.object));
    }

    let mf = MF::new();
    let rdf = RDF::new();

    let manifest_node = manifest_graph
        .subjects_for_predicate_object(rdf.type_, mf.manifest)
        .next()
        .ok_or_else(|| format!("mf:Manifest not found in {}", path.display()))?;

    let mut includes = Vec::new();
    for obj in manifest_graph.objects_for_subject_predicate(manifest_node, mf.include) {
        if let TermRef::NamedNode(nn) = obj {
            let iri = nn.as_str();
            if iri == manifest_url {
                includes.push(path.to_path_buf());
            } else if let Some(pb) = iri_to_file_path(iri) {
                includes.push(pb);
            }
        }
    }
    Ok(includes)
}
