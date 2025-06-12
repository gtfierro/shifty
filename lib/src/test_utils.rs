use crate::named_nodes::{MF, RDF, RDFS, SHACL, SHT};
use crate::components::ToSubjectRef;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{Graph, SubjectRef, TermRef, TripleRef};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use url::Url;

#[derive(Debug)]
pub struct TestCase {
    pub name: String,
    pub status: String,
    pub data_graph_path: PathBuf,
    pub shapes_graph_path: PathBuf,
    pub expected_report: Graph,
}

#[derive(Debug)]
pub struct Manifest {
    pub path: PathBuf,
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

fn extract_path_graph(manifest_graph: &Graph, path_node: SubjectRef, report_graph: &mut Graph) {
    let sh = SHACL::new();
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
                if triple.predicate == sh.result_path {
                    if let TermRef::NamedNode(_) | TermRef::BlankNode(_) = triple.object {
                        let path_subject = triple.object.to_subject_ref();
                        extract_path_graph(manifest_graph, path_subject, &mut report_graph);
                    }
                }
            }
        }
    }
    report_graph
}

pub fn load_manifest(path: &Path) -> Result<Manifest, String> {
    let manifest_content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read manifest file {}: {}", path.display(), e))?;

    let manifest_url = Url::from_file_path(path.canonicalize().map_err(|e| e.to_string())?)
        .map_err(|_| "Invalid path".to_string())?
        .to_string();

    let mut manifest_graph = Graph::new();
    let parser = RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri(&manifest_url)
        .map_err(|e| e.to_string())?;
    for quad in parser.for_reader(Cursor::new(manifest_content)) {
        let quad = quad.map_err(|e| e.to_string())?;
        manifest_graph.insert(quad.into());
    }

    let mf = MF::new();
    let sht = SHT::new();
    let rdf = RDF::new();
    let rdfs = RDFS::new();

    let manifest_node = manifest_graph
        .subjects_for_predicate_object(rdf.type_, mf.manifest)
        .next()
        .ok_or_else(|| format!("mf:Manifest not found in {}", path.display()))?;

    let mut test_cases = Vec::new();

    // Handle test entries
    if let Some(entries_list_head) =
        manifest_graph.object_for_subject_predicate(manifest_node, mf.entries)
    {
        if let TermRef::NamedNode(_) | TermRef::BlankNode(_) = entries_list_head {
            let mut current_node = entries_list_head.to_subject_ref();
            let nil_ref: SubjectRef = rdf.nil.into();
            while current_node != nil_ref {
                let entry = manifest_graph
                    .object_for_subject_predicate(current_node, rdf.first)
                    .and_then(|t| match t {
                        TermRef::NamedNode(_) | TermRef::BlankNode(_) => Some(t.to_subject_ref()),
                        _ => None,
                    })
                    .ok_or_else(|| "Invalid RDF list for mf:entries: missing rdf:first".to_string())?;

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

                    let status = manifest_graph
                        .object_for_subject_predicate(entry, mf.status)
                        .and_then(|t| match t {
                            TermRef::NamedNode(nn) => Some(nn.as_str().to_string()),
                            _ => None,
                        })
                        .ok_or_else(|| format!("Test '{}' has no mf:status", name))?;

                    let action_node = manifest_graph
                        .object_for_subject_predicate(entry, mf.action)
                        .and_then(|t| match t {
                            TermRef::NamedNode(_) | TermRef::BlankNode(_) => Some(t.to_subject_ref()),
                            _ => None,
                        })
                        .ok_or_else(|| format!("Test '{}' has no mf:action", name))?;

                    let data_graph_term = manifest_graph
                        .object_for_subject_predicate(action_node, sht.data_graph)
                        .ok_or_else(|| format!("Test '{}' has no sht:dataGraph", name))?;

                    let shapes_graph_term = manifest_graph
                        .object_for_subject_predicate(action_node, sht.shapes_graph)
                        .ok_or_else(|| format!("Test '{}' has no sht:shapesGraph", name))?;

                    let result_node = manifest_graph
                        .object_for_subject_predicate(entry, mf.result)
                        .and_then(|t| match t {
                            TermRef::NamedNode(_) | TermRef::BlankNode(_) => Some(t.to_subject_ref()),
                            _ => None,
                        })
                        .ok_or_else(|| format!("Test '{}' has no mf:result", name))?;

                    let data_graph_path_str = match data_graph_term {
                        TermRef::NamedNode(nn) => nn.as_str(),
                        _ => {
                            return Err(format!(
                                "Test '{}' has a non-IRI value for sht:dataGraph",
                                name
                            ))
                        }
                    };
                    let data_graph_path = resolve_path(path, data_graph_path_str);

                    let shapes_graph_path_str = match shapes_graph_term {
                        TermRef::NamedNode(nn) => nn.as_str(),
                        _ => {
                            return Err(format!(
                                "Test '{}' has a non-IRI value for sht:shapesGraph",
                                name
                            ))
                        }
                    };
                    let shapes_graph_path = resolve_path(path, shapes_graph_path_str);

                    let expected_report = extract_report_graph(&manifest_graph, result_node);

                    test_cases.push(TestCase {
                        name,
                        status,
                        data_graph_path,
                        shapes_graph_path,
                        expected_report,
                    });
                }

                current_node = manifest_graph
                    .object_for_subject_predicate(current_node, rdf.rest)
                    .and_then(|t| match t {
                        TermRef::NamedNode(_) | TermRef::BlankNode(_) => Some(t.to_subject_ref()),
                        _ => None,
                    })
                    .ok_or_else(|| "Invalid RDF list for mf:entries: missing rdf:rest".to_string())?;
            }
        }
    }

    Ok(Manifest {
        path: path.to_path_buf(),
        test_cases,
    })
}
