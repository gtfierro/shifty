//! SHACL-AF result annotation declarations, shared by all result producers.
use crate::{Loaded, vocab};
use oxrdf::{NamedOrBlankNode, Term};
use shifty_algebra::ResultAnnotation;

pub fn result_annotations(
    graph: &Loaded,
    owner: &NamedOrBlankNode,
) -> Result<Vec<ResultAnnotation>, String> {
    let mut annotations = Vec::new();
    for term in graph.objects(owner, vocab::SH_RESULT_ANNOTATION) {
        let node = match term {
            Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
            Term::BlankNode(n) => NamedOrBlankNode::BlankNode(n),
            _ => {
                return Err(format!(
                    "sh:resultAnnotation on {owner} must reference a resource"
                ));
            }
        };
        let properties = graph.objects(&node, vocab::SH_ANNOTATION_PROPERTY);
        let [Term::NamedNode(property)] = properties.as_slice() else {
            return Err(format!(
                "result annotation {node} requires exactly one IRI sh:annotationProperty"
            ));
        };
        let names = graph.objects(&node, vocab::SH_ANNOTATION_VAR_NAME);
        let variable = match names.as_slice() {
            [] => None,
            [Term::Literal(name)] if name.datatype() == oxrdf::vocab::xsd::STRING => {
                Some(name.value().to_string())
            }
            _ => {
                return Err(format!(
                    "result annotation {node} accepts at most one xsd:string sh:annotationVarName"
                ));
            }
        };
        annotations.push(ResultAnnotation {
            property: property.clone(),
            variable,
            defaults: graph.objects(&node, vocab::SH_ANNOTATION_VALUE),
        });
    }
    // RDF traversal order is not part of the result contract.
    annotations.sort_by_key(|annotation| format!("{annotation:?}"));
    Ok(annotations)
}
