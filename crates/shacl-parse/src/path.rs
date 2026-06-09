//! Parse a `sh:path` value into the [`Path`] algebra (gap-analysis **P1** for the
//! `oneOrMore`/`zeroOrOne` sugar).

use crate::graph::{term_to_node, Loaded};
use crate::vocab;
use oxrdf::Term;
use shacl_algebra::Path;

/// Parse a SHACL path term. Returns an error string for malformed paths.
pub fn parse_path(g: &Loaded, term: &Term) -> Result<Path, String> {
    match term {
        Term::NamedNode(n) => Ok(Path::Pred(n.clone())),
        Term::Literal(_) => Err("sh:path value is a literal".to_string()),
        Term::BlankNode(_) => {
            let node = term_to_node(term).expect("blank node is a node");

            if let Some(x) = g.object(&node, vocab::SH_INVERSE_PATH) {
                return Ok(parse_path(g, &x)?.inverse());
            }
            if let Some(list) = g.object(&node, vocab::SH_ALTERNATIVE_PATH) {
                let members = g.read_list(&list);
                let parts = members
                    .iter()
                    .map(|m| parse_path(g, m))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(Path::alt(parts));
            }
            if let Some(x) = g.object(&node, vocab::SH_ZERO_OR_MORE_PATH) {
                return Ok(parse_path(g, &x)?.star());
            }
            if let Some(x) = g.object(&node, vocab::SH_ONE_OR_MORE_PATH) {
                return Ok(parse_path(g, &x)?.one_or_more());
            }
            if let Some(x) = g.object(&node, vocab::SH_ZERO_OR_ONE_PATH) {
                return Ok(parse_path(g, &x)?.zero_or_one());
            }
            // Otherwise the blank node should be an rdf:list = a sequence path.
            if g.object(&node, vocab::RDF_FIRST).is_some() {
                let members = g.read_list(term);
                let parts = members
                    .iter()
                    .map(|m| parse_path(g, m))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(Path::seq(parts));
            }
            Err("unrecognized blank-node path expression".to_string())
        }
    }
}
