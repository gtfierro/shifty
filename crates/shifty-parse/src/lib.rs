//! RDF shapes graph -> formalism IR lowering (Layer 2).
//!
//! [`parse_turtle`] reads a SHACL shapes graph and lowers all supported Core +
//! AF vocabulary into the [`shifty_algebra::Schema`] IR, applying every sugar
//! rule from `docs/01-gap-analysis.md`. Unsupported custom components, JS, and
//! richer AF constructs produce [`Diagnostic`]s rather than silent wrong
//! answers.

pub mod diagnostics;
pub mod graph;
pub mod lower;
pub mod path;
pub mod vocab;

pub use diagnostics::{DiagLevel, Diagnostic, ParseError};
pub use graph::{Loaded, RdfFormat};

use shifty_algebra::Schema;

/// The result of lowering a shapes graph.
pub struct ParseOutput {
    pub schema: Schema,
    pub diagnostics: Vec<Diagnostic>,
}

/// Load a Turtle shapes graph (for inspecting the raw RDF stage).
pub fn load_turtle(data: &[u8], base: Option<&str>) -> Result<Loaded, ParseError> {
    Loaded::from_turtle(data, base)
}

pub fn load_ntriples(data: &[u8]) -> Result<Loaded, ParseError> {
    Loaded::from_ntriples(data)
}

/// Load an RDF graph using content type, source extension, lightweight content
/// sniffing, and finally a cascading try across common RDF formats.
pub fn load_rdf_auto(
    data: &[u8],
    content_type: Option<&str>,
    source: Option<&str>,
    base: Option<&str>,
) -> Result<Loaded, ParseError> {
    Loaded::from_rdf_auto(data, content_type, source, base)
}

/// Parse and lower a Turtle shapes graph into the algebra IR.
pub fn parse_turtle(data: &[u8], base: Option<&str>) -> Result<ParseOutput, ParseError> {
    let loaded = Loaded::from_turtle(data, base)?;
    let lowered = lower::lower(&loaded);
    Ok(ParseOutput {
        schema: lowered.schema,
        diagnostics: lowered.diagnostics,
    })
}

/// Lower an already-loaded graph into the algebra IR.
pub fn parse_loaded(loaded: &Loaded) -> ParseOutput {
    let lowered = lower::lower(loaded);
    ParseOutput {
        schema: lowered.schema,
        diagnostics: lowered.diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_algebra::render::schema_to_text;

    const SHAPES: &str = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
                sh:maxCount 1 ;
                sh:datatype xsd:string ;
            ] ;
            sh:property [
                sh:path [ sh:inversePath ex:child ] ;
                sh:nodeKind sh:IRI ;
            ] .
    "#;

    #[test]
    fn lowers_person_shape() {
        let out = parse_turtle(SHAPES.as_bytes(), None).unwrap();
        assert!(out.diagnostics.is_empty(), "diags: {:?}", out.diagnostics);

        let text = schema_to_text(&out.schema);
        // a class-target statement was produced
        assert!(text.contains("rdf:type/rdfs:subClassOf*"), "text:\n{text}");
        // cardinality on ex:name lowered to an interval count
        assert!(text.contains("[1..1] <http://ex/name>"), "text:\n{text}");
        // inverse path rendered
        assert!(text.contains("^<http://ex/child>"), "text:\n{text}");
        // datatype facet present
        assert!(text.contains("datatype(xsd:string)"), "text:\n{text}");
    }

    #[test]
    fn auto_loads_rdf_xml_shapes() {
        let rdfxml = br#"<?xml version="1.0"?>
            <rdf:RDF
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:sh="http://www.w3.org/ns/shacl#"
                xmlns:ex="http://ex/">
              <sh:NodeShape rdf:about="http://ex/S">
                <sh:targetClass rdf:resource="http://ex/Thing"/>
              </sh:NodeShape>
            </rdf:RDF>"#;
        let loaded = load_rdf_auto(
            rdfxml,
            Some("application/rdf+xml"),
            Some("https://example.test/shapes.rdf"),
            Some("https://example.test/shapes.rdf"),
        )
        .unwrap();
        let out = parse_loaded(&loaded);
        let text = schema_to_text(&out.schema);
        assert!(text.contains("rdf:type/rdfs:subClassOf*"), "text:\n{text}");
    }

    #[test]
    fn lowers_triple_rule() {
        let ttl = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ;
                sh:targetClass ex:Rectangle ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:area ;
                    sh:object [ sh:path ex:width ] ;
                    sh:condition ex:S ;
                    sh:order 1 ;
                ] .
        "#;
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(out.diagnostics.is_empty(), "diags: {:?}", out.diagnostics);
        assert_eq!(out.schema.rules.len(), 1);
        let r = &out.schema.rules[0];
        assert_eq!(r.order, Some(1));
        assert_eq!(r.conditions.len(), 1);
        use shifty_algebra::{NodeExpr, RuleHead};
        match &r.head {
            RuleHead::Triple {
                subject,
                predicate,
                object,
            } => {
                assert!(matches!(subject, NodeExpr::This));
                assert!(matches!(predicate, NodeExpr::Constant(_)));
                assert!(matches!(object, NodeExpr::Path(_)));
            }
            other => panic!("expected TripleRule, got {other:?}"),
        }
    }

    #[test]
    fn lowers_sparql_rule_opaque() {
        let ttl = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:rule [ a sh:SPARQLRule ; sh:construct "CONSTRUCT { ?this ex:p ?this } WHERE {}" ] .
        "#;
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert_eq!(out.schema.rules.len(), 1);
        assert!(matches!(
            out.schema.rules[0].head,
            shifty_algebra::RuleHead::Sparql(_)
        ));
    }

    #[test]
    fn lowers_sparql_constraint() {
        let ttl = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:sparql [ sh:select "SELECT $this WHERE {}" ] .
        "#;
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(out.diagnostics.is_empty(), "diags: {:?}", out.diagnostics);
        let root = out.schema.statements[0].shape;
        let shifty_algebra::Shape::Annotated { severity, shape } = out.schema.arena.get(root)
        else {
            panic!("expected severity annotation");
        };
        assert_eq!(severity, &shifty_algebra::Severity::Violation);
        assert!(matches!(
            out.schema.arena.get(*shape),
            shifty_algebra::Shape::Sparql(_)
        ));
    }

    #[test]
    fn lowers_sparql_target() {
        let ttl = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ;
                sh:target [ sh:select "SELECT ?this WHERE { ?this a ex:Person }" ] .
        "#;
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(out.diagnostics.is_empty(), "diags: {:?}", out.diagnostics);
        assert!(matches!(
            out.schema.statements[0].selector,
            shifty_algebra::Selector::Sparql(_)
        ));
    }
}
