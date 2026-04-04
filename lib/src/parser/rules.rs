use super::{ToSubjectRef, parse_shacl_path_recursive, term_is_true};
use crate::context::ParsingContext;
use crate::model::rules::{
    Rule, RuleCondition, RuleOrder, SparqlRule, TriplePatternTerm, TripleRule,
};
use crate::named_nodes::{RDF, SHACL};
use crate::parser::components::resolve_shape_reference;
use crate::sparql::SparqlExecutor;
use crate::types::{Path, RuleID};
use oxigraph::model::{GraphName, GraphNameRef, NamedNodeRef, NamedOrBlankNodeRef, Term, TermRef};
use std::cmp::Ordering;
use std::collections::HashMap;

pub(crate) fn parse_rules_for_shape(
    context: &mut ParsingContext,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<Vec<RuleID>, String> {
    let shacl = SHACL::new();
    let shape_graph = GraphName::NamedNode(context.shape_graph_iri.clone());
    let mut rule_ids = Vec::new();
    let owner_ref = owner_shape.as_ref();
    let subject_ref = owner_ref.to_subject_ref();

    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(shacl.rule),
            None,
            Some(shape_graph.as_ref()),
        )
        .filter_map(Result::ok)
    {
        let rule_term = quad.object;
        let rule_id = parse_rule(context, rule_term, owner_shape, unique_lang)?;
        rule_ids.push(rule_id);
    }

    rule_ids.sort_by(|a, b| compare_rules(context, *a, *b));
    Ok(rule_ids)
}

fn compare_rules(context: &ParsingContext, a: RuleID, b: RuleID) -> Ordering {
    let order_a = context
        .rules
        .get(&a)
        .and_then(|rule| rule.order().map(|o| o.0));
    let order_b = context
        .rules
        .get(&b)
        .and_then(|rule| rule.order().map(|o| o.0));

    match (order_a, order_b) {
        (Some(x), Some(y)) => x
            .partial_cmp(&y)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.0.cmp(&b.0),
    }
}

fn parse_rule(
    context: &mut ParsingContext,
    rule_term: Term,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<RuleID, String> {
    let rule_id = context.get_or_create_rule_id(rule_term.clone());
    if context.rules.contains_key(&rule_id) {
        return Ok(rule_id);
    }

    let shacl = SHACL::new();
    let rdf = RDF::new();
    let rule_term_ref = rule_term.as_ref();
    let subject_ref = rule_term_ref.to_subject_ref();

    let mut is_triple = false;
    let mut is_sparql = false;

    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(rdf.type_),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        if let Term::NamedNode(nn) = quad.object {
            if nn.as_str() == shacl.triple_rule.as_str() {
                is_triple = true;
            }
            if nn.as_str() == shacl.sparql_rule.as_str() {
                is_sparql = true;
            }
        }
    }

    let rule = if is_triple {
        Rule::Triple(parse_triple_rule(
            context,
            rule_id,
            &rule_term,
            owner_shape,
            unique_lang,
        )?)
    } else if is_sparql {
        Rule::Sparql(parse_sparql_rule(
            context,
            rule_id,
            &rule_term,
            owner_shape,
            unique_lang,
        )?)
    } else {
        return Err(format!(
            "Rule {} must declare rdf:type sh:TripleRule or sh:SPARQLRule",
            rule_term
        ));
    };

    context.rules.insert(rule_id, rule);
    Ok(rule_id)
}

fn parse_triple_rule(
    context: &mut ParsingContext,
    rule_id: RuleID,
    rule_term: &Term,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<TripleRule, String> {
    let shacl = SHACL::new();
    let rule_term_ref = rule_term.as_ref();
    let subject_ref = rule_term_ref.to_subject_ref();

    let subject_template = single_object(
        context,
        subject_ref,
        shacl.rule_subject,
        context.shape_graph_iri_ref(),
    )?
    .ok_or_else(|| format!("Triple rule {} must provide sh:subject", rule_term))?;
    let predicate_term = single_object(
        context,
        subject_ref,
        shacl.rule_predicate,
        context.shape_graph_iri_ref(),
    )?
    .ok_or_else(|| format!("Triple rule {} must provide sh:predicate", rule_term))?;
    let object_template = single_object(
        context,
        subject_ref,
        shacl.rule_object,
        context.shape_graph_iri_ref(),
    )?
    .ok_or_else(|| format!("Triple rule {} must provide sh:object", rule_term))?;

    let predicate = match predicate_term {
        Term::NamedNode(nn) => nn,
        other => {
            return Err(format!(
                "Triple rule {} predicate must be an IRI, found {:?}",
                rule_term, other
            ));
        }
    };

    let subject_pattern = parse_triple_pattern_term(context, subject_template)?;
    let object_pattern = parse_triple_pattern_term(context, object_template)?;

    let deactivated = is_deactivated(context, subject_ref, context.shape_graph_iri_ref());
    let order = parse_order(context, subject_ref, context.shape_graph_iri_ref())?;
    let conditions = collect_conditions(context, subject_ref, owner_shape, unique_lang)?;

    Ok(TripleRule {
        id: rule_id,
        subject: subject_pattern,
        predicate,
        object: object_pattern,
        condition_shapes: conditions,
        deactivated,
        order,
        source_term: rule_term.clone(),
    })
}

fn parse_sparql_rule(
    context: &mut ParsingContext,
    rule_id: RuleID,
    rule_term: &Term,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<SparqlRule, String> {
    let shacl = SHACL::new();
    let rule_term_ref = rule_term.as_ref();
    let subject_ref = rule_term_ref.to_subject_ref();

    let construct_term = single_object(
        context,
        subject_ref,
        shacl.construct,
        context.shape_graph_iri_ref(),
    )?
    .ok_or_else(|| format!("SPARQL rule {} must provide sh:construct", rule_term))?;

    let construct_literal = match construct_term {
        Term::Literal(lit) => lit.value().to_string(),
        other => {
            return Err(format!(
                "SPARQL rule {} sh:construct must be a literal, found {:?}",
                rule_term, other
            ));
        }
    };

    let prefixes = context
        .sparql
        .prefixes_for_node(
            rule_term,
            &context.store,
            &context.env,
            context.shape_graph_iri_ref(),
        )
        .map_err(|e| {
            format!(
                "Failed to resolve prefixes for SPARQL rule {}: {}",
                rule_term, e
            )
        })?;

    let query = if prefixes.trim().is_empty() {
        construct_literal
    } else {
        format!("{}\n{}", prefixes, construct_literal)
    };

    let deactivated = is_deactivated(context, subject_ref, context.shape_graph_iri_ref());
    let order = parse_order(context, subject_ref, context.shape_graph_iri_ref())?;
    let conditions = collect_conditions(context, subject_ref, owner_shape, unique_lang)?;

    Ok(SparqlRule {
        id: rule_id,
        query,
        source_term: rule_term.clone(),
        condition_shapes: conditions,
        deactivated,
        order,
    })
}

fn parse_triple_pattern_term(
    context: &mut ParsingContext,
    template: Term,
) -> Result<TriplePatternTerm, String> {
    let shacl = SHACL::new();
    if let Term::NamedNode(nn) = template.clone() {
        if nn.as_str() == shacl.this.as_str() {
            return Ok(TriplePatternTerm::This);
        }
        return Ok(TriplePatternTerm::Constant(template));
    }

    if matches!(template, Term::Literal(_)) {
        return Ok(TriplePatternTerm::Constant(template));
    }

    if matches!(template, Term::BlankNode(_)) {
        let path = parse_blank_node_path(context, template.as_ref())?;
        return Ok(TriplePatternTerm::Path(path));
    }

    Err(format!(
        "Unsupported triple rule term {:?}; only sh:this, IRIs, literals, or path blanks are supported",
        template
    ))
}

fn parse_blank_node_path(context: &ParsingContext, term_ref: TermRef) -> Result<Path, String> {
    let shacl = SHACL::new();
    if let Some(quad) = context
        .store
        .quads_for_pattern(
            Some(term_ref.to_subject_ref()),
            Some(shacl.path),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .next()
    {
        return parse_shacl_path_recursive(context, quad.object.as_ref());
    }

    Err(format!(
        "Unsupported blank node expression {:?} in triple rule; expected sh:path",
        term_ref
    ))
}

fn collect_conditions(
    context: &mut ParsingContext,
    rule_subject: NamedOrBlankNodeRef<'_>,
    owner_shape: &Term,
    unique_lang: &HashMap<Term, String>,
) -> Result<Vec<RuleCondition>, String> {
    let shacl = SHACL::new();
    let graph = context.shape_graph_iri_ref();
    let mut conditions = Vec::new();
    for quad in context
        .store
        .quads_for_pattern(Some(rule_subject), Some(shacl.condition), None, Some(graph))
        .filter_map(Result::ok)
    {
        let shape_id = resolve_shape_reference(context, &quad.object, owner_shape, unique_lang)?;
        conditions.push(RuleCondition::NodeShape(shape_id));
    }
    Ok(conditions)
}

fn is_deactivated(
    context: &ParsingContext,
    rule_subject: NamedOrBlankNodeRef<'_>,
    graph: GraphNameRef<'_>,
) -> bool {
    let shacl = SHACL::new();
    context
        .store
        .quads_for_pattern(
            Some(rule_subject),
            Some(shacl.deactivated),
            None,
            Some(graph),
        )
        .filter_map(Result::ok)
        .any(|quad| term_is_true(&quad.object))
}

fn parse_order(
    context: &ParsingContext,
    rule_subject: NamedOrBlankNodeRef<'_>,
    graph: GraphNameRef<'_>,
) -> Result<Option<RuleOrder>, String> {
    let shacl = SHACL::new();
    let order_term = context
        .store
        .quads_for_pattern(Some(rule_subject), Some(shacl.order), None, Some(graph))
        .filter_map(Result::ok)
        .map(|quad| quad.object)
        .next();

    if let Some(term) = order_term {
        match term {
            Term::Literal(lit) => {
                let value = lit.value().parse::<f64>().map_err(|e| {
                    format!("Failed to parse sh:order literal {}: {}", lit.value(), e)
                })?;
                Ok(Some(RuleOrder(value)))
            }
            other => Err(format!(
                "Rule sh:order must be a literal value, found {:?}",
                other
            )),
        }
    } else {
        Ok(None)
    }
}

fn single_object(
    context: &ParsingContext,
    subject: NamedOrBlankNodeRef<'_>,
    predicate: NamedNodeRef<'_>,
    graph: GraphNameRef<'_>,
) -> Result<Option<Term>, String> {
    Ok(context
        .store
        .quads_for_pattern(Some(subject), Some(predicate), None, Some(graph))
        .filter_map(Result::ok)
        .map(|q| q.object)
        .next())
}
