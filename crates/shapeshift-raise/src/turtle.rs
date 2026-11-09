//! A small Turtle writer that produces what a person would write: blank nodes
//! referenced once are nested as `[ ... ]`, well-formed RDF lists are written
//! as `( ... )`, subjects keep insertion order, and `rdf:type` comes first.

use oxrdf::{BlankNode, Literal, NamedOrBlankNode, Term, Triple};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

pub struct Writer<'a> {
    prefixes: &'a [(String, String)],
    by_subject: HashMap<NamedOrBlankNode, Vec<(oxrdf::NamedNode, Term)>>,
    order: Vec<NamedOrBlankNode>,
    inline: HashSet<BlankNode>,
    lists: HashMap<BlankNode, Vec<Term>>,
}

fn is_pn_local(s: &str) -> bool {
    !s.is_empty()
        && !s.ends_with('.')
        && !s.starts_with('.')
        && !s.starts_with('-')
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
}

impl<'a> Writer<'a> {
    pub fn new(triples: &[Triple], prefixes: &'a [(String, String)]) -> Self {
        let mut by_subject: HashMap<NamedOrBlankNode, Vec<(oxrdf::NamedNode, Term)>> =
            HashMap::new();
        let mut order = Vec::new();
        let mut refs: HashMap<BlankNode, usize> = HashMap::new();
        for t in triples {
            let entry = by_subject.entry(t.subject.clone()).or_insert_with(|| {
                order.push(t.subject.clone());
                Vec::new()
            });
            if !entry.contains(&(t.predicate.clone(), t.object.clone())) {
                entry.push((t.predicate.clone(), t.object.clone()));
            }
            if let Term::BlankNode(b) = &t.object {
                *refs.entry(b.clone()).or_default() += 1;
            }
        }
        let mut inline = HashSet::new();
        for (b, n) in &refs {
            if *n == 1 && by_subject.contains_key(&NamedOrBlankNode::BlankNode(b.clone())) {
                inline.insert(b.clone());
            }
        }
        let mut w = Writer {
            prefixes,
            by_subject,
            order,
            inline,
            lists: HashMap::new(),
        };
        // Lists: a chain of blank nodes each with exactly rdf:first + rdf:rest.
        let candidates: Vec<BlankNode> = w.inline.iter().cloned().collect();
        for b in candidates {
            if let Some(items) = w.read_list(&b) {
                w.lists.insert(b, items);
            }
        }
        // Guard against inlining cycles: a blank node reachable from itself
        // through inlined references must be written at top level.
        let inl: Vec<BlankNode> = w.inline.iter().cloned().collect();
        for b in inl {
            if w.reaches_self(&b) {
                w.inline.remove(&b);
            }
        }
        w
    }

    fn reaches_self(&self, start: &BlankNode) -> bool {
        let mut stack = vec![start.clone()];
        let mut seen = HashSet::new();
        while let Some(b) = stack.pop() {
            if let Some(po) = self.by_subject.get(&NamedOrBlankNode::BlankNode(b.clone())) {
                for (_, o) in po {
                    if let Term::BlankNode(c) = o {
                        if c == start {
                            return true;
                        }
                        if self.inline.contains(c) && seen.insert(c.clone()) {
                            stack.push(c.clone());
                        }
                    }
                }
            }
        }
        false
    }

    fn read_list(&self, head: &BlankNode) -> Option<Vec<Term>> {
        let mut items = Vec::new();
        let mut cur = head.clone();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(cur.clone()) {
                return None;
            }
            let po = self
                .by_subject
                .get(&NamedOrBlankNode::BlankNode(cur.clone()))?;
            if po.len() != 2 {
                return None;
            }
            let first = po.iter().find(|(p, _)| p.as_str() == RDF_FIRST)?;
            let rest = po.iter().find(|(p, _)| p.as_str() == RDF_REST)?;
            items.push(first.1.clone());
            match &rest.1 {
                Term::NamedNode(n) if n.as_str() == RDF_NIL => return Some(items),
                Term::BlankNode(b) if cur == *head || self.inline.contains(b) => {
                    cur = b.clone();
                }
                _ => return None,
            }
        }
    }

    fn is_list_member(&self, b: &BlankNode) -> bool {
        // Interior cells of a list are written as part of their head.
        self.lists.keys().any(|head| {
            let mut cur = head.clone();
            loop {
                let Some(po) = self
                    .by_subject
                    .get(&NamedOrBlankNode::BlankNode(cur.clone()))
                else {
                    return false;
                };
                match po
                    .iter()
                    .find(|(p, _)| p.as_str() == RDF_REST)
                    .map(|x| &x.1)
                {
                    Some(Term::BlankNode(n)) => {
                        if n == b {
                            return true;
                        }
                        cur = n.clone();
                    }
                    _ => return false,
                }
            }
        })
    }

    fn predicate(&self, iri: &str) -> String {
        if iri == RDF_TYPE {
            "a".into()
        } else {
            self.iri(iri)
        }
    }

    pub fn iri(&self, iri: &str) -> String {
        let mut best: Option<(usize, String)> = None;
        for (p, ns) in self.prefixes {
            if let Some(local) = iri.strip_prefix(ns.as_str())
                && (local.is_empty() || is_pn_local(local))
                && best.as_ref().is_none_or(|(l, _)| ns.len() > *l)
            {
                best = Some((ns.len(), format!("{p}:{local}")));
            }
        }
        best.map(|(_, s)| s).unwrap_or_else(|| format!("<{iri}>"))
    }

    fn literal(&self, l: &Literal) -> String {
        let dt = l.datatype().as_str();
        let v = l.value();
        if let Some(lang) = l.language() {
            return format!("{}@{lang}", quote(v));
        }
        let plain_number = |s: &str| {
            s.chars()
                .all(|c| c.is_ascii_digit() || c == '-' || c == '+')
                && s.chars().any(|c| c.is_ascii_digit())
        };
        if dt == format!("{XSD}integer") && plain_number(v) {
            return v.to_string();
        }
        if dt == format!("{XSD}decimal")
            && v.contains('.')
            && v.chars()
                .all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')
        {
            return v.to_string();
        }
        if dt == format!("{XSD}boolean") && (v == "true" || v == "false") {
            return v.to_string();
        }
        if dt == format!("{XSD}string") {
            return quote(v);
        }
        format!("{}^^{}", quote(v), self.iri(dt))
    }

    fn term(&self, t: &Term, depth: usize, out: &mut String) {
        match t {
            Term::NamedNode(n) => out.push_str(&self.iri(n.as_str())),
            Term::Literal(l) => out.push_str(&self.literal(l)),
            Term::BlankNode(b) => {
                if let Some(items) = self.lists.get(b) {
                    out.push('(');
                    for item in items {
                        out.push(' ');
                        self.term(item, depth, out);
                    }
                    out.push_str(" )");
                } else if self.inline.contains(b) {
                    let po = &self.by_subject[&NamedOrBlankNode::BlankNode(b.clone())];
                    out.push_str("[\n");
                    self.predicates(po, depth + 1, out);
                    write!(out, "\n{}]", "    ".repeat(depth)).unwrap();
                } else {
                    write!(out, "_:{}", b.as_str()).unwrap();
                }
            }
        }
    }

    fn predicates(&self, po: &[(oxrdf::NamedNode, Term)], depth: usize, out: &mut String) {
        let indent = "    ".repeat(depth);
        let mut preds: Vec<&oxrdf::NamedNode> = Vec::new();
        for (p, _) in po {
            if !preds.contains(&p) {
                preds.push(p);
            }
        }
        preds.sort_by_key(|p| p.as_str() != RDF_TYPE);
        for (i, p) in preds.iter().enumerate() {
            if i > 0 {
                out.push_str(" ;\n");
            }
            write!(out, "{indent}{} ", self.predicate(p.as_str())).unwrap();
            let objs: Vec<&Term> = po.iter().filter(|(q, _)| q == *p).map(|(_, o)| o).collect();
            for (j, o) in objs.iter().enumerate() {
                if j > 0 {
                    out.push_str(", ");
                }
                self.term(o, depth, out);
            }
        }
    }

    pub fn write(&self) -> String {
        let mut out = String::new();
        for (p, ns) in self.prefixes {
            writeln!(out, "@prefix {p}: <{ns}> .").unwrap();
        }
        for s in &self.order {
            if let NamedOrBlankNode::BlankNode(b) = s
                && (self.inline.contains(b) || self.is_list_member(b))
            {
                continue;
            }
            out.push('\n');
            match s {
                NamedOrBlankNode::NamedNode(n) => out.push_str(&self.iri(n.as_str())),
                NamedOrBlankNode::BlankNode(b) => write!(out, "_:{}", b.as_str()).unwrap(),
            }
            out.push('\n');
            self.predicates(&self.by_subject[s], 1, &mut out);
            out.push_str(" .\n");
        }
        out
    }
}

pub fn quote(s: &str) -> String {
    let multiline = s.contains('\n');
    let mut out = String::new();
    if multiline {
        out.push_str("\"\"\"");
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                c => out.push(c),
            }
        }
        out.push_str("\"\"\"");
        return out;
    }
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
