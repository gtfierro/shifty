use clap::Parser;
use oxigraph::model::Term;
use serde::Serialize;
use shifty::shacl_ir::{Path, Target};
use shifty::Source;
use shifty::{ir::IRComponentDescriptor as ComponentDescriptor, Validator};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// Path to the SHACL shapes graph (Turtle)
    shapes: PathBuf,
    /// Where to write the generated TypeScript (stdout if omitted)
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TermDesc {
    kind: &'static str,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    datatype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ConstraintDesc {
    Class {
        class: TermDesc,
    },
    Datatype {
        datatype: TermDesc,
    },
    NodeKind {
        node_kind: String,
    },
    MinCount {
        min: u64,
    },
    MaxCount {
        max: u64,
    },
    MinLength {
        min: u64,
    },
    MaxLength {
        max: u64,
    },
    Pattern {
        pattern: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        flags: Option<String>,
    },
    HasValue {
        value: TermDesc,
    },
    In {
        values: Vec<TermDesc>,
    },
    NodeShape {
        #[serde(rename = "shapeId")]
        shape_id: String,
    },
    QualifiedValueShape {
        #[serde(rename = "shapeId")]
        shape_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        disjoint: Option<bool>,
    },
    Not {
        #[serde(rename = "shapeId")]
        shape_id: String,
    },
    And {
        #[serde(rename = "shapeIds")]
        shape_ids: Vec<String>,
    },
    Or {
        #[serde(rename = "shapeIds")]
        shape_ids: Vec<String>,
    },
    Xone {
        #[serde(rename = "shapeIds")]
        shape_ids: Vec<String>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PropertyShapeDesc {
    id: String,
    path: PathDesc,
    constraints: Vec<ConstraintDesc>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetDesc {
    #[serde(rename = "type")]
    target_type: String,
    value: TermDesc,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeShapeDesc {
    id: String,
    targets: Vec<TargetDesc>,
    constraints: Vec<ConstraintDesc>,
    properties: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PathDesc {
    sparql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let validator = Validator::builder()
        .with_shapes_source(Source::File(args.shapes.clone()))
        .with_data_source(Source::Empty)
        .with_af_enabled(false)
        .with_rules_enabled(false)
        .with_do_imports(false)
        .build()?;

    let ir = validator.context().shape_ir().clone();
    let ts = generate_typescript(&ir)?;

    if let Some(out) = args.out {
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&out, ts)?;
        let pkg_path = out
            .parent()
            .map(|p| p.join("package.json"))
            .unwrap_or_else(|| PathBuf::from("package.json"));
        fs::write(pkg_path, package_json())?;
    } else {
        println!("{ts}");
        fs::write("package.json", package_json())?;
    }

    Ok(())
}

fn package_json() -> String {
    r#"{
  "name": "shacl-ts-validator",
  "version": "0.1.0",
  "type": "module",
  "dependencies": {
    "oxigraph": "^0.5.2",
    "@rdfjs/types": "^1.1.0"
  }
}
"#
    .to_string()
}

fn term_to_desc(term: &Term) -> Result<TermDesc, String> {
    match term {
        Term::NamedNode(nn) => Ok(TermDesc {
            kind: "NamedNode",
            value: nn.as_str().to_string(),
            datatype: None,
            language: None,
        }),
        Term::BlankNode(bn) => Ok(TermDesc {
            kind: "BlankNode",
            value: bn.as_str().to_string(),
            datatype: None,
            language: None,
        }),
        Term::Literal(lit) => Ok(TermDesc {
            kind: "Literal",
            value: lit.value().to_string(),
            datatype: Some(lit.datatype().as_str().to_string()),
            language: lit.language().map(|lang| lang.to_string()),
        }),
    }
}

fn node_kind_iri(term: &Term) -> Option<String> {
    let iri = match term {
        Term::NamedNode(nn) => nn.as_str(),
        _ => return None,
    };
    if iri.ends_with("#IRI") {
        Some("iri".to_string())
    } else if iri.ends_with("#BlankNode") {
        Some("blankNode".to_string())
    } else if iri.ends_with("#Literal") {
        Some("literal".to_string())
    } else {
        None
    }
}

fn path_to_desc(path: &Path) -> Result<PathDesc, String> {
    let sparql = path
        .to_sparql_path()
        .map_err(|e| format!("Failed to convert path to SPARQL: {e}"))?;
    let predicate = match path {
        Path::Simple(Term::NamedNode(nn)) => Some(nn.as_str().to_string()),
        _ => None,
    };
    Ok(PathDesc { sparql, predicate })
}

fn constraint_to_desc(
    desc: &ComponentDescriptor,
    node_ids: &HashMap<u64, String>,
    prop_ids: &HashMap<u64, String>,
) -> Result<Option<ConstraintDesc>, String> {
    match desc {
        ComponentDescriptor::Class { class } => Ok(Some(ConstraintDesc::Class {
            class: term_to_desc(class)?,
        })),
        ComponentDescriptor::Datatype { datatype } => Ok(Some(ConstraintDesc::Datatype {
            datatype: term_to_desc(datatype)?,
        })),
        ComponentDescriptor::NodeKind { node_kind } => Ok(node_kind_iri(node_kind)
            .map(|nk| ConstraintDesc::NodeKind { node_kind: nk })
            .ok_or_else(|| format!("Unsupported nodeKind value: {node_kind:?}"))?
            .into()),
        ComponentDescriptor::MinCount { min_count } => {
            Ok(Some(ConstraintDesc::MinCount { min: *min_count }))
        }
        ComponentDescriptor::MaxCount { max_count } => {
            Ok(Some(ConstraintDesc::MaxCount { max: *max_count }))
        }
        ComponentDescriptor::MinLength { length } => {
            Ok(Some(ConstraintDesc::MinLength { min: *length }))
        }
        ComponentDescriptor::MaxLength { length } => {
            Ok(Some(ConstraintDesc::MaxLength { max: *length }))
        }
        ComponentDescriptor::Pattern { pattern, flags } => Ok(Some(ConstraintDesc::Pattern {
            pattern: pattern.clone(),
            flags: flags.clone(),
        })),
        ComponentDescriptor::HasValue { value } => Ok(Some(ConstraintDesc::HasValue {
            value: term_to_desc(value)?,
        })),
        ComponentDescriptor::In { values } => {
            let vs = values
                .iter()
                .map(term_to_desc)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(ConstraintDesc::In { values: vs }))
        }
        ComponentDescriptor::Node { shape } => {
            let sid = node_ids
                .get(&shape.0)
                .ok_or_else(|| format!("Unknown node shape reference {}", shape.0))?
                .clone();
            Ok(Some(ConstraintDesc::NodeShape { shape_id: sid }))
        }
        ComponentDescriptor::Property { shape } => {
            let sid = prop_ids
                .get(&shape.0)
                .ok_or_else(|| format!("Unknown property shape reference {}", shape.0))?
                .clone();
            Ok(Some(ConstraintDesc::NodeShape { shape_id: sid }))
        }
        ComponentDescriptor::QualifiedValueShape {
            shape,
            min_count,
            max_count,
            disjoint,
        } => {
            let sid = node_ids
                .get(&shape.0)
                .ok_or_else(|| format!("Unknown node shape reference {}", shape.0))?
                .clone();
            Ok(Some(ConstraintDesc::QualifiedValueShape {
                shape_id: sid,
                min: *min_count,
                max: *max_count,
                disjoint: *disjoint,
            }))
        }
        ComponentDescriptor::Not { shape } => {
            let sid = node_ids
                .get(&shape.0)
                .ok_or_else(|| format!("Unknown node shape reference {}", shape.0))?
                .clone();
            Ok(Some(ConstraintDesc::Not { shape_id: sid }))
        }
        ComponentDescriptor::And { shapes } => {
            let mut ids = Vec::new();
            for s in shapes {
                let sid = node_ids
                    .get(&s.0)
                    .ok_or_else(|| format!("Unknown node shape reference {}", s.0))?
                    .clone();
                ids.push(sid);
            }
            Ok(Some(ConstraintDesc::And { shape_ids: ids }))
        }
        ComponentDescriptor::Or { shapes } => {
            let mut ids = Vec::new();
            for s in shapes {
                let sid = node_ids
                    .get(&s.0)
                    .ok_or_else(|| format!("Unknown node shape reference {}", s.0))?
                    .clone();
                ids.push(sid);
            }
            Ok(Some(ConstraintDesc::Or { shape_ids: ids }))
        }
        ComponentDescriptor::Xone { shapes } => {
            let mut ids = Vec::new();
            for s in shapes {
                let sid = node_ids
                    .get(&s.0)
                    .ok_or_else(|| format!("Unknown node shape reference {}", s.0))?
                    .clone();
                ids.push(sid);
            }
            Ok(Some(ConstraintDesc::Xone { shape_ids: ids }))
        }
        _ => Err(format!("Unsupported constraint in TS generator: {desc:?}")),
    }
}

pub(crate) fn generate_typescript(ir: &shifty::shacl_ir::ShapeIR) -> Result<String, String> {
    let mut node_id_map: HashMap<u64, String> = HashMap::new();
    let mut prop_id_map: HashMap<u64, String> = HashMap::new();
    for node in &ir.node_shapes {
        node_id_map.insert(node.id.0, format!("ns_{}", node.id.0));
    }
    for prop in &ir.property_shapes {
        prop_id_map.insert(prop.id.0, format!("ps_{}", prop.id.0));
    }

    let mut property_shapes = Vec::new();
    for prop in &ir.property_shapes {
        let path = path_to_desc(&prop.path)?;
        let mut constraints = Vec::new();
        for cid in &prop.constraints {
            let descriptor = ir.components.get(cid).ok_or_else(|| {
                format!(
                    "Component {} not found for property shape {}",
                    cid.0, prop.id.0
                )
            })?;
            if let Some(desc) = constraint_to_desc(descriptor, &node_id_map, &prop_id_map)? {
                constraints.push(desc);
            }
        }
        property_shapes.push(PropertyShapeDesc {
            id: prop_id_map
                .get(&prop.id.0)
                .cloned()
                .unwrap_or_else(|| format!("ps_{}", prop.id.0)),
            path,
            constraints,
        });
    }

    let mut node_shapes = Vec::new();
    for node in &ir.node_shapes {
        let mut targets = Vec::new();
        for target in &node.targets {
            let (target_type, term) = match target {
                Target::Class(t) => ("class".to_string(), term_to_desc(t)?),
                Target::Node(t) => ("node".to_string(), term_to_desc(t)?),
                Target::SubjectsOf(t) => ("subjectsOf".to_string(), term_to_desc(t)?),
                Target::ObjectsOf(t) => ("objectsOf".to_string(), term_to_desc(t)?),
                Target::Advanced(_) => {
                    return Err("Advanced targets are not supported in TS generation".to_string())
                }
            };
            targets.push(TargetDesc {
                target_type,
                value: term,
            });
        }

        let mut constraints = Vec::new();
        let mut property_refs = HashSet::new();
        for cid in &node.constraints {
            let descriptor = ir.components.get(cid).ok_or_else(|| {
                format!("Component {} not found for node shape {}", cid.0, node.id.0)
            })?;
            match descriptor {
                ComponentDescriptor::Property { shape } => {
                    let sid = prop_id_map.get(&shape.0).ok_or_else(|| {
                        format!(
                            "Property shape {} not found for node shape {}",
                            shape.0, node.id.0
                        )
                    })?;
                    property_refs.insert(sid.clone());
                }
                _ => {
                    if let Some(desc) = constraint_to_desc(descriptor, &node_id_map, &prop_id_map)?
                    {
                        constraints.push(desc);
                    }
                }
            }
        }

        node_shapes.push(NodeShapeDesc {
            id: node_id_map
                .get(&node.id.0)
                .cloned()
                .unwrap_or_else(|| format!("ns_{}", node.id.0)),
            targets,
            constraints,
            properties: property_refs.into_iter().collect(),
        });
    }

    let node_json = serde_json::to_string_pretty(&node_shapes).map_err(|e| e.to_string())?;
    let prop_json = serde_json::to_string_pretty(&property_shapes).map_err(|e| e.to_string())?;

    let ts = format!(
        r#"// Auto-generated from SHACL shapes. Base features only.
// Usage: call validate with an RDF/JS DatasetCore; oxigraph's Store/DataFactory work out of the box.
import {{ Store }} from 'oxigraph';
import * as oxigraph from 'oxigraph';
import type {{ DatasetCore, Term, NamedNode, Literal, DataFactory }} from '@rdfjs/types';

const defaultFactory: DataFactory = oxigraph as unknown as DataFactory;

type TermDesc = {{"kind":"NamedNode"|"BlankNode"|"Literal","value":string,"datatype"?:string,"language"?:string}};
type Path = {{"sparql":string,"predicate"?:string}};
type Constraint =
  | {{"type":"class","class":TermDesc}}
  | {{"type":"datatype","datatype":TermDesc}}
  | {{"type":"nodeKind","nodeKind":"iri"|"blankNode"|"literal"}}
  | {{"type":"minCount","min":number}}
  | {{"type":"maxCount","max":number}}
  | {{"type":"minLength","min":number}}
  | {{"type":"maxLength","max":number}}
  | {{"type":"pattern","pattern":string,"flags"?:string}}
  | {{"type":"hasValue","value":TermDesc}}
  | {{"type":"in","values":TermDesc[]}}
  | {{"type":"nodeShape","shapeId":string}}
  | {{"type":"qualifiedValueShape","shapeId":string,"min"?:number,"max"?:number,"disjoint"?:boolean}}
  | {{"type":"not","shapeId":string}}
  | {{"type":"and","shapeIds":string[]}}
  | {{"type":"or","shapeIds":string[]}}
  | {{"type":"xone","shapeIds":string[]}};

type PropertyShape = {{"id":string,"path":Path,"constraints":Constraint[]}};
type TargetDesc = {{"type":"class"|"node"|"subjectsOf"|"objectsOf","value":TermDesc}};
type NodeShape = {{"id":string,"targets":TargetDesc[],"constraints":Constraint[],"properties":string[]}};
type ValidationResult = {{"focusNode":Term,"resultPath"?:NamedNode,"value"?:Term,"message":string,"sourceShape":string,"sourceConstraint":string,"severity":"Violation"}};
type ValidationReport = {{"conforms":boolean,"results":ValidationResult[]}};

const nodeShapes: NodeShape[] = {node_json};
const propertyShapeList: PropertyShape[] = {prop_json};
const propertyShapes: Record<string, PropertyShape> = Object.fromEntries(propertyShapeList.map(ps => [ps.id, ps]));

function termFromDesc(desc: TermDesc, factory: DataFactory): Term {{
  if (desc.kind === 'NamedNode') return factory.namedNode(desc.value);
  if (desc.kind === 'BlankNode') return factory.blankNode(desc.value);
  const dt = desc.datatype ? factory.namedNode(desc.datatype) : undefined;
  return factory.literal(desc.value, desc.language ?? dt);
}}

function termToSparql(term: Term): string {{
  const escape = (s: string) => s.replace(/["\\\\]/g, '\\\\$&');
  switch ((term as any).termType) {{
    case 'NamedNode':
      return `<${{(term as any).value}}>`;
    case 'BlankNode':
      return `_:${{(term as any).value}}`;
    case 'Literal': {{
      const lit = term as any as Literal;
      const escaped = escape(lit.value);
      if (lit.language) return `"${{escaped}}"@${{lit.language}}`;
      return `"${{escaped}}"^^<${{lit.datatype.value}}>`;
    }}
    default:
      return `<${{(term as any).value ?? String(term)}}>`;
  }}
}}

function termEquals(a: Term, b: Term): boolean {{
  if ((a as any).equals) return (a as any).equals(b);
  if (a.termType !== b.termType) return false;
  if (a.termType === 'Literal' && b.termType === 'Literal') {{
    const la = a as Literal;
    const lb = b as Literal;
    return la.value === lb.value && la.language === lb.language && la.datatype.value === lb.datatype.value;
  }}
  return (a as any).value === (b as any).value;
}}

function collectTargets(shape: NodeShape, dataset: DatasetCore, factory: DataFactory): Term[] {{
  const RDF_TYPE = factory.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  const nodes: Term[] = [];
  for (const tgt of shape.targets) {{
    const term = termFromDesc(tgt.value, factory);
    switch (tgt.type) {{
      case 'class': {{
        for (const quad of dataset.match(null, RDF_TYPE, term, null)) {{
          nodes.push(quad.subject);
        }}
        break;
      }}
      case 'node':
        nodes.push(term);
        break;
      case 'subjectsOf': {{
        for (const quad of dataset.match(null, term as any, null, null)) {{
          nodes.push(quad.subject);
        }}
        break;
      }}
      case 'objectsOf': {{
        for (const quad of dataset.match(null, term as any, null, null)) {{
          nodes.push(quad.object);
        }}
        break;
      }}
    }}
  }}
  return nodes;
}}

function valuesForPath(focus: Term, path: Path, dataset: DatasetCore, factory: DataFactory): Term[] {{
  if (path.predicate) {{
    const predicate = factory.namedNode(path.predicate);
    return Array.from(dataset.match(focus as any, predicate, null, null)).map(q => q.object);
  }}
  const maybeQuery = (dataset as any).query as undefined | ((q: string) => Iterable<any>);
  if (!maybeQuery) return [];
  const out: Term[] = [];
  const seen = new Set<string>();
  const query = `SELECT ?o WHERE {{ VALUES ?s {{ ${{termToSparql(focus)}} }} ?s ${{path.sparql}} ?o }}`;
  for (const binding of maybeQuery.call(dataset, query)) {{
    const obj = binding.get('o') as Term | undefined;
    if (!obj) continue;
    const key = (obj as any).value ?? (obj as any).id ?? String(obj);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(obj);
  }}
  return out;
}}

function handleNodeConstraint(focus: Term, constraint: Constraint, dataset: DatasetCore, factory: DataFactory, results: ValidationResult[], shapeId: string, evaluateShape: (shapeId: string, node: Term) => boolean) {{
  const RDF_TYPE = factory.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  switch (constraint.type) {{
    case 'class': {{
      const classTerm = termFromDesc(constraint.class, factory);
      let any = false;
      for (const _ of dataset.match(focus as any, RDF_TYPE, classTerm, null)) {{
        any = true;
        break;
      }}
      if (!any) results.push({{ focusNode: focus, message: `Expected rdf:type ${{classTerm.value}}`, sourceShape: shapeId, sourceConstraint: 'class', severity: 'Violation', resultPath: RDF_TYPE }});
      break;
    }}
    case 'datatype': {{
      const dt = termFromDesc(constraint.datatype, factory);
      const lit = focus as any as Literal;
      if ((focus as any).termType !== 'Literal' || lit.datatype.value !== (dt as any).value) {{
        results.push({{ focusNode: focus, message: `Expected datatype ${{(dt as any).value}}`, sourceShape: shapeId, sourceConstraint: 'datatype', severity: 'Violation' }});
      }}
      break;
    }}
    case 'nodeKind': {{
      const tt = (focus as any).termType;
      const ok =
        (constraint.nodeKind === 'iri' && tt === 'NamedNode') ||
        (constraint.nodeKind === 'blankNode' && tt === 'BlankNode') ||
        (constraint.nodeKind === 'literal' && tt === 'Literal');
      if (!ok) results.push({{ focusNode: focus, message: `Expected node kind ${{constraint.nodeKind}}`, sourceShape: shapeId, sourceConstraint: 'nodeKind', severity: 'Violation' }});
      break;
    }}
    case 'nodeShape': {{
      evaluateShape(constraint.shapeId, focus);
      break;
    }}
    case 'not': {{
      const ok = !evaluateShape(constraint.shapeId, focus);
      if (!ok) results.push({{ focusNode: focus, message: 'NOT constraint violated', sourceShape: shapeId, sourceConstraint: 'not', severity: 'Violation' }});
      break;
    }}
    case 'and': {{
      let allOk = true;
      for (const sid of constraint.shapeIds) {{
        if (!evaluateShape(sid, focus)) {{
          allOk = false;
          break;
        }}
      }}
      if (!allOk) results.push({{ focusNode: focus, message: 'AND constraint violated', sourceShape: shapeId, sourceConstraint: 'and', severity: 'Violation' }});
      break;
    }}
    case 'or': {{
      let anyOk = false;
      for (const sid of constraint.shapeIds) {{
        if (evaluateShape(sid, focus)) {{
          anyOk = true;
          break;
        }}
      }}
      if (!anyOk) results.push({{ focusNode: focus, message: 'OR constraint violated', sourceShape: shapeId, sourceConstraint: 'or', severity: 'Violation' }});
      break;
    }}
    case 'xone': {{
      let count = 0;
      for (const sid of constraint.shapeIds) {{
        if (evaluateShape(sid, focus)) count++;
      }}
      if (count !== 1) results.push({{ focusNode: focus, message: 'XONE constraint violated', sourceShape: shapeId, sourceConstraint: 'xone', severity: 'Violation' }});
      break;
    }}
    default:
      break;
  }}
}}

function handlePropertyConstraint(focus: Term, value: Term, constraint: Constraint, dataset: DatasetCore, factory: DataFactory, results: ValidationResult[], shapeId: string, path: NamedNode | undefined, evaluateShape: (shapeId: string, node: Term) => boolean) {{
  const rdfType = factory.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  switch (constraint.type) {{
    case 'datatype': {{
      const dt = termFromDesc(constraint.datatype, factory);
      const lit = value as any as Literal;
      const ok = (value as any).termType === 'Literal' && lit.datatype.value === (dt as any).value;
      if (!ok) results.push({{ focusNode: focus, value, resultPath: path, message: `Expected datatype ${{(dt as any).value}}`, sourceShape: shapeId, sourceConstraint: 'datatype', severity: 'Violation' }});
      break;
    }}
    case 'nodeKind': {{
      const tt = (value as any).termType;
      const ok =
        (constraint.nodeKind === 'iri' && tt === 'NamedNode') ||
        (constraint.nodeKind === 'blankNode' && tt === 'BlankNode') ||
        (constraint.nodeKind === 'literal' && tt === 'Literal');
      if (!ok) results.push({{ focusNode: focus, value, resultPath: path, message: `Expected node kind ${{constraint.nodeKind}}`, sourceShape: shapeId, sourceConstraint: 'nodeKind', severity: 'Violation' }});
      break;
    }}
    case 'class': {{
      const classTerm = termFromDesc(constraint.class, factory);
      let any = false;
      for (const _ of dataset.match(value as any, rdfType, classTerm, null)) {{
        any = true;
        break;
      }}
      if (!any) results.push({{ focusNode: focus, value, resultPath: path, message: `Value missing rdf:type ${{(classTerm as any).value}}`, sourceShape: shapeId, sourceConstraint: 'class', severity: 'Violation' }});
      break;
    }}
    case 'hasValue': {{
      const want = termFromDesc(constraint.value, factory);
      if (!termEquals(value, want)) results.push({{ focusNode: focus, value, resultPath: path, message: 'Value does not match required hasValue', sourceShape: shapeId, sourceConstraint: 'hasValue', severity: 'Violation' }});
      break;
    }}
    case 'in': {{
      const allowed = constraint.values.map(v => termFromDesc(v, factory));
      const ok = allowed.some(v => termEquals(v, value));
      if (!ok) results.push({{ focusNode: focus, value, resultPath: path, message: 'Value not in allowed sh:in list', sourceShape: shapeId, sourceConstraint: 'in', severity: 'Violation' }});
      break;
    }}
    case 'minLength': {{
      const lit = value as any as Literal;
      if ((value as any).termType !== 'Literal' || lit.value.length < constraint.min) {{
        results.push({{ focusNode: focus, value, resultPath: path, message: `Literal shorter than ${{constraint.min}}`, sourceShape: shapeId, sourceConstraint: 'minLength', severity: 'Violation' }});
      }}
      break;
    }}
    case 'maxLength': {{
      const lit = value as any as Literal;
      if ((value as any).termType !== 'Literal' || lit.value.length > constraint.max) {{
        results.push({{ focusNode: focus, value, resultPath: path, message: `Literal longer than ${{constraint.max}}`, sourceShape: shapeId, sourceConstraint: 'maxLength', severity: 'Violation' }});
      }}
      break;
    }}
    case 'pattern': {{
      const lit = value as any as Literal;
      const re = new RegExp(constraint.pattern, constraint.flags);
      if ((value as any).termType !== 'Literal' || !re.test(lit.value)) {{
        results.push({{ focusNode: focus, value, resultPath: path, message: `Literal does not match pattern ${{constraint.pattern}}`, sourceShape: shapeId, sourceConstraint: 'pattern', severity: 'Violation' }});
      }}
      break;
    }}
    case 'nodeShape': {{
      evaluateShape(constraint.shapeId, value);
      break;
    }}
    case 'not': {{
      const ok = !evaluateShape(constraint.shapeId, value);
      if (!ok) results.push({{ focusNode: focus, value, resultPath: path, message: 'NOT constraint violated', sourceShape: shapeId, sourceConstraint: 'not', severity: 'Violation' }});
      break;
    }}
    case 'and': {{
      let allOk = true;
      for (const sid of constraint.shapeIds) {{
        if (!evaluateShape(sid, value)) {{
          allOk = false;
          break;
        }}
      }}
      if (!allOk) results.push({{ focusNode: focus, value, resultPath: path, message: 'AND constraint violated', sourceShape: shapeId, sourceConstraint: 'and', severity: 'Violation' }});
      break;
    }}
    case 'or': {{
      let anyOk = false;
      for (const sid of constraint.shapeIds) {{
        if (evaluateShape(sid, value)) {{
          anyOk = true;
          break;
        }}
      }}
      if (!anyOk) results.push({{ focusNode: focus, value, resultPath: path, message: 'OR constraint violated', sourceShape: shapeId, sourceConstraint: 'or', severity: 'Violation' }});
      break;
    }}
    case 'xone': {{
      let count = 0;
      for (const sid of constraint.shapeIds) {{
        if (evaluateShape(sid, value)) count++;
      }}
      if (count !== 1) results.push({{ focusNode: focus, value, resultPath: path, message: 'XONE constraint violated', sourceShape: shapeId, sourceConstraint: 'xone', severity: 'Violation' }});
      break;
    }}
    default:
      break;
  }}
}}

export function validateWithStore(store: Store): ValidationReport {{
  return validate(store as unknown as DatasetCore, defaultFactory);
}}

export function validate(dataset: DatasetCore, factory: DataFactory = defaultFactory): ValidationReport {{
  const results: ValidationResult[] = [];
  const cache = new Map<string, boolean>();
  const processing = new Set<string>();

  const evaluateShape = (shapeId: string, node: Term): boolean => {{
    const key = `${{shapeId}}|${{(node as any).value ?? (node as any).id ?? String(node)}}`;
    if (cache.has(key)) return cache.get(key)!;
    if (processing.has(key)) return true;
    processing.add(key);

    const shape = nodeShapes.find(s => s.id === shapeId);
    if (!shape) {{
      results.push({{ focusNode: node, message: `Unknown node shape ${{shapeId}}`, sourceShape: shapeId, sourceConstraint: 'nodeShape', severity: 'Violation' }});
      processing.delete(key);
      cache.set(key, false);
      return false;
    }}

    const startLen = results.length;
    for (const constraint of shape.constraints) {{
      handleNodeConstraint(node, constraint, dataset, factory, results, shapeId, evaluateShape);
    }}
    for (const propId of shape.properties) {{
      const ps = propertyShapes[propId];
      if (!ps) {{
        results.push({{ focusNode: node, message: `Unknown property shape ${{propId}}`, sourceShape: shapeId, sourceConstraint: 'property', severity: 'Violation' }});
        continue;
      }}
      validatePropertyShape(ps, node, dataset, factory, results, evaluateShape);
    }}
    const conforms = results.length === startLen;
    processing.delete(key);
    cache.set(key, conforms);
    return conforms;
  }};

  const validatePropertyShape = (ps: PropertyShape, focus: Term, dataset: DatasetCore, factory: DataFactory, results: ValidationResult[], evaluateShape: (shapeId: string, node: Term) => boolean) => {{
    const values = valuesForPath(focus, ps.path, dataset, factory);
    const resultPath = ps.path.predicate ? factory.namedNode(ps.path.predicate) : undefined;

    const min = ps.constraints.find(c => c.type === 'minCount') as any;
    const max = ps.constraints.find(c => c.type === 'maxCount') as any;
    if (min && values.length < min.min) {{
      results.push({{ focusNode: focus, resultPath, message: `Expected at least ${{min.min}} value(s)`, sourceShape: ps.id, sourceConstraint: 'minCount', severity: 'Violation' }});
    }}
    if (max && values.length > max.max) {{
      results.push({{ focusNode: focus, resultPath, message: `Expected at most ${{max.max}} value(s)`, sourceShape: ps.id, sourceConstraint: 'maxCount', severity: 'Violation', value: values[max.max] }});
    }}

    const qvss = ps.constraints.filter(c => c.type === 'qualifiedValueShape') as any[];
    for (const constraint of qvss) {{
      const qualifying = values.filter(v => evaluateShape(constraint.shapeId, v));
      if (constraint.min !== undefined && qualifying.length < constraint.min) {{
        results.push({{ focusNode: focus, resultPath, message: `Expected at least ${{constraint.min}} value(s) matching qualified shape`, sourceShape: ps.id, sourceConstraint: 'qualifiedValueShape', severity: 'Violation' }});
      }}
      if (constraint.max !== undefined && qualifying.length > constraint.max) {{
        results.push({{ focusNode: focus, resultPath, message: `Expected at most ${{constraint.max}} value(s) matching qualified shape`, sourceShape: ps.id, sourceConstraint: 'qualifiedValueShape', severity: 'Violation', value: qualifying[constraint.max] }});
      }}
    }}

    for (const value of values) {{
      for (const constraint of ps.constraints) {{
        if (constraint.type === 'minCount' || constraint.type === 'maxCount' || constraint.type === 'qualifiedValueShape') continue;
        handlePropertyConstraint(focus, value, constraint, dataset, factory, results, ps.id, resultPath, evaluateShape);
      }}
    }}
  }};

  for (const shape of nodeShapes) {{
    const focusNodes = collectTargets(shape, dataset, factory);
    for (const focus of focusNodes) {{
      evaluateShape(shape.id, focus);
    }}
  }}

  return {{ conforms: results.length === 0, results }};
}}

function guessFormat(path: string): string {{
  const lower = path.toLowerCase();
  if (lower.endsWith('.ttl')) return 'text/turtle';
  if (lower.endsWith('.trig')) return 'application/trig';
  if (lower.endsWith('.nq')) return 'application/n-quads';
  if (lower.endsWith('.nt')) return 'application/n-triples';
  if (lower.endsWith('.jsonld')) return 'application/ld+json';
  if (lower.endsWith('.rdf') || lower.endsWith('.xml')) return 'application/rdf+xml';
  return 'text/turtle';
}}

async function readText(path: string): Promise<string> {{
  if (typeof Deno !== 'undefined' && (Deno as any).readTextFile) {{
    return (Deno as any).readTextFile(path);
  }}
  if (typeof process !== 'undefined') {{
    const fs = await import('fs/promises');
    return (fs as any).readFile(path, 'utf8');
  }}
  throw new Error('No filesystem API available to load data graph');
}}

async function runCli(args: string[]) {{
  const dataPath = args[0];
  if (!dataPath) {{
    console.error('Usage: deno run file.ts <data-file>');
    return;
  }}
  const text = await readText(dataPath);
  const store = new Store();
  const baseIri = dataPath.startsWith('http://') || dataPath.startsWith('https://') || dataPath.startsWith('file:') ? dataPath : new URL(dataPath, 'file://').href;
  store.load(text, {{ format: guessFormat(dataPath), base_iri: baseIri }});
  const report = validateWithStore(store);
  console.log(JSON.stringify(report, null, 2));
}}

function shouldRunCli(): boolean {{
  if (typeof Deno !== 'undefined' && (import.meta as any).main) return true;
  if (typeof process !== 'undefined' && Array.isArray((process as any).argv) && (import.meta as any).url) {{
    const path = new URL((import.meta as any).url).pathname;
    const argv1 = (process as any).argv[1] ? (process as any).argv[1] : '';
    return argv1 === path || argv1 === decodeURIComponent(path);
  }}
  return false;
}}

if (shouldRunCli()) {{
  const args = typeof Deno !== 'undefined' ? (Deno as any).args : (process as any).argv.slice(2);
  runCli(args).catch(err => {{
    console.error(err);
    if (typeof process !== 'undefined') (process as any).exitCode = 1;
    if (typeof Deno !== 'undefined' && (Deno as any).exit) (Deno as any).exit(1);
  }});
}}
"#
    );

    Ok(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::fs;
    use std::process::Command;
    use tempfile::tempdir;

    #[test]
    fn generated_ts_produces_expected_report() {
        // Skip if deno is not available in the environment.
        if Command::new("deno").arg("--version").output().is_err() {
            eprintln!("deno not found; skipping generated TS validation test");
            return;
        }

        const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/> .

ex:EmployeeShape a sh:NodeShape ;
  sh:class ex:Employee .

ex:ContractorShape a sh:NodeShape ;
  sh:class ex:Contractor .

ex:PersonShape a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:targetNode ex:alice, ex:charlie, ex:dana ;
  sh:or ( ex:EmployeeShape ex:ContractorShape ) ;
  sh:property [
    sh:path ex:knows ;
    sh:minCount 1 ;
    sh:class ex:Person
  ] .
"#;

        let tmp = tempdir().expect("tmpdir");
        let shapes_path = tmp.path().join("shapes.ttl");
        fs::write(&shapes_path, SHAPES).expect("write shapes");

        let validator = Validator::builder()
            .with_shapes_source(Source::File(shapes_path))
            .with_data_source(Source::Empty)
            .with_af_enabled(false)
            .with_rules_enabled(false)
            .with_do_imports(false)
            .build()
            .expect("validator");

        let ir = validator.context().shape_ir().clone();
        let ts_source = generate_typescript(&ir).expect("ts generation");

        // Stub oxigraph imports with a lightweight in-memory dataset so Deno can execute
        // without fetching npm packages.
        let mut ts = ts_source.replace(
            "import { Store } from 'oxigraph';\nimport * as oxigraph from 'oxigraph';\nimport type { DatasetCore, Term, NamedNode, Literal, DataFactory } from '@rdfjs/types';\n\nconst defaultFactory: DataFactory = oxigraph as unknown as DataFactory;\n",
            r#"type Term = any;
type NamedNode = any;
type Literal = any;
type DataFactory = any;
type DatasetCore = any;

const oxigraph = {
  namedNode: (value: string) => ({ termType: 'NamedNode', value }),
  blankNode: (value?: string) => ({ termType: 'BlankNode', value: value ?? 'b1' }),
  literal: (value: string, languageOrDt?: any) => {
    if (typeof languageOrDt === 'string') {
      return { termType: 'Literal', value, language: languageOrDt, datatype: { value: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString' } };
    }
    if (languageOrDt) {
      const dtVal = typeof languageOrDt === 'string' ? languageOrDt : (languageOrDt.value ?? languageOrDt);
      return { termType: 'Literal', value, language: '', datatype: { value: dtVal } };
    }
    return { termType: 'Literal', value, language: '', datatype: { value: 'http://www.w3.org/2001/XMLSchema#string' } };
  },
};

class Store {
  quads: any[];
  constructor(quads: any[] = []) { this.quads = quads; }
  match(s?: any, p?: any, o?: any, _g?: any) {
    return this.quads.filter(q =>
      (!s || termEquals(q.subject, s)) &&
      (!p || termEquals(q.predicate, p)) &&
      (!o || termEquals(q.object, o))
    );
  }
}

const defaultFactory: DataFactory = oxigraph as unknown as DataFactory;
"#,
        );

        // Harness builds a small dataset and prints the report as JSON.
        ts.push_str(
            r#"
const nn = (iri: string) => oxigraph.namedNode(iri);
const quads = [
  { subject: nn('http://example.com/alice'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Person') },
  { subject: nn('http://example.com/alice'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Employee') },
  { subject: nn('http://example.com/alice'), predicate: nn('http://example.com/knows'), object: nn('http://example.com/bob') },
  { subject: nn('http://example.com/bob'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Person') },

  { subject: nn('http://example.com/charlie'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Person') },

  { subject: nn('http://example.com/dana'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Person') },
  { subject: nn('http://example.com/dana'), predicate: nn('http://example.com/knows'), object: nn('http://example.com/eve') },
  { subject: nn('http://example.com/eve'), predicate: nn('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object: nn('http://example.com/Person') },
];

const store = new Store(quads);
const report = validate(store as unknown as any, oxigraph as unknown as any);
console.log(JSON.stringify(report));
"#,
        );

        let ts_path = tmp.path().join("validator.ts");
        fs::write(&ts_path, ts).expect("write ts");

        let output = Command::new("deno")
            .arg("run")
            .arg("--allow-read")
            .arg(&ts_path)
            .output()
            .expect("run deno");
        assert!(
            output.status.success(),
            "deno run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let report: Value =
            serde_json::from_str(&stdout).expect("json report from generated validator");
        if report["conforms"] != Value::Bool(false) {
            eprintln!("unexpected report: {}", report);
        }
        assert_eq!(report["conforms"], Value::Bool(false));
        let results = report["results"].as_array().expect("results array");
        if results.len() != 7 {
            eprintln!("unexpected results: {}", report);
        }
        assert_eq!(results.len(), 7, "expected seven validation results");
    }
}
