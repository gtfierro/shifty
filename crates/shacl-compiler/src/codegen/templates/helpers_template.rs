use serde_json;
use shifty::shacl_ir::ShapeIR;
use std::cell::RefCell;
fn graph_ref(graph: Option<&NamedNode>) -> Option<GraphNameRef<'_>> {
    graph.map(|g| GraphNameRef::NamedNode(g.as_ref()))
}
fn query_mentions_bound_var(query: &str, var: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    let needle_q = format!("bound(?{})", var);
    let needle_d = format!("bound(${})", var);
    lower.contains(&needle_q) || lower.contains(&needle_d)
}

#[derive(Default, Clone)]
struct SparqlProfileStat {
    calls: u64,
    total_micros: u128,
    max_micros: u128,
}

fn sparql_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("SHACL_PROFILE_SPARQL").is_ok())
}

fn sparql_profile_stats() -> &'static std::sync::Mutex<HashMap<String, SparqlProfileStat>> {
    static STATS: OnceLock<std::sync::Mutex<HashMap<String, SparqlProfileStat>>> = OnceLock::new();
    STATS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn sparql_profile_call_counter() -> &'static std::sync::atomic::AtomicU64 {
    static CALLS: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
    CALLS.get_or_init(|| std::sync::atomic::AtomicU64::new(0))
}

fn query_key_id(query_key: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    query_key.hash(&mut hasher);
    hasher.finish()
}

fn maybe_record_sparql_timing(query_key: &str, elapsed: std::time::Duration) {
    if !sparql_profile_enabled() {
        return;
    }

    let elapsed_micros = elapsed.as_micros();
    {
        let mut stats = sparql_profile_stats()
            .lock()
            .expect("sparql profile stats mutex poisoned");
        let entry = stats.entry(query_key.to_string()).or_default();
        entry.calls += 1;
        entry.total_micros += elapsed_micros;
        if elapsed_micros > entry.max_micros {
            entry.max_micros = elapsed_micros;
        }
    }

    let call_no = sparql_profile_call_counter()
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    if call_no % 200 != 0 {
        return;
    }

    let (unique_count, snapshot) = {
        let stats = sparql_profile_stats()
            .lock()
            .expect("sparql profile stats mutex poisoned");
        let mut rows: Vec<(String, SparqlProfileStat)> = stats
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        rows.sort_by(|a, b| b.1.total_micros.cmp(&a.1.total_micros));
        let unique_count = rows.len();
        rows.truncate(5);
        (unique_count, rows)
    };

    eprintln!(
        "[sparql-profile] calls={} unique={} top-total-ms:",
        call_no,
        unique_count
    );
    for (query, stat) in snapshot {
        let label = query
            .lines()
            .map(|line| line.trim())
            .find(|line| !line.is_empty())
            .unwrap_or("<empty>");
        eprintln!(
            "[sparql-profile] id={:016x} total_ms={:.3} max_ms={:.3} calls={} query={}",
            query_key_id(&query),
            stat.total_micros as f64 / 1000.0,
            stat.max_micros as f64 / 1000.0,
            stat.calls,
            label
        );
    }
}

fn cached_prepared_query(query_str: &str) -> Result<PreparedSparqlQuery, String> {
    if let Some(cached) = sparql_prepared_cache().get(query_str) {
        return Ok(cached.value().clone());
    }

    let mut prepared = SparqlEvaluator::new()
        .parse_query(query_str)
        .map_err(|e| e.to_string())?;
    prepared.dataset_mut().set_default_graph_as_union();
    sparql_prepared_cache().insert(query_str.to_string(), prepared.clone());
    Ok(prepared)
}
thread_local! {
    static CURRENT_SUBCLASS_CLOSURE: RefCell<SubclassClosure> =
        RefCell::new(SubclassClosure::new());
    static CLOSED_WORLD_VIOLATION_CACHE: RefCell<HashMap<(u64, String), HashMap<Term, Vec<(NamedNode, Term)>>>> =
        RefCell::new(HashMap::new());
    static ORIGINAL_VALUE_INDEX: RefCell<Option<OriginalValueIndex>> = RefCell::new(None);
}

fn target_class_cache() -> &'static DashMap<(String, String), Vec<Term>> {
    static CACHE: OnceLock<DashMap<(String, String), Vec<Term>>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn type_index_cache() -> &'static DashMap<String, Arc<ClassConstraintIndex>> {
    static CACHE: OnceLock<DashMap<String, Arc<ClassConstraintIndex>>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn class_membership_cache() -> &'static DashMap<String, Arc<ClassConstraintIndex>> {
    static CACHE: OnceLock<DashMap<String, Arc<ClassConstraintIndex>>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn class_membership_memo_cache() -> &'static DashMap<(String, Term, String), bool> {
    static CACHE: OnceLock<DashMap<(String, Term, String), bool>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn advanced_target_cache() -> &'static DashMap<(String, Term), Vec<Term>> {
    static CACHE: OnceLock<DashMap<(String, Term), Vec<Term>>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn sparql_prefix_cache() -> &'static DashMap<Term, String> {
    static CACHE: OnceLock<DashMap<Term, String>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn subject_prefix_presence_cache() -> &'static DashMap<(String, String, String), bool> {
    static CACHE: OnceLock<DashMap<(String, String, String), bool>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn sparql_prepared_cache() -> &'static DashMap<String, PreparedSparqlQuery> {
    static CACHE: OnceLock<DashMap<String, PreparedSparqlQuery>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}
#[derive(Hash, Eq, PartialEq, Clone)]
struct LiteralKey {
    lexical: String,
    language: Option<String>,
    datatype: String,
}
impl LiteralKey {
    fn from_literal(lit: &Literal) -> Self {
        let mut lexical = lit.value().to_string();
        if lit.datatype().as_str() == "http://www.w3.org/2001/XMLSchema#decimal" {
            lexical = normalize_decimal_literal(&lexical);
        }
        let language = lit.language().map(|l| l.to_ascii_lowercase());
        let datatype = lit.datatype().as_str().to_string();
        LiteralKey {
            lexical,
            language,
            datatype,
        }
    }
}
#[derive(Default, Clone)]
pub struct OriginalValueIndex {
    literals: HashMap<Term, HashMap<NamedNode, HashMap<LiteralKey, VecDeque<Term>>>>,
}
impl OriginalValueIndex {
    pub fn new() -> Self {
        Self::default()
    }
    fn record_triple(
        &mut self,
        subject: NamedOrBlankNode,
        predicate: NamedNode,
        object: Term,
    ) {
        if let Term::Literal(lit) = object {
            let subject_term: Term = match subject {
                NamedOrBlankNode::NamedNode(nn) => Term::NamedNode(nn),
                NamedOrBlankNode::BlankNode(bn) => Term::BlankNode(bn),
            };
            let entry = self
                .literals
                .entry(subject_term)
                .or_default()
                .entry(predicate)
                .or_default()
                .entry(LiteralKey::from_literal(&lit))
                .or_default();
            entry.push_back(Term::Literal(lit));
        }
    }
    pub fn resolve_literal(
        &self,
        subject: &Term,
        predicate: &NamedNode,
        candidate: &Literal,
    ) -> Option<Term> {
        let key = LiteralKey::from_literal(candidate);
        let predicate_map = self.literals.get(subject)?.get(predicate)?;
        if let Some(candidates) = predicate_map.get(&key) {
            if candidates.is_empty() {
                return None;
            }
            let candidate_term = Term::Literal(candidate.clone());
            if candidates.iter().any(|term| term == &candidate_term) {
                return Some(candidate_term);
            }
            return candidates.front().cloned();
        }
        for (other_key, candidates) in predicate_map {
            if other_key.lexical == key.lexical && other_key.language == key.language {
                return candidates.front().cloned();
            }
        }
        None
    }
}
pub fn load_original_value_index(path: &Path) -> Result<OriginalValueIndex, String> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    let format = match extension.as_deref() {
        Some("ttl") | Some("turtle") => Some(RdfFormat::Turtle),
        Some("nt") => Some(RdfFormat::NTriples),
        Some("rdf") | Some("xml") => Some(RdfFormat::RdfXml),
        _ => None,
    };
    let format = match format {
        Some(f) => f,
        None => return Ok(OriginalValueIndex::new()),
    };
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);
    let parser = RdfParser::from_format(format).without_named_graphs();
    let mut index = OriginalValueIndex::new();
    for quad in parser.for_reader(reader) {
        let triple = quad.map_err(|e| e.to_string())?;
        index.record_triple(triple.subject, triple.predicate, triple.object);
    }
    Ok(index)
}
pub fn set_original_value_index(index: Option<OriginalValueIndex>) {
    ORIGINAL_VALUE_INDEX.with(|cell| *cell.borrow_mut() = index);
}
fn with_original_value_index<F, R>(f: F) -> R
where
    F: FnOnce(Option<&OriginalValueIndex>) -> R,
{
    ORIGINAL_VALUE_INDEX.with(|cell| f(cell.borrow().as_ref()))
}
#[derive(Clone)]
struct SubclassClosure {
    parents: HashMap<String, Vec<String>>,
}
fn init_thread_state(closure: SubclassClosure) {
    CURRENT_SUBCLASS_CLOSURE.with(|cell| *cell.borrow_mut() = closure);
    clear_target_class_caches();
}
impl SubclassClosure {
    fn new() -> Self {
        SubclassClosure {
            parents: HashMap::new(),
        }
    }
    fn from_edges(edges: &[(&str, &str)]) -> Self {
        let mut closure = SubclassClosure::new();
        for (sub, sup) in edges.iter() {
            closure.add_edge(sub, sup);
        }
        closure
    }
    fn add_edge(&mut self, subclass: &str, superclass: &str) {
        self.parents
            .entry(subclass.to_string())
            .or_default()
            .push(superclass.to_string());
    }
    fn extend_from_store(&mut self, store: &Store, graph: Option<GraphNameRef<'_>>) {
        let subclass_pred = NamedNodeRef::new(RDFS_SUBCLASS_OF).unwrap();
        for quad in store.quads_for_pattern(None, Some(subclass_pred), None, graph) {
            if let Ok(quad) = quad {
                if let (NamedOrBlankNode::NamedNode(sub), Term::NamedNode(sup)) = (
                    &quad.subject,
                    &quad.object,
                ) {
                    self.add_edge(sub.as_str(), sup.as_str());
                }
            }
        }
    }
    fn is_subclass_of(&self, subclass: &str, superclass: &str) -> bool {
        if subclass == superclass {
            return true;
        }
        let mut stack: Vec<String> = vec![subclass.to_string()];
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(curr) = stack.pop() {
            if curr == superclass {
                return true;
            }
            if !seen.insert(curr.clone()) {
                continue;
            }
            if let Some(parents) = self.parents.get(&curr) {
                for parent in parents {
                    stack.push(parent.clone());
                }
            }
        }
        false
    }
}
fn subclass_closure_from_shape_edges() -> SubclassClosure {
    SubclassClosure::from_edges(SHAPE_SUBCLASS_EDGES)
}
fn set_current_subclass_closure(closure: SubclassClosure) {
    CURRENT_SUBCLASS_CLOSURE.with(|cell| *cell.borrow_mut() = closure);
}
fn extend_current_subclass_closure_from_store(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
) {
    CURRENT_SUBCLASS_CLOSURE
        .with(|cell| cell.borrow_mut().extend_from_store(store, graph));
}
fn with_subclass_closure<F, R>(f: F) -> R
where
    F: FnOnce(&SubclassClosure) -> R,
{
    CURRENT_SUBCLASS_CLOSURE.with(|cell| f(&cell.borrow()))
}
fn graph_cache_key(graph: Option<GraphNameRef<'_>>) -> String {
    match graph {
        Some(g) => g.to_string(),
        None => "__all__".to_string(),
    }
}
fn has_rdf_type_cache_key(graph: Option<GraphNameRef<'_>>) -> String {
    match graph {
        Some(g) => {
            let shape_graph = shape_graph_ref();
            if g == shape_graph {
                format!("{}|types-only", graph_cache_key(Some(g)))
            } else {
                format!("{}|types+shape", graph_cache_key(Some(g)))
            }
        }
        None => "__all__|types-only".to_string(),
    }
}
struct ClassConstraintIndex {
    class_to_id: HashMap<String, usize>,
    descendants_by_super: Vec<FixedBitSet>,
    subject_type_bits: HashMap<Term, FixedBitSet>,
}

impl ClassConstraintIndex {
    fn class_bitset(&self, class_iri: &str) -> Option<&FixedBitSet> {
        let class_id = self.class_to_id.get(class_iri).copied()?;
        self.descendants_by_super.get(class_id)
    }

    fn matches(&self, term: &Term, class_iri: &str) -> bool {
        let Some(descendants) = self.class_bitset(class_iri) else {
            return false;
        };
        self.subject_type_bits
            .get(term)
            .map(|type_bits| type_bits.intersection(descendants).next().is_some())
            .unwrap_or(false)
    }

    fn instances_of_class(&self, class_iri: &str) -> Vec<Term> {
        let Some(descendants) = self.class_bitset(class_iri) else {
            return Vec::new();
        };
        self.subject_type_bits
            .iter()
            .filter(|(_, type_bits)| type_bits.intersection(descendants).next().is_some())
            .map(|(subject, _)| subject.clone())
            .collect()
    }
}

fn class_index_graphs(
    graph: Option<GraphNameRef<'_>>,
    include_shape_graph: bool,
) -> Vec<Option<GraphNameRef<'_>>> {
    let mut graphs: Vec<Option<GraphNameRef<'_>>> = Vec::new();
    if let Some(g) = graph {
        graphs.push(Some(g));
        if include_shape_graph {
            let shape_graph = shape_graph_ref();
            if g != shape_graph {
                graphs.push(Some(shape_graph));
            }
        }
    } else {
        graphs.push(None);
    }
    graphs
}

fn build_class_constraint_index(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    include_shape_graph: bool,
) -> ClassConstraintIndex {
    let graph_key = graph_cache_key(graph);
    compiled_stage(&format!(
        "optimization pass class index build start graph={} include_shape_graph={}",
        graph_key,
        include_shape_graph
    ));

    let closure_parents = with_subclass_closure(|closure| closure.parents.clone());

    let mut next_class_id = 0usize;
    let mut class_to_id: HashMap<String, usize> = HashMap::new();
    let mut intern = |iri: &str| {
        if let Some(id) = class_to_id.get(iri).copied() {
            id
        } else {
            let id = next_class_id;
            next_class_id += 1;
            class_to_id.insert(iri.to_string(), id);
            id
        }
    };

    let mut parent_edges: Vec<(usize, usize)> = Vec::new();
    for (subclass, parents) in &closure_parents {
        let sub_id = intern(subclass);
        for parent in parents {
            let super_id = intern(parent);
            parent_edges.push((sub_id, super_id));
        }
    }

    let rdf_type = NamedNodeRef::new(RDF_TYPE).unwrap();
    let mut subject_type_ids: HashMap<Term, Vec<usize>> = HashMap::new();
    for graph_opt in class_index_graphs(graph, include_shape_graph) {
        for quad in store.quads_for_pattern(None, Some(rdf_type), None, graph_opt) {
            let quad = match quad {
                Ok(quad) => quad,
                Err(_) => continue,
            };
            let class_node = match &quad.object {
                Term::NamedNode(node) => node,
                _ => continue,
            };
            let class_id = intern(class_node.as_str());
            let subject: Term = quad.subject.into();
            subject_type_ids.entry(subject).or_default().push(class_id);
        }
    }

    let class_count = next_class_id;
    let mut descendants_by_super: Vec<FixedBitSet> = (0..class_count)
        .map(|_| FixedBitSet::with_capacity(class_count))
        .collect();

    if class_count > 0 {
        let mut parents_by_subclass: Vec<Vec<usize>> = vec![Vec::new(); class_count];
        for (child, parent) in parent_edges {
            if !parents_by_subclass[child].contains(&parent) {
                parents_by_subclass[child].push(parent);
            }
        }

        let mut ancestors_by_subclass: Vec<FixedBitSet> = (0..class_count)
            .map(|_| FixedBitSet::with_capacity(class_count))
            .collect();
        for subclass in 0..class_count {
            let mut stack = vec![subclass];
            while let Some(node) = stack.pop() {
                if ancestors_by_subclass[subclass].contains(node) {
                    continue;
                }
                ancestors_by_subclass[subclass].insert(node);
                for parent in &parents_by_subclass[node] {
                    stack.push(*parent);
                }
            }
        }

        for (subclass, ancestors) in ancestors_by_subclass.iter().enumerate() {
            for super_id in ancestors.ones() {
                descendants_by_super[super_id].insert(subclass);
            }
        }
    }

    let mut subject_type_bits: HashMap<Term, FixedBitSet> = HashMap::new();
    for (subject, class_ids) in subject_type_ids {
        let mut bits = FixedBitSet::with_capacity(class_count);
        for class_id in class_ids {
            if class_id < class_count {
                bits.insert(class_id);
            }
        }
        subject_type_bits.insert(subject, bits);
    }

    let classes = class_to_id.len();
    let subjects = subject_type_bits.len();
    compiled_stage(&format!(
        "optimization pass class index build finish graph={} include_shape_graph={} classes={} subjects={}",
        graph_key,
        include_shape_graph,
        classes,
        subjects
    ));

    ClassConstraintIndex {
        class_to_id,
        descendants_by_super,
        subject_type_bits,
    }
}

fn collect_closed_world_violations_for_targets(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    targets: &[Term],
    allowed: &HashSet<String>,
) -> HashMap<Term, Vec<(NamedNode, Term)>> {
    let mut out: HashMap<Term, Vec<(NamedNode, Term)>> = HashMap::new();
    for focus in targets {
        let subject = match subject_ref(focus) {
            Some(subject) => subject,
            None => continue,
        };
        for quad in store.quads_for_pattern(Some(subject), None, None, graph) {
            let quad = match quad {
                Ok(quad) => quad,
                Err(_) => continue,
            };
            let predicate = quad.predicate;
            if allowed.contains(predicate.as_str()) {
                continue;
            }
            out.entry(focus.clone()).or_default().push((predicate, quad.object));
        }
    }
    out
}

fn clear_target_class_caches() {
    target_class_cache().clear();
    type_index_cache().clear();
    class_membership_cache().clear();
    class_membership_memo_cache().clear();
    advanced_target_cache().clear();
    sparql_prefix_cache().clear();
    subject_prefix_presence_cache().clear();
    sparql_prepared_cache().clear();
    CLOSED_WORLD_VIOLATION_CACHE.with(|cell| cell.borrow_mut().clear());
}

fn targets_for_class(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    class_iri: &str,
) -> Vec<Term> {
    let graph_key = graph_cache_key(graph);
    let cache_key = (graph_key.clone(), class_iri.to_string());
    if let Some(cached) = target_class_cache().get(&cache_key) {
        return cached.value().clone();
    }

    let index = if let Some(cached) = type_index_cache().get(&graph_key) {
        cached.value().clone()
    } else {
        let built = Arc::new(build_class_constraint_index(store, graph, false));
        type_index_cache().insert(graph_key.clone(), built.clone());
        built
    };
    let targets = index.instances_of_class(class_iri);

    target_class_cache().insert(cache_key, targets.clone());
    targets
}
fn shape_graph_ref() -> GraphNameRef<'static> {
    GraphNameRef::NamedNode(NamedNodeRef::new(SHAPE_GRAPH).unwrap())
}
pub fn data_graph_named() -> NamedNode {
    NamedNode::new(DATA_GRAPH).unwrap()
}
fn union_shape_graph_into_data_graph(store: &Store, data_graph: &NamedNode) -> Result<(), String> {
    let shape_graph = shape_graph_ref();
    let data_graph_name = GraphName::NamedNode(data_graph.clone());

    for quad in store.quads_for_pattern(None, None, None, Some(shape_graph)) {
        let quad = quad.map_err(|e| e.to_string())?;
        let merged = Quad::new(
            quad.subject.clone(),
            quad.predicate.clone(),
            quad.object.clone(),
            data_graph_name.clone(),
        );
        store.insert(merged.as_ref()).map_err(|e| e.to_string())?;
    }

    Ok(())
}
fn subject_ref(term: &Term) -> Option<NamedOrBlankNodeRef<'_>> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNodeRef::NamedNode(node.as_ref())),
        Term::BlankNode(node) => Some(NamedOrBlankNodeRef::BlankNode(node.as_ref())),
        _ => None,
    }
}
fn term_ref(term: &Term) -> TermRef<'_> {
    match term {
        Term::NamedNode(node) => TermRef::NamedNode(node.as_ref()),
        Term::BlankNode(node) => TermRef::BlankNode(node.as_ref()),
        Term::Literal(lit) => TermRef::Literal(lit.as_ref()),
    }
}
fn literal_signature(lit: &Literal) -> (String, Option<String>, String) {
    let mut lexical = lit.value().to_string();
    if lit.datatype().as_str() == "http://www.w3.org/2001/XMLSchema#decimal" {
        lexical = normalize_decimal_literal(&lexical);
    }
    (
        lexical,
        lit.language().map(|lang| lang.to_ascii_lowercase()),
        lit.datatype().as_str().to_string(),
    )
}
fn lookup_by_signature(
    buckets: &mut HashMap<(String, Option<String>, String), VecDeque<Term>>,
    exact_matches: &mut HashSet<Term>,
    lit: &Literal,
) -> Option<Term> {
    let key = literal_signature(lit);
    if let Some(queue) = buckets.get_mut(&key) {
        if let Some(term) = queue.pop_front() {
            exact_matches.remove(&term);
            return Some(term);
        }
    }
    None
}
fn canonicalize_values_for_predicate(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    focus: &Term,
    predicate_iri: &str,
    mut nodes: Vec<Term>,
) -> Vec<Term> {
    if nodes.is_empty() {
        return nodes;
    }
    let predicate_ref = match NamedNodeRef::new(predicate_iri) {
        Ok(p) => p,
        Err(_) => return nodes,
    };
    let subject = match subject_ref(focus) {
        Some(s) => s,
        None => return nodes,
    };
    let mut raw_objects: Vec<Term> = Vec::new();
    for quad in store.quads_for_pattern(Some(subject), Some(predicate_ref), None, graph)
    {
        if let Ok(quad) = quad {
            raw_objects.push(quad.object);
        }
    }
    let has_original_index = with_original_value_index(|idx| idx.is_some());
    if raw_objects.is_empty() && !has_original_index {
        return nodes;
    }
    let mut exact_matches: HashSet<Term> = HashSet::with_capacity(raw_objects.len());
    let mut literals_by_signature: HashMap<
        (String, Option<String>, String),
        VecDeque<Term>,
    > = HashMap::new();
    for term in raw_objects {
        if let Term::Literal(lit) = &term {
            let key = literal_signature(lit);
            literals_by_signature.entry(key).or_default().push_back(term.clone());
        }
        exact_matches.insert(term);
    }
    let predicate = NamedNode::new_unchecked(predicate_iri);
    for node in &mut nodes {
        let current = node.clone();
        if let Term::Literal(ref lit) = current {
            let resolved = with_original_value_index(|idx| {
                idx.and_then(|index| index.resolve_literal(focus, &predicate, lit))
            });
            if let Some(original) = resolved {
                if original != current {
                    exact_matches.remove(&original);
                    *node = original;
                    continue;
                }
            }
        }
        if exact_matches.remove(&current) {
            continue;
        }
        if let Term::Literal(ref lit) = current {
            if let Some(term) = lookup_by_signature(
                &mut literals_by_signature,
                &mut exact_matches,
                lit,
            ) {
                *node = term;
            }
        }
    }
    nodes
}
fn is_literal_with_datatype(term: &Term, datatype_iri: &str) -> bool {
    let lit = match term {
        Term::Literal(lit) => lit,
        _ => return false,
    };
    if datatype_iri == rdf::LANG_STRING.as_str() {
        return lit.language().is_some();
    }
    let lit_datatype = lit.datatype();
    let mut datatype_matches = lit_datatype.as_str() == datatype_iri;
    if !datatype_matches && datatype_iri == xsd::DECIMAL.as_str()
        && lit_datatype == xsd::INTEGER
    {
        datatype_matches = true;
    }
    if !datatype_matches {
        return false;
    }
    let literal_value = lit.value();
    if datatype_iri == xsd::STRING.as_str() {
        true
    } else if datatype_iri == xsd::BOOLEAN.as_str() {
        Boolean::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::DECIMAL.as_str() {
        Decimal::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::INTEGER.as_str() {
        Integer::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::BYTE.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= i64::from(i8::MIN) && value <= i64::from(i8::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::SHORT.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= i64::from(i16::MIN) && value <= i64::from(i16::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::INT.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= i64::from(i32::MIN) && value <= i64::from(i32::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::LONG.as_str() {
        Integer::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::UNSIGNED_BYTE.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= 0 && value <= i64::from(u8::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::UNSIGNED_SHORT.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= 0 && value <= i64::from(u16::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::UNSIGNED_INT.as_str() {
        Integer::from_str(literal_value)
            .map(|v: Integer| {
                let value: i64 = v.into();
                value >= 0 && value <= i64::from(u32::MAX)
            })
            .unwrap_or(false)
    } else if datatype_iri == xsd::DOUBLE.as_str() {
        Double::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::FLOAT.as_str() {
        Float::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::DATE.as_str() {
        Date::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::TIME.as_str() {
        Time::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::DATE_TIME.as_str() {
        DateTime::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::G_YEAR.as_str() {
        GYear::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::G_MONTH.as_str() {
        GMonth::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::G_DAY.as_str() {
        GDay::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::G_YEAR_MONTH.as_str() {
        GYearMonth::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::G_MONTH_DAY.as_str() {
        GMonthDay::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::DURATION.as_str() {
        Duration::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::YEAR_MONTH_DURATION.as_str() {
        YearMonthDuration::from_str(literal_value).is_ok()
    } else if datatype_iri == xsd::DAY_TIME_DURATION.as_str() {
        DayTimeDuration::from_str(literal_value).is_ok()
    } else {
        true
    }
}
fn advanced_select_query(store: &Store, selector: &Term) -> Option<String> {
    let select_pred = NamedNodeRef::new(SHACL_SELECT).unwrap();
    let selector_subject = subject_ref(selector).map(|subject| match subject {
        NamedOrBlankNodeRef::NamedNode(node) => NamedOrBlankNode::NamedNode(node.into_owned()),
        NamedOrBlankNodeRef::BlankNode(node) => NamedOrBlankNode::BlankNode(node.into_owned()),
    });
    let mut selector_select: Option<String> = None;
    let mut fallback_select: Option<String> = None;
    for quad in store.quads_for_pattern(None, Some(select_pred), None, None) {
        if let Ok(quad) = quad {
            if let Term::Literal(lit) = quad.object {
                if selector_select.is_none() {
                    if let Some(subject) = &selector_subject {
                        if &quad.subject == subject {
                            selector_select = Some(lit.value().to_string());
                            break;
                        }
                    }
                }
                if fallback_select.is_none() {
                    fallback_select = Some(lit.value().to_string());
                }
            }
        }
    }
    selector_select.or(fallback_select)
}
fn prefixes_for_selector(store: &Store, selector: &Term) -> String {
    if let Some(cached) = sparql_prefix_cache().get(selector) {
        return cached.value().clone();
    }

    let sh_prefixes = NamedNodeRef::new(SHACL_PREFIXES).unwrap();
    let sh_declare = NamedNodeRef::new(SHACL_DECLARE).unwrap();
    let sh_prefix = NamedNodeRef::new(SHACL_PREFIX).unwrap();
    let sh_namespace = NamedNodeRef::new(SHACL_NAMESPACE).unwrap();
    let shape_graph = shape_graph_ref();
    let mut subjects: Vec<Term> = Vec::new();
    if let Some(subject) = subject_ref(selector) {
        for quad in store
            .quads_for_pattern(Some(subject), Some(sh_prefixes), None, Some(shape_graph))
        {
            if let Ok(quad) = quad {
                subjects.push(quad.object);
            }
        }
    }
    for quad in store.quads_for_pattern(None, Some(sh_declare), None, None) {
        if let Ok(quad) = quad {
            subjects.push(quad.subject.into());
        }
    }
    let mut prefixes: HashMap<String, String> = HashMap::new();
    for subject_term in subjects {
        let subject_node = match subject_ref(&subject_term) {
            Some(value) => value,
            None => continue,
        };
        for quad in store
            .quads_for_pattern(Some(subject_node), Some(sh_declare), None, None)
        {
            let quad = match quad {
                Ok(q) => q,
                Err(_) => continue,
            };
            let decl_term = quad.object;
            let decl_ref = match subject_ref(&decl_term) {
                Some(value) => value,
                None => continue,
            };
            let prefix_val = store
                .quads_for_pattern(Some(decl_ref), Some(sh_prefix), None, None)
                .filter_map(Result::ok)
                .map(|q| q.object)
                .next();
            let namespace_val = store
                .quads_for_pattern(Some(decl_ref), Some(sh_namespace), None, None)
                .filter_map(Result::ok)
                .map(|q| q.object)
                .next();
            if let (Some(Term::Literal(prefix_lit)), Some(Term::Literal(ns_lit))) = (
                prefix_val,
                namespace_val,
            ) {
                let prefix = prefix_lit.value().to_string();
                let namespace = ns_lit.value().to_string();
                prefixes.insert(prefix, namespace);
            }
        }
    }
    if prefixes.is_empty() {
        sparql_prefix_cache().insert(selector.clone(), String::new());
        return String::new();
    }
    let mut keys: Vec<String> = prefixes.keys().cloned().collect();
    keys.sort();
    let mut out = String::new();
    for key in keys {
        if let Some(ns) = prefixes.get(&key) {
            out.push_str(&format!("PREFIX {}: <{}>\n", key, ns));
        }
    }
    sparql_prefix_cache().insert(selector.clone(), out.clone());
    out
}
fn advanced_targets_for(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    selector: &Term,
) -> Vec<Term> {
    let graph_key = graph_cache_key(graph);
    let cache_key = (graph_key.clone(), selector.clone());
    if let Some(cached) = advanced_target_cache().get(&cache_key) {
        return cached.value().clone();
    }

    let query = match advanced_select_query(store, selector) {
        Some(value) => value,
        None => {
            let empty = Vec::new();
            advanced_target_cache().insert(cache_key, empty.clone());
            return empty;
        }
    };
    let normalized_query = query.replace('$', "?");
    let prefixes = prefixes_for_selector(store, selector);
    let query_str = if prefixes.trim().is_empty() {
        normalized_query.clone()
    } else {
        format!("{}\n{}", prefixes, normalized_query)
    };
    let debug = std::env::var("SHACL_DEBUG_SPARQL").is_ok();
    if debug {
        eprintln!("SPARQL query:\n{}", query_str);
    }
    let mut prepared = match cached_prepared_query(&query_str) {
        Ok(prepared) => prepared,
        Err(_) => {
            let empty = Vec::new();
            advanced_target_cache().insert(cache_key, empty.clone());
            return empty;
        }
    };
    if let Some(graph) = graph {
        prepared.dataset_mut().set_default_graph(vec![graph.into_owned()]);
    } else {
        prepared.dataset_mut().set_default_graph_as_union();
    }
    let mut results: HashSet<Term> = HashSet::new();
    let query_start = std::time::Instant::now();
    match prepared.on_store(store).execute() {
        Ok(QueryResults::Solutions(solutions)) => {
            for solution in solutions {
                let solution = match solution {
                    Ok(sol) => sol,
                    Err(_) => continue,
                };
                if let Some(value) = solution.get("this") {
                    results.insert(value.to_owned());
                } else if let Some(value) = solution.get("target") {
                    results.insert(value.to_owned());
                } else if let Some(var) = solution.variables().first() {
                    if let Some(value) = solution.get(var.as_str()) {
                        results.insert(value.to_owned());
                    }
                }
            }
        }
        _ => {}
    }
    maybe_record_sparql_timing(&normalized_query, query_start.elapsed());
    let out: Vec<Term> = results.into_iter().collect();
    advanced_target_cache().insert(cache_key, out.clone());
    out
}
fn has_rdf_type(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    term: &Term,
    class_iri: &str,
) -> bool {
    if subject_ref(term).is_none() {
        return false;
    }
    let cache_key = has_rdf_type_cache_key(graph);
    let memo_key = (cache_key.clone(), term.clone(), class_iri.to_string());
    if let Some(cached) = class_membership_memo_cache().get(&memo_key) {
        return *cached.value();
    }

    let index = if let Some(cached) = class_membership_cache().get(&cache_key) {
        cached.value().clone()
    } else {
        let built = Arc::new(build_class_constraint_index(store, graph, true));
        class_membership_cache().insert(cache_key, built.clone());
        built
    };
    let result = index.matches(term, class_iri);
    class_membership_memo_cache().insert(memo_key, result);
    result
}
fn matches_node_kind(term: &Term, node_kind_iri: &str) -> bool {
    match node_kind_iri {
        "http://www.w3.org/ns/shacl#IRI" => matches!(term, Term::NamedNode(_)),
        "http://www.w3.org/ns/shacl#BlankNode" => matches!(term, Term::BlankNode(_)),
        "http://www.w3.org/ns/shacl#Literal" => matches!(term, Term::Literal(_)),
        "http://www.w3.org/ns/shacl#BlankNodeOrIRI" => {
            matches!(term, Term::BlankNode(_) | Term::NamedNode(_))
        }
        "http://www.w3.org/ns/shacl#BlankNodeOrLiteral" => {
            matches!(term, Term::BlankNode(_) | Term::Literal(_))
        }
        "http://www.w3.org/ns/shacl#IRIOrLiteral" => {
            matches!(term, Term::NamedNode(_) | Term::Literal(_))
        }
        _ => false,
    }
}
fn literal_length_at_least(term: &Term, min: u64) -> bool {
    match term {
        Term::Literal(lit) => (lit.value().chars().count() as u64) >= min,
        Term::NamedNode(node) => (node.as_str().chars().count() as u64) >= min,
        _ => false,
    }
}
fn literal_length_at_most(term: &Term, max: u64) -> bool {
    match term {
        Term::Literal(lit) => (lit.value().chars().count() as u64) <= max,
        Term::NamedNode(node) => (node.as_str().chars().count() as u64) <= max,
        _ => false,
    }
}
fn literal_matches_regex(term: &Term, regex: &Regex) -> bool {
    match term {
        Term::Literal(lit) => regex.is_match(lit.value()),
        _ => false,
    }
}
fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag_lower = tag.to_ascii_lowercase();
    let range_lower = range.to_ascii_lowercase();
    if tag_lower == range_lower {
        return true;
    }
    if range_lower.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        if tag_lower.starts_with(&format!("{}-", range_lower)) {
            return true;
        }
    }
    false
}
fn language_in_allowed(term: &Term, allowed: &[&str]) -> bool {
    match term {
        Term::Literal(lit) => {
            let lang = lit.language().unwrap_or("");
            if allowed.is_empty() {
                return false;
            }
            allowed.iter().any(|range| lang_matches(lang, range))
        }
        _ => false,
    }
}
fn values_for_predicate(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    focus: &Term,
    predicate_iri: &str,
) -> Vec<Term> {
    let subject = match subject_ref(focus) {
        Some(s) => s,
        None => return Vec::new(),
    };
    let predicate = NamedNodeRef::new(predicate_iri).unwrap();
    let data_graph = data_graph_named();
    let graph_ref = match graph {
        Some(g) => g,
        None => GraphNameRef::NamedNode(data_graph.as_ref()),
    };
    store
        .quads_for_pattern(Some(subject), Some(predicate), None, Some(graph_ref))
        .filter_map(Result::ok)
        .map(|q| q.object)
        .collect()
}
fn term_to_sparql(term: &Term) -> String {
    term.to_string()
}
fn term_to_sparql_ground(term: &Term) -> Option<String> {
    match term {
        Term::BlankNode(_) => None,
        _ => Some(term.to_string()),
    }
}
fn inject_values_clause(query: &str, values_clause: &str) -> String {
    if values_clause.trim().is_empty() {
        return query.to_string();
    }
    if let Some(pos) = query.find('{') {
        let mut out = String::with_capacity(query.len() + values_clause.len() + 2);
        out.push_str(&query[..pos + 1]);
        out.push('\n');
        out.push_str(values_clause);
        out.push('\n');
        out.push_str(&query[pos + 1..]);
        out
    } else {
        query.to_string()
    }
}
fn inject_bindings_everywhere(query: &str, bindings_clause: &str) -> String {
    if bindings_clause.trim().is_empty() {
        return query.to_string();
    }
    let mut out = String::with_capacity(query.len() + bindings_clause.len() * 2);
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        out.push(ch);
        if ch == '{' {
            let mut j = i + 1;
            while j < bytes.len() {
                let c = bytes[j] as char;
                if c.is_whitespace() {
                    j += 1;
                    continue;
                }
                if c == '#' {
                    while j < bytes.len() && bytes[j] as char != '\n' {
                        j += 1;
                    }
                    continue;
                }
                break;
            }
            let rest = &query[j..];
            if rest.len() < 6 || !rest[..6].eq_ignore_ascii_case("select") {
                out.push('\n');
                out.push_str(bindings_clause);
                out.push('\n');
            }
        }
        i += 1;
    }
    out
}
fn inject_bindings_in_where(query: &str, bindings_clause: &str) -> String {
    if bindings_clause.trim().is_empty() {
        {
            return query.to_string();
        }
    }
    let bytes = query.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    let mut in_iri = false;
    let mut in_comment = false;
    while i < bytes.len() {
        {
            let ch = bytes[i] as char;
            if in_comment {
                {
                    if ch == '\n' {
                        {
                            in_comment = false;
                        }
                    }
                    i += 1;
                    continue;
                }
            }
            if in_string {
                {
                    if ch == '\\' {
                        {
                            i += 2;
                            continue;
                        }
                    }
                    if ch == '"' {
                        {
                            in_string = false;
                        }
                    }
                    i += 1;
                    continue;
                }
            }
            if in_iri {
                {
                    if ch == '>' {
                        {
                            in_iri = false;
                        }
                    }
                    i += 1;
                    continue;
                }
            }
            if ch == '#' {
                {
                    in_comment = true;
                    i += 1;
                    continue;
                }
            }
            if ch == '"' {
                {
                    in_string = true;
                    i += 1;
                    continue;
                }
            }
            if ch == '<' {
                {
                    in_iri = true;
                    i += 1;
                    continue;
                }
            }
            if i + 5 <= bytes.len() {
                {
                    let slice = &query[i..i + 5];
                    if slice.eq_ignore_ascii_case("where") {
                        {
                            let before_ok = i == 0
                                || !((bytes[i - 1] as char).is_ascii_alphanumeric()
                                    || bytes[i - 1] as char == '_');
                            let after_idx = i + 5;
                            let after_ok = after_idx >= bytes.len()
                                || !((bytes[after_idx] as char).is_ascii_alphanumeric()
                                    || bytes[after_idx] as char == '_');
                            if !(before_ok && after_ok) {
                                {
                                    i += 1;
                                    continue;
                                }
                            }
                            let mut j = i + 5;
                            while j < bytes.len() {
                                {
                                    let c = bytes[j] as char;
                                    if c.is_whitespace() {
                                        {
                                            j += 1;
                                            continue;
                                        }
                                    }
                                    if c == '#' {
                                        {
                                            while j < bytes.len() && bytes[j] as char != '\n' {
                                                {
                                                    j += 1;
                                                }
                                            }
                                            continue;
                                        }
                                    }
                                    break;
                                }
                            }
                            if j < bytes.len() && bytes[j] as char == '{' {
                                {
                                    let mut out = String::with_capacity(
                                        query.len() + bindings_clause.len() + 2,
                                    );
                                    out.push_str(&query[..j + 1]);
                                    out.push('\n');
                                    out.push_str(bindings_clause);
                                    out.push('\n');
                                    out.push_str(&query[j + 1..]);
                                    return out;
                                }
                            }
                        }
                    }
                }
            }
            i += 1;
        }
    }
    query.to_string()
}
fn query_mentions_var(query: &str, var: &str) -> bool {
    fn contains(query: &str, prefix: char, var: &str) -> bool {
        let mut start = 0;
        let bytes = query.as_bytes();
        let var_bytes = var.as_bytes();
        while let Some(pos) = query[start..].find(prefix) {
            let idx = start + pos + 1;
            if bytes.len() >= idx + var_bytes.len()
                && &bytes[idx..idx + var_bytes.len()] == var_bytes
            {
                let after = idx + var_bytes.len();
                if after >= bytes.len() {
                    return true;
                }
                let next = bytes[after] as char;
                if !next.is_ascii_alphanumeric() && next != '_' {
                    return true;
                }
            }
            start += pos + 1;
        }
        false
    }
    contains(query, '?', var) || contains(query, '$', var)
}

fn extract_subject_predicate_var(query: &str) -> Option<String> {
    static TRIPLE_RE: OnceLock<Regex> = OnceLock::new();
    let triple_re = TRIPLE_RE.get_or_init(|| {
        Regex::new(
            r"(?is)[\?\$]this\s+[\?\$]([A-Za-z_][A-Za-z0-9_]*)\s+[\?\$][A-Za-z_][A-Za-z0-9_]*\s*\."
        )
        .unwrap()
    });
    let captures = triple_re.captures(query)?;
    captures.get(1).map(|m| m.as_str().to_string())
}

fn extract_strstarts_prefix_for_var(query: &str, var: &str) -> Option<String> {
    static STRSTARTS_RE: OnceLock<Regex> = OnceLock::new();
    let strstarts_re = STRSTARTS_RE.get_or_init(|| {
        Regex::new(
            r#"(?is)STRSTARTS\s*\(\s*str\s*\(\s*[\?\$]([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*,\s*\"([^\"]+)\"\s*\)"#,
        )
        .unwrap()
    });
    for captures in strstarts_re.captures_iter(query) {
        let capture_var = captures.get(1).map(|m| m.as_str())?;
        if capture_var == var {
            return captures.get(2).map(|m| m.as_str().to_string());
        }
    }
    None
}

fn keyword_scan_text(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let bytes = query.as_bytes();
    let mut i = 0usize;
    let mut in_iri = false;
    let mut in_string = false;
    let mut in_comment = false;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        if in_comment {
            if ch == '\n' {
                in_comment = false;
                out.push(' ');
            }
            i += 1;
            continue;
        }
        if in_iri {
            if ch == '>' {
                in_iri = false;
            }
            i += 1;
            continue;
        }
        if in_string {
            if ch == '\\' {
                i += 2;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if ch == '#' {
            in_comment = true;
            i += 1;
            continue;
        }
        if ch == '<' {
            in_iri = true;
            out.push(' ');
            i += 1;
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push(' ');
            i += 1;
            continue;
        }

        out.push(ch);
        i += 1;
    }

    out
}

fn query_is_simple_subject_prefix_filter(query: &str) -> bool {
    static COMPLEX_RE: OnceLock<Regex> = OnceLock::new();
    let complex_re = COMPLEX_RE.get_or_init(|| {
        Regex::new(
            r"(?is)\b(union|optional|minus)\b",
        )
        .unwrap()
    });

    let scan = keyword_scan_text(query);
    !complex_re.is_match(&scan)
}

fn quick_reject_subject_prefix_query(
    query: &str,
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    focus: Option<&Term>,
) -> bool {
    if !query_is_simple_subject_prefix_filter(query) {
        return false;
    }

    let focus = match focus {
        Some(term) => term,
        None => return false,
    };
    let subject = match subject_ref(focus) {
        Some(subject) => subject,
        None => return false,
    };

    let predicate_var = match extract_subject_predicate_var(query) {
        Some(var) => var,
        None => return false,
    };
    let predicate_prefix = match extract_strstarts_prefix_for_var(query, &predicate_var) {
        Some(prefix) => prefix,
        None => return false,
    };

    let cache_key = (
        graph_cache_key(graph),
        subject.to_string(),
        predicate_prefix.clone(),
    );
    if let Some(cached) = subject_prefix_presence_cache().get(&cache_key) {
        return !*cached.value();
    }

    let has_prefix = store
        .quads_for_pattern(Some(subject), None, None, graph)
        .filter_map(Result::ok)
        .any(|quad| quad.predicate.as_str().starts_with(&predicate_prefix));

    subject_prefix_presence_cache().insert(cache_key, has_prefix);
    !has_prefix
}
fn sparql_any_solution(
    query: &str,
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    selector: Option<&Term>,
    focus: Option<&Term>,
    value: Option<&Term>,
) -> bool {
    let prefixes = match selector {
        Some(selector) => prefixes_for_selector(store, selector),
        None => String::new(),
    };
    sparql_any_solution_with_bindings(query, &prefixes, store, graph, focus, value, &[])
}

fn sparql_any_solution_lenient(
    query: &str,
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    selector: Option<&Term>,
    focus: Option<&Term>,
    value: Option<&Term>,
) -> bool {
    // Keep this wrapper for backwards-compatible generated validators that
    // call the lenient helper for range checks.
    sparql_any_solution(query, store, graph, selector, focus, value)
}

fn sparql_any_solution_with_bindings(
    query: &str,
    prefixes: &str,
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    focus: Option<&Term>,
    value: Option<&Term>,
    bindings: &[(&str, Term)],
) -> bool {
    let mut normalized_query = query.replace('$', "?");
    let debug = std::env::var("SHACL_DEBUG_SPARQL").is_ok();
    if quick_reject_subject_prefix_query(query, store, graph, focus) {
        if debug {
            eprintln!("SPARQL quick reject: no matching predicate prefix for bound subject");
        }
        return false;
    }
    let mut bind_lines: Vec<String> = Vec::new();
    let mut remaining: Vec<(String, Term)> = Vec::new();
    let mut bind_everywhere = false;
    if let Some(focus) = focus {
        if query_mentions_var(query, "this") {
            if let Some(ground) = term_to_sparql_ground(focus) {
                bind_lines.push(format!("BIND({} AS ?this)", ground));
                bind_everywhere = true;
            } else {
                remaining.push(("this".to_string(), focus.clone()));
            }
        }
    }
    if let Some(value) = value {
        if query_mentions_var(query, "value") {
            if let Some(ground) = term_to_sparql_ground(value) {
                bind_lines.push(format!("BIND({} AS ?value)", ground));
                bind_everywhere = true;
            } else {
                remaining.push(("value".to_string(), value.clone()));
            }
        }
    }
    for (name, term) in bindings {
        if query_mentions_var(query, name) {
            if let Some(ground) = term_to_sparql_ground(term) {
                bind_lines.push(format!("BIND({} AS ?{})", ground, name));
            } else {
                remaining.push(((*name).to_string(), term.clone()));
            }
        }
    }
    if !bind_lines.is_empty() {
        let bindings_clause = bind_lines.join("\n");
        normalized_query = if bind_everywhere {
            inject_bindings_everywhere(&normalized_query, &bindings_clause)
        } else {
            inject_values_clause(&normalized_query, &bindings_clause)
        };
    }
    let query_str = if prefixes.trim().is_empty() {
        normalized_query.clone()
    } else {
        format!("{}\n{}", prefixes, normalized_query)
    };
    if debug {
        eprintln!("SPARQL query:\n{}", query_str);
    }
    let mut prepared = match cached_prepared_query(&query_str) {
        Ok(prepared) => prepared,
        Err(err) => {
            if debug {
                eprintln!("SPARQL parse error: {}", err);
            }
            return false;
        }
    };
    if let Some(graph) = graph {
        prepared.dataset_mut().set_default_graph(vec![graph.into_owned()]);
    } else {
        prepared.dataset_mut().set_default_graph_as_union();
    }
    let mut bound = prepared.on_store(store);
    for (name, term) in &remaining {
        bound = bound
            .substitute_variable(Variable::new_unchecked(name.as_str()), term.clone());
    }
    let query_start = std::time::Instant::now();
    let result = match bound.execute() {
        Ok(QueryResults::Solutions(solutions)) => {
            let mut found = false;
            for row in solutions {
                if row.is_ok() {
                    found = true;
                    break;
                }
            }
            found
        }
        Ok(QueryResults::Boolean(val)) => val,
        Ok(QueryResults::Graph(_)) => false,
        Err(err) => {
            if debug {
                eprintln!("SPARQL execute error: {}", err);
            }
            false
        }
    };
    maybe_record_sparql_timing(query, query_start.elapsed());
    result
}
fn sparql_select_solutions_with_bindings(
    query: &str,
    prefixes: &str,
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    focus: Option<&Term>,
    value: Option<&Term>,
    bindings: &[(&str, Term)],
) -> Vec<HashMap<String, Term>> {
    let mut normalized_query = query.replace('$', "?");
    let debug = std::env::var("SHACL_DEBUG_SPARQL").is_ok();
    if quick_reject_subject_prefix_query(query, store, graph, focus) {
        if debug {
            eprintln!("SPARQL quick reject: no matching predicate prefix for bound subject");
        }
        return Vec::new();
    }
    let mut bind_lines: Vec<String> = Vec::new();
    let mut remaining: Vec<(String, Term)> = Vec::new();
    let mut bind_everywhere = false;
    if let Some(focus) = focus {
        if query_mentions_var(query, "this") {
            if let Some(ground) = term_to_sparql_ground(focus) {
                bind_lines.push(format!("BIND({} AS ?this)", ground));
                bind_everywhere = true;
            } else {
                remaining.push(("this".to_string(), focus.clone()));
            }
        }
    }
    if let Some(value) = value {
        if query_mentions_var(query, "value") {
            if let Some(ground) = term_to_sparql_ground(value) {
                bind_lines.push(format!("BIND({} AS ?value)", ground));
                bind_everywhere = true;
            } else {
                remaining.push(("value".to_string(), value.clone()));
            }
        }
    }
    for (name, term) in bindings {
        if query_mentions_var(query, name) {
            if let Some(ground) = term_to_sparql_ground(term) {
                bind_lines.push(format!("BIND({} AS ?{})", ground, name));
            } else {
                remaining.push(((*name).to_string(), term.clone()));
            }
        }
    }
    if !bind_lines.is_empty() {
        let bindings_clause = bind_lines.join("\n");
        normalized_query = if bind_everywhere {
            inject_bindings_everywhere(&normalized_query, &bindings_clause)
        } else {
            inject_values_clause(&normalized_query, &bindings_clause)
        };
    }
    let query_str = if prefixes.trim().is_empty() {
        normalized_query.clone()
    } else {
        format!("{}\n{}", prefixes, normalized_query)
    };
    if debug {
        eprintln!("SPARQL query:\n{}", query_str);
    }
    let mut prepared = match cached_prepared_query(&query_str) {
        Ok(prepared) => prepared,
        Err(err) => {
            if debug {
                eprintln!("SPARQL parse error: {}", err);
            }
            return Vec::new();
        }
    };
    if let Some(graph) = graph {
        prepared.dataset_mut().set_default_graph(vec![graph.into_owned()]);
    } else {
        prepared.dataset_mut().set_default_graph_as_union();
    }
    let mut bound = prepared.on_store(store);
    for (name, term) in &remaining {
        bound = bound
            .substitute_variable(Variable::new_unchecked(name.as_str()), term.clone());
    }
    let mut out: Vec<HashMap<String, Term>> = Vec::new();
    let query_start = std::time::Instant::now();
    match bound.execute() {
        Ok(QueryResults::Solutions(solutions)) => {
            for solution in solutions {
                let solution = match solution {
                    Ok(solution) => solution,
                    Err(_) => continue,
                };
                let mut row: HashMap<String, Term> = HashMap::new();
                for var in solution.variables() {
                    if let Some(term) = solution.get(var) {
                        row.insert(var.as_str().to_string(), term.clone());
                    }
                }
                out.push(row);
            }
        }
        Ok(_) => {}
        Err(err) => {
            if debug {
                eprintln!("SPARQL execute error: {}", err);
            }
        }
    }
    maybe_record_sparql_timing(query, query_start.elapsed());
    if debug {
        eprintln!("SPARQL solutions: {}", out.len());
    }
    out
}
const SHACL_VALUE: &str = "http://www.w3.org/ns/shacl#value";
const SHACL_MESSAGE: &str = "http://www.w3.org/ns/shacl#message";
const SHACL_RESULT_SEVERITY: &str = "http://www.w3.org/ns/shacl#resultSeverity";
const SHACL_VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";
const SHACL_CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";
fn bool_literal(value: bool) -> &'static str {
    if value {
        "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"
    } else {
        "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"
    }
}
fn term_to_turtle_value(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => format!("<{}>", escape_iri(node.as_str())),
        Term::BlankNode(node) => format!("_:{}", node.as_str()),
        Term::Literal(lit) => {
            let mut out = String::new();
            let lex = lit.value().to_string();
            out.push('\"');
            out.push_str(&escape_literal(&lex));
            out.push('\"');
            if let Some(lang) = lit.language() {
                out.push('@');
                out.push_str(lang);
            } else if lit.datatype().as_str()
                != "http://www.w3.org/2001/XMLSchema#string"
            {
                out.push_str("^^<");
                out.push_str(&escape_iri(lit.datatype().as_str()));
                out.push('>');
            }
            out
        }
    }
}
fn literal_string(value: &str) -> String {
    format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#string>", escape_literal(value))
}
fn escape_literal(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if (ch as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04X}", ch as u32))
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}
fn is_plain_decimal_integer(value: &str) -> bool {
    let mut chars = value.chars();
    let first = match chars.next() {
        Some(ch) => ch,
        None => return false,
    };
    let mut has_digit = false;
    if first.is_ascii_digit() {
        has_digit = true;
    } else if first != '+' && first != '-' {
        return false;
    }
    for ch in chars {
        if !ch.is_ascii_digit() {
            return false;
        }
        has_digit = true;
    }
    has_digit
}
fn normalize_decimal_literal(value: &str) -> String {
    if is_plain_decimal_integer(value) {
        format!("{}.0", value)
    } else {
        value.to_string()
    }
}
fn escape_iri(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '<' | '>' | '\"' | '{' | '}' | '|' | '^' | '`' | '\\' => {
                out.push_str(&format!("\\u{:04X}", ch as u32));
            }
            ch if (ch as u32) < 0x20 || (ch as u32) == 0x7F => {
                out.push_str(&format!("\\u{:04X}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out
}
fn message_terms_for_subject(store: &Store, subject_iri: &str) -> Vec<Term> {
    let mut out = Vec::new();
    let subject = match NamedNode::new(subject_iri) {
        Ok(node) => node,
        Err(_) => return out,
    };
    let sh_message = NamedNodeRef::new(SHACL_MESSAGE).unwrap();
    let shape_graph = shape_graph_ref();
    for quad in store
        .quads_for_pattern(
            Some(NamedOrBlankNodeRef::NamedNode(subject.as_ref())),
            Some(sh_message),
            None,
            Some(shape_graph),
        )
    {
        if let Ok(quad) = quad {
            out.push(quad.object);
        }
    }
    out
}
fn message_terms_for_term(store: &Store, term: &Term) -> Vec<Term> {
    let mut out = Vec::new();
    let subject = match term {
        Term::NamedNode(node) => NamedOrBlankNodeRef::NamedNode(node.as_ref()),
        Term::BlankNode(node) => NamedOrBlankNodeRef::BlankNode(node.as_ref()),
        _ => return out,
    };
    let sh_message = NamedNodeRef::new(SHACL_MESSAGE).unwrap();
    let shape_graph = shape_graph_ref();
    for quad in store
        .quads_for_pattern(Some(subject), Some(sh_message), None, Some(shape_graph))
    {
        if let Ok(quad) = quad {
            out.push(quad.object);
        }
    }
    out
}
fn collect_message_terms(
    store: &Store,
    shape_iri: &str,
    component_iri: &str,
    constraint_term: Option<&Term>,
) -> Vec<Term> {
    let mut messages = message_terms_for_subject(store, shape_iri);
    if messages.is_empty() {
        if let Some(term) = constraint_term {
            messages = message_terms_for_term(store, term);
        }
    }
    if messages.is_empty() {
        messages = message_terms_for_subject(store, component_iri);
    }
    messages
}
fn deskolemize_term(term: Term) -> Term {
    if let Term::NamedNode(node) = &term {
        if let Some(idx) = node.as_str().find("/.sk/") {
            let suffix = &node.as_str()[idx + 5..];
            return Term::BlankNode(BlankNode::new_unchecked(suffix));
        }
    }
    term
}
fn format_validation_report(report: &Report, store: &Store) -> String {
    let graph = validation_report_graph(report, store);
    graph_to_turtle(&graph)
}
fn validation_report_graph(report: &Report, store: &Store) -> Graph {
    let mut graph = Graph::new();
    let report_node: NamedOrBlankNode = BlankNode::default().into();
    let sh_validation_report = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#ValidationReport",
    );
    let sh_validation_result = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#ValidationResult",
    );
    let sh_conforms = NamedNode::new_unchecked("http://www.w3.org/ns/shacl#conforms");
    let sh_result = NamedNode::new_unchecked("http://www.w3.org/ns/shacl#result");
    let sh_focus_node = NamedNode::new_unchecked("http://www.w3.org/ns/shacl#focusNode");
    let sh_source_shape = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#sourceShape",
    );
    let sh_source_constraint_component = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#sourceConstraintComponent",
    );
    let sh_source_constraint = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#sourceConstraint",
    );
    let sh_value = NamedNode::new_unchecked("http://www.w3.org/ns/shacl#value");
    let sh_result_path = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#resultPath",
    );
    let sh_result_severity = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#resultSeverity",
    );
    let sh_result_message = NamedNode::new_unchecked(
        "http://www.w3.org/ns/shacl#resultMessage",
    );
    graph
        .insert(
            Triple::new(
                    report_node.clone(),
                    rdf::TYPE,
                    Term::from(sh_validation_report.clone()),
                )
                .as_ref(),
        );
    graph
        .insert(
            Triple::new(
                    report_node.clone(),
                    sh_conforms.clone(),
                    Term::from(Literal::from(report.violations.is_empty())),
                )
                .as_ref(),
        );
    for violation in report.violations.iter() {
        let result_node: NamedOrBlankNode = BlankNode::default().into();
        graph
            .insert(
                Triple::new(
                        report_node.clone(),
                        sh_result.clone(),
                        Term::from(result_node.clone()),
                    )
                    .as_ref(),
            );
        graph
            .insert(
                Triple::new(
                        result_node.clone(),
                        rdf::TYPE,
                        Term::from(sh_validation_result.clone()),
                    )
                    .as_ref(),
            );
        let focus_term = deskolemize_term(violation.focus.clone());
        graph
            .insert(
                Triple::new(result_node.clone(), sh_focus_node.clone(), focus_term)
                    .as_ref(),
            );
        let shape_iri_str = shape_iri(violation.shape_id);
        let shape_term = deskolemize_term(
            Term::from(NamedNode::new_unchecked(shape_iri_str)),
        );
        graph
            .insert(
                Triple::new(result_node.clone(), sh_source_shape.clone(), shape_term)
                    .as_ref(),
            );
        let component_term = component_iri(violation.component_id);
        let component_node = NamedNode::new_unchecked(component_term);
        graph
            .insert(
                Triple::new(
                        result_node.clone(),
                        sh_source_constraint_component.clone(),
                        Term::from(component_node),
                    )
                    .as_ref(),
            );
        let source_constraint = component_source_constraint(violation.component_id);
        if let Some(source_constraint_term) = source_constraint.as_ref() {
            let source_constraint = deskolemize_term(source_constraint_term.clone());
            graph
                .insert(
                    Triple::new(
                            result_node.clone(),
                            sh_source_constraint.clone(),
                            source_constraint,
                        )
                        .as_ref(),
                );
        }
        if let Some(value) = &violation.value {
            let value_term = deskolemize_term(value.clone());
            graph
                .insert(
                    Triple::new(result_node.clone(), sh_value.clone(), value_term)
                        .as_ref(),
                );
        }
        if let Some(path) = &violation.path {
            let path_term = match path {
                ResultPath::PathId(id) => path_term(*id, &mut graph),
                ResultPath::Term(term) => term.clone(),
            };
            let path_term = deskolemize_term(path_term);
            graph
                .insert(
                    Triple::new(result_node.clone(), sh_result_path.clone(), path_term)
                        .as_ref(),
                );
        }
        let severity_node = NamedNode::new_unchecked(severity_term(violation.shape_id));
        graph
            .insert(
                Triple::new(
                        result_node.clone(),
                        sh_result_severity.clone(),
                        Term::from(severity_node),
                    )
                    .as_ref(),
            );
        let message_terms = collect_message_terms(
            store,
            shape_iri_str,
            component_term,
            source_constraint.as_ref(),
        );
        for message in message_terms {
            graph
                .insert(
                    Triple::new(result_node.clone(), sh_result_message.clone(), message)
                        .as_ref(),
                );
        }
    }
    graph
}
fn subject_to_turtle(subject: &NamedOrBlankNodeRef<'_>) -> String {
    match subject {
        NamedOrBlankNodeRef::NamedNode(node) => {
            format!("<{}>", escape_iri(node.as_str()))
        }
        NamedOrBlankNodeRef::BlankNode(node) => format!("_:{}", node.as_str()),
    }
}
fn predicate_to_turtle(predicate: &NamedNodeRef<'_>) -> String {
    format!("<{}>", escape_iri(predicate.as_str()))
}
fn term_ref_to_turtle_value(term: &TermRef<'_>) -> String {
    match term {
        TermRef::NamedNode(node) => format!("<{}>", escape_iri(node.as_str())),
        TermRef::BlankNode(node) => format!("_:{}", node.as_str()),
        TermRef::Literal(lit) => {
            let mut out = String::new();
            let lex = lit.value().to_string();
            out.push('\"');
            out.push_str(&escape_literal(&lex));
            out.push('\"');
            if let Some(lang) = lit.language() {
                out.push('@');
                out.push_str(lang);
            } else if lit.datatype().as_str()
                != "http://www.w3.org/2001/XMLSchema#string"
            {
                out.push_str("^^<");
                out.push_str(&escape_iri(lit.datatype().as_str()));
                out.push('>');
            }
            out
        }
    }
}
fn graph_to_turtle(graph: &Graph) -> String {
    let mut out = String::new();
    for triple in graph.iter() {
        let subject = subject_to_turtle(&triple.subject);
        let predicate = predicate_to_turtle(&triple.predicate);
        let object = term_ref_to_turtle_value(&triple.object);
        out.push_str(&subject);
        out.push(' ');
        out.push_str(&predicate);
        out.push(' ');
        out.push_str(&object);
        out.push_str(" .\n");
    }
    if out.is_empty() {
        out.push_str("\n.");
    }
    out
}
fn follow_validation_bnodes(graph: &mut Graph, store: &Store) {
    let mut queue: VecDeque<Term> = VecDeque::new();
    let mut seen: HashSet<Term> = HashSet::new();
    for triple in graph.iter() {
        if let NamedOrBlankNodeRef::BlankNode(bn) = triple.subject {
            queue.push_back(Term::BlankNode(bn.into_owned()));
        }
        if let TermRef::BlankNode(bn) = triple.object {
            queue.push_back(Term::BlankNode(bn.into_owned()));
        }
    }
    while let Some(term) = queue.pop_front() {
        if !seen.insert(term.clone()) {
            continue;
        }
        let subject = match subject_ref(&term) {
            Some(subject) => subject,
            None => continue,
        };
        for quad in store.quads_for_pattern(Some(subject), None, None, None) {
            if let Ok(quad) = quad {
                let object = quad.object.clone();
                let triple = Triple::new(
                    quad.subject.clone(),
                    quad.predicate.clone(),
                    object.clone(),
                );
                graph.insert(triple.as_ref());
                if let Term::BlankNode(bn) = object {
                    queue.push_back(Term::BlankNode(bn));
                }
            }
        }
    }
}
pub fn render_report(report: &Report, store: &Store, follow_bnodes: bool) -> String {
    let mut graph = validation_report_graph(report, store);
    if follow_bnodes {
        follow_validation_bnodes(&mut graph, store);
    }
    graph_to_turtle(&graph)
}
fn shape_iri(_shape_id: u64) -> &'static str {
    ""
}
fn component_iri(_component_id: u64) -> &'static str {
    "http://www.w3.org/ns/shacl#ConstraintComponent"
}
fn component_source_constraint(_component_id: u64) -> Option<Term> {
    None
}
fn severity_term(_shape_id: u64) -> &'static str {
    "http://www.w3.org/ns/shacl#Violation"
}
const SHAPE_SUBCLASS_EDGES: &[(&str, &str)] = &[];
