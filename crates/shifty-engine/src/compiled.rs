//! Shapes-document ownership and admission for reusable evaluation sessions.

use crate::frozen::SourceStorage;
use crate::sparql::FunctionDef;
use crate::validate::NonStratifiable;
use shifty_algebra::{Schema, ShapeId};
use shifty_opt::{
    AccessCatalog, PhysicalPlan, RuleDependencies, Stratification, analyze,
    normalize_with_mapping_and_analysis, plan, rule_dependencies, rule_guard_dependencies,
};
use shifty_parse::{Diagnostic, Loaded, ParseError, parse_loaded};
use spargebra::Query;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

pub(crate) type ParsedQueries = HashMap<String, Query>;

/// A shapes document compiled once for use with many data snapshots.
#[derive(Clone)]
pub struct CompiledShapes {
    pub(crate) inner: Arc<CompiledShapesInner>,
}

pub(crate) struct CompiledShapesInner {
    pub(crate) source: Arc<Loaded>,
    pub(crate) authored: Arc<Schema>,
    pub(crate) normalized: Arc<Schema>,
    statement_map: Arc<Vec<usize>>,
    pub(crate) shape_map: Arc<Vec<Option<ShapeId>>>,
    pub(crate) raw_by_normalized: Arc<Vec<Vec<usize>>>,
    pub(crate) diagnostics: Vec<Diagnostic>,
    pub(crate) functions: Vec<FunctionDef>,
    pub(crate) rules: Vec<CompiledRuleSchedule>,
    pub(crate) access: AccessCatalog,
    pub(crate) parsed_queries: Arc<ParsedQueries>,
    physical: OnceLock<PhysicalPlan>,
    source_storage: OnceLock<Arc<SourceStorage>>,
}

pub(crate) struct CompiledRuleSchedule {
    pub(crate) index: usize,
    pub(crate) order: i64,
    pub(crate) dependencies: RuleDependencies,
    pub(crate) guard_dependencies: RuleDependencies,
}

#[derive(Debug)]
pub enum CompileError {
    Invalid(ParseError),
    NonStratifiable(NonStratifiable),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => write!(f, "{error}"),
            Self::NonStratifiable(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for CompileError {}

fn check_strata(schema: &Schema) -> Result<Stratification, CompileError> {
    let analysis = analyze(&schema.arena);
    if analysis.stratifiable {
        return Ok(analysis);
    }
    Err(CompileError::NonStratifiable(NonStratifiable {
        components: analysis
            .strata
            .into_iter()
            .filter(|stratum| !stratum.stratifiable)
            .map(|stratum| stratum.shapes)
            .collect(),
    }))
}

impl CompiledShapes {
    pub fn compile(source: Loaded) -> Result<Self, CompileError> {
        let parsed = parse_loaded(&source);
        parsed.require_valid().map_err(CompileError::Invalid)?;
        let authored_analysis = check_strata(&parsed.schema)?;
        let normalized = normalize_with_mapping_and_analysis(&parsed.schema, &authored_analysis);
        check_strata(&normalized.schema)?;
        let mut raw_by_normalized = vec![Vec::new(); normalized.schema.statements.len()];
        for (authored, normalized_id) in normalized.statement_map.iter().copied().enumerate() {
            raw_by_normalized[normalized_id].push(authored);
        }
        let functions = shifty_parse::collect_functions(&source);
        let access_started = crate::profile::is_enabled().then(web_time::Instant::now);
        let access = AccessCatalog::compile(&parsed.schema, &functions);
        if let Some(access_started) = access_started {
            crate::profile::record_access_catalog_time(access_started.elapsed().as_micros() as u64);
        }
        let parsed_queries = Arc::new(
            access
                .queries
                .iter()
                .filter_map(|query| {
                    query
                        .parsed
                        .clone()
                        .map(|parsed| (query.text.clone(), parsed))
                })
                .collect(),
        );
        let mut rules: Vec<_> = normalized
            .schema
            .rules
            .iter()
            .enumerate()
            .filter(|(_, rule)| !rule.deactivated)
            .map(|(index, rule)| CompiledRuleSchedule {
                index,
                order: rule.order.unwrap_or(0),
                dependencies: rule_dependencies(rule, &normalized.schema.arena),
                guard_dependencies: rule_guard_dependencies(rule, &normalized.schema.arena),
            })
            .collect();
        rules.sort_by_key(|rule| (rule.order, rule.index));
        Ok(Self {
            inner: Arc::new(CompiledShapesInner {
                source: Arc::new(source),
                authored: Arc::new(parsed.schema),
                normalized: Arc::new(normalized.schema),
                statement_map: Arc::new(normalized.statement_map),
                shape_map: Arc::new(normalized.shape_map),
                raw_by_normalized: Arc::new(raw_by_normalized),
                diagnostics: parsed.diagnostics,
                functions,
                rules,
                access,
                parsed_queries,
                physical: OnceLock::new(),
                source_storage: OnceLock::new(),
            }),
        })
    }

    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.inner.diagnostics
    }

    /// Data-independent graph reads associated with authored consumers.
    pub fn access_catalog(&self) -> &AccessCatalog {
        &self.inner.access
    }

    pub fn source(&self) -> &Loaded {
        &self.inner.source
    }

    /// Share the authored RDF document without copying its graph.
    pub fn source_shared(&self) -> Arc<Loaded> {
        Arc::clone(&self.inner.source)
    }

    pub(crate) fn source_storage(&self) -> Arc<SourceStorage> {
        Arc::clone(self.inner.source_storage.get_or_init(|| {
            SourceStorage::encode_with_demand(&self.inner.source.graph, &self.inner.access)
        }))
    }

    pub(crate) fn parsed_queries(&self) -> Arc<ParsedQueries> {
        Arc::clone(&self.inner.parsed_queries)
    }

    /// Mapping from authored statement IDs to normalized execution IDs.
    pub fn statement_map(&self) -> &[usize] {
        &self.inner.statement_map
    }

    /// Share the authored-to-normalized statement mapping.
    pub fn statement_map_shared(&self) -> Arc<Vec<usize>> {
        Arc::clone(&self.inner.statement_map)
    }

    /// The authored schema retained for source-oriented inspection.
    pub fn authored_schema(&self) -> &Schema {
        &self.inner.authored
    }

    /// Share the authored schema with a result or adapter.
    pub fn authored_schema_shared(&self) -> Arc<Schema> {
        Arc::clone(&self.inner.authored)
    }

    /// The canonical schema used by sessions.
    pub fn normalized_schema(&self) -> &Schema {
        &self.inner.normalized
    }

    /// Share the executable schema with a result or adapter.
    pub fn normalized_schema_shared(&self) -> Arc<Schema> {
        Arc::clone(&self.inner.normalized)
    }

    /// Build the physical validation plan only when explicitly requested.
    pub fn physical_plan(&self) -> &PhysicalPlan {
        self.inner
            .physical
            .get_or_init(|| plan(&self.inner.normalized))
    }
}

// Compilation contains only owned RDF, IR, and immutable planning data.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CompiledShapes>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FindingOptions, SessionData, SessionOptions};

    #[test]
    fn source_encoding_is_lazy_and_shared_by_clones() {
        let source =
            shifty_parse::load_turtle(br#"@prefix ex: <http://ex/> . ex:a ex:p ex:b ."#, None)
                .unwrap();
        let compiled = CompiledShapes::compile(source).unwrap();
        assert!(compiled.inner.source_storage.get().is_none());
        crate::profile::enable();
        let first = compiled.source_storage();
        let second = compiled.clone().source_storage();
        assert!(Arc::ptr_eq(&first, &second));
        let storage = crate::profile::take().unwrap().storage().clone();
        assert_eq!(storage.source_builds, 1);
        assert_eq!(storage.source_rows, 1);
    }

    #[test]
    fn canonical_query_templates_are_parsed_once_and_shared() {
        let source = shifty_parse::load_turtle(
            br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ; sh:targetNode ex:a ;
                sh:sparql [ sh:select "SELECT $this WHERE { $this ex:p ?o }" ] .
            "#,
            None,
        )
        .unwrap();
        let compiled = CompiledShapes::compile(source).unwrap();
        assert_eq!(compiled.inner.parsed_queries.len(), 1);
        assert!(Arc::ptr_eq(
            &compiled.parsed_queries(),
            &compiled.clone().parsed_queries()
        ));
    }

    #[test]
    fn sessions_share_compilation_and_plan_only_when_validation_uses_it() {
        let source = shifty_parse::load_turtle(
            br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ; sh:targetNode ex:a ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            "#,
            None,
        )
        .unwrap();
        let compiled = CompiledShapes::compile(source).unwrap();
        let clone = compiled.clone();
        assert!(Arc::ptr_eq(&compiled.inner, &clone.inner));
        assert!(compiled.inner.physical.get().is_none());

        crate::profile::enable();
        let session = compiled
            .session(
                SessionData::Embedded,
                SessionOptions {
                    inference: true,
                    ..SessionOptions::default()
                },
            )
            .unwrap();
        assert_eq!(
            crate::profile::take().unwrap().storage().dataset_builds,
            0,
            "a compilation with no executable rules needs no inference dataset",
        );
        assert!(
            compiled.inner.source_storage.get().is_none(),
            "source encoding should wait until a graph reader needs it",
        );
        assert!(compiled.inner.physical.get().is_none());
        assert!(!session.has_prepared_dataset());
        session.validate(&FindingOptions::default()).unwrap();
        assert!(compiled.inner.source_storage.get().is_some());
        assert!(compiled.inner.physical.get().is_some());
        assert!(session.has_prepared_dataset());
        assert!(std::ptr::eq(
            compiled.physical_plan(),
            clone.physical_plan()
        ));

        crate::profile::enable();
        let second = clone
            .session(SessionData::Embedded, SessionOptions::default())
            .unwrap();
        second.validate(&FindingOptions::default()).unwrap();
        let storage = crate::profile::take().unwrap().storage().clone();
        assert_eq!(
            storage.source_builds, 0,
            "source was encoded by the first session"
        );
        assert_eq!(storage.dataset_builds, 1);
    }
}
