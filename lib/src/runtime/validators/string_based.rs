use crate::context::{sanitize_graphviz_string, Context, ValidationContext};
use crate::runtime::validators::parallel_value_node_checks;
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::{NamedNode, TermRef};
// Removed: use regex::Regex;
use std::collections::HashSet;
use std::sync::Arc;

use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure,
};

// string-based constraints
#[derive(Debug)]
pub struct MinLengthConstraintComponent {
    min_length: u64,
}

impl MinLengthConstraintComponent {
    pub fn new(min_length: u64) -> Self {
        MinLengthConstraintComponent { min_length }
    }
}

impl GraphvizOutput for MinLengthConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MinLengthConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MinLength: {}\"];",
            component_id.to_graphviz_id(),
            self.min_length
        )
    }
}

impl ValidateComponent for MinLengthConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let results = c
            .value_nodes()
            .cloned()
            .map(|value_nodes| {
                let min_len = self.min_length as usize;
                parallel_value_node_checks(value_nodes, c, move |value_node, mut ctx| {
                    let len = match value_node.as_ref() {
                        TermRef::BlankNode(_) => {
                            ctx.with_value(value_node.clone());
                            let failure = ValidationFailure {
                                component_id,
                                failed_value_node: Some(value_node.clone()),
                                message: format!(
                                    "Blank node {:?} found where string length constraints apply (minLength).",
                                    value_node
                                ),
                                result_path: None,
                                source_constraint: None,

                                severity: None,

                                message_terms: Vec::new(),
                            };
                            return Some(ComponentValidationResult::Fail(ctx, failure));
                        }
                        TermRef::NamedNode(nn) => {
                            if validation_context.is_data_skolem_iri(nn) {
                                ctx.with_value(value_node.clone());
                                let failure = ValidationFailure {
                                    component_id,
                                    failed_value_node: Some(value_node.clone()),
                                    message: format!(
                                        "Blank node {:?} found where string length constraints apply (minLength).",
                                        value_node
                                    ),
                                    result_path: None,
                                    source_constraint: None,

                                    severity: None,

                                    message_terms: Vec::new(),
                                };
                                return Some(ComponentValidationResult::Fail(ctx, failure));
                            }
                            nn.as_str().chars().count()
                        }
                        TermRef::Literal(literal) => literal.value().chars().count(),
                    };

                    if len < min_len {
                        ctx.with_value(value_node.clone());
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Value {:?} has length {} which is less than minLength {}.",
                                value_node, len, self.min_length
                            ),
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        };
                        return Some(ComponentValidationResult::Fail(ctx, failure));
                    }

                    None
                })
            })
            .unwrap_or_default();

        Ok(results)
    }
}

#[derive(Debug)]
pub struct MaxLengthConstraintComponent {
    max_length: u64,
}

impl MaxLengthConstraintComponent {
    pub fn new(max_length: u64) -> Self {
        MaxLengthConstraintComponent { max_length }
    }
}

impl GraphvizOutput for MaxLengthConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MaxLengthConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MaxLength: {}\"];",
            component_id.to_graphviz_id(),
            self.max_length
        )
    }
}

impl ValidateComponent for MaxLengthConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let results = c
            .value_nodes()
            .cloned()
            .map(|value_nodes| {
                let max_len = self.max_length as usize;
                parallel_value_node_checks(value_nodes, c, move |value_node, mut ctx| {
                    let len = match value_node.as_ref() {
                        TermRef::BlankNode(_) => {
                            ctx.with_value(value_node.clone());
                            let failure = ValidationFailure {
                                component_id,
                                failed_value_node: Some(value_node.clone()),
                                message: format!(
                                    "Blank node {:?} found where string length constraints apply (maxLength).",
                                    value_node
                                ),
                                result_path: None,
                                source_constraint: None,

                                severity: None,

                                message_terms: Vec::new(),
                            };
                            return Some(ComponentValidationResult::Fail(ctx, failure));
                        }
                        TermRef::NamedNode(nn) => {
                            if validation_context.is_data_skolem_iri(nn) {
                                ctx.with_value(value_node.clone());
                                let failure = ValidationFailure {
                                    component_id,
                                    failed_value_node: Some(value_node.clone()),
                                    message: format!(
                                        "Blank node {:?} found where string length constraints apply (maxLength).",
                                        value_node
                                    ),
                                    result_path: None,
                                    source_constraint: None,

                                    severity: None,

                                    message_terms: Vec::new(),
                                };
                                return Some(ComponentValidationResult::Fail(ctx, failure));
                            }
                            nn.as_str().chars().count()
                        }
                        TermRef::Literal(literal) => literal.value().chars().count(),
                    };

                    if len > max_len {
                        ctx.with_value(value_node.clone());
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Value {:?} has length {} which is greater than maxLength {}.",
                                value_node, len, self.max_length
                            ),
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        };
                        return Some(ComponentValidationResult::Fail(ctx, failure));
                    }

                    None
                })
            })
            .unwrap_or_default();

        Ok(results)
    }
}

#[derive(Debug)]
pub struct PatternConstraintComponent {
    pattern: String,
    flags: Option<String>,
}

impl PatternConstraintComponent {
    pub fn new(pattern: String, flags: Option<String>) -> Self {
        PatternConstraintComponent { pattern, flags }
    }
}

impl GraphvizOutput for PatternConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#PatternConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let flags_str = self.flags.as_deref().unwrap_or("");
        format!(
            "{} [label=\"Pattern: {}\\nFlags: {}\"];",
            component_id.to_graphviz_id(),
            sanitize_graphviz_string(&self.pattern), // Pattern is a String, not a Term
            flags_str
        )
    }
}

impl ValidateComponent for PatternConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let mut pattern_builder = regex::RegexBuilder::new(&self.pattern);
        if let Some(flags) = &self.flags {
            // Note: SHACL flags are not identical to Rust regex crate flags.
            // 'i' for case-insensitive is common.
            // 'm' (multiline), 's' (dot matches newline), 'x' (ignore whitespace)
            // 'U' (ungreedy) are other SPARQL flags.
            // Rust regex crate uses (?i), (?m), (?s), (?x) within the pattern.
            // We'll only handle 'i' for simplicity here.
            // A full implementation would parse SPARQL flags and convert them.
            if flags.contains('i') {
                pattern_builder.case_insensitive(true);
            }
            // Other flags would need more complex handling or might not be directly supported.
        }

        let re = match pattern_builder.build() {
            Ok(r) => r,
            Err(e) => return Err(format!("Invalid regex pattern '{}': {}", self.pattern, e)),
        };

        let re = Arc::new(re);
        let pattern_value = Arc::new(self.pattern.clone());
        let flags_value = Arc::new(self.flags.clone());
        let results = parallel_value_node_checks(
            c.value_nodes().cloned().unwrap_or_default(),
            c,
            move |value_node, mut ctx| {
                let value_str = match value_node.as_ref() {
                    TermRef::BlankNode(_) => {
                        ctx.with_value(value_node.clone());
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Blank node {:?} cannot be matched against a pattern.",
                                value_node
                            ),
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        };
                        return Some(ComponentValidationResult::Fail(ctx, failure));
                    }
                    TermRef::NamedNode(nn) => {
                        if validation_context.is_data_skolem_iri(nn) {
                            ctx.with_value(value_node.clone());
                            let failure = ValidationFailure {
                                component_id,
                                failed_value_node: Some(value_node.clone()),
                                message: format!(
                                    "Blank node {:?} cannot be matched against a pattern.",
                                    value_node
                                ),
                                result_path: None,
                                source_constraint: None,

                                severity: None,

                                message_terms: Vec::new(),
                            };
                            return Some(ComponentValidationResult::Fail(ctx, failure));
                        }
                        nn.as_str().to_string()
                    }
                    TermRef::Literal(literal) => literal.value().to_string(),
                };

                if !re.is_match(&value_str) {
                    ctx.with_value(value_node.clone());
                    let flag_suffix = flags_value
                        .as_ref()
                        .as_ref()
                        .map(|f| format!(" with flags '{}'", f))
                        .unwrap_or_default();
                    let message = format!(
                        "Value {:?} does not match pattern '{}'{}.",
                        value_node,
                        pattern_value.as_str(),
                        flag_suffix
                    );
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message,
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    return Some(ComponentValidationResult::Fail(ctx, failure));
                }

                None
            },
        );

        Ok(results)
    }
}

#[derive(Debug)]
pub struct LanguageInConstraintComponent {
    languages: Vec<String>,
}

impl LanguageInConstraintComponent {
    pub fn new(languages: Vec<String>) -> Self {
        LanguageInConstraintComponent { languages }
    }
}

impl GraphvizOutput for LanguageInConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#LanguageInConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"LanguageIn: [{}]\"];",
            component_id.to_graphviz_id(),
            self.languages.join(", ")
        )
    }
}

/// Implements SPARQL langMatches behavior.
/// tag: the language tag of the literal (e.g., "en-US")
/// range: the language range from sh:languageIn (e.g., "en", "en-GB", "*")
fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag_lower = tag.to_lowercase();
    let range_lower = range.to_lowercase();

    if tag_lower == range_lower {
        return true;
    }

    // Check if range is a prefix of tag, separated by '-'
    // e.g., tag "en-us-calif", range "en-us"
    if range_lower
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        // Basic check for valid lang-range prefix
        if tag_lower.starts_with(&format!("{}-", range_lower)) {
            return true;
        }
    }
    false
}

impl ValidateComponent for LanguageInConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        _validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let languages = Arc::new(self.languages.clone());
        let results = parallel_value_node_checks(
            c.value_nodes().cloned().unwrap_or_default(),
            c,
            move |value_node, mut ctx| match value_node.as_ref() {
                TermRef::Literal(literal) => {
                    let lit_lang = literal.language().unwrap_or("");
                    let allowed_langs = languages.as_ref();
                    if allowed_langs.is_empty() {
                        ctx.with_value(value_node.clone());
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Value {:?} fails sh:languageIn constraint because the list of allowed languages is empty.",
                                value_node
                            ),
                            result_path: None,
                            source_constraint: None,
                            severity: None,
                            message_terms: Vec::new(),
                        };
                        Some(ComponentValidationResult::Fail(ctx, failure))
                    } else {
                        let matched = allowed_langs
                            .iter()
                            .any(|allowed_lang| lang_matches(lit_lang, allowed_lang));
                        if !matched {
                            ctx.with_value(value_node.clone());
                            let failure = ValidationFailure {
                                component_id,
                                failed_value_node: Some(value_node.clone()),
                                message: format!(
                                    "Language tag '{}' of value {:?} is not in the allowed list {:?}.",
                                    lit_lang, value_node, allowed_langs
                                ),
                                result_path: None,
                                source_constraint: None,
                                severity: None,
                                message_terms: Vec::new(),
                            };
                            Some(ComponentValidationResult::Fail(ctx, failure))
                        } else {
                            None
                        }
                    }
                }
                _ => {
                    ctx.with_value(value_node.clone());
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {:?} is not a literal, but sh:languageIn applies to literals.",
                            value_node
                        ),
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    Some(ComponentValidationResult::Fail(ctx, failure))
                }
            },
        );

        Ok(results)
    }
}

#[derive(Debug)]
pub struct UniqueLangConstraintComponent {
    unique_lang: bool,
}

impl UniqueLangConstraintComponent {
    pub fn new(unique_lang: bool) -> Self {
        UniqueLangConstraintComponent { unique_lang }
    }
}

impl GraphvizOutput for UniqueLangConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#UniqueLangConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"UniqueLang: {}\"];",
            component_id.to_graphviz_id(),
            self.unique_lang
        )
    }
}

impl ValidateComponent for UniqueLangConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        _validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if !self.unique_lang {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        if let Some(value_nodes) = c.value_nodes() {
            let mut lang_tags_seen = HashSet::new();
            let mut duplicated_tags = HashSet::new();

            for vn in value_nodes {
                if let TermRef::Literal(lit) = vn.as_ref() {
                    if let Some(lang) = lit.language() {
                        if !lang.is_empty() {
                            // SHACL spec: "for each non-empty language tag"
                            if !lang_tags_seen.insert(lang.to_lowercase()) {
                                // If insert returns false, it means the value was already present.
                                duplicated_tags.insert(lang.to_lowercase());
                            }
                        }
                    }
                }
            }

            for duplicated_tag in duplicated_tags {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: None, // sh:uniqueLang does not produce a sh:value
                    message: format!(
                        "Language tag '{}' is used by more than one value node, but sh:uniqueLang is true.",
                        duplicated_tag
                    ),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                results.push(ComponentValidationResult::Fail(c.clone(), failure));
            }
        }
        Ok(results)
    }
}
