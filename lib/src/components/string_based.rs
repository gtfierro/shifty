use crate::context::{sanitize_graphviz_string, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, TermRef};
// Removed: use regex::Regex;
use std::collections::HashSet;

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent};

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
        SHACL::new().min_length_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        if let Some(value_nodes) = c.value_nodes() {
            for vn in value_nodes {
                let len = match vn.as_ref() {
                    TermRef::BlankNode(_) => {
                        return Err(format!(
                            "Blank node {:?} found where string length constraints apply (minLength).",
                            vn
                        ));
                    }
                    TermRef::NamedNode(nn) => nn.as_str().chars().count(),
                    TermRef::Literal(literal) => literal.value().chars().count(),
                    _ => {
                        return Err(format!(
                            "Unexpected term type for minLength check: {:?}",
                            vn
                        ));
                    }
                };
                if len < self.min_length as usize {
                    return Err(format!(
                        "Value {:?} has length {} which is less than minLength {}.",
                        vn, len, self.min_length
                    ));
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
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
        SHACL::new().max_length_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        if let Some(value_nodes) = c.value_nodes() {
            for vn in value_nodes {
                let len = match vn.as_ref() {
                    TermRef::BlankNode(_) => {
                        return Err(format!(
                            "Blank node {:?} found where string length constraints apply (maxLength).",
                            vn
                        ));
                    }
                    TermRef::NamedNode(nn) => nn.as_str().chars().count(),
                    TermRef::Literal(literal) => literal.value().chars().count(),
                    _ => {
                        return Err(format!(
                            "Unexpected term type for maxLength check: {:?}",
                            vn
                        ));
                    }
                };
                if len > self.max_length as usize {
                    return Err(format!(
                        "Value {:?} has length {} which is greater than maxLength {}.",
                        vn, len, self.max_length
                    ));
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
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
        SHACL::new().pattern_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
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

        if let Some(value_nodes) = c.value_nodes() {
            for vn in value_nodes {
                let value_str = match vn.as_ref() {
                    TermRef::BlankNode(_) => {
                        return Err(format!(
                            "Blank node {:?} cannot be matched against a pattern.",
                            vn
                        ));
                    }
                    TermRef::NamedNode(nn) => nn.as_str().to_string(),
                    TermRef::Literal(literal) => literal.value().to_string(),
                    _ => {
                        return Err(format!("Unexpected term type for pattern check: {:?}", vn));
                    }
                };

                if !re.is_match(&value_str) {
                    return Err(format!(
                        "Value {:?} does not match pattern '{}'{}.",
                        vn,
                        self.pattern,
                        self.flags
                            .as_ref()
                            .map_or("".to_string(), |f| format!(" with flags '{}'", f))
                    ));
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
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
        SHACL::new().language_in_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        if let Some(value_nodes) = c.value_nodes() {
            for vn in value_nodes {
                match vn.as_ref() {
                    TermRef::Literal(lit) => {
                        let lit_lang = lit.language().unwrap_or("");
                        if self.languages.is_empty() {
                            // "If the SHACL list is empty, then no value nodes can satisfy the constraint."
                            // This implies any literal (tagged or not) fails if there are value nodes.
                            // If there are no value nodes, it passes (covered by outer Some(value_nodes)).
                            return Err(format!(
                                "Value {:?} fails sh:languageIn constraint because the list of allowed languages is empty.",
                                vn
                            ));
                        }
                        let matched = self
                            .languages
                            .iter()
                            .any(|allowed_lang| lang_matches(lit_lang, allowed_lang));
                        if !matched {
                            return Err(format!(
                                "Language tag '{}' of value {:?} is not in the allowed list {:?}.",
                                lit_lang, vn, self.languages
                            ));
                        }
                    }
                    _ => {
                        // Not a literal, so it cannot conform to a languageIn constraint.
                        return Err(format!(
                            "Value {:?} is not a literal, but sh:languageIn applies to literals.",
                            vn
                        ));
                    }
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
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
        SHACL::new().unique_lang_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        if !self.unique_lang {
            return Ok(ComponentValidationResult::Pass(component_id));
        }

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

            if !duplicated_tags.is_empty() {
                return Err(format!(
                    "Duplicate language tags found: {:?}. sh:uniqueLang is true.",
                    duplicated_tags
                        .into_iter()
                        .collect::<Vec<String>>()
                        .join(", ")
                ));
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}
