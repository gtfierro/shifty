//! Build-time diagnostics, shared by the parser, the builder, and the import
//! resolver.

use serde::{Deserialize, Serialize};
use shifty_algebra::SourceLocation;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Level {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: Level,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    /// Extra lines shown under the message (e.g. a recursion path).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl Diagnostic {
    pub fn error(message: impl Into<String>, location: Option<SourceLocation>) -> Self {
        Self {
            level: Level::Error,
            message: message.into(),
            location,
            notes: Vec::new(),
            hint: None,
        }
    }

    pub fn warning(message: impl Into<String>, location: Option<SourceLocation>) -> Self {
        Self {
            level: Level::Warning,
            ..Self::error(message, location)
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    pub fn is_error(&self) -> bool {
        self.level == Level::Error
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.level {
            Level::Error => "error",
            Level::Warning => "warning",
        };
        writeln!(f, "{level}: {}", self.message)?;
        if let Some(loc) = &self.location {
            writeln!(f, "  --> {}:{}:{}", loc.file, loc.line, loc.column)?;
        }
        for note in &self.notes {
            writeln!(f, "  {note}")?;
        }
        if let Some(hint) = &self.hint {
            writeln!(f, "  hint: {hint}")?;
        }
        Ok(())
    }
}

/// Render a list of diagnostics, one block each.
pub fn render_all(diagnostics: &[Diagnostic]) -> String {
    diagnostics.iter().map(ToString::to_string).collect()
}

/// Levenshtein distance, for nearest-name suggestions.
pub fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(!a[i - 1].eq_ignore_ascii_case(&b[j - 1]));
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// The candidate closest to `name`, if it is close enough to be a plausible
/// typo (distance at most a third of the name's length, minimum 2).
pub fn nearest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let limit = (name.chars().count() / 3).max(2);
    candidates
        .into_iter()
        .map(|c| (edit_distance(name, c), c))
        .filter(|(d, _)| *d <= limit)
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
        .map(|(_, c)| c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_finds_typos() {
        let names = ["hasPoint", "hasPart", "isFedBy"];
        assert_eq!(nearest("hasPoints", names), Some("hasPoint"));
        assert_eq!(nearest("zzzzzz", names), None);
    }
}
