//! Parse/lowering diagnostics. Unsupported constructs are reported here rather
//! than silently dropped (gap-analysis principle: never a silent wrong answer).

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagLevel {
    Warning,
    Unsupported,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: DiagLevel,
    pub message: String,
    /// The shape/term the diagnostic concerns, rendered for display.
    pub subject: Option<String>,
}

impl Diagnostic {
    pub fn new(level: DiagLevel, message: impl Into<String>, subject: Option<String>) -> Self {
        Self { level, message: message.into(), subject }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag = match self.level {
            DiagLevel::Warning => "warning",
            DiagLevel::Unsupported => "unsupported",
            DiagLevel::Error => "error",
        };
        match &self.subject {
            Some(s) => write!(f, "[{tag}] {} ({s})", self.message),
            None => write!(f, "[{tag}] {}", self.message),
        }
    }
}

/// Fatal parse error (e.g. malformed Turtle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}
