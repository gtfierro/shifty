use crate::NamedNode;
use serde::{Deserialize, Serialize};
use std::fmt;

const SH_INFO: &str = "http://www.w3.org/ns/shacl#Info";
const SH_WARNING: &str = "http://www.w3.org/ns/shacl#Warning";
const SH_VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";

/// The severity attached to a SHACL shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum Severity {
    Info,
    Warning,
    #[default]
    Violation,
    /// A custom SHACL severity. Custom levels are treated as violations when
    /// applying a standard severity threshold.
    Custom(NamedNode),
}

impl Severity {
    pub fn from_named_node(value: NamedNode) -> Self {
        match value.as_str() {
            SH_INFO => Self::Info,
            SH_WARNING => Self::Warning,
            SH_VIOLATION => Self::Violation,
            _ => Self::Custom(value),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Info => SH_INFO,
            Self::Warning => SH_WARNING,
            Self::Violation => SH_VIOLATION,
            Self::Custom(value) => value.as_str(),
        }
    }

    pub fn label(&self) -> &str {
        match self {
            Self::Info => "Info",
            Self::Warning => "Warning",
            Self::Violation => "Violation",
            Self::Custom(value) => value.as_str(),
        }
    }

    /// Standard severity rank, ordered from least to most severe.
    pub fn rank(&self) -> u8 {
        match self {
            Self::Info => 0,
            Self::Warning => 1,
            Self::Violation | Self::Custom(_) => 2,
        }
    }

    pub fn meets(&self, minimum: &Self) -> bool {
        self.rank() >= minimum.rank()
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}
