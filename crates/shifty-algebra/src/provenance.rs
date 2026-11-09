//! Authoring provenance for arena slots built by a front end other than SHACL.
//!
//! [`Schema::sources`](crate::Schema::sources) maps a slot back to the RDF node
//! it was lowered from. A text front end (Shapeshift) has no RDF node to point
//! at: a slot comes from a named shape, a field line, a macro call, an
//! `include`, or one branch of a boolean frame. [`Origin`] records that, with
//! the source location, so a violation can be reported against the line the
//! author wrote, through every macro and include it passed through.
//!
//! Provenance is a sidecar: the evaluator never consults it, so the execution
//! IR stays small and canonical. A slot can carry several origins, because
//! arena interning or normalization may merge structurally identical slots,
//! and every author-visible origin of a merged slot must stay reportable.

use crate::shape::ShapeId;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Where in a source file a construct was written. Lines and columns are
/// 1-based, matching what editors display.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

impl fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

/// The connective a boolean frame names. A frame compiles to ordinary
/// `And`/`Or`/`Not` terms; recording which frame it was is what lets a raiser
/// restore the idiom (`sh:xone`, `sh:or ( [ sh:not C ] B )`) after the smart
/// constructors have flattened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BooleanFrame {
    AllOf,
    AnyOf,
    ExactlyOneOf,
    NoneOf,
    /// `when {C} then {B}`; branch 0 is the condition, branch 1 the body.
    When,
    /// `at least n of {...} {...}`.
    AtLeast(u64),
    /// `at most n of {...} {...}`.
    AtMost(u64),
}

impl BooleanFrame {
    /// The phrase the author wrote.
    pub fn phrase(&self) -> String {
        match self {
            Self::AllOf => "all of".into(),
            Self::AnyOf => "any of".into(),
            Self::ExactlyOneOf => "exactly one of".into(),
            Self::NoneOf => "none of".into(),
            Self::When => "when".into(),
            Self::AtLeast(n) => format!("at least {n} of"),
            Self::AtMost(n) => format!("at most {n} of"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OriginKind {
    /// The slot is the body of the named shape `name`.
    Shape { name: String },
    /// The slot is the expansion of a template call.
    MacroCall { name: String, args: Vec<String> },
    /// The slot was pulled into `into` by `include from`.
    Include { from: String, into: String },
    /// The slot is one field line of `shape`. `text` is the line as the
    /// formatter renders it, for reports.
    Field {
        shape: String,
        name: Option<String>,
        path: String,
        text: String,
    },
    /// The slot is the compiled term of a boolean frame. `branches` are the
    /// branch slots in authored order, before any flattening.
    Frame {
        frame: BooleanFrame,
        branches: Vec<ShapeId>,
    },
    /// The slot is branch `index` of a boolean frame.
    BooleanBranch { frame: BooleanFrame, index: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Origin {
    pub kind: OriginKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
}

impl Origin {
    pub fn new(kind: OriginKind, location: Option<SourceLocation>) -> Self {
        Self { kind, location }
    }

    /// Remap the shape ids this origin mentions (frame branches).
    pub fn remap(&self, f: &impl Fn(ShapeId) -> ShapeId) -> Origin {
        let kind = match &self.kind {
            OriginKind::Frame { frame, branches } => OriginKind::Frame {
                frame: *frame,
                branches: branches.iter().map(|b| f(*b)).collect(),
            },
            other => other.clone(),
        };
        Origin {
            kind,
            location: self.location.clone(),
        }
    }
}
