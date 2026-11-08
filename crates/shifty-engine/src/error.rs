//! Failures to execute validation, distinct from findings about the data.

use crate::validate::NonStratifiable;
use std::fmt;

/// Which SPARQL operation could not be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SparqlOperation {
    Target,
    Constraint,
    AskValidator,
    SelectValidator,
}

/// An evaluation could not complete. No validation verdict is available.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvaluationError {
    ForeignPair,
    Sparql {
        operation: SparqlOperation,
        message: String,
    },
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignPair => write!(f, "selected pair belongs to another session"),
            Self::Sparql { operation, message } => {
                write!(f, "SPARQL {operation:?} evaluation failed: {message}")
            }
        }
    }
}

impl std::error::Error for EvaluationError {}

/// Failure of a standalone validation call, during preparation or evaluation.
#[derive(Debug)]
#[non_exhaustive]
pub enum ValidationError {
    NonStratifiable(NonStratifiable),
    Evaluation(EvaluationError),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonStratifiable(error) => error.fmt(f),
            Self::Evaluation(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::NonStratifiable(error) => error,
            Self::Evaluation(error) => error,
        })
    }
}

impl From<NonStratifiable> for ValidationError {
    fn from(error: NonStratifiable) -> Self {
        Self::NonStratifiable(error)
    }
}

impl From<EvaluationError> for ValidationError {
    fn from(error: EvaluationError) -> Self {
        Self::Evaluation(error)
    }
}
