#![allow(dead_code, clippy::large_enum_variant)]
use crate::context::{Context, ValidationContext};
use crate::runtime::validators::{
    AndConstraintComponent, ClassConstraintComponent, ClosedConstraintComponent,
    CustomConstraintComponent, DatatypeConstraintComponent, DisjointConstraintComponent,
    EqualsConstraintComponent, HasValueConstraintComponent, InConstraintComponent,
    LanguageInConstraintComponent, LessThanConstraintComponent,
    LessThanOrEqualsConstraintComponent, MaxCountConstraintComponent,
    MaxExclusiveConstraintComponent, MaxInclusiveConstraintComponent, MaxLengthConstraintComponent,
    MinCountConstraintComponent, MinExclusiveConstraintComponent, MinInclusiveConstraintComponent,
    MinLengthConstraintComponent, NodeConstraintComponent, NodeKindConstraintComponent,
    NotConstraintComponent, OrConstraintComponent, PatternConstraintComponent,
    PropertyConstraintComponent, QualifiedValueShapeComponent, SPARQLConstraintComponent,
    UniqueLangConstraintComponent, XoneConstraintComponent,
};
use crate::shape::NodeShape;
use crate::types::{ComponentID, Path, Severity, TraceItem};
use oxigraph::model::{NamedNode, NamedOrBlankNodeRef as SubjectRef, Term, TermRef};

/// The result of validating a single value node against a constraint component.
#[derive(Debug, Clone)]
pub(crate) enum ComponentValidationResult {
    /// Indicates that the validation passed. Contains the context of the validation.
    Pass(Context),
    /// Indicates that the validation failed. Contains the context and details of the failure.
    Fail(Context, ValidationFailure),
}

/// The result of a conformance check for a node against a shape.
/// Used by logical constraints like `sh:not`, `sh:and`, etc.
#[derive(Debug, Clone)]
pub(crate) enum ConformanceReport {
    /// The node conforms to the shape.
    Conforms,
    /// The node does not conform to the shape, with details of the first failure.
    NonConforms(ValidationFailure),
}

/// Details about a single validation failure.
#[derive(Debug, Clone)]
pub(crate) struct ValidationFailure {
    /// The ID of the component that was violated.
    pub component_id: ComponentID,
    /// The specific value node that failed validation, if applicable.
    pub failed_value_node: Option<Term>,
    /// A human-readable message describing the failure.
    pub message: String,
    /// The path of the validation result, which can be overridden by SPARQL-based constraints.
    pub result_path: Option<Path>,
    /// The constraint that was violated, for `sh:sparql` constraints.
    pub source_constraint: Option<Term>,
    /// An optional severity override provided by the constraint.
    pub severity: Option<Severity>,
    /// RDF message terms contributed by the constraint or validator.
    pub message_terms: Vec<Term>,
}

impl ValidationFailure {
    /// Convenience constructor that initializes optional metadata with sane defaults.
    pub fn new(
        component_id: ComponentID,
        failed_value_node: Option<Term>,
        message: String,
        result_path: Option<Path>,
        source_constraint: Option<Term>,
    ) -> Self {
        ValidationFailure {
            component_id,
            failed_value_node,
            message,
            result_path,
            source_constraint,
            severity: None,
            message_terms: Vec::new(),
        }
    }

    /// Attaches a severity override to the failure.
    pub fn with_severity(mut self, severity: Option<Severity>) -> Self {
        self.severity = severity;
        self
    }

    /// Attaches message RDF terms to the failure.
    pub fn with_message_terms(mut self, terms: Vec<Term>) -> Self {
        if !terms.is_empty() {
            self.message_terms = terms;
        }
        self
    }
}

/// A trait for components that can be represented in Graphviz DOT format.
pub(crate) trait GraphvizOutput {
    /// Generates a Graphviz DOT string representation for the component.
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String;
    /// Returns the SHACL IRI for the component type (e.g., `sh:MinCountConstraintComponent`).
    fn component_type(&self) -> NamedNode;
}

/// A trait for constraint components that can perform validation.
pub(crate) trait ValidateComponent {
    /// Validates the given context against the component's logic.
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String>;
}

/// A trait for converting `Term` or `TermRef` into `SubjectRef`.
pub(crate) trait ToSubjectRef {
    /// Converts to `SubjectRef`, panicking if the term is a `Literal`.
    fn to_subject_ref(&self) -> SubjectRef<'_>;
    /// Tries to convert to `SubjectRef`, returning a `Result`.
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String>;
}

impl ToSubjectRef for Term {
    fn to_subject_ref(&self) -> SubjectRef<'_> {
        self.try_to_subject_ref().expect("Invalid subject term")
    }
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String> {
        match self {
            Term::NamedNode(n) => Ok(n.into()),
            Term::BlankNode(b) => Ok(b.into()),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

impl<'a> ToSubjectRef for TermRef<'a> {
    fn to_subject_ref(&self) -> SubjectRef<'a> {
        match self {
            TermRef::NamedNode(n) => (*n).into(),
            TermRef::BlankNode(b) => (*b).into(),
            _ => panic!("Invalid subject term {:?}", self),
        }
    }
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'a>, String> {
        match self {
            TermRef::NamedNode(n) => Ok((*n).into()),
            TermRef::BlankNode(b) => Ok((*b).into()),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

/// An enum representing any of the SHACL constraint components.
#[derive(Debug)]
pub(crate) enum Component {
    /// `sh:node`
    NodeConstraint(NodeConstraintComponent),
    /// `sh:property`
    PropertyConstraint(PropertyConstraintComponent),
    /// `sh:qualifiedValueShape`
    QualifiedValueShape(QualifiedValueShapeComponent),
    /// `sh:class`
    ClassConstraint(ClassConstraintComponent),
    /// `sh:datatype`
    DatatypeConstraint(DatatypeConstraintComponent),
    /// `sh:nodeKind`
    NodeKindConstraint(NodeKindConstraintComponent),
    /// `sh:minCount`
    MinCount(MinCountConstraintComponent),
    /// `sh:maxCount`
    MaxCount(MaxCountConstraintComponent),
    /// `sh:minExclusive`
    MinExclusiveConstraint(MinExclusiveConstraintComponent),
    /// `sh:minInclusive`
    MinInclusiveConstraint(MinInclusiveConstraintComponent),
    /// `sh:maxExclusive`
    MaxExclusiveConstraint(MaxExclusiveConstraintComponent),
    /// `sh:maxInclusive`
    MaxInclusiveConstraint(MaxInclusiveConstraintComponent),
    /// `sh:minLength`
    MinLengthConstraint(MinLengthConstraintComponent),
    /// `sh:maxLength`
    MaxLengthConstraint(MaxLengthConstraintComponent),
    /// `sh:pattern`
    PatternConstraint(PatternConstraintComponent),
    /// `sh:languageIn`
    LanguageInConstraint(LanguageInConstraintComponent),
    /// `sh:uniqueLang`
    UniqueLangConstraint(UniqueLangConstraintComponent),
    /// `sh:equals`
    EqualsConstraint(EqualsConstraintComponent),
    /// `sh:disjoint`
    DisjointConstraint(DisjointConstraintComponent),
    /// `sh:lessThan`
    LessThanConstraint(LessThanConstraintComponent),
    /// `sh:lessThanOrEquals`
    LessThanOrEqualsConstraint(LessThanOrEqualsConstraintComponent),
    /// `sh:not`
    NotConstraint(NotConstraintComponent),
    /// `sh:and`
    AndConstraint(AndConstraintComponent),
    /// `sh:or`
    OrConstraint(OrConstraintComponent),
    /// `sh:xone`
    XoneConstraint(XoneConstraintComponent),
    /// `sh:closed`
    ClosedConstraint(ClosedConstraintComponent),
    /// `sh:hasValue`
    HasValueConstraint(HasValueConstraintComponent),
    /// `sh:in`
    InConstraint(InConstraintComponent),
    /// `sh:sparql`
    SPARQLConstraint(SPARQLConstraintComponent),
    /// A constraint from a SPARQL-based constraint component
    CustomConstraint(CustomConstraintComponent),
}

impl Component {
    /// Returns a human-readable label for the component type.
    pub(crate) fn label(&self) -> String {
        match self {
            Component::NodeConstraint(_) => "NodeConstraint".to_string(),
            Component::PropertyConstraint(_) => "PropertyConstraint".to_string(),
            Component::QualifiedValueShape(_) => "QualifiedValueShape".to_string(),
            Component::ClassConstraint(_) => "ClassConstraint".to_string(),
            Component::DatatypeConstraint(_) => "DatatypeConstraint".to_string(),
            Component::NodeKindConstraint(_) => "NodeKindConstraint".to_string(),
            Component::MinCount(_) => "MinCount".to_string(),
            Component::MaxCount(_) => "MaxCount".to_string(),
            Component::MinExclusiveConstraint(_) => "MinExclusiveConstraint".to_string(),
            Component::MinInclusiveConstraint(_) => "MinInclusiveConstraint".to_string(),
            Component::MaxExclusiveConstraint(_) => "MaxExclusiveConstraint".to_string(),
            Component::MaxInclusiveConstraint(_) => "MaxInclusiveConstraint".to_string(),
            Component::MinLengthConstraint(_) => "MinLengthConstraint".to_string(),
            Component::MaxLengthConstraint(_) => "MaxLengthConstraint".to_string(),
            Component::PatternConstraint(_) => "PatternConstraint".to_string(),
            Component::LanguageInConstraint(_) => "LanguageInConstraint".to_string(),
            Component::UniqueLangConstraint(_) => "UniqueLangConstraint".to_string(),
            Component::EqualsConstraint(_) => "EqualsConstraint".to_string(),
            Component::DisjointConstraint(_) => "DisjointConstraint".to_string(),
            Component::LessThanConstraint(_) => "LessThanConstraint".to_string(),
            Component::LessThanOrEqualsConstraint(_) => "LessThanOrEqualsConstraint".to_string(),
            Component::NotConstraint(_) => "NotConstraint".to_string(),
            Component::AndConstraint(_) => "AndConstraint".to_string(),
            Component::OrConstraint(_) => "OrConstraint".to_string(),
            Component::XoneConstraint(_) => "XoneConstraint".to_string(),
            Component::ClosedConstraint(_) => "ClosedConstraint".to_string(),
            Component::HasValueConstraint(_) => "HasValueConstraint".to_string(),
            Component::InConstraint(_) => "InConstraint".to_string(),
            Component::SPARQLConstraint(_) => "SPARQLConstraint".to_string(),
            Component::CustomConstraint(c) => c.local_name(),
        }
    }

    /// Delegates to the inner component to get its SHACL IRI type.
    pub(crate) fn component_type(&self) -> NamedNode {
        match self {
            Component::NodeConstraint(c) => c.component_type(),
            Component::PropertyConstraint(c) => c.component_type(),
            Component::QualifiedValueShape(c) => c.component_type(),
            Component::ClassConstraint(c) => c.component_type(),
            Component::DatatypeConstraint(c) => c.component_type(),
            Component::NodeKindConstraint(c) => c.component_type(),
            Component::MinCount(c) => c.component_type(),
            Component::MaxCount(c) => c.component_type(),
            Component::MinExclusiveConstraint(c) => c.component_type(),
            Component::MinInclusiveConstraint(c) => c.component_type(),
            Component::MaxExclusiveConstraint(c) => c.component_type(),
            Component::MaxInclusiveConstraint(c) => c.component_type(),
            Component::MinLengthConstraint(c) => c.component_type(),
            Component::MaxLengthConstraint(c) => c.component_type(),
            Component::PatternConstraint(c) => c.component_type(),
            Component::LanguageInConstraint(c) => c.component_type(),
            Component::UniqueLangConstraint(c) => c.component_type(),
            Component::EqualsConstraint(c) => c.component_type(),
            Component::DisjointConstraint(c) => c.component_type(),
            Component::LessThanConstraint(c) => c.component_type(),
            Component::LessThanOrEqualsConstraint(c) => c.component_type(),
            Component::NotConstraint(c) => c.component_type(),
            Component::AndConstraint(c) => c.component_type(),
            Component::OrConstraint(c) => c.component_type(),
            Component::XoneConstraint(c) => c.component_type(),
            Component::ClosedConstraint(c) => c.component_type(),
            Component::HasValueConstraint(c) => c.component_type(),
            Component::InConstraint(c) => c.component_type(),
            Component::SPARQLConstraint(c) => c.component_type(),
            Component::CustomConstraint(c) => c.component_type(),
        }
    }

    /// Delegates to the inner component to generate its Graphviz representation.
    pub(crate) fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        context: &ValidationContext,
    ) -> String {
        match self {
            Component::NodeConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::PropertyConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::QualifiedValueShape(c) => c.to_graphviz_string(component_id, context),
            Component::ClassConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::DatatypeConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::NodeKindConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinCount(c) => c.to_graphviz_string(component_id, context),
            Component::MaxCount(c) => c.to_graphviz_string(component_id, context),
            Component::MinExclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinInclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxExclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxInclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinLengthConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxLengthConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::PatternConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LanguageInConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::UniqueLangConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::EqualsConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::DisjointConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LessThanConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LessThanOrEqualsConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::NotConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::AndConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::OrConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::XoneConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::ClosedConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::HasValueConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::InConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::SPARQLConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::CustomConstraint(c) => c.to_graphviz_string(component_id, context),
        }
    }

    /// Delegates validation to the specific inner component.
    pub(crate) fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        trace.push(TraceItem::Component(component_id));
        let _component_scope = context.enter_component_scope(component_id, c.source_shape());
        match self {
            Component::ClassConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NodeConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::PropertyConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::QualifiedValueShape(comp) => comp.validate(component_id, c, context, trace),
            Component::DatatypeConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NodeKindConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::MinCount(comp) => comp.validate(component_id, c, context, trace),
            Component::MaxCount(comp) => comp.validate(component_id, c, context, trace),
            Component::MinLengthConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::MaxLengthConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::PatternConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LanguageInConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::UniqueLangConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NotConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::AndConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::OrConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::XoneConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::HasValueConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::InConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::SPARQLConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::DisjointConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::EqualsConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LessThanConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LessThanOrEqualsConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MinExclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MinInclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MaxExclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MaxInclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::ClosedConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::CustomConstraint(comp) => comp.validate(component_id, c, context, trace),
        }
    }
}

/// Checks if a given node conforms to the provided shape using the runtime components cache.
pub(crate) fn check_conformance_for_node(
    node_as_context: &mut Context,
    shape_to_check_against: &NodeShape,
    main_validation_context: &ValidationContext,
    trace: &mut Vec<TraceItem>,
) -> Result<ConformanceReport, String> {
    if shape_to_check_against.is_deactivated() {
        return Ok(ConformanceReport::Conforms);
    }

    trace.push(TraceItem::NodeShape(*shape_to_check_against.identifier()));

    for constraint_id in shape_to_check_against.constraints() {
        let component = main_validation_context
            .get_component(constraint_id)
            .ok_or_else(|| format!("Logical check: Component not found: {}", constraint_id))?;

        match component.validate(
            *constraint_id,
            node_as_context,
            main_validation_context,
            trace,
        ) {
            Ok(validation_results) => {
                if let Some(ComponentValidationResult::Fail(_ctx, failure)) = validation_results
                    .into_iter()
                    .find(|r| matches!(r, ComponentValidationResult::Fail(_, _)))
                {
                    return Ok(ConformanceReport::NonConforms(failure));
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(ConformanceReport::Conforms)
}
