mod component;
mod handlers;

pub use component::{ComponentCodegen, EmitContext, PropertyEmission};

use crate::plan::ComponentKind;
use handlers::{
    class::ClassHandler,
    closed::ClosedHandler,
    custom::CustomHandler,
    datatype::DatatypeHandler,
    has_value::HasValueHandler,
    in_list::InHandler,
    language::{LanguageInHandler, UniqueLangHandler},
    length::{MaxLengthHandler, MinLengthHandler},
    logic::{AndHandler, NotHandler, OrHandler, XoneHandler},
    max_count::MaxCountHandler,
    min_count::MinCountHandler,
    node::NodeHandler,
    node_kind::NodeKindHandler,
    pattern::PatternHandler,
    property::PropertyHandler,
    property_pair::{DisjointHandler, EqualsHandler, LessThanHandler, LessThanOrEqualsHandler},
    qualified::QualifiedValueShapeHandler,
    sparql::SparqlHandler,
    value_range::{
        MaxExclusiveHandler, MaxInclusiveHandler, MinExclusiveHandler, MinInclusiveHandler,
    },
};

static DATATYPE_HANDLER: DatatypeHandler = DatatypeHandler;
static CLASS_HANDLER: ClassHandler = ClassHandler;
static NODE_KIND_HANDLER: NodeKindHandler = NodeKindHandler;
static NODE_HANDLER: NodeHandler = NodeHandler;
static MIN_EXCLUSIVE_HANDLER: MinExclusiveHandler = MinExclusiveHandler;
static MIN_INCLUSIVE_HANDLER: MinInclusiveHandler = MinInclusiveHandler;
static MAX_EXCLUSIVE_HANDLER: MaxExclusiveHandler = MaxExclusiveHandler;
static MAX_INCLUSIVE_HANDLER: MaxInclusiveHandler = MaxInclusiveHandler;
static MAX_COUNT_HANDLER: MaxCountHandler = MaxCountHandler;
static MIN_COUNT_HANDLER: MinCountHandler = MinCountHandler;
static MIN_LENGTH_HANDLER: MinLengthHandler = MinLengthHandler;
static MAX_LENGTH_HANDLER: MaxLengthHandler = MaxLengthHandler;
static NOT_HANDLER: NotHandler = NotHandler;
static AND_HANDLER: AndHandler = AndHandler;
static OR_HANDLER: OrHandler = OrHandler;
static XONE_HANDLER: XoneHandler = XoneHandler;
static PATTERN_HANDLER: PatternHandler = PatternHandler;
static LANGUAGE_IN_HANDLER: LanguageInHandler = LanguageInHandler;
static UNIQUE_LANG_HANDLER: UniqueLangHandler = UniqueLangHandler;
static EQUALS_HANDLER: EqualsHandler = EqualsHandler;
static DISJOINT_HANDLER: DisjointHandler = DisjointHandler;
static LESS_THAN_HANDLER: LessThanHandler = LessThanHandler;
static LESS_THAN_OR_EQUALS_HANDLER: LessThanOrEqualsHandler = LessThanOrEqualsHandler;
static HAS_VALUE_HANDLER: HasValueHandler = HasValueHandler;
static IN_HANDLER: InHandler = InHandler;
static QUALIFIED_HANDLER: QualifiedValueShapeHandler = QualifiedValueShapeHandler;
static CLOSED_HANDLER: ClosedHandler = ClosedHandler;
static PROPERTY_HANDLER: PropertyHandler = PropertyHandler;
static SPARQL_HANDLER: SparqlHandler = SparqlHandler;
static CUSTOM_HANDLER: CustomHandler = CustomHandler;
pub fn lookup(kind: ComponentKind) -> &'static dyn ComponentCodegen {
    match kind {
        ComponentKind::Class => &CLASS_HANDLER,
        ComponentKind::Datatype => &DATATYPE_HANDLER,
        ComponentKind::NodeKind => &NODE_KIND_HANDLER,
        ComponentKind::Node => &NODE_HANDLER,
        ComponentKind::MinExclusive => &MIN_EXCLUSIVE_HANDLER,
        ComponentKind::MinInclusive => &MIN_INCLUSIVE_HANDLER,
        ComponentKind::MaxExclusive => &MAX_EXCLUSIVE_HANDLER,
        ComponentKind::MaxInclusive => &MAX_INCLUSIVE_HANDLER,
        ComponentKind::MaxCount => &MAX_COUNT_HANDLER,
        ComponentKind::MinCount => &MIN_COUNT_HANDLER,
        ComponentKind::MinLength => &MIN_LENGTH_HANDLER,
        ComponentKind::MaxLength => &MAX_LENGTH_HANDLER,
        ComponentKind::Not => &NOT_HANDLER,
        ComponentKind::And => &AND_HANDLER,
        ComponentKind::Or => &OR_HANDLER,
        ComponentKind::Xone => &XONE_HANDLER,
        ComponentKind::Pattern => &PATTERN_HANDLER,
        ComponentKind::LanguageIn => &LANGUAGE_IN_HANDLER,
        ComponentKind::UniqueLang => &UNIQUE_LANG_HANDLER,
        ComponentKind::Equals => &EQUALS_HANDLER,
        ComponentKind::Disjoint => &DISJOINT_HANDLER,
        ComponentKind::LessThan => &LESS_THAN_HANDLER,
        ComponentKind::LessThanOrEquals => &LESS_THAN_OR_EQUALS_HANDLER,
        ComponentKind::HasValue => &HAS_VALUE_HANDLER,
        ComponentKind::In => &IN_HANDLER,
        ComponentKind::QualifiedValueShape => &QUALIFIED_HANDLER,
        ComponentKind::Closed => &CLOSED_HANDLER,
        ComponentKind::Property => &PROPERTY_HANDLER,
        ComponentKind::Sparql => &SPARQL_HANDLER,
        ComponentKind::Custom => &CUSTOM_HANDLER,
    }
}
