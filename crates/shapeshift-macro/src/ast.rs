//! The Shapeshift abstract syntax tree.
//!
//! The serialized AST, not the text, is the interchange format between layers:
//! the text grammar (`shapeshift-parse`) is one rendering of it, and a binding
//! may construct it directly and hand it to Rust as JSON. Templates are kept
//! unexpanded here, so a binding can ship a template definition and have the
//! builder expand, check, and raise it.
//!
//! Every node that a diagnostic or report may point at carries a [`Span`].
//! Comments are trivia the compiler ignores; they live on the AST only so the
//! formatter can preserve them.

use serde::{Deserialize, Serialize};

/// A 1-based source position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Span {
    pub line: u32,
    pub column: u32,
}

impl Span {
    pub fn new(line: u32, column: u32) -> Self {
        Self { line, column }
    }
}

/// One `.shm` file.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Module {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefixes: Vec<PrefixDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<ImportDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shapes: Vec<ShapeDecl>,
    /// Comments before the first declaration (formatter trivia).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub leading_comments: Vec<String>,
    /// Comments after the last declaration (formatter trivia).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trailing_comments: Vec<String>,
    /// Header order as written, for the formatter: `base`, prefixes, and
    /// imports interleave freely in the source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub header_order: Vec<HeaderRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderRef {
    Base,
    Prefix(usize),
    Import(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixDecl {
    pub name: String,
    pub iri: String,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportTarget {
    /// `import <https://...>`
    Iri(String),
    /// `import "./x.shm"`
    Path(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportDecl {
    pub source: ImportTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trailing_comment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeverityLevel {
    Info,
    Warning,
    Violation,
}

impl SeverityLevel {
    pub fn word(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Violation => "violation",
        }
    }
}

/// Annotations that attach to the next shape or line: docstring, severity,
/// and author messages.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Annotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<SeverityLevel>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
}

impl Annotations {
    pub fn is_empty(&self) -> bool {
        self.doc.is_none() && self.severity.is_none() && self.messages.is_empty()
    }
}

/// `shape NAME params? target* { line* }`. A shape with parameters is a
/// template; one without is a named shape (a zero-argument macro).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShapeDecl {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<TargetDecl>,
    #[serde(default)]
    pub body: Vec<Line>,
    #[serde(default, skip_serializing_if = "Annotations::is_empty")]
    pub annotations: Annotations,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
    /// Comments after the last line of the body (formatter trivia).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trailing_comments: Vec<String>,
}

impl ShapeDecl {
    pub fn is_template(&self) -> bool {
        !self.params.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetDecl {
    pub kind: Target,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trailing_comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Target {
    /// `for every X` — instances of class `X`, with subclass closure.
    Class(NodeRef),
    /// `for every node with p` — subjects of `p`.
    SubjectsOf(NodeRef),
    /// `for every value of p` — objects of `p`.
    ObjectsOf(NodeRef),
    /// `for node n` — one specific node.
    Node(Value),
    /// `for every node matching S` — a shape-based target.
    Matching(Call),
    /// `for sparql "SELECT ?this ..."`.
    Sparql(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Line {
    pub kind: LineKind,
    #[serde(default, skip_serializing_if = "Annotations::is_empty")]
    pub annotations: Annotations,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trailing_comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LineKind {
    Field(Field),
    Include(Call),
    Boolean(Boolean),
    /// `only these properties`
    Closed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub path: PathExpr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<Count>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub qualifiers: Vec<Qualifier>,
    /// Written in block form (`name: { ... }`); formatter hint only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub block: bool,
}

/// An integer that may be a template parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntArg {
    Lit(u64),
    Param(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Count {
    Required,
    Optional,
    Exactly(IntArg),
    AtLeast(IntArg),
    AtMost(IntArg),
    Between(IntArg, IntArg),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualifiedBound {
    AtLeast,
    AtMost,
    Exactly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Qualifier {
    /// `each T` — all values satisfy `T` (also written as a bare test).
    Each(Test),
    /// `at least n is T` / `at most n is T` / `exactly n is T`.
    Qualified {
        bound: QualifiedBound,
        n: IntArg,
        test: Test,
    },
    /// `same as q` — `sh:equals`.
    SameAs(NodeRef),
    /// `different from q` — `sh:disjoint`.
    DifferentFrom(NodeRef),
    /// `less than q` — `sh:lessThan`.
    LessThan(NodeRef),
    /// `at most q` — `sh:lessThanOrEquals`.
    AtMostProperty(NodeRef),
    /// `unique language`.
    UniqueLanguage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    Iri,
    Literal,
    Blank,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DatatypeRef {
    /// A built-in keyword: `string`, `integer`, `decimal`, ...
    Builtin(String),
    /// `datatype D`
    Node(NodeRef),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Test {
    /// `a X` (or `is a X`) — class membership with subclass closure.
    Class(NodeRef),
    /// `matches S(...)` — conforms to a named shape.
    Matches(Call),
    /// `is X` — exactly this node or literal.
    Is(Value),
    /// `one of X Y Z`.
    OneOf(Vec<Value>),
    Datatype(DatatypeRef),
    Kind(NodeKind),
    /// `length a to b` (or `length n`).
    Length {
        min: Option<u64>,
        max: Option<u64>,
    },
    /// `from a to b`, `from a`, or `up to b` — inclusive numeric range.
    Range {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<Number>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<Number>,
    },
    /// `pattern "regex"` with optional `flags "i"`.
    Pattern {
        regex: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        flags: String,
    },
    /// `language "en" "fr"`.
    Language(Vec<String>),
}

/// A numeric literal as written; keeps the lexical form so `1.50` round-trips.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Number {
    pub lexical: String,
}

impl Number {
    pub fn is_integer(&self) -> bool {
        !self.lexical.contains(['.', 'e', 'E'])
    }
}

/// A reference to an RDF node or a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRef {
    /// `<https://...>`
    Iri(String),
    /// `brick:VAV`
    Prefixed { prefix: String, local: String },
    /// A bare name: a template parameter, or (in shape positions) a shape.
    /// May be dotted (`alias.Shape`, `Shape.field`).
    Name(String),
}

/// A node or a literal, for `is`, `one of`, and `for node`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Node(NodeRef),
    String {
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lang: Option<String>,
    },
    Number(Number),
    Boolean(bool),
}

/// `NAME` or `NAME(args)`; `NAME` may be `alias.NAME` or `Shape.field`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Call {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Arg>,
    #[serde(default)]
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Arg {
    /// A node, or a bare name bound to a parameter or naming a shape.
    Node(NodeRef),
    /// A literal value argument (string, number, boolean).
    Literal(Value),
    /// A path that is not a single node (`^p`, `p/q`, ...).
    Path(PathExpr),
    Int(u64),
    /// A shape call with arguments.
    Call(Call),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PathExpr {
    Node(NodeRef),
    /// `self`
    SelfPath,
    /// `^p`
    Inverse(Box<PathExpr>),
    /// `p / q`
    Seq(Vec<PathExpr>),
    /// `p or q`
    Alt(Vec<PathExpr>),
    /// `p*`
    Star(Box<PathExpr>),
    /// `p+`
    Plus(Box<PathExpr>),
    /// `p?`
    Optional(Box<PathExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Frame {
    AllOf,
    AnyOf,
    ExactlyOneOf,
    NoneOf,
    /// `when {C} then {B}`: blocks are `[C, B]`.
    When,
    /// `at least n of`.
    AtLeast(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Boolean {
    pub frame: Frame,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub lines: Vec<Line>,
    #[serde(default)]
    pub span: Span,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trailing_comments: Vec<String>,
}
