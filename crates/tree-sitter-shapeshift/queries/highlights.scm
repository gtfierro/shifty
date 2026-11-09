; Shapeshift highlighting

(comment) @comment
(string) @string
(iri) @string.special.url
(integer) @number
(decimal) @number
(date) @number
(date_time) @number
(time) @number
(boolean_literal) @constant.builtin

[
  "base" "prefix" "import" "as"
  "shape" "for" "every" "node" "value" "of" "with" "matching" "sparql"
  "only" "these" "properties"
  "when" "then" "one" "path"
] @keyword

[
  "required" "optional" "exactly" "at" "least" "most" "between" "and"
  "each" "is" "a" "matches" "same" "different" "from" "above" "below"
  "unique" "language" "length" "to" "up" "pattern" "flags" "datatype"
  "iri" "literal" "blank" "or"
] @keyword.operator

[ "doc" "message" "severity" ] @attribute
(severity_level) @constant.builtin
(frame) @keyword

(datatype) @type.builtin
(self) @variable.builtin

(shape_decl name: (identifier) @type.definition)
(path_decl name: (identifier) @function.macro)
(group path: (identifier) @function.macro)
(row name: (label) @property)
(parameter name: (identifier) @variable.parameter)
(named_argument name: (identifier) @variable.parameter)
(call name: (_) @function)
(test class: (_) @type)
(target class: (_) @type)
(label) @property
(prefixed_name) @constant
(identifier) @variable

[ "{" "}" "(" ")" ] @punctuation.bracket
[ "," ";" ] @punctuation.delimiter
[ "^" "/" "*" "+" "?" ] @operator
