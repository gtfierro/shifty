; Shapeshift highlighting

(comment) @comment
(string) @string
(iri) @string.special.url
(integer) @number
(decimal) @number
(boolean_literal) @constant.builtin

[
  "base" "prefix" "import" "as"
  "shape" "for" "every" "node" "value" "of" "with" "matching" "sparql"
  "include" "only" "these" "properties"
  "when" "then" "one"
] @keyword

[
  "required" "optional" "exactly" "at" "least" "most" "between" "and"
  "each" "is" "a" "matches" "same" "different" "from" "less" "than"
  "unique" "language" "length" "to" "up" "pattern" "flags" "datatype"
  "iri" "literal" "blank" "or"
] @keyword.operator

[ "doc" "message" "severity" ] @attribute
(severity_level) @constant.builtin
(frame) @keyword

(datatype) @type.builtin
(self) @variable.builtin

(shape_decl name: (identifier) @type.definition)
(parameters param: (identifier) @variable.parameter)
(call name: (_) @function)
(test class: (_) @type)
(target class: (_) @type)
(label) @property
(prefixed_name) @constant
(identifier) @variable

[ "{" "}" "(" ")" ] @punctuation.bracket
[ "," ";" ] @punctuation.delimiter
[ "^" "/" "*" "+" "?" ] @operator
