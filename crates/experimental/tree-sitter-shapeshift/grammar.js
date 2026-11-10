/**
 * Tree-sitter grammar for Shapeshift, a macro language for RDF validation
 * rules over the shifty shape algebra.
 *
 * This grammar is for editors and the REPL: highlighting, folding, and
 * error-tolerant parsing as you type. The compiler's own parser lives in
 * crates/experimental/shapeshift-parse; a test keeps the two in agreement.
 *
 * Newlines are significant inside shape bodies (a line ends at a newline or
 * `;`), so they are not extras: rules allow them explicitly where the
 * language does (between targets, before a body, before `then`).
 */

const commaSep1 = (rule) => seq(rule, repeat(seq(',', rule)));

/**
 * `( item, item )`, where the items may sit one per line with a trailing
 * comma, as the formatter writes long calls.
 */
const parenList = ($, item, required) => {
  const nl = optional($._nls);
  const items = seq(item, nl, repeat(seq(',', nl, item, nl)), optional(seq(',', nl)));
  return seq('(', nl, required ? items : optional(items), ')');
};

/** `{ item (sep+ item)* }` with optional leading/trailing separators. */
const braced = ($, item) =>
  seq(
    '{',
    repeat($._sep),
    optional(seq(item, repeat(seq(repeat1($._sep), item)), repeat($._sep))),
    '}',
  );

/**
 * Tests of a value. Every test starts with the word that says what kind of
 * test it is: `a` for a class, `one of` for values, `matches` for a shape.
 */
const valueTests = ($) => [
  // `a brick:AHU`, or one of several classes: `a brick:AHU or brick:RTU`
  seq('a', field('class', $._node), repeat(seq('or', field('class', $._node)))),
  seq('one', 'of', repeat1($._value)),
  $.datatype,
  seq('datatype', $._node),
  'iri',
  'literal',
  'blank',
  prec.right(seq(
    'length',
    choice(
      $.integer,
      seq('from', $.integer, optional(seq('to', $.integer))),
      seq('up', 'to', $.integer),
    ),
  )),
  // `from` and `up to` include the bound, `above` and `below` do not.
  // `above 0 below ex:q` reads `below` as the range's upper end. The ends
  // are numbers, dates, dateTimes, or times.
  prec.right(seq(
    choice(seq('from', $._bound), seq('above', $._bound)),
    optional(choice(seq('to', $._bound), seq('below', $._bound))),
  )),
  seq('up', 'to', $._bound),
  seq('below', $._bound),
  seq('pattern', $.string, optional(seq('flags', $.string))),
  seq('language', repeat1($.string)),
];

module.exports = grammar({
  name: 'shapeshift',

  extras: ($) => [/[ \t\r]/, $.comment],

  word: ($) => $.identifier,

  conflicts: ($) => [
    [$.boolean],
  ],

  rules: {
    source_file: ($) => repeat(choice($._declaration, $._nl)),

    _nl: (_) => /\n/,

    _nls: ($) => repeat1($._nl),

    comment: (_) => token(seq('--', /[^\n]*/)),

    _declaration: ($) =>
      choice($.base_decl, $.prefix_decl, $.import_decl, $.annotation, $.path_decl, $.shape_decl),

    base_decl: ($) => seq('base', field('iri', $.iri)),

    prefix_decl: ($) => seq('prefix', field('name', $.label), field('iri', $.iri)),

    import_decl: ($) =>
      seq(
        'import',
        field('source', choice($.iri, $.string)),
        optional(seq('as', field('alias', $.identifier))),
      ),

    annotation: ($) =>
      choice(
        seq('doc', $.string),
        seq('message', $.string),
        seq('severity', field('level', alias(choice('info', 'warning', 'violation'), $.severity_level))),
      ),

    /** `path points = brick:hasPoint`, `path downstream(p = brick:hasPart?) = brick:feeds+/p` */
    path_decl: ($) =>
      seq(
        'path',
        field('name', $.identifier),
        optional($.parameters),
        '=',
        field('path', $._path),
      ),

    shape_decl: ($) =>
      seq(
        'shape',
        field('name', $.identifier),
        optional($.parameters),
        repeat(choice($._nl, $.target)),
        field('body', $.body),
      ),

    parameters: ($) => parenList($, field('param', $.parameter), true),

    /** `name` or `name = default` */
    parameter: ($) =>
      seq(field('name', $.identifier), optional(seq('=', field('default', $._arg)))),

    target: ($) =>
      seq(
        'for',
        choice(
          seq('every', 'node', 'with', $._node),
          seq('every', 'node', 'matching', $.call),
          seq('every', 'value', 'of', $._node),
          seq('every', field('class', $._node)),
          seq('node', $._value),
          seq('sparql', $.string),
        ),
      ),

    body: ($) => braced($, $._line),

    _sep: ($) => choice($._nl, ';'),

    _line: ($) =>
      choice(
        $.annotation, $.field, $.boolean, $.closed, $.is_line, $.group,
      ),

    /** `is a brick:AHU`: a test on the focus node itself. A shape is
     * composed by writing it alone on a line, which parses as a field. */
    is_line: ($) => seq('is', alias($._value_test, $.test)),

    /** `brick:hasPoint [count] { row* }`: named fields along one path. */
    group: ($) =>
      seq(
        field('path', $._path),
        optional($.count),
        braced($, choice($.annotation, $.row)),
      ),

    /** `label: ITEM [count] [{ ... }]`, its `doc` on the line before. */
    row: ($) =>
      seq(
        optional(field('name', $.label)),
        field('item', $.call),
        repeat(choice($.count, $.block)),
      ),

    closed: (_) => seq('only', 'these', 'properties'),

    // `Sensor` alone on a line composes it, and parses as a field's path.
    // `PATH COUNT? {` is a group: no qualifier starts with `{`.
    field: ($) =>
      seq(
        optional(field('name', $.label)),
        field('path', $._path),
        optional($.count),
        repeat(seq(optional(','), $.qualifier)),
      ),

    count: ($) =>
      choice(
        'required',
        'optional',
        seq('exactly', $._int),
        seq('at', 'least', $._int),
        seq('at', 'most', $._int),
        seq('between', $._int, 'and', $._int),
      ),

    // A test of the values always follows `each`: after a path, `ex:p ex:q`
    // is more likely a typo for `ex:p/ex:q`.
    qualifier: ($) =>
      choice(
        seq('each', $.test),
        seq(
          choice('exactly', seq('at', 'least'), seq('at', 'most')),
          $._int,
          choice(
            seq('is', alias($._is_test, $.test)),
            alias($._matches, $.test),
          ),
        ),
        seq('same', 'as', $._node),
        seq('different', 'from', $._node),
        seq('below', $._node),
        seq('up', 'to', $._node),
        seq('unique', 'language'),
      ),

    test: ($) => choice(...valueTests($), $._matches, $.block),

    _value_test: ($) => choice(...valueTests($)),

    _is_test: ($) => choice(...valueTests($), $.block),

    _matches: ($) => seq('matches', $.call),

    datatype: (_) =>
      choice(
        'string', 'integer', 'decimal', 'double', 'float', 'boolean',
        'date', 'dateTime', 'time', 'anyURI', 'langString',
      ),

    boolean: ($) =>
      choice(
        seq(
          field('frame', alias(choice(
            seq('any', 'of'),
            seq('none', 'of'),
            seq('exactly', 'one', 'of'),
            seq('at', 'least', $.integer, 'of'),
            seq('at', 'most', $.integer, 'of'),
          ), $.frame)),
          // the formatter puts long frames' blocks on aligned lines
          $.block,
          repeat(seq(repeat($._nl), $.block)),
        ),
        seq('when', field('condition', $.block), repeat($._nl), 'then', field('consequence', $.block)),
      ),

    block: ($) => braced($, $._line),

    call: ($) =>
      seq(
        field('name', choice($.identifier, $.prefixed_name, $.iri)),
        optional($.arguments),
      ),

    arguments: ($) => parenList($, choice($._arg, $.named_argument), false),

    named_argument: ($) => seq(field('name', $.identifier), '=', field('value', $._arg)),

    // `T(x)` here is a shape or a path template, told apart once names are
    // resolved; it parses as a path.
    _arg: ($) =>
      choice($._path, $.string, $._number, $._temporal, $.boolean_literal, $.block),

    _call_args: ($) => seq(field('name', $.identifier), $.arguments),

    _path: ($) =>
      choice($.alternative_path, $.sequence_path, $.inverse_path, $.modified_path, $._path_primary),

    alternative_path: ($) => prec.left(1, seq($._path, 'or', $._path)),
    sequence_path: ($) => prec.left(2, seq($._path, '/', $._path)),
    inverse_path: ($) => prec(3, seq('^', $._path)),
    modified_path: ($) => prec(4, seq($._path, choice('*', '+', '?'))),
    _path_primary: ($) =>
      choice($._node, alias('self', $.self), seq('(', $._path, ')'), alias($._call_args, $.call)),

    _node: ($) => choice($.prefixed_name, $.iri, $.identifier),

    _value: ($) => choice($._node, $._literal),

    _literal: ($) => choice($.string, $._number, $._temporal, $.boolean_literal),

    _bound: ($) => choice($._number, $._temporal),

    _temporal: ($) => choice($.date, $.date_time, $.time),

    _int: ($) => choice($.integer, $.identifier),

    _number: ($) => choice($.integer, $.decimal),

    boolean_literal: (_) => choice('true', 'false'),

    iri: (_) => token(seq('<', /[^<>"{}|^`\\\s]*/, '>')),

    string: (_) =>
      token(
        seq(
          choice(
            seq('"""', repeat(choice(/[^"\\]/, /\\./, /"[^"]/, /""[^"]/)), '"""'),
            seq('"', repeat(choice(/[^"\\\n]/, /\\./)), '"'),
          ),
          optional(seq('@', /[A-Za-z]+(-[A-Za-z0-9]+)*/)),
        ),
      ),

    integer: (_) => /\d+/,

    // XSD lexical forms, written bare: `2024-01-31`, `2024-01-31T08:00:00Z`,
    // `08:00:00.5+02:00`. Longer than an integer, so they win where both fit.
    date: (_) => token(/\d{4}-\d{2}-\d{2}(Z|[+-]\d{2}:\d{2})?/),

    date_time: (_) =>
      token(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?/),

    time: (_) => token(/\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?/),

    decimal: (_) => /-?(\d+\.\d+|\d+(\.\d+)?[eE][+-]?\d+|\.\d+)|-\d+/,

    /** `prefix:local` with a non-empty local name. */
    prefixed_name: (_) => /([A-Za-z_][A-Za-z0-9_-]*)?:[A-Za-z0-9_%]([A-Za-z0-9_.%-]*[A-Za-z0-9_%-])?/,

    /** `name:` — a prefix in `prefix` declarations, a field name in bodies. */
    label: (_) => /[A-Za-z_][A-Za-z0-9_-]*:/,

    identifier: (_) => /[A-Za-z_][A-Za-z0-9_-]*(\.[A-Za-z_][A-Za-z0-9_-]*)*/,
  },
});
