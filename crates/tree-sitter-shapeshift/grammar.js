/**
 * Tree-sitter grammar for Shapeshift, a macro language for RDF validation
 * rules over the shifty shape algebra.
 *
 * This grammar is for editors and the REPL: highlighting, folding, and
 * error-tolerant parsing as you type. The compiler's own parser lives in
 * crates/shapeshift-parse; a test there keeps the two in agreement.
 *
 * Newlines are significant inside shape bodies (a line ends at a newline or
 * `;`), so they are not extras: rules allow them explicitly where the
 * language does (between targets, before a body, before `then`).
 */

const commaSep1 = (rule) => seq(rule, repeat(seq(',', rule)));

/** `{ item (sep+ item)* }` with optional leading/trailing separators. */
const braced = ($, item) =>
  seq(
    '{',
    repeat($._sep),
    optional(seq(item, repeat(seq(repeat1($._sep), item)), repeat($._sep))),
    '}',
  );

module.exports = grammar({
  name: 'shapeshift',

  extras: ($) => [/[ \t\r]/, $.comment],

  word: ($) => $.identifier,

  conflicts: ($) => [
    [$.count, $.qualifier],
    [$._node, $.call],
  ],

  rules: {
    source_file: ($) => repeat(choice($._declaration, $._nl)),

    _nl: (_) => /\n/,

    comment: (_) => token(seq('--', /[^\n]*/)),

    _declaration: ($) =>
      choice($.base_decl, $.prefix_decl, $.import_decl, $.annotation, $.shape_decl),

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

    shape_decl: ($) =>
      seq(
        'shape',
        field('name', $.identifier),
        optional($.parameters),
        repeat(choice($._nl, $.target)),
        field('body', $.body),
      ),

    parameters: ($) => seq('(', commaSep1(field('param', $.identifier)), ')'),

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

    _line: ($) => choice($.annotation, $.field, $.include, $.boolean, $.closed),

    include: ($) => seq('include', $.call),

    closed: (_) => seq('only', 'these', 'properties'),

    field: ($) =>
      choice(
        seq(
          optional(field('name', $.label)),
          field('path', $._path),
          optional($.count),
          repeat(seq(optional(','), $.qualifier)),
        ),
        seq(field('name', $.label), $.field_block),
      ),

    field_block: ($) =>
      braced($, choice($.annotation, $.count, $.qualifier, field('path', $._path))),

    count: ($) =>
      choice(
        'required',
        'optional',
        seq('exactly', $._int),
        seq('at', 'least', $._int),
        seq('at', 'most', $._int),
        seq('between', $._int, 'and', $._int),
      ),

    qualifier: ($) =>
      choice(
        seq('each', $.test),
        seq(
          choice('exactly', seq('at', 'least'), seq('at', 'most')),
          $._int,
          'is',
          choice($.test, $._value),
        ),
        seq('same', 'as', $._node),
        seq('different', 'from', $._node),
        seq('less', 'than', $._node),
        seq('at', 'most', choice($.prefixed_name, $.iri)),
        seq('unique', 'language'),
        $.test,
      ),

    test: ($) =>
      choice(
        seq('a', field('class', $._node)),
        seq('is', 'a', field('class', $._node)),
        seq('matches', $.call),
        seq('is', $._value),
        seq('one', 'of', repeat1($._value)),
        $.datatype,
        seq('datatype', $._node),
        'iri',
        'literal',
        'blank',
        seq('length', $.integer, optional(seq('to', $.integer))),
        seq('length', 'at', choice('least', 'most'), $.integer),
        seq('from', $._number, optional(seq('to', $._number))),
        seq('up', 'to', $._number),
        seq('pattern', $.string, optional(seq('flags', $.string))),
        seq('language', repeat1($.string)),
      ),

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
            seq('all', 'of'),
            seq('none', 'of'),
            seq('exactly', 'one', 'of'),
            seq('at', 'least', $.integer, 'of'),
          ), $.frame)),
          repeat1($.block),
        ),
        seq('when', field('condition', $.block), repeat($._nl), 'then', field('consequence', $.block)),
      ),

    block: ($) => braced($, $._line),

    call: ($) =>
      seq(
        field('name', choice($.identifier, $.prefixed_name, $.iri)),
        optional($.arguments),
      ),

    arguments: ($) => seq('(', optional(commaSep1($._arg)), ')'),

    _arg: ($) => choice($._path, $.string, $._number, $.boolean_literal, $.call),

    _path: ($) =>
      choice($.alternative_path, $.sequence_path, $.inverse_path, $.modified_path, $._path_primary),

    alternative_path: ($) => prec.left(1, seq($._path, 'or', $._path)),
    sequence_path: ($) => prec.left(2, seq($._path, '/', $._path)),
    inverse_path: ($) => prec(3, seq('^', $._path)),
    modified_path: ($) => prec(4, seq($._path, choice('*', '+', '?'))),
    _path_primary: ($) => choice($._node, alias('self', $.self), seq('(', $._path, ')')),

    _node: ($) => choice($.prefixed_name, $.iri, $.identifier),

    _value: ($) => choice($._node, $.string, $._number, $.boolean_literal),

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

    decimal: (_) => /-?(\d+\.\d+|\d+(\.\d+)?[eE][+-]?\d+|\.\d+)|-\d+/,

    /** `prefix:local` with a non-empty local name. */
    prefixed_name: (_) => /([A-Za-z_][A-Za-z0-9_-]*)?:[A-Za-z0-9_%]([A-Za-z0-9_.%-]*[A-Za-z0-9_%-])?/,

    /** `name:` — a prefix in `prefix` declarations, a field name in bodies. */
    label: (_) => /[A-Za-z_][A-Za-z0-9_-]*:/,

    identifier: (_) => /[A-Za-z_][A-Za-z0-9_-]*(\.[A-Za-z_][A-Za-z0-9_-]*)*/,
  },
});
